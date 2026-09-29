"""Operation queue manager — per-agent parallel processor.

Reads pending operations from the operation_queue table and executes them.
Operations on different agents run concurrently; operations on the same agent
are serialized. All state is in the DB — survives restarts.
"""

import asyncio
import json
import logging

import httpx

from file_hunter.core import cancel_task
from file_hunter.db import db_writer, read_db
from file_hunter.services.activity import op_label, register, unregister
from file_hunter.services.dup_counts import run_hash_file
from file_hunter.ws.scan import broadcast
from file_hunter.helpers import utc_now

logger = logging.getLogger("file_hunter")

running = False
loop_task: asyncio.Task | None = None
paused = False

# Event that running operations await between iterations.
# Set = running normally. Cleared = suspended (pause active).
pause_event: asyncio.Event = asyncio.Event()
pause_event.set()  # start in running state

# Running operations: op_id -> (agent_id, asyncio.Task)
running_ops: dict[int, tuple[int | None, asyncio.Task]] = {}
# Running operation types: op_id -> op_type string
running_op_types: dict[int, str] = {}


async def enqueue(op_type: str, agent_id: int | None, params: dict) -> int:
    """Insert a new operation into the operation_queue table.

    Args:
        op_type: Operation type string (e.g. "scan_dir", "backfill_location",
            "rehash_partial", "hash_file").
        agent_id: Target agent ID, or None for agent-independent operations.
        params: JSON-serializable dict of operation parameters (typically
            includes location_id and location_name).

    Returns:
        int: The auto-generated operation ID (operation_queue.id).

    Notes:
    """
    async with db_writer() as db:
        cursor = await db.execute(
            "INSERT INTO operation_queue (type, agent_id, params, created_at) "
            "VALUES (?, ?, ?, ?)",
            (op_type, agent_id, json.dumps(params), utc_now("auto")),
        )
        op_id = cursor.lastrowid
    await broadcast_queue_state()
    return op_id


async def cancel(op_id: int) -> bool:
    """Cancel a pending or running operation.

    Pending: sets status to cancelled in the DB.
    Running: cancels the executing task (triggers CancelledError in the handler).
    Returns True if the operation was found and cancelled.
    """
    if op_id in running_ops:
        _, task = running_ops[op_id]
        task.cancel()
        # broadcast happens when reap_finished picks up the CancelledError
        return True

    async with db_writer() as db:
        cursor = await db.execute(
            "UPDATE operation_queue SET status = 'cancelled', completed_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (utc_now("auto"), op_id),
        )
        changed = cursor.rowcount
    if changed > 0:
        await broadcast_queue_state()
        return True
    return False


async def cancel_by_location(location_id: int) -> bool:
    """Cancel a running or pending operation for a location. Returns True if found."""
    async with read_db() as db:
        # Check running ops first
        for op_id, (_, task) in list(running_ops.items()):
            row = await db.execute_fetchall(
                "SELECT params FROM operation_queue WHERE id = ?", (op_id,)
            )
            if row:
                params = json.loads(row[0]["params"] or "{}")
                if params.get("location_id") == location_id:
                    task.cancel()
                    # broadcast happens when reap_finished picks up the CancelledError
                    return True

        # Check pending ops
        rows = await db.execute_fetchall(
            "SELECT id, params FROM operation_queue WHERE status = 'pending' ORDER BY id"
        )
    for row in rows:
        params = json.loads(row["params"] or "{}")
        if params.get("location_id") == location_id:
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE operation_queue SET status = 'cancelled', completed_at = ? "
                    "WHERE id = ?",
                    (utc_now("auto"), row["id"]),
                )
            await broadcast_queue_state()
            return True

    return False


async def get_pending_count(agent_id: int | None = None) -> int:
    """Return count of pending operations, optionally filtered by agent."""
    async with read_db() as db:
        if agent_id is not None:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM operation_queue "
                "WHERE status = 'pending' AND agent_id = ?",
                (agent_id,),
            )
        else:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM operation_queue WHERE status = 'pending'"
            )
        row = await cursor.fetchone()
    return row[0]


async def get_queue_status() -> list[dict]:
    """Return pending and running operations for UI display."""
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT o.id, o.type, o.status, o.agent_id, o.params, "
            "o.created_at, o.started_at "
            "FROM operation_queue o "
            "WHERE o.status IN ('pending', 'running') "
            "ORDER BY o.id"
        )
        results = []
        for r in rows:
            item = dict(r)
            params = json.loads(item.get("params") or "{}")
            item["params"] = params
            loc_id = params.get("location_id")
            if loc_id:
                loc_row = await db.execute_fetchall(
                    "SELECT name FROM locations WHERE id = ?", (loc_id,)
                )
                item["location_id"] = loc_id
                item["location_name"] = loc_row[0]["name"] if loc_row else None
            results.append(item)
    return results


def is_location_running(location_id: int) -> bool:
    """Sync check: is a scan currently running for this location?

    Inspects in-memory running ops only (no DB access). Used by
    extensions.is_agent_scanning as a fallback.
    """
    for op_id, (_, task) in running_ops.items():
        if task.done():
            continue
        # We don't have params in memory, but we can check the DB
        # synchronously is not possible. Use a cached approach instead.
        pass
    # Fall back to checking running_locations cache
    return location_id in running_locations


# Cache of location_ids with running operations (updated by set_running)
running_locations: set[int] = set()


def track_location(op_id: int, location_id: int | None):
    """Track that a location has a running operation."""
    if location_id is not None:
        running_locations.add(location_id)


def untrack_location(location_id: int | None):
    """Remove location from running set."""
    if location_id is not None:
        running_locations.discard(location_id)


def start():
    """Start the queue manager background loop."""
    global running, loop_task
    if running:
        return
    running = True
    loop_task = asyncio.create_task(run())
    logger.info("Queue manager started")


async def stop():
    """Stop the queue manager and wait for running operations to finish."""
    global running, loop_task
    running = False

    # Wait for running operations to complete gracefully
    running_tasks = [task for _, task in running_ops.values()]
    if running_tasks:
        logger.info("Waiting for %d operation(s) to complete...", len(running_tasks))
        done, pending = await asyncio.wait(running_tasks, timeout=10)
        for task in done:
            try:
                task.result()
            except BaseException:
                pass
        for task in pending:
            task.cancel()
        if pending:
            done2, _ = await asyncio.wait(pending, timeout=2)
            for task in done2:
                try:
                    task.result()
                except BaseException:
                    pass

    await cancel_task(loop_task)
    loop_task = None

    # Clean up activity registrations for any ops that were still running
    for op_id in list(running_ops):
        unregister(f"op-{op_id}")
    running_ops.clear()
    running_op_types.clear()

    logger.info("Queue manager stopped")


async def pause():
    """Suspend all running operations and prevent new ones from dispatching.

    Clears pause_event so running ops block at their next checkpoint.
    Waits briefly for ops to reach a checkpoint, then returns.
    """
    global paused
    if paused:
        return
    paused = True
    pause_event.clear()
    logger.info("Queue manager: suspending %d running op(s)", len(running_ops))
    # Give running ops time to hit their checkpoint and block
    await asyncio.sleep(0.5)
    logger.info("Queue manager: paused")


def resume():
    """Resume the queue manager — unblock suspended operations."""
    global paused
    if not paused:
        return
    paused = False
    pause_event.set()
    logger.info("Queue manager: resumed, operations unblocked")


async def wait_if_paused():
    """Checkpoint for long-running operations. Blocks while queue is paused."""
    await pause_event.wait()


class paused_queue:
    """Async context manager: pause queue, broadcast, yield, resume on exit.

    Usage:
        async with paused_queue("import", location_name):
            await do_work()
    """

    def __init__(self, reason: str, location: str = ""):
        self.reason = reason
        self.location = location

    async def __aenter__(self):
        await pause()
        await broadcast(
            {
                "type": "queue_paused",
                "reason": self.reason,
                "location": self.location,
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        resume()
        await broadcast({"type": "queue_resumed"})
        return False


async def recover_interrupted():
    """On startup, reset any 'running' operations back to 'pending'
    and mark orphaned scan records as 'interrupted'."""
    async with db_writer() as db:
        # Migration: delete_location moved to housekeeping queue
        del_cursor = await db.execute(
            "DELETE FROM operation_queue WHERE type = 'delete_location'"
        )
        if del_cursor.rowcount > 0:
            logger.info(
                "Queue manager: removed %d stale delete_location ops (moved to housekeeping)",
                del_cursor.rowcount,
            )

        cursor = await db.execute(
            "UPDATE operation_queue SET status = 'pending', started_at = NULL "
            "WHERE status = 'running'"
        )
        if cursor.rowcount > 0:
            logger.info(
                "Queue manager: recovered %d interrupted operations", cursor.rowcount
            )
        # Mark any scan records left as 'running' (server crashed mid-scan)
        scan_cursor = await db.execute(
            "UPDATE scans SET status = 'interrupted' WHERE status = 'running'"
        )
        if scan_cursor.rowcount > 0:
            logger.info(
                "Queue manager: marked %d orphaned scans as interrupted",
                scan_cursor.rowcount,
            )


async def run():
    """Main loop — poll for pending operations and start them per-agent."""
    await recover_interrupted()

    while running:
        try:
            await reap_finished()

            # When paused, don't dispatch new ops
            if paused:
                await asyncio.sleep(1)
                continue

            busy_agents = {aid for aid, _ in running_ops.values()}

            ops = await next_pending_ops(busy_agents)
            if not ops:
                await asyncio.sleep(1)
                continue

            for op in ops:
                op_id = op["id"]
                op_type = op["type"]
                agent_id = op["agent_id"]
                params = json.loads(op["params"])

                loc_id = params.get("location_id")
                loc_name = params.get("location_name", "")
                await set_running(op_id)
                track_location(op_id, loc_id)
                logger.info("Queue manager: starting %s (id=%d)", op_type, op_id)

                register(f"op-{op_id}", op_label(op_type, loc_name))

                task = asyncio.create_task(execute(op_type, op_id, agent_id, params))
                running_ops[op_id] = (agent_id, task)
                running_op_types[op_id] = op_type

            # Broadcast updated queue state (pending→running transitions)
            await broadcast_queue_state()

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Queue manager: unexpected error in main loop")
            await asyncio.sleep(5)

    # Running ops are handled by stop() — don't cancel here


async def reap_finished():
    """Check running tasks for completion and update their status."""
    reaped = False
    for op_id in list(running_ops):
        agent_id, task = running_ops[op_id]
        if not task.done():
            continue

        reaped = True
        del running_ops[op_id]
        running_op_types.pop(op_id, None)

        unregister(f"op-{op_id}")

        # Untrack location
        async with read_db() as db:
            row = await db.execute_fetchall(
                "SELECT params FROM operation_queue WHERE id = ?", (op_id,)
            )
        if row:
            loc_id = json.loads(row[0]["params"] or "{}").get("location_id")
            untrack_location(loc_id)

        try:
            task.result()
            await set_completed(op_id)
            logger.info("Queue manager: completed (id=%d)", op_id)
        except asyncio.CancelledError:
            await set_status(op_id, "cancelled")
            logger.info("Queue manager: cancelled (id=%d)", op_id)
        except (ConnectionError, OSError, httpx.ConnectError, httpx.TransportError) as e:
            logger.warning(
                "Queue manager: agent unavailable (id=%d): %s — re-queuing",
                op_id,
                e,
            )
            await set_status_pending(op_id)
        except Exception as e:
            logger.exception("Queue manager: failed (id=%d)", op_id)
            await set_failed(op_id, str(e))

    if reaped:
        await broadcast_queue_state()


# Op types that write to a shared resource (ChromaDB) and must not run
# concurrently with each other, regardless of which agent they belong to.
SERIALISE_GROUP = {"similarity_scan", "embed_file", "extract_markdown"}

# Op types that run independently on the agent and can proceed even when
# the agent is busy with another operation (e.g. a long-running scan).
# The agent enforces its own concurrency limits for these.
CONCURRENT_OPS = {"transcode", "raw_convert"}


async def next_pending_ops(busy_agents: set) -> list[dict]:
    """Fetch pending operations for agents that are online and not busy."""
    from file_hunter.ws.agent import get_online_agent_ids

    online_agents = set(get_online_agent_ids())

    # Check if a serialised op is already running
    serialised_running = any(
        op_type in SERIALISE_GROUP
        for op_type in running_op_types.values()
    )

    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, type, status, agent_id, params "
            "FROM operation_queue WHERE status = 'pending' "
            "ORDER BY id"
        )
    result = []
    seen_agents: set[int | None] = set()
    serialised_seen = False
    for row in rows:
        aid = row["agent_id"]
        if aid is not None and aid not in online_agents:
            continue
        concurrent = row["type"] in CONCURRENT_OPS
        if not concurrent:
            if aid in busy_agents or aid in seen_agents:
                continue
        # Serialised ops: skip if one is already running or already picked
        if row["type"] in SERIALISE_GROUP:
            if serialised_running or serialised_seen:
                continue
            serialised_seen = True
        if not concurrent:
            seen_agents.add(aid)
        result.append(dict(row))
    return result


async def update_params(op_id: int, params: dict):
    """Persist updated params for a running operation (e.g. traversal state)."""
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET params = ? WHERE id = ?",
            (json.dumps(params), op_id),
        )


async def set_running(op_id: int):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'running', started_at = ? WHERE id = ?",
            (utc_now("auto"), op_id),
        )


async def set_completed(op_id: int):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'completed', completed_at = ? "
            "WHERE id = ?",
            (utc_now("auto"), op_id),
        )


async def set_failed(op_id: int, error: str):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'failed', completed_at = ?, error = ? "
            "WHERE id = ?",
            (utc_now("auto"), error, op_id),
        )


async def set_status_pending(op_id: int):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'pending', started_at = NULL WHERE id = ?",
            (op_id,),
        )


async def set_status(op_id: int, status: str):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = ?, completed_at = ? WHERE id = ?",
            (status, utc_now("auto"), op_id),
        )


async def execute(op_type: str, op_id: int, agent_id: int | None, params: dict):
    """Dispatch to the handler for this operation type."""
    handler = HANDLERS.get(op_type)
    if handler is None:
        raise ValueError(f"Unknown operation type: {op_type}")
    await handler(op_id, agent_id, params)


async def handle_scan_dir(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.scan import run_scan

    await run_scan(op_id, agent_id, params)


async def handle_backfill_location(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.hash_backfill import run_backfill

    location_id = params["location_id"]
    location_name = params.get("location_name", "")
    scan_prefix = params.get("scan_prefix")
    await run_backfill(agent_id, location_id, location_name, scan_prefix)


async def handle_rehash_partial(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.rehash_partial import run_rehash_partial

    await run_rehash_partial(op_id, agent_id, params)


async def handle_hash_file(op_id: int, agent_id: int | None, params: dict):
    await run_hash_file(op_id, agent_id, params)


async def handle_batch_delete(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.batch import batch_delete

    await batch_delete(
        params.get("file_ids", []),
        params.get("folder_ids", []),
        params.get("all_duplicates", False),
    )


async def handle_merge(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.merge import run_merge

    await run_merge(
        params["source_id"],
        params["source_info"],
        params["destination_id"],
        params["dest_info"],
        mode=params.get("mode", "move"),
    )


async def handle_batch_consolidate(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.consolidate import run_batch_consolidation

    await run_batch_consolidation(
        params["file_ids"],
        params["mode"],
        params.get("dest_folder_id"),
        filename_match_only=params.get("filename_match_only", False),
        consolidate_mode=params.get("consolidate_mode", "move"),
        stub_file_ids=params.get("stub_file_ids"),
    )


async def handle_batch_rehash(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.routes.files import run_batch_rehash

    await run_batch_rehash(params["file_ids"])


async def handle_batch_tag(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.batch import batch_tag

    await batch_tag(
        params.get("file_ids", []),
        params.get("add_tags", []),
        params.get("remove_tags", []),
    )


async def handle_reset_stale(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.delete import reset_stale

    await reset_stale(
        folder_id=params.get("folder_id"),
        location_id=params.get("location_id"),
        label=params.get("label", ""),
    )


async def handle_transcode(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.conversions import transcode

    await transcode.run(op_id, agent_id, params)


async def handle_similarity_scan(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.similarity import run_similarity_scan

    await run_similarity_scan(op_id, agent_id, params)


async def handle_embed_file(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.similarity import run_embed_file

    await run_embed_file(op_id, agent_id, params)


async def handle_extract_markdown(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.similarity import run_extract_markdown

    await run_extract_markdown(op_id, agent_id, params)


async def handle_raw_convert(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.conversions import raw_convert

    await raw_convert.run(op_id, agent_id, params)


HANDLERS = {
    "scan_dir": handle_scan_dir,
    "backfill_location": handle_backfill_location,
    "rehash_partial": handle_rehash_partial,
    "hash_file": handle_hash_file,
    "batch_delete": handle_batch_delete,
    "merge": handle_merge,
    "batch_consolidate": handle_batch_consolidate,
    "batch_rehash": handle_batch_rehash,
    "batch_tag": handle_batch_tag,
    "reset_stale": handle_reset_stale,
    "transcode": handle_transcode,
    "similarity_scan": handle_similarity_scan,
    "embed_file": handle_embed_file,
    "extract_markdown": handle_extract_markdown,
    "raw_convert": handle_raw_convert,
}


async def get_queue_status_for_broadcast() -> dict:
    """Build the queue state dict in the format the frontend expects."""
    status = await get_queue_status()

    # Split running ops by type so frontend can show correct badges.
    # Transcode is a file-level operation with its own progress UI —
    # it must not affect location tree badges.
    NO_TREE_BADGE = {"transcode", "raw_convert"}

    scanning_ids = []
    backfilling_ids = []
    all_running_ids = []
    for item in status:
        if item.get("status") != "running":
            continue
        loc_id = item.get("location_id")
        if not loc_id:
            continue
        op_type = item.get("type", "")
        if op_type in NO_TREE_BADGE:
            continue
        all_running_ids.append(loc_id)
        if op_type == "scan_dir":
            scanning_ids.append(loc_id)
        elif op_type in ("backfill_location", "hash_file"):
            backfilling_ids.append(loc_id)

    pending = [
        {
            "queue_id": item["id"],
            "location_id": item.get("location_id"),
            "name": item.get("location_name", ""),
            "type": item.get("type", ""),
            "queued_at": item.get("created_at", ""),
        }
        for item in status
        if item.get("status") == "pending" and item.get("type", "") not in NO_TREE_BADGE
    ]
    return {
        "running_location_ids": all_running_ids,
        "running_location_id": all_running_ids[0] if all_running_ids else None,
        "scanning_location_ids": scanning_ids,
        "backfilling_location_ids": backfilling_ids,
        "pending": pending,
    }


async def broadcast_queue_state():
    """Build and broadcast the current queue state to all connected browsers."""
    queue = await get_queue_status_for_broadcast()
    await broadcast({"type": "scan_queue_updated", "queue": queue})

