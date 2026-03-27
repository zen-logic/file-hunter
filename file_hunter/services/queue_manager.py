"""Operation queue manager — per-agent parallel processor.

Reads pending operations from the operation_queue table and executes them.
Operations on different agents run concurrently; operations on the same agent
are serialized. All state is in the DB — survives restarts.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from file_hunter.db import db_writer, read_db
from file_hunter.services.activity import op_label, register, unregister
from file_hunter.services.dup_counts import run_hash_file
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

_running = False
_task: asyncio.Task | None = None
_paused = False

# Event that running operations await between iterations.
# Set = running normally. Cleared = suspended (pause active).
_pause_event: asyncio.Event = asyncio.Event()
_pause_event.set()  # start in running state

# Running operations: op_id -> (agent_id, asyncio.Task)
_running_ops: dict[int, tuple[int | None, asyncio.Task]] = {}


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

    Side effects:
        Writes to operation_queue table via db_writer. Broadcasts
        scan_queue_updated to all connected browsers.

    Notes:
        Called from HTTP endpoints (scan, backfill) and internal services.
        The queue manager main loop picks up pending ops automatically.
    """
    async with db_writer() as db:
        cursor = await db.execute(
            "INSERT INTO operation_queue (type, agent_id, params, created_at) "
            "VALUES (?, ?, ?, ?)",
            (op_type, agent_id, json.dumps(params), _now()),
        )
        op_id = cursor.lastrowid
    await _broadcast_queue_state()
    return op_id


async def cancel(op_id: int) -> bool:
    """Cancel a pending or running operation.

    Pending: sets status to cancelled in the DB.
    Running: cancels the executing task (triggers CancelledError in the handler).
    Returns True if the operation was found and cancelled.
    """
    if op_id in _running_ops:
        _, task = _running_ops[op_id]
        task.cancel()
        # broadcast happens when _reap_finished picks up the CancelledError
        return True

    async with db_writer() as db:
        cursor = await db.execute(
            "UPDATE operation_queue SET status = 'cancelled', completed_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (_now(), op_id),
        )
        changed = cursor.rowcount
    if changed > 0:
        await _broadcast_queue_state()
        return True
    return False


async def cancel_by_location(location_id: int) -> bool:
    """Cancel a running or pending operation for a location. Returns True if found."""
    async with read_db() as db:
        # Check running ops first
        for op_id, (_, task) in list(_running_ops.items()):
            row = await db.execute_fetchall(
                "SELECT params FROM operation_queue WHERE id = ?", (op_id,)
            )
            if row:
                params = json.loads(row[0]["params"] or "{}")
                if params.get("location_id") == location_id:
                    task.cancel()
                    # broadcast happens when _reap_finished picks up the CancelledError
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
                    (_now(), row["id"]),
                )
            await _broadcast_queue_state()
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
    for op_id, (_, task) in _running_ops.items():
        if task.done():
            continue
        # We don't have params in memory, but we can check the DB
        # synchronously is not possible. Use a cached approach instead.
        pass
    # Fall back to checking _running_locations cache
    return location_id in _running_locations


# Cache of location_ids with running operations (updated by _set_running)
_running_locations: set[int] = set()


def _track_location(op_id: int, location_id: int | None):
    """Track that a location has a running operation."""
    if location_id is not None:
        _running_locations.add(location_id)


def _untrack_location(location_id: int | None):
    """Remove location from running set."""
    if location_id is not None:
        _running_locations.discard(location_id)


def start():
    """Start the queue manager background loop."""
    global _running, _task
    if _running:
        return
    _running = True
    _task = asyncio.create_task(_run())
    logger.info("Queue manager started")


async def stop():
    """Stop the queue manager and wait for running operations to finish."""
    global _running, _task
    _running = False

    # Wait for running operations to complete gracefully
    running_tasks = [task for _, task in _running_ops.values()]
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

    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
    _task = None

    # Clean up activity registrations for any ops that were still running
    for op_id in list(_running_ops):
        unregister(f"op-{op_id}")
    _running_ops.clear()

    logger.info("Queue manager stopped")


async def pause():
    """Suspend all running operations and prevent new ones from dispatching.

    Clears _pause_event so running ops block at their next checkpoint.
    Waits briefly for ops to reach a checkpoint, then returns.
    """
    global _paused
    if _paused:
        return
    _paused = True
    _pause_event.clear()
    logger.info("Queue manager: suspending %d running op(s)", len(_running_ops))
    # Give running ops time to hit their checkpoint and block
    await asyncio.sleep(0.5)
    logger.info("Queue manager: paused")


def resume():
    """Resume the queue manager — unblock suspended operations."""
    global _paused
    if not _paused:
        return
    _paused = False
    _pause_event.set()
    logger.info("Queue manager: resumed, operations unblocked")


async def wait_if_paused():
    """Checkpoint for long-running operations. Blocks while queue is paused."""
    await _pause_event.wait()


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


async def _recover_interrupted():
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


async def _run():
    """Main loop — poll for pending operations and start them per-agent."""
    await _recover_interrupted()

    while _running:
        try:
            await _reap_finished()

            # When paused, don't dispatch new ops
            if _paused:
                await asyncio.sleep(1)
                continue

            busy_agents = {aid for aid, _ in _running_ops.values()}

            ops = await _next_pending_ops(busy_agents)
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
                await _set_running(op_id)
                _track_location(op_id, loc_id)
                logger.info("Queue manager: starting %s (id=%d)", op_type, op_id)

                register(f"op-{op_id}", op_label(op_type, loc_name))

                task = asyncio.create_task(_execute(op_type, op_id, agent_id, params))
                _running_ops[op_id] = (agent_id, task)

            # Broadcast updated queue state (pending→running transitions)
            await _broadcast_queue_state()

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Queue manager: unexpected error in main loop")
            await asyncio.sleep(5)

    # Running ops are handled by stop() — don't cancel here


async def _reap_finished():
    """Check running tasks for completion and update their status."""
    reaped = False
    for op_id in list(_running_ops):
        agent_id, task = _running_ops[op_id]
        if not task.done():
            continue

        reaped = True
        del _running_ops[op_id]

        unregister(f"op-{op_id}")

        # Untrack location
        async with read_db() as db:
            row = await db.execute_fetchall(
                "SELECT params FROM operation_queue WHERE id = ?", (op_id,)
            )
        if row:
            loc_id = json.loads(row[0]["params"] or "{}").get("location_id")
            _untrack_location(loc_id)

        try:
            task.result()
            await _set_completed(op_id)
            logger.info("Queue manager: completed (id=%d)", op_id)
        except asyncio.CancelledError:
            await _set_status(op_id, "cancelled")
            logger.info("Queue manager: cancelled (id=%d)", op_id)
        except (ConnectionError, OSError, httpx.ConnectError) as e:
            logger.warning(
                "Queue manager: agent unavailable (id=%d): %s — re-queuing",
                op_id,
                e,
            )
            await _set_status_pending(op_id)
        except Exception as e:
            logger.exception("Queue manager: failed (id=%d)", op_id)
            await _set_failed(op_id, str(e))

    if reaped:
        await _broadcast_queue_state()


async def _next_pending_ops(busy_agents: set) -> list[dict]:
    """Fetch pending operations for agents that are online and not busy."""
    from file_hunter.ws.agent import get_online_agent_ids

    online_agents = set(get_online_agent_ids())

    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, type, status, agent_id, params "
            "FROM operation_queue WHERE status = 'pending' "
            "ORDER BY id"
        )
    result = []
    seen_agents: set[int | None] = set()
    for row in rows:
        aid = row["agent_id"]
        if aid is not None and aid not in online_agents:
            continue
        if aid in busy_agents or aid in seen_agents:
            continue
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


async def _set_running(op_id: int):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'running', started_at = ? WHERE id = ?",
            (_now(), op_id),
        )


async def _set_completed(op_id: int):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'completed', completed_at = ? "
            "WHERE id = ?",
            (_now(), op_id),
        )


async def _set_failed(op_id: int, error: str):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'failed', completed_at = ?, error = ? "
            "WHERE id = ?",
            (_now(), error, op_id),
        )


async def _set_status_pending(op_id: int):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = 'pending', started_at = NULL WHERE id = ?",
            (op_id,),
        )


async def _set_status(op_id: int, status: str):
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET status = ?, completed_at = ? WHERE id = ?",
            (status, _now(), op_id),
        )


async def _execute(op_type: str, op_id: int, agent_id: int | None, params: dict):
    """Dispatch to the handler for this operation type."""
    handler = _HANDLERS.get(op_type)
    if handler is None:
        raise ValueError(f"Unknown operation type: {op_type}")
    await handler(op_id, agent_id, params)


async def _handle_scan_dir(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.scan import run_scan

    await run_scan(op_id, agent_id, params)


async def _handle_backfill_location(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.hash_backfill import run_backfill

    location_id = params["location_id"]
    location_name = params.get("location_name", "")
    scan_prefix = params.get("scan_prefix")
    await run_backfill(agent_id, location_id, location_name, scan_prefix)


async def _handle_rehash_partial(op_id: int, agent_id: int | None, params: dict):
    from file_hunter.services.rehash_partial import run_rehash_partial

    await run_rehash_partial(op_id, agent_id, params)


async def _handle_hash_file(op_id: int, agent_id: int | None, params: dict):
    await run_hash_file(op_id, agent_id, params)


_HANDLERS = {
    "scan_dir": _handle_scan_dir,
    "backfill_location": _handle_backfill_location,
    "rehash_partial": _handle_rehash_partial,
    "hash_file": _handle_hash_file,
}


async def get_queue_status_for_broadcast() -> dict:
    """Build the queue state dict in the format the frontend expects."""
    status = await get_queue_status()

    # Split running ops by type so frontend can show correct badges
    scanning_ids = []
    backfilling_ids = []
    all_running_ids = []
    for item in status:
        if item.get("status") != "running":
            continue
        loc_id = item.get("location_id")
        if not loc_id:
            continue
        all_running_ids.append(loc_id)
        op_type = item.get("type", "")
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
        if item.get("status") == "pending"
    ]
    return {
        "running_location_ids": all_running_ids,
        "running_location_id": all_running_ids[0] if all_running_ids else None,
        "scanning_location_ids": scanning_ids,
        "backfilling_location_ids": backfilling_ids,
        "pending": pending,
    }


async def _broadcast_queue_state():
    """Build and broadcast the current queue state to all connected browsers."""
    queue = await get_queue_status_for_broadcast()
    await broadcast({"type": "scan_queue_updated", "queue": queue})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
