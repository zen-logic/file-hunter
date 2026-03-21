"""Background housekeeping queue — invisible maintenance tasks.

Separate from the operation queue (which is for user-visible operations).
Housekeeping runs ONLY when the system is idle — no queue operations
running, no import running. Checks idle state between every batch.

Tasks survive restarts via a persistent DB table. 500-row batches keep
writer hold time under 100ms — UI stays responsive if a user operation
starts mid-batch.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from file_hunter.db import db_writer, read_db
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

_running = False
_task: asyncio.Task | None = None

_SCHEMA = """
CREATE TABLE IF NOT EXISTS housekeeping_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    agent_id INTEGER,
    params TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    error TEXT
);
"""

PURGE_BATCH = 500


async def init_schema():
    """Create the housekeeping table if it doesn't exist."""
    async with db_writer() as db:
        await db.executescript(_SCHEMA)


async def enqueue(task_type: str, agent_id: int | None, params: dict):
    """Insert a housekeeping task."""
    async with db_writer() as db:
        await db.execute(
            "INSERT INTO housekeeping_queue (type, agent_id, params, created_at) "
            "VALUES (?, ?, ?, ?)",
            (task_type, agent_id, json.dumps(params), _now()),
        )


def start():
    """Start the housekeeping background loop."""
    global _running, _task
    _running = True
    _task = asyncio.create_task(_run())


async def stop():
    """Stop the housekeeping loop gracefully."""
    global _running, _task
    _running = False
    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
    _task = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_idle() -> bool:
    """Check if the system is idle — no user operations running."""
    from file_hunter.services.queue_manager import _running_ops, _paused

    # Queue is paused = import has exclusive access
    if _paused:
        return False

    # Any queue operations running
    if _running_ops:
        return False

    return True


async def _has_pending_primary_ops() -> bool:
    """Check if primary queue has pending operations."""
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM operation_queue WHERE status = 'pending'"
        )
    return rows[0]["c"] > 0


async def _recover_interrupted():
    """On startup, reset any 'running' housekeeping tasks back to 'pending'."""
    async with db_writer() as db:
        cursor = await db.execute(
            "UPDATE housekeeping_queue SET status = 'pending', started_at = NULL "
            "WHERE status = 'running'"
        )
        if cursor.rowcount > 0:
            logger.info(
                "Housekeeping: recovered %d interrupted task(s)", cursor.rowcount
            )

    # Safety net: detect orphaned __deleting_ locations without a housekeeping entry
    async with read_db() as db:
        orphans = await db.execute_fetchall(
            "SELECT id, name, agent_id FROM locations WHERE name LIKE '__deleting_%'"
        )
        if not orphans:
            return

        existing = await db.execute_fetchall(
            "SELECT params FROM housekeeping_queue "
            "WHERE type = 'purge_location' AND status IN ('pending', 'running')"
        )
    existing_ids = set()
    for row in existing:
        p = json.loads(row["params"] or "{}")
        lid = p.get("location_id")
        if lid is not None:
            existing_ids.add(lid)

    for orphan in orphans:
        if orphan["id"] not in existing_ids:
            logger.info(
                "Housekeeping: re-queuing orphaned __deleting_ location %d",
                orphan["id"],
            )
            await enqueue(
                "purge_location",
                orphan["agent_id"],
                {"location_id": orphan["id"], "location_name": orphan["name"]},
            )



async def _run():
    """Main loop — poll for housekeeping tasks, run when idle."""
    await _recover_interrupted()

    while _running:
        try:
            # Only run when system is idle
            if not _is_idle():
                await asyncio.sleep(2)
                continue

            # Check for pending primary ops too
            if await _has_pending_primary_ops():
                await asyncio.sleep(2)
                continue

            # Fetch next pending housekeeping task
            async with read_db() as db:
                rows = await db.execute_fetchall(
                    "SELECT id, type, agent_id, params FROM housekeeping_queue "
                    "WHERE status = 'pending' ORDER BY id LIMIT 1"
                )

            if not rows:
                await asyncio.sleep(5)
                continue

            task = rows[0]
            task_id = task["id"]
            task_type = task["type"]
            params = json.loads(task["params"])

            # Mark as running
            async with db_writer() as db:
                await db.execute(
                    "UPDATE housekeeping_queue SET status = 'running', started_at = ? "
                    "WHERE id = ?",
                    (_now(), task_id),
                )

            await broadcast({"type": "activity", "message": "Housekeeping..."})
            logger.info("Housekeeping: starting %s (id=%d)", task_type, task_id)

            try:
                await _execute(task_type, task_id, task["agent_id"], params)

                async with db_writer() as db:
                    await db.execute(
                        "UPDATE housekeeping_queue SET status = 'completed', "
                        "completed_at = ? WHERE id = ?",
                        (_now(), task_id),
                    )
                logger.info("Housekeeping: completed %s (id=%d)", task_type, task_id)

            except Exception as e:
                logger.exception("Housekeeping: failed %s (id=%d)", task_type, task_id)
                async with db_writer() as db:
                    await db.execute(
                        "UPDATE housekeeping_queue SET status = 'failed', "
                        "completed_at = ?, error = ? WHERE id = ?",
                        (_now(), str(e), task_id),
                    )

            # Check if queue is now empty
            async with read_db() as db:
                remaining = await db.execute_fetchall(
                    "SELECT COUNT(*) as c FROM housekeeping_queue "
                    "WHERE status = 'pending'"
                )
            if remaining[0]["c"] == 0:
                await broadcast({"type": "activity", "message": "Housekeeping done."})

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Housekeeping: unexpected error in main loop")
            await asyncio.sleep(5)


async def _execute(task_type: str, task_id: int, agent_id: int | None, params: dict):
    """Dispatch a housekeeping task to its handler."""
    if task_type == "purge_location":
        await _run_purge_location(task_id, agent_id, params)
    else:
        raise ValueError(f"Unknown housekeeping task type: {task_type}")


async def _run_purge_location(task_id: int, agent_id: int | None, params: dict):
    """Purge a deleted location's data from all three databases.

    Batched at PURGE_BATCH rows, with idle check between batches.
    """
    location_id = params["location_id"]
    location_name = params.get("location_name", f"location {location_id}")

    # Collect affected hashes before deletion (for dup recount after)
    from file_hunter.services.location_delete import _collect_affected_hashes

    affected_fast, affected_strong = await _collect_affected_hashes(location_id)
    logger.info(
        "Housekeeping purge #%d: %d affected hash_fast, %d affected hash_strong",
        location_id,
        len(affected_fast),
        len(affected_strong),
    )

    # Remove from hashes.db and stats.db (fast, indexed by location_id)
    from file_hunter.hashes_db import remove_location_hashes
    from file_hunter.stats_db import remove_location_stats

    await remove_location_hashes(location_id)
    await remove_location_stats(location_id)

    # Batch-delete files — check idle between batches
    while True:
        if not _is_idle():
            await asyncio.sleep(2)
            continue

        async with db_writer() as db:
            cursor = await db.execute(
                "DELETE FROM files WHERE rowid IN "
                "(SELECT rowid FROM files WHERE location_id = ? LIMIT ?)",
                (location_id, PURGE_BATCH),
            )
            deleted = cursor.rowcount
        if deleted < PURGE_BATCH:
            break
        await asyncio.sleep(0)

    # Batch-delete folders
    while True:
        if not _is_idle():
            await asyncio.sleep(2)
            continue

        async with db_writer() as db:
            cursor = await db.execute(
                "DELETE FROM folders WHERE rowid IN "
                "(SELECT rowid FROM folders WHERE location_id = ? LIMIT ?)",
                (location_id, PURGE_BATCH),
            )
            deleted = cursor.rowcount
        if deleted < PURGE_BATCH:
            break
        await asyncio.sleep(0)

    # Small tables — single deletes
    async with db_writer() as db:
        await db.execute(
            "DELETE FROM operation_queue WHERE params LIKE ? AND type != 'delete_location'",
            (f'%"location_id": {location_id}%',),
        )

    async with db_writer() as db:
        await db.execute(
            "DELETE FROM pending_backfills WHERE location_id = ?", (location_id,)
        )
        await db.execute(
            "DELETE FROM pending_hashes WHERE location_id = ?", (location_id,)
        )
        await db.execute("DELETE FROM locations WHERE id = ?", (location_id,))

    # Submit affected hashes for dup recount
    if affected_fast or affected_strong:
        from file_hunter.services.dup_counts import submit_hashes_for_recalc

        submit_hashes_for_recalc(
            strong_hashes=affected_strong or None,
            fast_hashes=affected_fast or None,
            source=f"delete location {location_name}",
        )

    from file_hunter.services.agent_ops import invalidate_loc_cache
    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_loc_cache(location_id)
    invalidate_stats_cache()

    await broadcast(
        {
            "type": "activity",
            "message": f"{location_name} deletion completed",
        }
    )


