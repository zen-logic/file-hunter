"""Post-scan hash backfill for agent locations.

After an agent scan completes, most files lack hash_strong because the agent
only computes SHA-256 when duplicate sizes exist within its 50-file batches.
This service identifies files needing full hashes (via size + hash_partial
matches across locations) and requests them from the agent via HTTP, then
backfills matching local files too.
"""

import asyncio
import logging
from datetime import datetime, timezone

from file_hunter.db import db_writer, read_db, open_connection, execute_write
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# agent_id -> cancel flag (True = cancel requested)
_active_backfills: dict[int, bool] = {}
# agent_id -> (location_id, location_name, scan_prefix) for running backfills
_backfill_info: dict[int, tuple[int, str, str | None]] = {}
# agent_id -> queue of (location_id, location_name, scan_prefix) awaiting backfill
_pending_backfills: dict[int, list[tuple[int, str, str | None]]] = {}


def cancel_backfill(agent_id: int):
    """Request cancellation of a running backfill for this agent."""
    if agent_id in _active_backfills:
        _active_backfills[agent_id] = True


def cancel_backfill_by_location(location_id: int) -> bool:
    """Request cancellation of a running backfill by location_id. Returns True if found."""
    for aid, (loc_id, _, _sp) in _backfill_info.items():
        if loc_id == location_id:
            cancel_backfill(aid)
            return True
    return False


def get_active_backfill_info(agent_id: int) -> tuple[int, str, str | None] | None:
    """Return (location_id, location_name, scan_prefix) if a backfill is running for this agent."""
    return _backfill_info.get(agent_id)


async def queue_pending_backfill(
    agent_id: int,
    location_id: int,
    location_name: str,
    scan_prefix: str | None = None,
    *,
    front: bool = False,
):
    """Add a backfill to the per-agent queue. Deduplicates by location_id.

    Use front=True to re-queue an interrupted backfill at the head of the queue.
    """
    queue = _pending_backfills.setdefault(agent_id, [])
    if any(lid == location_id for lid, _, _sp in queue):
        return
    if front:
        queue.insert(0, (location_id, location_name, scan_prefix))
    else:
        queue.append((location_id, location_name, scan_prefix))
    await persist_backfill(agent_id, location_id, location_name, scan_prefix)


async def pop_pending_backfill(agent_id: int) -> tuple[int, str, str | None] | None:
    """Pop and return the next queued backfill for this agent, or None."""
    queue = _pending_backfills.get(agent_id, [])
    if not queue:
        return None
    item = queue.pop(0)
    await clear_persisted_backfill(agent_id, item[0])
    if not queue:
        _pending_backfills.pop(agent_id, None)
    return item


async def persist_backfill(
    agent_id: int,
    location_id: int,
    location_name: str,
    scan_prefix: str | None = None,
):
    """Persist a pending backfill to the database."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    async def _write(conn, aid, lid, lname, sp, ts):
        await conn.execute(
            "INSERT OR REPLACE INTO pending_backfills "
            "(agent_id, location_id, location_name, scan_prefix, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (aid, lid, lname, sp, ts),
        )
        await conn.commit()

    await execute_write(_write, agent_id, location_id, location_name, scan_prefix, now)


async def clear_persisted_backfill(agent_id: int, location_id: int):
    """Remove a persisted backfill from the database."""

    async def _delete(conn, aid, lid):
        await conn.execute(
            "DELETE FROM pending_backfills WHERE agent_id = ? AND location_id = ?",
            (aid, lid),
        )
        await conn.commit()

    await execute_write(_delete, agent_id, location_id)


async def load_persisted_backfills() -> list[tuple[int, int, str, str | None]]:
    """Load all persisted backfills from the database."""
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT agent_id, location_id, location_name, scan_prefix FROM pending_backfills"
        )
    return [
        (
            r["agent_id"],
            r["location_id"],
            r["location_name"],
            dict(r).get("scan_prefix"),
        )
        for r in rows
    ]


async def restore_backfills():
    """Reload pending backfills from DB into memory on startup."""
    rows = await load_persisted_backfills()
    for agent_id, location_id, location_name, scan_prefix in rows:
        queue = _pending_backfills.setdefault(agent_id, [])
        queue.append((location_id, location_name, scan_prefix))
    if rows:
        logger.info("Restored %d pending backfill(s) from previous session", len(rows))


async def run_backfill(
    agent_id: int,
    location_id: int,
    location_name: str,
    scan_prefix: str | None = None,
):
    """Find files missing hash_strong that have cross-location size+partial
    matches, request full hashes from the agent, then backfill local matches.

    When scan_prefix is set, only backfills files within that subtree.
    """
    # One backfill at a time per agent — queue if already running
    if agent_id in _active_backfills:
        await queue_pending_backfill(agent_id, location_id, location_name, scan_prefix)
        logger.info(
            "Backfill queued for %s (location %d) — agent #%d already running backfill",
            location_name,
            location_id,
            agent_id,
        )
        return

    from file_hunter.services.agent_ops import dispatch

    _active_backfills[agent_id] = False
    _backfill_info[agent_id] = (location_id, location_name, scan_prefix)
    await persist_backfill(agent_id, location_id, location_name, scan_prefix)

    try:
        # Broadcast immediately so UI shows backfill state before slow query
        await broadcast(
            {
                "type": "backfill_started",
                "locationId": location_id,
                "location": location_name,
                "totalFiles": 0,
            }
        )

        from file_hunter.services.dup_counts import find_dup_candidates

        all_candidates = await find_dup_candidates(location_id=location_id)

        # Filter to this location + optional prefix
        candidates = [
            c
            for c in all_candidates
            if c["location_id"] == location_id
            and (not scan_prefix or c["full_path"].startswith(scan_prefix + "/"))
        ]

        if not candidates:
            logger.info(
                "Backfill: no candidates for %s (location %d)",
                location_name,
                location_id,
            )
            await broadcast(
                {
                    "type": "backfill_completed",
                    "locationId": location_id,
                    "location": location_name,
                    "agentFilesHashed": 0,
                    "agentErrors": 0,
                    "cancelled": False,
                }
            )
            return

        total = len(candidates)
        logger.info(
            "Backfill: %d candidates for %s (location %d)",
            total,
            location_name,
            location_id,
        )

        # Update with actual candidate count
        await broadcast(
            {
                "type": "backfill_progress",
                "locationId": location_id,
                "location": location_name,
                "filesHashed": 0,
                "totalFiles": total,
            }
        )

        sem = asyncio.Semaphore(3)
        agent_hashed = 0
        agent_errors = 0
        pending_writes: list[tuple] = []
        affected_hashes: set[str] = set()
        batch_size = 1

        async def _hash_one(file_id: int, full_path: str):
            nonlocal agent_hashed, agent_errors
            if _active_backfills.get(agent_id):
                return
            async with sem:
                if _active_backfills.get(agent_id):
                    return
                try:
                    result = await dispatch("file_hash", location_id, path=full_path)
                    pending_writes.append((file_id, result["hash_fast"]))
                    affected_hashes.add(result["hash_fast"])
                    agent_hashed += 1
                except Exception as e:
                    agent_errors += 1
                    logger.warning("Backfill: hash failed for %s: %r", full_path, e)

        for i, row in enumerate(candidates):
            # Checkpoint: block here while queue is paused (e.g. during import)
            from file_hunter.services.queue_manager import wait_if_paused

            await wait_if_paused()

            if _active_backfills.get(agent_id):
                break

            await _hash_one(row["id"], row["full_path"])

            if len(pending_writes) >= batch_size:
                await _flush_writes(pending_writes)
                pending_writes.clear()
                await broadcast(
                    {
                        "type": "backfill_progress",
                        "locationId": location_id,
                        "location": location_name,
                        "filesHashed": agent_hashed,
                        "totalFiles": total,
                    }
                )

        if pending_writes:
            await _flush_writes(pending_writes)
            pending_writes.clear()

        invalidate_stats_cache()

        cancelled = _active_backfills.get(agent_id, False)

        # Mark complete and notify UI immediately — don't block on
        # cross-agent hashing or dup count recalculation
        if not cancelled:
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE locations SET backfill_needed = 0 WHERE id = ?",
                    (location_id,),
                )

        await broadcast(
            {
                "type": "backfill_completed",
                "locationId": location_id,
                "location": location_name,
                "agentFilesHashed": agent_hashed,
                "agentErrors": agent_errors,
                "cancelled": cancelled,
            }
        )

        logger.info(
            "Backfill %s for %s: %d agent hashed, %d errors",
            "cancelled" if cancelled else "completed",
            location_name,
            agent_hashed,
            agent_errors,
        )

        # Cross-agent backfill: hash files on other connected agents
        if not cancelled:
            await _backfill_agents(
                agent_id, location_id, location_name, affected_hashes
            )

        if affected_hashes:
            from file_hunter.services.dup_counts import submit_hashes_for_recalc

            submit_hashes_for_recalc(
                fast_hashes=affected_hashes,
                source=f"backfill {location_name}",
                location_ids={location_id},
            )

    except Exception as e:
        logger.error("Backfill error for %s: %s", location_name, e)
        await broadcast(
            {
                "type": "backfill_completed",
                "locationId": location_id,
                "location": location_name,
                "agentFilesHashed": 0,
                "agentErrors": 0,
                "cancelled": True,
                "error": str(e),
            }
        )
    finally:
        _active_backfills.pop(agent_id, None)
        _backfill_info.pop(agent_id, None)
        await clear_persisted_backfill(agent_id, location_id)

        # Chain to next queued backfill for this agent
        next_item = await pop_pending_backfill(agent_id)
        if next_item:
            logger.info(
                "Chaining backfill to %s (location %d) for agent #%d",
                next_item[1],
                next_item[0],
                agent_id,
            )
            asyncio.create_task(
                run_backfill(agent_id, next_item[0], next_item[1], next_item[2])
            )


async def _flush_writes(writes: list[tuple[int, str]]):
    """Batch-update hash_fast for a list of file IDs."""
    async with db_writer() as wdb:
        for file_id, hash_fast in writes:
            await wdb.execute(
                "UPDATE files SET hash_fast = ? WHERE id = ?",
                (hash_fast, file_id),
            )


async def _backfill_agents(
    agent_id: int,
    agent_location_id: int,
    location_name: str,
    affected_hashes: set[str],
) -> int:
    """Hash files on OTHER agent locations that match by (size, partial)."""
    from file_hunter.services.agent_ops import dispatch
    from file_hunter.services.online_check import agent_online_check

    logger.info(
        "Cross-agent backfill: querying candidates for location %d", agent_location_id
    )

    db = await open_connection()
    try:
        rows = await db.execute_fetchall(
            """SELECT f.id, f.full_path, f.location_id
               FROM files f
               WHERE f.hash_fast IS NULL
                 AND f.hash_partial IS NOT NULL
                 AND f.file_size > 0
                 AND f.stale = 0
                 AND f.location_id != ?
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.location_id = ?
                       AND f2.file_size = f.file_size
                       AND f2.hash_partial = f.hash_partial
                       AND f2.hash_fast IS NOT NULL
                 )
                 AND f.location_id IN (
                     SELECT id FROM locations WHERE agent_id IS NOT NULL
                 )""",
            (agent_location_id, agent_location_id),
        )
    finally:
        await db.close()

    if not rows:
        logger.info("Cross-agent backfill: no candidates found")
        return 0

    logger.info("Cross-agent backfill: %d candidates across other locations", len(rows))

    candidate_loc_ids = {row["location_id"] for row in rows}
    online_loc_ids = set()
    for loc_id in candidate_loc_ids:
        status = agent_online_check({"id": loc_id})
        if status is True:
            online_loc_ids.add(loc_id)
        elif status is False:
            logger.info("Cross-agent backfill: skipping offline location %d", loc_id)

    hashed = 0
    errors = 0
    pending: list[tuple] = []
    total = sum(1 for r in rows if r["location_id"] in online_loc_ids)

    if total == 0:
        logger.info("Cross-agent backfill: no online candidates")
        return 0

    for row in rows:
        # Checkpoint: block here while queue is paused (e.g. during import)
        from file_hunter.services.queue_manager import wait_if_paused

        await wait_if_paused()

        if row["location_id"] not in online_loc_ids:
            continue
        if _active_backfills.get(agent_id):
            break
        try:
            result = await dispatch(
                "file_hash", row["location_id"], path=row["full_path"]
            )
            pending.append((row["id"], result["hash_fast"]))
            affected_hashes.add(result["hash_fast"])
            hashed += 1
        except Exception as e:
            errors += 1
            logger.warning(
                "Cross-agent backfill: hash failed for %s: %r", row["full_path"], e
            )

        if len(pending) >= 20:
            await _flush_writes(pending)
            pending.clear()

        if (hashed + errors) % 10 == 0:
            await broadcast(
                {
                    "type": "backfill_progress",
                    "locationId": agent_location_id,
                    "location": location_name,
                    "filesHashed": hashed,
                    "totalFiles": total,
                    "phase": "cross_location",
                }
            )

    if pending:
        await _flush_writes(pending)

    logger.info("Cross-agent backfill: complete, %d files hashed", hashed)
    return hashed
