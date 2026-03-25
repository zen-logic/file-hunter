"""Queued location deletion — agent-first, then catalog cleanup.

Runs as a queue_manager operation so it survives server/agent restarts.
The agent must confirm deletion from its config before the server
removes anything from the catalog.
"""

import asyncio
import logging

from file_hunter.helpers import post_op_stats
from file_hunter.services.agent_ops import delete_agent_location, invalidate_loc_cache

logger = logging.getLogger("file_hunter")


async def run_delete_location(op_id: int, agent_id: int | None, params: dict):
    """Execute a location delete operation.

    1. Tell agent to remove location from its config
    2. Agent confirms (or agent is None for local locations)
    3. Collect affected dup hashes before deletion
    4. Clean up catalog: files, folders, scans, queue entries, backfills
    5. Recount dup_count for affected hashes
    6. Broadcast completion
    """
    location_id = params["location_id"]
    location_name = params.get("location_name", "")
    root_path = params.get("root_path", "")

    # Step 1: Tell agent to delete (if agent-backed)
    if agent_id is not None:
        await delete_agent_location(agent_id, root_path, location_id=location_id)
        logger.info(
            "Agent #%d confirmed delete of '%s' (location %d)",
            agent_id,
            location_name,
            location_id,
        )

    # Step 2: Collect affected hashes before deletion
    affected_fast, affected_strong = await _collect_affected_hashes(location_id)
    logger.info(
        "Delete location #%d: %d affected hash_fast, %d affected hash_strong",
        location_id,
        len(affected_fast),
        len(affected_strong),
    )

    # Step 3: Clean up catalog — batched to avoid holding writer lock
    await _purge_location_batched(location_id)

    # Step 4: Submit affected hashes + invalidate stats
    await post_op_stats(
        strong_hashes=affected_strong or None,
        fast_hashes=affected_fast or None,
        source=f"delete location {location_name}",
    )

    # Step 5: Clean up in-memory state
    invalidate_loc_cache(location_id)

    logger.info(
        "Location deleted: #%d '%s'",
        location_id,
        location_name,
    )


async def _collect_affected_hashes(location_id: int) -> tuple[set[str], set[str]]:
    """Collect hash_fast and hash_strong values from files with dups.

    Only collects hashes where dup_count > 0 — unique files don't affect
    anything else when deleted.
    """
    affected_fast: set[str] = set()
    affected_strong: set[str] = set()

    from file_hunter.hashes_db import read_hashes

    async with read_hashes() as hdb:
        fast_rows = await hdb.execute_fetchall(
            "SELECT DISTINCT hash_fast FROM active_hashes "
            "WHERE location_id = ? AND dup_count > 0 "
            "AND hash_fast IS NOT NULL AND hash_fast != ''",
            (location_id,),
        )
        for r in fast_rows:
            affected_fast.add(r["hash_fast"])

        strong_rows = await hdb.execute_fetchall(
            "SELECT DISTINCT hash_strong FROM active_hashes "
            "WHERE location_id = ? AND dup_count > 0 "
            "AND hash_strong IS NOT NULL AND hash_strong != ''",
            (location_id,),
        )
        for r in strong_rows:
            affected_strong.add(r["hash_strong"])

    return affected_fast, affected_strong


async def _purge_location_batched(location_id: int):
    """Remove all traces of a location, batched to avoid holding writer lock."""
    from file_hunter.db import db_writer
    from file_hunter.hashes_db import remove_location_hashes
    from file_hunter.stats_db import remove_location_stats

    # Remove all hashes and stats for this location
    await remove_location_hashes(location_id)
    await remove_location_stats(location_id)

    DELETE_BATCH = 5000

    # Batch-delete files first (largest table)
    while True:
        async with db_writer() as db:
            cursor = await db.execute(
                "DELETE FROM files WHERE rowid IN "
                "(SELECT rowid FROM files WHERE location_id = ? LIMIT ?)",
                (location_id, DELETE_BATCH),
            )
            deleted = cursor.rowcount
        if deleted < DELETE_BATCH:
            break
        await asyncio.sleep(0)

    # Batch-delete folders
    while True:
        async with db_writer() as db:
            cursor = await db.execute(
                "DELETE FROM folders WHERE rowid IN "
                "(SELECT rowid FROM folders WHERE location_id = ? LIMIT ?)",
                (location_id, DELETE_BATCH),
            )
            deleted = cursor.rowcount
        if deleted < DELETE_BATCH:
            break
        await asyncio.sleep(0)

    # Small tables — single deletes, each releases writer
    # Only delete OTHER ops for this location (scans, backfills) — not the
    # currently running delete_location op. The queue manager owns its lifecycle.
    async with db_writer() as db:
        await db.execute(
            "DELETE FROM operation_queue WHERE params LIKE ? AND type != 'delete_location'",
            (f'%"location_id": {location_id}%',),
        )

    # Final cleanup — single writer call so location row can't survive if
    # server crashes between steps
    async with db_writer() as db:
        await db.execute(
            "DELETE FROM pending_backfills WHERE location_id = ?", (location_id,)
        )
        await db.execute(
            "DELETE FROM pending_hashes WHERE location_id = ?", (location_id,)
        )
        await db.execute("DELETE FROM locations WHERE id = ?", (location_id,))
