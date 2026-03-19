"""Queued location deletion — agent-first, then catalog cleanup.

Runs as a queue_manager operation so it survives server/agent restarts.
The agent must confirm deletion from its config before the server
removes anything from the catalog.
"""

import asyncio
import logging

from file_hunter.db import db_writer, execute_write, read_db
from file_hunter.services.agent_ops import delete_agent_location, invalidate_loc_cache
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

RECOUNT_BATCH = 500


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

    # Step 2: Collect hashes that will need recounting after deletion
    affected_fast, affected_strong = await _collect_affected_hashes(location_id)
    logger.info(
        "Delete location #%d: %d affected hash_fast, %d affected hash_strong",
        location_id, len(affected_fast), len(affected_strong),
    )

    # Step 3: Clean up catalog
    await execute_write(_purge_location, location_id)

    # Step 4: Recount dup_count for affected hashes
    if affected_fast or affected_strong:
        await _recount_affected_hashes(affected_fast, affected_strong)
        logger.info(
            "Delete location #%d: dup recount complete", location_id
        )

    # Step 5: Recalculate sizes for locations that had affected dups
    if affected_fast or affected_strong:
        from file_hunter.services.sizes import recalculate_location_sizes
        affected_loc_ids = await _find_affected_locations(
            affected_fast, affected_strong
        )
        for lid in affected_loc_ids:
            await recalculate_location_sizes(lid)

    # Step 6: Clean up in-memory state
    invalidate_loc_cache(location_id)
    invalidate_stats_cache()

    # Step 7: Broadcast completion
    await broadcast(
        {
            "type": "location_deleted",
            "locationId": f"loc-{location_id}",
            "name": location_name,
        }
    )

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

    async with read_db() as rdb:
        fast_rows = await rdb.execute_fetchall(
            "SELECT DISTINCT hash_fast FROM files "
            "WHERE location_id = ? AND dup_count > 0 "
            "AND hash_fast IS NOT NULL AND hash_fast != ''",
            (location_id,),
        )
        for r in fast_rows:
            affected_fast.add(r["hash_fast"])

        strong_rows = await rdb.execute_fetchall(
            "SELECT DISTINCT hash_strong FROM files "
            "WHERE location_id = ? AND dup_count > 0 "
            "AND hash_strong IS NOT NULL AND hash_strong != ''",
            (location_id,),
        )
        for r in strong_rows:
            affected_strong.add(r["hash_strong"])

    return affected_fast, affected_strong


async def _recount_affected_hashes(
    affected_fast: set[str], affected_strong: set[str]
):
    """Recount dup_count for files sharing the given hash values.

    After the location's files are deleted, GROUP BY the remaining files
    to get new counts, then UPDATE in batches.
    """
    configs = [
        ("hash_strong", affected_strong, ""),
        ("hash_fast", affected_fast, " AND hash_strong IS NULL"),
    ]

    for hash_column, hash_set, update_extra in configs:
        if not hash_set:
            continue

        hash_list = list(hash_set)

        # GROUP BY in batches to get new counts
        from collections import defaultdict
        by_dc: dict[int, list[str]] = defaultdict(list)

        for i in range(0, len(hash_list), RECOUNT_BATCH):
            batch = hash_list[i : i + RECOUNT_BATCH]
            ph = ",".join("?" for _ in batch)
            async with read_db() as rdb:
                rows = await rdb.execute_fetchall(
                    f"SELECT {hash_column} as hash_val, COUNT(*) as cnt "
                    f"FROM files "
                    f"WHERE {hash_column} IN ({ph}) "
                    f"AND stale = 0 AND dup_exclude = 0 "
                    f"GROUP BY {hash_column}",
                    batch,
                )
            counted_hashes = set()
            for r in rows:
                cnt = r["cnt"]
                dc = cnt - 1 if cnt > 1 else 0
                by_dc[dc].append(r["hash_val"])
                counted_hashes.add(r["hash_val"])

            # Hashes with no remaining files — zero out
            for h in batch:
                if h not in counted_hashes:
                    by_dc[0].append(h)

        # Write updated dup_counts
        for dc, dc_hashes in by_dc.items():
            for i in range(0, len(dc_hashes), RECOUNT_BATCH):
                batch = dc_hashes[i : i + RECOUNT_BATCH]
                ph = ",".join("?" for _ in batch)
                async with db_writer() as wdb:
                    await wdb.execute(
                        f"UPDATE files SET dup_count = ? "
                        f"WHERE {hash_column} IN ({ph}) "
                        f"AND stale = 0 AND dup_exclude = 0"
                        f"{update_extra}",
                        [dc] + batch,
                    )
                await asyncio.sleep(0)

        logger.info(
            "Recount %s: %d hashes recounted",
            hash_column, len(hash_set),
        )


async def _find_affected_locations(
    affected_fast: set[str], affected_strong: set[str]
) -> set[int]:
    """Find location IDs that have files with any of the affected hashes."""
    loc_ids: set[int] = set()

    for hash_column, hash_set in [
        ("hash_fast", affected_fast),
        ("hash_strong", affected_strong),
    ]:
        if not hash_set:
            continue
        hash_list = list(hash_set)
        for i in range(0, len(hash_list), RECOUNT_BATCH):
            batch = hash_list[i : i + RECOUNT_BATCH]
            ph = ",".join("?" for _ in batch)
            async with read_db() as rdb:
                rows = await rdb.execute_fetchall(
                    f"SELECT DISTINCT location_id FROM files "
                    f"WHERE {hash_column} IN ({ph}) AND stale = 0",
                    batch,
                )
            for r in rows:
                loc_ids.add(r["location_id"])

    return loc_ids


async def _purge_location(db, location_id: int):
    """Remove all traces of a location from the database."""
    # Operation queue entries (params is JSON, no FK cascade)
    await db.execute(
        "DELETE FROM operation_queue WHERE params LIKE ?",
        (f'%"location_id": {location_id}%',),
    )

    # Pending backfills (no FK cascade)
    await db.execute(
        "DELETE FROM pending_backfills WHERE location_id = ?", (location_id,)
    )

    # Pending hashes (no FK cascade)
    await db.execute(
        "DELETE FROM pending_hashes WHERE location_id = ?", (location_id,)
    )

    # Location row (CASCADE handles files, folders, scans, consolidation_jobs, ignored_files)
    await db.execute("DELETE FROM locations WHERE id = ?", (location_id,))
    await db.commit()
