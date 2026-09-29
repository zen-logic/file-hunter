"""Persistent counter recalculation for folders and locations.

All four stored counters (file_count, total_size, duplicate_count,
type_counts) are recalculated after mutations (scan, delete, move,
upload, merge, consolidate).  This eliminates expensive recursive CTEs
and SUM/GROUP BY aggregates on every tree/stats request.
"""

import asyncio
import logging
import time
from collections import Counter

from file_hunter.db import open_connection, read_db
from file_hunter.hashes_db import open_hashes_connection
from file_hunter.services.activity import register, unregister
from file_hunter.services.stats import invalidate_stats_cache, patch_cache_dup_counts
from file_hunter.stats_rollup import (
    DIRECT_SQL,
    DUP_BATCH,
    DUP_FILES_SQL,
    FOLDER_STATS_UPSERT,
    FOLDERS_SQL,
    HIDDEN_SQL,
    LOCATION_STATS_UPSERT,
    TYPES_SQL,
    dup_folders_sql,
    location_counters,
    location_dup_counts,
)
from file_hunter.stats_db import read_stats, stats_writer
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)


def schedule_size_recalc(*location_ids: int):
    """Fire-and-forget counter recalculation via db_writer().

    Safe to call from route handlers — runs in a background task so the
    HTTP response is not blocked by O(files+folders) DB work.
    """
    ids = [lid for lid in location_ids if lid is not None]
    if ids:
        asyncio.create_task(bg_recalc_sizes(ids))


async def bg_recalc_sizes(location_ids: list[int]):
    activity_name = f"size-recalc-{id(location_ids)}"
    register(activity_name, "Size recalc")
    try:
        await broadcast({"type": "size_recalc_started", "locationIds": location_ids})
        for lid in location_ids:
            await recalculate_location_sizes(lid)
        # Clear stats cache so the next API request reads the fresh values
        # from stats.db — without this, the cache refresh that ran during
        # the recalc has already filled the cache with stale data.
        invalidate_stats_cache()
        await broadcast({"type": "size_recalc_completed", "locationIds": location_ids})
    except Exception:
        log.error("Background size recalc failed", exc_info=True)
    finally:
        unregister(activity_name)


async def dup_folder_counts(db, location_id: int) -> Counter:
    """Duplicate files per folder (None for the location root): file ids
    from hashes.db, mapped to their folders through the catalog."""
    hconn = await open_hashes_connection()
    try:
        dup_file_rows = await hconn.execute_fetchall(DUP_FILES_SQL, (location_id,))
    finally:
        await hconn.close()

    counts = Counter()
    dup_ids = [r["file_id"] for r in dup_file_rows]
    for i in range(0, len(dup_ids), DUP_BATCH):
        batch = dup_ids[i : i + DUP_BATCH]
        rows = await db.execute_fetchall(dup_folders_sql(len(batch)), batch)
        counts.update(r["folder_id"] for r in rows)
    return counts


async def recalculate_location_sizes(location_id: int):
    """Recompute every stored counter (file_count, total_size,
    duplicate_count, hidden_count, type_counts) for every folder in a
    location and for the location itself, and write them to stats.db."""
    db = await open_connection()
    try:
        direct_rows = await db.execute_fetchall(DIRECT_SQL, (location_id,))
        dup_counts = await dup_folder_counts(db, location_id)
        hidden_rows = await db.execute_fetchall(HIDDEN_SQL, (location_id,))
        type_rows = await db.execute_fetchall(TYPES_SQL, (location_id,))
        folder_rows = await db.execute_fetchall(FOLDERS_SQL, (location_id,))
    finally:
        await db.close()

    folder_params, location_params, cum_dup, loc_dup = location_counters(
        location_id, direct_rows, dup_counts, hidden_rows, type_rows, folder_rows
    )

    async with stats_writer() as sdb:
        await sdb.executemany(FOLDER_STATS_UPSERT, folder_params)
        await sdb.execute(LOCATION_STATS_UPSERT, location_params)

    # Patch stats cache with updated dup counts — no full cache clear needed
    patch_cache_dup_counts(location_id, loc_dup, cum_dup)


async def recalculate_folder_dup_counts(location_id: int):
    """Recompute only duplicate_count for every folder in a location and the
    location itself. Used after a dup recalc, without a full rebuild."""
    db = await open_connection()
    try:
        dup_counts = await dup_folder_counts(db, location_id)
        folder_rows = await db.execute_fetchall(FOLDERS_SQL, (location_id,))
    finally:
        await db.close()

    cum_dup, loc_dup, all_folder_ids = location_dup_counts(dup_counts, folder_rows)

    async with stats_writer() as sdb:
        for i in range(0, len(all_folder_ids), 500):
            batch = all_folder_ids[i : i + 500]
            await sdb.executemany(
                "UPDATE folder_stats SET duplicate_count = ? WHERE folder_id = ?",
                [(cum_dup.get(fid, 0), fid) for fid in batch],
            )
        await sdb.execute(
            "UPDATE location_stats SET duplicate_count = ? WHERE location_id = ?",
            (loc_dup, location_id),
        )

    patch_cache_dup_counts(location_id, loc_dup, cum_dup)


async def populate_all_sizes_if_needed():
    """Populate stats for locations missing from stats.db."""
    async with read_db() as db:
        all_locs = await db.execute_fetchall(
            "SELECT id, name FROM locations WHERE name NOT LIKE '__deleting_%'"
        )
    if not all_locs:
        return

    # Check which locations have no entry in stats.db
    async with read_stats() as sdb:
        existing = await sdb.execute_fetchall("SELECT location_id FROM location_stats")
    existing_ids = {r["location_id"] for r in existing}
    null_locs = [loc for loc in all_locs if loc["id"] not in existing_ids]

    if not null_locs:
        return

    print(f"Calculating folder sizes for {len(null_locs)} locations...")
    t0 = time.monotonic()
    for loc in null_locs:
        t1 = time.monotonic()
        await recalculate_location_sizes(loc["id"])
        print(f"  {loc['name']} — {time.monotonic() - t1:.1f}s")
    print(f"Folder sizes populated in {time.monotonic() - t0:.1f}s.")
