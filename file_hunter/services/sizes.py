"""Persistent size recalculation for folders and locations.

Sizes are stored in the database and recalculated after mutations (scan,
delete, move, upload, merge, consolidate).  This eliminates expensive
recursive CTEs and SUM aggregates on every tree/stats request.
"""

import asyncio
import logging

log = logging.getLogger(__name__)


def schedule_size_recalc(*location_ids: int):
    """Fire-and-forget size recalculation on its own connection.

    Safe to call from route handlers — runs in a background task so the
    HTTP response is not blocked by O(files+folders) DB work.
    """
    ids = [lid for lid in location_ids if lid is not None]
    if ids:
        asyncio.create_task(_bg_recalc_sizes(ids))


async def _bg_recalc_sizes(location_ids: list[int]):
    from file_hunter.db import open_connection
    from file_hunter.ws.scan import broadcast

    db = await open_connection()
    try:
        for lid in location_ids:
            await recalculate_location_sizes(db, lid)
        await broadcast({"type": "size_recalc_completed", "locationIds": location_ids})
    except Exception:
        log.error("Background size recalc failed", exc_info=True)
    finally:
        await db.close()


async def recalculate_location_sizes(db, location_id: int):
    """Recompute total_size and file_count for all folders in a location,
    then update the location totals.

    Strategy:
    1. Direct sizes per folder via indexed GROUP BY on files.
    2. Build folder tree in memory from (id, parent_id).
    3. Bottom-up accumulation: leaf folders get direct sizes, parents sum children.
    4. Batch UPDATE folders, then UPDATE the location row.
    """
    # 1. Direct file sizes and counts per folder
    direct_rows = await db.execute_fetchall(
        "SELECT folder_id, SUM(file_size) AS total, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND stale = 0 GROUP BY folder_id",
        (location_id,),
    )
    direct_size = {}
    direct_count = {}
    # folder_id=None means files at the location root (no folder)
    root_file_size = 0
    root_file_count = 0
    for r in direct_rows:
        fid = r["folder_id"]
        if fid is None:
            root_file_size = r["total"] or 0
            root_file_count = r["cnt"] or 0
        else:
            direct_size[fid] = r["total"] or 0
            direct_count[fid] = r["cnt"] or 0

    # 2. Build folder tree in memory
    folder_rows = await db.execute_fetchall(
        "SELECT id, parent_id FROM folders WHERE location_id = ?",
        (location_id,),
    )
    children_of = {}  # parent_id -> [child_id, ...]
    all_folder_ids = []
    for f in folder_rows:
        fid = f["id"]
        pid = f["parent_id"]
        all_folder_ids.append(fid)
        if pid not in children_of:
            children_of[pid] = []
        children_of[pid].append(fid)

    # 3. Bottom-up accumulation
    cum_size = {}
    cum_count = {}

    def accumulate(fid):
        size = direct_size.get(fid, 0)
        count = direct_count.get(fid, 0)
        for child_id in children_of.get(fid, []):
            accumulate(child_id)
            size += cum_size[child_id]
            count += cum_count[child_id]
        cum_size[fid] = size
        cum_count[fid] = count

    # Start from roots (parent_id IS NULL)
    for root_id in children_of.get(None, []):
        accumulate(root_id)

    # 4. Batch UPDATE folders
    await db.executemany(
        "UPDATE folders SET total_size = ?, file_count = ? WHERE id = ?",
        [(cum_size.get(fid, 0), cum_count.get(fid, 0), fid) for fid in all_folder_ids],
    )

    # 5. UPDATE location totals (sum of all files, including root-level)
    loc_total_size = root_file_size + sum(
        direct_size.get(fid, 0) for fid in all_folder_ids
    )
    loc_total_count = root_file_count + sum(
        direct_count.get(fid, 0) for fid in all_folder_ids
    )
    await db.execute(
        "UPDATE locations SET total_size = ?, file_count = ? WHERE id = ?",
        (loc_total_size, loc_total_count, location_id),
    )
    await db.commit()


async def populate_all_sizes_if_needed(db):
    """One-time migration: populate sizes for locations where total_size IS NULL."""
    import time

    null_locs = await db.execute_fetchall(
        "SELECT id, name FROM locations WHERE total_size IS NULL"
    )
    if not null_locs:
        return

    print(f"Calculating folder sizes for {len(null_locs)} locations...")
    t0 = time.monotonic()
    for loc in null_locs:
        t1 = time.monotonic()
        await recalculate_location_sizes(db, loc["id"])
        print(f"  {loc['name']} — {time.monotonic() - t1:.1f}s")
    print(f"Folder sizes populated in {time.monotonic() - t0:.1f}s.")
