"""Persistent counter recalculation for folders and locations.

All four stored counters (file_count, total_size, duplicate_count,
type_counts) are recalculated after mutations (scan, delete, move,
upload, merge, consolidate).  This eliminates expensive recursive CTEs
and SUM/GROUP BY aggregates on every tree/stats request.
"""

import asyncio
import json
import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def schedule_size_recalc(*location_ids: int):
    """Fire-and-forget counter recalculation via db_writer().

    Safe to call from route handlers — runs in a background task so the
    HTTP response is not blocked by O(files+folders) DB work.
    """
    ids = [lid for lid in location_ids if lid is not None]
    if ids:
        asyncio.create_task(_bg_recalc_sizes(ids))


async def _bg_recalc_sizes(location_ids: list[int]):
    from file_hunter.ws.scan import broadcast

    try:
        for lid in location_ids:
            await recalculate_location_sizes(lid)
        await broadcast({"type": "size_recalc_completed", "locationIds": location_ids})
    except Exception:
        log.error("Background size recalc failed", exc_info=True)


def _merge_type_counts(a: dict, b: dict) -> dict:
    """Merge two type_counts dicts by summing values."""
    merged = dict(a)
    for k, v in b.items():
        merged[k] = merged.get(k, 0) + v
    return merged


async def recalculate_location_sizes(location_id: int):
    """Recompute all four stored counters for every folder in a location,
    then update the location totals.

    Counters: file_count, total_size, duplicate_count, type_counts (JSON).

    Strategy:
    1. Direct values per folder via indexed GROUP BY on files (read via get_db()).
    2. Build folder tree in memory from (id, parent_id).
    3. Bottom-up accumulation: leaf folders get direct values, parents sum children.
    4. Batch UPDATE folders, then UPDATE the location row (write via db_writer()).
    """
    from file_hunter.db import db_writer, get_db

    db = await get_db()

    # 1a. Direct file sizes and counts per folder
    direct_rows = await db.execute_fetchall(
        "SELECT folder_id, SUM(file_size) AS total, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND stale = 0 GROUP BY folder_id",
        (location_id,),
    )
    direct_size: dict[int, int] = {}
    direct_count: dict[int, int] = {}
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

    # 1b. Direct duplicate counts per folder
    dup_rows = await db.execute_fetchall(
        "SELECT folder_id, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND stale = 0 "
        "AND dup_exclude = 0 AND dup_count > 0 "
        "GROUP BY folder_id",
        (location_id,),
    )
    direct_dup: dict[int, int] = {}
    root_dup_count = 0
    for r in dup_rows:
        fid = r["folder_id"]
        if fid is None:
            root_dup_count = r["cnt"] or 0
        else:
            direct_dup[fid] = r["cnt"] or 0

    # 1c. Direct type counts per folder
    type_rows = await db.execute_fetchall(
        "SELECT folder_id, file_type_high, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND stale = 0 "
        "GROUP BY folder_id, file_type_high",
        (location_id,),
    )
    direct_types: dict[int | None, dict[str, int]] = defaultdict(dict)
    for r in type_rows:
        fid = r["folder_id"]
        ftype = r["file_type_high"] or ""
        direct_types[fid][ftype] = r["cnt"] or 0
    root_type_counts = dict(direct_types.get(None, {}))

    # 2. Build folder tree in memory
    folder_rows = await db.execute_fetchall(
        "SELECT id, parent_id FROM folders WHERE location_id = ?",
        (location_id,),
    )
    children_of: dict[int | None, list[int]] = {}
    all_folder_ids: list[int] = []
    for f in folder_rows:
        fid = f["id"]
        pid = f["parent_id"]
        all_folder_ids.append(fid)
        if pid not in children_of:
            children_of[pid] = []
        children_of[pid].append(fid)

    # 3. Bottom-up accumulation
    cum_size: dict[int, int] = {}
    cum_count: dict[int, int] = {}
    cum_dup: dict[int, int] = {}
    cum_types: dict[int, dict[str, int]] = {}

    def accumulate(fid):
        size = direct_size.get(fid, 0)
        count = direct_count.get(fid, 0)
        dup = direct_dup.get(fid, 0)
        types = dict(direct_types.get(fid, {}))
        for child_id in children_of.get(fid, []):
            accumulate(child_id)
            size += cum_size[child_id]
            count += cum_count[child_id]
            dup += cum_dup[child_id]
            types = _merge_type_counts(types, cum_types[child_id])
        cum_size[fid] = size
        cum_count[fid] = count
        cum_dup[fid] = dup
        cum_types[fid] = types

    for root_id in children_of.get(None, []):
        accumulate(root_id)

    # 4+5. Write phase: batch UPDATE folders + location totals
    loc_total_size = root_file_size + sum(
        direct_size.get(fid, 0) for fid in all_folder_ids
    )
    loc_total_count = root_file_count + sum(
        direct_count.get(fid, 0) for fid in all_folder_ids
    )
    loc_dup_count = root_dup_count + sum(
        direct_dup.get(fid, 0) for fid in all_folder_ids
    )
    loc_type_counts = dict(root_type_counts)
    for fid in all_folder_ids:
        for ftype, cnt in direct_types.get(fid, {}).items():
            loc_type_counts[ftype] = loc_type_counts.get(ftype, 0) + cnt

    async with db_writer() as wdb:
        await wdb.executemany(
            "UPDATE folders SET total_size = ?, file_count = ?, "
            "duplicate_count = ?, type_counts = ? WHERE id = ?",
            [
                (
                    cum_size.get(fid, 0),
                    cum_count.get(fid, 0),
                    cum_dup.get(fid, 0),
                    json.dumps(cum_types.get(fid, {})),
                    fid,
                )
                for fid in all_folder_ids
            ],
        )

        await wdb.execute(
            "UPDATE locations SET total_size = ?, file_count = ?, "
            "duplicate_count = ?, type_counts = ? WHERE id = ?",
            (
                loc_total_size,
                loc_total_count,
                loc_dup_count,
                json.dumps(loc_type_counts),
                location_id,
            ),
        )


async def populate_all_sizes_if_needed():
    """One-time migration: populate sizes for locations where total_size IS NULL."""
    import time

    from file_hunter.db import get_db

    db = await get_db()
    null_locs = await db.execute_fetchall(
        "SELECT id, name FROM locations WHERE total_size IS NULL"
    )
    if not null_locs:
        return

    print(f"Calculating folder sizes for {len(null_locs)} locations...")
    t0 = time.monotonic()
    for loc in null_locs:
        t1 = time.monotonic()
        await recalculate_location_sizes(loc["id"])
        print(f"  {loc['name']} — {time.monotonic() - t1:.1f}s")
    print(f"Folder sizes populated in {time.monotonic() - t0:.1f}s.")
