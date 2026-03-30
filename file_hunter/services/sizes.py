"""Persistent counter recalculation for folders and locations.

All four stored counters (file_count, total_size, duplicate_count,
type_counts) are recalculated after mutations (scan, delete, move,
upload, merge, consolidate).  This eliminates expensive recursive CTEs
and SUM/GROUP BY aggregates on every tree/stats request.
"""

import asyncio
import json
import logging
import time
from collections import Counter, defaultdict

from file_hunter.db import read_db
from file_hunter.hashes_db import open_hashes_connection
from file_hunter.services.activity import register, unregister
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
        asyncio.create_task(_bg_recalc_sizes(ids))


async def _bg_recalc_sizes(location_ids: list[int]):
    activity_name = f"size-recalc-{id(location_ids)}"
    register(activity_name, "Size recalc")
    try:
        for lid in location_ids:
            await recalculate_location_sizes(lid)
        await broadcast({"type": "size_recalc_completed", "locationIds": location_ids})
    except Exception:
        log.error("Background size recalc failed", exc_info=True)
    finally:
        unregister(activity_name)


def _merge_type_counts(a: dict, b: dict) -> dict:
    """Merge two type_counts dicts by summing values."""
    merged = dict(a)
    for k, v in b.items():
        merged[k] = merged.get(k, 0) + v
    return merged


async def recalculate_location_sizes(location_id: int):
    """Recompute all stored counters for every folder in a location,
    then update the location totals.

    Counters: file_count, total_size, duplicate_count, hidden_count, type_counts (JSON).

    Strategy:
    1. Direct values per folder via indexed GROUP BY on files (read via read_db()).
    2. Build folder tree in memory from (id, parent_id).
    3. Bottom-up accumulation: leaf folders get direct values, parents sum children.
    4. Batch UPDATE folders, then UPDATE the location row (write via db_writer()).
    """
    async with read_db() as db:
        # 1a. Direct file sizes and counts per folder
        direct_rows = await db.execute_fetchall(
            "SELECT folder_id, SUM(file_size) AS total, COUNT(*) AS cnt "
            "FROM files WHERE location_id = ? AND stale = 0 GROUP BY folder_id",
            (location_id,),
        )

        # 1b. Direct duplicate counts per folder — dup_count from hashes.db
        hconn = await open_hashes_connection()
        try:
            dup_file_rows = await hconn.execute_fetchall(
                "SELECT file_id FROM active_hashes "
                "WHERE location_id = ? AND dup_count > 0",
                (location_id,),
            )
        finally:
            await hconn.close()

        dup_rows = []
        if dup_file_rows:
            dup_ids = [r["file_id"] for r in dup_file_rows]
            # Map file_ids to folder_ids from catalog
            for i in range(0, len(dup_ids), 500):
                batch = dup_ids[i : i + 500]
                ph = ",".join("?" for _ in batch)
                folder_rows_batch = await db.execute_fetchall(
                    f"SELECT folder_id FROM files WHERE id IN ({ph})",
                    batch,
                )
                dup_rows.extend(folder_rows_batch)

        # Group by folder_id to get counts
        _dup_folder_counts = Counter(r["folder_id"] for r in dup_rows)
        dup_rows = [
            {"folder_id": fid, "cnt": cnt} for fid, cnt in _dup_folder_counts.items()
        ]

        # 1c. Direct hidden counts per folder
        hidden_rows = await db.execute_fetchall(
            "SELECT folder_id, COUNT(*) AS cnt "
            "FROM files WHERE location_id = ? AND stale = 0 AND hidden = 1 "
            "GROUP BY folder_id",
            (location_id,),
        )

        # 1d. Direct type counts per folder
        type_rows = await db.execute_fetchall(
            "SELECT folder_id, file_type_high, COUNT(*) AS cnt "
            "FROM files WHERE location_id = ? AND stale = 0 "
            "GROUP BY folder_id, file_type_high",
            (location_id,),
        )

        # 2. Build folder tree in memory
        folder_rows = await db.execute_fetchall(
            "SELECT id, parent_id FROM folders WHERE location_id = ?",
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

    direct_dup: dict[int, int] = {}
    root_dup_count = 0
    for r in dup_rows:
        fid = r["folder_id"]
        if fid is None:
            root_dup_count = r["cnt"] or 0
        else:
            direct_dup[fid] = r["cnt"] or 0

    direct_hidden: dict[int, int] = {}
    root_hidden_count = 0
    for r in hidden_rows:
        fid = r["folder_id"]
        if fid is None:
            root_hidden_count = r["cnt"] or 0
        else:
            direct_hidden[fid] = r["cnt"] or 0

    direct_types: dict[int | None, dict[str, int]] = defaultdict(dict)
    for r in type_rows:
        fid = r["folder_id"]
        ftype = r["file_type_high"] or ""
        direct_types[fid][ftype] = r["cnt"] or 0
    root_type_counts = dict(direct_types.get(None, {}))
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
    cum_hidden: dict[int, int] = {}
    cum_types: dict[int, dict[str, int]] = {}

    def accumulate(fid):
        size = direct_size.get(fid, 0)
        count = direct_count.get(fid, 0)
        dup = direct_dup.get(fid, 0)
        hidden = direct_hidden.get(fid, 0)
        types = dict(direct_types.get(fid, {}))
        for child_id in children_of.get(fid, []):
            accumulate(child_id)
            size += cum_size[child_id]
            count += cum_count[child_id]
            dup += cum_dup[child_id]
            hidden += cum_hidden[child_id]
            types = _merge_type_counts(types, cum_types[child_id])
        cum_size[fid] = size
        cum_count[fid] = count
        cum_dup[fid] = dup
        cum_hidden[fid] = hidden
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
    loc_hidden_count = root_hidden_count + sum(
        direct_hidden.get(fid, 0) for fid in all_folder_ids
    )
    loc_type_counts = dict(root_type_counts)
    for fid in all_folder_ids:
        for ftype, cnt in direct_types.get(fid, {}).items():
            loc_type_counts[ftype] = loc_type_counts.get(ftype, 0) + cnt

    # Write to stats.db — own writer, no catalog contention
    async with stats_writer() as sdb:
        await sdb.executemany(
            "INSERT INTO folder_stats "
            "(folder_id, location_id, file_count, total_size, "
            "duplicate_count, hidden_count, type_counts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(folder_id) DO UPDATE SET "
            "file_count=excluded.file_count, total_size=excluded.total_size, "
            "duplicate_count=excluded.duplicate_count, hidden_count=excluded.hidden_count, "
            "type_counts=excluded.type_counts",
            [
                (
                    fid,
                    location_id,
                    cum_count.get(fid, 0),
                    cum_size.get(fid, 0),
                    cum_dup.get(fid, 0),
                    cum_hidden.get(fid, 0),
                    json.dumps(cum_types.get(fid, {})),
                )
                for fid in all_folder_ids
            ],
        )

        await sdb.execute(
            "INSERT INTO location_stats "
            "(location_id, file_count, total_size, "
            "duplicate_count, hidden_count, type_counts) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(location_id) DO UPDATE SET "
            "file_count=excluded.file_count, total_size=excluded.total_size, "
            "duplicate_count=excluded.duplicate_count, hidden_count=excluded.hidden_count, "
            "type_counts=excluded.type_counts",
            (
                location_id,
                loc_total_count,
                loc_total_size,
                loc_dup_count,
                loc_hidden_count,
                json.dumps(loc_type_counts),
            ),
        )

    # Patch stats cache with updated dup counts — no full cache clear needed
    from file_hunter.services.stats import patch_cache_dup_counts

    patch_cache_dup_counts(location_id, loc_dup_count, cum_dup)


async def recalculate_folder_dup_counts(location_id: int):
    """Recompute only the duplicate_count rollup for every folder in a location.

    Same tree-walk strategy as recalculate_location_sizes but only touches
    duplicate_count — skips sizes, file counts, hidden counts, and type counts.
    Used after dup recalc to update affected locations without a full rebuild.
    """
    async with read_db() as db:
        hconn = await open_hashes_connection()
        try:
            dup_file_rows = await hconn.execute_fetchall(
                "SELECT file_id FROM active_hashes "
                "WHERE location_id = ? AND dup_count > 0",
                (location_id,),
            )
        finally:
            await hconn.close()

        dup_rows = []
        if dup_file_rows:
            dup_ids = [r["file_id"] for r in dup_file_rows]
            for i in range(0, len(dup_ids), 500):
                batch = dup_ids[i : i + 500]
                ph = ",".join("?" for _ in batch)
                folder_rows_batch = await db.execute_fetchall(
                    f"SELECT folder_id FROM files WHERE id IN ({ph})",
                    batch,
                )
                dup_rows.extend(folder_rows_batch)

        _dup_folder_counts = Counter(r["folder_id"] for r in dup_rows)

        folder_rows = await db.execute_fetchall(
            "SELECT id, parent_id FROM folders WHERE location_id = ?",
            (location_id,),
        )

    direct_dup: dict[int, int] = {}
    root_dup_count = 0
    for fid, cnt in _dup_folder_counts.items():
        if fid is None:
            root_dup_count = cnt
        else:
            direct_dup[fid] = cnt

    children_of: dict[int | None, list[int]] = {}
    all_folder_ids: list[int] = []
    for f in folder_rows:
        fid = f["id"]
        pid = f["parent_id"]
        all_folder_ids.append(fid)
        if pid not in children_of:
            children_of[pid] = []
        children_of[pid].append(fid)

    cum_dup: dict[int, int] = {}

    def accumulate(fid):
        dup = direct_dup.get(fid, 0)
        for child_id in children_of.get(fid, []):
            accumulate(child_id)
            dup += cum_dup[child_id]
        cum_dup[fid] = dup

    for root_id in children_of.get(None, []):
        accumulate(root_id)

    loc_dup_count = root_dup_count + sum(
        direct_dup.get(fid, 0) for fid in all_folder_ids
    )

    async with stats_writer() as sdb:
        for i in range(0, len(all_folder_ids), 500):
            batch = all_folder_ids[i : i + 500]
            await sdb.executemany(
                "UPDATE folder_stats SET duplicate_count = ? WHERE folder_id = ?",
                [(cum_dup.get(fid, 0), fid) for fid in batch],
            )
        await sdb.execute(
            "UPDATE location_stats SET duplicate_count = ? WHERE location_id = ?",
            (loc_dup_count, location_id),
        )

    from file_hunter.services.stats import patch_cache_dup_counts

    patch_cache_dup_counts(location_id, loc_dup_count, cum_dup)


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
