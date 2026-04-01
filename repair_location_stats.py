#!/usr/bin/env python3
"""Recalculate location_stats and folder_stats for specific locations.

Usage:
    python repair_location_stats.py --locations 14,80 [--data PATH]
"""

import argparse
import json
import sqlite3
import time
from collections import Counter, defaultdict


def recalculate(data_path: str, location_ids: list[int]):
    catalog = sqlite3.connect(f"{data_path}/file_hunter.db")
    catalog.row_factory = sqlite3.Row
    hashes = sqlite3.connect(f"{data_path}/hashes.db")
    hashes.row_factory = sqlite3.Row
    stats = sqlite3.connect(f"{data_path}/stats.db")
    stats.execute("PRAGMA journal_mode=WAL")

    for loc_id in location_ids:
        t0 = time.monotonic()
        print(f"\nLocation {loc_id}:")

        # 1a. Direct file sizes and counts per folder
        direct_rows = catalog.execute(
            "SELECT folder_id, SUM(file_size) AS total, COUNT(*) AS cnt "
            "FROM files WHERE location_id = ? AND stale = 0 GROUP BY folder_id",
            (loc_id,),
        ).fetchall()

        # 1b. Duplicate counts per folder
        dup_file_rows = hashes.execute(
            "SELECT file_id FROM active_hashes "
            "WHERE location_id = ? AND dup_count > 0",
            (loc_id,),
        ).fetchall()

        dup_folder_counts = Counter()
        if dup_file_rows:
            dup_ids = [r["file_id"] for r in dup_file_rows]
            for i in range(0, len(dup_ids), 500):
                batch = dup_ids[i : i + 500]
                ph = ",".join("?" for _ in batch)
                rows = catalog.execute(
                    f"SELECT folder_id FROM files WHERE id IN ({ph})", batch
                ).fetchall()
                dup_folder_counts.update(r["folder_id"] for r in rows)

        # 1c. Hidden counts per folder
        hidden_rows = catalog.execute(
            "SELECT folder_id, COUNT(*) AS cnt "
            "FROM files WHERE location_id = ? AND stale = 0 AND hidden = 1 "
            "GROUP BY folder_id",
            (loc_id,),
        ).fetchall()

        # 1d. Type counts per folder
        type_rows = catalog.execute(
            "SELECT folder_id, file_type_high, COUNT(*) AS cnt "
            "FROM files WHERE location_id = ? AND stale = 0 "
            "GROUP BY folder_id, file_type_high",
            (loc_id,),
        ).fetchall()

        # 2. Build folder tree
        folder_rows = catalog.execute(
            "SELECT id, parent_id FROM folders WHERE location_id = ?",
            (loc_id,),
        ).fetchall()

        direct_size = {}
        direct_count = {}
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

        direct_dup = {}
        root_dup_count = dup_folder_counts.get(None, 0)
        for fid, cnt in dup_folder_counts.items():
            if fid is not None:
                direct_dup[fid] = cnt

        direct_hidden = {}
        root_hidden_count = 0
        for r in hidden_rows:
            fid = r["folder_id"]
            if fid is None:
                root_hidden_count = r["cnt"] or 0
            else:
                direct_hidden[fid] = r["cnt"] or 0

        direct_types = defaultdict(dict)
        for r in type_rows:
            fid = r["folder_id"]
            ftype = r["file_type_high"] or ""
            direct_types[fid][ftype] = r["cnt"] or 0
        root_type_counts = dict(direct_types.get(None, {}))

        children_of = {}
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
        cum_dup = {}
        cum_hidden = {}
        cum_types = {}

        def merge_types(a, b):
            result = dict(a)
            for k, v in b.items():
                result[k] = result.get(k, 0) + v
            return result

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
                types = merge_types(types, cum_types[child_id])
            cum_size[fid] = size
            cum_count[fid] = count
            cum_dup[fid] = dup
            cum_hidden[fid] = hidden
            cum_types[fid] = types

        for root_id in children_of.get(None, []):
            accumulate(root_id)

        # 4. Write to stats.db
        stats.executemany(
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
                    loc_id,
                    cum_count.get(fid, 0),
                    cum_size.get(fid, 0),
                    cum_dup.get(fid, 0),
                    cum_hidden.get(fid, 0),
                    json.dumps(cum_types.get(fid, {})),
                )
                for fid in all_folder_ids
            ],
        )

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

        stats.execute(
            "INSERT INTO location_stats "
            "(location_id, file_count, total_size, "
            "duplicate_count, hidden_count, type_counts) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(location_id) DO UPDATE SET "
            "file_count=excluded.file_count, total_size=excluded.total_size, "
            "duplicate_count=excluded.duplicate_count, hidden_count=excluded.hidden_count, "
            "type_counts=excluded.type_counts",
            (
                loc_id,
                loc_total_count,
                loc_total_size,
                loc_dup_count,
                loc_hidden_count,
                json.dumps(loc_type_counts),
            ),
        )
        stats.commit()

        elapsed = time.monotonic() - t0
        print(f"  {len(all_folder_ids)} folders, {loc_total_count:,} files, {loc_total_size:,} bytes")
        print(f"  {loc_dup_count:,} duplicates, {loc_hidden_count:,} hidden")
        print(f"  Done in {elapsed:.1f}s")

    catalog.close()
    hashes.close()
    stats.close()


def main():
    parser = argparse.ArgumentParser(description="Recalculate location stats")
    parser.add_argument("--locations", required=True, help="Comma-separated location IDs")
    parser.add_argument("--data", default="data", help="Path to data directory")
    args = parser.parse_args()

    location_ids = [int(x.strip()) for x in args.locations.split(",")]
    recalculate(args.data, location_ids)


if __name__ == "__main__":
    main()
