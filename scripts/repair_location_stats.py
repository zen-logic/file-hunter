#!/usr/bin/env python3
"""Recalculate location_stats and folder_stats for specific locations.

Usage:
    python repair_location_stats.py --locations 14,80 [--data PATH]
"""

import argparse
import os
import sqlite3
import sys
import time
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from file_hunter.stats_rollup import (  # noqa: E402
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
)


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

        direct_rows = catalog.execute(DIRECT_SQL, (loc_id,)).fetchall()

        dup_ids = [r["file_id"] for r in hashes.execute(DUP_FILES_SQL, (loc_id,))]
        dup_counts = Counter()
        for i in range(0, len(dup_ids), DUP_BATCH):
            batch = dup_ids[i : i + DUP_BATCH]
            rows = catalog.execute(dup_folders_sql(len(batch)), batch).fetchall()
            dup_counts.update(r["folder_id"] for r in rows)

        hidden_rows = catalog.execute(HIDDEN_SQL, (loc_id,)).fetchall()
        type_rows = catalog.execute(TYPES_SQL, (loc_id,)).fetchall()
        folder_rows = catalog.execute(FOLDERS_SQL, (loc_id,)).fetchall()

        folder_params, location_params, _, _ = location_counters(
            loc_id, direct_rows, dup_counts, hidden_rows, type_rows, folder_rows
        )
        stats.executemany(FOLDER_STATS_UPSERT, folder_params)
        stats.execute(LOCATION_STATS_UPSERT, location_params)
        stats.commit()

        _, loc_count, loc_size, loc_dup, loc_hidden, _ = location_params
        elapsed = time.monotonic() - t0
        print(f"  {len(folder_params)} folders, {loc_count:,} files, {loc_size:,} bytes")
        print(f"  {loc_dup:,} duplicates, {loc_hidden:,} hidden")
        print(f"  Done in {elapsed:.1f}s")

    catalog.close()
    hashes.close()
    stats.close()


def main():
    parser = argparse.ArgumentParser(description="Recalculate location stats")
    parser.add_argument("--locations", required=True, help="Comma-separated location IDs")
    parser.add_argument("--data", default=os.path.join(ROOT, "data"), help="Path to data directory")
    args = parser.parse_args()

    location_ids = [int(x.strip()) for x in args.locations.split(",")]
    recalculate(args.data, location_ids)


if __name__ == "__main__":
    main()
