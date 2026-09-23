#!/usr/bin/env python3
"""Migrate stats from catalog DB to stats.db.

Copies folder and location counters (file_count, total_size,
duplicate_count, type_counts, hidden_count) into stats.db.
Run once after upgrading. Safe to re-run — uses INSERT OR REPLACE.

Usage:
    python migrate_stats.py [--catalog PATH] [--stats PATH]

Defaults:
    --catalog  data/file_hunter.db
    --stats    data/stats.db
"""

import argparse
import os
import sqlite3
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def migrate(catalog_path: str, stats_path: str):
    print(f"Catalog: {catalog_path}")
    print(f"Stats:   {stats_path}")

    cat = sqlite3.connect(f"file:{catalog_path}?mode=ro", uri=True)
    cat.row_factory = sqlite3.Row

    sdb = sqlite3.connect(stats_path)
    sdb.execute("PRAGMA journal_mode=WAL")
    sdb.execute("PRAGMA synchronous=NORMAL")
    sdb.execute(
        """CREATE TABLE IF NOT EXISTS folder_stats (
            folder_id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            file_count INTEGER NOT NULL DEFAULT 0,
            total_size INTEGER NOT NULL DEFAULT 0,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            type_counts TEXT NOT NULL DEFAULT '{}',
            hidden_count INTEGER NOT NULL DEFAULT 0
        )"""
    )
    sdb.execute(
        """CREATE TABLE IF NOT EXISTS location_stats (
            location_id INTEGER PRIMARY KEY,
            file_count INTEGER NOT NULL DEFAULT 0,
            total_size INTEGER NOT NULL DEFAULT 0,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            type_counts TEXT NOT NULL DEFAULT '{}',
            hidden_count INTEGER NOT NULL DEFAULT 0
        )"""
    )
    sdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_folder_stats_location "
        "ON folder_stats(location_id)"
    )
    sdb.commit()

    # --- Locations ---
    loc_rows = cat.execute(
        "SELECT id, file_count, total_size, duplicate_count, type_counts, hidden_count "
        "FROM locations WHERE name NOT LIKE '__deleting_%'"
    ).fetchall()

    print(f"\n{len(loc_rows)} locations to migrate")

    loc_batch = [
        (
            r["id"],
            r["file_count"] or 0,
            r["total_size"] or 0,
            r["duplicate_count"] or 0,
            r["type_counts"] or "{}",
            r["hidden_count"] or 0,
        )
        for r in loc_rows
    ]
    sdb.executemany(
        "INSERT OR REPLACE INTO location_stats "
        "(location_id, file_count, total_size, duplicate_count, type_counts, hidden_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        loc_batch,
    )
    sdb.commit()
    print(f"  {len(loc_batch)} locations migrated")

    # --- Folders ---
    total_folders = cat.execute("SELECT COUNT(*) FROM folders").fetchone()[0]

    print(f"\n{total_folders:,} folders to migrate\n")

    BATCH = 10000
    last_id = 0
    migrated = 0
    t0 = time.monotonic()
    last_print = t0

    while True:
        rows = cat.execute(
            "SELECT id, location_id, file_count, total_size, "
            "duplicate_count, type_counts, hidden_count "
            "FROM folders WHERE id > ? ORDER BY id LIMIT ?",
            (last_id, BATCH),
        ).fetchall()

        if not rows:
            break

        batch = [
            (
                r["id"],
                r["location_id"],
                r["file_count"] or 0,
                r["total_size"] or 0,
                r["duplicate_count"] or 0,
                r["type_counts"] or "{}",
                r["hidden_count"] or 0,
            )
            for r in rows
        ]

        sdb.executemany(
            "INSERT OR REPLACE INTO folder_stats "
            "(folder_id, location_id, file_count, total_size, "
            "duplicate_count, type_counts, hidden_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        sdb.commit()

        last_id = rows[-1]["id"]
        migrated += len(batch)

        now = time.monotonic()
        if now - last_print >= 1.0 or migrated >= total_folders:
            elapsed = now - t0
            rate = migrated / elapsed if elapsed > 0 else 0
            pct = migrated / total_folders * 100 if total_folders else 100
            remaining = (total_folders - migrated) / rate if rate > 0 else 0
            print(
                f"\r  {migrated:>10,} / {total_folders:,}  ({pct:5.1f}%)  "
                f"{rate:,.0f} folders/sec  "
                f"ETA {remaining:.0f}s\033[K",
                end="",
                flush=True,
            )
            last_print = now

    elapsed = time.monotonic() - t0
    print(f"\n\nMigrated {migrated:,} folders in {elapsed:.1f}s")

    cat.close()
    sdb.close()


def main():
    parser = argparse.ArgumentParser(
        description="Migrate stats from catalog to stats.db"
    )
    parser.add_argument(
        "--catalog",
        default=os.path.join(_ROOT, "data", "file_hunter.db"),
        help="Path to catalog DB",
    )
    parser.add_argument(
        "--stats",
        default=os.path.join(_ROOT, "data", "stats.db"),
        help="Path to stats DB",
    )
    args = parser.parse_args()

    try:
        migrate(args.catalog, args.stats)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except sqlite3.OperationalError as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
