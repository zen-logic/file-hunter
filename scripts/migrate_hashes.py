#!/usr/bin/env python3
"""Migrate hash data from catalog DB to hashes.db.

Run once after upgrading. Safe to re-run — uses INSERT OR REPLACE.
Does not block the server. Does not modify the catalog.

Usage:
    python migrate_hashes.py [--catalog PATH] [--hashes PATH]

Defaults:
    --catalog  data/file_hunter.db
    --hashes   data/hashes.db
"""

import argparse
import os
import sqlite3
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def migrate(catalog_path: str, hashes_path: str):
    print(f"Catalog: {catalog_path}")
    print(f"Hashes:  {hashes_path}")

    # Open catalog read-only
    cat = sqlite3.connect(f"file:{catalog_path}?mode=ro", uri=True)
    cat.row_factory = sqlite3.Row

    # Open/create hashes DB
    hdb = sqlite3.connect(hashes_path)
    hdb.execute("PRAGMA journal_mode=WAL")
    hdb.execute("PRAGMA synchronous=NORMAL")
    hdb.execute(
        """CREATE TABLE IF NOT EXISTS file_hashes (
            file_id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            file_size INTEGER NOT NULL,
            hash_partial TEXT,
            hash_fast TEXT,
            hash_strong TEXT,
            dup_count INTEGER NOT NULL DEFAULT 0
        )"""
    )
    hdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_hashes_size_partial "
        "ON file_hashes(file_size, hash_partial)"
    )
    hdb.execute("CREATE INDEX IF NOT EXISTS idx_hashes_fast ON file_hashes(hash_fast)")
    hdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_hashes_strong ON file_hashes(hash_strong)"
    )
    hdb.execute(
        "CREATE INDEX IF NOT EXISTS idx_hashes_location ON file_hashes(location_id)"
    )
    hdb.commit()

    # Count total files with any hash data (including excluded — they keep their hashes)
    total = cat.execute(
        "SELECT COUNT(*) FROM files "
        "WHERE stale = 0 "
        "AND (hash_partial IS NOT NULL OR hash_fast IS NOT NULL OR hash_strong IS NOT NULL)"
    ).fetchone()[0]

    print(f"\n{total:,} files with hash data to migrate\n")

    if total == 0:
        print("Nothing to migrate.")
        cat.close()
        hdb.close()
        return

    BATCH = 5000
    last_id = 0
    migrated = 0
    t0 = time.monotonic()
    last_print = t0

    while True:
        rows = cat.execute(
            "SELECT id, location_id, file_size, hash_partial, hash_fast, hash_strong, dup_exclude "
            "FROM files "
            "WHERE id > ? AND stale = 0 "
            "AND (hash_partial IS NOT NULL OR hash_fast IS NOT NULL OR hash_strong IS NOT NULL) "
            "ORDER BY id LIMIT ?",
            (last_id, BATCH),
        ).fetchall()

        if not rows:
            break

        batch = [
            (
                r["id"],
                r["location_id"],
                r["file_size"],
                r["hash_partial"],
                r["hash_fast"],
                r["hash_strong"],
                1 if r["dup_exclude"] else 0,
            )
            for r in rows
        ]

        hdb.executemany(
            "INSERT OR REPLACE INTO file_hashes "
            "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong, excluded) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        hdb.commit()

        last_id = rows[-1]["id"]
        migrated += len(batch)

        now = time.monotonic()
        if now - last_print >= 1.0 or migrated == total:
            elapsed = now - t0
            rate = migrated / elapsed if elapsed > 0 else 0
            pct = migrated / total * 100
            remaining = (total - migrated) / rate if rate > 0 else 0
            print(
                f"\r  {migrated:>10,} / {total:,}  ({pct:5.1f}%)  "
                f"{rate:,.0f} files/sec  "
                f"ETA {remaining:.0f}s\033[K",
                end="",
                flush=True,
            )
            last_print = now

    elapsed = time.monotonic() - t0
    print(f"\n\nMigrated {migrated:,} files in {elapsed:.1f}s")

    # --- Recount dup_count ---
    print("\nRecounting duplicates...")
    t1 = time.monotonic()

    hash_configs = [
        ("hash_strong", ""),
        ("hash_fast", " AND hash_strong IS NULL"),
    ]

    total_updated = 0
    for hash_col, update_extra in hash_configs:
        # GROUP BY to get counts
        rows = hdb.execute(
            f"SELECT {hash_col}, COUNT(*) as cnt FROM file_hashes "
            f"WHERE {hash_col} IS NOT NULL AND {hash_col} != '' "
            f"GROUP BY {hash_col} HAVING COUNT(*) > 1"
        ).fetchall()

        dup_count = len(rows)
        print(f"  {hash_col}: {dup_count:,} duplicate groups")

        if not rows:
            continue

        # Batch update dup_count
        updated = 0
        for r in rows:
            dc = r[1] - 1  # count - 1
            hdb.execute(
                f"UPDATE file_hashes SET dup_count = ? "
                f"WHERE {hash_col} = ?{update_extra}",
                (dc, r[0]),
            )
            updated += 1
            if updated % 10000 == 0:
                hdb.commit()
                print(
                    f"\r    {updated:,} / {dup_count:,} groups updated\033[K",
                    end="",
                    flush=True,
                )

        hdb.commit()
        total_updated += updated
        if dup_count > 0:
            print(f"\r    {updated:,} / {dup_count:,} groups updated")

    # Zero out non-duplicates (anything not touched above keeps default 0)

    elapsed2 = time.monotonic() - t1
    print(f"\nDup recount complete: {total_updated:,} groups in {elapsed2:.1f}s")

    total_elapsed = time.monotonic() - t0
    print(f"\nTotal: {total_elapsed:.1f}s")

    cat.close()
    hdb.close()


def main():
    parser = argparse.ArgumentParser(
        description="Migrate hashes from catalog to hashes.db"
    )
    parser.add_argument(
        "--catalog",
        default=os.path.join(_ROOT, "data", "file_hunter.db"),
        help="Path to catalog DB",
    )
    parser.add_argument(
        "--hashes",
        default=os.path.join(_ROOT, "data", "hashes.db"),
        help="Path to hashes DB",
    )
    args = parser.parse_args()

    try:
        migrate(args.catalog, args.hashes)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except sqlite3.OperationalError as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
