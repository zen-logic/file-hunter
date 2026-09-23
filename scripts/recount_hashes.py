#!/usr/bin/env python3
"""Recount dup_count in hashes.db.

Fixes inflated counts from the swapped hash_configs bug in migrate_hashes.py.
Resets all dup_count to 0, then recounts hash_strong then hash_fast groups.

Usage:
    python recount_hashes.py [--hashes PATH]
"""

import argparse
import os
import sqlite3
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def recount(hashes_path: str):
    print(f"Hashes: {hashes_path}")

    hdb = sqlite3.connect(hashes_path)
    hdb.row_factory = sqlite3.Row
    hdb.execute("PRAGMA journal_mode=WAL")

    # Ensure excluded column and view exist
    try:
        hdb.execute(
            "ALTER TABLE file_hashes ADD COLUMN excluded INTEGER NOT NULL DEFAULT 0"
        )
        hdb.commit()
    except sqlite3.OperationalError:
        pass
    hdb.execute(
        "CREATE VIEW IF NOT EXISTS active_hashes AS "
        "SELECT * FROM file_hashes WHERE excluded = 0"
    )
    hdb.commit()

    # Reset all dup_count to 0
    total = hdb.execute("SELECT COUNT(*) FROM active_hashes").fetchone()[0]
    print(f"\n{total:,} active entries — resetting dup_count to 0...")
    hdb.execute("UPDATE file_hashes SET dup_count = 0")
    hdb.commit()

    print("\nRecounting duplicates...\n")
    t0 = time.monotonic()

    hash_configs = [
        ("hash_strong", ""),
        ("hash_fast", " AND hash_strong IS NULL"),
    ]

    total_updated = 0
    for hash_col, update_extra in hash_configs:
        rows = hdb.execute(
            f"SELECT {hash_col}, COUNT(*) as cnt FROM active_hashes "
            f"WHERE {hash_col} IS NOT NULL AND {hash_col} != '' "
            f"GROUP BY {hash_col} HAVING COUNT(*) > 1"
        ).fetchall()

        dup_count = len(rows)
        print(f"  {hash_col}: {dup_count:,} duplicate groups")

        if not rows:
            continue

        updated = 0
        for r in rows:
            dc = r[1] - 1
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
            print(f"\r    {updated:,} / {dup_count:,} groups updated\033[K")

    # Summary
    with_dups = hdb.execute(
        "SELECT COUNT(*) FROM active_hashes WHERE dup_count > 0"
    ).fetchone()[0]

    elapsed = time.monotonic() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(
        f"Files with duplicates: {with_dups:,} / {total:,} ({with_dups / total * 100:.1f}%)"
    )

    hdb.close()


def main():
    parser = argparse.ArgumentParser(description="Recount dup_count in hashes.db")
    parser.add_argument(
        "--hashes",
        default=os.path.join(_ROOT, "data", "hashes.db"),
        help="Path to hashes DB",
    )
    args = parser.parse_args()

    try:
        recount(args.hashes)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except sqlite3.OperationalError as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
