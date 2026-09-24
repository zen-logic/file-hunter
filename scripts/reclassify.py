#!/usr/bin/env python3
"""Reclassify files whose extension is now recognized but was 'other' at scan time.

Run this after upgrading File Hunter if new file types have been added.
Safe to run while the server is running (uses its own connection with WAL).

Usage:
    python scripts/reclassify.py
    python scripts/reclassify.py --db /path/to/file_hunter.db
"""

import argparse
import asyncio
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)


def _default_db() -> str:
    """The catalogue the server uses: config.json "database", relative to the
    install root."""
    from file_hunter.config import load_config

    db_path = load_config().get("database", "file_hunter.db")
    return db_path if os.path.isabs(db_path) else os.path.join(_ROOT, db_path)


async def main(db_path: str):
    try:
        import aiosqlite
    except ImportError:
        print("Error: aiosqlite not installed. Run: pip install aiosqlite")
        sys.exit(1)

    try:
        from file_hunter_core.classify import _EXT_MAP
    except ImportError:
        print("Error: file_hunter_core not found.")
        sys.exit(1)

    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA busy_timeout=30000")

    try:
        rows = await conn.execute_fetchall(
            "SELECT DISTINCT file_type_low FROM files WHERE file_type_high = 'other'"
        )
        stale_exts = {r["file_type_low"] for r in rows}

        if not stale_exts:
            print("No files need reclassification.")
            return

        print(f"Found {len(stale_exts)} extension(s) classified as 'other'")

        total_updated = 0
        for ext in sorted(stale_exts):
            mapped = _EXT_MAP.get(ext)
            if not mapped or mapped[0] == "other":
                continue
            high, low = mapped
            cur = await conn.execute(
                "UPDATE files SET file_type_high = ?, file_type_low = ? "
                "WHERE file_type_high = 'other' AND file_type_low = ?",
                (high, low, ext),
            )
            if cur.rowcount > 0:
                total_updated += cur.rowcount
                print(f"  Reclassified {cur.rowcount} .{ext} files as {high}/{low}")
            await conn.commit()

        if total_updated:
            print(f"\nReclassified {total_updated} files total.")
        else:
            print("\nNo files matched new classifications.")
    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reclassify file types in File Hunter database"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to database (default: the server's, from config.json)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.db or _default_db()))
