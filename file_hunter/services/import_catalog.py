"""Import a catalog DB into the File Hunter server database.

Reads from a portable catalog.db produced by file-hunter-catalog,
bulk-writes files and folders into the server DB through db_writer().
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone

from file_hunter.db import db_writer, get_db

logger = logging.getLogger("file_hunter")

# In-memory progress — polled by the route
_progress = {
    "status": "idle",
    "files_total": 0,
    "files_imported": 0,
    "folders_created": 0,
    "error": None,
}

BATCH_SIZE = 2000


def get_progress() -> dict:
    return dict(_progress)


def read_catalog_meta(path: str) -> dict:
    """Read metadata from a catalog DB file. Returns dict with info."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    meta = {}
    for row in conn.execute("SELECT key, value FROM catalog_meta"):
        meta[row["key"]] = row["value"]

    file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    folder_count = conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0]
    total_size = conn.execute(
        "SELECT COALESCE(SUM(file_size), 0) FROM files"
    ).fetchone()[0]

    conn.close()

    return {
        "root_path": meta.get("root_path", ""),
        "hostname": meta.get("hostname", ""),
        "platform": meta.get("platform", ""),
        "created_at": meta.get("created_at", ""),
        "completed_at": meta.get("completed_at"),
        "file_count": file_count,
        "folder_count": folder_count,
        "total_size": total_size,
    }


async def run_import(
    catalog_path: str,
    location_id: int,
    root_path: str,
):
    """Bulk import catalog into server DB. Updates _progress in-place."""
    global _progress
    _progress = {
        "status": "running",
        "files_total": 0,
        "files_imported": 0,
        "folders_created": 0,
        "error": None,
    }

    try:
        cat = sqlite3.connect(catalog_path)
        cat.row_factory = sqlite3.Row

        _progress["files_total"] = cat.execute("SELECT COUNT(*) FROM files").fetchone()[
            0
        ]

        # --- Import folders ---
        cat_folders = cat.execute(
            "SELECT id, parent_id, name, rel_path, hidden "
            "FROM folders ORDER BY length(rel_path)"
        ).fetchall()

        # Build catalog_folder_id -> server_folder_id mapping
        folder_map: dict[int, int] = {}

        for f in cat_folders:
            cat_id = f["id"]
            rel_path = f["rel_path"]

            # Check if folder already exists
            db = await get_db()
            rows = await db.execute_fetchall(
                "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
                (location_id, rel_path),
            )
            row = rows[0] if rows else None

            if row:
                folder_map[cat_id] = row["id"]
                continue

            # Determine server parent_id
            cat_parent = f["parent_id"]
            server_parent = folder_map.get(cat_parent) if cat_parent else None

            async with db_writer() as wdb:
                cursor = await wdb.execute(
                    "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (location_id, server_parent, f["name"], rel_path, f["hidden"]),
                )
                folder_map[cat_id] = cursor.lastrowid

            _progress["folders_created"] += 1

        # --- Import files in batches ---
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        offset = 0

        while True:
            rows = cat.execute(
                "SELECT folder_id, filename, rel_path, file_size, "
                "file_type_high, file_type_low, hash_partial, "
                "created_date, modified_date, hidden "
                "FROM files LIMIT ? OFFSET ?",
                (BATCH_SIZE, offset),
            ).fetchall()

            if not rows:
                break

            batch = []
            for r in rows:
                cat_folder_id = r["folder_id"]
                server_folder_id = (
                    folder_map.get(cat_folder_id) if cat_folder_id else None
                )
                full_path = os.path.join(root_path, r["rel_path"])

                batch.append(
                    (
                        r["filename"],
                        full_path,
                        r["rel_path"],
                        location_id,
                        server_folder_id,
                        r["file_type_high"],
                        r["file_type_low"],
                        r["file_size"],
                        r["hash_partial"],
                        "",  # description
                        "",  # tags
                        r["created_date"],
                        r["modified_date"],
                        now,  # date_cataloged
                        now,  # date_last_seen
                        r["hidden"],
                    )
                )

            async with db_writer() as wdb:
                await wdb.executemany(
                    "INSERT INTO files "
                    "(filename, full_path, rel_path, location_id, folder_id, "
                    "file_type_high, file_type_low, file_size, "
                    "hash_partial, "
                    "description, tags, created_date, modified_date, "
                    "date_cataloged, date_last_seen, hidden) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(location_id, rel_path) DO UPDATE SET "
                    "filename=excluded.filename, full_path=excluded.full_path, "
                    "folder_id=excluded.folder_id, "
                    "file_type_high=excluded.file_type_high, "
                    "file_type_low=excluded.file_type_low, "
                    "file_size=excluded.file_size, "
                    "hash_partial=excluded.hash_partial, "
                    "created_date=excluded.created_date, "
                    "modified_date=excluded.modified_date, "
                    "date_last_seen=excluded.date_last_seen, "
                    "hidden=excluded.hidden",
                    batch,
                )

            _progress["files_imported"] += len(batch)
            offset += BATCH_SIZE

        cat.close()

        # --- Recalculate location sizes ---
        _progress["status"] = "recalculating"
        await _recalculate_location_sizes(location_id)

        # Update date_last_scanned and record import in scans table
        async with db_writer() as wdb:
            await wdb.execute(
                "UPDATE locations SET date_last_scanned = ? WHERE id = ?",
                (now, location_id),
            )
            await wdb.execute(
                "INSERT INTO scans (location_id, status, started_at, completed_at, "
                "files_found) VALUES (?, 'imported', ?, ?, ?)",
                (location_id, now, now, _progress["files_imported"]),
            )

        _progress["status"] = "complete"
        logger.info(
            "Catalog import complete: %d files, %d folders into location %d",
            _progress["files_imported"],
            _progress["folders_created"],
            location_id,
        )

    except Exception as e:
        logger.exception("Catalog import failed")
        _progress["status"] = "error"
        _progress["error"] = str(e)
    finally:
        # Clean up temp file
        try:
            os.unlink(catalog_path)
        except OSError:
            pass


async def _recalculate_location_sizes(location_id: int):
    """Rebuild all stored counters for a location after import."""
    from file_hunter.services.sizes import recalculate_location_sizes

    await recalculate_location_sizes(location_id)
