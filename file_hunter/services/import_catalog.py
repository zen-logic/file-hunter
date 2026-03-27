"""Import a catalog DB into the File Hunter server database.

Reads from a portable catalog.db produced by file-hunter-catalog,
bulk-writes files and folders into the server DB through db_writer().
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone

from file_hunter.core import ProgressTracker
from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import hashes_writer

logger = logging.getLogger("file_hunter")

_progress = ProgressTracker(
    files_total=0,
    files_imported=0,
    files_new=0,
    folders_created=0,
    dup_hashes_done=0,
    dup_hashes_total=0,
)

BATCH_SIZE = 2000


def get_progress() -> dict:
    return _progress.snapshot()


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
    _progress.update(
        status="running",
        files_total=0,
        files_imported=0,
        files_new=0,
        folders_created=0,
        dup_hashes_done=0,
        dup_hashes_total=0,
        error=None,
    )

    try:
        cat = sqlite3.connect(catalog_path)
        cat.row_factory = sqlite3.Row

        _progress["files_total"] = cat.execute("SELECT COUNT(*) FROM files").fetchone()[
            0
        ]

        # Count existing files so we can report new vs updated
        async with read_db() as db:
            before_count_rows = await db.execute_fetchall(
                "SELECT COUNT(*) as c FROM files WHERE location_id = ?",
                (location_id,),
            )
        files_before = before_count_rows[0]["c"]

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
            async with read_db() as db:
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
            # hash_fast may not exist in older catalogs
            try:
                rows = cat.execute(
                    "SELECT folder_id, filename, rel_path, file_size, "
                    "file_type_high, file_type_low, hash_partial, hash_fast, "
                    "created_date, modified_date, hidden "
                    "FROM files LIMIT ? OFFSET ?",
                    (BATCH_SIZE, offset),
                ).fetchall()
                has_hash_fast = True
            except Exception:
                rows = cat.execute(
                    "SELECT folder_id, filename, rel_path, file_size, "
                    "file_type_high, file_type_low, hash_partial, "
                    "created_date, modified_date, hidden "
                    "FROM files LIMIT ? OFFSET ?",
                    (BATCH_SIZE, offset),
                ).fetchall()
                has_hash_fast = False

            if not rows:
                break

            catalog_batch = []
            hash_batch = []
            for r in rows:
                cat_folder_id = r["folder_id"]
                server_folder_id = (
                    folder_map.get(cat_folder_id) if cat_folder_id else None
                )
                full_path = os.path.join(root_path, r["rel_path"])

                catalog_batch.append(
                    (
                        r["filename"],
                        full_path,
                        r["rel_path"],
                        location_id,
                        server_folder_id,
                        r["file_type_high"],
                        r["file_type_low"],
                        r["file_size"],
                        "",  # description
                        "",  # tags
                        r["created_date"],
                        r["modified_date"],
                        now,  # date_cataloged
                        now,  # date_last_seen
                        r["hidden"],
                    )
                )

                # Collect hashes for hashes.db
                hp = r["hash_partial"]
                hf = r["hash_fast"] if has_hash_fast else None
                if hp or hf:
                    # file_id not known yet — will be resolved after catalog insert
                    hash_batch.append(
                        (
                            r["rel_path"],
                            r["file_size"],
                            hp,
                            hf,
                        )
                    )

            async with db_writer() as wdb:
                await wdb.executemany(
                    "INSERT INTO files "
                    "(filename, full_path, rel_path, location_id, folder_id, "
                    "file_type_high, file_type_low, file_size, "
                    "description, tags, created_date, modified_date, "
                    "date_cataloged, date_last_seen, hidden) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(location_id, rel_path) DO UPDATE SET "
                    "filename=excluded.filename, full_path=excluded.full_path, "
                    "folder_id=excluded.folder_id, "
                    "file_type_high=excluded.file_type_high, "
                    "file_type_low=excluded.file_type_low, "
                    "file_size=excluded.file_size, "
                    "created_date=excluded.created_date, "
                    "modified_date=excluded.modified_date, "
                    "date_last_seen=excluded.date_last_seen, "
                    "hidden=excluded.hidden, "
                    "description=COALESCE(description, ''), "
                    "tags=COALESCE(tags, '')",
                    catalog_batch,
                )

            # Resolve file_ids and register hashes in hashes.db
            if hash_batch:
                rel_paths = [h[0] for h in hash_batch]
                hash_by_rel: dict[str, tuple] = {h[0]: h for h in hash_batch}
                async with read_db() as rdb:
                    for i in range(0, len(rel_paths), 500):
                        rp_batch = rel_paths[i : i + 500]
                        ph = ",".join("?" for _ in rp_batch)
                        id_rows = await rdb.execute_fetchall(
                            f"SELECT id, rel_path, file_size FROM files "
                            f"WHERE location_id = ? AND rel_path IN ({ph})",
                            [location_id] + rp_batch,
                        )
                        hashes_to_insert = []
                        for ir in id_rows:
                            h = hash_by_rel.get(ir["rel_path"])
                            if h:
                                hashes_to_insert.append(
                                    (
                                        ir["id"],
                                        location_id,
                                        ir["file_size"],
                                        h[2],
                                        h[3],
                                        None,  # hash_partial, hash_fast, hash_strong
                                    )
                                )
                        if hashes_to_insert:
                            async with hashes_writer() as hdb:
                                await hdb.executemany(
                                    "INSERT INTO file_hashes "
                                    "(file_id, location_id, file_size, "
                                    "hash_partial, hash_fast, hash_strong) "
                                    "VALUES (?, ?, ?, ?, ?, ?) "
                                    "ON CONFLICT(file_id) DO UPDATE SET "
                                    "file_size=excluded.file_size, "
                                    "hash_partial=COALESCE(excluded.hash_partial, file_hashes.hash_partial), "
                                    "hash_fast=COALESCE(excluded.hash_fast, file_hashes.hash_fast)",
                                    hashes_to_insert,
                                )

            _progress["files_imported"] += len(catalog_batch)
            offset += BATCH_SIZE

        cat.close()

        # Count new files
        async with read_db() as db:
            after_count_rows = await db.execute_fetchall(
                "SELECT COUNT(*) as c FROM files WHERE location_id = ?",
                (location_id,),
            )
        files_after = after_count_rows[0]["c"]
        _progress["files_new"] = files_after - files_before

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
