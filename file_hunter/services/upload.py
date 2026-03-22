"""Upload processing — hash uploaded files, detect duplicates, catalog or stub."""

from datetime import datetime, timezone

from file_hunter.core import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.services import fs
from file_hunter.ws.scan import broadcast


async def run_upload(
    location_id: int,
    location_name: str,
    root_path: str,
    folder_id: int | None,
    saved_files: list[dict],
):
    """Background task: hash each saved file, check for duplicates, catalog or write .moved stub.

    saved_files: list of { 'filename': str, 'full_path': str, 'rel_path': str }
    """
    total = len(saved_files)
    cataloged = 0
    duplicates = 0
    affected_hashes: set[str] = set()

    # Look up agent_id for hash_partial computation
    async with read_db() as db:
        loc_row = await db.execute_fetchall(
            "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
        )
    agent_id = loc_row[0]["agent_id"] if loc_row else None

    await broadcast(
        {
            "type": "upload_started",
            "locationId": location_id,
            "location": location_name,
            "fileCount": total,
        }
    )

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for i, sf in enumerate(saved_files):
        try:
            (hash_fast,) = await fs.file_hash(sf["full_path"], location_id)
            hash_strong = None

            # Compute hash_partial — needed for dup candidate detection
            hash_partial = None
            if agent_id is not None:
                from file_hunter.services.agent_ops import hash_partial_batch

                try:
                    hp_result = await hash_partial_batch(agent_id, [sf["full_path"]])
                    for hr in hp_result.get("results", []):
                        if hr.get("path") == sf["full_path"]:
                            hash_partial = hr.get("hash_partial")
                            break
                except Exception:
                    pass  # hash_partial is optional — hash_fast/strong still work

            # Check for existing file with same hash (read)
            from file_hunter.hashes_db import read_hashes

            async with read_hashes() as hdb:
                hash_rows = await hdb.execute_fetchall(
                    "SELECT file_id FROM active_hashes WHERE hash_fast = ? LIMIT 1",
                    (hash_fast,),
                )
            rows = []
            if hash_rows:
                async with read_db() as db:
                    rows = await db.execute_fetchall(
                        """SELECT f.id, f.filename, f.full_path, l.name as location_name
                           FROM files f
                           JOIN locations l ON l.id = f.location_id
                           WHERE f.id = ?""",
                        (hash_rows[0]["file_id"],),
                    )

            if rows:
                # Duplicate — remove uploaded file, write .moved stub
                existing = dict(rows[0])
                await fs.file_delete(sf["full_path"], location_id)

                stub_path = sf["full_path"] + ".moved"
                stub_text = (
                    f"Consolidated by File Hunter\n"
                    f"Original: {sf['filename']}\n"
                    f"Moved to: {existing['full_path']}\n"
                    f"Date: {now_iso}\n"
                )
                await fs.file_write_text(stub_path, stub_text, location_id)

                # Insert stub record
                st = await fs.file_stat(stub_path, location_id)
                stub_size = st["size"] if st else 0
                stub_name = sf["filename"] + ".moved"
                stub_rel = sf["rel_path"] + ".moved"

                async with db_writer() as wdb:
                    await wdb.execute(
                        """INSERT INTO files
                           (filename, full_path, rel_path, location_id, folder_id,
                            file_type_high, file_type_low, file_size,
                            description, tags,
                            created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                           VALUES (?, ?, ?, ?, ?, 'text', 'moved', ?, '', '',
                                   ?, ?, ?, ?, NULL)""",
                        (
                            stub_name,
                            stub_path,
                            stub_rel,
                            location_id,
                            folder_id,
                            stub_size,
                            now_iso,
                            now_iso,
                            now_iso,
                            now_iso,
                        ),
                    )

                affected_hashes.add(hash_fast)
                duplicates += 1

                # Stats: stub file added
                from file_hunter.stats_db import update_stats_for_files

                await update_stats_for_files(
                    location_id,
                    added=[(folder_id, stub_size, "text", 0)],
                )

                await broadcast(
                    {
                        "type": "upload_duplicate",
                        "locationId": location_id,
                        "filename": sf["filename"],
                        "existingLocation": existing["location_name"],
                        "existingPath": existing["full_path"],
                    }
                )
            else:
                # New file — catalog it
                type_high, type_low = classify_file(sf["filename"])
                st = await fs.file_stat(sf["full_path"], location_id)
                if st:
                    file_size = st["size"]
                    modified = datetime.fromtimestamp(
                        st["mtime"], tz=timezone.utc
                    ).isoformat(timespec="seconds")
                    created = modified  # agent stat doesn't return birthtime
                else:
                    file_size = 0
                    created = now_iso
                    modified = now_iso

                async with db_writer() as wdb:
                    await wdb.execute(
                        """INSERT INTO files
                           (filename, full_path, rel_path, location_id, folder_id,
                            file_type_high, file_type_low, file_size,
                            description, tags,
                            created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '',
                                   ?, ?, ?, ?, NULL)""",
                        (
                            sf["filename"],
                            sf["full_path"],
                            sf["rel_path"],
                            location_id,
                            folder_id,
                            type_high,
                            type_low,
                            file_size,
                            created,
                            modified,
                            now_iso,
                            now_iso,
                        ),
                    )
                    # Get file ID inside same writer context
                    cursor_lid = await wdb.execute("SELECT last_insert_rowid()")
                    row_lid = await cursor_lid.fetchone()
                    file_id = row_lid[0]

                # Register in hashes.db for dup detection
                from file_hunter.hashes_db import hashes_writer

                async with hashes_writer() as hdb:
                    await hdb.execute(
                        "INSERT INTO file_hashes "
                        "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                        "VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(file_id) DO UPDATE SET "
                        "hash_partial=COALESCE(excluded.hash_partial, file_hashes.hash_partial), "
                        "hash_fast=excluded.hash_fast, "
                        "hash_strong=excluded.hash_strong",
                        (file_id, location_id, file_size, hash_partial, hash_fast, hash_strong),
                    )

                affected_hashes.add(hash_fast)
                cataloged += 1

                # Broadcast so the file list can show the new file immediately
                await broadcast(
                    {
                        "type": "file_added",
                        "locationId": location_id,
                        "folderId": folder_id,
                        "file": {
                            "id": file_id,
                            "name": sf["filename"],
                            "typeHigh": type_high,
                            "typeLow": type_low,
                            "size": file_size,
                            "date": modified,
                            "dups": 0,
                            "hashStrong": hash_strong,
                            "hashFast": hash_fast,
                            "stale": False,
                            "missing": False,
                            "hidden": bool(sf["filename"].startswith(".")),
                        },
                    }
                )

                # Stats: new file added
                is_hidden = 1 if sf["filename"].startswith(".") else 0
                from file_hunter.stats_db import update_stats_for_files

                await update_stats_for_files(
                    location_id,
                    added=[(folder_id, file_size, type_high, is_hidden)],
                )

            await broadcast(
                {
                    "type": "upload_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "processed": i + 1,
                    "total": total,
                    "cataloged": cataloged,
                    "duplicates": duplicates,
                    "currentFile": sf["filename"],
                }
            )

        except Exception as exc:
            await broadcast(
                {
                    "type": "upload_file_error",
                    "locationId": location_id,
                    "filename": sf["filename"],
                    "error": str(exc),
                }
            )

    await broadcast(
        {
            "type": "upload_completed",
            "locationId": location_id,
            "location": location_name,
            "total": total,
            "cataloged": cataloged,
            "duplicates": duplicates,
        }
    )
    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    from file_hunter.services.sizes import recalculate_location_sizes

    try:
        await recalculate_location_sizes(location_id)
    except Exception:
        pass

    from file_hunter.services.dup_counts import recalculate_dup_counts

    try:
        await recalculate_dup_counts(
            fast_hashes=affected_hashes, source=f"upload to {location_name}"
        )
    except Exception:
        pass
