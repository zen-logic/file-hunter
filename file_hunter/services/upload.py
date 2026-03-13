"""Upload processing — hash uploaded files, detect duplicates, catalog or stub."""

from datetime import datetime, timezone

from file_hunter.core import classify_file
from file_hunter.db import db_writer, get_db
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

    await broadcast(
        {
            "type": "upload_started",
            "locationId": location_id,
            "location": location_name,
            "fileCount": total,
        }
    )

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    db = await get_db()

    for i, sf in enumerate(saved_files):
        try:
            hash_fast, hash_strong = await fs.file_hash(
                sf["full_path"], location_id, strong=True
            )

            # Check for existing file with same strong hash (read)
            rows = await db.execute_fetchall(
                """SELECT f.id, f.filename, f.full_path, l.name as location_name
                   FROM files f
                   JOIN locations l ON l.id = f.location_id
                   WHERE f.hash_strong = ? LIMIT 1""",
                (hash_strong,),
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
                            hash_fast, hash_strong, description, tags,
                            created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                           VALUES (?, ?, ?, ?, ?, 'text', 'moved', ?, NULL, NULL, '', '',
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

                affected_hashes.add(hash_strong)
                duplicates += 1
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
                            hash_fast, hash_strong, description, tags,
                            created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '',
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
                            hash_fast,
                            hash_strong,
                            created,
                            modified,
                            now_iso,
                            now_iso,
                        ),
                    )

                affected_hashes.add(hash_strong)
                cataloged += 1

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
            strong_hashes=affected_hashes, source=f"upload to {location_name}"
        )
    except Exception:
        pass
