"""Consolidation logic — copy/keep canonical file, write .moved stubs and .sources metadata."""

import os
from datetime import datetime, timezone

from file_hunter.db import db_writer, get_db
from file_hunter.services import fs
from file_hunter.ws.scan import broadcast

# Guard against concurrent consolidation of the same hash
_active_consolidations: set[str] = set()


def is_consolidation_running(hash_strong: str) -> bool:
    return hash_strong in _active_consolidations


async def run_consolidation(file_id: int, mode: str, dest_folder_id: str | None):
    """Main consolidation background task.

    mode: 'keep_here' or 'copy_to'
    dest_folder_id: required for copy_to (e.g. 'loc-4' or 'fld-7')

    Reads via get_db(), writes via db_writer(). No owned connection.
    """
    hash_strong = None
    filename = None
    stubs_written = 0
    stubs_queued = 0
    try:
        db = await get_db()
        # Load the selected file
        rows = await db.execute_fetchall(
            """SELECT f.*, l.name as location_name, l.root_path
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (file_id,),
        )
        if not rows:
            await broadcast(
                {
                    "type": "consolidate_error",
                    "fileId": file_id,
                    "filename": "",
                    "error": "File not found.",
                }
            )
            return

        selected = dict(rows[0])
        filename = selected["filename"]
        hash_strong = selected["hash_strong"]
        selected_loc_id = selected["location_id"]

        if not hash_strong:
            await broadcast(
                {
                    "type": "consolidate_error",
                    "fileId": file_id,
                    "filename": filename,
                    "error": "File has no hash — scan it first.",
                }
            )
            return

        # Guard concurrent consolidation
        if hash_strong in _active_consolidations:
            await broadcast(
                {
                    "type": "consolidate_error",
                    "fileId": file_id,
                    "filename": filename,
                    "error": "Consolidation already in progress for this file.",
                }
            )
            return
        _active_consolidations.add(hash_strong)

        await broadcast(
            {
                "type": "consolidate_started",
                "fileId": file_id,
                "filename": filename,
            }
        )

        # Find ALL copies by hash_strong (including the selected file)
        all_copies = await db.execute_fetchall(
            """SELECT f.id, f.filename, f.full_path, f.rel_path,
                      f.location_id, f.folder_id,
                      f.file_type_high, f.file_type_low, f.file_size,
                      f.hash_fast, f.hash_strong, f.description, f.tags,
                      f.created_date, f.modified_date, f.date_cataloged,
                      l.name as location_name, l.root_path
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.hash_strong = ? AND f.hidden = 0""",
            (hash_strong,),
        )
        all_copies = [dict(r) for r in all_copies]

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        if mode == "keep_here":
            canonical_path = selected["full_path"]
            canonical_id = file_id
            dest_loc_id = selected_loc_id

            # Verify canonical exists
            if not await fs.file_exists(canonical_path, selected_loc_id):
                await broadcast(
                    {
                        "type": "consolidate_error",
                        "fileId": file_id,
                        "filename": filename,
                        "error": f"Canonical file not found on disk: {canonical_path}",
                    }
                )
                return

            # Compute real hashes (seed data may have fakes)
            await broadcast(
                {
                    "type": "consolidate_progress",
                    "fileId": file_id,
                    "filename": filename,
                    "phase": "verifying",
                }
            )
            real_fast, real_strong = await fs.file_hash(
                canonical_path, selected_loc_id, strong=True
            )
            if real_strong != hash_strong:
                # DB hash was stale — update the canonical record
                async with db_writer() as wdb:
                    await wdb.execute(
                        "UPDATE files SET hash_fast=?, hash_strong=? WHERE id=?",
                        (real_fast, real_strong, file_id),
                    )
                selected["hash_fast"] = real_fast
                selected["hash_strong"] = real_strong

        elif mode == "copy_to":
            # Resolve destination path and location_id
            dest_dir, dest_loc_id = await _resolve_folder_path_with_loc(
                db, dest_folder_id
            )
            if dest_dir is None:
                await broadcast(
                    {
                        "type": "consolidate_error",
                        "fileId": file_id,
                        "filename": filename,
                        "error": "Destination folder not found.",
                    }
                )
                return

            if not await fs.dir_exists(dest_dir, dest_loc_id):
                await broadcast(
                    {
                        "type": "consolidate_error",
                        "fileId": file_id,
                        "filename": filename,
                        "error": f"Destination is offline: {dest_dir}",
                    }
                )
                return

            canonical_path = os.path.join(dest_dir, filename)

            # Prevent overwriting existing file
            if await fs.path_exists(canonical_path, dest_loc_id):
                await broadcast(
                    {
                        "type": "consolidate_error",
                        "fileId": file_id,
                        "filename": filename,
                        "error": f"File already exists at destination: {canonical_path}",
                    }
                )
                return

            # Find a source copy that's online
            source_path = None
            source_loc_id = None
            for copy in all_copies:
                copy_loc_id = copy["location_id"]
                if await fs.file_exists(copy["full_path"], copy_loc_id):
                    source_path = copy["full_path"]
                    source_loc_id = copy_loc_id
                    break

            if source_path is None:
                await broadcast(
                    {
                        "type": "consolidate_error",
                        "fileId": file_id,
                        "filename": filename,
                        "error": "No online copy available to copy from.",
                    }
                )
                return

            # Copy with progress — streaming, constant memory
            async def _copy_progress(bytes_sent, total_bytes):
                await broadcast(
                    {
                        "type": "consolidate_progress",
                        "fileId": file_id,
                        "filename": filename,
                        "phase": "copying",
                        "bytesSent": bytes_sent,
                        "bytesTotal": total_bytes,
                    }
                )

            try:
                await fs.copy_file(
                    source_path,
                    source_loc_id,
                    canonical_path,
                    dest_loc_id,
                    on_progress=_copy_progress,
                )
            except Exception as copy_exc:
                # Clean up partial file at destination
                try:
                    await fs.file_delete(canonical_path, dest_loc_id)
                except Exception:
                    pass
                raise RuntimeError(f"Copy failed: {copy_exc}") from copy_exc

            # Hash-verify against source (not DB hash, which may be stale)
            await broadcast(
                {
                    "type": "consolidate_progress",
                    "fileId": file_id,
                    "filename": filename,
                    "phase": "verifying",
                }
            )
            source_hash_fast, source_hash_strong = await fs.file_hash(
                source_path, source_loc_id, strong=True
            )
            copy_hash_fast, copy_hash_strong = await fs.file_hash(
                canonical_path, dest_loc_id, strong=True
            )
            if copy_hash_strong != source_hash_strong:
                # Hash mismatch — clean up and fail
                await fs.file_delete(canonical_path, dest_loc_id)
                await broadcast(
                    {
                        "type": "consolidate_error",
                        "fileId": file_id,
                        "filename": filename,
                        "error": "Hash verification failed after copy.",
                    }
                )
                return

            # Update selected record with real hashes (seed data may have fakes)
            selected["hash_fast"] = source_hash_fast
            selected["hash_strong"] = source_hash_strong

            # Create DB record for the canonical copy at destination
            canonical_id = await _ensure_canonical_record(
                canonical_path, dest_folder_id, dest_loc_id, selected, now_iso
            )

        else:
            await broadcast(
                {
                    "type": "consolidate_error",
                    "fileId": file_id,
                    "filename": filename,
                    "error": f"Unknown mode: {mode}",
                }
            )
            return

        # Write .sources file next to canonical
        await fs.write_sources_file(canonical_path, all_copies, now_iso, dest_loc_id)
        await _insert_stub_record(
            canonical_path + ".sources",
            canonical_id,
            dest_loc_id,
            now_iso,
        )

        # Process each duplicate (everything except the canonical)
        duplicates = [c for c in all_copies if c["id"] != canonical_id]
        total_dups = len(duplicates)

        for idx, copy in enumerate(duplicates, 1):
            original_path = copy["full_path"]
            loc_root = copy["root_path"]
            copy_loc_id = copy["location_id"]
            location_online = await fs.dir_exists(loc_root, copy_loc_id)
            file_on_disk = location_online and await fs.file_exists(
                original_path, copy_loc_id
            )

            await broadcast(
                {
                    "type": "consolidate_progress",
                    "fileId": file_id,
                    "filename": filename,
                    "phase": "stubs",
                    "current": idx,
                    "total": total_dups,
                    "currentFile": copy["filename"],
                    "location": copy["location_name"],
                }
            )

            if file_on_disk:
                try:
                    await fs.write_moved_stub(
                        original_path,
                        copy["filename"],
                        canonical_path,
                        now_iso,
                        copy_loc_id,
                    )
                    # Replace the original file record with a record for the .moved stub
                    stub_path = original_path + ".moved"
                    stub_name = copy["filename"] + ".moved"
                    stub_rel = copy["rel_path"] + ".moved"
                    st = await fs.file_stat(stub_path, copy_loc_id)
                    stub_size = st["size"] if st else 0
                    async with db_writer() as wdb:
                        await wdb.execute(
                            """UPDATE files SET
                                filename=?, full_path=?, rel_path=?,
                                file_type_high='text', file_type_low='moved',
                                file_size=?, hash_fast=NULL, hash_strong=NULL,
                                modified_date=?, date_last_seen=?
                               WHERE id=?""",
                            (
                                stub_name,
                                stub_path,
                                stub_rel,
                                stub_size,
                                now_iso,
                                now_iso,
                                copy["id"],
                            ),
                        )
                    stubs_written += 1
                except Exception:
                    stubs_queued += 1
                    async with db_writer() as wdb:
                        await wdb.execute(
                            """INSERT INTO consolidation_jobs
                               (source_file, source_location_id, source_path,
                                destination_path, status, date_created)
                               VALUES (?, ?, ?, ?, 'pending', ?)""",
                            (
                                copy["filename"],
                                copy_loc_id,
                                original_path,
                                canonical_path,
                                now_iso,
                            ),
                        )
            else:
                # Offline or file missing — queue for later
                stubs_queued += 1
                async with db_writer() as wdb:
                    await wdb.execute(
                        """INSERT INTO consolidation_jobs
                           (source_file, source_location_id, source_path,
                            destination_path, status, date_created)
                           VALUES (?, ?, ?, ?, 'pending', ?)""",
                        (
                            copy["filename"],
                            copy_loc_id,
                            original_path,
                            canonical_path,
                            now_iso,
                        ),
                    )

        await broadcast(
            {
                "type": "consolidate_completed",
                "fileId": canonical_id,
                "filename": filename,
                "canonicalPath": canonical_path,
                "stubsWritten": stubs_written,
                "stubsQueued": stubs_queued,
            }
        )
        from file_hunter.services.stats import invalidate_stats_cache

        invalidate_stats_cache()

        from file_hunter.services.sizes import recalculate_location_sizes

        affected_loc_ids = {c["location_id"] for c in all_copies}
        for lid in affected_loc_ids:
            await recalculate_location_sizes(lid)

        from file_hunter.services.dup_counts import recalculate_dup_counts

        await recalculate_dup_counts(
            strong_hashes={hash_strong}, source=f"consolidate {filename}"
        )

    except Exception as exc:
        await broadcast(
            {
                "type": "consolidate_error",
                "fileId": file_id,
                "filename": filename or "",
                "error": str(exc),
                "stubsWritten": stubs_written,
                "stubsQueued": stubs_queued,
            }
        )

    finally:
        if hash_strong:
            _active_consolidations.discard(hash_strong)


async def run_batch_consolidation(
    file_ids: list[int], mode: str, dest_folder_id: str | None
):
    """Run consolidation for multiple files sequentially in the background."""
    completed = 0
    errors = 0

    for file_id in file_ids:
        try:
            await run_consolidation(file_id, mode, dest_folder_id)
            completed += 1
        except Exception:
            errors += 1

    await broadcast(
        {
            "type": "batch_consolidate_completed",
            "completed": completed,
            "errors": errors,
            "total": len(file_ids),
        }
    )


async def drain_pending_jobs(location_id: int, root_path: str):
    """Drain queued consolidation jobs for a location that's now online.

    Called when an agent reconnects and its locations come online.
    Reads via get_db(), writes via db_writer().
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """SELECT id, source_file, source_path, destination_path
           FROM consolidation_jobs
           WHERE source_location_id = ? AND status = 'pending'""",
        (location_id,),
    )

    if not rows:
        return

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    jobs_completed = 0

    for job in rows:
        source_path = job["source_path"]
        dest_path = job["destination_path"]
        source_file = job["source_file"]

        if await fs.file_exists(source_path, location_id):
            try:
                await fs.write_moved_stub(
                    source_path, source_file, dest_path, now_iso, location_id
                )
                stub_path = source_path + ".moved"
                stub_name = source_file + ".moved"
                # Update the existing file record to reflect the .moved stub
                file_rows = await db.execute_fetchall(
                    "SELECT id, rel_path FROM files WHERE full_path = ? AND location_id = ?",
                    (source_path, location_id),
                )

                async with db_writer() as wdb:
                    if file_rows:
                        fid = file_rows[0]["id"]
                        stub_rel = file_rows[0]["rel_path"] + ".moved"
                        st = await fs.file_stat(stub_path, location_id)
                        stub_size = st["size"] if st else 0
                        await wdb.execute(
                            """UPDATE files SET
                                filename=?, full_path=?, rel_path=?,
                                file_type_high='text', file_type_low='moved',
                                file_size=?, hash_fast=NULL, hash_strong=NULL,
                                modified_date=?, date_last_seen=?
                               WHERE id=?""",
                            (
                                stub_name,
                                stub_path,
                                stub_rel,
                                stub_size,
                                now_iso,
                                now_iso,
                                fid,
                            ),
                        )
                    else:
                        await wdb.execute(
                            "DELETE FROM files WHERE full_path = ? AND location_id = ?",
                            (source_path, location_id),
                        )
                    await wdb.execute(
                        "UPDATE consolidation_jobs SET status = 'completed', date_completed = ? WHERE id = ?",
                        (now_iso, job["id"]),
                    )
                jobs_completed += 1
            except Exception:
                pass
        else:
            # File already gone — mark job completed
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE consolidation_jobs SET status = 'completed', date_completed = ? WHERE id = ?",
                    (now_iso, job["id"]),
                )
            jobs_completed += 1

    if jobs_completed > 0:
        await broadcast(
            {
                "type": "consolidate_queue_drained",
                "locationId": location_id,
                "jobsCompleted": jobs_completed,
            }
        )


async def _resolve_folder_path(db, folder_id: str) -> str | None:
    """Resolve a loc-N or fld-N identifier to an absolute filesystem path."""
    if folder_id.startswith("loc-"):
        loc_id = int(folder_id[4:])
        rows = await db.execute_fetchall(
            "SELECT root_path FROM locations WHERE id = ?", (loc_id,)
        )
        return rows[0]["root_path"] if rows else None

    elif folder_id.startswith("fld-"):
        fld_id = int(folder_id[4:])
        rows = await db.execute_fetchall(
            """SELECT f.rel_path, l.root_path
               FROM folders f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (fld_id,),
        )
        if not rows:
            return None
        return os.path.join(rows[0]["root_path"], rows[0]["rel_path"])

    return None


async def _resolve_folder_path_with_loc(
    db, folder_id: str
) -> tuple[str | None, int | None]:
    """Resolve a loc-N or fld-N to (abs_path, location_id)."""
    if folder_id.startswith("loc-"):
        loc_id = int(folder_id[4:])
        rows = await db.execute_fetchall(
            "SELECT id, root_path FROM locations WHERE id = ?", (loc_id,)
        )
        if not rows:
            return None, None
        return rows[0]["root_path"], rows[0]["id"]

    elif folder_id.startswith("fld-"):
        fld_id = int(folder_id[4:])
        rows = await db.execute_fetchall(
            """SELECT f.rel_path, f.location_id, l.root_path
               FROM folders f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (fld_id,),
        )
        if not rows:
            return None, None
        return os.path.join(rows[0]["root_path"], rows[0]["rel_path"]), rows[0][
            "location_id"
        ]

    return None, None


async def _ensure_canonical_record(
    canonical_path: str,
    dest_folder_id: str,
    dest_loc_id: int,
    source: dict,
    now_iso: str,
) -> int:
    """Insert a file record for the canonical copy at its destination."""
    from file_hunter.core import classify_file

    db = await get_db()

    # Determine location_id and folder_id from dest_folder_id
    if dest_folder_id.startswith("loc-"):
        location_id = int(dest_folder_id[4:])
        folder_id = None
        rel_path = source["filename"]
    elif dest_folder_id.startswith("fld-"):
        fld_id = int(dest_folder_id[4:])
        rows = await db.execute_fetchall(
            "SELECT id, location_id, rel_path FROM folders WHERE id = ?", (fld_id,)
        )
        if not rows:
            return -1
        folder_id = fld_id
        location_id = rows[0]["location_id"]
        rel_path = os.path.join(rows[0]["rel_path"], source["filename"])
    else:
        return -1

    type_high, type_low = classify_file(source["filename"])
    st = await fs.file_stat(canonical_path, dest_loc_id)
    file_size = st["size"] if st else source.get("file_size", 0)

    async with db_writer() as wdb:
        cursor = await wdb.execute(
            """INSERT INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                hash_fast, hash_strong, description, tags,
                created_date, modified_date, date_cataloged, date_last_seen, scan_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (
                source["filename"],
                canonical_path,
                rel_path,
                location_id,
                folder_id,
                type_high,
                type_low,
                file_size,
                source["hash_fast"],
                source["hash_strong"],
                source.get("description", ""),
                source.get("tags", ""),
                source.get("created_date", now_iso),
                source.get("modified_date", now_iso),
                now_iso,
                now_iso,
            ),
        )
        return cursor.lastrowid


async def _insert_stub_record(
    stub_path: str, sibling_file_id: int, location_id: int, now_iso: str
):
    """Insert a DB record for a .moved or .sources stub file alongside its sibling."""
    db = await get_db()
    # Get location/folder info from the sibling file record
    rows = await db.execute_fetchall(
        "SELECT location_id, folder_id, rel_path FROM files WHERE id = ?",
        (sibling_file_id,),
    )
    if not rows:
        return
    sibling = rows[0]
    stub_name = os.path.basename(stub_path)
    # rel_path for the stub: sibling's directory + stub filename
    sibling_rel = sibling["rel_path"]
    sibling_dir = os.path.dirname(sibling_rel)
    stub_rel = os.path.join(sibling_dir, stub_name) if sibling_dir else stub_name

    st = await fs.file_stat(stub_path, location_id)
    if st is None:
        return

    async with db_writer() as wdb:
        await wdb.execute(
            """INSERT OR IGNORE INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                hash_fast, hash_strong, description, tags,
                created_date, modified_date, date_cataloged, date_last_seen, scan_id)
               VALUES (?, ?, ?, ?, ?, 'text', 'sources', ?, NULL, NULL, '', '',
                       ?, ?, ?, ?, NULL)""",
            (
                stub_name,
                stub_path,
                stub_rel,
                sibling["location_id"],
                sibling["folder_id"],
                st["size"],
                now_iso,
                now_iso,
                now_iso,
                now_iso,
            ),
        )
