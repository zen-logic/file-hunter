"""Consolidation logic — copy/keep canonical file, write .moved stubs and .sources metadata."""

import logging
import os
from datetime import datetime, timezone

from file_hunter.db import db_writer, read_db
from file_hunter.helpers import (
    get_effective_hash,
    parse_folder_id,
    parse_mtime,
    parse_prefixed_id,
    post_op_stats,
    resolve_target,
)
from file_hunter.services import fs
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# Guard against concurrent consolidation of the same hash
_active_consolidations: set[str] = set()


def is_consolidation_running(hash_value: str) -> bool:
    """Check whether a consolidation is already in progress for a given file hash.

    Args:
        hash_value: The effective hash (hash_strong or hash_fast) to check.

    Returns:
        True if a consolidation task is currently running for this hash.

    Called by:
        routes/consolidate.py (pre-flight check before launching a task).
    """
    return hash_value in _active_consolidations


async def run_consolidation(
    file_id: int,
    mode: str,
    dest_folder_id: str | None,
    shared_csv_path: str | None = None,
    shared_csv_loc_id: int | None = None,
    skip_post_processing: bool = False,
    filename_match_only: bool = False,
):
    """Consolidate duplicate files by electing a canonical copy and stubbing the rest.

    For 'keep_here' mode, the selected file stays in place as the canonical.
    For 'copy_to' mode, the selected file is copied to a destination folder,
    verified by hash, and a new DB record is created there. In both modes,
    every other copy of the same hash is replaced on disk with a .moved stub
    and its DB record is updated accordingly. Offline or missing copies are
    queued in consolidation_jobs for later processing.

    Args:
        file_id: The DB id of the file chosen as the canonical copy.
        mode: 'keep_here' (leave canonical where it is) or 'copy_to' (copy
            canonical to dest_folder_id first).
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') of the destination.
            Required for 'copy_to', ignored for 'keep_here'.
        shared_csv_path: If set, append to this shared result-log CSV instead
            of creating a new one. Used by run_batch_consolidation.
        shared_csv_loc_id: Location id for the shared CSV. Required when
            shared_csv_path is set.
        skip_post_processing: If True, skip dup-count recalculation and
            stats refresh. Used by run_batch_consolidation which does a
            single post-processing pass at the end.

    Returns:
        None.

    Side effects:
        - DB writes: updates file records to .moved stubs, inserts canonical
          record (copy_to), inserts .sources record, inserts consolidation_jobs
          for offline copies. Writes via db_writer().
        - Hashes DB: removes hashes for stubbed files, inserts hashes for new
          canonical (copy_to). Via hashes_writer().
        - Stats DB: adjusts folder/location stats for every stubbed file.
        - File I/O: copies file (copy_to), writes .moved stubs, writes
          .sources metadata file, deletes original files via agent fs calls.
        - Broadcasts: consolidate_started, consolidate_progress,
          consolidate_completed, consolidate_error via WebSocket.
        - Result log: creates or appends to a CSV operation log.
        - Concurrency guard: adds/removes effective_hash in
          _active_consolidations.
        - Post-processing: recalculates dup counts and location stats unless
          skip_post_processing is True.

    Called by:
        routes/consolidate.py (single file, via asyncio.create_task),
        run_batch_consolidation (batch, with shared CSV and deferred post-processing).
    """
    effective_hash = None
    filename = None
    stubs_written = 0
    stubs_queued = 0
    try:
        async with read_db() as db:
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
        selected_loc_id = selected["location_id"]

        effective_hash, hash_col = await get_effective_hash(file_id)

        if not effective_hash:
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
        if effective_hash in _active_consolidations:
            await broadcast(
                {
                    "type": "consolidate_error",
                    "fileId": file_id,
                    "filename": filename,
                    "error": "Consolidation already in progress for this file.",
                }
            )
            return
        _active_consolidations.add(effective_hash)

        await broadcast(
            {
                "type": "consolidate_started",
                "fileId": file_id,
                "filename": filename,
            }
        )

        # Find ALL copies by effective hash from hashes.db
        from file_hunter.hashes_db import read_hashes

        async with read_hashes() as hdb:
            dup_rows = await hdb.execute_fetchall(
                f"SELECT file_id FROM active_hashes WHERE {hash_col} = ?",
                (effective_hash,),
            )
        copy_ids = [r["file_id"] for r in dup_rows]

        async with read_db() as db:
            ph = ",".join("?" for _ in copy_ids)
            all_copies = await db.execute_fetchall(
                f"""SELECT f.id, f.filename, f.full_path, f.rel_path,
                          f.location_id, f.folder_id, f.dup_exclude,
                          f.file_type_high, f.file_type_low, f.file_size,
                          f.description, f.tags,
                          f.created_date, f.modified_date, f.date_cataloged,
                          l.name as location_name, l.root_path
                   FROM files f
                   JOIN locations l ON l.id = f.location_id
                   WHERE f.id IN ({ph})""",
                copy_ids,
            )
        all_copies = [dict(r) for r in all_copies]

        # Filter to matching filenames only if requested
        if filename_match_only:
            all_copies = [c for c in all_copies if c["filename"] == filename]

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

        elif mode == "copy_to":
            # Resolve destination path and location_id
            async with read_db() as db:
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

            canonical_path = await fs.unique_dest_path(
                os.path.join(dest_dir, filename), dest_loc_id
            )
            # Update filename if collision rename happened
            actual_filename = os.path.basename(canonical_path)
            if actual_filename != filename:
                selected["filename"] = actual_filename
                filename = actual_filename

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
                    mtime=parse_mtime(selected["modified_date"]),
                )
            except Exception as copy_exc:
                # Clean up partial file at destination
                try:
                    await fs.file_delete(canonical_path, dest_loc_id)
                except Exception:
                    pass
                raise RuntimeError(f"Copy failed: {copy_exc}") from copy_exc

            # Hash-verify the copy against source using hash_fast
            await broadcast(
                {
                    "type": "consolidate_progress",
                    "fileId": file_id,
                    "filename": filename,
                    "phase": "verifying",
                }
            )
            (source_hash_fast,) = await fs.file_hash(source_path, source_loc_id)
            (copy_hash_fast,) = await fs.file_hash(canonical_path, dest_loc_id)
            if copy_hash_fast != source_hash_fast:
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

            selected["hash_fast"] = source_hash_fast

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

        # Write .sources file next to canonical (best-effort)
        try:
            await fs.write_sources_file(
                canonical_path, all_copies, now_iso, dest_loc_id
            )
            await _insert_stub_record(
                canonical_path + ".sources",
                canonical_id,
                dest_loc_id,
                now_iso,
            )
        except Exception as e:
            logger.warning("Could not write .sources file: %s", e)

        # Result log CSV — use shared one from batch, or create per-file
        from file_hunter.services.op_result_log import (
            create_log,
            append_row,
            add_to_catalog,
        )

        owns_csv = shared_csv_path is None
        if owns_csv:
            dest_dir = os.path.dirname(canonical_path)
            csv_path = await create_log(dest_dir, dest_loc_id, "consolidate")
            csv_loc_id = dest_loc_id
            csv_folder_id = None
            if mode == "keep_here":
                csv_folder_id = selected.get("folder_id")
            elif mode == "copy_to":
                kind, _ = parse_prefixed_id(dest_folder_id)
                if kind == "fld":
                    csv_folder_id = parse_folder_id(dest_folder_id)
        else:
            csv_path = shared_csv_path
            csv_loc_id = shared_csv_loc_id
            csv_folder_id = None  # batch manages catalog entry

        # Resolve destination location name
        if mode == "keep_here":
            dest_loc_name = selected["location_name"]
        else:
            async with read_db() as db:
                _ln = await db.execute_fetchall(
                    "SELECT name FROM locations WHERE id = ?", (dest_loc_id,)
                )
            dest_loc_name = _ln[0]["name"] if _ln else ""

        # Log the canonical file
        await append_row(
            csv_path,
            csv_loc_id,
            selected["location_name"],
            canonical_path,
            dest_loc_name,
            canonical_path,
            "canonical",
        )

        # Process each duplicate (skip canonical and dup-excluded files)
        duplicates = [
            c for c in all_copies if c["id"] != canonical_id and not c["dup_exclude"]
        ]
        total_dups = len(duplicates)
        writable_cache: dict[str, bool] = {}

        for idx, copy in enumerate(duplicates, 1):
            original_path = copy["full_path"]
            loc_root = copy["root_path"]
            copy_loc_id = copy["location_id"]

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

            # Check if location is online
            try:
                location_online = await fs.dir_exists(loc_root, copy_loc_id)
            except ConnectionError:
                location_online = False

            if not location_online:
                # Offline — queue for later
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
                await append_row(
                    csv_path,
                    csv_loc_id,
                    copy["location_name"],
                    original_path,
                    dest_loc_name,
                    canonical_path,
                    "offline - queued",
                )
                continue

            file_on_disk = await fs.file_exists(original_path, copy_loc_id)
            if not file_on_disk:
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
                await append_row(
                    csv_path,
                    csv_loc_id,
                    copy["location_name"],
                    original_path,
                    dest_loc_name,
                    canonical_path,
                    "file missing - queued",
                )
                continue

            # Check folder writability (cached per directory)
            copy_dir = os.path.dirname(original_path)
            if copy_dir not in writable_cache:
                try:
                    test_path = os.path.join(copy_dir, ".fh-write-test")
                    await fs.file_write_text(test_path, "", copy_loc_id)
                    await fs.file_delete(test_path, copy_loc_id)
                    writable_cache[copy_dir] = True
                except Exception:
                    writable_cache[copy_dir] = False

            if writable_cache[copy_dir]:
                try:
                    await fs.write_moved_stub(
                        original_path,
                        copy["filename"],
                        canonical_path,
                        now_iso,
                        copy_loc_id,
                        dest_location_name=dest_loc_name,
                    )
                    # Replace the original file record with the .moved stub
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
                                file_size=?,
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
                    from file_hunter.hashes_db import remove_file_hashes

                    await remove_file_hashes([copy["id"]])

                    from file_hunter.stats_db import update_stats_for_files

                    await update_stats_for_files(
                        copy_loc_id,
                        removed=[
                            (
                                copy["folder_id"],
                                copy["file_size"] or 0,
                                copy["file_type_high"],
                                0,
                            )
                        ],
                        added=[(copy["folder_id"], stub_size, "text", 0)],
                    )
                    stubs_written += 1
                    await append_row(
                        csv_path,
                        csv_loc_id,
                        copy["location_name"],
                        original_path,
                        dest_loc_name,
                        canonical_path,
                        "stubbed",
                    )
                except Exception as e:
                    logger.warning("Stub write failed for %s: %s", original_path, e)
                    await append_row(
                        csv_path,
                        csv_loc_id,
                        copy["location_name"],
                        original_path,
                        dest_loc_name,
                        canonical_path,
                        "stub failed",
                        str(e),
                    )
            else:
                # Read-only folder — skip stub, don't queue
                await append_row(
                    csv_path,
                    csv_loc_id,
                    copy["location_name"],
                    original_path,
                    dest_loc_name,
                    canonical_path,
                    "stub failed (read-only)",
                    copy_dir,
                )

        # Add result CSV to catalog (only if this consolidation owns it)
        if owns_csv:
            await add_to_catalog(csv_path, csv_loc_id, csv_folder_id)

        await broadcast(
            {
                "type": "consolidate_completed",
                "fileId": canonical_id,
                "filename": filename,
                "canonicalPath": canonical_path,
                "stubsWritten": stubs_written,
                "stubsQueued": stubs_queued,
                "batch": not owns_csv,
            }
        )
        if not skip_post_processing:
            recalc_strong = set()
            recalc_fast = set()
            if selected.get("hash_strong"):
                recalc_strong.add(selected["hash_strong"])
            if hash_col == "hash_fast":
                recalc_fast.add(effective_hash)
            affected_loc_ids = {c["location_id"] for c in all_copies}
            await post_op_stats(
                location_ids=affected_loc_ids,
                strong_hashes=recalc_strong or None,
                fast_hashes=recalc_fast or None,
                source=f"consolidate {filename}",
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
        if effective_hash:
            _active_consolidations.discard(effective_hash)


async def run_batch_consolidation(
    file_ids: list[int], mode: str, dest_folder_id: str | None,
    filename_match_only: bool = False,
):
    """Run consolidation for multiple files sequentially, sharing a single result log.

    Creates a shared CSV result log, then calls run_consolidation for each file
    with skip_post_processing=True. After all files are processed, performs a
    single dup-count recalculation and stats refresh for the batch.

    Args:
        file_ids: List of DB file ids to consolidate (each becomes a canonical).
        mode: 'keep_here' or 'copy_to' (passed through to run_consolidation).
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') for copy_to mode.

    Returns:
        None.

    Side effects:
        - All side effects of run_consolidation (per file).
        - Creates a shared CSV result log and adds it to the file catalog.
        - Recalculates dup counts once for the entire batch.
        - Broadcasts batch_consolidate_completed with totals.

    Called by:
        routes/consolidate.py (via asyncio.create_task).
    """
    from file_hunter.services.op_result_log import (
        create_log,
        add_to_catalog,
    )

    # Resolve destination for the shared CSV
    dest_dir = None
    dest_loc_id = None
    csv_folder_id = None
    if dest_folder_id:
        async with read_db() as db:
            d, lid = await _resolve_folder_path_with_loc(db, dest_folder_id)
        dest_dir = d
        dest_loc_id = lid
        kind, _ = parse_prefixed_id(dest_folder_id)
        if kind == "fld":
            csv_folder_id = parse_folder_id(dest_folder_id)

    if not dest_dir and file_ids:
        # keep_here mode — resolve from first file
        async with read_db() as db:
            row = await db.execute_fetchall(
                """SELECT f.full_path, f.folder_id, f.location_id
                   FROM files f WHERE f.id = ?""",
                (file_ids[0],),
            )
        if row:
            dest_dir = os.path.dirname(row[0]["full_path"])
            dest_loc_id = row[0]["location_id"]
            csv_folder_id = row[0]["folder_id"]

    if not dest_dir or not dest_loc_id:
        logger.error("Batch consolidation: could not resolve destination")
        return

    csv_path = await create_log(dest_dir, dest_loc_id, "consolidate")

    completed = 0
    errors = 0

    for file_id in file_ids:
        try:
            await run_consolidation(
                file_id,
                mode,
                dest_folder_id,
                shared_csv_path=csv_path,
                shared_csv_loc_id=dest_loc_id,
                skip_post_processing=True,
                filename_match_only=filename_match_only,
            )
            completed += 1
        except Exception:
            errors += 1

    await add_to_catalog(csv_path, dest_loc_id, csv_folder_id)

    # Post-processing once for the entire batch
    from file_hunter.services.dup_counts import recalculate_dup_counts

    await post_op_stats(
        location_ids={dest_loc_id},
        source=f"batch consolidate ({len(file_ids)} files)",
    )
    await recalculate_dup_counts(source=f"batch consolidate ({len(file_ids)} files)")

    await broadcast(
        {
            "type": "batch_consolidate_completed",
            "completed": completed,
            "errors": errors,
            "total": len(file_ids),
        }
    )


async def drain_pending_jobs(location_id: int, root_path: str):
    """Process pending consolidation_jobs for a location that has come back online.

    Iterates all 'pending' jobs for the given location. For each job, if the
    source file still exists on disk, writes a .moved stub and updates the DB
    record (or deletes it if no record found). If the source file is already
    gone, marks the job as completed. Errors are silently swallowed per-job.

    Args:
        location_id: The DB id of the location whose jobs should be drained.
        root_path: The filesystem root path of the location (used for
            online-check context, not directly in queries).

    Returns:
        None.

    Side effects:
        - DB writes: updates file records to .moved stubs or deletes them,
          marks consolidation_jobs as 'completed'. Via db_writer().
        - Hashes DB: removes hashes for stubbed/deleted files.
        - Stats DB: adjusts folder/location stats for affected files.
        - File I/O: writes .moved stub files via agent fs calls.
        - Broadcasts: consolidate_queue_drained with job count.

    Called by:
        ws/agent.py when an agent reconnects and its locations come online.
    """
    async with read_db() as db:
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
                    source_path,
                    source_file,
                    dest_path,
                    now_iso,
                    location_id,
                )
                stub_path = source_path + ".moved"
                stub_name = source_file + ".moved"
                # Update the existing file record to reflect the .moved stub
                async with read_db() as db:
                    file_rows = await db.execute_fetchall(
                        "SELECT id, rel_path, folder_id, file_size, file_type_high "
                        "FROM files WHERE full_path = ? AND location_id = ?",
                        (source_path, location_id),
                    )

                async with db_writer() as wdb:
                    if file_rows:
                        fid = file_rows[0]["id"]
                        stub_rel = file_rows[0]["rel_path"] + ".moved"
                        orig_folder_id = file_rows[0]["folder_id"]
                        orig_size = file_rows[0]["file_size"] or 0
                        orig_type = file_rows[0]["file_type_high"]
                        st = await fs.file_stat(stub_path, location_id)
                        stub_size = st["size"] if st else 0
                        await wdb.execute(
                            """UPDATE files SET
                                filename=?, full_path=?, rel_path=?,
                                file_type_high='text', file_type_low='moved',
                                file_size=?,
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
                        from file_hunter.hashes_db import remove_file_hashes

                        await remove_file_hashes([fid])
                        # Stats: original removed, stub added
                        from file_hunter.stats_db import update_stats_for_files

                        await update_stats_for_files(
                            location_id,
                            removed=[(orig_folder_id, orig_size, orig_type, 0)],
                            added=[(orig_folder_id, stub_size, "text", 0)],
                        )
                    else:
                        del_rows = await wdb.execute_fetchall(
                            "SELECT id, folder_id, file_size, file_type_high "
                            "FROM files WHERE full_path = ? AND location_id = ?",
                            (source_path, location_id),
                        )
                        await wdb.execute(
                            "DELETE FROM files WHERE full_path = ? AND location_id = ?",
                            (source_path, location_id),
                        )
                        if del_rows:
                            from file_hunter.hashes_db import remove_file_hashes

                            await remove_file_hashes([r["id"] for r in del_rows])
                            from file_hunter.stats_db import update_stats_for_files

                            await update_stats_for_files(
                                location_id,
                                removed=[
                                    (
                                        r["folder_id"],
                                        r["file_size"] or 0,
                                        r["file_type_high"],
                                        0,
                                    )
                                    for r in del_rows
                                ],
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
    """Resolve a prefixed folder identifier to an absolute filesystem path.

    Args:
        db: An open aiosqlite read connection.
        folder_id: Prefixed identifier ('loc-N' or 'fld-N').

    Returns:
        The absolute path as a str, or None if the identifier cannot be resolved.

    Called by:
        Not currently referenced (superseded by _resolve_folder_path_with_loc).
    """
    target = await resolve_target(db, folder_id)
    return target["abs_path"] if target else None


async def _resolve_folder_path_with_loc(
    db, folder_id: str
) -> tuple[str | None, int | None]:
    """Resolve a prefixed folder identifier to its absolute path and location id.

    Args:
        db: An open aiosqlite read connection.
        folder_id: Prefixed identifier ('loc-N' or 'fld-N').

    Returns:
        Tuple of (abs_path, location_id), or (None, None) if not found.

    Called by:
        run_consolidation (to resolve copy_to destination),
        run_batch_consolidation (to resolve shared CSV destination),
        routes/consolidate.py (pre-flight destination validation).
    """
    target = await resolve_target(db, folder_id)
    if not target:
        return None, None
    return target["abs_path"], target["location_id"]


async def _ensure_canonical_record(
    canonical_path: str,
    dest_folder_id: str,
    dest_loc_id: int,
    source: dict,
    now_iso: str,
) -> int:
    """Insert a files-table record for the newly copied canonical file at its destination.

    Resolves the destination folder hierarchy, classifies the file type,
    stats the file on disk for size, inserts into the files table, and
    registers hashes in hashes.db.

    Args:
        canonical_path: Absolute path where the canonical copy now lives.
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') of the destination.
        dest_loc_id: Location id of the destination (for fs calls).
        source: Dict of the original file's metadata (filename, hash_fast,
            hash_strong, description, tags, dates, file_size, etc.).
        now_iso: ISO-8601 UTC timestamp for date_cataloged / date_last_seen.

    Returns:
        The new file id (int), or -1 if the destination could not be resolved.

    Side effects:
        - DB write: INSERT into files table via db_writer().
        - Hashes DB: INSERT/upsert into file_hashes via hashes_writer().
        - File I/O: stats the canonical file via fs.file_stat.

    Called by:
        run_consolidation (copy_to mode only, after file copy and verification).
    """
    from file_hunter.core import classify_file

    # Determine location_id and folder_id from dest_folder_id
    async with read_db() as db:
        target = await resolve_target(db, dest_folder_id)
    if not target:
        return -1
    location_id = target["location_id"]
    folder_id = target["folder_id"]
    if target["kind"] == "loc":
        rel_path = source["filename"]
    else:
        rel_path = os.path.join(target["rel_path"], source["filename"])

    type_high, type_low = classify_file(source["filename"])
    st = await fs.file_stat(canonical_path, dest_loc_id)
    file_size = st["size"] if st else source.get("file_size", 0)

    async with db_writer() as wdb:
        cursor = await wdb.execute(
            """INSERT INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen, scan_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (
                source["filename"],
                canonical_path,
                rel_path,
                location_id,
                folder_id,
                type_high,
                type_low,
                file_size,
                source.get("description", ""),
                source.get("tags", ""),
                source.get("created_date", now_iso),
                source.get("modified_date", now_iso),
                now_iso,
                now_iso,
            ),
        )
        new_id = cursor.lastrowid

    # Register hashes in hashes.db for the new file record
    hash_fast = source.get("hash_fast")
    hash_strong = source.get("hash_strong")
    if hash_fast or hash_strong:
        from file_hunter.hashes_db import hashes_writer

        async with hashes_writer() as hdb:
            await hdb.execute(
                "INSERT INTO file_hashes "
                "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                "VALUES (?, ?, ?, NULL, ?, ?) "
                "ON CONFLICT(file_id) DO UPDATE SET "
                "hash_fast=excluded.hash_fast, hash_strong=excluded.hash_strong",
                (new_id, location_id, file_size, hash_fast, hash_strong),
            )

    return new_id


async def _insert_stub_record(
    stub_path: str, sibling_file_id: int, location_id: int, now_iso: str
):
    """Insert a DB record for a .sources metadata file alongside its sibling file.

    Looks up the sibling file's location/folder/rel_path to derive the stub's
    placement, stats the stub on disk, and inserts a new files record with
    file_type 'text/sources'. Uses INSERT OR IGNORE to avoid duplicates.

    Args:
        stub_path: Absolute path to the .sources file on disk.
        sibling_file_id: DB id of the file this stub sits beside (used to
            derive location_id, folder_id, and relative path).
        location_id: Location id for the fs.file_stat call.
        now_iso: ISO-8601 UTC timestamp for all date columns.

    Returns:
        None. Silently returns if the sibling is not found or the stub file
        does not exist on disk.

    Side effects:
        - DB write: INSERT OR IGNORE into files table via db_writer().

    Called by:
        run_consolidation (to catalog the .sources file next to the canonical).
    """
    async with read_db() as db:
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
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen, scan_id)
               VALUES (?, ?, ?, ?, ?, 'text', 'sources', ?, '', '',
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
