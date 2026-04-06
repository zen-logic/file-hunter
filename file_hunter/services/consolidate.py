"""Consolidation logic — elect canonical file, stub duplicates, write provenance."""

import logging
import os
from datetime import datetime, timezone

from file_hunter.core import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import get_file_hashes, hashes_writer, read_hashes, remove_file_hashes
from file_hunter.helpers import (
    get_effective_hash,
    get_effective_hashes,
    parse_folder_id,
    parse_mtime,
    parse_prefixed_id,
    post_op_stats,
    resolve_target,
)
from file_hunter.services import fs
from file_hunter.services.activity import (
    register as activity_register,
    unregister as activity_unregister,
    update as activity_update,
)
from file_hunter.services.dup_counts import recalculate_dup_counts
from file_hunter.services.op_result_log import add_to_catalog, append_row, create_log
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.agent import get_agent_location_ids
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


def _get_online_location_ids() -> set[int]:
    """Return the set of location_ids whose agents are currently connected."""
    online = set()
    for lids in get_agent_location_ids().values():
        online.update(lids)
    return online


def _build_stub_text(filename, dest_path, dest_loc_name, now_iso):
    """Build the text content for a .moved stub file."""
    moved_to = f"{dest_loc_name}: {dest_path}" if dest_loc_name else dest_path
    return (
        f"Consolidated by File Hunter\n"
        f"Original: {filename}\n"
        f"Moved to: {moved_to}\n"
        f"Date: {now_iso}\n"
    )


async def _stub_and_delete(copy, canonical_path, dest_loc_name, now_iso):
    """Write .moved stub, delete original, update DB for a single duplicate.

    Per-file sequence:
        1. Write .moved stub text file (agent)
        2. Delete original file (agent)
        3. Update DB record to stub, remove hashes, update stats

    Returns:
        "stubbed" on success, raises on failure.
    """
    original_path = copy["full_path"]
    copy_loc_id = copy["location_id"]

    stub_text = _build_stub_text(
        copy["filename"], canonical_path, dest_loc_name, now_iso
    )
    stub_path = original_path + ".moved"
    stub_name = copy["filename"] + ".moved"
    stub_rel = copy["rel_path"] + ".moved"
    stub_size = len(stub_text.encode())

    # 1. Write stub
    await fs.file_write_text(stub_path, stub_text, copy_loc_id)

    # 2. Delete original
    await fs.file_delete(original_path, copy_loc_id)

    # 3. Update DB
    async with db_writer() as wdb:
        await wdb.execute(
            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
            (copy_loc_id, stub_rel, copy["id"]),
        )
        await wdb.execute(
            """UPDATE files SET
                filename=?, full_path=?, rel_path=?,
                file_type_high='text', file_type_low='moved',
                file_size=?,
                modified_date=?, date_last_seen=?
               WHERE id=?""",
            (stub_name, stub_path, stub_rel, stub_size, now_iso, now_iso, copy["id"]),
        )
    await remove_file_hashes([copy["id"]])
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
    every other copy of the same hash is stubbed on disk (.moved) and deleted.
    Offline copies are queued in consolidation_jobs for later processing.

    Per-duplicate sequence:
        1. Write .moved stub (agent)
        2. Delete original (agent)
        3. Update DB: record to stub, remove hashes, update stats

    Args:
        file_id: The DB id of the file chosen as the canonical copy.
        mode: 'keep_here' or 'copy_to'.
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') for copy_to.
        shared_csv_path: Shared result-log CSV from batch consolidation.
        shared_csv_loc_id: Location id for shared CSV.
        skip_post_processing: Skip dup-count recalc (batch does it once).
        filename_match_only: Only consolidate copies with the same filename.

    Called by:
        routes/consolidate.py (single), run_batch_consolidation (batch).
    """
    effective_hash = None
    filename = None
    selected_loc_id = None
    dest_loc_id = None
    stubs_written = 0
    stubs_queued = 0
    act_name = f"consolidate_{file_id}"
    activity_register(act_name, "Consolidating")
    try:
        async with read_db() as db:
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

        # Carry hash_partial for destination record
        _h_map = await get_file_hashes([file_id])
        selected["hash_partial"] = _h_map.get(file_id, {}).get("hash_partial")

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
                "locationId": selected_loc_id,
            }
        )

        # Find ALL copies by effective hash from hashes.db
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
                   WHERE f.id IN ({ph}) AND f.stale = 0""",
                copy_ids,
            )
        all_copies = [dict(r) for r in all_copies]

        # Filter to matching filenames only if requested
        if filename_match_only:
            all_copies = [c for c in all_copies if c["filename"] == filename]

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        # Merge tags and find earliest modified_date across all copies
        all_tags: set[str] = set()
        earliest_modified = None
        for copy in all_copies:
            raw = copy.get("tags") or ""
            for t in raw.split(","):
                t = t.strip()
                if t:
                    all_tags.add(t)
            md = copy.get("modified_date")
            if md and (earliest_modified is None or md < earliest_modified):
                earliest_modified = md
        merged_tags = ",".join(sorted(all_tags))

        if mode == "keep_here":
            canonical_path = selected["full_path"]
            canonical_id = file_id
            dest_loc_id = selected_loc_id

            # Apply merged tags and earliest modified_date to canonical
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE files SET tags = ?, modified_date = ? WHERE id = ?",
                    (
                        merged_tags,
                        earliest_modified or selected["modified_date"],
                        canonical_id,
                    ),
                )

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

            # Check if a copy already lives at the destination — use it as
            # canonical instead of copying a duplicate
            dest_file_path = os.path.join(dest_dir, filename)
            existing_at_dest = next(
                (
                    c
                    for c in all_copies
                    if c["full_path"] == dest_file_path
                    and c["location_id"] == dest_loc_id
                ),
                None,
            )

            if existing_at_dest:
                # A copy already exists at the destination — promote it
                canonical_path = existing_at_dest["full_path"]
                canonical_id = existing_at_dest["id"]

                # Apply merged tags and earliest modified_date
                async with db_writer() as wdb:
                    await wdb.execute(
                        "UPDATE files SET tags = ?, modified_date = ? WHERE id = ?",
                        (
                            merged_tags,
                            earliest_modified or existing_at_dest["modified_date"],
                            canonical_id,
                        ),
                    )
            else:
                # Name collision: check catalog instead of filesystem
                canonical_path = dest_file_path
                async with read_db() as db:
                    dest_rel_paths = {
                        r["rel_path"].lower()
                        for r in await db.execute_fetchall(
                            "SELECT rel_path FROM files "
                            "WHERE location_id = ? AND stale = 0",
                            (dest_loc_id,),
                        )
                    }
                target = (
                    await resolve_target(db, dest_folder_id) if dest_folder_id else None
                )
                if target:
                    check_rel = (
                        os.path.join(target.get("rel_path", ""), filename)
                        if target.get("rel_path")
                        else filename
                    )
                else:
                    check_rel = filename

                if check_rel.lower() in dest_rel_paths:
                    base, ext = os.path.splitext(filename)
                    counter = 1
                    while check_rel.lower() in dest_rel_paths:
                        new_name = f"{base}_{counter}{ext}"
                        check_rel = (
                            os.path.join(target.get("rel_path", ""), new_name)
                            if target and target.get("rel_path")
                            else new_name
                        )
                        counter += 1
                    actual_filename = os.path.basename(check_rel)
                    canonical_path = os.path.join(dest_dir, actual_filename)
                    selected["filename"] = actual_filename
                    filename = actual_filename

                # Find a source copy on an online location
                online_loc_ids = _get_online_location_ids()
                source_path = None
                source_loc_id = None
                for copy in all_copies:
                    if copy["location_id"] in online_loc_ids:
                        source_path = copy["full_path"]
                        source_loc_id = copy["location_id"]
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

                # Copy with progress
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
                        mtime=parse_mtime(
                            earliest_modified or selected["modified_date"]
                        ),
                    )
                except Exception as copy_exc:
                    try:
                        await fs.file_delete(canonical_path, dest_loc_id)
                    except Exception:
                        pass
                    raise RuntimeError(f"Copy failed: {copy_exc}") from copy_exc

                # Hash-verify: compare hash_fast (same algorithm both sides)
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
                selected["tags"] = merged_tags
                selected["modified_date"] = (
                    earliest_modified or selected["modified_date"]
                )

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

        # Write .sources file next to canonical — single append call
        try:
            sources_entries = "".join(
                f"- {c['location_name']}: {c['rel_path']}\n" for c in all_copies
            )
            await fs.file_write_text(
                canonical_path + ".sources",
                sources_entries,
                dest_loc_id,
                append=True,
            )
            await _upsert_sources_record(
                canonical_path, canonical_id, dest_loc_id, sources_entries, now_iso
            )
        except Exception as e:
            logger.warning("Could not write .sources file: %s", e)

        # Result log CSV — use shared one from batch, or create per-file
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

        # Build online location set once
        online_loc_ids = _get_online_location_ids()

        for idx, copy in enumerate(duplicates, 1):
            original_path = copy["full_path"]
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

            # Check if location is online via agent registry
            if copy_loc_id not in online_loc_ids:
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

            # Try stub + delete; handle failures
            try:
                await _stub_and_delete(copy, canonical_path, dest_loc_name, now_iso)
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
            except FileNotFoundError:
                # File missing on disk — queue for later
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
            except PermissionError:
                # Read-only — log and continue
                await append_row(
                    csv_path,
                    csv_loc_id,
                    copy["location_name"],
                    original_path,
                    dest_loc_name,
                    canonical_path,
                    "stub failed (read-only)",
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

        # Add result CSV to catalog (only if this consolidation owns it)
        if owns_csv:
            await add_to_catalog(csv_path, csv_loc_id, csv_folder_id)

        await broadcast(
            {
                "type": "consolidate_completed",
                "fileId": canonical_id,
                "filename": filename,
                "canonicalPath": canonical_path,
                "destLocationId": dest_loc_id,
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
                "destLocationId": dest_loc_id or selected_loc_id,
                "error": str(exc),
                "stubsWritten": stubs_written,
                "stubsQueued": stubs_queued,
            }
        )

    finally:
        activity_unregister(act_name)
        if effective_hash:
            _active_consolidations.discard(effective_hash)


async def run_batch_consolidation(
    file_ids: list[int],
    mode: str,
    dest_folder_id: str | None,
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
        filename_match_only: Only consolidate copies with the same filename.

    Called by:
        routes/consolidate.py (via asyncio.create_task).
    """
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

    # Deduplicate by effective hash — multiple IDs sharing a hash are the
    # same file; consolidating the first one stubs the rest.
    hash_map = await get_effective_hashes(file_ids)
    seen_hashes: set[str] = set()
    unique_ids: list[int] = []
    for fid in file_ids:
        eff_hash, _ = hash_map.get(fid, (None, None))
        if eff_hash and eff_hash not in seen_hashes:
            seen_hashes.add(eff_hash)
            unique_ids.append(fid)
        elif not eff_hash:
            unique_ids.append(fid)

    skipped = len(file_ids) - len(unique_ids)
    completed = 0
    errors = 0
    batch_act = f"batch_consolidate_{len(file_ids)}"
    activity_register(batch_act, f"Batch consolidate ({len(unique_ids)} files)")

    for file_id in unique_ids:
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
        activity_update(batch_act, progress=f"{completed + errors}/{len(unique_ids)}")

    activity_unregister(batch_act)
    await add_to_catalog(csv_path, dest_loc_id, csv_folder_id)

    # Post-processing once for the entire batch
    await post_op_stats(
        location_ids={dest_loc_id},
        source=f"batch consolidate ({len(file_ids)} files)",
    )
    await recalculate_dup_counts(source=f"batch consolidate ({len(file_ids)} files)")

    await broadcast(
        {
            "type": "batch_consolidate_completed",
            "completed": completed,
            "skipped": skipped,
            "errors": errors,
            "total": len(file_ids),
        }
    )


async def drain_pending_jobs(location_id: int, root_path: str):
    """Process pending consolidation_jobs for a location that has come back online.

    For each pending job: try stub+delete. If the file is already gone,
    mark the job completed. Errors are logged per-job.

    Args:
        location_id: The DB id of the location whose jobs should be drained.
        root_path: The filesystem root path of the location.

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

        try:
            # Build a stub text for this job
            stub_text = _build_stub_text(source_file, dest_path, "", now_iso)
            stub_path = source_path + ".moved"
            stub_name = source_file + ".moved"
            stub_size = len(stub_text.encode())

            # Write stub, delete original
            await fs.file_write_text(stub_path, stub_text, location_id)
            await fs.file_delete(source_path, location_id)

            # Update DB record
            async with read_db() as rdb:
                file_rows = await rdb.execute_fetchall(
                    "SELECT id, rel_path, folder_id, file_size, file_type_high "
                    "FROM files WHERE full_path = ? AND location_id = ?",
                    (source_path, location_id),
                )

            async with db_writer() as wdb:
                if file_rows:
                    fid = file_rows[0]["id"]
                    stub_rel = file_rows[0]["rel_path"] + ".moved"
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
                    await remove_file_hashes([fid])
                    await update_stats_for_files(
                        location_id,
                        removed=[
                            (
                                file_rows[0]["folder_id"],
                                file_rows[0]["file_size"] or 0,
                                file_rows[0]["file_type_high"],
                                0,
                            )
                        ],
                        added=[(file_rows[0]["folder_id"], stub_size, "text", 0)],
                    )
                else:
                    # No file record — just clean up any orphan
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
                        await remove_file_hashes([r["id"] for r in del_rows])
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
                    "UPDATE consolidation_jobs SET status = 'completed', "
                    "date_completed = ? WHERE id = ?",
                    (now_iso, job["id"]),
                )
            jobs_completed += 1

        except FileNotFoundError:
            # File already gone — mark job completed
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE consolidation_jobs SET status = 'completed', "
                    "date_completed = ? WHERE id = ?",
                    (now_iso, job["id"]),
                )
            jobs_completed += 1

        except Exception:
            logger.warning(
                "drain_pending_jobs: failed for %s", source_path, exc_info=True
            )

    if jobs_completed > 0:
        await broadcast(
            {
                "type": "consolidate_queue_drained",
                "locationId": location_id,
                "jobsCompleted": jobs_completed,
            }
        )


async def _resolve_folder_path_with_loc(
    db, folder_id: str
) -> tuple[str | None, int | None]:
    """Resolve a prefixed folder identifier to its absolute path and location id."""
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
    """Insert a files-table record for the newly copied canonical file.

    Uses source file_size (verified by hash) instead of agent file_stat.
    """
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
    file_size = source.get("file_size", 0) or 0

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

    # Register hashes for the new file record
    hash_partial = source.get("hash_partial")
    hash_fast = source.get("hash_fast")
    hash_strong = source.get("hash_strong")
    if hash_fast or hash_strong:
        async with hashes_writer() as hdb:
            await hdb.execute(
                "INSERT INTO file_hashes "
                "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(file_id) DO UPDATE SET "
                "hash_partial=excluded.hash_partial, "
                "hash_fast=excluded.hash_fast, "
                "hash_strong=excluded.hash_strong",
                (new_id, location_id, file_size, hash_partial, hash_fast, hash_strong),
            )

    return new_id


async def _upsert_sources_record(
    canonical_path, canonical_id, dest_loc_id, sources_text, now_iso
):
    """Insert or update the .sources file's DB record.

    Uses len(sources_text) as file size — exact for new files, approximate
    after multiple appends.
    """
    sources_path = canonical_path + ".sources"
    sources_name = os.path.basename(sources_path)
    sources_size = len(sources_text.encode())

    # Get folder info from the canonical file
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT folder_id, rel_path FROM files WHERE id = ?",
            (canonical_id,),
        )
    if not rows:
        return
    folder_id = rows[0]["folder_id"]
    rel_dir = os.path.dirname(rows[0]["rel_path"])
    sources_rel = os.path.join(rel_dir, sources_name) if rel_dir else sources_name

    async with read_db() as db:
        existing = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
            (dest_loc_id, sources_rel),
        )

    async with db_writer() as wdb:
        if existing:
            await wdb.execute(
                "UPDATE files SET modified_date=?, date_last_seen=? WHERE id=?",
                (now_iso, now_iso, existing[0]["id"]),
            )
        else:
            await wdb.execute(
                """INSERT OR IGNORE INTO files
                   (filename, full_path, rel_path, location_id, folder_id,
                    file_type_high, file_type_low, file_size,
                    description, tags,
                    created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                   VALUES (?, ?, ?, ?, ?, 'text', 'sources', ?, '', '',
                           ?, ?, ?, ?, NULL)""",
                (
                    sources_name,
                    sources_path,
                    sources_rel,
                    dest_loc_id,
                    folder_id,
                    sources_size,
                    now_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
