"""Consolidation logic — elect canonical file, stub duplicates, write provenance."""

import logging
import os

from file_hunter.core import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import get_file_hashes, read_hashes, remove_file_hashes, set_file_hashes
from file_hunter.helpers import (
    catalog_rel_paths,
    get_effective_hash,
    get_effective_hashes,
    parse_folder_id,
    parse_mtime,
    parse_prefixed_id,
    post_op_stats,
    resolve_target,
    unique_rel_name,
    utc_now,
)
from file_hunter.services import fs
from file_hunter.services.activity import (
    register as activity_register,
    unregister as activity_unregister,
    update as activity_update,
)
from file_hunter.services.dup_counts import recalculate_dup_counts
from file_hunter.services.provenance import (
    build_stub_text,
    file_folder,
    upsert_sources_record,
    write_stub_record,
)
from file_hunter.services.op_result_log import add_to_catalog, append_row, create_log
from file_hunter.services.tags import get_merged_tags, parse_tags, set_file_tags
from file_hunter.stats_db import update_dup_counts_for_files, update_stats_for_files
from file_hunter.ws.agent import get_agent_location_ids
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# Guard against concurrent consolidation of the same hash
active_consolidations: set[str] = set()


def is_consolidation_running(hash_value: str) -> bool:
    """Check whether a consolidation is already in progress for a given file hash.

    Args:
        hash_value: The effective hash (hash_strong or hash_fast) to check.

    Returns:
        True if a consolidation task is currently running for this hash.
    """
    return hash_value in active_consolidations


def get_online_location_ids() -> set[int]:
    """Return the set of location_ids whose agents are currently connected."""
    online = set()
    for lids in get_agent_location_ids().values():
        online.update(lids)
    return online


def format_location(loc_name, agent_name=None):
    """Format location name with agent: 'Location [Agent]' or just 'Location'."""
    if agent_name:
        return f"{loc_name} [{agent_name}]"
    return loc_name


async def stub_and_delete(copy, canonical_path, dest_loc_name, now_iso):
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

    stub_text = build_stub_text(
        copy["filename"], canonical_path, dest_loc_name, now_iso
    )
    stub_path = original_path + ".moved"
    stub_name = copy["filename"] + ".moved"
    stub_rel = copy["rel_path"] + ".moved"
    stub_size = len(stub_text.encode())

    await fs.file_write_text(stub_path, stub_text, copy_loc_id)

    await fs.file_delete(original_path, copy_loc_id)

    # 3. Update DB
    async with db_writer() as wdb:
        await wdb.execute(
            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
            (copy_loc_id, stub_rel, copy["id"]),
        )
        await write_stub_record(
            wdb, copy["id"], stub_name, stub_path, stub_rel, stub_size, now_iso
        )
    await release_stubbed(copy_loc_id, copy, stub_size)


async def release_stubbed(location_id, f, stub_size):
    """After a file became a stub: drop its hashes, take it out of the
    duplicate counts, and swap it for the stub in the stats."""
    h = await get_file_hashes([f["id"]])
    was_dup = (h.get(f["id"], {}).get("dup_count") or 0) > 0

    await remove_file_hashes([f["id"]])

    if was_dup:
        await update_dup_counts_for_files(location_id, [(f["folder_id"], -1)])

    await update_stats_for_files(
        location_id,
        removed=[(f["folder_id"], f["file_size"] or 0, f["file_type_high"], 0)],
        added=[(f["folder_id"], stub_size, "text", 0)],
    )


async def find_and_merge_copies(file_id: int, filename_match_only: bool = False):
    """Find all duplicate copies and merge their metadata.

    Shared by both copy and move operations. Loads the selected file,
    finds all copies by effective hash, and merges tags, description,
    and earliest modified date across all copies.

    Returns:
        dict with keys: selected, all_copies, effective_hash, hash_col,
        merged_tags, merged_description, earliest_modified, now_iso.
        Or None if the file is not found or has no hash (error broadcast).
    """
    async with read_db() as db:
        rows = await db.execute_fetchall(
            """SELECT f.*, l.name as location_name, l.root_path,
                      a.name as agent_name
               FROM files f
               JOIN locations l ON l.id = f.location_id
               LEFT JOIN agents a ON a.id = l.agent_id
               WHERE f.id = ?""",
            (file_id,),
        )
    if not rows:
        await broadcast_error(file_id, "", "File not found.")
        return None

    selected = dict(rows[0])
    filename = selected["filename"]

    effective_hash, hash_col = await get_effective_hash(file_id)

    # Carry hash_partial for destination record
    h_map = await get_file_hashes([file_id])
    selected["hash_partial"] = h_map.get(file_id, {}).get("hash_partial")

    if not effective_hash:
        await broadcast_error(file_id, filename, "File has no hash — scan it first.")
        return None

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
                      f.description,
                      f.created_date, f.modified_date, f.date_cataloged,
                      l.name as location_name, l.root_path,
                      a.name as agent_name
               FROM files f
               JOIN locations l ON l.id = f.location_id
               LEFT JOIN agents a ON a.id = l.agent_id
               WHERE f.id IN ({ph}) AND f.stale = 0""",
            copy_ids,
        )
    all_copies = [dict(r) for r in all_copies]

    if filename_match_only:
        all_copies = [c for c in all_copies if c["filename"] == filename]

    now_iso = utc_now()

    # Merge description and earliest modified_date across all copies
    earliest_modified = None
    merged_description = ""
    for copy in all_copies:
        md = copy.get("modified_date")
        if md and (earliest_modified is None or md < earliest_modified):
            earliest_modified = md
        desc = (copy.get("description") or "").strip()
        if desc and not merged_description:
            merged_description = desc

    # The survivor inherits every tag any copy carried
    async with read_db() as db:
        merged_tags = await get_merged_tags(db, [c["id"] for c in all_copies])

    return {
        "selected": selected,
        "all_copies": all_copies,
        "effective_hash": effective_hash,
        "hash_col": hash_col,
        "merged_tags": merged_tags,
        "merged_description": merged_description,
        "earliest_modified": earliest_modified,
        "now_iso": now_iso,
    }


async def queue_stub(copy, canonical_path, now_iso):
    """Queue a copy for stubbing when its location is next online."""
    async with db_writer() as wdb:
        await wdb.execute(
            """INSERT INTO consolidation_jobs
               (source_file, source_location_id, source_path,
                destination_path, status, date_created)
               VALUES (?, ?, ?, ?, 'pending', ?)""",
            (
                copy["filename"],
                copy["location_id"],
                copy["full_path"],
                canonical_path,
                now_iso,
            ),
        )


async def broadcast_error(file_id, filename, error, **extra):
    await broadcast(
        {
            "type": "consolidate_error",
            "fileId": file_id,
            "filename": filename,
            "error": error,
            **extra,
        }
    )


async def claim(effective_hash, file_id, filename, location_id, busy_error) -> bool:
    """Mark the hash as in progress and announce the start, unless another
    operation on the same file already holds it."""
    if effective_hash in active_consolidations:
        await broadcast_error(file_id, filename, busy_error)
        return False
    active_consolidations.add(effective_hash)
    await broadcast(
        {
            "type": "consolidate_started",
            "fileId": file_id,
            "filename": filename,
            "locationId": location_id,
        }
    )
    return True


async def resolve_destination(file_id, filename, dest_folder_id):
    """(dest_dir, dest_loc_id), or None after reporting the error."""
    async with read_db() as db:
        dest_dir, dest_loc_id = await resolve_folder_path_with_loc(
            db, dest_folder_id
        )
    if dest_dir is None:
        await broadcast_error(file_id, filename, "Destination folder not found.")
        return None
    return dest_dir, dest_loc_id


async def location_label(location_id) -> str:
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT l.name, a.name as agent_name "
            "FROM locations l LEFT JOIN agents a ON a.id = l.agent_id "
            "WHERE l.id = ?",
            (location_id,),
        )
    return format_location(rows[0]["name"], rows[0]["agent_name"]) if rows else ""


async def apply_merged_metadata(file_id, description, modified_date, tags):
    async with db_writer() as wdb:
        await wdb.execute(
            "UPDATE files SET description = ?, modified_date = ? WHERE id = ?",
            (description, modified_date, file_id),
        )
        await set_file_tags(wdb, file_id, tags)


async def copy_to_destination(file_id, prep, dest_folder_id, dest_dir, dest_loc_id):
    """Copy the file into the destination folder and catalogue it there.

    The name is made unique against the catalogue, the bytes come from any
    online copy and are verified by hash, and the new record gets the merged
    tags and earliest modified date. Returns
    (canonical_path, canonical_id, filename), or None after reporting the
    error. A copy that fails is removed from the destination.
    """
    selected = prep["selected"]
    modified = prep["earliest_modified"] or selected["modified_date"]

    async with read_db() as db:
        target = await resolve_target(db, dest_folder_id) if dest_folder_id else None
        taken = await catalog_rel_paths(db, dest_loc_id)
    rel_dir = (target.get("rel_path") or "") if target else ""
    filename = unique_rel_name(taken, rel_dir, selected["filename"])
    canonical_path = os.path.join(dest_dir, filename)

    online_loc_ids = get_online_location_ids()
    source = next(
        (c for c in prep["all_copies"] if c["location_id"] in online_loc_ids), None
    )
    if source is None:
        await broadcast_error(
            file_id, filename, "No online copy available to copy from."
        )
        return None

    async def copy_progress(bytes_sent, total_bytes):
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
            source["full_path"],
            source["location_id"],
            canonical_path,
            dest_loc_id,
            on_progress=copy_progress,
            mtime=parse_mtime(modified),
        )
    except Exception as copy_exc:
        try:
            await fs.file_delete(canonical_path, dest_loc_id)
        except Exception:
            pass
        raise RuntimeError(f"Copy failed: {copy_exc}") from copy_exc

    await broadcast(
        {
            "type": "consolidate_progress",
            "fileId": file_id,
            "filename": filename,
            "phase": "verifying",
        }
    )
    (source_hash_fast,) = await fs.file_hash(source["full_path"], source["location_id"])
    (copy_hash_fast,) = await fs.file_hash(canonical_path, dest_loc_id)
    if copy_hash_fast != source_hash_fast:
        await fs.file_delete(canonical_path, dest_loc_id)
        await broadcast_error(file_id, filename, "Hash verification failed after copy.")
        return None

    selected["filename"] = filename
    selected["hash_fast"] = source_hash_fast
    selected["tags"] = prep["merged_tags"]
    selected["modified_date"] = modified
    canonical_id = await ensure_canonical_record(
        canonical_path, dest_folder_id, dest_loc_id, selected, prep["now_iso"]
    )
    return canonical_path, canonical_id, filename


def folder_id_of(node_id):
    """The folder id of a fld-N node id; None for a location."""
    kind, _ = parse_prefixed_id(node_id)
    return parse_folder_id(node_id) if kind == "fld" else None


async def open_result_log(
    shared_csv_path, shared_csv_loc_id, dest_dir, dest_loc_id, folder_id
):
    """(csv_path, csv_loc_id, csv_folder_id, owns_csv): the batch's shared
    result log, or a new one in dest_dir, catalogued in folder_id."""
    if shared_csv_path is not None:
        # the batch catalogues its own log
        return shared_csv_path, shared_csv_loc_id, None, False
    csv_path = await create_log(dest_dir, dest_loc_id, "consolidate")
    return csv_path, dest_loc_id, folder_id, True


async def finish_consolidation(
    result_log, canonical_id, filename, canonical_path, dest_loc_id,
    stubs_written, stubs_queued,
):
    """Catalogue the result log if this consolidation owns it, and tell the
    UI the file is done."""
    csv_path, csv_loc_id, csv_folder_id, owns_csv = result_log
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


async def run_copy(
    file_id: int,
    dest_folder_id: str,
    shared_csv_path: str | None = None,
    shared_csv_loc_id: int | None = None,
    skip_post_processing: bool = False,
    filename_match_only: bool = False,
):
    """Copy a file to a destination with merged metadata from all duplicate copies.

    Finds all duplicates by hash, merges tags/description/earliest mtime,
    copies the file to the destination, and writes a CSV audit trail.
    Original files are not modified — no stubbing, no deletion.

    Args:
        file_id: The DB id of the source file to copy.
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') for destination.
        shared_csv_path: Shared result-log CSV from batch operation.
        shared_csv_loc_id: Location id for shared CSV.
        skip_post_processing: Skip stats refresh (batch does it once).
        filename_match_only: Only merge metadata from copies with same filename.
    """
    effective_hash = None
    filename = None
    dest_loc_id = None
    act_name = f"copy_{file_id}"
    activity_register(act_name, "Copying")
    try:
        prep = await find_and_merge_copies(file_id, filename_match_only)
        if not prep:
            return

        selected = prep["selected"]
        effective_hash = prep["effective_hash"]
        filename = selected["filename"]

        if not await claim(
            effective_hash, file_id, filename, selected["location_id"],
            "Operation already in progress for this file.",
        ):
            return

        dest = await resolve_destination(file_id, filename, dest_folder_id)
        if not dest:
            return
        dest_dir, dest_loc_id = dest
        selected["description"] = prep["merged_description"]
        copied = await copy_to_destination(
            file_id, prep, dest_folder_id, dest_dir, dest_loc_id
        )
        if not copied:
            return
        canonical_path, canonical_id, filename = copied

        # CSV audit trail
        result_log = await open_result_log(
            shared_csv_path, shared_csv_loc_id, dest_dir, dest_loc_id,
            folder_id_of(dest_folder_id),
        )
        csv_path, csv_loc_id = result_log[:2]

        dest_loc_name = await location_label(dest_loc_id)

        await append_row(
            csv_path, csv_loc_id,
            selected["location_name"], selected["full_path"],
            dest_loc_name, canonical_path, "copied",
        )

        await finish_consolidation(
            result_log, canonical_id, filename, canonical_path, dest_loc_id, 0, 0
        )

        if not skip_post_processing:
            await post_op_stats(
                location_ids={dest_loc_id},
                source=f"copy {filename}",
            )

    except Exception as exc:
        await broadcast_error(
            file_id, filename or "", str(exc), destLocationId=dest_loc_id
        )

    finally:
        activity_unregister(act_name)
        if effective_hash:
            active_consolidations.discard(effective_hash)


async def run_consolidation(
    file_id: int,
    mode: str,
    dest_folder_id: str | None,
    shared_csv_path: str | None = None,
    shared_csv_loc_id: int | None = None,
    skip_post_processing: bool = False,
    filename_match_only: bool = False,
    stub_file_ids: list[int] | None = None,
):
    """Consolidate duplicate files by electing a canonical copy and stubbing the rest.

    For 'keep_here' mode, the selected file stays in place as the canonical.
    For 'move_to' mode, the selected file is copied to a destination folder,
    verified by hash, and a new DB record is created there. In both modes,
    every other copy of the same hash is stubbed on disk (.moved) and deleted.
    Offline copies are queued in consolidation_jobs for later processing.

    Per-duplicate sequence:
        1. Write .moved stub (agent)
        2. Delete original (agent)
        3. Update DB: record to stub, remove hashes, update stats

    Args:
        file_id: The DB id of the file chosen as the canonical copy.
        mode: 'keep_here' or 'move_to'.
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') for move_to.
        shared_csv_path: Shared result-log CSV from batch consolidation.
        shared_csv_loc_id: Location id for shared CSV.
        skip_post_processing: Skip dup-count recalc (batch does it once).
        filename_match_only: Only consolidate copies with the same filename.
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
        prep = await find_and_merge_copies(file_id, filename_match_only)
        if not prep:
            return

        selected = prep["selected"]
        all_copies = prep["all_copies"]
        effective_hash = prep["effective_hash"]
        hash_col = prep["hash_col"]
        merged_tags = prep["merged_tags"]
        merged_description = prep["merged_description"]
        earliest_modified = prep["earliest_modified"]
        now_iso = prep["now_iso"]
        filename = selected["filename"]
        selected_loc_id = selected["location_id"]

        if not await claim(
            effective_hash, file_id, filename, selected_loc_id,
            "Consolidation already in progress for this file.",
        ):
            return

        if mode == "keep_here":
            canonical_path = selected["full_path"]
            canonical_id = file_id
            dest_loc_id = selected_loc_id

            await apply_merged_metadata(
                canonical_id,
                merged_description or selected.get("description") or "",
                earliest_modified or selected["modified_date"],
                merged_tags,
            )

        elif mode == "move_to":
            dest = await resolve_destination(file_id, filename, dest_folder_id)
            if not dest:
                return
            dest_dir, dest_loc_id = dest

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

                await apply_merged_metadata(
                    canonical_id,
                    merged_description or existing_at_dest.get("description") or "",
                    earliest_modified or existing_at_dest["modified_date"],
                    merged_tags,
                )
            else:
                copied = await copy_to_destination(
                    file_id, prep, dest_folder_id, dest_dir, dest_loc_id
                )
                if not copied:
                    return
                canonical_path, canonical_id, filename = copied

        else:
            await broadcast_error(file_id, filename, f"Unknown mode: {mode}")
            return

        # Write .sources file next to canonical — single append call
        try:
            sources_entries = "".join(
                f"- {format_location(c['location_name'], c.get('agent_name'))}: {c['rel_path']}\n"
                for c in all_copies
            )
            await fs.file_write_text(
                canonical_path + ".sources",
                sources_entries,
                dest_loc_id,
                append=True,
            )
            # the record goes in the canonical file's folder; none if it
            # isn't catalogued
            found = await file_folder(canonical_id)
            if found:
                await upsert_sources_record(
                    canonical_path, dest_loc_id, *found, sources_entries, now_iso
                )
        except Exception as e:
            logger.warning("Could not write .sources file: %s", e)

        # Result log CSV — use shared one from batch, or create per-file
        if mode == "keep_here":
            log_folder_id = selected.get("folder_id")
        elif mode == "move_to":
            log_folder_id = folder_id_of(dest_folder_id)
        else:
            log_folder_id = None
        result_log = await open_result_log(
            shared_csv_path, shared_csv_loc_id, os.path.dirname(canonical_path),
            dest_loc_id, log_folder_id,
        )
        csv_path, csv_loc_id = result_log[:2]

        # Resolve destination location label
        if mode == "keep_here":
            dest_loc_name = format_location(
                selected["location_name"], selected.get("agent_name")
            )
        else:
            dest_loc_name = await location_label(dest_loc_id)

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
        # If stub_file_ids provided, only stub those specific files
        stub_set = set(stub_file_ids) if stub_file_ids else None
        duplicates = [
            c for c in all_copies
            if c["id"] != canonical_id
            and not c["dup_exclude"]
            and (stub_set is None or c["id"] in stub_set)
        ]
        total_dups = len(duplicates)

        # Build online location set once
        online_loc_ids = get_online_location_ids()

        for idx, copy in enumerate(duplicates, 1):
            original_path = copy["full_path"]
            copy_loc_id = copy["location_id"]

            def log_copy(status, *detail):
                return append_row(
                    csv_path, csv_loc_id, copy["location_name"], original_path,
                    dest_loc_name, canonical_path, status, *detail,
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

            # Check if location is online via agent registry
            if copy_loc_id not in online_loc_ids:
                stubs_queued += 1
                await queue_stub(copy, canonical_path, now_iso)
                await log_copy("offline - queued")
                continue

            # Try stub + delete; handle failures
            try:
                await stub_and_delete(copy, canonical_path, dest_loc_name, now_iso)
                stubs_written += 1
                await log_copy("stubbed")
            except FileNotFoundError:
                # File missing on disk — queue for later
                stubs_queued += 1
                await queue_stub(copy, canonical_path, now_iso)
                await log_copy("file missing - queued")
            except PermissionError:
                # Read-only — log and continue
                await log_copy("stub failed (read-only)")
            except Exception as e:
                logger.warning("Stub write failed for %s: %s", original_path, e)
                await log_copy("stub failed", str(e))

        await finish_consolidation(
            result_log, canonical_id, filename, canonical_path, dest_loc_id,
            stubs_written, stubs_queued,
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
        await broadcast_error(
            file_id, filename or "", str(exc),
            destLocationId=dest_loc_id or selected_loc_id,
            stubsWritten=stubs_written,
            stubsQueued=stubs_queued,
        )

    finally:
        activity_unregister(act_name)
        if effective_hash:
            active_consolidations.discard(effective_hash)


async def run_batch_consolidation(
    file_ids: list[int],
    mode: str,
    dest_folder_id: str | None,
    filename_match_only: bool = False,
    consolidate_mode: str = "move",
    stub_file_ids: list[int] | None = None,
):
    """Run copy or move consolidation for multiple files, sharing a single result log.

    Creates a shared CSV result log, then calls run_copy or run_consolidation
    for each file with skip_post_processing=True. After all files are processed,
    performs a single dup-count recalculation and stats refresh for the batch.

    Args:
        file_ids: List of DB file ids to consolidate (each becomes a canonical).
        mode: 'keep_here' or 'move_to' (passed through to run_consolidation).
        dest_folder_id: Prefixed id ('loc-N' or 'fld-N') for move_to mode.
        filename_match_only: Only consolidate copies with the same filename.
    """
    # Resolve destination for the shared CSV
    dest_dir = None
    dest_loc_id = None
    csv_folder_id = None
    if dest_folder_id:
        async with read_db() as db:
            d, lid = await resolve_folder_path_with_loc(db, dest_folder_id)
        dest_dir = d
        dest_loc_id = lid
        csv_folder_id = folder_id_of(dest_folder_id)

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

    is_copy = consolidate_mode == "copy"

    for file_id in unique_ids:
        try:
            if is_copy:
                await run_copy(
                    file_id,
                    dest_folder_id,
                    shared_csv_path=csv_path,
                    shared_csv_loc_id=dest_loc_id,
                    skip_post_processing=True,
                    filename_match_only=filename_match_only,
                )
            else:
                await run_consolidation(
                    file_id,
                    mode,
                    dest_folder_id,
                    shared_csv_path=csv_path,
                    shared_csv_loc_id=dest_loc_id,
                    skip_post_processing=True,
                    filename_match_only=filename_match_only,
                    stub_file_ids=stub_file_ids,
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
    # Recount the hash groups we actually touched, so the denormalized
    # files.dup_count (which the "duplicates only" filter reads) matches the
    # now-reduced groups. Without the hashes this call is a no-op, leaving
    # consolidated files stuck in duplicates-only results with a live badge
    # of 0. hash_map was captured before stubbing, so it holds every group.
    recalc_strong = {h for h, col in hash_map.values() if col == "hash_strong" and h}
    recalc_fast = {h for h, col in hash_map.values() if col == "hash_fast" and h}
    await recalculate_dup_counts(
        strong_hashes=recalc_strong,
        fast_hashes=recalc_fast,
        source=f"batch consolidate ({len(file_ids)} files)",
    )

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

    now_iso = utc_now()
    jobs_completed = 0

    for job in rows:
        source_path = job["source_path"]
        dest_path = job["destination_path"]
        source_file = job["source_file"]

        try:
            # Resolve destination location label from canonical file
            async with read_db() as rdb:
                dest_rows = await rdb.execute_fetchall(
                    "SELECT l.name, a.name as agent_name "
                    "FROM files f "
                    "JOIN locations l ON l.id = f.location_id "
                    "LEFT JOIN agents a ON a.id = l.agent_id "
                    "WHERE f.full_path = ? LIMIT 1",
                    (dest_path,),
                )
            dest_loc_label = format_location(
                dest_rows[0]["name"], dest_rows[0]["agent_name"]
            ) if dest_rows else ""

            # Build a stub text for this job
            stub_text = build_stub_text(source_file, dest_path, dest_loc_label, now_iso)
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
                    f = file_rows[0]
                    await write_stub_record(
                        wdb, f["id"], stub_name, stub_path,
                        f["rel_path"] + ".moved", stub_size, now_iso,
                    )
                    await release_stubbed(location_id, f, stub_size)
                else:
                    # No file record — just clean up any orphan
                    del_rows = await wdb.execute_fetchall(
                        "SELECT id, folder_id, file_size, file_type_high "
                        "FROM files WHERE full_path = ? AND location_id = ?",
                        (source_path, location_id),
                    )
                    if del_rows:
                        del_ids = [r["id"] for r in del_rows]
                        h = await get_file_hashes(del_ids)
                        dup_deltas = [
                            (r["folder_id"], -1)
                            for r in del_rows
                            if (h.get(r["id"], {}).get("dup_count") or 0) > 0
                        ]
                    await wdb.execute(
                        "DELETE FROM files WHERE full_path = ? AND location_id = ?",
                        (source_path, location_id),
                    )
                    if del_rows:
                        await remove_file_hashes(del_ids)
                        if dup_deltas:
                    
                            await update_dup_counts_for_files(location_id, dup_deltas)
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


async def resolve_folder_path_with_loc(
    db, folder_id: str
) -> tuple[str | None, int | None]:
    """Resolve a prefixed folder identifier to its absolute path and location id."""
    target = await resolve_target(db, folder_id)
    if not target:
        return None, None
    return target["abs_path"], target["location_id"]


async def ensure_canonical_record(
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
                description,
                created_date, modified_date, date_cataloged, date_last_seen, scan_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
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
                source.get("created_date", now_iso),
                source.get("modified_date", now_iso),
                now_iso,
                now_iso,
            ),
        )
        new_id = cursor.lastrowid
        await set_file_tags(wdb, new_id, parse_tags(source.get("tags")))

    # Register hashes for the new file record
    hash_partial = source.get("hash_partial")
    hash_fast = source.get("hash_fast")
    hash_strong = source.get("hash_strong")
    if hash_fast or hash_strong:
        await set_file_hashes(
            new_id, location_id, file_size, hash_partial, hash_fast, hash_strong
        )

    return new_id
