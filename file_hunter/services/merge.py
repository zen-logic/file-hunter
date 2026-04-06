"""Merge logic — move or copy files to destination."""

import logging
import os
import time
from datetime import datetime, timezone

from file_hunter_core.classify import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import (
    get_file_hashes,
    hashes_writer,
    remove_file_hashes,
)
from file_hunter.helpers import parse_mtime, parse_prefixed_id
from file_hunter.services import fs
from file_hunter.services.activity import (
    register as activity_register,
    unregister as activity_unregister,
    update as activity_update,
)
from file_hunter.services.dup_counts import recalculate_dup_counts
from file_hunter.services.op_result_log import add_to_catalog, append_row, create_log
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

_merge_running: bool = False
_merge_cancel_requested: bool = False


def is_merge_running() -> bool:
    """Check whether a merge operation is currently in progress.

    Returns:
        True if a merge task is running.

    Called by:
        routes/merge.py (to reject concurrent merges and to validate cancel requests).
    """
    return _merge_running


def request_merge_cancel():
    """Signal the running merge task to stop after the current file.

    Sets a module-level flag that run_merge checks at the top of each
    per-file iteration. The merge will finish the file it is currently
    processing, then exit cleanly with a merge_cancelled broadcast.

    Returns:
        None.

    Side effects:
        Sets _merge_cancel_requested to True.

    Called by:
        routes/merge.py (cancel endpoint).
    """
    global _merge_cancel_requested
    _merge_cancel_requested = True


async def resolve_merge_target(db, target_id: str) -> dict | None:
    """Resolve a prefixed location/folder identifier to a target info dict.

    For 'loc-N', returns the location root. For 'fld-N', returns the folder
    joined to its location root, with the folder's rel_path as rel_prefix.

    Args:
        db: An open aiosqlite read connection.
        target_id: Prefixed identifier ('loc-N' or 'fld-N').

    Returns:
        Dict with keys { label, abs_path, root_path, location_id, folder_id,
        rel_prefix }, or None if the identifier cannot be resolved.

    Called by:
        routes/merge.py (to resolve source and destination before launching merge).
    """
    kind, num_id = parse_prefixed_id(target_id)

    if kind == "loc":
        rows = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE id = ?", (num_id,)
        )
        if not rows:
            return None
        loc = rows[0]
        return {
            "label": loc["name"],
            "abs_path": loc["root_path"],
            "root_path": loc["root_path"],
            "location_id": loc["id"],
            "folder_id": None,
            "rel_prefix": "",
        }

    elif kind == "fld":
        rows = await db.execute_fetchall(
            """SELECT f.id, f.name, f.rel_path, f.location_id, l.name as loc_name, l.root_path
               FROM folders f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (num_id,),
        )
        if not rows:
            return None
        fld = rows[0]
        return {
            "label": f"{fld['loc_name']} / {fld['name']}",
            "abs_path": os.path.join(fld["root_path"], fld["rel_path"]),
            "root_path": fld["root_path"],
            "location_id": fld["location_id"],
            "folder_id": fld["id"],
            "rel_prefix": fld["rel_path"],
        }

    return None


async def run_merge(source_id, source_info, destination_id, dest_info, mode="move"):
    """Merge source location/folder into destination.

    Two modes:
    - "move": unique files are copied then source is stubbed/deleted.
      Duplicate files are stubbed/deleted at source. Provenance metadata
      (.moved stubs and .sources entries) written for all files.
    - "copy": unique files are copied to destination. Duplicates are
      skipped entirely. No source modification.

    Per-file sequence (move mode, unique):
        1. Copy to destination (agent)
        2. Hash verify copy (agent)
        3. Append .sources at destination (agent)
        4. Write .moved stub at source (agent)
        5. Delete original at source (agent)
        6. Update DB: insert dest record, update source to stub, hashes, stats

    Per-file sequence (move mode, duplicate):
        1. Append .sources at destination (agent)
        2. Write .moved stub at source (agent)
        3. Delete original at source (agent)
        4. Update DB: source to stub, remove hashes, stats

    Per-file sequence (copy mode, unique):
        1. Copy to destination (agent)
        2. Hash verify copy (agent)
        3. Update DB: insert dest record, hashes, stats

    Per-file sequence (copy mode, duplicate):
        Skip — file already exists at destination.

    Args:
        source_id: Prefixed identifier ('loc-N' or 'fld-N') of the source.
        source_info: Dict from resolve_merge_target for the source.
        destination_id: Prefixed identifier ('loc-N' or 'fld-N') of the dest.
        dest_info: Dict from resolve_merge_target for the destination.
        mode: "move" (default) or "copy".

    Side effects:
        - DB writes via db_writer(): file inserts, stub updates, deletes.
        - Hashes DB via hashes_writer(): inserts for copies, removes for stubs.
        - Stats DB: folder/location stat adjustments per file.
        - File I/O via agent: copy, hash, stub write, delete, .sources append.
        - Broadcasts: merge_started, merge_progress, merge_completed,
          merge_cancelled, merge_error.
        - Result log: CSV at destination added to catalog.
        - Cache: invalidates stats, recalculates location sizes and dup counts.
        - Module state: sets/clears _merge_running and _merge_cancel_requested.

    Called by:
        routes/merge.py (via queue_manager).
    """
    global _merge_running, _merge_cancel_requested
    _merge_running = True
    _merge_cancel_requested = False

    is_move = mode == "move"
    source_label = source_info["label"]
    dest_label = dest_info["label"]
    src_loc_id = source_info["location_id"]
    dest_loc_id = dest_info["location_id"]
    files_copied = 0
    files_stubbed = 0
    files_skipped = 0
    files_duplicate = 0
    total_files = 0
    processed = 0
    last_broadcast = 0.0

    affected_hashes: set[str] = set()

    mode_label = "Moving" if is_move else "Copying"
    act_name = f"merge_{source_label}_{dest_label}"
    activity_register(act_name, f"{mode_label} {source_label} → {dest_label}")

    try:
        await broadcast(
            {
                "type": "merge_started",
                "source": source_label,
                "destination": dest_label,
                "mode": mode,
            }
        )

        # Load source files (excludes stale, stubs, .sources)
        async with read_db() as db:
            source_files = await _load_source_files(db, source_id, source_info)
        total_files = len(source_files)

        if total_files == 0:
            await broadcast(
                {
                    "type": "merge_completed",
                    "source": source_label,
                    "destination": dest_label,
                    "mode": mode,
                    "filesCopied": 0,
                    "filesStubbed": 0,
                    "filesDuplicate": 0,
                    "filesSkipped": 0,
                }
            )
            return

        # Build destination hash index (excludes stale files)
        async with read_db() as db:
            dest_hash_index = await _build_dest_hash_index(db, dest_loc_id)

        # Build set of destination rel_paths for catalog-based name collision
        async with read_db() as db:
            dest_rel_paths = await _build_dest_rel_paths(db, dest_loc_id)

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        folder_cache: dict[str, int] = {}
        dir_created: set[str] = set()

        # Create result log CSV at destination
        csv_path = await create_log(dest_info["abs_path"], dest_loc_id, "merge")
        csv_folder_id = dest_info["folder_id"]

        for src_file in source_files:
            if _merge_cancel_requested:
                break

            processed += 1
            src_path = src_file["full_path"]
            src_rel = os.path.relpath(src_path, source_info["abs_path"])
            is_hidden = 1 if src_file["hidden"] else 0

            # Determine effective hash for duplicate detection
            hash_strong = src_file["hash_strong"]
            hash_fast = src_file["hash_fast"]
            effective_hash = hash_strong or hash_fast

            # ── Duplicate: hash found in destination ─────────────────
            if effective_hash and effective_hash in dest_hash_index:
                dest_entry = dest_hash_index[effective_hash]
                dest_canonical = dest_entry["full_path"]
                affected_hashes.add(effective_hash)

                if not is_move:
                    # Copy mode: nothing to do for duplicates
                    files_duplicate += 1
                    await append_row(
                        csv_path,
                        dest_loc_id,
                        source_label,
                        src_path,
                        dest_label,
                        dest_canonical,
                        "duplicate (skipped)",
                    )
                else:
                    # Move mode: write provenance, stub source, delete original
                    try:
                        # 1. Append .sources at destination
                        sources_entry = (
                            f"- {src_file['location_name']}: {src_file['rel_path']}\n"
                        )
                        await fs.file_write_text(
                            dest_canonical + ".sources",
                            sources_entry,
                            dest_loc_id,
                            append=True,
                        )
                        await _upsert_sources_record(
                            dest_canonical,
                            dest_loc_id,
                            dest_entry,
                            sources_entry,
                            now_iso,
                        )

                        # 2. Write .moved stub at source
                        stub_text = _build_stub_text(
                            src_file["filename"],
                            dest_canonical,
                            dest_label,
                            now_iso,
                        )
                        stub_path = src_path + ".moved"
                        await fs.file_write_text(stub_path, stub_text, src_loc_id)

                        # 3. Delete original source file
                        await fs.file_delete(src_path, src_loc_id)

                        # 4. Update DB
                        await _stub_source_record(
                            src_file,
                            stub_text,
                            now_iso,
                        )
                        await remove_file_hashes([src_file["id"]])
                        await update_stats_for_files(
                            src_file["location_id"],
                            removed=[
                                (
                                    src_file["folder_id"],
                                    src_file["file_size"] or 0,
                                    src_file["file_type_high"],
                                    src_file["hidden"],
                                )
                            ],
                            added=[(src_file["folder_id"], 0, "text", 0)],
                        )

                        files_stubbed += 1
                        await append_row(
                            csv_path,
                            dest_loc_id,
                            source_label,
                            src_path,
                            dest_label,
                            dest_canonical,
                            "duplicate + stubbed",
                        )
                    except FileNotFoundError:
                        files_skipped += 1
                        await append_row(
                            csv_path,
                            dest_loc_id,
                            source_label,
                            src_path,
                            dest_label,
                            "",
                            "skipped (file missing)",
                        )
                    except Exception as e:
                        files_skipped += 1
                        logger.warning("Merge stub failed for %s: %s", src_path, e)
                        await append_row(
                            csv_path,
                            dest_loc_id,
                            source_label,
                            src_path,
                            dest_label,
                            dest_canonical,
                            "error",
                            str(e),
                        )

            # ── Unique: not in destination ────────────────────────────
            else:
                try:
                    # Compute destination path with catalog-based collision check
                    dest_rel_path = os.path.join(dest_info["abs_path"], src_rel)
                    dest_file_name = os.path.basename(dest_rel_path)
                    dest_file_rel_dir = os.path.relpath(
                        os.path.dirname(dest_rel_path), dest_info["root_path"]
                    )
                    if dest_file_rel_dir == ".":
                        dest_file_rel_dir = ""

                    if dest_info["rel_prefix"]:
                        raw_rel = os.path.relpath(
                            os.path.dirname(dest_rel_path), dest_info["abs_path"]
                        )
                        if raw_rel == ".":
                            full_rel_dir = dest_info["rel_prefix"]
                        else:
                            full_rel_dir = os.path.join(
                                dest_info["rel_prefix"], raw_rel
                            )
                    else:
                        full_rel_dir = dest_file_rel_dir

                    dest_file_rel = (
                        os.path.join(full_rel_dir, dest_file_name)
                        if full_rel_dir
                        else dest_file_name
                    )

                    # Name collision: check catalog, append suffix if needed
                    actual_dest_rel = dest_file_rel
                    actual_dest = dest_rel_path
                    if dest_file_rel.lower() in dest_rel_paths:
                        base, ext = os.path.splitext(dest_file_name)
                        counter = 1
                        while actual_dest_rel.lower() in dest_rel_paths:
                            new_name = f"{base}_{counter}{ext}"
                            actual_dest_rel = (
                                os.path.join(full_rel_dir, new_name)
                                if full_rel_dir
                                else new_name
                            )
                            actual_dest = os.path.join(
                                os.path.dirname(dest_rel_path), new_name
                            )
                            counter += 1
                    dest_file_name = os.path.basename(actual_dest)
                    dest_file_rel = actual_dest_rel

                    # 1. Create destination directory (cached)
                    dest_dir = os.path.dirname(actual_dest)
                    if dest_dir not in dir_created:
                        await fs.dir_create(dest_dir, dest_loc_id, exist_ok=True)
                        dir_created.add(dest_dir)

                    # 2. Copy file to destination
                    await fs.copy_file(
                        src_path,
                        src_loc_id,
                        actual_dest,
                        dest_loc_id,
                        mtime=parse_mtime(src_file["modified_date"]),
                    )

                    # 3. Hash verify the copy (xxHash64)
                    (copy_hash,) = await fs.file_hash(actual_dest, dest_loc_id)
                    if hash_fast and copy_hash != hash_fast:
                        # Hash mismatch — delete the bad copy and skip
                        await fs.file_delete(actual_dest, dest_loc_id)
                        files_skipped += 1
                        await append_row(
                            csv_path,
                            dest_loc_id,
                            source_label,
                            src_path,
                            dest_label,
                            actual_dest,
                            "skipped (hash mismatch)",
                        )
                        continue

                    # Use copy hash as hash_fast for the destination record
                    hash_fast = copy_hash
                    if not effective_hash:
                        effective_hash = copy_hash

                    # Move mode: write provenance, stub, delete
                    if is_move:
                        # 4. Append .sources at destination
                        sources_entry = (
                            f"- {src_file['location_name']}: {src_file['rel_path']}\n"
                        )
                        await fs.file_write_text(
                            actual_dest + ".sources",
                            sources_entry,
                            dest_loc_id,
                            append=True,
                        )

                        # 5. Write .moved stub at source
                        stub_text = _build_stub_text(
                            src_file["filename"],
                            actual_dest,
                            dest_label,
                            now_iso,
                        )
                        stub_path = src_path + ".moved"
                        await fs.file_write_text(stub_path, stub_text, src_loc_id)

                        # 6. Delete original at source
                        await fs.file_delete(src_path, src_loc_id)

                    # 7. Register destination file in DB
                    dest_folder_id = dest_info["folder_id"]
                    if full_rel_dir:
                        dest_folder_id = await _ensure_folder_hierarchy(
                            dest_loc_id, full_rel_dir, folder_cache
                        )

                    type_high, type_low = classify_file(dest_file_name)
                    file_size = src_file["file_size"] or 0

                    async with db_writer() as wdb:
                        await wdb.execute(
                            """INSERT OR IGNORE INTO files
                               (filename, full_path, rel_path, location_id, folder_id,
                                file_type_high, file_type_low, file_size,
                                description, tags,
                                created_date, modified_date, date_cataloged, date_last_seen,
                                scan_id, hidden)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, NULL, ?)""",
                            (
                                dest_file_name,
                                actual_dest,
                                dest_file_rel,
                                dest_loc_id,
                                dest_folder_id,
                                type_high,
                                type_low,
                                file_size,
                                src_file.get("created_date", now_iso),
                                src_file.get("modified_date", now_iso),
                                now_iso,
                                now_iso,
                                is_hidden,
                            ),
                        )
                        new_rows = await wdb.execute_fetchall(
                            "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
                            (dest_loc_id, dest_file_rel),
                        )
                        new_dest_file_id = new_rows[0]["id"] if new_rows else None

                    # Register hashes for the new destination file
                    if new_dest_file_id:
                        async with hashes_writer() as hdb:
                            await hdb.execute(
                                "INSERT INTO file_hashes "
                                "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                                "VALUES (?, ?, ?, NULL, ?, ?) "
                                "ON CONFLICT(file_id) DO UPDATE SET "
                                "hash_fast=excluded.hash_fast, hash_strong=excluded.hash_strong",
                                (
                                    new_dest_file_id,
                                    dest_loc_id,
                                    file_size,
                                    copy_hash,
                                    hash_strong,
                                ),
                            )

                    await update_stats_for_files(
                        dest_loc_id,
                        added=[(dest_folder_id, file_size, type_high, is_hidden)],
                    )

                    # Track in indexes so subsequent files see this one
                    dest_hash_index[effective_hash] = {
                        "id": new_dest_file_id,
                        "full_path": actual_dest,
                    }
                    dest_rel_paths.add(dest_file_rel.lower())

                    # Move mode: update source DB + .sources record
                    if is_move:
                        await _stub_source_record(
                            src_file,
                            stub_text,
                            now_iso,
                        )
                        await remove_file_hashes([src_file["id"]])
                        await update_stats_for_files(
                            src_file["location_id"],
                            removed=[
                                (
                                    src_file["folder_id"],
                                    src_file["file_size"] or 0,
                                    src_file["file_type_high"],
                                    src_file["hidden"],
                                )
                            ],
                            added=[(src_file["folder_id"], 0, "text", 0)],
                        )
                        await _upsert_sources_record(
                            actual_dest,
                            dest_loc_id,
                            {
                                "id": new_dest_file_id,
                                "folder_id": dest_folder_id,
                                "rel_dir": full_rel_dir,
                            },
                            sources_entry,
                            now_iso,
                        )

                    affected_hashes.add(effective_hash)
                    files_copied += 1
                    await append_row(
                        csv_path,
                        dest_loc_id,
                        source_label,
                        src_path,
                        dest_label,
                        actual_dest,
                        "copied + stubbed" if is_move else "copied",
                    )

                except FileNotFoundError:
                    files_skipped += 1
                    await append_row(
                        csv_path,
                        dest_loc_id,
                        source_label,
                        src_path,
                        dest_label,
                        "",
                        "skipped (file missing)",
                    )
                except Exception as e:
                    files_skipped += 1
                    logger.warning("Merge error for %s: %s", src_path, e)
                    await append_row(
                        csv_path,
                        dest_loc_id,
                        source_label,
                        src_path,
                        dest_label,
                        "",
                        "error",
                        str(e),
                    )

            # Throttled progress broadcast
            now = time.monotonic()
            if now - last_broadcast >= 0.5:
                last_broadcast = now
                activity_update(act_name, progress=f"{processed}/{total_files}")
                await broadcast(
                    {
                        "type": "merge_progress",
                        "source": source_label,
                        "destination": dest_label,
                        "mode": mode,
                        "processed": processed,
                        "total": total_files,
                        "copied": files_copied,
                        "stubbed": files_stubbed,
                        "duplicate": files_duplicate,
                        "skipped": files_skipped,
                    }
                )

        # Add result CSV to catalog
        await add_to_catalog(csv_path, dest_loc_id, csv_folder_id)

        if _merge_cancel_requested:
            await broadcast(
                {
                    "type": "merge_cancelled",
                    "source": source_label,
                    "destination": dest_label,
                    "mode": mode,
                    "processed": processed,
                    "total": total_files,
                    "copied": files_copied,
                    "stubbed": files_stubbed,
                    "duplicate": files_duplicate,
                    "skipped": files_skipped,
                }
            )
        else:
            await broadcast(
                {
                    "type": "merge_completed",
                    "source": source_label,
                    "destination": dest_label,
                    "mode": mode,
                    "filesCopied": files_copied,
                    "filesStubbed": files_stubbed,
                    "filesDuplicate": files_duplicate,
                    "filesSkipped": files_skipped,
                }
            )
        invalidate_stats_cache()
        locations_to_recalc = {dest_loc_id}
        if is_move:
            locations_to_recalc.add(src_loc_id)
        for lid in locations_to_recalc:
            try:
                await recalculate_location_sizes(lid)
            except Exception:
                pass

        affected_strong = {h for h in affected_hashes if len(h) == 64}
        affected_fast = {h for h in affected_hashes if len(h) == 16}
        await recalculate_dup_counts(
            strong_hashes=affected_strong or None,
            fast_hashes=affected_fast or None,
            source=f"merge {source_label} → {dest_label}",
        )

    except Exception as exc:
        await broadcast(
            {
                "type": "merge_error",
                "source": source_label,
                "destination": dest_label,
                "error": str(exc),
            }
        )

    finally:
        activity_unregister(act_name)
        _merge_running = False
        _merge_cancel_requested = False


def _build_stub_text(filename, dest_path, dest_label, now_iso):
    """Build the text content for a .moved stub file."""
    moved_to = f"{dest_label}: {dest_path}" if dest_label else dest_path
    return (
        f"Consolidated by File Hunter\n"
        f"Original: {filename}\n"
        f"Moved to: {moved_to}\n"
        f"Date: {now_iso}\n"
    )


async def _stub_source_record(src_file, stub_text, now_iso):
    """Update the source file's DB record to reflect the .moved stub.

    Replaces the file record in-place: renames to .moved, sets type to
    text/moved, sets file_size to the stub content length. Removes any
    conflicting stub records at the same rel_path.
    """
    stub_path = src_file["full_path"] + ".moved"
    stub_name = src_file["filename"] + ".moved"
    stub_rel = src_file["rel_path"] + ".moved"
    stub_size = len(stub_text.encode())

    async with db_writer() as wdb:
        # Remove any existing record at the stub's rel_path
        await wdb.execute(
            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
            (src_file["location_id"], stub_rel, src_file["id"]),
        )
        # Update the source record to the stub
        await wdb.execute(
            """UPDATE files SET
                filename=?, full_path=?, rel_path=?,
                file_type_high='text', file_type_low='moved',
                file_size=?, modified_date=?, date_last_seen=?
               WHERE id=?""",
            (
                stub_name,
                stub_path,
                stub_rel,
                stub_size,
                now_iso,
                now_iso,
                src_file["id"],
            ),
        )


async def _upsert_sources_record(
    canonical_path, dest_loc_id, dest_entry, sources_entry, now_iso
):
    """Insert or update the .sources file's DB record.

    Uses the length of sources_entry as an approximation of file size.
    For files with multiple source entries the size will be slightly off
    but correct enough for a metadata file without an agent round-trip.
    """
    sources_path = canonical_path + ".sources"
    sources_name = os.path.basename(sources_path)

    dest_file_id = dest_entry.get("id")
    dest_folder_id = dest_entry.get("folder_id")
    dest_rel_dir = dest_entry.get("rel_dir", "")

    # If we don't have folder info, look it up from the destination file
    if dest_file_id and dest_folder_id is None:
        async with read_db() as db:
            drows = await db.execute_fetchall(
                "SELECT folder_id, rel_path FROM files WHERE id = ?",
                (dest_file_id,),
            )
        if drows:
            dest_folder_id = drows[0]["folder_id"]
            dest_rel_dir = os.path.dirname(drows[0]["rel_path"])

    sources_rel = (
        os.path.join(dest_rel_dir, sources_name) if dest_rel_dir else sources_name
    )
    sources_size = len(sources_entry.encode())

    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
            (dest_loc_id, sources_rel),
        )

    async with db_writer() as wdb:
        if rows:
            await wdb.execute(
                "UPDATE files SET modified_date=?, date_last_seen=? WHERE id=?",
                (now_iso, now_iso, rows[0]["id"]),
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
                    dest_folder_id,
                    sources_size,
                    now_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )


async def _load_source_files(db, source_id, source_info):
    """Load non-stale, non-stub files for the source location or folder."""
    kind, num_id = parse_prefixed_id(source_id)

    if kind == "loc":
        rows = await db.execute_fetchall(
            """SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.created_date, fi.modified_date,
                      fi.hidden, l.name as location_name
               FROM files fi
               JOIN locations l ON l.id = fi.location_id
               WHERE fi.location_id = ?
                 AND fi.stale = 0
                 AND fi.file_type_low != 'moved'
                 AND fi.file_type_low != 'sources'""",
            (num_id,),
        )
    elif kind == "fld":
        rows = await db.execute_fetchall(
            """WITH RECURSIVE descendants(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
               )
               SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.created_date, fi.modified_date,
                      fi.hidden, l.name as location_name
               FROM files fi
               JOIN locations l ON l.id = fi.location_id
               WHERE fi.folder_id IN (SELECT id FROM descendants)
                 AND fi.stale = 0
                 AND fi.file_type_low != 'moved'
                 AND fi.file_type_low != 'sources'""",
            (num_id,),
        )
    else:
        return []

    result = [dict(r) for r in rows]

    # Fetch hashes from hashes.db
    if result:
        file_ids = [r["id"] for r in result]
        h_map = await get_file_hashes(file_ids)
        for r in result:
            h = h_map.get(r["id"], {})
            r["hash_fast"] = h.get("hash_fast")
            r["hash_strong"] = h.get("hash_strong")

    return result


async def _build_dest_hash_index(db, location_id):
    """Build hash→file dict for non-stale hashed files in the destination."""
    file_rows = await db.execute_fetchall(
        "SELECT id, full_path, folder_id, rel_path FROM files "
        "WHERE location_id = ? AND stale = 0",
        (location_id,),
    )
    if not file_rows:
        return {}

    file_ids = [r["id"] for r in file_rows]
    hash_map = await get_file_hashes(file_ids)

    result = {}
    for r in file_rows:
        h = hash_map.get(r["id"])
        if not h:
            continue
        effective = h.get("hash_strong") or h.get("hash_fast")
        if effective:
            rel_dir = os.path.dirname(r["rel_path"])
            result[effective] = {
                "id": r["id"],
                "full_path": r["full_path"],
                "folder_id": r["folder_id"],
                "rel_dir": rel_dir,
            }
    return result


async def _build_dest_rel_paths(db, location_id):
    """Build a set of lowercase rel_paths at the destination for collision checks."""
    rows = await db.execute_fetchall(
        "SELECT rel_path FROM files WHERE location_id = ? AND stale = 0",
        (location_id,),
    )
    return {r["rel_path"].lower() for r in rows}


async def _ensure_folder_hierarchy(
    location_id: int,
    rel_dir_path: str,
    folder_cache: dict[str, int],
) -> int:
    """Create/find folder records for a full relative directory path."""
    parts = rel_dir_path.split("/")
    current_path = ""
    parent_id = None

    for part in parts:
        current_path = f"{current_path}/{part}" if current_path else part

        if current_path in folder_cache:
            parent_id = folder_cache[current_path]
            continue

        async with read_db() as db:
            row = await db.execute_fetchall(
                "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
                (location_id, current_path),
            )
        if row:
            folder_id = row[0]["id"]
        else:
            is_hidden_folder = 1 if part.startswith(".") else 0
            async with db_writer() as wdb:
                cursor = await wdb.execute(
                    "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden) VALUES (?, ?, ?, ?, ?)",
                    (location_id, parent_id, part, current_path, is_hidden_folder),
                )
                folder_id = cursor.lastrowid

        folder_cache[current_path] = folder_id
        parent_id = folder_id

    return parent_id
