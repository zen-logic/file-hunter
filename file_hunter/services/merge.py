"""Merge logic — copy unique files to destination, stub duplicates at source."""

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


async def run_merge(source_id, source_info, destination_id, dest_info):
    """Merge source location/folder into destination, copying unique files and stubbing duplicates.

    Loads all non-stub files from the source, builds an O(1) hash index of the
    destination, then iterates each source file:
    - If already present at destination (by effective hash), the source is
      replaced with a .moved stub and a .sources metadata entry is written
      at the destination.
    - If unique, the file is copied to the destination (preserving directory
      structure), hash-verified, registered in the DB and hashes.db, then
      the source is stubbed.
    - Files that are missing on disk or fail hashing are skipped.

    Supports cancellation via request_merge_cancel(). Progress is broadcast
    at 500ms intervals to avoid flooding the WebSocket.

    Args:
        source_id: Prefixed identifier ('loc-N' or 'fld-N') of the source.
        source_info: Dict from resolve_merge_target for the source.
        destination_id: Prefixed identifier ('loc-N' or 'fld-N') of the
            destination.
        dest_info: Dict from resolve_merge_target for the destination.

    Returns:
        None.

    Side effects:
        - DB writes: inserts new file records at destination, updates source
          records to .moved stubs, deletes conflicting stub records. Via
          db_writer().
        - Hashes DB: inserts hashes for copied files, removes hashes for
          stubbed files, updates hash_fast for unhashed source files.
        - Stats DB: adjusts folder/location stats for every copied/stubbed file.
        - File I/O: copies files, writes .moved stubs and .sources metadata,
          creates directories, hash-verifies copies — all via agent fs calls.
        - Broadcasts: merge_started, merge_progress, merge_completed,
          merge_cancelled, merge_error via WebSocket.
        - Result log: creates a CSV operation log at destination and adds it
          to the file catalog.
        - Cache invalidation: invalidates stats cache, recalculates location
          sizes for both source and destination, recalculates dup counts for
          affected hashes.
        - Module state: sets/clears _merge_running and _merge_cancel_requested.

    Called by:
        routes/merge.py (via asyncio.create_task).
    """
    global _merge_running, _merge_cancel_requested
    _merge_running = True
    _merge_cancel_requested = False

    source_label = source_info["label"]
    dest_label = dest_info["label"]
    src_loc_id = source_info["location_id"]
    dest_loc_id = dest_info["location_id"]
    files_copied = 0
    files_stubbed = 0
    files_skipped = 0
    files_hashed = 0
    total_files = 0
    processed = 0
    last_broadcast = 0.0

    affected_hashes: set[str] = set()

    try:
        await broadcast(
            {
                "type": "merge_started",
                "source": source_label,
                "destination": dest_label,
            }
        )

        # Load source files
        async with read_db() as db:
            source_files = await _load_source_files(db, source_id, source_info)
        total_files = len(source_files)

        if total_files == 0:
            await broadcast(
                {
                    "type": "merge_completed",
                    "source": source_label,
                    "destination": dest_label,
                    "filesCopied": 0,
                    "filesStubbed": 0,
                    "filesSkipped": 0,
                }
            )
            return

        # Build destination hash index for O(1) duplicate lookup
        async with read_db() as db:
            dest_hash_index = await _build_dest_hash_index(db, dest_loc_id)

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        folder_cache: dict[str, int] = {}

        # Create result log CSV at destination
        csv_path = await create_log(dest_info["abs_path"], dest_loc_id, "merge")
        csv_folder_id = dest_info["folder_id"]

        # Cache per-folder writability to avoid repeated probes
        writable_cache: dict[str, bool] = {}

        for src_file in source_files:
            if _merge_cancel_requested:
                break

            processed += 1

            # Check source file exists on disk
            src_path = src_file["full_path"]
            exists = await fs.file_exists(src_path, src_loc_id)
            if not exists:
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
                # Throttled progress
                now = time.monotonic()
                if now - last_broadcast >= 0.5:
                    last_broadcast = now
                    await broadcast(
                        {
                            "type": "merge_progress",
                            "source": source_label,
                            "destination": dest_label,
                            "processed": processed,
                            "total": total_files,
                            "copied": files_copied,
                            "stubbed": files_stubbed,
                            "skipped": files_skipped,
                        }
                    )
                continue

            # Compute relative path within source
            src_rel = os.path.relpath(src_path, source_info["abs_path"])
            is_hidden = 1 if src_file["hidden"] else 0

            # Hash file if needed (hash_fast only — SHA-256 is user-triggered)
            hash_strong = src_file["hash_strong"]
            hash_fast = src_file["hash_fast"]
            effective_hash = hash_strong or hash_fast
            if not effective_hash:
                try:
                    (hash_fast,) = await fs.file_hash(src_path, src_loc_id)
                    effective_hash = hash_fast
                    files_hashed += 1
                    async with hashes_writer() as hdb:
                        await hdb.execute(
                            "UPDATE file_hashes SET hash_fast=? WHERE file_id=?",
                            (hash_fast, src_file["id"]),
                        )
                except (
                    PermissionError,
                    FileNotFoundError,
                    OSError,
                    ConnectionError,
                ) as e:
                    files_skipped += 1
                    await append_row(
                        csv_path,
                        dest_loc_id,
                        source_label,
                        src_path,
                        dest_label,
                        "",
                        "skipped (hash failed)",
                        str(e),
                    )
                    continue

            # Check if duplicate exists in destination location
            if effective_hash in dest_hash_index:
                # Duplicate — already at destination
                dest_entry = dest_hash_index[effective_hash]
                dest_canonical = dest_entry["full_path"]
                affected_hashes.add(effective_hash)
                files_stubbed += 1

                # Best-effort: stub at source, .sources at destination
                stub_result = "duplicate"
                src_dir = os.path.dirname(src_path)
                if src_dir not in writable_cache:
                    try:
                        test_path = os.path.join(src_dir, ".fh-write-test")
                        await fs.file_write_text(test_path, "", src_loc_id)
                        await fs.file_delete(test_path, src_loc_id)
                        writable_cache[src_dir] = True
                    except Exception:
                        writable_cache[src_dir] = False

                if writable_cache[src_dir]:
                    try:
                        stub_path = src_path + ".moved"
                        stub_name = src_file["filename"] + ".moved"
                        stub_rel = src_file["rel_path"] + ".moved"

                        async with db_writer() as wdb:
                            _replaced = await wdb.execute_fetchall(
                                "SELECT id FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                                (src_file["location_id"], stub_rel, src_file["id"]),
                            )
                            await wdb.execute(
                                "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                                (src_file["location_id"], stub_rel, src_file["id"]),
                            )
                            await wdb.execute(
                                """UPDATE files SET
                                    filename=?, full_path=?, rel_path=?,
                                    file_type_high='text', file_type_low='moved',
                                    file_size=0, modified_date=?, date_last_seen=?
                                   WHERE id=?""",
                                (
                                    stub_name,
                                    stub_path,
                                    stub_rel,
                                    now_iso,
                                    now_iso,
                                    src_file["id"],
                                ),
                            )

                        _del_ids = [r["id"] for r in _replaced] + [src_file["id"]]
                        await remove_file_hashes(_del_ids)
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

                        await fs.write_moved_stub(
                            src_path,
                            src_file["filename"],
                            dest_canonical,
                            now_iso,
                            src_loc_id,
                            dest_location_name=dest_label,
                        )
                        st = await fs.file_stat(stub_path, src_loc_id)
                        if st:
                            async with db_writer() as wdb:
                                await wdb.execute(
                                    "UPDATE files SET file_size=? WHERE id=?",
                                    (st["size"], src_file["id"]),
                                )
                        stub_result = "duplicate + stubbed"
                    except Exception as e:
                        logger.warning("Stub write failed for %s: %s", src_path, e)
                        stub_result = "duplicate (stub failed)"
                else:
                    stub_result = "duplicate (source read-only)"

                # Best-effort: .sources at destination
                try:
                    await fs.write_or_append_sources(
                        dest_canonical,
                        src_file["location_name"],
                        src_file["rel_path"],
                        now_iso,
                        dest_loc_id,
                    )
                    dest_file_id = dest_entry.get("id")
                    if dest_file_id:
                        async with read_db() as db:
                            drows = await db.execute_fetchall(
                                "SELECT folder_id, rel_path FROM files WHERE id = ?",
                                (dest_file_id,),
                            )
                        if drows:
                            d_rel_dir = os.path.dirname(drows[0]["rel_path"])
                            await _upsert_sources_record(
                                dest_canonical,
                                dest_loc_id,
                                drows[0]["folder_id"],
                                d_rel_dir,
                                now_iso,
                            )
                except Exception as e:
                    logger.warning("Sources write failed for %s: %s", dest_canonical, e)

                await append_row(
                    csv_path,
                    dest_loc_id,
                    source_label,
                    src_path,
                    dest_label,
                    dest_canonical,
                    stub_result,
                )
            else:
                # Unique — copy to destination
                dest_rel_path = os.path.join(dest_info["abs_path"], src_rel)
                dest_dir = os.path.dirname(dest_rel_path)

                # Phase 1: Copy and register at destination
                try:
                    await fs.dir_create(dest_dir, dest_loc_id, exist_ok=True)
                    actual_dest = await fs.unique_dest_path(dest_rel_path, dest_loc_id)
                    await fs.copy_file(
                        src_path,
                        src_loc_id,
                        actual_dest,
                        dest_loc_id,
                        mtime=parse_mtime(src_file["modified_date"]),
                    )

                    (copy_fast,) = await fs.file_hash(actual_dest, dest_loc_id)
                    copy_strong = None
                    if copy_fast != hash_fast:
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

                    dest_file_rel_dir = os.path.relpath(
                        os.path.dirname(actual_dest), dest_info["root_path"]
                    )
                    if dest_file_rel_dir == ".":
                        dest_file_rel_dir = ""

                    dest_file_name = os.path.basename(actual_dest)
                    if dest_info["rel_prefix"]:
                        full_rel_dir = dest_info["rel_prefix"]
                        if dest_file_rel_dir:
                            raw_rel = os.path.relpath(
                                os.path.dirname(actual_dest), dest_info["abs_path"]
                            )
                            if raw_rel == ".":
                                full_rel_dir = dest_info["rel_prefix"]
                            else:
                                full_rel_dir = os.path.join(
                                    dest_info["rel_prefix"], raw_rel
                                )
                    else:
                        full_rel_dir = dest_file_rel_dir

                    dest_folder_id = dest_info["folder_id"]
                    if full_rel_dir:
                        dest_folder_id = await _ensure_folder_hierarchy(
                            dest_loc_id, full_rel_dir, folder_cache
                        )

                    dest_file_rel = (
                        os.path.join(full_rel_dir, dest_file_name)
                        if full_rel_dir
                        else dest_file_name
                    )
                    type_high, type_low = classify_file(dest_file_name)
                    st = await fs.file_stat(actual_dest, dest_loc_id)
                    file_size = st["size"] if st else 0

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

                    # Register hashes in hashes.db for the new file record
                    if new_dest_file_id and (copy_fast or copy_strong):
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
                                    copy_fast,
                                    copy_strong,
                                ),
                            )

                    await update_stats_for_files(
                        dest_loc_id,
                        added=[(dest_folder_id, file_size, type_high, is_hidden)],
                    )

                    dest_hash_index[copy_fast] = {
                        "id": new_dest_file_id,
                        "full_path": actual_dest,
                    }
                    affected_hashes.add(effective_hash)
                    files_copied += 1
                except Exception as e:
                    files_skipped += 1
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
                    continue

                # Phase 2: Stub source + .sources (best-effort)
                stub_result = "copied"
                src_dir = os.path.dirname(src_path)
                if src_dir not in writable_cache:
                    try:
                        test_path = os.path.join(src_dir, ".fh-write-test")
                        await fs.file_write_text(test_path, "", src_loc_id)
                        await fs.file_delete(test_path, src_loc_id)
                        writable_cache[src_dir] = True
                    except Exception:
                        writable_cache[src_dir] = False

                if writable_cache[src_dir]:
                    try:
                        stub_path = src_path + ".moved"
                        stub_name = src_file["filename"] + ".moved"
                        stub_rel = src_file["rel_path"] + ".moved"

                        async with db_writer() as wdb:
                            _replaced = await wdb.execute_fetchall(
                                "SELECT id FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                                (src_file["location_id"], stub_rel, src_file["id"]),
                            )
                            await wdb.execute(
                                "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                                (src_file["location_id"], stub_rel, src_file["id"]),
                            )
                            await wdb.execute(
                                """UPDATE files SET
                                    filename=?, full_path=?, rel_path=?,
                                    file_type_high='text', file_type_low='moved',
                                    file_size=0, modified_date=?, date_last_seen=?
                                   WHERE id=?""",
                                (
                                    stub_name,
                                    stub_path,
                                    stub_rel,
                                    now_iso,
                                    now_iso,
                                    src_file["id"],
                                ),
                            )

                        _del_ids = [r["id"] for r in _replaced] + [src_file["id"]]
                        await remove_file_hashes(_del_ids)
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

                        await fs.write_moved_stub(
                            src_path,
                            src_file["filename"],
                            actual_dest,
                            now_iso,
                            src_loc_id,
                            dest_location_name=dest_label,
                        )
                        st_stub = await fs.file_stat(stub_path, src_loc_id)
                        if st_stub:
                            async with db_writer() as wdb:
                                await wdb.execute(
                                    "UPDATE files SET file_size=? WHERE id=?",
                                    (st_stub["size"], src_file["id"]),
                                )
                        stub_result = "copied + stubbed"
                    except Exception as e:
                        logger.warning("Stub write failed for %s: %s", src_path, e)
                        stub_result = "copied (stub failed)"
                else:
                    stub_result = "copied (source read-only)"

                # Best-effort: .sources at destination
                try:
                    await fs.write_or_append_sources(
                        actual_dest,
                        src_file["location_name"],
                        src_file["rel_path"],
                        now_iso,
                        dest_loc_id,
                    )
                    await _upsert_sources_record(
                        actual_dest,
                        dest_loc_id,
                        dest_folder_id,
                        full_rel_dir,
                        now_iso,
                    )
                except Exception as e:
                    logger.warning("Sources write failed for %s: %s", actual_dest, e)

                await append_row(
                    csv_path,
                    dest_loc_id,
                    source_label,
                    src_path,
                    dest_label,
                    actual_dest,
                    stub_result,
                )

            # Throttled progress
            now = time.monotonic()
            if now - last_broadcast >= 0.5:
                last_broadcast = now
                await broadcast(
                    {
                        "type": "merge_progress",
                        "source": source_label,
                        "destination": dest_label,
                        "processed": processed,
                        "total": total_files,
                        "copied": files_copied,
                        "stubbed": files_stubbed,
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
                    "processed": processed,
                    "total": total_files,
                    "copied": files_copied,
                    "stubbed": files_stubbed,
                    "skipped": files_skipped,
                }
            )
        else:
            await broadcast(
                {
                    "type": "merge_completed",
                    "source": source_label,
                    "destination": dest_label,
                    "filesCopied": files_copied,
                    "filesStubbed": files_stubbed,
                    "filesSkipped": files_skipped,
                }
            )
        invalidate_stats_cache()
        for lid in {src_loc_id, dest_loc_id}:
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
        _merge_running = False
        _merge_cancel_requested = False


async def _load_source_files(db, source_id, source_info):
    """Load all files for the source (location or folder)."""
    kind, num_id = parse_prefixed_id(source_id)

    if kind == "loc":
        rows = await db.execute_fetchall(
            """SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.created_date, fi.modified_date,
                      fi.hidden, l.name as location_name
               FROM files fi
               JOIN locations l ON l.id = fi.location_id
               WHERE fi.location_id = ? AND fi.file_type_low != 'moved' AND fi.file_type_low != 'sources'""",
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
                 AND fi.file_type_low != 'moved' AND fi.file_type_low != 'sources'""",
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
    """Build a hash->file dict for all hashed files in the destination location."""
    # Get file IDs and paths from catalog
    file_rows = await db.execute_fetchall(
        "SELECT id, full_path FROM files WHERE location_id = ?",
        (location_id,),
    )
    if not file_rows:
        return {}

    # Get hashes from hashes.db
    file_ids = [r["id"] for r in file_rows]
    hash_map = await get_file_hashes(file_ids)

    path_by_id = {r["id"]: r["full_path"] for r in file_rows}
    result = {}
    for fid, h in hash_map.items():
        effective = h.get("hash_strong") or h.get("hash_fast")
        if effective:
            result[effective] = {"id": fid, "full_path": path_by_id[fid]}
    return result


async def _upsert_sources_record(
    canonical_path: str, dest_location_id, dest_folder_id, dest_rel_dir, now_iso
):
    """Insert or update the DB record for a .sources stub file."""
    sources_path = canonical_path + ".sources"
    sources_name = os.path.basename(sources_path)
    sources_rel = (
        os.path.join(dest_rel_dir, sources_name) if dest_rel_dir else sources_name
    )

    st = await fs.file_stat(sources_path, dest_location_id)
    if st is None:
        return

    async with read_db() as db:
        # Check if record already exists
        rows = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
            (dest_location_id, sources_rel),
        )

    async with db_writer() as wdb:
        if rows:
            await wdb.execute(
                "UPDATE files SET file_size=?, modified_date=?, date_last_seen=? WHERE id=?",
                (st["size"], now_iso, now_iso, rows[0]["id"]),
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
                    dest_location_id,
                    dest_folder_id,
                    st["size"],
                    now_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )


async def _ensure_folder_hierarchy(
    location_id: int,
    rel_dir_path: str,
    folder_cache: dict[str, int],
) -> int:
    """Create/find folder records for a full relative directory path."""
    async with read_db() as db:
        parts = rel_dir_path.split("/")
    current_path = ""
    parent_id = None

    for part in parts:
        current_path = f"{current_path}/{part}" if current_path else part

        if current_path in folder_cache:
            parent_id = folder_cache[current_path]
            continue

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
