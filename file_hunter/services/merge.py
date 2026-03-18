"""Merge logic — copy unique files to destination, stub duplicates at source."""

import os
import time
from datetime import datetime, timezone

from file_hunter_core.classify import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.services import fs
from file_hunter.ws.scan import broadcast

_merge_running: bool = False
_merge_cancel_requested: bool = False


def is_merge_running() -> bool:
    return _merge_running


def request_merge_cancel():
    global _merge_cancel_requested
    _merge_cancel_requested = True


async def resolve_merge_target(db, target_id: str) -> dict | None:
    """Resolve loc-N or fld-N to target info dict.

    Returns { label, abs_path, root_path, location_id, folder_id, rel_prefix }
    or None if not found.
    """
    if target_id.startswith("loc-"):
        loc_id = int(target_id[4:])
        rows = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE id = ?", (loc_id,)
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

    elif target_id.startswith("fld-"):
        fld_id = int(target_id[4:])
        rows = await db.execute_fetchall(
            """SELECT f.id, f.name, f.rel_path, f.location_id, l.name as loc_name, l.root_path
               FROM folders f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (fld_id,),
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
    """Main merge background task.

    Reads via read_db(), writes via db_writer(). No owned connection.
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
    hidden_folder_map: dict[str, str] = {}

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

        for src_file in source_files:
            if _merge_cancel_requested:
                break

            processed += 1

            # Check source file exists on disk
            src_path = src_file["full_path"]
            exists = await fs.file_exists(src_path, src_loc_id)
            if not exists:
                files_skipped += 1
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

            # --- Hidden file branch: copy-only, no hashing, no dup check ---
            if src_file["hidden"]:
                try:
                    # Determine destination path with collision handling
                    src_rel_parts = src_rel.replace("\\", "/").split("/")

                    # Find the first hidden folder component (excluding filename)
                    hidden_folder_idx = None
                    for idx, part in enumerate(src_rel_parts[:-1]):
                        if part.startswith("."):
                            hidden_folder_idx = idx
                            break

                    if hidden_folder_idx is not None:
                        # File is inside a hidden folder — resolve folder collision
                        hidden_folder_rel = "/".join(
                            src_rel_parts[: hidden_folder_idx + 1]
                        )
                        if hidden_folder_rel not in hidden_folder_map:
                            dest_folder_abs = os.path.join(
                                dest_info["abs_path"], hidden_folder_rel
                            )
                            actual_folder = await fs.unique_hidden_path(
                                dest_folder_abs, dest_loc_id
                            )
                            hidden_folder_map[hidden_folder_rel] = os.path.basename(
                                actual_folder
                            )
                        # Rewrite src_rel with renamed hidden folder
                        new_parts = list(src_rel_parts)
                        new_parts[hidden_folder_idx] = hidden_folder_map[
                            hidden_folder_rel
                        ]
                        dest_rel = "/".join(new_parts)
                        actual_dest = os.path.join(dest_info["abs_path"], dest_rel)
                    else:
                        # Standalone dotfile — file-level collision
                        dest_path_candidate = os.path.join(
                            dest_info["abs_path"], src_rel
                        )
                        actual_dest = await fs.unique_hidden_path(
                            dest_path_candidate, dest_loc_id
                        )
                        dest_rel = os.path.relpath(actual_dest, dest_info["abs_path"])

                    dest_dir = os.path.dirname(actual_dest)
                    await fs.dir_create(dest_dir, dest_loc_id, exist_ok=True)
                    await fs.copy_file(src_path, src_loc_id, actual_dest, dest_loc_id)

                    # Build folder hierarchy at destination
                    dest_file_rel_dir = os.path.dirname(dest_rel)
                    if dest_info["rel_prefix"]:
                        if dest_file_rel_dir:
                            full_rel_dir = os.path.join(
                                dest_info["rel_prefix"], dest_file_rel_dir
                            )
                        else:
                            full_rel_dir = dest_info["rel_prefix"]
                    else:
                        full_rel_dir = dest_file_rel_dir

                    dest_folder_id = dest_info["folder_id"]
                    if full_rel_dir:
                        dest_folder_id = await _ensure_folder_hierarchy(
                            dest_loc_id, full_rel_dir, folder_cache, hidden=True
                        )

                    dest_file_name = os.path.basename(actual_dest)
                    dest_file_rel = (
                        os.path.join(full_rel_dir, dest_file_name)
                        if full_rel_dir
                        else dest_file_name
                    )
                    type_high, type_low = classify_file(dest_file_name)
                    st = await fs.file_stat(actual_dest, dest_loc_id)
                    file_size = st["size"] if st else 0

                    # DB writes for hidden file
                    stub_path = src_path + ".moved"
                    stub_name = src_file["filename"] + ".moved"
                    stub_rel = src_file["rel_path"] + ".moved"

                    async with db_writer() as wdb:
                        # Insert destination record (hidden=1, no hashes)
                        await wdb.execute(
                            """INSERT OR IGNORE INTO files
                               (filename, full_path, rel_path, location_id, folder_id,
                                file_type_high, file_type_low, file_size,
                                hash_fast, hash_strong, description, tags,
                                created_date, modified_date, date_cataloged, date_last_seen,
                                scan_id, hidden)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, '', '', ?, ?, ?, ?, NULL, 1)""",
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
                            ),
                        )

                        # Stub source with .moved
                        await wdb.execute(
                            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                            (src_file["location_id"], stub_rel, src_file["id"]),
                        )
                        await wdb.execute(
                            """UPDATE files SET
                                filename=?, full_path=?, rel_path=?,
                                file_type_high='text', file_type_low='moved',
                                file_size=0, hash_fast=NULL, hash_strong=NULL,
                                hash_partial=NULL, modified_date=?, date_last_seen=?
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

                    # Filesystem I/O outside write lock
                    await fs.write_moved_stub(
                        src_path, src_file["filename"], actual_dest, now_iso, src_loc_id
                    )
                    st_stub = await fs.file_stat(stub_path, src_loc_id)
                    if st_stub:
                        async with db_writer() as wdb:
                            await wdb.execute(
                                "UPDATE files SET file_size=? WHERE id=?",
                                (st_stub["size"], src_file["id"]),
                            )

                    files_copied += 1
                except Exception:
                    files_skipped += 1

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

            # --- Non-hidden file: hash, dup-check, copy/stub ---

            # Hash file if needed
            hash_strong = src_file["hash_strong"]
            hash_fast = src_file["hash_fast"]
            if not hash_strong:
                try:
                    hash_fast, hash_strong = await fs.file_hash(
                        src_path, src_loc_id, strong=True
                    )
                    files_hashed += 1
                    # Update source file record with hash
                    async with db_writer() as wdb:
                        await wdb.execute(
                            "UPDATE files SET hash_fast=?, hash_strong=? WHERE id=?",
                            (hash_fast, hash_strong, src_file["id"]),
                        )
                except (PermissionError, FileNotFoundError, OSError, ConnectionError):
                    files_skipped += 1
                    continue

            # Check if duplicate exists in destination location
            if hash_strong in dest_hash_index:
                # Duplicate — stub at source, append .sources at destination
                try:
                    dest_entry = dest_hash_index[hash_strong]
                    dest_canonical = dest_entry["full_path"]

                    # DB operations first (before touching filesystem)
                    stub_path = src_path + ".moved"
                    stub_name = src_file["filename"] + ".moved"
                    stub_rel = src_file["rel_path"] + ".moved"

                    async with db_writer() as wdb:
                        # Remove any stale .moved record (e.g. from a previous merge)
                        await wdb.execute(
                            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                            (src_file["location_id"], stub_rel, src_file["id"]),
                        )
                        await wdb.execute(
                            """UPDATE files SET
                                filename=?, full_path=?, rel_path=?,
                                file_type_high='text', file_type_low='moved',
                                file_size=0, hash_fast=NULL, hash_strong=NULL,
                                hash_partial=NULL, modified_date=?, date_last_seen=?
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

                    # Filesystem: write .moved stub at source (outside write lock)
                    await fs.write_moved_stub(
                        src_path,
                        src_file["filename"],
                        dest_canonical,
                        now_iso,
                        src_loc_id,
                    )
                    # Update file_size now that stub exists
                    st = await fs.file_stat(stub_path, src_loc_id)
                    if st:
                        async with db_writer() as wdb:
                            await wdb.execute(
                                "UPDATE files SET file_size=? WHERE id=?",
                                (st["size"], src_file["id"]),
                            )

                    # Write/append .sources next to destination file
                    await fs.write_or_append_sources(
                        dest_canonical,
                        src_file["location_name"],
                        src_file["rel_path"],
                        now_iso,
                        dest_loc_id,
                    )
                    # Resolve dest file's rel_dir for the .sources DB record
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
                    affected_hashes.add(hash_strong)
                    files_stubbed += 1
                except Exception:
                    files_skipped += 1
            else:
                # Unique — copy to destination
                dest_rel_path = os.path.join(dest_info["abs_path"], src_rel)
                dest_dir = os.path.dirname(dest_rel_path)

                try:
                    await fs.dir_create(dest_dir, dest_loc_id, exist_ok=True)

                    # Handle filename collision
                    actual_dest = await fs.unique_dest_path(dest_rel_path, dest_loc_id)

                    await fs.copy_file(src_path, src_loc_id, actual_dest, dest_loc_id)

                    # Hash-verify the copy
                    copy_fast, copy_strong = await fs.file_hash(
                        actual_dest, dest_loc_id, strong=True
                    )
                    if copy_strong != hash_strong:
                        await fs.file_delete(actual_dest, dest_loc_id)
                        files_skipped += 1
                        continue

                    # Build folder hierarchy at destination
                    dest_file_rel_dir = os.path.relpath(
                        os.path.dirname(actual_dest), dest_info["root_path"]
                    )
                    if dest_file_rel_dir == ".":
                        dest_file_rel_dir = ""

                    # Full relative path for destination within the location
                    dest_file_name = os.path.basename(actual_dest)
                    if dest_info["rel_prefix"]:
                        full_rel_dir = dest_info["rel_prefix"]
                        if dest_file_rel_dir:
                            # Strip the dest rel_prefix from dest_file_rel_dir if present
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

                    stub_path = src_path + ".moved"
                    stub_name = src_file["filename"] + ".moved"
                    stub_rel = src_file["rel_path"] + ".moved"

                    async with db_writer() as wdb:
                        await wdb.execute(
                            """INSERT OR IGNORE INTO files
                               (filename, full_path, rel_path, location_id, folder_id,
                                file_type_high, file_type_low, file_size,
                                hash_fast, hash_strong, description, tags,
                                created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, NULL)""",
                            (
                                dest_file_name,
                                actual_dest,
                                dest_file_rel,
                                dest_loc_id,
                                dest_folder_id,
                                type_high,
                                type_low,
                                file_size,
                                copy_fast,
                                copy_strong,
                                src_file.get("created_date", now_iso),
                                src_file.get("modified_date", now_iso),
                                now_iso,
                                now_iso,
                            ),
                        )

                        # Look up the new dest file's DB id
                        new_rows = await wdb.execute_fetchall(
                            "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
                            (dest_loc_id, dest_file_rel),
                        )
                        new_dest_file_id = new_rows[0]["id"] if new_rows else None

                        # DB operations for source stub
                        # Remove any stale .moved record (e.g. from a previous merge)
                        await wdb.execute(
                            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
                            (src_file["location_id"], stub_rel, src_file["id"]),
                        )
                        await wdb.execute(
                            """UPDATE files SET
                                filename=?, full_path=?, rel_path=?,
                                file_type_high='text', file_type_low='moved',
                                file_size=0, hash_fast=NULL, hash_strong=NULL,
                                hash_partial=NULL, modified_date=?, date_last_seen=?
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

                    # Add to index so subsequent files with same hash are caught
                    dest_hash_index[copy_strong] = {
                        "id": new_dest_file_id,
                        "full_path": actual_dest,
                    }

                    # Filesystem: write .moved stub at source (outside write lock)
                    await fs.write_moved_stub(
                        src_path, src_file["filename"], actual_dest, now_iso, src_loc_id
                    )
                    st_stub = await fs.file_stat(stub_path, src_loc_id)
                    if st_stub:
                        async with db_writer() as wdb:
                            await wdb.execute(
                                "UPDATE files SET file_size=? WHERE id=?",
                                (st_stub["size"], src_file["id"]),
                            )

                    # Write .sources next to the new destination file
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

                    affected_hashes.add(hash_strong)
                    files_copied += 1
                except Exception:
                    files_skipped += 1
                    continue

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
        from file_hunter.services.stats import invalidate_stats_cache

        invalidate_stats_cache()

        from file_hunter.services.sizes import recalculate_location_sizes

        for lid in {src_loc_id, dest_loc_id}:
            try:
                await recalculate_location_sizes(lid)
            except Exception:
                pass

        from file_hunter.services.dup_counts import recalculate_dup_counts

        await recalculate_dup_counts(
            strong_hashes=affected_hashes, source=f"merge {source_label} → {dest_label}"
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
    if source_id.startswith("loc-"):
        loc_id = int(source_id[4:])
        rows = await db.execute_fetchall(
            """SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.hash_fast, fi.hash_strong, fi.created_date, fi.modified_date,
                      fi.hidden, l.name as location_name
               FROM files fi
               JOIN locations l ON l.id = fi.location_id
               WHERE fi.location_id = ? AND fi.file_type_low != 'moved' AND fi.file_type_low != 'sources'""",
            (loc_id,),
        )
    elif source_id.startswith("fld-"):
        fld_id = int(source_id[4:])
        rows = await db.execute_fetchall(
            """WITH RECURSIVE descendants(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
               )
               SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.hash_fast, fi.hash_strong, fi.created_date, fi.modified_date,
                      fi.hidden, l.name as location_name
               FROM files fi
               JOIN locations l ON l.id = fi.location_id
               WHERE fi.folder_id IN (SELECT id FROM descendants)
                 AND fi.file_type_low != 'moved' AND fi.file_type_low != 'sources'""",
            (fld_id,),
        )
    else:
        return []

    return [dict(r) for r in rows]


async def _build_dest_hash_index(db, location_id):
    """Build a hash->file dict for all hashed files in the destination location."""
    rows = await db.execute_fetchall(
        "SELECT id, hash_strong, full_path FROM files WHERE location_id = ? AND hash_strong IS NOT NULL",
        (location_id,),
    )
    return {
        row["hash_strong"]: {"id": row["id"], "full_path": row["full_path"]}
        for row in rows
    }


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
                    hash_fast, hash_strong, description, tags,
                    created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                   VALUES (?, ?, ?, ?, ?, 'text', 'sources', ?, NULL, NULL, '', '',
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
    hidden: bool = False,
) -> int:
    """Create/find folder records for a full relative directory path."""
    async with read_db() as db:
        parts = rel_dir_path.replace("\\", "/").split("/")
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
            async with db_writer() as wdb:
                if hidden and part.startswith("."):
                    cursor = await wdb.execute(
                        "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden) VALUES (?, ?, ?, ?, 1)",
                        (location_id, parent_id, part, current_path),
                    )
                else:
                    cursor = await wdb.execute(
                        "INSERT INTO folders (location_id, parent_id, name, rel_path) VALUES (?, ?, ?, ?)",
                        (location_id, parent_id, part, current_path),
                    )
                folder_id = cursor.lastrowid

        folder_cache[current_path] = folder_id
        parent_id = folder_id

    return parent_id
