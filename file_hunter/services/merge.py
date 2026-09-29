"""Merge logic — move or copy files to destination."""

import logging
import os
import time

from file_hunter_core.classify import classify_file
from file_hunter.db import db_writer, read_db, in_folder_tree
from file_hunter.hashes_db import (
    get_file_hashes,
    remove_file_hashes,
    set_file_hashes,
)
from file_hunter.helpers import (
    catalog_rel_paths,
    parse_mtime,
    parse_prefixed_id,
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
from file_hunter.services.scanner import ensure_folder_hierarchy
from file_hunter.services.provenance import (
    build_stub_text,
    file_folder,
    upsert_sources_record,
    write_stub_record,
)
from file_hunter.services.op_result_log import add_to_catalog, append_row, create_log
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.services.tags import copy_file_tags
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.stats_db import update_dup_counts_for_files, update_stats_for_files
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

merge_running: bool = False
merge_cancel_requested: bool = False


def is_merge_running() -> bool:
    """Check whether a merge operation is currently in progress.

    Returns:
        True if a merge task is running.
    """
    return merge_running


def request_merge_cancel():
    """Signal the running merge task to stop after the current file.

    Sets a module-level flag that run_merge checks at the top of each
    per-file iteration. The merge will finish the file it is currently
    processing, then exit cleanly with a merge_cancelled broadcast.
    """
    global merge_cancel_requested
    merge_cancel_requested = True


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
    """
    global merge_running, merge_cancel_requested
    merge_running = True
    merge_cancel_requested = False

    is_move = mode == "move"
    source_label = source_info["label"]
    dest_label = dest_info["label"]
    src_loc_id = source_info["location_id"]
    dest_loc_id = dest_info["location_id"]
    files_copied = 0
    files_skipped = 0
    files_duplicate = 0
    total_files = 0
    processed = 0
    last_broadcast = 0.0

    affected_hashes: set[str] = set()
    stub_dup_deltas: list[tuple[int | None, int]] = []  # (folder_id, -1) for each stubbed dup

    mode_label = "Moving" if is_move else "Copying"
    act_name = f"merge_{source_label}_{dest_label}"
    activity_register(act_name, f"{mode_label} {source_label} → {dest_label}")

    merge_ids = {
        "source": source_label,
        "destination": dest_label,
        "srcLocationId": src_loc_id,
        "destLocationId": dest_loc_id,
    }

    async def announce(msg_type, **fields):
        await broadcast({"type": msg_type, **merge_ids, **fields})

    try:
        await announce("merge_started", mode=mode)

        # Load source files (excludes stale, stubs, .sources)
        async with read_db() as db:
            source_files = await load_source_files(db, source_id, source_info)
        total_files = len(source_files)

        if total_files == 0:
            await announce(
                "merge_completed",
                mode=mode, filesCopied=0, filesDuplicate=0, filesSkipped=0,
            )
            return

        # Build destination hash index (excludes stale files, scoped to folder)
        dest_folder_id = dest_info["folder_id"]
        async with read_db() as db:
            dest_hash_index = await build_dest_hash_index(
                db, dest_loc_id, dest_folder_id
            )

        # Build set of destination rel_paths for catalog-based name collision
        async with read_db() as db:
            dest_rel_paths = await catalog_rel_paths(
                db, dest_loc_id, dest_folder_id
            )

        now_iso = utc_now()
        folder_cache: dict[str, tuple] = {}
        dir_created: set[str] = set()

        # Create result log CSV at destination
        csv_path = await create_log(dest_info["abs_path"], dest_loc_id, "merge")

        async def log_result(dest_path, result, detail=""):
            """One CSV row for the current source file (src_path)."""
            await append_row(
                csv_path, dest_loc_id, source_label, src_path, dest_label,
                dest_path, result, detail,
            )
        csv_folder_id = dest_info["folder_id"]

        for src_file in source_files:
            if merge_cancel_requested:
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
                    await log_result(dest_canonical, "duplicate (skipped)")
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
                        await record_merged_sources(
                            dest_canonical,
                            dest_loc_id,
                            dest_entry,
                            sources_entry,
                            now_iso,
                        )

                        # 2. Write .moved stub at source
                        stub_text = build_stub_text(
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
                        await stub_source_record(
                            src_file, stub_text, now_iso, stub_dup_deltas
                        )

                        files_duplicate += 1
                        await log_result(dest_canonical, "duplicate + stubbed")
                    except FileNotFoundError:
                        files_skipped += 1
                        await log_result("", "skipped (file missing)")
                    except Exception as e:
                        files_skipped += 1
                        logger.warning("Merge stub failed for %s: %s", src_path, e)
                        await log_result(dest_canonical, "error", str(e))

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

                    dest_file_name = unique_rel_name(
                        dest_rel_paths, full_rel_dir, dest_file_name
                    )
                    dest_file_rel = os.path.join(full_rel_dir, dest_file_name)
                    actual_dest = os.path.join(
                        os.path.dirname(dest_rel_path), dest_file_name
                    )

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
                        await log_result(actual_dest, "skipped (hash mismatch)")
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
                        stub_text = build_stub_text(
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
                        async with db_writer() as wdb:
                            dest_folder_id = (
                                await ensure_folder_hierarchy(
                                    wdb, dest_loc_id, full_rel_dir, folder_cache
                                )
                            )[0]

                    type_high, type_low = classify_file(dest_file_name)
                    file_size = src_file["file_size"] or 0

                    async with db_writer() as wdb:
                        await wdb.execute(
                            """INSERT OR IGNORE INTO files
                               (filename, full_path, rel_path, location_id, folder_id,
                                file_type_high, file_type_low, file_size,
                                description,
                                created_date, modified_date, date_cataloged, date_last_seen,
                                scan_id, hidden)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                            (
                                dest_file_name,
                                actual_dest,
                                dest_file_rel,
                                dest_loc_id,
                                dest_folder_id,
                                type_high,
                                type_low,
                                file_size,
                                src_file.get("description") or "",
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
                        if new_dest_file_id:
                            await copy_file_tags(wdb, src_file["id"], new_dest_file_id)

                    # Register hashes for the new destination file
                    if new_dest_file_id:
                        await set_file_hashes(
                            new_dest_file_id,
                            dest_loc_id,
                            file_size,
                            src_file.get("hash_partial"),
                            copy_hash,
                            hash_strong,
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
                        await stub_source_record(
                            src_file, stub_text, now_iso, stub_dup_deltas
                        )
                        await record_merged_sources(
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
                    await log_result(actual_dest, "copied + stubbed" if is_move else "copied")

                except FileNotFoundError:
                    files_skipped += 1
                    await log_result("", "skipped (file missing)")
                except Exception as e:
                    files_skipped += 1
                    logger.warning("Merge error for %s: %s", src_path, e)
                    await log_result("", "error", str(e))

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
                        "duplicate": files_duplicate,
                        "skipped": files_skipped,
                    }
                )

        # Add result CSV to catalog
        await add_to_catalog(csv_path, dest_loc_id, csv_folder_id)

        if merge_cancel_requested:
            await announce(
                "merge_cancelled",
                mode=mode,
                processed=processed,
                total=total_files,
                copied=files_copied,
                duplicate=files_duplicate,
                skipped=files_skipped,
            )
        else:
            await announce(
                "merge_completed",
                mode=mode,
                filesCopied=files_copied,
                filesDuplicate=files_duplicate,
                filesSkipped=files_skipped,
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

        # Apply dup count deltas for stubbed source files
        if stub_dup_deltas:
            await update_dup_counts_for_files(src_loc_id, stub_dup_deltas)

        affected_strong = {h for h in affected_hashes if len(h) == 64}
        affected_fast = {h for h in affected_hashes if len(h) == 16}
        await recalculate_dup_counts(
            strong_hashes=affected_strong or None,
            fast_hashes=affected_fast or None,
            source=f"merge {source_label} → {dest_label}",
        )

    except Exception as exc:
        await announce("merge_error", error=str(exc))

    finally:
        activity_unregister(act_name)
        merge_running = False
        merge_cancel_requested = False


async def stub_source_record(src_file, stub_text, now_iso, stub_dup_deltas):
    """Turn the source file's record into its .moved stub, drop its hashes,
    and swap it for the stub in the stats. Dup-count deltas are collected in
    stub_dup_deltas and applied once at the end of the merge.
    """
    stub_rel = src_file["rel_path"] + ".moved"

    async with db_writer() as wdb:
        # Remove any existing record at the stub's rel_path
        await wdb.execute(
            "DELETE FROM files WHERE location_id=? AND rel_path=? AND id!=?",
            (src_file["location_id"], stub_rel, src_file["id"]),
        )
        await write_stub_record(
            wdb,
            src_file["id"],
            src_file["filename"] + ".moved",
            src_file["full_path"] + ".moved",
            stub_rel,
            len(stub_text.encode()),
            now_iso,
        )

    await remove_file_hashes([src_file["id"]])
    if src_file["dup_count"] > 0:
        stub_dup_deltas.append((src_file["folder_id"], -1))
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


async def record_merged_sources(
    canonical_path, dest_loc_id, dest_entry, sources_entry, now_iso
):
    """dest_entry carries the destination file's id and, when known, its
    folder_id and rel_dir; otherwise they're looked up from the file."""
    dest_file_id = dest_entry.get("id")
    dest_folder_id = dest_entry.get("folder_id")
    dest_rel_dir = dest_entry.get("rel_dir", "")

    if dest_file_id and dest_folder_id is None:
        found = await file_folder(dest_file_id)
        if found:
            dest_folder_id, dest_rel_dir = found

    await upsert_sources_record(
        canonical_path,
        dest_loc_id,
        dest_folder_id,
        dest_rel_dir,
        sources_entry,
        now_iso,
    )


async def load_source_files(db, source_id, source_info):
    """Load non-stale, non-stub files for the source location or folder."""
    kind, num_id = parse_prefixed_id(source_id)

    if kind == "loc":
        rows = await db.execute_fetchall(
            """SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.description,
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
            f"""SELECT fi.id, fi.filename, fi.full_path, fi.rel_path, fi.location_id,
                      fi.folder_id, fi.file_type_high, fi.file_type_low, fi.file_size,
                      fi.description,
                      fi.created_date, fi.modified_date,
                      fi.hidden, l.name as location_name
               FROM files fi
               JOIN locations l ON l.id = fi.location_id
               WHERE {in_folder_tree("fi.folder_id")}
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
            r["hash_partial"] = h.get("hash_partial")
            r["hash_fast"] = h.get("hash_fast")
            r["hash_strong"] = h.get("hash_strong")
            r["dup_count"] = h.get("dup_count") or 0

    return result


async def build_dest_hash_index(db, location_id, folder_id=None):
    """Build hash→file dict for non-stale hashed files in the destination folder.

    When folder_id is set, scopes to that folder and its descendants.
    When None, scopes to the entire location.
    """
    if folder_id:
        file_rows = await db.execute_fetchall(
            f"""SELECT fi.id, fi.full_path, fi.folder_id, fi.rel_path
               FROM files fi
               WHERE fi.location_id = ? AND fi.stale = 0
                 AND {in_folder_tree("fi.folder_id")}""",
            (location_id, folder_id),
        )
    else:
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
