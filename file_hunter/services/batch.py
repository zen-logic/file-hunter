"""Batch operations — delete, move, tag, and download multiple items."""

import logging

from file_hunter.db import db_writer, read_db, folder_tree_ids, id_batches
from file_hunter.hashes_db import get_file_hashes, read_hashes
from file_hunter.helpers import (
    expand_to_duplicates,
    post_op_stats,
    resolve_target,
)
from file_hunter.services import fs
from file_hunter.services.activity import register, unregister, update
from file_hunter.services.deferred_ops import queue_deferred_op
from file_hunter.services.delete import delete_folder, settle_deleted_files
from file_hunter.services.files import move_file
from file_hunter.services.locations import move_folder
from file_hunter.services.op_result_log import create_log, insert_written_file
from file_hunter.services.tags import add_tags_to_files, parse_tags, remove_file_tags
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def batch_delete(
    file_ids: list[int], folder_ids: list[int], all_duplicates: bool = False
):
    """Delete multiple files and folders from disk and catalog as a background task.

    Folders are deleted first (they may contain listed files). Files on offline
    locations are queued as deferred ops. When all_duplicates is True, the file
    list is expanded to include all files sharing the same hash (strong or fast)
    before deletion.

    Runs as a background task — broadcasts progress and completion via WebSocket.
    """
    total = len(file_ids) + len(folder_ids)
    act_name = f"batch-delete-{id(file_ids)}"
    register(act_name, "Deleting files", f"0/{total}")
    done = 0

    deleted_files = 0
    deleted_folders = 0
    deleted_from_disk = 0

    try:
        # Delete folders first (they may contain some of the listed files)
        for fid in folder_ids:
            async with db_writer() as db:
                result = await delete_folder(db, fid)
            if result:
                deleted_folders += 1
                if result.get("deleted_from_disk"):
                    deleted_from_disk += 1
            done += 1
            update(act_name, progress=f"{done}/{total}")

        if not file_ids:
            await broadcast(
                {
                    "type": "batch_deleted",
                    "deletedFiles": 0,
                    "deletedFolders": deleted_folders,
                }
            )
            return

        # Expand to include duplicates if requested
        if all_duplicates:
            h_map = await get_file_hashes(file_ids)
            strong_set = {
                h["hash_strong"] for h in h_map.values() if h.get("hash_strong")
            }
            fast_set = {
                h["hash_fast"]
                for h in h_map.values()
                if not h.get("hash_strong") and h.get("hash_fast")
            }

            all_ids = set(file_ids)
            async with read_hashes() as hdb:
                if strong_set:
                    ph = ",".join("?" for _ in strong_set)
                    rows = await hdb.execute_fetchall(
                        f"SELECT file_id FROM active_hashes WHERE hash_strong IN ({ph})",
                        list(strong_set),
                    )
                    all_ids.update(r["file_id"] for r in rows)
                if fast_set:
                    ph = ",".join("?" for _ in fast_set)
                    rows = await hdb.execute_fetchall(
                        f"SELECT file_id FROM active_hashes "
                        f"WHERE hash_fast IN ({ph}) AND hash_strong IS NULL",
                        list(fast_set),
                    )
                    all_ids.update(r["file_id"] for r in rows)
            file_ids = list(all_ids)

        # Load all file records in one query
        async with read_db() as db:
            ph = ",".join("?" for _ in file_ids)
            all_rows = await db.execute_fetchall(
                f"""SELECT f.id, f.filename, f.full_path, f.location_id, f.folder_id,
                          f.file_size, f.file_type_high, f.hidden, l.root_path
                   FROM files f
                   JOIN locations l ON l.id = f.location_id
                   WHERE f.id IN ({ph})""",
                file_ids,
            )
        if not all_rows:
            await broadcast(
                {
                    "type": "batch_deleted",
                    "deletedFiles": 0,
                    "deletedFolders": deleted_folders,
                }
            )
            return

        # Get hashes for dup recalc
        fids = [r["id"] for r in all_rows]
        h_map = await get_file_hashes(fids)

        # Check online once per location
        online_cache: dict[int, bool] = {}
        deleted_ids: list[int] = []
        affected_strong: set[str] = set()
        affected_fast: set[str] = set()

        for rec in all_rows:
            fid = rec["id"]
            loc_id = rec["location_id"]

            if loc_id not in online_cache:
                try:
                    online_cache[loc_id] = await fs.dir_exists(rec["root_path"], loc_id)
                except Exception:
                    online_cache[loc_id] = False

            if online_cache[loc_id]:
                try:
                    await fs.file_delete(rec["full_path"], loc_id)
                    deleted_from_disk += 1
                except FileNotFoundError:
                    pass  # already gone from disk
                except Exception:
                    pass  # file delete failed, still remove from catalog
                deleted_ids.append(fid)
                deleted_files += 1
            else:
                async with db_writer() as db:
                    await queue_deferred_op(db, fid, loc_id, "delete")

            h = h_map.get(fid, {})
            if h.get("hash_strong"):
                affected_strong.add(h["hash_strong"])
            elif h.get("hash_fast"):
                affected_fast.add(h["hash_fast"])

            done += 1
            update(act_name, progress=f"{done}/{total}")
            await broadcast(
                {
                    "type": "batch_delete_progress",
                    "done": done,
                    "total": total,
                    "name": rec["filename"],
                }
            )

        # Bulk delete from catalog
        if deleted_ids:
            async with db_writer() as db:
                for batch, bph in id_batches(deleted_ids):
                    await db.execute(f"DELETE FROM files WHERE id IN ({bph})", batch)

            deleted = set(deleted_ids)
            await settle_deleted_files([r for r in all_rows if r["id"] in deleted])

        await post_op_stats(
            strong_hashes=affected_strong or None,
            fast_hashes=affected_fast or None,
            source=f"batch delete ({len(file_ids)} files)",
        )

        await broadcast(
            {
                "type": "batch_deleted",
                "deletedFiles": deleted_files,
                "deletedFolders": deleted_folders,
            }
        )
    except Exception as exc:
        logger.exception("Batch delete failed: %s", exc)
        await broadcast(
            {
                "type": "batch_delete_error",
                "error": str(exc),
            }
        )
    finally:
        unregister(act_name)
        await broadcast({"type": "status_bar_idle"})


async def batch_move(
    db, file_ids: list[int], folder_ids: list[int], destination_folder_id: str,
    *, copy: bool = False,
) -> dict:
    """Move or copy multiple files and folders to a single destination.

    Folders are processed first, then files. Each operation is individual (not
    transactional) so partial success is possible — errors are collected and
    returned. Per-file post_op_stats is skipped; a single post_op_stats runs
    at the end covering all affected locations.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_ids: List of numeric file IDs to move/copy.
        folder_ids: List of numeric folder IDs to move/copy.
        destination_folder_id: Prefixed target ID ("loc-{id}" or "fld-{id}").
        copy: If True, copy instead of move — source items are left untouched.

    Returns:
        dict with keys: moved_files (int), moved_folders (int),
        errors (list[str] — per-item error messages for failed operations).
    """
    total = len(file_ids) + len(folder_ids)
    verb = "Copying" if copy else "Moving"
    act_name = f"batch-{'copy' if copy else 'move'}-{id(file_ids)}"
    register(act_name, f"{verb} files", f"0/{total}")

    moved_files = 0
    moved_folders = 0
    done = 0
    errors = []
    affected_loc_ids: set[int] = set()

    # Pre-fetch names for progress reporting
    name_map = {}
    if file_ids:
        ph = ",".join("?" for _ in file_ids)
        name_rows = await db.execute_fetchall(
            f"SELECT id, filename FROM files WHERE id IN ({ph})", file_ids
        )
        name_map = {r["id"]: r["filename"] for r in name_rows}
    if folder_ids:
        ph = ",".join("?" for _ in folder_ids)
        fld_rows = await db.execute_fetchall(
            f"SELECT id, name FROM folders WHERE id IN ({ph})", folder_ids
        )
        for r in fld_rows:
            name_map[r["id"]] = r["name"]

    # Resolve destination once — used for CSV and post_op_stats
    dest = await resolve_target(db, destination_folder_id)
    dest_loc_id = dest["location_id"] if dest else None

    # Capture source locations before moves change them
    if file_ids:
        ph = ",".join("?" for _ in file_ids)
        src_rows = await db.execute_fetchall(
            f"SELECT DISTINCT location_id FROM files WHERE id IN ({ph})", file_ids
        )
        for r in src_rows:
            affected_loc_ids.add(r["location_id"])
    if folder_ids:
        ph = ",".join("?" for _ in folder_ids)
        src_rows = await db.execute_fetchall(
            f"SELECT DISTINCT location_id FROM folders WHERE id IN ({ph})", folder_ids
        )
        for r in src_rows:
            affected_loc_ids.add(r["location_id"])

    # Shared CSV for batch file moves (not copies)
    csv_path = None
    csv_loc_id = None
    csv_folder_id = None
    if file_ids and not copy and dest:
        csv_path = await create_log(dest["abs_path"], dest_loc_id, "move")
        csv_loc_id = dest_loc_id
        csv_folder_id = dest.get("folder_id")

    try:
        # Move folders
        for fid in folder_ids:
            name = name_map.get(fid, f"Folder {fid}")
            await broadcast(
                {
                    "type": "batch_move_progress",
                    "done": done,
                    "total": total,
                    "name": name,
                }
            )
            try:
                await move_folder(db, fid, destination_folder_id, copy=copy)
                moved_folders += 1
            except (ValueError, OSError) as e:
                errors.append(f"Folder {fid}: {e}")
            done = moved_folders + moved_files
            update(act_name, progress=f"{done}/{total}")

        # Move files
        for fid in file_ids:
            name = name_map.get(fid, f"File {fid}")
            await broadcast(
                {
                    "type": "batch_move_progress",
                    "done": done,
                    "total": total,
                    "name": name,
                }
            )
            try:
                await move_file(
                    db,
                    fid,
                    destination_folder_id=destination_folder_id,
                    skip_post_processing=True,
                    copy=copy,
                    shared_csv_path=csv_path,
                    shared_csv_loc_id=csv_loc_id,
                )
                moved_files += 1
            except (ValueError, OSError) as e:
                errors.append(f"File {fid}: {e}")
            done = moved_folders + moved_files
            update(act_name, progress=f"{done}/{total}")
    finally:
        unregister(act_name)

    # Add shared CSV to catalog
    if csv_path and moved_files > 0:
        await insert_written_file(db, csv_path, csv_loc_id, csv_folder_id)

    # Post-processing once — recalc all affected locations
    if dest_loc_id:
        affected_loc_ids.add(dest_loc_id)
    await post_op_stats(
        location_ids=affected_loc_ids or None,
        source=f"batch {'copy' if copy else 'move'} ({moved_files} files, {moved_folders} folders)",
    )

    return {
        "moved_files": moved_files,
        "moved_folders": moved_folders,
        "errors": errors,
    }


async def batch_tag(
    file_ids: list[int], add_tags: list[str], remove_tags: list[str]
):
    """Add and/or remove tags on multiple files as a background task.

    Additions propagate to every active duplicate of the selected files —
    the same copies the UI's dup badge counts. Removals apply only to the
    files the user actually selected. Broadcasts completion via WebSocket.

    Runs as a background task — registers activity and broadcasts completion.
    """
    add_tags = parse_tags(add_tags)
    remove_tags = parse_tags(remove_tags)

    act_name = f"batch-tag-{id(file_ids)}"
    tag_label = ", ".join(add_tags) if add_tags else ", ".join(remove_tags)
    register(act_name, "Writing tags...", tag_label)
    updated = 0
    propagated = 0

    try:
        async with read_db() as db:
            ph = ",".join("?" for _ in file_ids)
            rows = await db.execute_fetchall(
                f"SELECT id FROM files WHERE id IN ({ph})", list(file_ids)
            )
        valid_ids = [r["id"] for r in rows]
        updated = len(valid_ids)

        if add_tags and valid_ids:
            targets = await expand_to_duplicates(valid_ids)
            propagated = len(targets) - len(valid_ids)
            async with db_writer() as db:
                await add_tags_to_files(db, targets, add_tags)

        if remove_tags:
            for fid in valid_ids:
                async with db_writer() as db:
                    await remove_file_tags(db, fid, remove_tags)
    finally:
        unregister(act_name)

    await broadcast({
        "type": "batch_tag_completed",
        "updated": updated,
        "propagated": propagated,
        "add_tags": add_tags,
        "remove_tags": remove_tags,
    })


async def batch_collect_files(
    db, file_ids: list[int], folder_ids: list[int]
) -> list[tuple[str, str, int]]:
    """Collect file paths for a batch download.

    Returns list of (full_path, arc_name, location_id).
    """
    all_files: list[tuple[str, str, int]] = []

    # Direct files
    if file_ids:
        placeholders = ",".join("?" * len(file_ids))
        rows = await db.execute_fetchall(
            f"""SELECT f.full_path, f.filename, f.location_id
                FROM files f
                WHERE f.id IN ({placeholders})""",
            file_ids,
        )
        for r in rows:
            all_files.append((r["full_path"], r["filename"], r["location_id"]))

    # Folder contents (recursive)
    for fid in folder_ids:
        frow = await db.execute_fetchall(
            """SELECT fld.name, fld.rel_path, fld.location_id
               FROM folders fld
               WHERE fld.id = ?""",
            (fid,),
        )
        if not frow:
            continue
        folder_name = frow[0]["name"]
        folder_rel = frow[0]["rel_path"]
        folder_loc_id = frow[0]["location_id"]

        desc_ids = await folder_tree_ids(db, fid)

        placeholders = ",".join("?" * len(desc_ids))
        files = await db.execute_fetchall(
            f"SELECT full_path, rel_path FROM files WHERE folder_id IN ({placeholders})",
            desc_ids,
        )

        prefix = folder_rel + "/" if folder_rel else ""
        for f in files:
            arc_name = f["rel_path"]
            if prefix and arc_name.startswith(prefix):
                arc_name = arc_name[len(prefix) :]
            arc_name = folder_name + "/" + arc_name
            all_files.append((f["full_path"], arc_name, folder_loc_id))

    return all_files
