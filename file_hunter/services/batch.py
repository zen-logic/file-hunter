"""Batch operations — delete, move, tag, and download multiple items."""

import os
import tempfile
import zipfile

from starlette.responses import StreamingResponse

from file_hunter.hashes_db import get_file_hashes, read_hashes, remove_file_hashes
from file_hunter.helpers import parse_prefixed_id, post_op_stats
from file_hunter.services import fs
from file_hunter.services.activity import register, unregister, update
from file_hunter.services.content_proxy import stream_agent_file
from file_hunter.services.deferred_ops import queue_deferred_op
from file_hunter.services.delete import delete_folder
from file_hunter.services.files import move_file, update_file
from file_hunter.services.locations import move_folder
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.scan import broadcast


async def batch_delete(
    db, file_ids: list[int], folder_ids: list[int], all_duplicates: bool = False
) -> dict:
    """Delete multiple files and folders from disk and catalog in a single operation.

    Folders are deleted first (they may contain listed files). Files on offline
    locations are queued as deferred ops. When all_duplicates is True, the file
    list is expanded to include all files sharing the same hash (strong or fast)
    before deletion.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_ids: List of numeric file IDs to delete.
        folder_ids: List of numeric folder IDs to delete.
        all_duplicates: If True, expand file_ids to include all duplicate files
            (by hash) from hashes.db before deleting.

    Returns:
        dict with keys: deleted_files (int), deleted_folders (int),
        deleted_from_disk (int).

    Side effects:
        Disk I/O — deletes files/folders via fs service for online locations.
        DB write + commit — bulk DELETE FROM files (batched by 500), folder
        deletes via delete_folder().
        Removes hashes from hashes.db via remove_file_hashes().
        Updates stats_db per affected location via update_stats_for_files().
        Registers/unregisters an activity entry for status bar progress.
        Broadcasts batch_delete_progress and status_bar_idle via WebSocket.
        Broadcasts updated stats and dup counts via post_op_stats().
        May queue deferred_ops for files on offline locations.

    Called by:
        Route handler batch_delete_route (POST /api/batch/delete) via execute_write.
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
            result = await delete_folder(db, fid)
            if result:
                deleted_folders += 1
                if result.get("deleted_from_disk"):
                    deleted_from_disk += 1
            done += 1
            update(act_name, progress=f"{done}/{total}")

        if not file_ids:
            return {
                "deleted_files": 0,
                "deleted_folders": deleted_folders,
                "deleted_from_disk": deleted_from_disk,
            }

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
            return {
                "deleted_files": 0,
                "deleted_folders": deleted_folders,
                "deleted_from_disk": deleted_from_disk,
            }

        # Get hashes for dup recalc
        fids = [r["id"] for r in all_rows]
        h_map = await get_file_hashes(fids)

        # Check online once per location
        online_cache: dict[int, bool] = {}
        deleted_ids: list[int] = []
        removed_by_loc: dict[int, list[tuple]] = {}
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
                    exists = await fs.file_exists(rec["full_path"], loc_id)
                    if exists:
                        await fs.file_delete(rec["full_path"], loc_id)
                        deleted_from_disk += 1
                except Exception:
                    pass  # file delete failed, still remove from catalog
                deleted_ids.append(fid)
                deleted_files += 1
                if loc_id not in removed_by_loc:
                    removed_by_loc[loc_id] = []
                removed_by_loc[loc_id].append(
                    (
                        rec["folder_id"],
                        rec["file_size"] or 0,
                        rec["file_type_high"],
                        rec["hidden"],
                    )
                )
            else:
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
            for i in range(0, len(deleted_ids), 500):
                batch = deleted_ids[i : i + 500]
                bph = ",".join("?" for _ in batch)
                await db.execute(f"DELETE FROM files WHERE id IN ({bph})", batch)
            await db.commit()

            await remove_file_hashes(deleted_ids)

        # Update stats once per location
        if removed_by_loc:
            for loc_id, removed in removed_by_loc.items():
                await update_stats_for_files(loc_id, removed=removed)

        await post_op_stats(
            strong_hashes=affected_strong or None,
            fast_hashes=affected_fast or None,
            source=f"batch delete ({len(file_ids)} files)",
        )

        return {
            "deleted_files": deleted_files,
            "deleted_folders": deleted_folders,
            "deleted_from_disk": deleted_from_disk,
        }
    finally:
        unregister(act_name)
        await broadcast({"type": "status_bar_idle"})


async def batch_move(
    db, file_ids: list[int], folder_ids: list[int], destination_folder_id: str
) -> dict:
    """Move multiple files and folders to a single destination folder or location root.

    Folders are moved first, then files. Each move is individual (not transactional)
    so partial success is possible — errors are collected and returned. Per-file
    post_op_stats is skipped; a single post_op_stats runs at the end covering all
    affected locations.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_ids: List of numeric file IDs to move.
        folder_ids: List of numeric folder IDs to move.
        destination_folder_id: Prefixed target ID ("loc-{id}" or "fld-{id}").

    Returns:
        dict with keys: moved_files (int), moved_folders (int),
        errors (list[str] — per-item error messages for failed moves).

    Side effects:
        Disk I/O — moves files/folders via fs service and move_file()/move_folder().
        DB write + commit — per-item catalog updates.
        Registers/unregisters an activity entry for status bar progress.
        Broadcasts batch_move_progress via WebSocket per item.
        Broadcasts updated stats via post_op_stats() once at the end.

    Called by:
        Route handler batch_move_route (POST /api/batch/move) via execute_write.
    """
    total = len(file_ids) + len(folder_ids)
    act_name = f"batch-move-{id(file_ids)}"
    register(act_name, "Moving files", f"0/{total}")

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
                await move_folder(db, fid, destination_folder_id)
                moved_folders += 1
            except ValueError as e:
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
                )
                moved_files += 1
            except ValueError as e:
                errors.append(f"File {fid}: {e}")
            done = moved_folders + moved_files
            update(act_name, progress=f"{done}/{total}")
    finally:
        unregister(act_name)

    # Post-processing once — recalc all affected locations
    kind, num_id = parse_prefixed_id(destination_folder_id)
    if kind == "loc":
        dest_loc_id = num_id
    else:
        row = await db.execute_fetchall(
            "SELECT location_id FROM folders WHERE id = ?", (num_id,)
        )
        dest_loc_id = row[0]["location_id"] if row else None

    if dest_loc_id:
        affected_loc_ids.add(dest_loc_id)
    await post_op_stats(
        location_ids=affected_loc_ids or None,
        source=f"batch move ({moved_files} files, {moved_folders} folders)",
    )

    return {
        "moved_files": moved_files,
        "moved_folders": moved_folders,
        "errors": errors,
    }


async def batch_tag(
    db, file_ids: list[int], add_tags: list[str], remove_tags: list[str]
) -> dict:
    """Add and/or remove tags on multiple files.

    Iterates each file, reads its current tags, applies additions (deduped)
    and removals, then writes back via update_file().

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_ids: List of numeric file IDs to update.
        add_tags: List of tag strings to add (skipped if already present).
        remove_tags: List of tag strings to remove.

    Returns:
        dict with key: updated (int — number of files successfully updated).

    Side effects:
        DB write + commit — per-file UPDATE via update_file(). Files that do
        not exist in the catalog are silently skipped.

    Called by:
        Route handler batch_tag_route (POST /api/batch/tag) via execute_write.
    """
    updated = 0

    for fid in file_ids:
        row = await db.execute_fetchall("SELECT tags FROM files WHERE id = ?", (fid,))
        if not row:
            continue

        current_raw = row[0]["tags"] or ""
        current = [t.strip() for t in current_raw.split(",") if t.strip()]

        # Add new tags (avoid duplicates)
        for tag in add_tags:
            if tag not in current:
                current.append(tag)

        # Remove tags
        for tag in remove_tags:
            current = [t for t in current if t != tag]

        await update_file(db, fid, tags=current)
        updated += 1

    return {"updated": updated}


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

        desc_rows = await db.execute_fetchall(
            """WITH RECURSIVE desc(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
               )
               SELECT id FROM desc""",
            (fid,),
        )
        desc_ids = [r["id"] for r in desc_rows]

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


async def build_streaming_zip(files, zip_name):
    """Build a ZIP from [(full_path, arc_name, location_id)] and return a StreamingResponse.

    Each file is streamed from its agent in chunks and written to the ZIP entry
    incrementally. The ZIP is built in a temp file to avoid accumulating the
    entire archive in memory.
    """
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(tmp_fd)

    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_STORED) as zf:
            for full_path, arc_name, loc_id in files:
                async with stream_agent_file(full_path, loc_id) as chunks:
                    if chunks is None:
                        continue
                    with zf.open(arc_name, "w", force_zip64=True) as entry:
                        async for chunk in chunks:
                            entry.write(chunk)
    except Exception:
        os.unlink(tmp_path)
        raise

    file_size = os.path.getsize(tmp_path)

    async def _stream_and_cleanup():
        try:
            with open(tmp_path, "rb") as f:
                while True:
                    chunk = f.read(1048576)
                    if not chunk:
                        break
                    yield chunk
        finally:
            os.unlink(tmp_path)

    return StreamingResponse(
        _stream_and_cleanup(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{zip_name}"',
            "Content-Length": str(file_size),
        },
    )
