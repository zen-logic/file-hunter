"""Delete service — remove files and folders from disk and catalog."""

import logging
import os
from collections import defaultdict

from file_hunter.hashes_db import (
    get_file_hashes,
    read_hashes,
    remove_file_hashes,
    hashes_of_files,
)
from file_hunter.db import db_writer, read_db, folder_tree_ids, in_folder_tree, id_batches
from file_hunter.helpers import get_effective_hash, post_op_stats
from file_hunter.services import fs
from file_hunter.services.activity import register, unregister, update
from file_hunter.services.deferred_ops import queue_deferred_op
from file_hunter.services.similarity import remove_embeddings
from file_hunter.stats_db import update_dup_counts_for_files, remove_folder_stats, update_stats_for_files
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def delete_file(db, file_id: int) -> dict:
    """Delete a single file from disk and remove it from the catalog.

    If the file's location is offline, the delete is queued as a deferred op
    and the file remains in the catalog with a pending_op indicator.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_id: Numeric file ID.

    Returns:
        dict with keys: filename (str), deleted_from_disk (bool),
        deferred (bool). Returns None if the file does not exist in the catalog.
    """
    row = await db.execute_fetchall(
        """SELECT f.id, f.filename, f.full_path, f.location_id,
                  f.folder_id, f.file_size, f.file_type_high, f.hidden,
                  l.root_path
           FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        return None

    rec = row[0]
    filename = rec["filename"]
    full_path = rec["full_path"]
    root_path = rec["root_path"]
    location_id = rec["location_id"]

    # Get hash values from hashes.db for dup recalc
    h_map = await get_file_hashes([file_id])
    h = h_map.get(file_id, {})
    hash_fast = h.get("hash_fast")
    hash_strong = h.get("hash_strong")

    # Check if location is online
    online = await fs.dir_exists(root_path, location_id)

    if not online:
        # Defer — keep file in catalog with pending_op indicator
        await queue_deferred_op(db, file_id, location_id, "delete")
        await db.commit()

        await post_op_stats()
        return {"filename": filename, "deleted_from_disk": False, "deferred": True}

    # Online — delete from disk and catalog immediately
    deleted_from_disk = False
    try:
        await fs.file_delete(full_path, location_id)
        deleted_from_disk = True
    except FileNotFoundError:
        pass  # already gone from disk

    await db.execute("DELETE FROM files WHERE id = ?", (file_id,))
    await db.commit()
    await settle_deleted_files([rec])

    await post_op_stats(
        strong_hashes={hash_strong} if hash_strong else None,
        fast_hashes={hash_fast} if hash_fast else None,
        source=f"delete {filename}",
    )

    return {
        "filename": filename,
        "deleted_from_disk": deleted_from_disk,
        "deferred": False,
    }


async def settle_deleted_files(rows):
    """After files are deleted from the catalog: drop their hashes and
    embeddings, take them out of their locations' stats, and take the ones
    that were duplicates out of their folders' duplicate counts.
    rows: id, location_id, folder_id, file_size, file_type_high, hidden."""
    ids = [r["id"] for r in rows]
    dup_ids = (await hashes_of_files(ids))[2]
    await remove_file_hashes(ids)
    await remove_embeddings(ids)
    removed_by_loc = defaultdict(list)
    dup_deltas_by_loc = defaultdict(list)
    for r in rows:
        removed_by_loc[r["location_id"]].append(
            (r["folder_id"], r["file_size"] or 0, r["file_type_high"], r["hidden"])
        )
        if r["id"] in dup_ids:
            dup_deltas_by_loc[r["location_id"]].append((r["folder_id"], -1))
    for loc_id, removed in removed_by_loc.items():
        await update_stats_for_files(loc_id, removed=removed)
    for loc_id, deltas in dup_deltas_by_loc.items():
        await update_dup_counts_for_files(loc_id, deltas)


async def delete_file_and_duplicates(db, file_id: int) -> dict:
    """Delete a file and all its duplicates (by effective hash) from disk and catalog.

    Uses hash_strong if available, otherwise hash_fast, to find all files sharing
    the same hash across all locations. Each duplicate is deleted from disk if its
    location is online; otherwise a deferred op is queued.

    Falls back to single-file delete_file() if no hash exists for the file.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_id: Numeric file ID of the primary file.

    Returns:
        dict with keys: filename (str), deleted_count (int),
        deleted_from_disk_count (int), deferred_count (int).
        Returns None if the primary file does not exist in the catalog.
    """
    # Look up filename from catalog, hash from hashes.db
    row = await db.execute_fetchall(
        "SELECT id, filename FROM files WHERE id = ?",
        (file_id,),
    )
    if not row:
        return None

    filename = row[0]["filename"]

    effective_hash, hash_col = await get_effective_hash(file_id)

    if not effective_hash:
        # No hash at all — fall back to single-file delete
        return await delete_file(db, file_id)

    # Find all files with the same effective hash from hashes.db
    async with read_hashes() as hdb:
        dup_rows = await hdb.execute_fetchall(
            f"SELECT file_id FROM active_hashes WHERE {hash_col} = ?",
            (effective_hash,),
        )
    dup_file_ids = [r["file_id"] for r in dup_rows]

    if not dup_file_ids:
        return await delete_file(db, file_id)

    ph = ",".join("?" for _ in dup_file_ids)
    all_rows = await db.execute_fetchall(
        f"""SELECT f.id, f.full_path, f.location_id, f.folder_id,
                  f.file_size, f.file_type_high, f.hidden, l.root_path
           FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id IN ({ph})""",
        dup_file_ids,
    )

    deleted_count = 0
    deleted_from_disk_count = 0
    deferred_count = 0
    deleted_ids: list[int] = []

    for rec in all_rows:
        fid = rec["id"]
        full_path = rec["full_path"]
        root_path = rec["root_path"]
        loc_id = rec["location_id"]

        online = await fs.dir_exists(root_path, loc_id)
        if online:
            try:
                await fs.file_delete(full_path, loc_id)
                deleted_from_disk_count += 1
            except FileNotFoundError:
                pass  # already gone from disk
            await db.execute("DELETE FROM files WHERE id = ?", (fid,))
            deleted_ids.append(fid)
            deleted_count += 1
        else:
            await queue_deferred_op(db, fid, loc_id, "delete")
            deferred_count += 1

    await db.commit()

    if deleted_ids:
        deleted = set(deleted_ids)
        await settle_deleted_files([r for r in all_rows if r["id"] in deleted])

    affected_loc_ids = {rec["location_id"] for rec in all_rows}
    await post_op_stats(
        location_ids=affected_loc_ids,
        strong_hashes={effective_hash} if hash_col == "hash_strong" else None,
        fast_hashes={effective_hash} if hash_col == "hash_fast" else None,
        source=f"delete {filename} + duplicates",
    )

    return {
        "filename": filename,
        "deleted_count": deleted_count,
        "deleted_from_disk_count": deleted_from_disk_count,
        "deferred_count": deferred_count,
    }


async def delete_folder(db, folder_id: int) -> dict:
    """Delete a folder, all descendant files/subfolders from disk, and all catalog records.

    Recursively collects all descendant folder IDs and their files. If the
    location is online and the folder exists on disk, removes the directory tree.
    Catalog records are deleted regardless of online status (files table first,
    then folders via CASCADE).

    Parameters:
        db: Writable database connection (called inside execute_write).
        folder_id: Numeric folder ID (unprefixed integer).

    Returns:
        dict with keys: name (str), file_count (int), deleted_from_disk (bool).
        Returns None if the folder does not exist in the catalog.
    """
    row = await db.execute_fetchall(
        """SELECT f.id, f.name, f.rel_path, f.location_id, l.root_path
           FROM folders f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (folder_id,),
    )
    if not row:
        return None

    rec = row[0]
    name = rec["name"]
    rel_path = rec["rel_path"]
    root_path = rec["root_path"]
    location_id = rec["location_id"]
    abs_path = os.path.join(root_path, rel_path)

    # Count files for the response
    count_row = await db.execute_fetchall(
        f"SELECT count(*) as cnt FROM files WHERE {in_folder_tree('folder_id')}",
        (folder_id,),
    )
    file_count = count_row[0]["cnt"] if count_row else 0

    # The files under the folder, for hashes and stats
    file_info_rows = await db.execute_fetchall(
        f"""SELECT id, location_id, folder_id, file_size, file_type_high, hidden
           FROM files WHERE {in_folder_tree("folder_id")}""",
        (folder_id,),
    )
    affected_strong, affected_fast = (
        await hashes_of_files([r["id"] for r in file_info_rows])
    )[:2]

    # Check if location is online and folder exists
    deleted_from_disk = False
    online = await fs.dir_exists(root_path, location_id)
    if online:
        exists = await fs.dir_exists(abs_path, location_id)
        if exists:
            await fs.dir_delete(abs_path, location_id)
            deleted_from_disk = True

    # Collect descendant folder IDs for stats cleanup
    deleted_folder_ids = await folder_tree_ids(db, folder_id)

    # Delete files first (folder FK is ON DELETE SET NULL, not CASCADE)
    await db.execute(
        f"DELETE FROM files WHERE {in_folder_tree('folder_id')}",
        (folder_id,),
    )

    # Delete folder — CASCADE handles child folders
    await db.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
    await db.commit()

    if file_info_rows:
        await settle_deleted_files(file_info_rows)
        await remove_folder_stats(deleted_folder_ids)

    await post_op_stats(
        location_ids={location_id},
        strong_hashes=affected_strong or None,
        fast_hashes=affected_fast or None,
        source=f"delete folder {name}",
    )

    return {
        "name": name,
        "file_count": file_count,
        "deleted_from_disk": deleted_from_disk,
    }


async def reset_stale(
    *, folder_id: int = None, location_id: int = None, label: str = ""
):
    """Remove all stale files and folders from the catalog under a subtree.

    Runs as a background task via the queue manager. Manages its own write
    connections per batch so the write lock is not held for the duration.

    Stale entries are files/folders marked stale by a scan that found them
    gone from disk. This purges them from the catalog entirely — no disk I/O
    since the files are already gone.

    Cleanup chain matches delete_folder: hashes.db, stats.db, dup counts,
    file_tags (CASCADE), and post_op_stats broadcast.
    """
    act_name = f"reset-stale-{folder_id or location_id}"
    register(act_name, f"Resetting stale: {label}", "collecting…")

    try:
        # --- Determine scope (read-only) ---
        async with read_db() as db:
            if folder_id is not None:
                loc_row = await db.execute_fetchall(
                    "SELECT location_id FROM folders WHERE id = ?", (folder_id,)
                )
                if not loc_row:
                    return
                loc_id = loc_row[0]["location_id"]

                scope_folder_ids = await folder_tree_ids(db, folder_id)
                ph = ",".join("?" for _ in scope_folder_ids)
                file_where = f"stale = 1 AND folder_id IN ({ph})"
                file_params = scope_folder_ids
                stale_folder_where = f"stale = 1 AND id IN ({ph}) AND id != ?"
                stale_folder_params = scope_folder_ids + [folder_id]
            else:
                loc_id = location_id
                file_where = "stale = 1 AND location_id = ?"
                file_params = [location_id]
                stale_folder_where = "stale = 1 AND location_id = ?"
                stale_folder_params = [location_id]

            stale_files = await db.execute_fetchall(
                f"""SELECT id, location_id, folder_id, file_size, file_type_high,
                           hidden
                    FROM files WHERE {file_where}""",
                file_params,
            )

        stale_file_ids = [r["id"] for r in stale_files]
        total = len(stale_file_ids)

        if not total:
            await broadcast(
                {"type": "stale_reset_complete", "staleFiles": 0, "staleFolders": 0}
            )
            return

        update(act_name, progress=f"0/{total} files")
        await broadcast(
            {"type": "stale_reset_progress", "done": 0, "total": total, "label": label}
        )

        # --- Collect hashes for dup recount ---
        affected_strong, affected_fast = (await hashes_of_files(stale_file_ids))[:2]

        # --- Delete stale files in batches ---
        done = 0
        for batch, bph in id_batches(stale_file_ids):
            async with db_writer() as db:
                await db.execute(f"DELETE FROM files WHERE id IN ({bph})", batch)
            done += len(batch)
            update(act_name, progress=f"{done}/{total} files")
            await broadcast(
                {
                    "type": "stale_reset_progress",
                    "done": done,
                    "total": total,
                    "label": label,
                }
            )

        # --- Delete stale folders that are now empty ---
        stale_folder_ids = []
        while True:
            async with db_writer() as db:
                empty_stale = await db.execute_fetchall(
                    f"""SELECT id FROM folders
                        WHERE {stale_folder_where}
                        AND NOT EXISTS (
                            SELECT 1 FROM files WHERE folder_id = folders.id)
                        AND NOT EXISTS (
                            SELECT 1 FROM folders f2
                            WHERE f2.parent_id = folders.id)""",
                    stale_folder_params,
                )
                if not empty_stale:
                    break
                batch_ids = [r["id"] for r in empty_stale]
                stale_folder_ids.extend(batch_ids)
                bph = ",".join("?" for _ in batch_ids)
                await db.execute(
                    f"DELETE FROM folders WHERE id IN ({bph})", batch_ids
                )

        await settle_deleted_files(stale_files)
        if stale_folder_ids:
            await remove_folder_stats(stale_folder_ids)

        file_count = len(stale_file_ids)
        folder_count = len(stale_folder_ids)
        logger.info(
            "Reset stale: removed %d file(s), %d folder(s) from location #%d",
            file_count, folder_count, loc_id,
        )

        await post_op_stats(
            location_ids={loc_id},
            strong_hashes=affected_strong or None,
            fast_hashes=affected_fast or None,
            source="reset stale",
        )

        await broadcast(
            {
                "type": "stale_reset_complete",
                "staleFiles": file_count,
                "staleFolders": folder_count,
            }
        )

    except Exception as exc:
        logger.exception("Reset stale failed: %s", exc)
        await broadcast({"type": "stale_reset_error", "error": str(exc)})
    finally:
        unregister(act_name)
