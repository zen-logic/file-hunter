"""Delete service — remove files and folders from disk and catalog."""

import os

from file_hunter.hashes_db import (
    get_file_hashes,
    open_hashes_connection,
    read_hashes,
    remove_file_hashes,
)
from file_hunter.helpers import get_effective_hash, post_op_stats
from file_hunter.services import fs
from file_hunter.services.deferred_ops import queue_deferred_op
from file_hunter.stats_db import remove_folder_stats, update_stats_for_files


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

    Side effects:
        Disk I/O — deletes the file via fs.file_delete() if location is online.
        DB write + commit — DELETE FROM files.
        Removes hashes from hashes.db via remove_file_hashes().
        Updates stats_db folder/location counters via update_stats_for_files().
        Broadcasts updated stats and dup counts via post_op_stats().
        May queue a deferred_op if location is offline.

    Called by:
        Route handler file_delete (DELETE /api/files/{id}).
        delete_file_and_duplicates() as fallback when no hash exists.
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
    exists = await fs.file_exists(full_path, location_id)
    if exists:
        await fs.file_delete(full_path, location_id)
        deleted_from_disk = True

    await db.execute("DELETE FROM files WHERE id = ?", (file_id,))
    await db.commit()

    await remove_file_hashes([file_id])

    await update_stats_for_files(
        location_id,
        removed=[
            (
                rec["folder_id"],
                rec["file_size"] or 0,
                rec["file_type_high"],
                rec["hidden"],
            )
        ],
    )

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

    Side effects:
        Disk I/O — deletes each file via fs.file_delete() if location is online.
        DB write + commit — DELETE FROM files for all online duplicates.
        Removes hashes from hashes.db via remove_file_hashes().
        Updates stats_db per affected location via update_stats_for_files().
        Broadcasts updated stats and dup counts via post_op_stats().
        May queue deferred_ops for files on offline locations.

    Called by:
        Route handler (DELETE /api/files/{id} with all_duplicates=true).
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
    removed_by_loc: dict[int, list[tuple]] = {}

    for rec in all_rows:
        fid = rec["id"]
        full_path = rec["full_path"]
        root_path = rec["root_path"]
        loc_id = rec["location_id"]

        online = await fs.dir_exists(root_path, loc_id)
        if online:
            exists = await fs.file_exists(full_path, loc_id)
            if exists:
                await fs.file_delete(full_path, loc_id)
                deleted_from_disk_count += 1
            await db.execute("DELETE FROM files WHERE id = ?", (fid,))
            deleted_ids.append(fid)
            deleted_count += 1
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
            deferred_count += 1

    await db.commit()

    if deleted_ids:
        await remove_file_hashes(deleted_ids)

    # Update stats per affected location
    if removed_by_loc:
        for loc_id, removed_files in removed_by_loc.items():
            await update_stats_for_files(loc_id, removed=removed_files)

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

    Side effects:
        Disk I/O — deletes the directory tree via fs.dir_delete() if online.
        DB write + commit — DELETE FROM files, DELETE FROM folders (CASCADE).
        Removes hashes from hashes.db via remove_file_hashes() (batched by 500).
        Updates stats_db via update_stats_for_files() and remove_folder_stats().
        Broadcasts updated stats and dup counts via post_op_stats().

    Called by:
        Route handler folder_delete (DELETE /api/folders/{id}).
        batch_delete() in batch.py (per-folder).
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
        """WITH RECURSIVE descendants(id) AS (
               SELECT ? UNION ALL
               SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
           )
           SELECT count(*) as cnt FROM files
           WHERE folder_id IN (SELECT id FROM descendants)""",
        (folder_id,),
    )
    file_count = count_row[0]["cnt"] if count_row else 0

    # Collect file IDs from catalog, then read hashes from hashes.db
    file_id_rows = await db.execute_fetchall(
        """WITH RECURSIVE descendants(id) AS (
               SELECT ? UNION ALL
               SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
           )
           SELECT id FROM files WHERE folder_id IN (SELECT id FROM descendants)""",
        (folder_id,),
    )
    affected_strong: set[str] = set()
    affected_fast: set[str] = set()
    if file_id_rows:
        hconn = await open_hashes_connection()
        try:
            fids = [r["id"] for r in file_id_rows]
            for i in range(0, len(fids), 500):
                batch = fids[i : i + 500]
                ph = ",".join("?" for _ in batch)
                hash_rows = await hconn.execute_fetchall(
                    f"SELECT hash_strong, hash_fast FROM file_hashes "
                    f"WHERE file_id IN ({ph})",
                    batch,
                )
                for r in hash_rows:
                    if r["hash_strong"]:
                        affected_strong.add(r["hash_strong"])
                    elif r["hash_fast"]:
                        affected_fast.add(r["hash_fast"])
        finally:
            await hconn.close()

    # Check if location is online and folder exists
    deleted_from_disk = False
    online = await fs.dir_exists(root_path, location_id)
    if online:
        exists = await fs.dir_exists(abs_path, location_id)
        if exists:
            await fs.dir_delete(abs_path, location_id)
            deleted_from_disk = True

    # Collect file info for hashes + stats cleanup before deleting
    file_info_rows = await db.execute_fetchall(
        """SELECT id, folder_id, file_size, file_type_high, hidden
           FROM files WHERE folder_id IN (
               WITH RECURSIVE descendants(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
               )
               SELECT id FROM descendants
           )""",
        (folder_id,),
    )
    deleted_file_ids = [r["id"] for r in file_info_rows]
    removed_deltas = [
        (r["folder_id"], r["file_size"] or 0, r["file_type_high"], r["hidden"])
        for r in file_info_rows
    ]

    # Collect descendant folder IDs for stats cleanup
    desc_folder_rows = await db.execute_fetchall(
        """WITH RECURSIVE descendants(id) AS (
               SELECT ? UNION ALL
               SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
           )
           SELECT id FROM descendants""",
        (folder_id,),
    )
    deleted_folder_ids = [r["id"] for r in desc_folder_rows]

    # Delete files first (folder FK is ON DELETE SET NULL, not CASCADE)
    await db.execute(
        """DELETE FROM files WHERE folder_id IN (
               WITH RECURSIVE descendants(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
               )
               SELECT id FROM descendants
           )""",
        (folder_id,),
    )

    # Delete folder — CASCADE handles child folders
    await db.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
    await db.commit()

    if deleted_file_ids:
        await remove_file_hashes(deleted_file_ids)

    # Update stats: remove file deltas from ancestor folders, remove folder_stats entries
    if removed_deltas:
        await update_stats_for_files(location_id, removed=removed_deltas)
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
