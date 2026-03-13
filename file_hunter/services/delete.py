"""Delete service — remove files and folders from disk and catalog."""

import os

from file_hunter.services import fs


async def delete_file(db, file_id: int) -> dict:
    """Delete a single file from disk and catalog."""
    row = await db.execute_fetchall(
        """SELECT f.id, f.filename, f.full_path, f.location_id, f.hash_fast, f.hash_strong, l.root_path
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
    hash_fast = rec["hash_fast"]
    hash_strong = rec["hash_strong"]

    # Check if location is online and file exists
    deleted_from_disk = False
    online = await fs.dir_exists(root_path, location_id)
    if online:
        exists = await fs.file_exists(full_path, location_id)
        if exists:
            await fs.file_delete(full_path, location_id)
            deleted_from_disk = True

    await db.execute("DELETE FROM files WHERE id = ?", (file_id,))
    await db.commit()

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    from file_hunter.services.sizes import schedule_size_recalc
    from file_hunter.services.dup_counts import submit_hashes_for_recalc

    schedule_size_recalc(location_id)
    if hash_strong:
        submit_hashes_for_recalc(
            strong_hashes={hash_strong}, source=f"delete {filename}"
        )
    elif hash_fast:
        submit_hashes_for_recalc(fast_hashes={hash_fast}, source=f"delete {filename}")

    return {"filename": filename, "deleted_from_disk": deleted_from_disk}


async def delete_file_and_duplicates(db, file_id: int) -> dict:
    """Delete a file and all its duplicates (same hash_strong) from disk and catalog."""
    # Look up the file's hash
    row = await db.execute_fetchall(
        """SELECT f.id, f.filename, f.hash_strong
           FROM files f WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        return None

    filename = row[0]["filename"]
    hash_strong = row[0]["hash_strong"]

    if not hash_strong:
        # No strong hash — fall back to single-file delete
        return await delete_file(db, file_id)

    # Find all files with the same hash_strong
    all_rows = await db.execute_fetchall(
        """SELECT f.id, f.full_path, f.location_id, l.root_path
           FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.hash_strong = ? AND f.hidden = 0""",
        (hash_strong,),
    )

    deleted_count = 0
    deleted_from_disk_count = 0

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
        deleted_count += 1

    await db.commit()

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    from file_hunter.services.sizes import schedule_size_recalc
    from file_hunter.services.dup_counts import submit_hashes_for_recalc

    affected_loc_ids = {rec["location_id"] for rec in all_rows}
    schedule_size_recalc(*affected_loc_ids)
    submit_hashes_for_recalc(
        strong_hashes={hash_strong}, source=f"delete {filename} + duplicates"
    )

    return {
        "filename": filename,
        "deleted_count": deleted_count,
        "deleted_from_disk_count": deleted_from_disk_count,
    }


async def delete_folder(db, folder_id: int) -> dict:
    """Delete a folder, its contents from disk, and all catalog records."""
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

    # Collect hashes before deletion for dup_count recalc
    hash_rows = await db.execute_fetchall(
        """WITH RECURSIVE descendants(id) AS (
               SELECT ? UNION ALL
               SELECT f.id FROM folders f JOIN descendants d ON f.parent_id = d.id
           )
           SELECT DISTINCT hash_strong, hash_fast FROM files
           WHERE folder_id IN (SELECT id FROM descendants)
           AND (hash_strong IS NOT NULL OR hash_fast IS NOT NULL)""",
        (folder_id,),
    )
    affected_strong = {r["hash_strong"] for r in hash_rows if r["hash_strong"]}
    affected_fast = {
        r["hash_fast"] for r in hash_rows if not r["hash_strong"] and r["hash_fast"]
    }

    # Check if location is online and folder exists
    deleted_from_disk = False
    online = await fs.dir_exists(root_path, location_id)
    if online:
        exists = await fs.dir_exists(abs_path, location_id)
        if exists:
            await fs.dir_delete(abs_path, location_id)
            deleted_from_disk = True

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

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    from file_hunter.services.sizes import schedule_size_recalc
    from file_hunter.services.dup_counts import submit_hashes_for_recalc

    schedule_size_recalc(location_id)
    submit_hashes_for_recalc(
        strong_hashes=affected_strong,
        fast_hashes=affected_fast,
        source=f"delete folder {name}",
    )

    return {
        "name": name,
        "file_count": file_count,
        "deleted_from_disk": deleted_from_disk,
    }
