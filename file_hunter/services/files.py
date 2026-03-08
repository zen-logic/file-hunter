"""File listing, detail, and update operations."""

import asyncio
import os

from file_hunter.core import classify_file

PAGE_SIZE = 120


SORT_COLUMNS = {
    "name": "f.filename",
    "type": "f.file_type_low",
    "size": "f.file_size",
    "date": "f.modified_date",
    "dups": "f.dup_count",
}


async def list_files(
    db,
    folder_id: str,
    *,
    page=0,
    sort="name",
    sort_dir="asc",
    filter_text=None,
    focus_file_id=None,
):
    """Return paged files and all subfolders for a given folder/location root."""
    from file_hunter.services.settings import get_setting

    col = SORT_COLUMNS.get(sort, "f.filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"
    offset = page * PAGE_SIZE

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    folder_hidden_filter = "" if show_hidden else " AND fld.hidden = 0"

    # Build WHERE clause for files
    location_id = None
    if folder_id.startswith("loc-"):
        loc_id = int(folder_id[4:])
        location_id = loc_id
        folder_where = "f.location_id = ? AND f.folder_id IS NULL"
        folder_params = [loc_id]
        # Subfolders at root level (include rel_path + root_path for missing check)
        folders = await db.execute_fetchall(
            f"""SELECT fld.id, fld.name, fld.rel_path, fld.hidden, l.root_path
               FROM folders fld JOIN locations l ON l.id = fld.location_id
               WHERE fld.location_id = ? AND fld.parent_id IS NULL{folder_hidden_filter} ORDER BY fld.name""",
            (loc_id,),
        )
    elif folder_id.startswith("fld-"):
        fld_id = int(folder_id[4:])
        folder_where = "f.folder_id = ?"
        folder_params = [fld_id]
        # Subfolders (include rel_path + root_path for missing check)
        folders = await db.execute_fetchall(
            f"""SELECT fld.id, fld.name, fld.rel_path, fld.hidden, l.root_path, fld.location_id
               FROM folders fld JOIN locations l ON l.id = fld.location_id
               WHERE fld.parent_id = ?{folder_hidden_filter} ORDER BY fld.name""",
            (fld_id,),
        )
        if folders:
            location_id = folders[0]["location_id"]
    else:
        return {
            "items": [],
            "folders": [],
            "total": 0,
            "page": 0,
            "pageSize": PAGE_SIZE,
        }

    # Optional filename filter
    filter_clause = ""
    filter_params = []
    if filter_text:
        escaped = (
            filter_text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        filter_clause = " AND f.filename LIKE ? ESCAPE '\\'"
        filter_params = [f"%{escaped}%"]

    where = folder_where + filter_clause + hidden_filter
    params = folder_params + filter_params

    # If focusing a specific file, compute which page it's on
    focus_found = False
    if focus_file_id:
        focus_row = await db.execute_fetchall(
            f"SELECT {col} as sort_val FROM files f WHERE f.id = ? AND {where}",
            [focus_file_id] + params,
        )
        if focus_row:
            focus_found = True
            sort_val = focus_row[0]["sort_val"]
            if direction == "ASC":
                op, id_op = "<", "<"
            else:
                op, id_op = ">", ">"
            pos_row = await db.execute_fetchall(
                f"""SELECT COUNT(*) as pos FROM files f
                    WHERE {where}
                    AND ({col} {op} ? OR ({col} = ? AND f.id {id_op} ?))""",
                params + [sort_val, sort_val, focus_file_id],
            )
            position = pos_row[0]["pos"] if pos_row else 0
            page = position // PAGE_SIZE
            offset = page * PAGE_SIZE

    # Count total files matching
    count_row = await db.execute_fetchall(
        f"SELECT COUNT(*) as cnt FROM files f WHERE {where}",
        params,
    )
    total = count_row[0]["cnt"] if count_row else 0

    # Fetch paged files (include full_path + root_path for missing check)
    files = await db.execute_fetchall(
        f"""SELECT f.id, f.filename, f.file_type_high, f.file_type_low,
                   f.file_size, f.modified_date, f.full_path,
                   f.hash_strong, f.stale, f.dup_count, f.hidden, l.root_path
            FROM files f
            JOIN locations l ON l.id = f.location_id
            WHERE {where}
            ORDER BY {col} {direction}
            LIMIT ? OFFSET ?""",
        params + [PAGE_SIZE, offset],
    )

    # Determine if location is online and batch-check existence
    missing_file_ids = set()
    missing_folder_ids = set()
    loc_online = False
    # All items share the same location — get root_path from either source
    root_path = None
    if files:
        root_path = files[0]["root_path"]
    elif folders:
        root_path = folders[0]["root_path"]
    if root_path and location_id:
        loc_online = False  # online status handled by agent check
    unstale_set = set()
    if loc_online:
        all_paths = [f["full_path"] for f in files] + [
            os.path.join(fld["root_path"], fld["rel_path"]) for fld in folders
        ]
        if all_paths:
            exists = await asyncio.to_thread(
                lambda ps: [os.path.exists(p) for p in ps], all_paths
            )
            n_files = len(files)
            for i in range(n_files):
                if files[i]["stale"] and exists[i]:
                    unstale_set.add(files[i]["id"])
                elif not files[i]["stale"] and not exists[i]:
                    missing_file_ids.add(files[i]["id"])
            missing_folder_ids = {
                folders[i - n_files]["id"]
                for i in range(n_files, len(all_paths))
                if not exists[i]
            }
        # Batch-clear stale flag for files found on disk
        if unstale_set:
            from file_hunter.db import execute_write

            async def _batch_clear_stale(conn, ids):
                placeholders = ",".join("?" * len(ids))
                await conn.execute(
                    f"UPDATE files SET stale = 0 WHERE id IN ({placeholders})",
                    list(ids),
                )
                await conn.commit()

            await execute_write(_batch_clear_stale, unstale_set)

    folder_items = [
        {
            "id": f"fld-{fld['id']}",
            "name": fld["name"],
            "type": "folder",
            "size": None,
            "date": None,
            "missing": fld["id"] in missing_folder_ids,
            "hidden": bool(fld["hidden"]),
        }
        for fld in folders
    ]

    from file_hunter.services.dup_counts import batch_dup_counts

    live_dups = await batch_dup_counts(db, [f["hash_strong"] for f in files])

    file_items = [
        {
            "id": f["id"],
            "name": f["filename"],
            "typeHigh": f["file_type_high"],
            "typeLow": f["file_type_low"],
            "size": f["file_size"],
            "date": f["modified_date"],
            "dups": live_dups.get(f["hash_strong"], 0),
            "hashStrong": f["hash_strong"],
            "stale": bool(f["stale"]) and f["id"] not in unstale_set,
            "missing": False
            if (f["stale"] and f["id"] not in unstale_set)
            else f["id"] in missing_file_ids,
            "hidden": bool(f["hidden"]),
        }
        for f in files
    ]

    # Build breadcrumb for folder navigation
    breadcrumb = []
    if folder_id.startswith("loc-"):
        loc_name_row = await db.execute_fetchall(
            "SELECT name FROM locations WHERE id = ?", (loc_id,)
        )
        if loc_name_row:
            breadcrumb = [{"nodeId": folder_id, "name": loc_name_row[0]["name"]}]
    elif folder_id.startswith("fld-"):
        fld_id = int(folder_id[4:])
        # Walk ancestor chain
        chain_rows = await db.execute_fetchall(
            """WITH RECURSIVE chain(id, name, parent_id, depth) AS (
                       SELECT id, name, parent_id, 0 FROM folders WHERE id = ?
                       UNION ALL
                       SELECT f.id, f.name, f.parent_id, c.depth + 1
                       FROM folders f JOIN chain c ON f.id = c.parent_id
                   )
                   SELECT id, name FROM chain ORDER BY depth DESC""",
            (fld_id,),
        )
        # Prepend location
        loc_row = await db.execute_fetchall(
            "SELECT l.id, l.name FROM folders fld JOIN locations l ON l.id = fld.location_id WHERE fld.id = ?",
            (fld_id,),
        )
        if loc_row:
            breadcrumb.append(
                {"nodeId": f"loc-{loc_row[0]['id']}", "name": loc_row[0]["name"]}
            )
        for r in chain_rows:
            breadcrumb.append({"nodeId": f"fld-{r['id']}", "name": r["name"]})

    result = {
        "items": file_items,
        "folders": folder_items,
        "total": total,
        "page": page,
        "pageSize": PAGE_SIZE,
        "breadcrumb": breadcrumb,
    }
    if focus_found:
        result["focusFileId"] = focus_file_id
    return result


async def get_file_detail(db, file_id: int):
    """Return full file detail with duplicates."""
    row = await db.execute_fetchall(
        """SELECT f.*, l.name as location_name, l.root_path as location_root_path
           FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        return None
    f = dict(row[0])

    # Find duplicates — other files with same hash_strong (capped at 100)
    dups = []
    dup_total = 0
    if f["hash_strong"]:
        count_row = await db.execute_fetchall(
            """SELECT COUNT(*) as cnt FROM files
               WHERE hash_strong = ? AND id != ? AND stale = 0 AND hidden = 0 AND dup_exclude = 0""",
            (f["hash_strong"], file_id),
        )
        dup_total = count_row[0]["cnt"] if count_row else 0

        dup_rows = await db.execute_fetchall(
            """SELECT f2.id, f2.filename, f2.rel_path, l.name as location_name
               FROM files f2
               JOIN locations l ON l.id = f2.location_id
               WHERE f2.hash_strong = ? AND f2.id != ? AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
               LIMIT 10""",
            (f["hash_strong"], file_id),
        )
        for d in dup_rows:
            dups.append(
                {
                    "fileId": d["id"],
                    "location": d["location_name"],
                    "path": f"/{d['rel_path']}",
                }
            )

    tags = [t.strip() for t in f["tags"].split(",") if t.strip()] if f["tags"] else []

    from file_hunter.services.locations import check_location_online

    location_online = await asyncio.to_thread(
        check_location_online, f["location_id"], f["location_root_path"]
    )

    # If stale but location is online, check if file actually exists
    stale = bool(f["stale"])
    if stale and location_online:
        from file_hunter.services import fs

        exists = await fs.file_exists(f["full_path"], f["location_id"])
        if exists:
            from file_hunter.db import execute_write

            async def _clear_stale(conn, fid):
                await conn.execute("UPDATE files SET stale = 0 WHERE id = ?", (fid,))
                await conn.commit()

            await execute_write(_clear_stale, file_id)
            stale = False

    # Tree node ID of containing folder (or location root)
    folder_id = f"fld-{f['folder_id']}" if f["folder_id"] else f"loc-{f['location_id']}"

    # Build breadcrumb: walk parent_id chain, prepend location
    breadcrumb = [{"nodeId": f"loc-{f['location_id']}", "name": f["location_name"]}]
    cur = f["folder_id"]
    chain = []
    while cur:
        row2 = await db.execute_fetchall(
            "SELECT id, name, parent_id FROM folders WHERE id = ?", (cur,)
        )
        if not row2:
            break
        chain.append({"nodeId": f"fld-{row2[0]['id']}", "name": row2[0]["name"]})
        cur = row2[0]["parent_id"]
    breadcrumb.extend(reversed(chain))

    return {
        "id": f["id"],
        "name": f["filename"],
        "folderId": folder_id,
        "locationId": f"loc-{f['location_id']}",
        "locationOnline": location_online,
        "locationName": f["location_name"],
        "path": f"/{f['location_name']}/{f['rel_path']}",
        "online": not stale and location_online,
        "typeHigh": f["file_type_high"],
        "typeLow": f["file_type_low"],
        "size": f["file_size"],
        "date": f["modified_date"],
        "created": f["created_date"],
        "cataloged": f["date_cataloged"],
        "lastSeen": f["date_last_seen"],
        "hashPartial": f["hash_partial"],
        "hashFast": f["hash_fast"],
        "hashStrong": f["hash_strong"],
        "stale": stale,
        "description": f["description"] or "",
        "tags": tags,
        "duplicates": dups,
        "dupTotal": dup_total,
        "breadcrumb": breadcrumb,
    }


async def update_file(db, file_id: int, description: str = None, tags: list = None):
    """Update file description and/or tags."""
    parts = []
    params = []
    if description is not None:
        parts.append("description = ?")
        params.append(description)
    if tags is not None:
        parts.append("tags = ?")
        params.append(",".join(tags))
    if not parts:
        return
    params.append(file_id)
    await db.execute(
        f"UPDATE files SET {', '.join(parts)} WHERE id = ?",
        params,
    )
    await db.commit()


async def move_file(
    db, file_id: int, *, new_name: str = None, destination_folder_id: str = None
):
    """Move and/or rename a file on disk and in the catalog."""
    from file_hunter.services import fs

    row = await db.execute_fetchall(
        """SELECT f.*, l.root_path AS location_root_path, l.name AS location_name
           FROM files f JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        raise ValueError("File not found.")
    f = dict(row[0])

    src_root = f["location_root_path"]
    src_loc_id = f["location_id"]

    if not await fs.dir_exists(src_root, src_loc_id):
        raise ValueError("Location is offline.")
    if not await fs.file_exists(f["full_path"], src_loc_id):
        raise ValueError("File not found on disk.")

    old_name = f["filename"]
    final_name = new_name if new_name else old_name
    final_location_id = src_loc_id
    final_folder_id = f["folder_id"]
    final_root = src_root
    dest_dir = os.path.dirname(f["full_path"])

    renamed = new_name is not None and new_name != old_name
    moved = False

    if destination_folder_id:
        moved = True
        # Resolve destination
        if destination_folder_id.startswith("loc-"):
            dest_loc_id = int(destination_folder_id[4:])
            dest_row = await db.execute_fetchall(
                "SELECT id, root_path FROM locations WHERE id = ?", (dest_loc_id,)
            )
            if not dest_row:
                raise ValueError("Destination location not found.")
            final_root = dest_row[0]["root_path"]
            final_location_id = dest_loc_id
            final_folder_id = None
            dest_dir = final_root
        elif destination_folder_id.startswith("fld-"):
            dest_fld_id = int(destination_folder_id[4:])
            dest_row = await db.execute_fetchall(
                """SELECT f.id, f.location_id, f.rel_path, l.root_path
                   FROM folders f JOIN locations l ON l.id = f.location_id
                   WHERE f.id = ?""",
                (dest_fld_id,),
            )
            if not dest_row:
                raise ValueError("Destination folder not found.")
            final_root = dest_row[0]["root_path"]
            final_location_id = dest_row[0]["location_id"]
            final_folder_id = dest_fld_id
            dest_dir = os.path.join(final_root, dest_row[0]["rel_path"])
        else:
            raise ValueError("Invalid destination_folder_id.")

        # Cross-location: check destination is online
        if final_root != src_root or final_location_id != src_loc_id:
            if not await fs.dir_exists(final_root, final_location_id):
                raise ValueError("Destination location is offline.")

        if not await fs.dir_exists(dest_dir, final_location_id):
            raise ValueError("Destination directory does not exist on disk.")

    new_full_path = os.path.join(dest_dir, final_name)
    new_rel_path = os.path.relpath(new_full_path, final_root)

    # Check collision on disk (unless it's the same file — rename case)
    if new_full_path != f["full_path"]:
        if await fs.path_exists(new_full_path, final_location_id):
            raise ValueError("A file with that name already exists at the destination.")

    # Check collision in DB
    if new_rel_path != f["rel_path"] or final_location_id != f["location_id"]:
        existing = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id = ? AND rel_path = ? AND id != ?",
            (final_location_id, new_rel_path, file_id),
        )
        if existing:
            raise ValueError("A file with that path already exists in the catalog.")

    # Move/rename on disk
    if new_full_path != f["full_path"]:
        cross_location = final_location_id != src_loc_id
        if cross_location:
            # Cross-location (possibly agent↔local): copy + delete
            await fs.copy_file(
                f["full_path"], src_loc_id, new_full_path, final_location_id
            )
            await fs.file_delete(f["full_path"], src_loc_id)
        else:
            await fs.file_move(f["full_path"], new_full_path, final_location_id)

    # Reclassify if extension changed
    new_type_high, new_type_low = classify_file(final_name)

    await db.execute(
        """UPDATE files SET filename = ?, full_path = ?, rel_path = ?,
              location_id = ?, folder_id = ?,
              file_type_high = ?, file_type_low = ?
           WHERE id = ?""",
        (
            final_name,
            new_full_path,
            new_rel_path,
            final_location_id,
            final_folder_id,
            new_type_high,
            new_type_low,
            file_id,
        ),
    )
    await db.commit()

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    from file_hunter.services.sizes import schedule_size_recalc

    if final_location_id != src_loc_id:
        schedule_size_recalc(src_loc_id, final_location_id)
    else:
        schedule_size_recalc(src_loc_id)

    return {
        "id": file_id,
        "old_name": old_name,
        "new_name": final_name,
        "renamed": renamed,
        "moved": moved,
    }
