"""File listing, detail, and update operations."""

import asyncio
import os
from datetime import datetime, timezone

from file_hunter.core import classify_file
from file_hunter.db import execute_write
from file_hunter.hashes_db import get_file_hashes, hashes_writer, read_hashes
from file_hunter.helpers import (
    parse_folder_id,
    parse_location_id,
    parse_mtime,
    post_op_stats,
    resolve_target,
)
from file_hunter.hashes_db import mark_hashes_stale
from file_hunter.services import fs
from file_hunter.services.agent_ops import dispatch
from file_hunter.ws.scan import broadcast
from file_hunter.services.deferred_ops import queue_deferred_op
from file_hunter.stats_db import update_stats_for_files
from file_hunter.services.dup_counts import batch_dup_counts
from file_hunter.services.locations import check_location_online
from file_hunter.services.settings import get_setting

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
    """Return paged file rows and immediate subfolders for a folder or location root.

    Parameters:
        db: Shared read-only database connection (from read_db()).
        folder_id: Prefixed tree-node ID — "loc-{id}" for a location root or
            "fld-{id}" for a subfolder.
        page: Zero-based page index. Overridden when focus_file_id is set.
        sort: Column key — one of "name", "type", "size", "date", "dups".
        sort_dir: "asc" or "desc".
        filter_text: Optional substring filter applied to filenames and folder names
            (LIKE with SQL-escaped wildcards).
        focus_file_id: If set, the page is recalculated so this file's row is visible.

    Returns:
        dict with keys: items (list[dict]), folders (list[dict]), total (int),
        page (int), pageSize (int), breadcrumb (list[dict]).
        When focus_file_id is found, also includes focusFileId.

    Side effects:
        None — read-only. Hash data is fetched from the separate hashes.db via
        get_file_hashes() and batch_dup_counts().

    Called by:
        Route handler files_list (GET /api/files).
    """
    col = SORT_COLUMNS.get(sort, "f.filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"
    offset = page * PAGE_SIZE

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    folder_hidden_filter = "" if show_hidden else " AND fld.hidden = 0"

    # Build folder name filter (applied to subfolders below)
    folder_name_filter = ""
    folder_name_params = []
    if filter_text:
        escaped_fld = (
            filter_text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        folder_name_filter = " AND fld.name LIKE ? ESCAPE '\\'"
        folder_name_params = [f"%{escaped_fld}%"]

    # Build WHERE clause for files
    if folder_id.startswith("loc-"):
        loc_id = parse_location_id(folder_id)
        folder_where = "f.location_id = ? AND f.folder_id IS NULL"
        folder_params = [loc_id]
        # Subfolders at root level (include rel_path + root_path for missing check)
        folders = await db.execute_fetchall(
            f"""SELECT fld.id, fld.name, fld.rel_path, fld.hidden, fld.stale, fld.is_favourite, l.root_path
               FROM folders fld JOIN locations l ON l.id = fld.location_id
               WHERE fld.location_id = ? AND fld.parent_id IS NULL{folder_hidden_filter}{folder_name_filter} ORDER BY fld.name""",
            [loc_id] + folder_name_params,
        )
    elif folder_id.startswith("fld-"):
        fld_id = parse_folder_id(folder_id)
        folder_where = "f.folder_id = ?"
        folder_params = [fld_id]
        # Subfolders (include rel_path + root_path for missing check)
        folders = await db.execute_fetchall(
            f"""SELECT fld.id, fld.name, fld.rel_path, fld.hidden, fld.stale, fld.is_favourite, l.root_path, fld.location_id
               FROM folders fld JOIN locations l ON l.id = fld.location_id
               WHERE fld.parent_id = ?{folder_hidden_filter}{folder_name_filter} ORDER BY fld.name""",
            [fld_id] + folder_name_params,
        )
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

    # Fetch paged files (structural data only — hashes come from hashes.db)
    files = await db.execute_fetchall(
        f"""SELECT f.id, f.filename, f.file_type_high, f.file_type_low,
                   f.file_size, f.modified_date, f.full_path,
                   f.stale, f.hidden, f.pending_op, l.root_path
            FROM files f
            JOIN locations l ON l.id = f.location_id
            WHERE {where}
            ORDER BY {col} {direction}
            LIMIT ? OFFSET ?""",
        params + [PAGE_SIZE, offset],
    )

    # All locations are agent-backed — no local file existence checks
    missing_file_ids = set()
    missing_folder_ids = set()
    unstale_set = set()

    folder_items = [
        {
            "id": f"fld-{fld['id']}",
            "name": fld["name"],
            "type": "folder",
            "size": None,
            "date": None,
            "missing": fld["id"] in missing_folder_ids,
            "hidden": bool(fld["hidden"]),
            "stale": bool(fld["stale"]),
            "favourite": bool(fld["is_favourite"]),
        }
        for fld in folders
    ]

    # Fetch hash data from hashes.db
    file_ids = [f["id"] for f in files]
    hash_map = await get_file_hashes(file_ids)

    strong_list = [h["hash_strong"] for h in hash_map.values() if h["hash_strong"]]
    fast_list = [
        h["hash_fast"]
        for h in hash_map.values()
        if not h["hash_strong"] and h["hash_fast"]
    ]
    live_dups = await batch_dup_counts(strong_hashes=strong_list, fast_hashes=fast_list)

    file_items = []
    for f in files:
        h = hash_map.get(f["id"], {})
        hs = h.get("hash_strong")
        hf = h.get("hash_fast")
        file_items.append(
            {
                "id": f["id"],
                "name": f["filename"],
                "typeHigh": f["file_type_high"],
                "typeLow": f["file_type_low"],
                "size": f["file_size"],
                "date": f["modified_date"],
                "dups": live_dups.get(hs or hf, 0),
                "hashStrong": hs,
                "hashFast": hf,
                "stale": bool(f["stale"]) and f["id"] not in unstale_set,
                "missing": False
                if (f["stale"] and f["id"] not in unstale_set)
                else f["id"] in missing_file_ids,
                "hidden": bool(f["hidden"]),
                "pendingOp": f["pending_op"],
            }
        )

    # Build breadcrumb for folder navigation
    breadcrumb = []
    if folder_id.startswith("loc-"):
        loc_name_row = await db.execute_fetchall(
            "SELECT name FROM locations WHERE id = ?", (loc_id,)
        )
        if loc_name_row:
            breadcrumb = [{"nodeId": folder_id, "name": loc_name_row[0]["name"]}]
    elif folder_id.startswith("fld-"):
        fld_id = parse_folder_id(folder_id)
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
    """Return full metadata, hash info, duplicate list, and breadcrumb for one file.

    Parameters:
        db: Shared read-only database connection (from read_db()).
        file_id: Numeric file ID (unprefixed integer).

    Returns:
        dict with full file detail (id, name, path, hashes, duplicates, breadcrumb,
        online status, tags, etc.) or None if the file does not exist.

    Side effects:
        DB write — if the file is marked stale but the location is online and the
        file exists on disk, the stale flag is cleared via execute_write().
        Hash and duplicate data are read from hashes.db.

    Called by:
        Route handler file_detail (GET /api/files/{id}).
    """
    row = await db.execute_fetchall(
        """SELECT f.id, f.filename, f.full_path, f.rel_path, f.location_id,
                  f.folder_id, f.file_type_high, f.file_type_low, f.file_size,
                  f.description, f.tags, f.created_date, f.modified_date,
                  f.date_cataloged, f.date_last_seen, f.stale, f.hidden, f.pending_op,
                  l.name as location_name, l.root_path as location_root_path
           FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        return None
    f = dict(row[0])

    # Hash data from hashes.db
    hash_map = await get_file_hashes([file_id])
    h = hash_map.get(file_id, {})
    hash_partial = h.get("hash_partial")
    hash_fast = h.get("hash_fast")
    hash_strong = h.get("hash_strong")

    # Find duplicates from hashes.db — use hash_strong if available, else hash_fast
    dups = []
    dup_total = 0
    effective_hash = hash_strong or hash_fast
    hash_col = "hash_strong" if hash_strong else "hash_fast"
    if effective_hash:
        async with read_hashes() as hdb:
            count_row = await hdb.execute_fetchall(
                f"SELECT COUNT(*) as cnt FROM active_hashes "
                f"WHERE {hash_col} = ? AND file_id != ?",
                (effective_hash, file_id),
            )
            dup_total = count_row[0]["cnt"] if count_row else 0

            dup_file_rows = await hdb.execute_fetchall(
                f"SELECT file_id FROM active_hashes "
                f"WHERE {hash_col} = ? AND file_id != ? LIMIT 10",
                (effective_hash, file_id),
            )

        # Fetch file info from catalog for the dup file IDs
        if dup_file_rows:
            dup_ids = [r["file_id"] for r in dup_file_rows]
            ph = ",".join("?" for _ in dup_ids)
            dup_rows = await db.execute_fetchall(
                f"SELECT f2.id, f2.filename, f2.rel_path, f2.location_id, "
                f"l.name as location_name, a.name as agent_name "
                f"FROM files f2 "
                f"JOIN locations l ON l.id = f2.location_id "
                f"LEFT JOIN agents a ON a.id = l.agent_id "
                f"WHERE f2.id IN ({ph})",
                dup_ids,
            )
            for d in dup_rows:
                dups.append(
                    {
                        "fileId": d["id"],
                        "name": d["filename"],
                        "location": d["location_name"],
                        "agent": d["agent_name"] or "",
                        "locationId": d["location_id"],
                        "path": f"/{d['rel_path']}",
                    }
                )

    tags = [t.strip() for t in f["tags"].split(",") if t.strip()] if f["tags"] else []

    location_online = await asyncio.to_thread(
        check_location_online, f["location_id"], f["location_root_path"]
    )

    # Check file freshness against agent
    stale = bool(f["stale"])
    if location_online:
        stat = await dispatch("file_stat", f["location_id"], path=f["full_path"])
        if stat is None:
            # File gone from disk
            if not stale:
                async def _mark_stale(conn, fid):
                    await conn.execute("UPDATE files SET stale = 1 WHERE id = ?", (fid,))
                    await conn.commit()
                await execute_write(_mark_stale, file_id)
                await mark_hashes_stale([file_id])
                stale = True
                await broadcast({"type": "file_freshness", "fileId": file_id, "stale": True})
        else:
            # File exists — clear stale if it was marked
            if stale:
                async def _clear_stale(conn, fid):
                    await conn.execute("UPDATE files SET stale = 0 WHERE id = ?", (fid,))
                    await conn.commit()
                await execute_write(_clear_stale, file_id)
                stale = False
                await broadcast({"type": "file_freshness", "fileId": file_id, "stale": False})
            # Update size if changed
            if stat["size"] != f["file_size"]:
                async def _update_size(conn, fid, new_size):
                    await conn.execute(
                        "UPDATE files SET file_size = ? WHERE id = ?", (new_size, fid)
                    )
                    await conn.commit()
                await execute_write(_update_size, file_id, stat["size"])
                f["file_size"] = stat["size"]
                await broadcast({"type": "file_freshness", "fileId": file_id, "size": stat["size"]})

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
        "hashPartial": hash_partial,
        "hashFast": hash_fast,
        "hashStrong": hash_strong,
        "verified": bool(hash_strong),
        "stale": stale,
        "pendingOp": f.get("pending_op"),
        "description": f["description"] or "",
        "tags": tags,
        "duplicates": dups,
        "dupTotal": dup_total,
        "breadcrumb": breadcrumb,
    }


async def update_file(db, file_id: int, description: str = None, tags: list = None):
    """Update a file's description and/or tags in the catalog.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_id: Numeric file ID.
        description: New description string, or None to leave unchanged.
        tags: New tag list (joined as comma-separated), or None to leave unchanged.

    Returns:
        None.

    Side effects:
        DB write + commit on the files table. No-op if both description and
        tags are None.

    Called by:
        Route handler file_update (POST /api/files/{id}/update).
        batch_tag() in batch.py (per-file tag update).
    """
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


async def insert_file_copy(db, *, source_file_id, source_row, filename,
                          full_path, rel_path, location_id, folder_id):
    """Insert a new file record as a copy of an existing file.

    Creates the catalog entry and duplicates the hash record from hashes.db.
    Does NOT copy the physical file — the caller handles that.

    Parameters:
        db: Writable database connection.
        source_file_id: ID of the source file (for hash lookup).
        source_row: Dict with source file metadata — must include:
            file_size, description, tags, created_date, modified_date.
        filename: Destination filename.
        full_path: Destination absolute path.
        rel_path: Destination relative path.
        location_id: Destination location ID.
        folder_id: Destination folder ID (or None for location root).

    Returns:
        int: The new file ID.
    """
    type_high, type_low = classify_file(filename)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    cursor = await db.execute(
        """INSERT INTO files (filename, full_path, rel_path, location_id,
              folder_id, file_type_high, file_type_low, file_size,
              description, tags, created_date, modified_date,
              date_cataloged, date_last_seen)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            filename, full_path, rel_path, location_id, folder_id,
            type_high, type_low, source_row["file_size"],
            source_row.get("description") or "",
            source_row.get("tags") or "",
            source_row.get("created_date"),
            source_row.get("modified_date"),
            now_iso, now_iso,
        ),
    )
    new_file_id = cursor.lastrowid

    # Copy hash record from source
    src_hashes = await get_file_hashes([source_file_id])
    if source_file_id in src_hashes:
        h = src_hashes[source_file_id]
        async with hashes_writer() as hdb:
            await hdb.execute(
                """INSERT OR REPLACE INTO file_hashes
                      (file_id, location_id, file_size, hash_partial,
                       hash_fast, hash_strong, dup_count, excluded, stale)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, 0)""",
                (
                    new_file_id, location_id,
                    h.get("file_size", source_row["file_size"] or 0),
                    h.get("hash_partial"), h.get("hash_fast"),
                    h.get("hash_strong"), h.get("excluded", 0),
                ),
            )

    return new_file_id


async def move_file(
    db,
    file_id: int,
    *,
    new_name: str = None,
    destination_folder_id: str = None,
    skip_post_processing: bool = False,
    copy: bool = False,
):
    """Move, copy, and/or rename a file on disk and update the catalog.

    Handles same-location moves, cross-location moves (copy+delete via agent),
    copies (to any destination without deleting source), renames, and
    combinations. If the destination location is offline (or source for moves),
    the operation is queued as a deferred op.

    Parameters:
        db: Writable database connection (called inside execute_write).
        file_id: Numeric file ID.
        new_name: New filename, or None to keep the current name.
        destination_folder_id: Prefixed target ID ("loc-{id}" or "fld-{id}"),
            or None if only renaming in place.
        skip_post_processing: If True, skip post_op_stats broadcast. Used by
            batch_move() which does a single post_op_stats at the end.
        copy: If True, copy instead of move — source is left untouched and
            a new catalog entry is created at the destination.

    Returns:
        dict with keys: id, old_name, new_name, renamed (bool), moved (bool),
        deferred (bool), copied (bool).

    Raises:
        ValueError: File not found, destination not found, file missing on disk,
            or name collision on rename.

    Side effects:
        Disk I/O — file move/rename/copy via fs service.
        DB write + commit — for moves: updates existing record. For copies:
        inserts new file record and new hash record.
        Reclassifies file type if extension changed.
        Broadcasts updated stats via post_op_stats (unless skip_post_processing).
        May queue a deferred_op if location is offline.

    Called by:
        Route handler file_move (POST /api/files/{id}/move).
        batch_move() in batch.py (per-file, with skip_post_processing=True).
    """
    row = await db.execute_fetchall(
        """SELECT f.*, l.root_path AS location_root_path, l.name AS location_name,
              l.agent_id AS src_agent_id
           FROM files f JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        raise ValueError("File not found.")
    f = dict(row[0])

    src_root = f["location_root_path"]
    src_loc_id = f["location_id"]

    src_online = await fs.dir_exists(src_root, src_loc_id)

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
        dest = await resolve_target(db, destination_folder_id)
        if not dest:
            raise ValueError("Destination not found.")
        final_root = dest["root_path"]
        final_location_id = dest["location_id"]
        final_folder_id = dest["folder_id"]
        dest_dir = dest["abs_path"]

    new_full_path = os.path.join(dest_dir, final_name)
    new_rel_path = os.path.relpath(new_full_path, final_root)

    cross_location = final_location_id != src_loc_id
    dst_online = True
    if cross_location:
        dst_online = await fs.dir_exists(final_root, final_location_id)

    # If any involved location is offline, defer the operation
    # For copy: defer only when destination is offline (source must be readable)
    # For move: defer when either side is offline
    if copy:
        any_offline = not dst_online if cross_location else False
        if not src_online:
            raise ValueError("Source location is offline.")
    else:
        any_offline = not src_online or (cross_location and not dst_online)
    if any_offline:
        op_type = "copy" if copy else "move"
        params = {
            "dst_full_path": new_full_path,
            "dst_rel_path": new_rel_path,
            "dst_location_id": final_location_id,
            "dst_folder_id": final_folder_id,
            "dst_filename": final_name,
        }
        await queue_deferred_op(db, file_id, src_loc_id, op_type, params)
        await db.commit()

        if not skip_post_processing:
            await post_op_stats()
        return {
            "id": file_id,
            "old_name": old_name,
            "new_name": final_name,
            "renamed": renamed,
            "moved": moved,
            "deferred": True,
            "copied": copy,
        }

    # Both sides online — validate and execute immediately
    if not await fs.file_exists(f["full_path"], src_loc_id):
        raise ValueError("File not found on disk.")

    if moved and not await fs.dir_exists(dest_dir, final_location_id):
        raise ValueError("Destination directory does not exist on disk.")

    # Handle collision — auto-rename when moving/copying, error when renaming
    if new_full_path != f["full_path"]:
        if moved and not renamed:
            # Moving/copying to a different folder — auto-rename on collision
            new_full_path = await fs.unique_dest_path(new_full_path, final_location_id)
            final_name = os.path.basename(new_full_path)
            new_rel_path = os.path.relpath(new_full_path, final_root)
        elif await fs.path_exists(new_full_path, final_location_id):
            raise ValueError("A file with that name already exists at the destination.")

    # Copy or move on disk
    if new_full_path != f["full_path"]:
        if copy:
            # Copy always uses fs.copy_file regardless of same/cross location
            await fs.copy_file(
                f["full_path"],
                src_loc_id,
                new_full_path,
                final_location_id,
                mtime=parse_mtime(f["modified_date"]),
            )
        elif cross_location:
            # Check if both locations are on the same agent
            dst_row = await db.execute_fetchall(
                "SELECT agent_id FROM locations WHERE id = ?",
                (final_location_id,),
            )
            dst_agent_id = dst_row[0]["agent_id"] if dst_row else None
            same_agent = (
                f["src_agent_id"] is not None and f["src_agent_id"] == dst_agent_id
            )
            if same_agent:
                # Same agent — direct move via agent filesystem
                await fs.file_move(f["full_path"], new_full_path, src_loc_id)
            else:
                # Different agents — stream through server
                await fs.copy_file(
                    f["full_path"],
                    src_loc_id,
                    new_full_path,
                    final_location_id,
                    mtime=parse_mtime(f["modified_date"]),
                )
                await fs.file_delete(f["full_path"], src_loc_id)
        else:
            await fs.file_move(f["full_path"], new_full_path, final_location_id)

    # Reclassify if extension changed
    new_type_high, new_type_low = classify_file(final_name)

    if copy:
        # Insert new file record — source stays untouched
        new_file_id = await insert_file_copy(
            db,
            source_file_id=file_id,
            source_row=f,
            filename=final_name,
            full_path=new_full_path,
            rel_path=new_rel_path,
            location_id=final_location_id,
            folder_id=final_folder_id,
        )
        await db.commit()

        # Update destination folder stats (no source removal for copy)
        file_size = f["file_size"] or 0
        await update_stats_for_files(
            final_location_id,
            added=[(final_folder_id, file_size, new_type_high, f["hidden"])],
        )

        result_id = new_file_id
    else:
        # Move — update existing record
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

        # Sync hashes.db location_id for cross-location moves
        if final_location_id != src_loc_id:
            async with hashes_writer() as hdb:
                await hdb.execute(
                    "UPDATE file_hashes SET location_id = ? WHERE file_id = ?",
                    (final_location_id, file_id),
                )

        # Update folder/location stats if file changed folder
        if moved and final_folder_id != f["folder_id"]:
            file_size = f["file_size"] or 0
            old_type = f["file_type_high"]
            hidden = f["hidden"]
            await update_stats_for_files(
                src_loc_id,
                removed=[(f["folder_id"], file_size, old_type, hidden)],
            )
            await update_stats_for_files(
                final_location_id,
                added=[(final_folder_id, file_size, new_type_high, hidden)],
            )

        result_id = file_id

    if not skip_post_processing:
        loc_ids = (
            {src_loc_id, final_location_id}
            if final_location_id != src_loc_id
            else {src_loc_id}
        )
        await post_op_stats(location_ids=loc_ids)

    return {
        "id": result_id,
        "old_name": old_name,
        "new_name": final_name,
        "renamed": renamed,
        "moved": moved,
        "deferred": False,
        "copied": copy,
    }
