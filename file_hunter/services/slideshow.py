"""Slideshow ID queries.

Supports two modes:
- search cache: reads image IDs from the cached search results DB
- folder_id: all images in a folder/location root

Returns all matching IDs in one call. Only includes non-stale files
on online locations. Pre-computes online location set and filters
in SQL WHERE.
"""

import asyncio
import os
import sqlite3

from file_hunter.helpers import parse_prefixed_id
from file_hunter.services.locations import check_location_online
from file_hunter.services import search as search_mod
from file_hunter.services.settings import get_setting

# Sort column maps — same keys as FileList.sortKey on the client
FOLDER_SORT = {
    "name": "f.filename COLLATE NOCASE",
    "type": "f.file_type_low",
    "size": "f.file_size",
    "date": "f.modified_date",
    "dups": "f.dup_count",
}

SEARCH_SORT = {
    "name": "filename COLLATE NOCASE",
    "type": "file_type_low",
    "size": "file_size",
    "date": "modified_date",
    "dups": "dup_count",
}


async def get_slideshow_ids_from_search(
    search_id: str, *, media_type: str = "image",
    sort: str = "name", sort_dir: str = "asc",
) -> list[int]:
    """Pull media IDs from the cached search results DB.

    Uses the existing search cache — no re-query, correct scope, fast.
    Returns empty list if the cache is missing or expired.
    """
    # Read live off the web UI's search context — the cache moved there in
    # 1.3.3 when search state became per-caller.
    ctx = search_mod.ui_context
    if not search_id or search_id != ctx.search_id or not ctx.search_db_path:
        return []
    path = str(ctx.search_db_path)
    if not os.path.exists(path):
        return []

    col = SEARCH_SORT.get(sort, "filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"

    def read(p, mt):
        sdb = sqlite3.connect(p)
        sdb.row_factory = sqlite3.Row
        rows = sdb.execute(
            f"SELECT file_id FROM results WHERE file_type_high = ? ORDER BY {col} {direction}",
            (mt,),
        ).fetchall()
        sdb.close()
        return [r["file_id"] for r in rows]

    return await asyncio.to_thread(read, path, media_type)


async def get_slideshow_ids(
    db, *, folder_id=None, media_type: str = "image",
    sort: str = "name", sort_dir: str = "asc",
):
    """Return list of IDs for media files (image or video).

    folder_id must be provided.
    Returns all matching IDs in one call — the client navigates locally.
    """
    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"

    if folder_id:
        return await ids_for_folder(
            db, folder_id, hidden_filter, media_type, sort=sort, sort_dir=sort_dir,
        )
    return []


async def build_online_loc_ids(db, loc_ids_with_paths):
    """Check which locations are online, return set of online location IDs."""
    online = set()
    for loc_id, root_path in loc_ids_with_paths:
        if await asyncio.to_thread(check_location_online, loc_id, root_path):
            online.add(loc_id)
    return online


async def get_online_loc_filter(db, base_where, base_params):
    """Get distinct locations matching the base query, check online status,
    return (sql_fragment, params) for the IN clause."""
    rows = await db.execute_fetchall(
        f"""SELECT DISTINCT l.id, l.root_path
            FROM files f
            JOIN locations l ON l.id = f.location_id
            WHERE {base_where}""",
        base_params,
    )
    if not rows:
        return None, []

    loc_ids_with_paths = [(r["id"], r["root_path"]) for r in rows]
    online_ids = await build_online_loc_ids(db, loc_ids_with_paths)
    if not online_ids:
        return None, []

    placeholders = ",".join("?" * len(online_ids))
    return f"f.location_id IN ({placeholders})", list(online_ids)


async def ids_for_folder(
    db, folder_id, hidden_filter, media_type="image",
    *, sort="name", sort_dir="asc",
):
    """All media IDs in a folder/location root on online locations."""
    try:
        kind, num_id = parse_prefixed_id(folder_id)
    except ValueError:
        return []

    if kind == "loc":
        where = "f.location_id = ? AND f.folder_id IS NULL"
        params = [num_id]
    else:
        where = "f.folder_id = ?"
        params = [num_id]

    base_where = f"{where} AND f.file_type_high = ? AND f.stale = 0{hidden_filter}"
    params.append(media_type)

    loc_filter, loc_params = await get_online_loc_filter(db, base_where, params)
    if loc_filter is None:
        return []

    full_where = f"{base_where} AND {loc_filter}"
    full_params = params + loc_params

    col = FOLDER_SORT.get(sort, "f.filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"

    rows = await db.execute_fetchall(
        f"""SELECT f.id FROM files f
            WHERE {full_where}
            ORDER BY {col} {direction}""",
        full_params,
    )

    return [r["id"] for r in rows]
