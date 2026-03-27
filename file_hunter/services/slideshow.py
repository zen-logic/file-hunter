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

from file_hunter.hashes_db import read_hashes
from file_hunter.helpers import parse_prefixed_id
from file_hunter.services.locations import check_location_online
from file_hunter.services.search import (
    _search_db_path,
    _search_id,
    build_condition_sql,
    parse_conditions_from_params,
    parse_size,
)
from file_hunter.services.settings import get_setting


async def get_slideshow_ids_from_search(
    search_id: str, *, media_type: str = "image"
) -> list[int]:
    """Pull media IDs from the cached search results DB.

    Uses the existing search cache — no re-query, correct scope, fast.
    Returns empty list if the cache is missing or expired.
    """
    if not search_id or search_id != _search_id or not _search_db_path:
        return []
    path = str(_search_db_path)
    if not os.path.exists(path):
        return []

    def _read(p, mt):
        sdb = sqlite3.connect(p)
        sdb.row_factory = sqlite3.Row
        rows = sdb.execute(
            "SELECT file_id FROM results WHERE file_type_high = ? ORDER BY filename",
            (mt,),
        ).fetchall()
        sdb.close()
        return [r["file_id"] for r in rows]

    return await asyncio.to_thread(_read, path, media_type)


async def get_slideshow_ids(db, *, folder_id=None, media_type: str = "image"):
    """Return list of IDs for media files (image or video).

    folder_id must be provided.
    Returns all matching IDs in one call — the client navigates locally.
    """
    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"

    if folder_id:
        return await _ids_for_folder(db, folder_id, hidden_filter, media_type)
    return []


async def _build_online_loc_ids(db, loc_ids_with_paths):
    """Check which locations are online, return set of online location IDs."""
    online = set()
    for loc_id, root_path in loc_ids_with_paths:
        if await asyncio.to_thread(check_location_online, loc_id, root_path):
            online.add(loc_id)
    return online


async def _get_online_loc_filter(db, base_where, base_params):
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
    online_ids = await _build_online_loc_ids(db, loc_ids_with_paths)
    if not online_ids:
        return None, []

    placeholders = ",".join("?" * len(online_ids))
    return f"f.location_id IN ({placeholders})", list(online_ids)


async def _ids_for_folder(db, folder_id, hidden_filter, media_type="image"):
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

    loc_filter, loc_params = await _get_online_loc_filter(db, base_where, params)
    if loc_filter is None:
        return []

    full_where = f"{base_where} AND {loc_filter}"
    full_params = params + loc_params

    rows = await db.execute_fetchall(
        f"""SELECT f.id FROM files f
            WHERE {full_where}
            ORDER BY f.filename""",
        full_params,
    )

    return [r["id"] for r in rows]


async def _ids_for_search(db, search_params, hidden_filter):
    """Search with image type filter, return all matching IDs."""
    if search_params.get("mode") == "advanced":
        return await _ids_for_advanced_search(db, search_params, hidden_filter)

    conditions = ["f.file_type_high = 'image'", "f.stale = 0"]
    params = []

    if hidden_filter:
        conditions.append("f.hidden = 0")

    name = search_params.get("name")
    name_match = search_params.get("nameMatch", "anywhere")
    if name:
        if name_match == "wildcard":
            escaped = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            pattern = escaped.replace("*", "%").replace("?", "_")
            conditions.append("f.filename LIKE ? ESCAPE '\\'")
        elif name_match == "exact":
            pattern = name
            conditions.append("f.filename = ?")
        else:
            match_patterns = {
                "starts": f"{name}%",
                "ends": f"%{name}",
            }
            pattern = match_patterns.get(name_match, f"%{name}%")
            conditions.append("f.filename LIKE ?")
        params.append(pattern)

    description = search_params.get("description")
    if description:
        conditions.append("f.description LIKE ?")
        params.append(f"%{description}%")

    tags = search_params.get("tags")
    if tags:
        for tag in [t.strip() for t in tags.split(",") if t.strip()]:
            conditions.append("f.tags LIKE ?")
            params.append(f"%{tag}%")

    size_min = search_params.get("sizeMin")
    size_max = search_params.get("sizeMax")
    size_min_bytes = parse_size(size_min) if size_min else None
    size_max_bytes = parse_size(size_max) if size_max else None

    if size_min_bytes is not None:
        conditions.append("f.file_size >= ?")
        params.append(size_min_bytes)
    if size_max_bytes is not None:
        conditions.append("f.file_size <= ?")
        params.append(size_max_bytes)

    date_from = search_params.get("dateFrom")
    date_to = search_params.get("dateTo")
    if date_from:
        conditions.append("f.modified_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("f.modified_date <= ?")
        params.append(date_to + "T23:59:59")

    dupes = search_params.get("dupes")
    if dupes:
        conditions.append("f.dup_count > 0")

    min_dups = search_params.get("minDups")
    if min_dups:
        try:
            min_dups_val = int(min_dups)
            if min_dups_val > 0:
                conditions.append("f.dup_count >= ?")
                params.append(min_dups_val)
        except (ValueError, TypeError):
            pass

    max_dups = search_params.get("maxDups")
    if max_dups:
        try:
            max_dups_val = int(max_dups)
            if max_dups_val > 0:
                conditions.append("f.dup_count <= ?")
                params.append(max_dups_val)
        except (ValueError, TypeError):
            pass

    hash_val = search_params.get("hash")
    if hash_val:
        # Hashes live in hashes.db, not catalog — look up file IDs there
        async with read_hashes() as hdb:
            hash_rows = await hdb.execute_fetchall(
                "SELECT file_id FROM active_hashes "
                "WHERE hash_strong = ? OR hash_fast = ?",
                (hash_val, hash_val),
            )
        if hash_rows:
            hash_file_ids = [r["file_id"] for r in hash_rows]
            ph = ",".join("?" for _ in hash_file_ids)
            conditions.append(f"f.id IN ({ph})")
            params.extend(hash_file_ids)
        else:
            conditions.append("0")

    base_where = " AND ".join(conditions)

    loc_filter, loc_params = await _get_online_loc_filter(db, base_where, params)
    if loc_filter is None:
        return []

    full_where = f"{base_where} AND {loc_filter}"
    full_params = params + loc_params

    rows = await db.execute_fetchall(
        f"""SELECT f.id FROM files f
            WHERE {full_where}
            ORDER BY f.filename""",
        full_params,
    )

    return [r["id"] for r in rows]


async def _ids_for_advanced_search(db, search_params, hidden_filter):
    """Advanced search with image type filter, return all matching IDs."""
    conditions_list = parse_conditions_from_params(search_params)

    where_parts = ["f.file_type_high = 'image'", "f.stale = 0"]
    params = []

    if hidden_filter:
        where_parts.append("f.hidden = 0")

    for cond in conditions_list:
        frag, cond_params = build_condition_sql(cond)
        if frag is None:
            continue
        if cond["op"] == "exclude":
            where_parts.append(f"NOT ({frag})")
        else:
            where_parts.append(f"({frag})")
        params.extend(cond_params)

    base_where = " AND ".join(where_parts)

    loc_filter, loc_params = await _get_online_loc_filter(db, base_where, params)
    if loc_filter is None:
        return []

    full_where = f"{base_where} AND {loc_filter}"
    full_params = params + loc_params

    rows = await db.execute_fetchall(
        f"""SELECT f.id FROM files f
            WHERE {full_where}
            ORDER BY f.filename""",
        full_params,
    )

    return [r["id"] for r in rows]
