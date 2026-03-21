"""Dynamic search query builder."""

import re

PAGE_SIZE = 120


def _escape_like(value: str) -> str:
    """Escape LIKE special characters (% and _) for literal matching."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def _build_scope_sql(db, location_id=None, folder_id=None):
    """Return (file_frag, folder_frag, params) for scope filtering.

    For folder scope, pre-fetches all descendant folder IDs so the main
    query uses a flat IN clause that SQLite can resolve via index.
    """
    if folder_id:
        rows = await db.execute_fetchall(
            "WITH RECURSIVE descendants(id) AS ("
            " SELECT ? UNION ALL"
            " SELECT fo.id FROM folders fo JOIN descendants d ON fo.parent_id = d.id"
            ") SELECT id FROM descendants",
            (folder_id,),
        )
        folder_ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(folder_ids))
        return (
            f"f.folder_id IN ({placeholders})",
            f"fld.id IN ({placeholders})",
            folder_ids,
        )
    if location_id:
        return (
            "f.location_id = ?",
            "fld.location_id = ?",
            [location_id],
        )
    return ("", "", [])


SORT_COLUMNS = {
    "name": "f.filename",
    "type": "f.file_type_low",
    "size": "f.file_size",
    "date": "f.modified_date",
    "dups": "f.dup_count",
}


def parse_size(value: str) -> int | None:
    """Parse human-readable size string to bytes. E.g. '5MB' -> 5242880."""
    if not value:
        return None
    value = value.strip().upper()
    match = re.match(r"^([\d.]+)\s*(B|KB|MB|GB|TB)?$", value)
    if not match:
        # Try as raw number (bytes)
        try:
            return int(float(value))
        except ValueError:
            return None
    num = float(match.group(1))
    unit = match.group(2) or "B"
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1048576,
        "GB": 1073741824,
        "TB": 1099511627776,
    }
    return int(num * multipliers[unit])


async def search_files(
    db,
    *,
    name=None,
    file_type=None,
    description=None,
    tags=None,
    size_min=None,
    size_max=None,
    date_from=None,
    date_to=None,
    name_match="anywhere",
    include_files=True,
    dupes_only=False,
    min_dups=None,
    max_dups=None,
    hash_strong=None,
    include_folders=False,
    location_id=None,
    folder_id=None,
    page=0,
    sort="name",
    sort_dir="asc",
    cached_total=None,
):
    """Search files with optional filters. Returns paged envelope."""
    from file_hunter.services.settings import get_setting

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    scope_file_frag, scope_folder_frag, scope_params = await _build_scope_sql(
        db, location_id=location_id, folder_id=folder_id
    )

    conditions = []
    params = list(scope_params)

    if scope_file_frag:
        conditions.append(scope_file_frag)

    if not show_hidden:
        conditions.append("f.hidden = 0")

    if name:
        if name_match == "wildcard":
            # Escape LIKE special chars, then convert glob wildcards
            escaped = _escape_like(name)
            pattern = escaped.replace("*", "%").replace("?", "_")
            conditions.append("f.filename LIKE ? ESCAPE '\\'")
        elif name_match == "exact":
            pattern = name
            conditions.append("f.filename = ?")
        else:
            escaped = _escape_like(name)
            match_patterns = {
                "starts": f"{escaped}%",
                "ends": f"%{escaped}",
            }
            pattern = match_patterns.get(name_match, f"%{escaped}%")
            conditions.append("f.filename LIKE ? ESCAPE '\\'")
        params.append(pattern)

    if file_type:
        if file_type == "other":
            conditions.append(
                "f.file_type_high NOT IN ('image','video','audio','document','text','compressed','font')"
            )
        else:
            conditions.append("f.file_type_high = ?")
            params.append(file_type)

    if description:
        conditions.append("f.description LIKE ? ESCAPE '\\'")
        params.append(f"%{_escape_like(description)}%")

    if tags:
        for tag in [t.strip() for t in tags.split(",") if t.strip()]:
            conditions.append("f.tags LIKE ? ESCAPE '\\'")
            params.append(f"%{_escape_like(tag)}%")

    size_min_bytes = parse_size(size_min) if size_min else None
    size_max_bytes = parse_size(size_max) if size_max else None

    if size_min_bytes is not None:
        conditions.append("f.file_size >= ?")
        params.append(size_min_bytes)

    if size_max_bytes is not None:
        conditions.append("f.file_size <= ?")
        params.append(size_max_bytes)

    if date_from:
        conditions.append("f.modified_date >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("f.modified_date <= ?")
        params.append(date_to + "T23:59:59")

    if dupes_only:
        conditions.append("f.dup_count > 0")

    if min_dups is not None:
        try:
            min_dups_val = int(min_dups)
            if min_dups_val > 0:
                conditions.append("f.dup_count >= ?")
                params.append(min_dups_val)
        except (ValueError, TypeError):
            pass

    if max_dups is not None:
        try:
            max_dups_val = int(max_dups)
            if max_dups_val > 0:
                conditions.append("f.dup_count <= ?")
                params.append(max_dups_val)
        except (ValueError, TypeError):
            pass

    if hash_strong:
        conditions.append("(f.hash_strong = ? OR f.hash_fast = ?)")
        params.extend([hash_strong, hash_strong])

    where = " AND ".join(conditions) if conditions else "1=1"

    col = SORT_COLUMNS.get(sort, "f.filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"
    offset = page * PAGE_SIZE

    total = 0
    items = []
    if include_files:
        # Count total matching — skip if client cached it from a previous page
        if cached_total is not None:
            total = cached_total
        else:
            count_row = await db.execute_fetchall(
                f"SELECT COUNT(*) as cnt FROM files f WHERE {where}",
                params,
            )
            total = count_row[0]["cnt"] if count_row else 0

        # Fetch paged results (structural data — hashes from hashes.db)
        rows = await db.execute_fetchall(
            f"""SELECT f.id, f.filename, f.file_type_high, f.file_type_low,
                       f.file_size, f.modified_date, f.full_path,
                       f.stale, f.location_id, f.hidden, l.root_path
                FROM files f
                JOIN locations l ON l.id = f.location_id
                WHERE {where}
                ORDER BY {col} {direction}
                LIMIT ? OFFSET ?""",
            params + [PAGE_SIZE, offset],
        )

        # All locations are agent-backed — no local file existence checks
        missing_set = set()

        from file_hunter.hashes_db import get_file_hashes
        from file_hunter.services.dup_counts import batch_dup_counts

        file_ids = [r["id"] for r in rows]
        hash_map = await get_file_hashes(file_ids)

        strong_list = [h["hash_strong"] for h in hash_map.values() if h["hash_strong"]]
        fast_list = [
            h["hash_fast"]
            for h in hash_map.values()
            if not h["hash_strong"] and h["hash_fast"]
        ]
        live_dups = await batch_dup_counts(
            strong_hashes=strong_list, fast_hashes=fast_list
        )

        items = []
        for r in rows:
            h = hash_map.get(r["id"], {})
            hs = h.get("hash_strong")
            hf = h.get("hash_fast")
            items.append(
                {
                    "id": r["id"],
                    "name": r["filename"],
                    "typeHigh": r["file_type_high"],
                    "typeLow": r["file_type_low"],
                    "size": r["file_size"],
                    "date": r["modified_date"],
                    "dups": live_dups.get(hs or hf, 0),
                    "hashStrong": hs,
                    "hashFast": hf,
                    "stale": bool(r["stale"]),
                    "missing": False if r["stale"] else r["id"] in missing_set,
                    "hidden": bool(r["hidden"]),
                }
            )

    # Folder search (name filter only)
    folders = []
    if include_folders and name:
        if name_match == "wildcard":
            escaped = _escape_like(name)
            folder_pattern = escaped.replace("*", "%").replace("?", "_")
            folder_cond = "fld.name LIKE ? ESCAPE '\\'"
        elif name_match == "exact":
            folder_pattern = name
            folder_cond = "fld.name = ?"
        else:
            escaped = _escape_like(name)
            folder_match = {
                "starts": f"{escaped}%",
                "ends": f"%{escaped}",
            }
            folder_pattern = folder_match.get(name_match, f"%{escaped}%")
            folder_cond = "fld.name LIKE ? ESCAPE '\\'"
        folder_hidden_filter = "" if show_hidden else " AND fld.hidden = 0"
        folder_scope_prefix = f"{scope_folder_frag} AND " if scope_folder_frag else ""
        folder_scope_params = list(scope_params) if scope_folder_frag else []
        folder_rows = await db.execute_fetchall(
            f"""SELECT fld.id, fld.name, fld.location_id, l.name as location_name
               FROM folders fld
               JOIN locations l ON l.id = fld.location_id
               WHERE {folder_scope_prefix}{folder_cond}{folder_hidden_filter}
               ORDER BY fld.name
               LIMIT ?""",
            folder_scope_params + [folder_pattern, PAGE_SIZE],
        )
        folders = [
            {
                "id": f"fld-{r['id']}",
                "name": r["name"],
                "type": "folder",
                "size": None,
                "date": None,
                "location": r["location_name"],
            }
            for r in folder_rows
        ]

    return {
        "items": items,
        "folders": folders,
        "total": total,
        "page": page,
        "pageSize": PAGE_SIZE,
    }


# ── Advanced search helpers ──


def parse_conditions_from_params(params) -> list[dict]:
    """Parse indexed condition params (c0_field, c0_op, c0_value, etc.)."""
    conditions = []
    i = 0
    while True:
        field = params.get(f"c{i}_field")
        if field is None:
            break
        cond = {
            "field": field,
            "op": params.get(f"c{i}_op", "include"),
        }
        if field in ("size",):
            cond["min"] = params.get(f"c{i}_min", "")
            cond["max"] = params.get(f"c{i}_max", "")
        elif field in ("date", "duplicates"):
            cond["from"] = params.get(f"c{i}_from", "")
            cond["to"] = params.get(f"c{i}_to", "")
        else:
            cond["value"] = params.get(f"c{i}_value", "")
            cond["match"] = params.get(f"c{i}_match", "")
        conditions.append(cond)
        i += 1
    return conditions


def _build_name_like(value, match_mode, column="f.filename"):
    """Build SQL fragment + params for a name/folder LIKE condition."""
    if match_mode == "wildcard":
        escaped = _escape_like(value)
        pattern = escaped.replace("*", "%").replace("?", "_")
        return f"{column} LIKE ? ESCAPE '\\'", [pattern]
    elif match_mode == "exact":
        return f"{column} = ?", [value]
    else:
        escaped = _escape_like(value)
        match_patterns = {
            "starts": f"{escaped}%",
            "ends": f"%{escaped}",
        }
        pattern = match_patterns.get(match_mode, f"%{escaped}%")
        return f"{column} LIKE ? ESCAPE '\\'", [pattern]


def build_condition_sql(cond):
    """Build (sql_fragment, params_list) for a single advanced condition.

    Returns (None, []) if the condition is empty/no-op.
    """
    field = cond["field"]
    value = cond.get("value", "")
    match_mode = cond.get("match", "wildcard")

    if field == "name":
        if not value:
            return None, []
        return _build_name_like(value, match_mode, "f.filename")

    elif field == "type":
        if not value:
            return None, []
        if value == "other":
            return (
                "f.file_type_high NOT IN ('image','video','audio','document','text','compressed','font')",
                [],
            )
        return "f.file_type_high = ?", [value]

    elif field == "description":
        if not value:
            return None, []
        return "f.description LIKE ? ESCAPE '\\'", [f"%{_escape_like(value)}%"]

    elif field == "tags":
        if not value:
            return None, []
        tag_list = [t.strip() for t in value.split(",") if t.strip()]
        if not tag_list:
            return None, []
        frags = []
        params = []
        for tag in tag_list:
            frags.append("f.tags LIKE ? ESCAPE '\\'")
            params.append(f"%{_escape_like(tag)}%")
        return "(" + " AND ".join(frags) + ")", params

    elif field == "size":
        min_val = cond.get("min", "")
        max_val = cond.get("max", "")
        min_bytes = parse_size(min_val) if min_val else None
        max_bytes = parse_size(max_val) if max_val else None
        if min_bytes is None and max_bytes is None:
            return None, []
        frags = []
        params = []
        if min_bytes is not None:
            frags.append("f.file_size >= ?")
            params.append(min_bytes)
        if max_bytes is not None:
            frags.append("f.file_size <= ?")
            params.append(max_bytes)
        return "(" + " AND ".join(frags) + ")", params

    elif field == "date":
        date_from = cond.get("from", "")
        date_to = cond.get("to", "")
        if not date_from and not date_to:
            return None, []
        frags = []
        params = []
        if date_from:
            frags.append("f.modified_date >= ?")
            params.append(date_from)
        if date_to:
            frags.append("f.modified_date <= ?")
            params.append(date_to + "T23:59:59")
        return "(" + " AND ".join(frags) + ")", params

    elif field == "folder":
        if not value:
            return None, []
        frag, params = _build_name_like(value, match_mode, "fld.name")
        return (
            f"EXISTS (SELECT 1 FROM folders fld WHERE fld.id = f.folder_id AND {frag})",
            params,
        )

    elif field == "duplicates":
        frags = []
        params = []
        if cond.get("from"):
            try:
                min_val = int(cond["from"])
                if min_val > 0:
                    frags.append("f.dup_count >= ?")
                    params.append(min_val)
            except (ValueError, TypeError):
                pass
        if cond.get("to"):
            try:
                max_val = int(cond["to"])
                if max_val > 0:
                    frags.append("f.dup_count <= ?")
                    params.append(max_val)
            except (ValueError, TypeError):
                pass
        if frags:
            return " AND ".join(frags), params
        return None, []

    return None, []


async def search_files_advanced(
    db,
    *,
    conditions,
    include_files=True,
    include_folders=False,
    location_id=None,
    folder_id=None,
    page=0,
    sort="name",
    sort_dir="asc",
    cached_total=None,
):
    """Search files with advanced include/exclude conditions."""
    from file_hunter.services.settings import get_setting

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    scope_file_frag, scope_folder_frag, scope_params = await _build_scope_sql(
        db, location_id=location_id, folder_id=folder_id
    )

    where_parts = []
    where_params = list(scope_params)

    if scope_file_frag:
        where_parts.append(scope_file_frag)

    if not show_hidden:
        where_parts.append("f.hidden = 0")

    for cond in conditions:
        frag, params = build_condition_sql(cond)
        if frag is None:
            continue
        if cond["op"] == "exclude":
            where_parts.append(f"NOT ({frag})")
        else:
            where_parts.append(f"({frag})")
        where_params.extend(params)

    where = " AND ".join(where_parts) if where_parts else "1=1"

    col = SORT_COLUMNS.get(sort, "f.filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"
    offset = page * PAGE_SIZE

    total = 0
    items = []
    if include_files:
        if cached_total is not None:
            total = cached_total
        else:
            count_row = await db.execute_fetchall(
                f"SELECT COUNT(*) as cnt FROM files f WHERE {where}",
                where_params,
            )
            total = count_row[0]["cnt"] if count_row else 0

        rows = await db.execute_fetchall(
            f"""SELECT f.id, f.filename, f.file_type_high, f.file_type_low,
                       f.file_size, f.modified_date, f.full_path,
                       f.stale, f.location_id, f.hidden, l.root_path
                FROM files f
                JOIN locations l ON l.id = f.location_id
                WHERE {where}
                ORDER BY {col} {direction}
                LIMIT ? OFFSET ?""",
            where_params + [PAGE_SIZE, offset],
        )

        missing_set = set()

        from file_hunter.hashes_db import get_file_hashes
        from file_hunter.services.dup_counts import batch_dup_counts

        file_ids = [r["id"] for r in rows]
        hash_map = await get_file_hashes(file_ids)

        strong_list = [h["hash_strong"] for h in hash_map.values() if h["hash_strong"]]
        fast_list = [
            h["hash_fast"]
            for h in hash_map.values()
            if not h["hash_strong"] and h["hash_fast"]
        ]
        live_dups = await batch_dup_counts(
            strong_hashes=strong_list, fast_hashes=fast_list
        )

        for r in rows:
            h = hash_map.get(r["id"], {})
            hs = h.get("hash_strong")
            hf = h.get("hash_fast")
            items.append(
                {
                    "id": r["id"],
                    "name": r["filename"],
                    "typeHigh": r["file_type_high"],
                    "typeLow": r["file_type_low"],
                    "size": r["file_size"],
                    "date": r["modified_date"],
                    "dups": live_dups.get(hs or hf, 0),
                    "hashStrong": hs,
                    "hashFast": hf,
                    "stale": bool(r["stale"]),
                    "missing": False if r["stale"] else r["id"] in missing_set,
                    "hidden": bool(r["hidden"]),
                }
            )

    # Folder search — apply name conditions to folder name
    folders = []
    if include_folders:
        folder_where_parts = []
        folder_params = list(scope_params) if scope_folder_frag else []
        if scope_folder_frag:
            folder_where_parts.append(scope_folder_frag)
        if not show_hidden:
            folder_where_parts.append("fld.hidden = 0")
        has_name_cond = False
        for cond in conditions:
            if cond["field"] != "name":
                continue
            value = cond.get("value", "")
            if not value:
                continue
            has_name_cond = True
            match_mode = cond.get("match", "wildcard")
            frag, params = _build_name_like(value, match_mode, "fld.name")
            if cond["op"] == "exclude":
                folder_where_parts.append(f"NOT ({frag})")
            else:
                folder_where_parts.append(f"({frag})")
            folder_params.extend(params)

        if has_name_cond and folder_where_parts:
            folder_where = " AND ".join(folder_where_parts)
            folder_rows = await db.execute_fetchall(
                f"""SELECT fld.id, fld.name, fld.location_id, l.name as location_name
                   FROM folders fld
                   JOIN locations l ON l.id = fld.location_id
                   WHERE {folder_where}
                   ORDER BY fld.name
                   LIMIT ?""",
                folder_params + [PAGE_SIZE],
            )
            folders = [
                {
                    "id": f"fld-{r['id']}",
                    "name": r["name"],
                    "type": "folder",
                    "size": None,
                    "date": None,
                    "location": r["location_name"],
                }
                for r in folder_rows
            ]

    return {
        "items": items,
        "folders": folders,
        "total": total,
        "page": page,
        "pageSize": PAGE_SIZE,
    }
