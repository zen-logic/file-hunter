import json

from starlette.requests import Request
from file_hunter.db import read_db, execute_write
from file_hunter.core import json_ok, json_error
from file_hunter.services.activity import register as _act_reg, unregister as _act_unreg
from file_hunter.services.search import (
    search_files,
    search_files_advanced,
    parse_conditions_from_params,
)


async def search(request: Request):
    page = int(request.query_params.get("page", 0))
    sort = request.query_params.get("sort", "name")
    sort_dir = request.query_params.get("sortDir", "asc")

    scope_type = request.query_params.get("scopeType")
    scope_id_raw = request.query_params.get("scopeId", "")
    # Node IDs are prefixed (e.g. "fld-123", "loc-42") — strip to numeric
    scope_id = scope_id_raw.split("-", 1)[-1] if "-" in scope_id_raw else scope_id_raw
    location_id = int(scope_id) if scope_type == "location" and scope_id else None
    folder_id = int(scope_id) if scope_type == "folder" and scope_id else None

    # Only track new searches, not cached page fetches
    is_new_search = not request.query_params.get("searchId")
    act_name = f"search-{id(request)}"
    if is_new_search:
        _act_reg(act_name, "Search")

    try:
        return await _do_search(request, page, sort, sort_dir, location_id, folder_id)
    except Exception as e:
        if "interrupted" in str(e):
            return json_ok(
                {
                    "items": [],
                    "folders": [],
                    "total": 0,
                    "page": 0,
                    "pageSize": 120,
                    "cancelled": True,
                }
            )
        raise
    finally:
        if is_new_search:
            _act_unreg(act_name)


async def _do_search(request, page, sort, sort_dir, location_id, folder_id):
    async with read_db() as db:
        if request.query_params.get("mode") == "advanced":
            conditions = parse_conditions_from_params(request.query_params)
            results = await search_files_advanced(
                db,
                conditions=conditions,
                include_files=request.query_params.get("files") != "false",
                include_folders=request.query_params.get("folders") == "true",
                location_id=location_id,
                folder_id=folder_id,
                page=page,
                sort=sort,
                sort_dir=sort_dir,
                cached_total=int(request.query_params["cachedTotal"])
                if "cachedTotal" in request.query_params
                else None,
                search_id=request.query_params.get("searchId"),
            )
        else:
            results = await search_files(
                db,
                name=request.query_params.get("name"),
                file_type=request.query_params.get("type"),
                description=request.query_params.get("description"),
                tags=request.query_params.get("tags"),
                size_min=request.query_params.get("sizeMin"),
                size_max=request.query_params.get("sizeMax"),
                date_from=request.query_params.get("dateFrom"),
                date_to=request.query_params.get("dateTo"),
                name_match=request.query_params.get("nameMatch", "anywhere"),
                include_files=request.query_params.get("files") != "false",
                dupes_only=bool(request.query_params.get("dupes")),
                min_dups=request.query_params.get("minDups"),
                max_dups=request.query_params.get("maxDups"),
                include_folders=request.query_params.get("folders") == "true",
                hash_strong=request.query_params.get("hash"),
                location_id=location_id,
                folder_id=folder_id,
                page=page,
                sort=sort,
                sort_dir=sort_dir,
                cached_total=int(request.query_params["cachedTotal"])
                if "cachedTotal" in request.query_params
                else None,
                search_id=request.query_params.get("searchId"),
            )
    return json_ok(results)


async def list_saved_searches(request: Request):
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, name, params, created_at FROM saved_searches ORDER BY created_at DESC"
        )
    return json_ok([dict(r) for r in rows])


async def create_saved_search(request: Request):
    data = await request.json()
    name = data.get("name", "").strip()
    params = data.get("params")
    if not name or not params:
        return json_error("name and params required")

    async def _insert(conn, n, p):
        cursor = await conn.execute(
            "INSERT INTO saved_searches (name, params) VALUES (?, ?)",
            (n, json.dumps(p) if isinstance(p, dict) else str(p)),
        )
        await conn.commit()
        return cursor.lastrowid

    row_id = await execute_write(_insert, name, params)
    return json_ok({"id": row_id})


async def delete_saved_search(request: Request):
    search_id = request.path_params["id"]

    async def _delete(conn, sid):
        await conn.execute("DELETE FROM saved_searches WHERE id = ?", (sid,))
        await conn.commit()

    await execute_write(_delete, search_id)
    return json_ok({})
