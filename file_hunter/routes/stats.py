from starlette.requests import Request
from file_hunter.db import open_connection
from file_hunter.core import json_ok, json_error
from file_hunter.services.stats import get_stats, get_location_stats, get_folder_stats


async def stats(request: Request):
    conn = await open_connection()
    try:
        data = await get_stats(conn)
    finally:
        await conn.close()
    return json_ok(data)


async def recalculate_stats(request: Request):
    """Force a full recalculation of all location sizes and stats cache."""
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.stats import invalidate_stats_cache

    conn = await open_connection()
    try:
        loc_rows = await conn.execute_fetchall("SELECT id FROM locations")
        for loc in loc_rows:
            await recalculate_location_sizes(conn, loc["id"])
    finally:
        await conn.close()
    invalidate_stats_cache()
    return json_ok({"recalculated": len(loc_rows)})


async def location_stats(request: Request):
    loc_id = int(request.path_params["id"])
    conn = await open_connection()
    try:
        data = await get_location_stats(conn, loc_id)
    finally:
        await conn.close()
    if not data:
        return json_error("Location not found.", 404)
    return json_ok(data)


async def folder_stats(request: Request):
    folder_id = int(request.path_params["id"])
    conn = await open_connection()
    try:
        data = await get_folder_stats(conn, folder_id)
    finally:
        await conn.close()
    if not data:
        return json_error("Folder not found.", 404)
    return json_ok(data)
