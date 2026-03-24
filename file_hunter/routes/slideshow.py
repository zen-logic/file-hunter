from starlette.requests import Request
from file_hunter.db import read_db
from file_hunter.core import json_ok
from file_hunter.services.slideshow import get_slideshow_ids, get_slideshow_ids_from_search


async def slideshow_ids(request: Request):
    folder_id = request.query_params.get("folder_id")
    search_id = request.query_params.get("searchId")
    media_type = request.query_params.get("mediaType", "image")

    # If we have a cached search, pull IDs from it directly —
    # no re-query, correct scope, fast.
    if search_id:
        ids = await get_slideshow_ids_from_search(search_id, media_type=media_type)
        return json_ok({"ids": ids, "total": len(ids)})

    # Folder-based slideshow/playlist
    async with read_db() as db:
        ids = await get_slideshow_ids(db, folder_id=folder_id, media_type=media_type)
    return json_ok({"ids": ids, "total": len(ids)})
