from starlette.requests import Request
from file_hunter.db import read_db
from file_hunter.core import json_ok
from file_hunter.services.slideshow import get_slideshow_ids


async def slideshow_ids(request: Request):
    folder_id = request.query_params.get("folder_id")

    # Collect search params if present
    search_keys = [
        "name",
        "nameMatch",
        "description",
        "tags",
        "sizeMin",
        "sizeMax",
        "dateFrom",
        "dateTo",
        "dupes",
        "minDups",
        "maxDups",
        "hash",
        "mode",
    ]
    search_params = {}
    for key in search_keys:
        val = request.query_params.get(key)
        if val is not None:
            search_params[key] = val

    # Collect advanced condition params (c0_field, c0_op, etc.)
    if search_params.get("mode") == "advanced":
        for key in request.query_params:
            if key.startswith("c") and "_" in key:
                search_params[key] = request.query_params[key]

    async with read_db() as db:
        ids = await get_slideshow_ids(
            db,
            folder_id=folder_id,
            search_params=search_params if search_params else None,
        )
    return json_ok({"ids": ids, "total": len(ids)})
