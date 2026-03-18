import asyncio
import os

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db
from file_hunter.services import fs
from file_hunter.services.merge import (
    resolve_merge_target,
    run_merge,
    is_merge_running,
    request_merge_cancel,
)


async def merge(request: Request):
    """POST /api/merge — start a merge background task."""
    body = await request.json()

    source_id = body.get("source_id")
    destination_id = body.get("destination_id")

    if not source_id or not destination_id:
        return json_error("source_id and destination_id are required.", 400)

    if source_id == destination_id:
        return json_error("Source and destination cannot be the same.", 400)

    # Resolve both targets
    async with read_db() as db:
        source_info = await resolve_merge_target(db, source_id)
        if not source_info:
            return json_error("Source not found.", 404)

        dest_info = await resolve_merge_target(db, destination_id)
    if not dest_info:
        return json_error("Destination not found.", 404)

    src_loc_id = source_info["location_id"]
    dest_loc_id = dest_info["location_id"]

    # Check both are online
    source_online = await fs.dir_exists(source_info["abs_path"], src_loc_id)
    if not source_online:
        return json_error("Source is offline.", 400)

    dest_online = await fs.dir_exists(dest_info["abs_path"], dest_loc_id)
    if not dest_online:
        return json_error("Destination is offline.", 400)

    # Prevent merging into self/descendant (only when same location)
    if src_loc_id == dest_loc_id:
        src_abs = os.path.normpath(source_info["abs_path"]) + os.sep
        dest_abs = os.path.normpath(dest_info["abs_path"])
        if dest_abs.startswith(src_abs) or dest_abs == os.path.normpath(
            source_info["abs_path"]
        ):
            return json_error("Cannot merge into source or its subfolder.", 400)

    # Guard concurrent merge
    if is_merge_running():
        return json_error("A merge is already in progress.", 409)

    asyncio.create_task(run_merge(source_id, source_info, destination_id, dest_info))

    return json_ok(
        {"message": f"Merge started: {source_info['label']} → {dest_info['label']}"}
    )


async def cancel_merge(request: Request):
    """POST /api/merge/cancel — cancel a running merge."""
    if not is_merge_running():
        return json_error("No merge is currently running.", 400)

    request_merge_cancel()
    return json_ok({"message": "Merge cancellation requested."})
