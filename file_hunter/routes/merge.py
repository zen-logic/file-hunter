import os

from starlette.requests import Request

from file_hunter.core import json_error, json_ok, parse_node_id, parse_str, read_body
from file_hunter.db import read_db
from file_hunter.services import fs
from file_hunter.services.merge import (
    resolve_merge_target,
    is_merge_running,
    request_merge_cancel,
)
from file_hunter.services.queue_manager import enqueue


async def merge(request: Request):
    """POST /api/merge — start a merge background task."""
    body = await read_body(request)

    source_id = parse_node_id(body.get("source_id"), "source_id")
    destination_id = parse_node_id(body.get("destination_id"), "destination_id")
    mode = parse_str(body.get("mode"), "mode", "move")

    if mode not in ("move", "copy"):
        return json_error("mode must be 'move' or 'copy'.", 400)

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

    await enqueue(
        "merge",
        None,
        {
            "source_id": source_id,
            "source_info": source_info,
            "destination_id": destination_id,
            "dest_info": dest_info,
            "mode": mode,
        },
    )

    mode_label = "Move" if mode == "move" else "Copy"
    return json_ok(
        {
            "message": f"{mode_label} started: {source_info['label']} → {dest_info['label']}"
        }
    )


async def cancel_merge(request: Request):
    """POST /api/merge/cancel — cancel a running merge."""
    if not is_merge_running():
        return json_error("No merge is currently running.", 400)

    request_merge_cancel()
    return json_ok({"message": "Merge cancellation requested."})
