"""Batch operation route handlers."""

from starlette.requests import Request
from file_hunter.db import read_db, execute_write
from file_hunter.core import json_ok, json_error
from file_hunter.services.batch import (
    batch_delete,
    batch_move,
    batch_tag,
    batch_collect_files,
    build_streaming_zip,
)
from file_hunter.helpers import post_op_stats
from file_hunter.ws.scan import broadcast


async def batch_delete_route(request: Request):
    """POST /api/batch/delete — delete multiple files and folders."""
    body = await request.json()
    file_ids = body.get("file_ids", [])
    folder_ids = body.get("folder_ids", [])
    all_duplicates = body.get("all_duplicates", False)

    if not file_ids and not folder_ids:
        return json_error("No items to delete.")

    result = await execute_write(batch_delete, file_ids, folder_ids, all_duplicates)
    await post_op_stats()

    await broadcast(
        {
            "type": "batch_deleted",
            "deletedFiles": result["deleted_files"],
            "deletedFolders": result["deleted_folders"],
        }
    )

    return json_ok(result)


async def batch_move_route(request: Request):
    """POST /api/batch/move — move multiple files and folders."""
    body = await request.json()
    file_ids = body.get("file_ids", [])
    folder_ids = body.get("folder_ids", [])
    destination_folder_id = body.get("destination_folder_id")

    if not file_ids and not folder_ids:
        return json_error("No items to move.")
    if not destination_folder_id:
        return json_error("destination_folder_id is required.")

    result = await execute_write(
        batch_move, file_ids, folder_ids, destination_folder_id
    )
    await post_op_stats()

    await broadcast(
        {
            "type": "batch_moved",
            "movedFiles": result["moved_files"],
            "movedFolders": result["moved_folders"],
        }
    )

    return json_ok(result)


async def batch_tag_route(request: Request):
    """POST /api/batch/tag — add/remove tags on multiple files."""
    body = await request.json()
    file_ids = body.get("file_ids", [])
    add_tags = body.get("add_tags", [])
    remove_tags = body.get("remove_tags", [])

    if not file_ids:
        return json_error("No files specified.")
    if not add_tags and not remove_tags:
        return json_error("No tags to add or remove.")

    result = await execute_write(batch_tag, file_ids, add_tags, remove_tags)

    return json_ok(result)


async def batch_download_route(request: Request):
    """POST /api/batch/download — download selected items as ZIP."""
    body = await request.json()
    file_ids = body.get("file_ids", [])
    folder_ids = body.get("folder_ids", [])

    if not file_ids and not folder_ids:
        return json_error("No items to download.")

    async with read_db() as db:
        files = await batch_collect_files(db, file_ids, folder_ids)
    return await build_streaming_zip(files, "file-hunter-selection.zip")
