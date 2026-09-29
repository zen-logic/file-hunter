"""Batch operation route handlers."""

from starlette.requests import Request
from file_hunter.db import read_db, execute_write
from file_hunter.core import json_error, json_ok, parse_bool, parse_int_array, parse_node_id, parse_tag_input, read_body
from file_hunter.services.batch import (
    batch_move,
    batch_collect_files,
)
from file_hunter.services.queue_manager import enqueue
from file_hunter.services.tags import parse_tags
from file_hunter.services.zip_download import start_build
from file_hunter.helpers import post_op_stats
from file_hunter.ws.scan import broadcast


async def batch_delete_route(request: Request):
    """POST /api/batch/delete — delete multiple files and folders."""
    body = await read_body(request)
    file_ids = parse_int_array(body.get("file_ids"), "file_ids")
    folder_ids = parse_int_array(body.get("folder_ids"), "folder_ids")
    all_duplicates = parse_bool(body.get("all_duplicates"), "all_duplicates")

    if not file_ids and not folder_ids:
        return json_error("No items to delete.")

    op_id = await enqueue("batch_delete", None, {
        "file_ids": file_ids,
        "folder_ids": folder_ids,
        "all_duplicates": all_duplicates,
    })
    return json_ok({"started": True, "op_id": op_id, "total": len(file_ids) + len(folder_ids)})


async def batch_move_route(request: Request):
    """POST /api/batch/move — move or copy multiple files and folders."""
    body = await read_body(request)
    file_ids = parse_int_array(body.get("file_ids"), "file_ids")
    folder_ids = parse_int_array(body.get("folder_ids"), "folder_ids")
    destination_folder_id = parse_node_id(
        body.get("destination_folder_id"), "destination_folder_id"
    )
    copy = parse_bool(body.get("copy"), "copy")

    if not file_ids and not folder_ids:
        return json_error("No items to move.")

    result = await execute_write(
        batch_move, file_ids, folder_ids, destination_folder_id, copy=copy
    )
    await post_op_stats()

    await broadcast(
        {
            "type": "batch_moved",
            "movedFiles": result["moved_files"],
            "movedFolders": result["moved_folders"],
            "copied": copy,
        }
    )

    return json_ok(result)


async def batch_tag_route(request: Request):
    """POST /api/batch/tag — add/remove tags on multiple files."""
    body = await read_body(request)
    file_ids = parse_int_array(body.get("file_ids"), "file_ids")
    add_tags = parse_tags(parse_tag_input(body.get("add_tags"), "add_tags"))
    remove_tags = parse_tags(parse_tag_input(body.get("remove_tags"), "remove_tags"))

    if not file_ids:
        return json_error("No files specified.")
    if not add_tags and not remove_tags:
        return json_error("No tags to add or remove.")

    op_id = await enqueue("batch_tag", None, {
        "file_ids": file_ids,
        "add_tags": add_tags,
        "remove_tags": remove_tags,
    })
    return json_ok({"started": True, "op_id": op_id, "total": len(file_ids)})


async def batch_download_route(request: Request):
    """POST /api/batch/download — start async ZIP build for selected items."""
    body = await read_body(request)
    file_ids = parse_int_array(body.get("file_ids"), "file_ids")
    folder_ids = parse_int_array(body.get("folder_ids"), "folder_ids")

    if not file_ids and not folder_ids:
        return json_error("No items to download.")

    async with read_db() as db:
        files = await batch_collect_files(db, file_ids, folder_ids)

    if not files:
        return json_error("No files to download.")

    job_id = await start_build(files, "file-hunter-selection.zip")
    return json_ok({"jobId": job_id, "total": len(files)})
