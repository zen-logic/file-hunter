import asyncio

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db
from file_hunter.services import fs
from file_hunter.services.consolidate import (
    run_consolidation,
    run_batch_consolidation,
    is_consolidation_running,
)


async def consolidate(request: Request):
    """POST /api/consolidate — start a consolidation background task."""
    body = await request.json()

    file_id = body.get("file_id")
    mode = body.get("mode")
    dest_folder_id = body.get("destination_folder_id")

    if not file_id or not mode:
        return json_error("file_id and mode are required.", 400)

    if mode not in ("keep_here", "copy_to"):
        return json_error("mode must be 'keep_here' or 'copy_to'.", 400)

    if mode == "copy_to" and not dest_folder_id:
        return json_error("destination_folder_id is required for copy_to mode.", 400)

    async with read_db() as db:
        # Verify file exists and has a hash
        rows = await db.execute_fetchall(
            "SELECT id, filename, hash_strong, hash_fast FROM files WHERE id = ?",
            (file_id,),
        )
        if not rows:
            return json_error("File not found.", 404)

        effective_hash = rows[0]["hash_strong"] or rows[0]["hash_fast"]
        if not effective_hash:
            return json_error("File has no hash — scan it first.", 400)

        # Guard concurrent consolidation
        if is_consolidation_running(effective_hash):
            return json_error("Consolidation already in progress for this file.", 409)

        # For copy_to, verify destination is online
        if mode == "copy_to":
            from file_hunter.services.consolidate import _resolve_folder_path_with_loc

            dest_path, dest_loc_id = await _resolve_folder_path_with_loc(db, dest_folder_id)
            if dest_path is None:
                return json_error("Destination folder not found.", 404)

            if not await fs.dir_exists(dest_path, dest_loc_id):
                return json_error("Destination is offline.", 400)

    asyncio.create_task(run_consolidation(file_id, mode, dest_folder_id))

    return json_ok({"message": f"Consolidation started for '{rows[0]['filename']}'"})


async def batch_consolidate(request: Request):
    """POST /api/batch/consolidate — consolidate multiple files in background."""
    body = await request.json()

    file_ids = body.get("file_ids", [])
    mode = body.get("mode")
    dest_folder_id = body.get("destination_folder_id")

    if not file_ids:
        return json_error("No files specified.", 400)

    if not mode or mode not in ("keep_here", "copy_to"):
        return json_error("mode must be 'keep_here' or 'copy_to'.", 400)

    if mode == "copy_to" and not dest_folder_id:
        return json_error("destination_folder_id is required for copy_to mode.", 400)

    # Validate destination once upfront
    if mode == "copy_to":
        from file_hunter.services.consolidate import _resolve_folder_path_with_loc

        async with read_db() as db:
            dest_path, dest_loc_id = await _resolve_folder_path_with_loc(db, dest_folder_id)
        if dest_path is None:
            return json_error("Destination folder not found.", 404)

        if not await fs.dir_exists(dest_path, dest_loc_id):
            return json_error("Destination is offline.", 400)

    asyncio.create_task(run_batch_consolidation(file_ids, mode, dest_folder_id))

    return json_ok(
        {"message": f"Batch consolidation started for {len(file_ids)} files"}
    )
