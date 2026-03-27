import asyncio

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db
from file_hunter.helpers import get_effective_hashes
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
        # Verify file exists
        rows = await db.execute_fetchall(
            "SELECT id, filename FROM files WHERE id = ?",
            (file_id,),
        )
        if not rows:
            return json_error("File not found.", 404)

        # Check hash from hashes.db
        from file_hunter.hashes_db import get_file_hashes

        h_map = await get_file_hashes([file_id])
        h = h_map.get(file_id, {})
        effective_hash = h.get("hash_strong") or h.get("hash_fast")
        if not effective_hash:
            return json_error("File has no hash — scan it first.", 400)

        # Guard concurrent consolidation
        if is_consolidation_running(effective_hash):
            return json_error("Consolidation already in progress for this file.", 409)

        # For copy_to, verify destination is online
        if mode == "copy_to":
            from file_hunter.services.consolidate import _resolve_folder_path_with_loc

            dest_path, dest_loc_id = await _resolve_folder_path_with_loc(
                db, dest_folder_id
            )
            if dest_path is None:
                return json_error("Destination folder not found.", 404)

            if not await fs.dir_exists(dest_path, dest_loc_id):
                return json_error("Destination is offline.", 400)

    filename_match_only = body.get("filename_match_only", False)
    asyncio.create_task(
        run_consolidation(
            file_id, mode, dest_folder_id, filename_match_only=filename_match_only
        )
    )

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
            dest_path, dest_loc_id = await _resolve_folder_path_with_loc(
                db, dest_folder_id
            )
        if dest_path is None:
            return json_error("Destination folder not found.", 404)

        if not await fs.dir_exists(dest_path, dest_loc_id):
            return json_error("Destination is offline.", 400)

    filename_match_only = body.get("filename_match_only", False)
    asyncio.create_task(
        run_batch_consolidation(
            file_ids, mode, dest_folder_id, filename_match_only=filename_match_only
        )
    )

    return json_ok(
        {"message": f"Batch consolidation started for {len(file_ids)} files"}
    )


async def consolidate_preview(request: Request):
    """POST /api/consolidate/preview — return dup counts for a set of files.

    Returns total_dups (all matching hashes) and filename_matched_dups
    (only dups with the same filename as the source file).
    """
    body = await request.json()
    file_ids = body.get("file_ids", [])
    if not file_ids:
        return json_ok({"total_dups": 0, "filename_matched_dups": 0})

    from file_hunter.hashes_db import open_hashes_connection

    # Get filenames and effective hashes for all selected files
    async with read_db() as db:
        ph = ",".join("?" for _ in file_ids)
        file_rows = await db.execute_fetchall(
            f"SELECT id, filename FROM files WHERE id IN ({ph})",
            file_ids,
        )
    filename_by_id = {r["id"]: r["filename"] for r in file_rows}
    hash_map = await get_effective_hashes(file_ids)

    total_dups = 0
    filename_matched_dups = 0

    hconn = await open_hashes_connection()
    try:
        for fid in file_ids:
            eff_hash, hash_col = hash_map.get(fid, (None, None))
            if not eff_hash or not hash_col:
                continue

            # Get all file_ids in this dup group from hashes.db
            dup_rows = await hconn.execute_fetchall(
                f"SELECT file_id FROM active_hashes WHERE {hash_col} = ?",
                (eff_hash,),
            )
            dup_ids = [r["file_id"] for r in dup_rows if r["file_id"] != fid]
            total_dups += len(dup_ids)

            # Count filename matches from catalog
            if dup_ids:
                fn = filename_by_id.get(fid)
                if fn:
                    async with read_db() as db:
                        dph = ",".join("?" for _ in dup_ids)
                        matched = await db.execute_fetchall(
                            f"SELECT COUNT(*) as c FROM files WHERE id IN ({dph}) AND filename = ?",
                            dup_ids + [fn],
                        )
                    filename_matched_dups += matched[0]["c"] if matched else 0
    finally:
        await hconn.close()

    return json_ok(
        {
            "total_dups": total_dups,
            "filename_matched_dups": filename_matched_dups,
        }
    )
