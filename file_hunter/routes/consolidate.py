import asyncio

from starlette.requests import Request

from file_hunter.core import BadRequest, NotFound, json_error, json_ok, parse_bool, parse_int, parse_int_array, parse_node_id, parse_str, read_body
from file_hunter.db import file_row, read_db, id_batches
from file_hunter.hashes_db import get_file_hashes, open_hashes_connection
from file_hunter.helpers import get_effective_hashes
from file_hunter.services import fs
from file_hunter.services.consolidate import (
    resolve_folder_path_with_loc,
    run_copy,
    run_consolidation,
    is_consolidation_running,
)
from file_hunter.services.queue_manager import enqueue


def check_mode(is_copy, mode, dest_folder_id):
    """BadRequest unless the mode and destination fit a copy or a move."""
    if is_copy:
        if not dest_folder_id:
            raise BadRequest("destination_folder_id is required for copy.")
    else:
        if mode not in ("keep_here", "move_to"):
            raise BadRequest("mode must be 'keep_here' or 'move_to'.")
        if mode == "move_to" and not dest_folder_id:
            raise BadRequest("destination_folder_id is required for move_to mode.")


async def check_destination(dest_folder_id):
    """NotFound if the destination folder doesn't exist, BadRequest if it's
    offline."""
    async with read_db() as db:
        dest_path, dest_loc_id = await resolve_folder_path_with_loc(
            db, dest_folder_id
        )
    if dest_path is None:
        raise NotFound("Destination folder not found.")
    if not await fs.dir_exists(dest_path, dest_loc_id):
        raise BadRequest("Destination is offline.")


async def consolidate(request: Request):
    """POST /api/consolidate — start a copy or move consolidation background task."""
    body = await read_body(request)

    file_id = parse_int(body.get("file_id"), "file_id", None)
    mode = parse_str(body.get("mode"), "mode", None)
    consolidate_mode = parse_str(body.get("consolidateMode"), "consolidateMode", "move")
    dest_folder_id = parse_node_id(
        body.get("destination_folder_id"), "destination_folder_id", None
    )

    if not file_id or not mode:
        return json_error("file_id and mode are required.", 400)

    is_copy = consolidate_mode == "copy"
    check_mode(is_copy, mode, dest_folder_id)

    file = await file_row(file_id, "filename")

    h_map = await get_file_hashes([file_id])
    h = h_map.get(file_id, {})
    effective_hash = h.get("hash_strong") or h.get("hash_fast")
    if not effective_hash:
        return json_error("File has no hash — scan it first.", 400)

    if is_consolidation_running(effective_hash):
        return json_error("Operation already in progress for this file.", 409)

    # Verify destination is online (both copy and move_to need it)
    if dest_folder_id:
        await check_destination(dest_folder_id)

    filename_match_only = parse_bool(body.get("filename_match_only"), "filename_match_only")
    stub_file_ids = parse_int_array(body.get("stub_file_ids"), "stub_file_ids")

    if is_copy:
        asyncio.create_task(
            run_copy(
                file_id, dest_folder_id, filename_match_only=filename_match_only
            )
        )
    else:
        asyncio.create_task(
            run_consolidation(
                file_id, mode, dest_folder_id,
                filename_match_only=filename_match_only,
                stub_file_ids=stub_file_ids,
            )
        )

    return json_ok({"message": f"Consolidation started for '{file['filename']}'"})


async def batch_consolidate(request: Request):
    """POST /api/batch/consolidate — batch copy or move consolidation in background."""
    body = await read_body(request)

    file_ids = parse_int_array(body.get("file_ids"), "file_ids")
    mode = parse_str(body.get("mode"), "mode", None)
    consolidate_mode = parse_str(body.get("consolidateMode"), "consolidateMode", "move")
    dest_folder_id = parse_node_id(
        body.get("destination_folder_id"), "destination_folder_id", None
    )

    if not file_ids:
        return json_error("No files specified.", 400)

    is_copy = consolidate_mode == "copy"
    check_mode(is_copy, mode, dest_folder_id)

    # Validate destination once upfront
    if dest_folder_id:
        await check_destination(dest_folder_id)

    filename_match_only = parse_bool(body.get("filename_match_only"), "filename_match_only")
    stub_file_ids = parse_int_array(body.get("stub_file_ids"), "stub_file_ids")
    await enqueue("batch_consolidate", None, {
        "file_ids": file_ids,
        "mode": mode,
        "consolidate_mode": consolidate_mode,
        "dest_folder_id": dest_folder_id,
        "filename_match_only": filename_match_only,
        "stub_file_ids": stub_file_ids,
    })

    return json_ok(
        {"message": f"Batch consolidation started for {len(file_ids)} files"}
    )


async def consolidate_preview(request: Request):
    """POST /api/consolidate/preview — return dup counts and locations for a set of files.

    Returns total_dups, filename_matched_dups, and a duplicates list with
    location/agent/path for all copies (including source files, since the
    merge step needs to show them for move operations).
    """
    body = await read_body(request)
    file_ids = parse_int_array(body.get("file_ids"), "file_ids")
    if not file_ids:
        return json_ok({"total_dups": 0, "filename_matched_dups": 0, "duplicates": []})

    # Get filenames and effective hashes for all selected files
    filename_by_id = {}
    async with read_db() as db:
        for batch, ph in id_batches(file_ids):
            rows = await db.execute_fetchall(
                f"SELECT id, filename FROM files WHERE id IN ({ph})",
                batch,
            )
            for r in rows:
                filename_by_id[r["id"]] = r["filename"]
    hash_map = await get_effective_hashes(file_ids)

    total_dups = 0
    filename_matched_dups = 0
    all_dup_ids = set()

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
            dup_ids = [r["file_id"] for r in dup_rows]
            total_dups += len(dup_ids)
            all_dup_ids.update(dup_ids)

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

    # Fetch location/agent/path detail for all duplicate file IDs
    duplicates = []
    if all_dup_ids:
        dup_id_list = list(all_dup_ids)
        async with read_db() as db:
            for batch, dph in id_batches(dup_id_list):
                rows = await db.execute_fetchall(
                    f"SELECT f.id, f.filename, f.rel_path, f.location_id, "
                    f"l.name as location_name, a.name as agent_name "
                    f"FROM files f "
                    f"JOIN locations l ON l.id = f.location_id "
                    f"LEFT JOIN agents a ON a.id = l.agent_id "
                    f"WHERE f.id IN ({dph})",
                    batch,
                )
                for d in rows:
                    duplicates.append(
                        {
                            "fileId": d["id"],
                            "name": d["filename"],
                            "location": d["location_name"],
                            "agent": d["agent_name"] or "",
                            "locationId": d["location_id"],
                            "path": f"/{d['rel_path']}",
                        }
                    )

    return json_ok(
        {
            "total_dups": total_dups,
            "filename_matched_dups": filename_matched_dups,
            "duplicates": duplicates,
        }
    )
