import asyncio
import logging

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db, db_writer, execute_write
from file_hunter.helpers import post_op_stats
from file_hunter.services.files import (
    list_files,
    get_file_detail,
    update_file,
    move_file,
)
from file_hunter.services.delete import (
    delete_file,
    delete_file_and_duplicates,
    delete_folder,
)
from file_hunter.extensions import get_content_proxy
from file_hunter.hashes_db import (
    get_file_hashes,
    hashes_writer,
    read_hashes,
    update_file_hash,
)
from file_hunter.services import fs
from file_hunter.services.agent_ops import hash_partial_batch
from file_hunter.services.batch import build_streaming_zip
from file_hunter.services.content_proxy import fetch_agent_byte_range
from file_hunter.services.deferred_ops import cancel_pending_op, queue_deferred_op
from file_hunter.services.dup_counts import batch_dup_counts
from file_hunter.services.dup_exclude import (
    get_progress,
    is_running,
    toggle_dup_exclude,
)
from file_hunter.services.settings import set_setting
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def files_list(request: Request):
    folder_id = request.query_params.get("folder_id", "")
    if not folder_id:
        return json_error("folder_id query parameter is required.")
    page = int(request.query_params.get("page", 0))
    sort = request.query_params.get("sort", "name")
    sort_dir = request.query_params.get("sortDir", "asc")
    filter_text = request.query_params.get("filter", "") or None
    focus_file = request.query_params.get("focusFile")
    focus_file_id = int(focus_file) if focus_file else None
    async with read_db() as db:
        data = await list_files(
            db,
            folder_id,
            page=page,
            sort=sort,
            sort_dir=sort_dir,
            filter_text=filter_text,
            focus_file_id=focus_file_id,
        )
    return json_ok(data)


async def file_dup_counts(request: Request):
    """POST /api/files/dup-counts — live dup counts for a list of hashes."""
    body = await request.json()
    hashes = body.get("hashes")
    if not hashes or not isinstance(hashes, list):
        return json_error("hashes list is required.")

    # Auto-detect: SHA-256 = 64 hex chars, xxHash64 = 16 hex chars
    strong = [h for h in hashes if h and len(h) == 64]
    fast = [h for h in hashes if h and len(h) == 16]
    counts = await batch_dup_counts(strong_hashes=strong, fast_hashes=fast)
    return json_ok({"counts": counts})


async def file_detail(request: Request):
    file_id = int(request.path_params["id"])
    async with read_db() as db:
        detail = await get_file_detail(db, file_id)
    if not detail:
        return json_error("File not found.", 404)
    return json_ok(detail)


async def file_content(request: Request):
    file_id = int(request.path_params["id"])
    async with read_db() as db:
        row = await db.execute(
            "SELECT full_path, filename, location_id FROM files WHERE id = ?",
            (file_id,),
        )
        row = await row.fetchone()
    if not row:
        return json_error("File not found.", 404)

    full_path = row["full_path"]
    filename = row["filename"]

    content_proxy = get_content_proxy()
    if content_proxy:
        response = await content_proxy(
            file_id, full_path, filename, request_headers=dict(request.headers)
        )
        if response is not None:
            return response
    return json_error("File not available (agent offline).", 404)


async def file_bytes(request: Request):
    """Return a slice of raw bytes from a file. For hex viewer paging."""
    from starlette.responses import Response

    file_id = int(request.path_params["id"])
    async with read_db() as db:
        row = await db.execute(
            "SELECT full_path, location_id, file_size FROM files WHERE id = ?",
            (file_id,),
        )
        row = await row.fetchone()
    if not row:
        return json_error("File not found.", 404)

    full_path = row["full_path"]
    location_id = row["location_id"]
    file_size = row["file_size"] or 0

    offset = int(request.query_params.get("offset", 0))
    limit = min(int(request.query_params.get("limit", 4096)), 65536)

    data = await fetch_agent_byte_range(full_path, location_id, offset, limit)
    if data is None:
        return json_error("File not available (agent offline).", 404)

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-File-Size": str(file_size),
            "X-Offset": str(offset),
        },
    )


async def file_update(request: Request):
    file_id = int(request.path_params["id"])
    body = await request.json()

    description = body.get("description")
    tags = body.get("tags")

    await execute_write(update_file, file_id, description=description, tags=tags)

    async with read_db() as db:
        detail = await get_file_detail(db, file_id)
    if not detail:
        return json_error("File not found.", 404)
    return json_ok(detail)


async def file_delete(request: Request):
    """DELETE /api/files/{id:int} — delete a file from disk and catalog."""
    file_id = int(request.path_params["id"])
    all_duplicates = request.query_params.get("all_duplicates", "").lower() in (
        "true",
        "1",
        "yes",
    )

    if all_duplicates:
        result = await execute_write(delete_file_and_duplicates, file_id)
        if result is None:
            return json_error("File not found.", 404)
        deferred = result.get("deferred_count", 0)
        if deferred and result["deleted_count"] == 0:
            # All copies were on offline locations — all deferred
            await broadcast(
                {
                    "type": "deferred_op_created",
                    "fileId": file_id,
                    "opType": "delete",
                    "filename": result["filename"],
                }
            )
        else:
            await broadcast(
                {
                    "type": "file_deleted",
                    "fileId": file_id,
                    "filename": result["filename"],
                    "deletedCount": result["deleted_count"],
                    "deletedFromDiskCount": result["deleted_from_disk_count"],
                    "allDuplicates": True,
                    "deferredCount": deferred,
                }
            )
    else:
        result = await execute_write(delete_file, file_id)
        if result is None:
            return json_error("File not found.", 404)
        if result.get("deferred"):
            await broadcast(
                {
                    "type": "deferred_op_created",
                    "fileId": file_id,
                    "opType": "delete",
                    "filename": result["filename"],
                }
            )
        else:
            await broadcast(
                {
                    "type": "file_deleted",
                    "fileId": file_id,
                    "filename": result["filename"],
                    "deletedFromDisk": result["deleted_from_disk"],
                }
            )
    return json_ok(result)


async def file_verify(request: Request):
    """POST /api/files/{id:int}/verify — compute SHA-256 for the entire dup group."""
    file_id = int(request.path_params["id"])

    async with read_db() as db:
        row = await db.execute_fetchall(
            """SELECT f.id, f.filename, f.full_path, f.location_id, l.root_path
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (file_id,),
        )
        if not row:
            return json_error("File not found.", 404)

        f = dict(row[0])

        h_map = await get_file_hashes([file_id])
        h = h_map.get(file_id, {})
        hash_fast = h.get("hash_fast")

        if h.get("hash_strong"):
            # Already verified — return detail
            detail = await get_file_detail(db, file_id)
            return json_ok(detail)

        if not hash_fast:
            return json_error("File has no hash — scan or re-hash it first.", 400)

    # Find all files in the same hash_fast group
    async with read_hashes() as hdb:
        group_rows = await hdb.execute_fetchall(
            "SELECT file_id FROM active_hashes WHERE hash_fast = ?",
            (hash_fast,),
        )
    group_file_ids = [r["file_id"] for r in group_rows]

    # Launch background task to verify the whole group
    asyncio.create_task(
        _run_group_verify(file_id, f["filename"], hash_fast, group_file_ids)
    )

    return json_ok(
        {"verifying": True, "filename": f["filename"], "groupSize": len(group_file_ids)}
    )


async def _run_group_verify(
    trigger_file_id: int, trigger_filename: str, hash_fast: str, file_ids: list[int]
):
    """Background: compute SHA-256 for all files in a hash_fast group."""
    verified = 0
    skipped = 0
    deferred = 0
    strong_hashes: set[str] = set()

    async with read_db() as db:
        ph = ",".join("?" for _ in file_ids)
        all_rows = await db.execute_fetchall(
            f"""SELECT f.id, f.filename, f.full_path, f.location_id, l.root_path
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id IN ({ph})""",
            file_ids,
        )

    # Check which already have hash_strong
    h_map = await get_file_hashes(file_ids)

    for f in all_rows:
        fid = f["id"]
        h = h_map.get(fid, {})
        if h.get("hash_strong"):
            strong_hashes.add(h["hash_strong"])
            verified += 1
            continue

        try:
            online = await fs.dir_exists(f["root_path"], f["location_id"])
        except Exception:
            online = False

        if not online:
            try:
                await execute_write(queue_deferred_op, fid, f["location_id"], "verify")
            except Exception:
                pass
            deferred += 1
            continue

        try:
            exists = await fs.file_exists(f["full_path"], f["location_id"])
            if not exists:
                skipped += 1
                continue

            new_fast, new_strong = await fs.file_hash(
                f["full_path"], f["location_id"], strong=True
            )
            await update_file_hash(fid, hash_fast=new_fast, hash_strong=new_strong)
            strong_hashes.add(new_strong)
            verified += 1
        except Exception as exc:
            logger.warning("Verify failed for file %d: %s", fid, exc)
            skipped += 1

        await broadcast(
            {
                "type": "verify_progress",
                "triggerFileId": trigger_file_id,
                "verified": verified,
                "total": len(all_rows),
                "filename": f["filename"],
            }
        )

    await post_op_stats(
        strong_hashes=strong_hashes or None,
        fast_hashes={hash_fast},
        source=f"verify group ({trigger_filename})",
    )

    await broadcast(
        {
            "type": "verify_completed",
            "triggerFileId": trigger_file_id,
            "filename": trigger_filename,
            "verified": verified,
            "deferred": deferred,
            "skipped": skipped,
            "total": len(all_rows),
        }
    )


async def file_rehash(request: Request):
    """POST /api/files/{id:int}/rehash — compute hash_partial + hash_fast."""
    file_id = int(request.path_params["id"])

    async with read_db() as db:
        row = await db.execute_fetchall(
            """SELECT f.id, f.filename, f.full_path, f.file_size,
                      f.location_id, l.root_path
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (file_id,),
        )
        if not row:
            return json_error("File not found.", 404)
        f = dict(row[0])

        loc_row = await db.execute_fetchall(
            "SELECT agent_id FROM locations WHERE id = ?", (f["location_id"],)
        )
    agent_id = loc_row[0]["agent_id"] if loc_row else None

    # Read old hashes before overwriting
    old_h = (await get_file_hashes([file_id])).get(file_id, {})
    old_fast = old_h.get("hash_fast")
    old_strong = old_h.get("hash_strong")

    online = await fs.dir_exists(f["root_path"], f["location_id"])
    if not online:
        return json_error("Location is offline.", 400)

    exists = await fs.file_exists(f["full_path"], f["location_id"])
    if not exists:
        return json_error("File not found on disk.", 400)

    # Compute hash_fast
    try:
        (hash_fast,) = await fs.file_hash(f["full_path"], f["location_id"])
    except Exception as exc:
        return json_error(f"Hash computation failed: {exc}", 500)

    # Compute hash_partial
    hash_partial = None
    if agent_id is not None:
        try:
            hp_result = await hash_partial_batch(agent_id, [f["full_path"]])
            for hr in hp_result.get("results", []):
                if hr.get("path") == f["full_path"]:
                    hash_partial = hr.get("hash_partial")
                    break
        except Exception:
            pass

    # Ensure entry exists in hashes.db, then update
    async with hashes_writer() as hdb:
        await hdb.execute(
            "INSERT INTO file_hashes (file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
            "VALUES (?, ?, ?, ?, ?, NULL) "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "hash_partial=COALESCE(excluded.hash_partial, file_hashes.hash_partial), "
            "hash_fast=excluded.hash_fast, "
            "hash_strong=NULL",
            (file_id, f["location_id"], f["file_size"], hash_partial, hash_fast),
        )

    recalc_fast = {hash_fast}
    if old_fast and old_fast != hash_fast:
        recalc_fast.add(old_fast)
    recalc_strong = {old_strong} if old_strong else None
    await post_op_stats(
        strong_hashes=recalc_strong,
        fast_hashes=recalc_fast,
        source=f"rehash {f['filename']}",
    )

    async with read_db() as db:
        detail = await get_file_detail(db, file_id)
    return json_ok(detail)


async def batch_rehash(request: Request):
    """POST /api/files/rehash — rehash multiple files. Body: { fileIds: [int] }."""
    body = await request.json()
    file_ids = body.get("fileIds", [])
    if not file_ids:
        return json_error("fileIds is required.")

    asyncio.create_task(_run_batch_rehash(file_ids))
    return json_ok({"queued": len(file_ids)})


async def _run_batch_rehash(file_ids: list[int]):
    """Background task: rehash each file (partial + fast)."""
    affected_fast: set[str] = set()
    affected_strong: set[str] = set()
    total = len(file_ids)

    for i, file_id in enumerate(file_ids):
        try:
            async with read_db() as db:
                row = await db.execute_fetchall(
                    """SELECT f.id, f.filename, f.full_path, f.file_size,
                              f.location_id, l.root_path
                       FROM files f
                       JOIN locations l ON l.id = f.location_id
                       WHERE f.id = ?""",
                    (file_id,),
                )
                if not row:
                    continue
                f = dict(row[0])

                loc_row = await db.execute_fetchall(
                    "SELECT agent_id FROM locations WHERE id = ?",
                    (f["location_id"],),
                )
            agent_id = loc_row[0]["agent_id"] if loc_row else None

            if not await fs.file_exists(f["full_path"], f["location_id"]):
                continue

            # Read old hash before overwriting
            old_h = (await get_file_hashes([file_id])).get(file_id, {})
            old_fast = old_h.get("hash_fast")
            old_strong = old_h.get("hash_strong")

            (hash_fast,) = await fs.file_hash(f["full_path"], f["location_id"])

            hash_partial = None
            if agent_id is not None:
                try:
                    hp_result = await hash_partial_batch(agent_id, [f["full_path"]])
                    for hr in hp_result.get("results", []):
                        if hr.get("path") == f["full_path"]:
                            hash_partial = hr.get("hash_partial")
                            break
                except Exception:
                    pass

            async with hashes_writer() as hdb:
                await hdb.execute(
                    "INSERT INTO file_hashes "
                    "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                    "VALUES (?, ?, ?, ?, ?, NULL) "
                    "ON CONFLICT(file_id) DO UPDATE SET "
                    "hash_partial=COALESCE(excluded.hash_partial, file_hashes.hash_partial), "
                    "hash_fast=excluded.hash_fast, "
                    "hash_strong=NULL",
                    (
                        file_id,
                        f["location_id"],
                        f["file_size"],
                        hash_partial,
                        hash_fast,
                    ),
                )

            affected_fast.add(hash_fast)
            if old_fast and old_fast != hash_fast:
                affected_fast.add(old_fast)
            if old_strong:
                affected_strong.add(old_strong)

            await broadcast(
                {
                    "type": "rehash_progress",
                    "processed": i + 1,
                    "total": total,
                    "filename": f["filename"],
                }
            )
        except Exception as exc:
            logger.warning("Rehash failed for file %d: %s", file_id, exc)

    await post_op_stats(
        strong_hashes=affected_strong or None,
        fast_hashes=affected_fast or None,
        source=f"batch rehash ({total} files)",
    )

    await broadcast({"type": "rehash_completed", "total": total})


async def folder_download(request: Request):
    """GET /api/folders/{id:int}/download — download folder as ZIP."""
    folder_id = int(request.path_params["id"])

    async with read_db() as db:
        row = await db.execute_fetchall(
            """SELECT f.name, f.rel_path, f.location_id
               FROM folders f WHERE f.id = ?""",
            (folder_id,),
        )
        if not row:
            return json_error("Folder not found.", 404)
        folder_name = row[0]["name"]
        folder_rel = row[0]["rel_path"]
        location_id = row[0]["location_id"]

        desc_rows = await db.execute_fetchall(
            """WITH RECURSIVE desc(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
               )
               SELECT id FROM desc""",
            (folder_id,),
        )
        desc_ids = [r["id"] for r in desc_rows]

        placeholders = ",".join("?" * len(desc_ids))
        files = await db.execute_fetchall(
            f"SELECT full_path, rel_path FROM files WHERE folder_id IN ({placeholders})",
            desc_ids,
        )

    prefix = folder_rel + "/" if folder_rel else ""
    zip_files = []
    for f in files:
        arc_name = f["rel_path"]
        if prefix and arc_name.startswith(prefix):
            arc_name = arc_name[len(prefix) :]
        zip_files.append((f["full_path"], arc_name, location_id))

    return await build_streaming_zip(zip_files, f"{folder_name}.zip")


async def location_download(request: Request):
    """GET /api/locations/{id:int}/download — download entire location as ZIP."""
    loc_id = int(request.path_params["id"])

    async with read_db() as db:
        row = await db.execute_fetchall(
            "SELECT name FROM locations WHERE id = ?", (loc_id,)
        )
        if not row:
            return json_error("Location not found.", 404)
        loc_name = row[0]["name"]

        files = await db.execute_fetchall(
            "SELECT full_path, rel_path FROM files WHERE location_id = ?", (loc_id,)
        )

    zip_files = [(f["full_path"], f["rel_path"], loc_id) for f in files]
    return await build_streaming_zip(zip_files, f"{loc_name}.zip")


async def file_move(request: Request):
    """POST /api/files/{id:int}/move — rename or move a file."""
    file_id = int(request.path_params["id"])
    body = await request.json()
    new_name = body.get("name")
    destination_folder_id = body.get("destination_folder_id")

    if not new_name and not destination_folder_id:
        return json_error("Provide name and/or destination_folder_id.")

    if new_name is not None:
        new_name = new_name.strip()
        if not new_name:
            return json_error("Name cannot be empty.")
        if "/" in new_name or "\\" in new_name:
            return json_error("Name cannot contain path separators.")

    try:
        result = await execute_write(
            move_file,
            file_id,
            new_name=new_name,
            destination_folder_id=destination_folder_id,
        )
    except ValueError as e:
        return json_error(str(e), 400)

    await post_op_stats()

    if result.get("deferred"):
        await broadcast(
            {
                "type": "deferred_op_created",
                "fileId": file_id,
                "opType": "move",
                "filename": result.get("old_name"),
            }
        )
    else:
        await broadcast(
            {
                "type": "file_moved",
                "fileId": file_id,
                "oldName": result.get("old_name"),
                "newName": result.get("new_name"),
                "renamed": result.get("renamed", False),
                "moved": result.get("moved", False),
            }
        )

    return json_ok(result)


async def file_cancel_pending(request: Request):
    """POST /api/files/{id:int}/cancel-pending — cancel a deferred operation."""
    file_id = int(request.path_params["id"])

    async with read_db() as db:
        row = await db.execute_fetchall(
            "SELECT id, pending_op, filename FROM files WHERE id = ?", (file_id,)
        )
    if not row:
        return json_error("File not found.", 404)
    if not row[0]["pending_op"]:
        return json_error("No pending operation on this file.", 400)

    await cancel_pending_op(file_id)

    return json_ok({"cancelled": True, "filename": row[0]["filename"]})


async def folder_dup_exclude(request: Request):
    """POST /api/folders/{id:int}/dup-exclude — toggle duplicate exclusion.

    Two-phase: first call without confirmed=true returns file/folder counts
    for a confirmation dialog. Second call with confirmed=true starts the
    operation.
    """
    folder_id = int(request.path_params["id"])
    body = await request.json()
    exclude = bool(body.get("exclude", False))
    confirmed = bool(body.get("confirmed", False))

    if is_running():
        return json_error("A duplicate exclusion operation is already running.")

    async with read_db() as db:
        row = await db.execute_fetchall(
            "SELECT name FROM folders WHERE id = ?", (folder_id,)
        )
        if not row:
            return json_error("Folder not found.", 404)
        folder_name = row[0]["name"]

        # Recursive CTE to count affected folders and files
        desc_rows = await db.execute_fetchall(
            """WITH RECURSIVE descendants(id) AS (
                   SELECT ?
                   UNION ALL
                   SELECT fo.id FROM folders fo JOIN descendants d ON fo.parent_id = d.id
               )
               SELECT id FROM descendants""",
            (folder_id,),
        )
        folder_count = len(desc_rows)

        # Get file count from stored folder counters (O(1), no aggregate query)
        folder_ids = [r["id"] for r in desc_rows]
        file_count = 0
        for fid in folder_ids:
            fc_row = await db.execute_fetchall(
                "SELECT file_count FROM folders WHERE id = ?", (fid,)
            )
            if fc_row and fc_row[0]["file_count"]:
                file_count += fc_row[0]["file_count"]

    if not confirmed:
        # Return counts for confirmation dialog
        return json_ok(
            {
                "confirm": True,
                "folderName": folder_name,
                "folderCount": folder_count,
                "fileCount": file_count,
                "direction": "exclude" if exclude else "include",
            }
        )

    # --- Confirmed: start the operation ---

    # Persist pending operation so it survives restarts
    async with db_writer() as wdb:
        await set_setting(
            wdb, "dup_exclude_pending", f"{folder_id}:{1 if exclude else 0}"
        )

    # Heavy work in background task
    asyncio.create_task(toggle_dup_exclude(folder_id, exclude))
    return json_ok({"started": True})


async def dup_exclude_progress(request: Request):
    """GET /api/dup-exclude/progress — poll dup_exclude operation progress."""
    return json_ok(get_progress())


async def folder_delete(request: Request):
    """DELETE /api/folders/{id:int} — delete a folder from disk and catalog."""
    folder_id = int(request.path_params["id"])
    result = await execute_write(delete_folder, folder_id)
    if result is None:
        return json_error("Folder not found.", 404)
    await broadcast(
        {
            "type": "folder_deleted",
            "folderId": folder_id,
            "name": result["name"],
            "fileCount": result["file_count"],
            "deletedFromDisk": result["deleted_from_disk"],
        }
    )
    return json_ok(result)
