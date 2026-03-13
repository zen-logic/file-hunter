import asyncio
import logging

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db, db_writer, execute_write
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
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def files_list(request: Request):
    db = await get_db()
    folder_id = request.query_params.get("folder_id", "")
    if not folder_id:
        return json_error("folder_id query parameter is required.")
    page = int(request.query_params.get("page", 0))
    sort = request.query_params.get("sort", "name")
    sort_dir = request.query_params.get("sortDir", "asc")
    filter_text = request.query_params.get("filter", "") or None
    focus_file = request.query_params.get("focusFile")
    focus_file_id = int(focus_file) if focus_file else None
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
    from file_hunter.services.dup_counts import batch_dup_counts

    # Auto-detect: SHA-256 = 64 hex chars, xxHash64 = 16 hex chars
    strong = [h for h in hashes if h and len(h) == 64]
    fast = [h for h in hashes if h and len(h) == 16]
    db = await get_db()
    counts = await batch_dup_counts(db, strong_hashes=strong, fast_hashes=fast)
    return json_ok({"counts": counts})


async def file_detail(request: Request):
    db = await get_db()
    file_id = int(request.path_params["id"])
    detail = await get_file_detail(db, file_id)
    if not detail:
        return json_error("File not found.", 404)
    return json_ok(detail)


async def file_content(request: Request):
    db = await get_db()
    file_id = int(request.path_params["id"])
    row = await db.execute(
        "SELECT full_path, filename, location_id FROM files WHERE id = ?", (file_id,)
    )
    row = await row.fetchone()
    if not row:
        return json_error("File not found.", 404)

    full_path = row["full_path"]
    filename = row["filename"]

    from file_hunter.extensions import get_content_proxy

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

    db = await get_db()
    file_id = int(request.path_params["id"])
    row = await db.execute(
        "SELECT full_path, location_id, file_size FROM files WHERE id = ?", (file_id,)
    )
    row = await row.fetchone()
    if not row:
        return json_error("File not found.", 404)

    full_path = row["full_path"]
    location_id = row["location_id"]
    file_size = row["file_size"] or 0

    offset = int(request.query_params.get("offset", 0))
    limit = min(int(request.query_params.get("limit", 4096)), 65536)

    from file_hunter.services.content_proxy import fetch_agent_byte_range

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

    db = await get_db()
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
        await broadcast(
            {
                "type": "file_deleted",
                "fileId": file_id,
                "filename": result["filename"],
                "deletedCount": result["deleted_count"],
                "deletedFromDiskCount": result["deleted_from_disk_count"],
                "allDuplicates": True,
            }
        )
    else:
        result = await execute_write(delete_file, file_id)
        if result is None:
            return json_error("File not found.", 404)
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
    """POST /api/files/{id:int}/verify — compute SHA-256 for duplicate verification."""
    file_id = int(request.path_params["id"])
    db = await get_db()

    row = await db.execute_fetchall(
        """SELECT f.id, f.filename, f.full_path, f.location_id,
                  f.hash_fast, f.hash_strong, l.root_path
           FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row:
        return json_error("File not found.", 404)

    f = row[0]
    if f["hash_strong"]:
        # Already verified — just return detail
        detail = await get_file_detail(db, file_id)
        return json_ok(detail)

    from file_hunter.services import fs

    online = await fs.dir_exists(f["root_path"], f["location_id"])
    if not online:
        return json_error("Location is offline.", 400)

    exists = await fs.file_exists(f["full_path"], f["location_id"])
    if not exists:
        return json_error("File not found on disk.", 400)

    try:
        hash_fast, hash_strong = await fs.file_hash(
            f["full_path"], f["location_id"], strong=True
        )
    except Exception as exc:
        logger.warning("Verify failed for %s: %r", f["full_path"], exc)
        return json_error(f"Hash computation failed: {exc}", 500)

    async with db_writer() as wdb:
        await wdb.execute(
            "UPDATE files SET hash_fast = ?, hash_strong = ? WHERE id = ?",
            (hash_fast, hash_strong, file_id),
        )

    # Recalc dup counts: new hash_strong group + old hash_fast group
    from file_hunter.services.dup_counts import submit_hashes_for_recalc

    old_hash_fast = f["hash_fast"]
    submit_hashes_for_recalc(
        strong_hashes={hash_strong},
        fast_hashes={old_hash_fast} if old_hash_fast else None,
        source=f"verify {f['filename']}",
    )

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    await broadcast(
        {
            "type": "file_verified",
            "fileId": file_id,
            "filename": f["filename"],
            "hashStrong": hash_strong,
        }
    )

    detail = await get_file_detail(db, file_id)
    return json_ok(detail)


async def folder_download(request: Request):
    """GET /api/folders/{id:int}/download — download folder as ZIP."""
    from file_hunter.services.batch import build_streaming_zip

    db = await get_db()
    folder_id = int(request.path_params["id"])

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
    from file_hunter.services.batch import build_streaming_zip

    db = await get_db()
    loc_id = int(request.path_params["id"])

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

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

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


async def folder_dup_exclude(request: Request):
    """POST /api/folders/{id:int}/dup-exclude — toggle duplicate exclusion."""
    folder_id = int(request.path_params["id"])
    body = await request.json()
    exclude = bool(body.get("exclude", False))

    from file_hunter.db import db_writer

    db = await get_db()
    flag = 1 if exclude else 0

    # Update the folder flag immediately so the detail panel shows the
    # correct checkbox state right away.
    row = await db.execute_fetchall(
        "SELECT name FROM folders WHERE id = ?", (folder_id,)
    )
    if not row:
        return json_error("Folder not found.", 404)
    folder_name = row[0]["name"]

    async with db_writer() as wdb:
        await wdb.execute(
            "UPDATE folders SET dup_exclude = ? WHERE id = ?", (flag, folder_id)
        )

    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    # Broadcast immediately so the activity panel shows feedback
    direction = "exclude" if exclude else "include"
    await broadcast(
        {
            "type": "dup_exclude_started",
            "folder": folder_name,
            "direction": direction,
        }
    )

    # Persist pending operation so it survives restarts
    from file_hunter.services.settings import set_setting

    async with db_writer() as wdb:
        await set_setting(
            wdb, "dup_exclude_pending", f"{folder_id}:{1 if exclude else 0}"
        )

    # Heavy work (descendant folders, files, dup_count recalc) in background
    from file_hunter.services.dup_exclude import toggle_dup_exclude

    asyncio.create_task(toggle_dup_exclude(folder_id, exclude))
    return json_ok({"queued": True})


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
