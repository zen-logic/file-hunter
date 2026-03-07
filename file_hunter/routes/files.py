import asyncio
import io
import logging
import os
import zipfile

from starlette.requests import Request
from starlette.responses import FileResponse, StreamingResponse

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db, execute_write
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

MIME_MAP = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "webp": "image/webp",
    "svg": "image/svg+xml",
    "bmp": "image/bmp",
    "mp4": "video/mp4",
    "webm": "video/webm",
    "mov": "video/quicktime",
    "avi": "video/x-msvideo",
    "mkv": "video/x-matroska",
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "flac": "audio/flac",
    "ogg": "audio/ogg",
    "aac": "audio/aac",
    "m4a": "audio/mp4",
    "txt": "text/plain",
    "md": "text/plain",
    "csv": "text/plain",
    "json": "text/plain",
    "xml": "text/plain",
    "log": "text/plain",
    "py": "text/plain",
    "js": "text/plain",
    "html": "text/plain",
    "css": "text/plain",
    "pdf": "application/pdf",
    "moved": "text/plain",
    "sources": "text/plain",
}


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

    db = await get_db()
    counts = await batch_dup_counts(db, hashes)
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
    location_id = row["location_id"]

    # Agent locations — always proxy, never serve from local disk
    from file_hunter.extensions import is_agent_location, get_content_proxy

    if is_agent_location(location_id):
        logger.info(
            "Content request for agent file #%d (%s), location #%d",
            file_id,
            filename,
            location_id,
        )
        content_proxy = get_content_proxy()
        if content_proxy:
            response = await content_proxy(
                file_id, full_path, filename, request_headers=dict(request.headers)
            )
            if response is not None:
                return response
            logger.warning(
                "Content proxy returned None for file #%d (%s)", file_id, full_path
            )
        else:
            logger.warning("No content_proxy hook registered")
        return json_error("File not available (agent offline).", 404)

    if not await asyncio.to_thread(os.path.isfile, full_path):
        return json_error("File not available (offline or missing).", 404)

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    media_type = MIME_MAP.get(ext, "application/octet-stream")

    return FileResponse(full_path, media_type=media_type)


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

    from file_hunter.extensions import is_agent_location, get_fetch_bytes

    if is_agent_location(location_id):
        fetch_bytes = get_fetch_bytes()
        if not fetch_bytes:
            return json_error("File not available (agent offline).", 404)

        body = await fetch_bytes(full_path, location_id)
        if body is None:
            return json_error("File not available (agent offline).", 404)

        offset = int(request.query_params.get("offset", 0))
        limit = min(int(request.query_params.get("limit", 4096)), 65536)
        actual_size = len(body)
        data = body[offset : offset + limit]

        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-File-Size": str(actual_size),
                "X-Offset": str(offset),
            },
        )

    if not await asyncio.to_thread(os.path.isfile, full_path):
        return json_error("File not available (offline or missing).", 404)

    offset = int(request.query_params.get("offset", 0))
    limit = min(int(request.query_params.get("limit", 4096)), 65536)

    def _read_slice():
        with open(full_path, "rb") as f:
            f.seek(offset)
            return f.read(limit)

    data = await asyncio.to_thread(_read_slice)
    # Use actual file size from disk if DB value is stale
    actual_size = await asyncio.to_thread(os.path.getsize, full_path)

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-File-Size": str(actual_size),
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


def _read_file_bytes(path):
    """Read entire file contents. Called via asyncio.to_thread()."""
    with open(path, "rb") as f:
        return f.read()


async def _fetch_file_data(full_path, location_id):
    """Read file bytes — agent locations always proxy, local reads from disk."""
    from file_hunter.extensions import is_agent_location, get_fetch_bytes

    if is_agent_location(location_id):
        fetch_bytes = get_fetch_bytes()
        if fetch_bytes:
            return await fetch_bytes(full_path, location_id)
        return None
    if await asyncio.to_thread(os.path.isfile, full_path):
        return await asyncio.to_thread(_read_file_bytes, full_path)
    return None


async def folder_download(request: Request):
    """GET /api/folders/{id:int}/download — download folder as ZIP."""
    db = await get_db()
    folder_id = int(request.path_params["id"])

    # Get folder info and location root
    row = await db.execute_fetchall(
        """SELECT f.name, f.rel_path, f.location_id, l.root_path
           FROM folders f JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (folder_id,),
    )
    if not row:
        return json_error("Folder not found.", 404)
    folder_name = row[0]["name"]
    folder_rel = row[0]["rel_path"]
    location_id = row[0]["location_id"]

    # Recursive CTE to get all descendant folder IDs + the folder itself
    desc_rows = await db.execute_fetchall(
        """WITH RECURSIVE desc(id) AS (
               SELECT ? UNION ALL
               SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
           )
           SELECT id FROM desc""",
        (folder_id,),
    )
    desc_ids = [r["id"] for r in desc_rows]

    # Get all files in these folders
    placeholders = ",".join("?" * len(desc_ids))
    files = await db.execute_fetchall(
        f"SELECT full_path, rel_path FROM files WHERE folder_id IN ({placeholders})",
        desc_ids,
    )

    buf = io.BytesIO()
    zf = zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED)
    prefix = folder_rel + "/" if folder_rel else ""
    for f in files:
        arc_name = f["rel_path"]
        if prefix and arc_name.startswith(prefix):
            arc_name = arc_name[len(prefix) :]
        data = await _fetch_file_data(f["full_path"], location_id)
        if data:
            zf.writestr(arc_name, data)
    zf.close()
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{folder_name}.zip"'},
    )


async def location_download(request: Request):
    """GET /api/locations/{id:int}/download — download entire location as ZIP."""
    db = await get_db()
    loc_id = int(request.path_params["id"])

    row = await db.execute_fetchall(
        "SELECT name, root_path FROM locations WHERE id = ?", (loc_id,)
    )
    if not row:
        return json_error("Location not found.", 404)
    loc_name = row[0]["name"]

    files = await db.execute_fetchall(
        "SELECT full_path, rel_path FROM files WHERE location_id = ?", (loc_id,)
    )

    buf = io.BytesIO()
    zf = zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED)
    for f in files:
        data = await _fetch_file_data(f["full_path"], loc_id)
        if data:
            zf.writestr(f["rel_path"], data)
    zf.close()
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{loc_name}.zip"'},
    )


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

    db = await get_db()
    flag = 1 if exclude else 0

    # Update the folder flag immediately on the shared connection so the
    # detail panel shows the correct checkbox state right away.
    row = await db.execute_fetchall(
        "SELECT name FROM folders WHERE id = ?", (folder_id,)
    )
    if not row:
        return json_error("Folder not found.", 404)
    folder_name = row[0]["name"]

    await db.execute(
        "UPDATE folders SET dup_exclude = ? WHERE id = ?", (flag, folder_id)
    )
    await db.commit()

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

    await set_setting(db, "dup_exclude_pending", f"{folder_id}:{1 if exclude else 0}")

    # Heavy work (descendant folders, files, dup_count recalc) in background
    from file_hunter.db import open_connection
    from file_hunter.services.dup_exclude import toggle_dup_exclude

    task_db = await open_connection()
    asyncio.create_task(toggle_dup_exclude(task_db, folder_id, exclude))
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
