"""Upload route — POST /api/upload, multipart/form-data."""

import asyncio
import os

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db
from file_hunter.helpers import resolve_target
from file_hunter.services import fs
from file_hunter.services.upload import run_upload
from file_hunter.ws.scan import broadcast


async def upload_files(request):
    """POST /api/upload — receive uploaded files and process them."""
    form = await request.form()
    target_id = form.get("target_id")
    if not target_id:
        return json_error("target_id is required.", 400)

    # Resolve target to location_id, root_path, folder_id, dest_dir
    async with read_db() as db:
        target = await resolve_target(db, target_id)
        if target is None:
            return json_error("Target not found.", 404)

        location_id = target["location_id"]
        location_name = (
            target["name"]
            if target["kind"] == "loc"
            else target.get("location_name", target["name"])
        )
        root_path = target["root_path"]
        folder_id = target["folder_id"]
        dest_dir = target["abs_path"]
        rel_prefix = target["rel_path"]

    # Check location is online
    if not await fs.dir_exists(dest_dir, location_id):
        return json_error("Location is offline or path does not exist.", 400)

    # Collect uploaded files
    files = form.getlist("files")
    if not files:
        return json_error("No files provided.", 400)

    total_files = sum(1 for f in files if hasattr(f, "filename") and f.filename)

    saved_files = []
    file_num = 0
    for upload_file in files:
        if not hasattr(upload_file, "filename") or not upload_file.filename:
            continue

        file_num += 1
        dest_path = os.path.join(dest_dir, upload_file.filename)

        # Skip if file already exists at this path
        if await fs.path_exists(dest_path, location_id):
            continue

        # Get file size without reading entire file into RAM
        upload_file.file.seek(0, 2)
        file_size = upload_file.file.tell()
        upload_file.file.seek(0)

        from file_hunter.services.fs import agent_upload_file

        file_size_mb = file_size / 1048576

        async def _progress(sent, total):
            pct = round((sent / total) * 100) if total else 100
            sent_mb = sent / 1048576
            await broadcast(
                {
                    "type": "upload_transfer",
                    "locationId": location_id,
                    "location": location_name,
                    "current": file_num,
                    "total": total_files,
                    "filename": upload_file.filename,
                    "pct": pct,
                    "sentMB": round(sent_mb, 1),
                    "totalMB": round(file_size_mb, 1),
                }
            )

        await agent_upload_file(
            dest_dir,
            upload_file.filename,
            upload_file.file,
            file_size,
            location_id,
            on_progress=_progress,
        )

        rel_path = (
            os.path.join(rel_prefix, upload_file.filename)
            if rel_prefix
            else upload_file.filename
        )
        saved_files.append(
            {
                "filename": upload_file.filename,
                "full_path": dest_path,
                "rel_path": rel_path,
            }
        )

    if not saved_files:
        return json_error("No new files to upload (all skipped or already exist).", 400)

    # Launch background processing
    asyncio.create_task(
        run_upload(location_id, location_name, root_path, folder_id, saved_files)
    )

    return json_ok(
        {
            "message": f"Uploading {len(saved_files)} file(s) to {location_name}",
            "fileCount": len(saved_files),
        }
    )
