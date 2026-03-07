"""Upload route — POST /api/upload, multipart/form-data."""

import asyncio
import os

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db, open_connection
from file_hunter.services import fs
from file_hunter.services.upload import run_upload
from file_hunter.ws.scan import broadcast


async def upload_files(request):
    """POST /api/upload — receive uploaded files and process them."""
    db = await get_db()

    form = await request.form()
    target_id = form.get("target_id")
    if not target_id:
        return json_error("target_id is required.", 400)

    # Resolve target to location_id, root_path, folder_id, dest_dir
    if target_id.startswith("loc-"):
        loc_id = int(target_id[4:])
        rows = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE id = ?", (loc_id,)
        )
        if not rows:
            return json_error("Location not found.", 404)
        location = dict(rows[0])
        location_id = location["id"]
        location_name = location["name"]
        root_path = location["root_path"]
        folder_id = None
        dest_dir = root_path
        rel_prefix = ""

    elif target_id.startswith("fld-"):
        fld_id = int(target_id[4:])
        rows = await db.execute_fetchall(
            """SELECT f.id as folder_id, f.rel_path, f.location_id,
                      l.name as location_name, l.root_path
               FROM folders f
               JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (fld_id,),
        )
        if not rows:
            return json_error("Folder not found.", 404)
        row = dict(rows[0])
        location_id = row["location_id"]
        location_name = row["location_name"]
        root_path = row["root_path"]
        folder_id = row["folder_id"]
        dest_dir = os.path.join(root_path, row["rel_path"])
        rel_prefix = row["rel_path"]

    else:
        return json_error("Invalid target_id format.", 400)

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

    # Launch background processing with its own DB connection
    task_db = await open_connection()
    asyncio.create_task(
        run_upload(
            task_db, location_id, location_name, root_path, folder_id, saved_files
        )
    )

    return json_ok(
        {
            "message": f"Uploading {len(saved_files)} file(s) to {location_name}",
            "fileCount": len(saved_files),
        }
    )
