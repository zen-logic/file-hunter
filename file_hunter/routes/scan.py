import logging

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db
from file_hunter.services.queue_manager import (
    enqueue,
    cancel,
    cancel_by_location,
    get_queue_status,
)
from file_hunter.services.hash_backfill import cancel_backfill_by_location

logger = logging.getLogger("file_hunter")


async def start_scan(request: Request):
    db = await get_db()
    body = await request.json()
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    # Verify location exists
    rows = await db.execute_fetchall(
        "SELECT id, name, root_path, agent_id FROM locations WHERE id = ?", (loc_id,)
    )
    if not rows:
        return json_error("Location not found.", 404)

    location_name = rows[0]["name"]
    root_path = rows[0]["root_path"]
    agent_id = rows[0]["agent_id"]

    if not agent_id:
        return json_error(
            f"Location '{location_name}' has no agent assigned. "
            "Configure your agent with this location path.",
            400,
        )

    # Check if already running or queued for this location
    status = await get_queue_status()
    for item in status:
        if item.get("location_id") == loc_id:
            return json_error(f"'{location_name}' already has a pending operation.", 409)

    # Resolve scan path (full location or subfolder)
    scan_path = root_path
    folder_name = None
    raw_folder_id = body.get("folder_id")
    if raw_folder_id:
        import os

        fld_id = int(str(raw_folder_id).replace("fld-", ""))
        fld_rows = await db.execute_fetchall(
            "SELECT id, name, rel_path, location_id FROM folders WHERE id = ?",
            (fld_id,),
        )
        if not fld_rows:
            return json_error("Folder not found.", 404)
        folder = fld_rows[0]
        if folder["location_id"] != loc_id:
            return json_error("Folder does not belong to this location.", 400)
        scan_path = os.path.join(root_path, folder["rel_path"])
        folder_name = folder["name"]

    op_id = await enqueue(
        "scan_dir",
        agent_id,
        {
            "location_id": loc_id,
            "path": scan_path,
            "root_path": root_path,
        },
    )

    label = f"{location_name} / {folder_name}" if folder_name else location_name
    return json_ok(
        {"message": f"Scan queued for '{label}'", "queue_id": op_id}
    )


async def cancel_scan(request: Request):
    body = await request.json()

    # Cancel by queue/operation ID
    queue_id = body.get("queue_id")
    if queue_id is not None:
        cancelled = await cancel(int(queue_id))
        if cancelled:
            return json_ok({"message": "Operation cancelled."})
        return json_error("Queue item not found.", 400)

    # Cancel by location_id
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    cancel_type = body.get("type", "scan")

    if cancel_type == "backfill":
        if cancel_backfill_by_location(loc_id):
            return json_ok({"message": "Backfill cancellation requested."})
        return json_error("No backfill running for this location.", 400)

    cancelled = await cancel_by_location(loc_id)
    if cancelled:
        return json_ok({"message": "Scan cancellation requested."})
    return json_error("No scan running for this location.", 400)


async def get_scan_queue(request: Request):
    status = await get_queue_status()
    # Transform to match the frontend's expected format
    running_ids = [
        item["location_id"]
        for item in status
        if item.get("status") == "running" and item.get("location_id")
    ]
    pending = [
        {
            "queue_id": item["id"],
            "location_id": item.get("location_id"),
            "name": item.get("location_name", ""),
            "folder_name": item.get("params", {}).get("folder_name"),
            "queued_at": item.get("created_at", ""),
        }
        for item in status
        if item.get("status") == "pending"
    ]
    return json_ok(
        {
            "running_location_ids": running_ids,
            "running_location_id": running_ids[0] if running_ids else None,
            "pending": pending,
        }
    )
