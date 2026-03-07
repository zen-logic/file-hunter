import logging

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db
from file_hunter.services.scan_queue import enqueue, dequeue, get_queue_state
from file_hunter.services.scan_ingest import is_location_scanning
from file_hunter.services.scan_trigger import agent_scan_cancel

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

    if not rows[0]["agent_id"]:
        return json_error(
            f"Location '{location_name}' has no agent assigned. "
            "Configure your agent with this location path.",
            400,
        )

    # Reject if agent is already scanning this location
    if is_location_scanning(loc_id):
        return json_error(f"Agent is already scanning '{location_name}'.", 409)

    # Optional subfolder scan
    scan_path = None
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

    # Enqueue (rejects duplicates — already running or already queued)
    try:
        entry = await enqueue(
            loc_id,
            location_name,
            root_path,
            scan_path=scan_path,
            folder_name=folder_name,
        )
    except ValueError as exc:
        return json_error(str(exc), 409)

    label = f"{location_name} / {folder_name}" if folder_name else location_name
    return json_ok(
        {"message": f"Scan queued for '{label}'", "queue_id": entry["queue_id"]}
    )


async def cancel_scan(request: Request):
    body = await request.json()

    # Support cancelling a pending queue item by queue_id
    queue_id = body.get("queue_id")
    if queue_id is not None:
        removed = await dequeue(int(queue_id))
        if removed:
            return json_ok({"message": f"Dequeued scan for '{removed['name']}'."})
        return json_error("Queue item not found.", 400)

    # Cancel a running scan by location_id
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    handled = await agent_scan_cancel(loc_id)
    if handled:
        return json_ok({"message": "Agent scan cancellation requested."})

    return json_error("No scan running for this location.", 400)


async def get_scan_queue(request: Request):
    return json_ok(get_queue_state())
