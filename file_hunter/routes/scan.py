import asyncio
import logging
import os

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db
from file_hunter.services.queue_manager import (
    enqueue,
    cancel,
    cancel_by_location,
    get_queue_status,
    get_queue_status_for_broadcast,
)
from file_hunter.services.hash_backfill import cancel_backfill_by_location
from file_hunter.services.quick_scan import run_quick_scan
from file_hunter.ws.agent import get_agent_capabilities
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def start_scan(request: Request):
    body = await request.json()
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    async with read_db() as db:
        # Verify location exists
        rows = await db.execute_fetchall(
            "SELECT id, name, root_path, agent_id FROM locations WHERE id = ?",
            (loc_id,),
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
                return json_error(
                    f"'{location_name}' already has a pending operation.", 409
                )

        # Resolve scan path (full location or subfolder)
        scan_path = root_path
        folder_name = None
        raw_folder_id = body.get("folder_id")
        if raw_folder_id:
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
            "location_name": location_name,
            "path": scan_path,
            "root_path": root_path,
        },
    )

    # Broadcast scan_queued for activity log (queue state already broadcast by enqueue)
    label = f"{location_name} / {folder_name}" if folder_name else location_name
    await broadcast(
        {
            "type": "scan_queued",
            "entry": {
                "queue_id": op_id,
                "location_id": loc_id,
                "name": label,
            },
            "queue": (await get_queue_status_for_broadcast()),
        }
    )

    return json_ok({"message": f"Scan queued for '{label}'", "queue_id": op_id})


async def scan_capabilities(request: Request):
    """GET /api/scan/capabilities?location_id=N — check what scan types the agent supports."""
    raw_id = request.query_params.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT agent_id FROM locations WHERE id = ?", (loc_id,)
        )
    if not rows or not rows[0]["agent_id"]:
        return json_ok({"quick_scan": False})

    caps = get_agent_capabilities(rows[0]["agent_id"])
    return json_ok({"quick_scan": "quick_scan" in caps})


async def start_quick_scan(request: Request):
    """POST /api/scan/quick — shallow scan of a single folder or location root."""
    body = await request.json()
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, name, agent_id FROM locations WHERE id = ?", (loc_id,)
        )
        if not rows:
            return json_error("Location not found.", 404)

        agent_id = rows[0]["agent_id"]
        if not agent_id:
            return json_error("Location has no agent assigned.", 400)

    # Check agent supports quick_scan
    caps = get_agent_capabilities(agent_id)
    if "quick_scan" not in caps:
        return json_error(
            "This agent does not support quick scan. Update the agent to enable this feature.",
            400,
        )

    folder_id = None
    raw_folder_id = body.get("folder_id")
    if raw_folder_id:
        folder_id = int(str(raw_folder_id).replace("fld-", ""))

    asyncio.create_task(run_quick_scan(loc_id, folder_id))
    return json_ok({"message": "Quick scan started"})


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

    # Resolve location name for the dequeue broadcast
    async with read_db() as db:
        loc_rows = await db.execute_fetchall(
            "SELECT name FROM locations WHERE id = ?", (loc_id,)
        )
    location_name = loc_rows[0]["name"] if loc_rows else ""

    cancelled = await cancel_by_location(loc_id)
    if cancelled:
        await broadcast(
            {
                "type": "scan_dequeued",
                "entry": {"location_id": loc_id, "name": location_name},
                "queue": (await get_queue_status_for_broadcast()),
            }
        )
        return json_ok({"message": "Scan cancellation requested."})
    return json_error("No scan running for this location.", 400)


async def get_scan_queue(request: Request):
    return json_ok(await get_queue_status_for_broadcast())
