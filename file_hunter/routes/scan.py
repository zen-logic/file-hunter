import logging

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

logger = logging.getLogger("file_hunter")


async def start_scan(request: Request):
    body = await request.json()
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))

    async with read_db() as db:
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
                return json_error(
                    f"'{location_name}' already has a pending operation.", 409
                )

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

    # Broadcast scan_queued for activity log (queue state already broadcast by enqueue)
    from file_hunter.ws.scan import broadcast

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
        from file_hunter.ws.scan import broadcast

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


async def start_fast_scan(request: Request):
    """POST /api/scan/fast — start a fast scan for a local location.

    Two-phase: first call without confirmed=true returns location info
    for a confirmation dialog. Second call with confirmed=true starts the scan.
    """
    import asyncio

    from file_hunter.services.fast_scan import is_running

    if is_running():
        return json_error("A fast scan is already running.")

    body = await request.json()
    raw_id = body.get("location_id", "")
    loc_id = int(str(raw_id).replace("loc-", ""))
    confirmed = bool(body.get("confirmed", False))

    async with read_db() as db:
        # Verify location exists and is local
        rows = await db.execute_fetchall(
            "SELECT l.id, l.name, l.root_path, l.agent_id, l.date_last_scanned, "
            "a.name as agent_name "
            "FROM locations l "
            "LEFT JOIN agents a ON a.id = l.agent_id "
            "WHERE l.id = ?",
            (loc_id,),
        )
    if not rows:
        return json_error("Location not found.", 404)

    loc = rows[0]
    if loc["agent_name"] != "Local Agent":
        return json_error("Fast scan is only available for local locations.")

    # Check if location has a pending queue operation
    status = await get_queue_status()
    for item in status:
        if item.get("location_id") == loc_id:
            return json_error(f"'{loc['name']}' already has a pending operation.", 409)

    previously_scanned = bool(loc["date_last_scanned"])

    if not confirmed:
        return json_ok(
            {
                "confirm": True,
                "locationName": loc["name"],
                "rootPath": loc["root_path"],
                "previouslyScanned": previously_scanned,
            }
        )

    # Confirmed — start the fast scan
    from file_hunter.services.fast_scan import run_fast_scan

    asyncio.create_task(run_fast_scan(loc_id, loc["root_path"], loc["name"]))
    return json_ok({"started": True})


async def fast_scan_progress(request: Request):
    """GET /api/scan/fast/progress — poll fast scan progress."""
    from file_hunter.services.fast_scan import get_progress

    return json_ok(get_progress())
