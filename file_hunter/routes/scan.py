import asyncio
import logging
import os

from starlette.requests import Request

from file_hunter.core import BadRequest, json_error, json_ok, parse_bool, parse_int, parse_str, read_body
from file_hunter.db import folder_row, location_row, read_db
from file_hunter.services.queue_manager import (
    enqueue,
    cancel,
    cancel_by_location,
    get_queue_status,
    get_queue_status_for_broadcast,
)
from file_hunter.services import settings as settings_svc
from file_hunter.services.similarity import embedding_url, scan_label
from file_hunter.services.hash_backfill import cancel_backfill_by_location
from file_hunter.services.quick_scan import run_quick_scan
from file_hunter.ws.agent import get_agent_capabilities
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def scan_payload(body, loc_id, loc):
    """The queued payload for scanning the location, or the folder named by
    the body's folder_id."""
    payload = {
        "location_id": loc_id,
        "location_name": loc["name"],
        "path": loc["root_path"],
        "root_path": loc["root_path"],
    }
    raw_folder_id = body.get("folder_id")
    if raw_folder_id:
        fld_id = parse_int(raw_folder_id, "folder_id", prefix="fld-")
        folder = await folder_row(fld_id, "name, rel_path, location_id")
        if folder["location_id"] != loc_id:
            raise BadRequest("Folder does not belong to this location.")
        payload["path"] = os.path.join(loc["root_path"], folder["rel_path"])
        payload["folder_id"] = fld_id
        if folder["name"]:
            payload["location_name"] = f"{loc['name']} / {folder['name']}"
    return payload


async def queue_scan(op_type, agent_id, payload, entry_name, what):
    """Queue the scan, announce it to the UI, and answer "<what> queued"."""
    op_id = await enqueue(op_type, agent_id, payload)
    await broadcast(
        {
            "type": "scan_queued",
            "entry": {
                "queue_id": op_id,
                "location_id": payload["location_id"],
                "name": entry_name,
            },
            "queue": (await get_queue_status_for_broadcast()),
        }
    )
    label = payload["location_name"]
    return json_ok({"message": f"{what} queued for '{label}'", "queue_id": op_id})


async def start_scan(request: Request):
    body = await read_body(request)
    raw_id = body.get("location_id", "")
    loc_id = parse_int(raw_id, "location_id", prefix="loc-")

    loc = await location_row(loc_id, "name, root_path, agent_id")
    location_name = loc["name"]
    agent_id = loc["agent_id"]

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

    payload = await scan_payload(body, loc_id, loc)
    return await queue_scan(
        "scan_dir", agent_id, payload, payload["location_name"], "Scan"
    )


async def scan_capabilities(request: Request):
    """GET /api/scan/capabilities?location_id=N — check what scan types the agent supports."""
    raw_id = request.query_params.get("location_id", "")
    loc_id = parse_int(raw_id, "location_id", prefix="loc-")

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
    body = await read_body(request)
    raw_id = body.get("location_id", "")
    loc_id = parse_int(raw_id, "location_id", prefix="loc-")

    agent_id = (await location_row(loc_id, "agent_id"))["agent_id"]
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
        folder_id = parse_int(raw_folder_id, "folder_id", prefix="fld-")

    asyncio.create_task(run_quick_scan(loc_id, folder_id))
    return json_ok({"message": "Quick scan started"})


async def cancel_scan(request: Request):
    body = await read_body(request)

    # Cancel by queue/operation ID
    queue_id = body.get("queue_id")
    if queue_id is not None:
        cancelled = await cancel(parse_int(queue_id, "queue_id"))
        if cancelled:
            return json_ok({"message": "Operation cancelled."})
        return json_error("Queue item not found.", 400)

    # Cancel by location_id
    raw_id = body.get("location_id", "")
    loc_id = parse_int(raw_id, "location_id", prefix="loc-")

    cancel_type = parse_str(body.get("type"), "type", "scan")

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


async def start_similarity_scan(request: Request):
    """POST /api/scan/similarity — index images for similarity search."""
    body = await read_body(request)
    raw_id = body.get("location_id", "")
    loc_id = parse_int(raw_id, "location_id", prefix="loc-")
    recursive = parse_bool(body.get("recursive"), "recursive", True)

    async with read_db() as db:
        # Check similarity search is enabled
        enabled = await settings_svc.get_setting(db, "similaritySearchEnabled")
        if enabled != "1":
            return json_error("Similarity search is not enabled.", 400)
        embed_url = await embedding_url(db)

    loc = await location_row(loc_id, "name, root_path, agent_id")
    payload = await scan_payload(body, loc_id, loc)

    # "image", "document", or None (all)
    embed_types = parse_str(body.get("embed_types"), "embed_types", None)
    payload["recursive"] = recursive
    payload["embed_url"] = embed_url
    if embed_types:
        payload["embed_types"] = embed_types

    return await queue_scan(
        "similarity_scan",
        loc["agent_id"],
        payload,
        scan_label(embed_types, payload["location_name"]),
        "Similarity scan",
    )


async def get_scan_queue(request: Request):
    return json_ok(await get_queue_status_for_broadcast())
