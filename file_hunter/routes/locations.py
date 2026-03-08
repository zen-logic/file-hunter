import asyncio
import os

from starlette.requests import Request
from file_hunter.db import get_db, execute_write
from file_hunter.core import json_ok, json_error
from file_hunter.services.locations import (
    get_shallow_tree,
    get_children,
    get_expand_path,
    create_folder,
    create_location,
    delete_location,
    rename_location,
    update_schedule,
    move_folder,
    get_treemap_children,
)
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast


async def list_locations(request: Request):
    db = await get_db()
    tree = await get_shallow_tree(db)
    return json_ok(tree)


async def add_location(request: Request):
    body = await request.json()
    name = body.get("name", "").strip()
    path = body.get("path", "").strip()
    if not name or not path:
        return json_error("Name and path are required.")
    if not await asyncio.to_thread(os.path.isdir, path):
        return json_error(f"Path does not exist or is not a directory: {path}", 400)

    # Find the local agent to assign this location to
    db = await get_db()
    cursor = await db.execute("SELECT id FROM agents WHERE name = 'Local Agent'")
    local_agent = await cursor.fetchone()
    agent_id = local_agent["id"] if local_agent else None

    try:
        loc = await execute_write(create_location, name, path, agent_id)
    except Exception as e:
        if "UNIQUE" in str(e):
            return json_error("A location with that path already exists.", 409)
        raise

    # Push location to agent config so it knows about the new path
    if agent_id:
        try:
            from file_hunter.services.agent_ops import _resolve_agent, _post

            resolved = _resolve_agent(agent_id)
            if resolved:
                host, port, token = resolved
                await _post(
                    host,
                    port,
                    token,
                    "/locations/add",
                    {"name": name, "path": path},
                )
        except Exception as e:
            import logging

            logging.getLogger("file_hunter").warning(
                "Failed to push new location to agent: %s", e
            )

    # Notify all connected browsers so they reload the tree
    await broadcast({"type": "location_changed", "action": "added", "location": loc})
    invalidate_stats_cache()
    return json_ok(loc, 201)


async def remove_location(request: Request):
    raw_id = request.path_params["id"]
    loc_id = int(raw_id.replace("loc-", ""))
    # Look up root_path before delete so we can notify the agent
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT root_path, agent_id FROM locations WHERE id = ?", (loc_id,)
    )
    root_path = row[0]["root_path"] if row else None
    agent_id = row[0]["agent_id"] if row else None
    await execute_write(delete_location, loc_id)
    await broadcast(
        {"type": "location_changed", "action": "deleted", "locationId": f"loc-{loc_id}"}
    )
    invalidate_stats_cache()
    # Notify agent (if agent-backed) so its config.json stays in sync
    from file_hunter.extensions import get_location_changed

    hook = get_location_changed()
    if hook and root_path:
        await hook("deleted", loc_id, root_path=root_path, agent_id=agent_id)
    return json_ok({"deleted": loc_id})


async def update_location(request: Request):
    raw_id = request.path_params["id"]
    loc_id = int(str(raw_id).replace("loc-", ""))
    body = await request.json()

    # Schedule update (can be sent alone or with name)
    if "scheduleEnabled" in body:
        import re

        enabled = bool(body.get("scheduleEnabled"))
        days = body.get("scheduleDays", [])
        time_val = body.get("scheduleTime", "03:00")
        # Validate days: list of ints 0-6
        if not isinstance(days, list) or not all(
            isinstance(d, int) and 0 <= d <= 6 for d in days
        ):
            return json_error("scheduleDays must be a list of integers 0-6.", 400)
        # Validate time: HH:MM
        if not re.match(r"^\d{2}:\d{2}$", time_val):
            return json_error("scheduleTime must be HH:MM format.", 400)
        await execute_write(update_schedule, loc_id, enabled, days, time_val)
        # If no name field, just broadcast and return
        if "name" not in body:
            await broadcast(
                {
                    "type": "location_changed",
                    "action": "updated",
                    "locationId": f"loc-{loc_id}",
                }
            )
            invalidate_stats_cache()
            return json_ok({"updated": True})

    new_name = body.get("name", "").strip()
    if not new_name:
        return json_error("Name is required.")
    result = await execute_write(rename_location, loc_id, new_name)
    if result is None:
        return json_error("Location not found.", 404)
    await broadcast(
        {"type": "location_changed", "action": "renamed", "location": result}
    )
    invalidate_stats_cache()
    # Notify agent (if agent-backed) so its config.json stays in sync
    from file_hunter.extensions import get_location_changed

    hook = get_location_changed()
    if hook:
        db = await get_db()
        row = await db.execute_fetchall(
            "SELECT root_path, agent_id FROM locations WHERE id = ?", (loc_id,)
        )
        if row:
            await hook(
                "renamed",
                loc_id,
                root_path=row[0]["root_path"],
                name=new_name,
                agent_id=row[0]["agent_id"],
            )
    return json_ok(result)


async def create_new_folder(request: Request):
    body = await request.json()
    parent_id = body.get("parent_id", "").strip()
    name = body.get("name", "").strip()
    if not parent_id or not name:
        return json_error("parent_id and name are required.")
    try:
        result = await execute_write(create_folder, parent_id, name)
    except ValueError as e:
        return json_error(str(e), 400)
    await broadcast({"type": "folder_created", "folder": result})
    invalidate_stats_cache()
    return json_ok(result, 201)


async def tree_children(request: Request):
    """Batch fetch immediate children for one or more folder IDs."""
    ids_raw = request.query_params.get("ids", "")
    if not ids_raw:
        return json_error("ids parameter is required.")
    try:
        folder_ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip()]
    except ValueError:
        return json_error("ids must be comma-separated integers.")
    if not folder_ids:
        return json_error("ids parameter is required.")
    db = await get_db()
    result = await get_children(db, folder_ids)
    return json_ok(result)


async def tree_expand(request: Request):
    """Get ancestor chain + children at each level for a target folder."""
    target_raw = request.query_params.get("target", "")
    if not target_raw:
        return json_error("target parameter is required.")
    try:
        target_id = int(target_raw)
    except ValueError:
        return json_error("target must be an integer folder ID.")
    db = await get_db()
    result = await get_expand_path(db, target_id)
    if result is None:
        return json_error("Folder not found.", 404)
    return json_ok(result)


async def treemap_data(request: Request):
    """GET /api/treemap/{id:int} — treemap children with cumulative sizes."""
    location_id = int(request.path_params["id"])
    parent_raw = request.query_params.get("parent_id", "")
    parent_id = int(parent_raw) if parent_raw else None
    db = await get_db()
    result = await get_treemap_children(db, location_id, parent_id)
    if result is None:
        return json_error("Location not found.", 404)
    return json_ok(result)


async def folder_move(request: Request):
    """POST /api/folders/{id:int}/move — move a folder to a new parent."""
    folder_id = int(request.path_params["id"])
    body = await request.json()
    destination_parent_id = body.get("destination_parent_id", "").strip()
    if not destination_parent_id:
        return json_error("destination_parent_id is required.")

    try:
        result = await execute_write(move_folder, folder_id, destination_parent_id)
    except ValueError as e:
        return json_error(str(e), 400)

    invalidate_stats_cache()

    await broadcast(
        {
            "type": "folder_moved",
            "folderId": folder_id,
            "name": result.get("name"),
        }
    )

    return json_ok(result)
