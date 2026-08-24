"""Catalog browsing API — hierarchical discovery of agents, locations, folders, files.

Same structure as the WebDAV interface but returns JSON:
    /api/browse/                          → list agents
    /api/browse/{agent}                   → list locations for agent
    /api/browse/{agent}/{location}        → root folders + files
    /api/browse/{agent}/{location}/{path} → folder contents
"""

from starlette.requests import Request
from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db


async def catalog_browse(request: Request):
    path = request.path_params.get("path", "").strip("/")

    if not path:
        return await _list_agents()

    parts = path.split("/", 2)
    agent_name = parts[0]

    async with read_db() as db:
        agent = await db.execute_fetchall(
            "SELECT id, name FROM agents WHERE name = ? ORDER BY id",
            (agent_name,),
        )
        if not agent:
            return json_error("Agent not found.", 404)
        agent_id = agent[0]["id"]
        agent_name = agent[0]["name"]

        if len(parts) == 1:
            return await _list_locations(db, agent_id, agent_name)

        loc_name = parts[1]
        loc = await db.execute_fetchall(
            "SELECT id, name FROM locations WHERE agent_id = ? AND name = ? ORDER BY id",
            (agent_id, loc_name),
        )
        if not loc:
            return json_error("Location not found.", 404)
        loc_id = loc[0]["id"]
        loc_name = loc[0]["name"]

        rel_path = parts[2] if len(parts) > 2 else ""

        if not rel_path:
            return await _list_folder(db, loc_id, None, agent_name, loc_name)

        # Resolve rel_path — folder or file?
        folder = await db.execute_fetchall(
            "SELECT id, name, rel_path FROM folders "
            "WHERE location_id = ? AND rel_path = ? AND stale = 0",
            (loc_id, rel_path),
        )
        if folder:
            return await _list_folder(db, loc_id, folder[0]["id"], agent_name, loc_name)

        file = await db.execute_fetchall(
            "SELECT id, filename, rel_path, file_size, file_type_high, "
            "file_type_low, description, modified_date, created_date, date_cataloged "
            "FROM files WHERE location_id = ? AND rel_path = ? AND stale = 0",
            (loc_id, rel_path),
        )
        if file:
            f = file[0]
            return json_ok({
                "kind": "file",
                "id": f["id"],
                "name": f["filename"],
                "path": f["rel_path"],
                "size": f["file_size"] or 0,
                "type": f["file_type_high"],
                "subtype": f["file_type_low"],
                "description": f["description"],
                "modified": f["modified_date"],
                "created": f["created_date"],
                "cataloged": f["date_cataloged"],
            })

        return json_error("Not found.", 404)


async def _list_agents():
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT DISTINCT a.id, a.name FROM agents a "
            "JOIN locations l ON l.agent_id = a.id "
            "ORDER BY a.name COLLATE NOCASE"
        )
    return json_ok({
        "kind": "root",
        "children": [{"kind": "agent", "name": r["name"]} for r in rows],
    })


async def _list_locations(db, agent_id, agent_name):
    rows = await db.execute_fetchall(
        "SELECT id, name FROM locations WHERE agent_id = ? ORDER BY name COLLATE NOCASE",
        (agent_id,),
    )
    return json_ok({
        "kind": "agent",
        "name": agent_name,
        "children": [{"kind": "location", "name": r["name"]} for r in rows],
    })


async def _list_folder(db, loc_id, folder_id, agent_name, loc_name):
    if folder_id is None:
        folders = await db.execute_fetchall(
            "SELECT id, name, rel_path FROM folders "
            "WHERE location_id = ? AND parent_id IS NULL AND stale = 0 "
            "ORDER BY name COLLATE NOCASE",
            (loc_id,),
        )
        files = await db.execute_fetchall(
            "SELECT id, filename, rel_path, file_size, file_type_high, "
            "file_type_low, modified_date "
            "FROM files "
            "WHERE location_id = ? AND folder_id IS NULL AND stale = 0 "
            "ORDER BY filename COLLATE NOCASE",
            (loc_id,),
        )
    else:
        folders = await db.execute_fetchall(
            "SELECT id, name, rel_path FROM folders "
            "WHERE parent_id = ? AND stale = 0 ORDER BY name COLLATE NOCASE",
            (folder_id,),
        )
        files = await db.execute_fetchall(
            "SELECT id, filename, rel_path, file_size, file_type_high, "
            "file_type_low, modified_date "
            "FROM files "
            "WHERE folder_id = ? AND stale = 0 ORDER BY filename COLLATE NOCASE",
            (folder_id,),
        )

    children = []
    for f in folders:
        children.append({
            "kind": "folder",
            "name": f["name"],
            "path": f["rel_path"],
        })
    for f in files:
        children.append({
            "kind": "file",
            "id": f["id"],
            "name": f["filename"],
            "path": f["rel_path"],
            "size": f["file_size"] or 0,
            "type": f["file_type_high"],
            "subtype": f["file_type_low"],
            "modified": f["modified_date"],
        })

    return json_ok({
        "kind": "location" if folder_id is None else "folder",
        "agent": agent_name,
        "location": loc_name,
        "children": children,
    })
