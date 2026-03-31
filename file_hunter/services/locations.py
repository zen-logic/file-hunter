"""Location CRUD and tree building."""

import asyncio
import logging
import os
from datetime import datetime

from file_hunter.extensions import get_agent_location_ids, get_agent_label_prefixes
from file_hunter.helpers import (
    parse_folder_id,
    parse_location_id,
    parse_mtime,
    post_op_stats,
)
from file_hunter.services import fs
from file_hunter.services.activity import register, unregister, update
from file_hunter.services.online_check import (
    agent_disk_stats,
    agent_online_check,
    register_agent_location,
)
from file_hunter.services.settings import get_setting
from file_hunter.stats_db import read_stats
from file_hunter.ws.scan import broadcast


async def get_tree(db):
    """Build the full navigation tree: locations with nested folders.

    Kept as internal backup — the locations endpoint now uses get_shallow_tree().
    """
    locations = await db.execute_fetchall(
        "SELECT id, name, root_path, date_added, date_last_scanned FROM locations ORDER BY name COLLATE NOCASE"
    )
    folders = await db.execute_fetchall(
        "SELECT id, location_id, parent_id, name, rel_path FROM folders ORDER BY name"
    )

    # Group folders by location_id
    folders_by_loc = {}
    for f in folders:
        loc_id = f["location_id"]
        if loc_id not in folders_by_loc:
            folders_by_loc[loc_id] = []
        folders_by_loc[loc_id].append(dict(f))

    # Check online status for all locations in a single thread call
    online_flags = await asyncio.to_thread(_check_paths_exist, list(locations))

    # Build the tree in a thread — even with O(n) algorithm, large folder
    # counts can take non-trivial CPU time that would block the event loop.
    tree = await asyncio.to_thread(
        _build_tree_sync, locations, folders_by_loc, online_flags
    )
    return tree


def _build_tree_sync(locations, folders_by_loc, online_flags):
    """Build the full navigation tree from pre-fetched data (runs in a worker thread).

    Args:
        locations: List of location row dicts with id, name, root_path, etc.
        folders_by_loc: Dict mapping location_id -> list of folder dicts.
        online_flags: List of bools parallel to locations, True if online.

    Returns:
        list[dict]: Tree nodes with id (prefixed "loc-N"), type, label, online,
        and nested children.

    Notes:
        Runs in asyncio.to_thread to avoid blocking the event loop on large
        folder counts. Called only by get_tree().
    """
    tree = []
    for loc, online in zip(locations, online_flags):
        node = {
            "id": f"loc-{loc['id']}",
            "type": "location",
            "label": loc["name"],
            "online": online,
            "children": _build_folder_tree(
                folders_by_loc.get(loc["id"], []), parent_id=None
            ),
        }
        tree.append(node)
    return tree


async def get_shallow_tree(db):
    """Build a shallow navigation tree: locations + root-level folders only.

    Each folder includes a hasChildren flag. children is set to null (None)
    to signal "not loaded yet" to the frontend.
    """
    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"

    locations = await db.execute_fetchall(
        "SELECT id, name, root_path, date_added, date_last_scanned, is_favourite "
        "FROM locations WHERE name NOT LIKE '__deleting_%' "
        "ORDER BY name COLLATE NOCASE"
    )

    # Root-level folders (parent_id IS NULL) with has_children flag
    root_folders = await db.execute_fetchall(
        f"""SELECT f.id, f.location_id, f.name, f.hidden, f.dup_exclude, f.stale, f.is_favourite,
                  EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
           FROM folders f
           WHERE f.parent_id IS NULL{hidden_filter}
           ORDER BY f.name"""
    )

    # Fetch sizes from stats.db
    loc_ids = [loc["id"] for loc in locations]
    folder_ids = [f["id"] for f in root_folders]

    loc_sizes: dict[int, int] = {}
    folder_sizes: dict[int, int] = {}
    async with read_stats() as sdb:
        if loc_ids:
            ph = ",".join("?" for _ in loc_ids)
            ls_rows = await sdb.execute_fetchall(
                f"SELECT location_id, total_size FROM location_stats "
                f"WHERE location_id IN ({ph})",
                loc_ids,
            )
            loc_sizes = {r["location_id"]: r["total_size"] or 0 for r in ls_rows}
        if folder_ids:
            ph = ",".join("?" for _ in folder_ids)
            fs_rows = await sdb.execute_fetchall(
                f"SELECT folder_id, total_size FROM folder_stats "
                f"WHERE folder_id IN ({ph})",
                folder_ids,
            )
            folder_sizes = {r["folder_id"]: r["total_size"] or 0 for r in fs_rows}

    # Group root folders by location_id (no I/O, do before gather)
    roots_by_loc = {}
    for f in root_folders:
        loc_id = f["location_id"]
        if loc_id not in roots_by_loc:
            roots_by_loc[loc_id] = []
        roots_by_loc[loc_id].append(f)

    online_flags = await asyncio.to_thread(_check_paths_exist, list(locations))

    # Gather disk stats for online locations (concurrent)
    disk_stats_tasks = []
    for loc, online in zip(locations, online_flags):
        if online:
            disk_stats_tasks.append(get_disk_stats(loc["id"], loc["root_path"]))
        else:
            disk_stats_tasks.append(asyncio.sleep(0, result=None))
    disk_stats_results = await asyncio.gather(*disk_stats_tasks)

    agent_loc_ids = get_agent_location_ids()
    agent_prefixes = get_agent_label_prefixes()

    tree = []
    for loc, online, ds in zip(locations, online_flags, disk_stats_results):
        children = []
        for f in roots_by_loc.get(loc["id"], []):
            child_node = {
                "id": f"fld-{f['id']}",
                "type": "folder",
                "label": f["name"],
                "hasChildren": bool(f["has_children"]),
                "totalSize": folder_sizes.get(f["id"], 0),
                "children": None,  # not loaded sentinel
            }
            if f["hidden"]:
                child_node["hidden"] = True
            if f["dup_exclude"]:
                child_node["dupExcluded"] = True
            if f["stale"]:
                child_node["stale"] = True
            if f["is_favourite"]:
                child_node["favourite"] = True
            children.append(child_node)
        label = loc["name"]
        agent_name = agent_prefixes.get(loc["id"])
        if agent_name:
            label = f"{label} [{agent_name}]"
        node = {
            "id": f"loc-{loc['id']}",
            "type": "location",
            "label": label,
            "online": online,
            "totalSize": loc_sizes.get(loc["id"], 0),
            "diskStats": ds,
            "children": children,
        }
        if loc["is_favourite"]:
            node["favourite"] = True
        if loc["id"] in agent_loc_ids:
            node["agent"] = "local" if agent_name == "Local Agent" else "remote"
        if loc["date_last_scanned"]:
            node["lastScanned"] = loc["date_last_scanned"]
        tree.append(node)
    return tree


async def get_children(db, folder_ids: list[int]):
    """Batch fetch immediate children for one or more folder IDs.

    Returns {folder_id: [child_nodes]} where each child has a hasChildren flag.
    """
    if not folder_ids:
        return {}

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"

    placeholders = ",".join("?" * len(folder_ids))
    rows = await db.execute_fetchall(
        f"""SELECT f.id, f.parent_id, f.name, f.hidden, f.dup_exclude, f.stale, f.is_favourite,
                   EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
            FROM folders f
            WHERE f.parent_id IN ({placeholders}){hidden_filter}
            ORDER BY f.name""",
        folder_ids,
    )

    # Fetch sizes from stats.db
    child_ids = [r["id"] for r in rows]
    child_sizes: dict[int, int] = {}
    if child_ids:
        async with read_stats() as sdb:
            ph = ",".join("?" for _ in child_ids)
            fs_rows = await sdb.execute_fetchall(
                f"SELECT folder_id, total_size FROM folder_stats "
                f"WHERE folder_id IN ({ph})",
                child_ids,
            )
            child_sizes = {r["folder_id"]: r["total_size"] or 0 for r in fs_rows}

    result = {}
    for r in rows:
        key = f"fld-{r['parent_id']}"
        if key not in result:
            result[key] = []
        child_node = {
            "id": f"fld-{r['id']}",
            "type": "folder",
            "label": r["name"],
            "hasChildren": bool(r["has_children"]),
            "totalSize": child_sizes.get(r["id"], 0),
            "children": None,
        }
        if r["hidden"]:
            child_node["hidden"] = True
        if r["dup_exclude"]:
            child_node["dupExcluded"] = True
        if r["stale"]:
            child_node["stale"] = True
        if r["is_favourite"]:
            child_node["favourite"] = True
        result[key].append(child_node)

    # Ensure every requested ID has an entry (empty list if no children)
    for fid in folder_ids:
        key = f"fld-{fid}"
        if key not in result:
            result[key] = []

    return result


async def get_expand_path(db, target_id: int):
    """Get the ancestor chain from root to target folder, plus children at each level.

    Used by navigateTo()/revealNode() when the target node isn't loaded yet.
    Returns {locationId, path, childrenByParent}.
    """
    # Recursive CTE to find ancestors from target to root
    ancestors = await db.execute_fetchall(
        """WITH RECURSIVE ancestors(id, parent_id, location_id, name, depth) AS (
               SELECT id, parent_id, location_id, name, 0
               FROM folders WHERE id = ?
               UNION ALL
               SELECT f.id, f.parent_id, f.location_id, f.name, a.depth + 1
               FROM folders f
               JOIN ancestors a ON f.id = a.parent_id
           )
           SELECT id, parent_id, location_id, name, depth FROM ancestors
           ORDER BY depth DESC""",
        (target_id,),
    )

    if not ancestors:
        return None

    # Build the path (root ancestor first, target last)
    path = [f"fld-{a['id']}" for a in ancestors]
    location_id = ancestors[0]["location_id"]

    # Collect parent IDs we need children for:
    # - The location root (parent_id IS NULL) for root-level siblings
    # - Each ancestor's parent_id for siblings at that level
    # We need children of: the location (root level), and each ancestor folder
    parent_ids_to_fetch = []
    for a in ancestors:
        parent_ids_to_fetch.append(a["id"])  # children of this ancestor

    # Fetch children for all ancestor folders (batch)
    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"

    children_by_parent = {}

    if parent_ids_to_fetch:
        placeholders = ",".join("?" * len(parent_ids_to_fetch))
        rows = await db.execute_fetchall(
            f"""SELECT f.id, f.parent_id, f.name, f.hidden, f.dup_exclude, f.stale, f.is_favourite,
                       EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
                FROM folders f
                WHERE f.parent_id IN ({placeholders}){hidden_filter}
                ORDER BY f.name""",
            parent_ids_to_fetch,
        )

        # Fetch sizes from stats.db
        all_ids = [r["id"] for r in rows]
        sz_map: dict[int, int] = {}
        if all_ids:
            async with read_stats() as sdb:
                ph = ",".join("?" for _ in all_ids)
                fs_rows = await sdb.execute_fetchall(
                    f"SELECT folder_id, total_size FROM folder_stats "
                    f"WHERE folder_id IN ({ph})",
                    all_ids,
                )
                sz_map = {r["folder_id"]: r["total_size"] or 0 for r in fs_rows}

        for r in rows:
            key = f"fld-{r['parent_id']}"
            if key not in children_by_parent:
                children_by_parent[key] = []
            child_node = {
                "id": f"fld-{r['id']}",
                "type": "folder",
                "label": r["name"],
                "hasChildren": bool(r["has_children"]),
                "totalSize": sz_map.get(r["id"], 0),
                "children": None,
            }
            if r["hidden"]:
                child_node["hidden"] = True
            if r["dup_exclude"]:
                child_node["dupExcluded"] = True
            if r["stale"]:
                child_node["stale"] = True
            if r["is_favourite"]:
                child_node["favourite"] = True
            children_by_parent[key].append(child_node)

    # Also fetch root-level siblings (children of the location)
    root_rows = await db.execute_fetchall(
        f"""SELECT f.id, f.name, f.hidden, f.dup_exclude, f.stale, f.is_favourite,
                  EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
           FROM folders f
           WHERE f.location_id = ? AND f.parent_id IS NULL{hidden_filter}
           ORDER BY f.name""",
        (location_id,),
    )

    # Fetch root folder sizes from stats.db
    root_ids = [r["id"] for r in root_rows]
    root_sz: dict[int, int] = {}
    if root_ids:
        async with read_stats() as sdb:
            ph = ",".join("?" for _ in root_ids)
            fs_rows = await sdb.execute_fetchall(
                f"SELECT folder_id, total_size FROM folder_stats "
                f"WHERE folder_id IN ({ph})",
                root_ids,
            )
            root_sz = {r["folder_id"]: r["total_size"] or 0 for r in fs_rows}

    loc_key = f"loc-{location_id}"
    children_by_parent[loc_key] = []
    for r in root_rows:
        child_node = {
            "id": f"fld-{r['id']}",
            "type": "folder",
            "label": r["name"],
            "hasChildren": bool(r["has_children"]),
            "totalSize": root_sz.get(r["id"], 0),
            "children": None,
        }
        if r["hidden"]:
            child_node["hidden"] = True
        if r["dup_exclude"]:
            child_node["dupExcluded"] = True
        if r["stale"]:
            child_node["stale"] = True
        if r["is_favourite"]:
            child_node["favourite"] = True
        children_by_parent[loc_key].append(child_node)

    return {
        "locationId": loc_key,
        "path": path,
        "childrenByParent": children_by_parent,
    }


def check_location_online(location_id: int, root_path: str) -> bool:
    """Check if a location is reachable via its agent.

    Args:
        location_id: Numeric location ID.
        root_path: Absolute path to the location root on the agent.

    Returns:
        bool: True if the agent reports the location as accessible.

    Notes:
        Delegates to agent_online_check(). Must be called from a worker thread
        (via asyncio.to_thread) since it performs synchronous I/O. Called by
        get_shallow_tree, get_location_stats, get_folder_stats, and others.
    """
    return agent_online_check({"id": location_id, "root_path": root_path})


async def get_disk_stats(location_id: int, root_path: str) -> dict | None:
    """Fetch disk usage statistics for a location from its agent.

    Args:
        location_id: Numeric location ID (used to route to the correct agent).
        root_path: Absolute path to the location root on the agent.

    Returns:
        dict | None: Dict with keys {mount, total, free, readonly} on success,
        {mount: false} if no mount found, or None on any error.

    Notes:
        Async — calls agent_disk_stats which makes an HTTP request to the agent.
        Called by get_shallow_tree, get_location_stats, and get_folder_stats.
    """
    try:
        return await agent_disk_stats(location_id, root_path)
    except Exception:
        return None


def _check_paths_exist(locations: list) -> list[bool]:
    """Batch-check online status for multiple locations in a single thread call.

    Args:
        locations: List of location row dicts, each with at least 'id' and
            'root_path' keys.

    Returns:
        list[bool]: Parallel list of online flags, one per input location.

    Notes:
        Runs in asyncio.to_thread. Delegates each check to agent_online_check().
        Called by get_tree() and get_shallow_tree().
    """
    return [agent_online_check(loc) for loc in locations]


def _build_folder_tree(folders, parent_id):
    """Build a nested tree structure from a flat list of folder dicts in O(n).

    Args:
        folders: List of folder dicts with id, parent_id, name keys.
        parent_id: The parent_id to use as the tree root (None for root-level).

    Returns:
        list[dict]: Nested tree nodes with id (prefixed "fld-N"), type, label,
        and children lists.

    Notes:
        Pre-groups folders by parent_id for O(1) lookup per node. Called by
        _build_tree_sync() for the full tree view.
    """
    # Pre-group folders by parent_id so each lookup is O(1)
    by_parent = {}
    for f in folders:
        pid = f["parent_id"]
        if pid not in by_parent:
            by_parent[pid] = []
        by_parent[pid].append(f)

    def _build(pid):
        children = []
        for f in by_parent.get(pid, []):
            node = {
                "id": f"fld-{f['id']}",
                "type": "folder",
                "label": f["name"],
                "children": _build(f["id"]),
            }
            children.append(node)
        return children

    return _build(parent_id)


async def create_folder(db, parent_id: str, name: str) -> dict:
    """Create a new folder on disk and in the catalog.

    Args:
        db: Write-capable DB connection (inside db_writer context).
        parent_id: Prefixed parent identifier — "loc-N" for location root
            or "fld-N" for a subfolder.
        name: Folder name (must not contain / or \\).

    Returns:
        dict: New tree node with id ("fld-N"), type, label, hasChildren,
        totalSize, parentId, and locationId.

    Raises:
        ValueError: If parent not found, location offline, invalid name,
            or name collision on disk or in the catalog.

    Side effects:
        Creates the directory on disk via fs.dir_create. Inserts a row into
        the folders table. Commits the transaction.

    Notes:
        Called from the create-folder HTTP endpoint.
    """
    # Resolve parent to get location info and path
    if str(parent_id).startswith("loc-"):
        loc_id = parse_location_id(parent_id)
        row = await db.execute_fetchall(
            "SELECT id, root_path FROM locations WHERE id = ?", (loc_id,)
        )
        if not row:
            raise ValueError("Location not found.")
        root_path = row[0]["root_path"]
        parent_fld_id = None
        parent_rel = ""
    elif str(parent_id).startswith("fld-"):
        fld_id = parse_folder_id(parent_id)
        row = await db.execute_fetchall(
            """SELECT f.id, f.location_id, f.rel_path, l.root_path
               FROM folders f JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (fld_id,),
        )
        if not row:
            raise ValueError("Parent folder not found.")
        loc_id = row[0]["location_id"]
        root_path = row[0]["root_path"]
        parent_fld_id = fld_id
        parent_rel = row[0]["rel_path"]
    else:
        raise ValueError("Invalid parent_id.")

    # Safety: location must be online
    if not await fs.dir_exists(root_path, loc_id):
        raise ValueError("Location is offline.")

    # Validate name
    if not name or "/" in name or "\\" in name:
        raise ValueError("Invalid folder name.")

    # Build paths
    rel_path = os.path.join(parent_rel, name) if parent_rel else name
    abs_path = os.path.join(root_path, rel_path)

    # Check for collision on disk
    if await fs.path_exists(abs_path, loc_id):
        raise ValueError("A folder with that name already exists on disk.")

    # Check for collision in DB
    existing = await db.execute_fetchall(
        "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
        (loc_id, rel_path),
    )
    if existing:
        raise ValueError("A folder with that name already exists in the catalog.")

    # Create on disk
    await fs.dir_create(abs_path, loc_id)

    # Insert into DB
    cursor = await db.execute(
        "INSERT INTO folders (location_id, parent_id, name, rel_path) VALUES (?, ?, ?, ?)",
        (loc_id, parent_fld_id, name, rel_path),
    )
    await db.commit()
    new_id = cursor.lastrowid

    # Check if it has children (it won't, it's new)
    return {
        "id": f"fld-{new_id}",
        "type": "folder",
        "label": name,
        "hasChildren": False,
        "totalSize": 0,
        "parentId": parent_id,
        "locationId": f"loc-{loc_id}",
    }


async def create_location(db, name: str, root_path: str, agent_id: int = None) -> dict:
    """Insert a new location into the catalog and return its tree node.

    Args:
        db: Write-capable DB connection (inside db_writer context).
        name: Display name for the location.
        root_path: Absolute path on the agent filesystem.
        agent_id: Optional agent ID to associate with this location.

    Returns:
        dict: Tree node with id ("loc-N"), type, label, online flag, and
        empty children list.

    Side effects:
        Writes to locations table. Registers the agent-location mapping in
        online_check state. Commits the transaction.

    Notes:
        Called from the add-location HTTP endpoint and agent sync.
    """
    now = datetime.now().isoformat(timespec="seconds")
    cursor = await db.execute(
        "INSERT INTO locations (name, root_path, agent_id, date_added) VALUES (?, ?, ?, ?)",
        (name, root_path, agent_id, now),
    )
    await db.commit()
    loc_id = cursor.lastrowid

    # Register in online check state so the location is immediately visible
    if agent_id is not None:
        register_agent_location(agent_id, loc_id)

    online = await asyncio.to_thread(check_location_online, loc_id, root_path)
    return {
        "id": f"loc-{loc_id}",
        "type": "location",
        "label": name,
        "online": online,
        "children": [],
    }


async def rename_location(db, loc_id: int, new_name: str) -> dict | None:
    """Rename an existing location in the catalog.

    Args:
        db: Write-capable DB connection (inside db_writer context).
        loc_id: Numeric location ID.
        new_name: New display name.

    Returns:
        dict | None: Updated node dict with id, label, online flag. None if
        the location does not exist.

    Side effects:
        Writes to locations table. Commits the transaction.

    Notes:
        Called from the rename-location HTTP endpoint.
    """
    row = await db.execute_fetchall(
        "SELECT id, root_path FROM locations WHERE id = ?", (loc_id,)
    )
    if not row:
        return None
    await db.execute("UPDATE locations SET name = ? WHERE id = ?", (new_name, loc_id))
    await db.commit()
    online = await asyncio.to_thread(check_location_online, loc_id, row[0]["root_path"])
    return {
        "id": f"loc-{loc_id}",
        "label": new_name,
        "online": online,
    }


async def update_schedule(db, loc_id: int, enabled: bool, days: list[int], time: str):
    """Update the scan schedule columns for a location.

    Args:
        db: Write-capable DB connection (inside db_writer context).
        loc_id: Numeric location ID.
        enabled: Whether the schedule is active.
        days: List of weekday ints (0=Monday .. 6=Sunday).
        time: Time string in "HH:MM" format.

    Side effects:
        Writes scan_schedule_enabled, scan_schedule_days, scan_schedule_time
        on the locations row. Commits the transaction.

    Notes:
        Called from the schedule HTTP endpoint.
    """
    enabled_int = 1 if enabled else 0
    days_str = ",".join(str(d) for d in sorted(days))
    await db.execute(
        "UPDATE locations SET scan_schedule_enabled = ?, scan_schedule_days = ?, "
        "scan_schedule_time = ? WHERE id = ?",
        (enabled_int, days_str, time, loc_id),
    )
    await db.commit()


async def get_treemap_children(db, location_id: int, parent_folder_id: int | None):
    """Return immediate children with cumulative sizes for treemap rendering.

    If parent_folder_id is None, returns root-level folders for the location.
    Also returns direct files size/count, breadcrumb, and total size.
    """
    # Verify location exists
    loc_row = await db.execute_fetchall(
        "SELECT id, name FROM locations WHERE id = ?", (location_id,)
    )
    if not loc_row:
        return None
    loc_name = loc_row[0]["name"]

    # Fetch child folders (structural data from catalog)
    if parent_folder_id is None:
        children_rows = await db.execute_fetchall(
            """SELECT f.id, f.name,
                      EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id) AS has_children
               FROM folders f
               WHERE f.location_id = ? AND f.parent_id IS NULL
               ORDER BY f.name""",
            (location_id,),
        )
        direct_row = await db.execute_fetchall(
            """SELECT COALESCE(SUM(file_size), 0) AS total, COUNT(*) AS cnt
               FROM files WHERE location_id = ? AND folder_id IS NULL""",
            (location_id,),
        )
    else:
        children_rows = await db.execute_fetchall(
            """SELECT f.id, f.name,
                      EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id) AS has_children
               FROM folders f
               WHERE f.parent_id = ?
               ORDER BY f.name""",
            (parent_folder_id,),
        )
        # Direct files in this folder
        direct_row = await db.execute_fetchall(
            """SELECT COALESCE(SUM(file_size), 0) AS total, COUNT(*) AS cnt
               FROM files WHERE folder_id = ?""",
            (parent_folder_id,),
        )

    direct_files_size = direct_row[0]["total"] if direct_row else 0
    direct_files_count = direct_row[0]["cnt"] if direct_row else 0

    # Fetch top 50 largest individual files for treemap display
    if parent_folder_id is None:
        top_files = await db.execute_fetchall(
            """SELECT id, filename, file_size
               FROM files WHERE location_id = ? AND folder_id IS NULL
               ORDER BY file_size DESC LIMIT 50""",
            (location_id,),
        )
    else:
        top_files = await db.execute_fetchall(
            """SELECT id, filename, file_size
               FROM files WHERE folder_id = ?
               ORDER BY file_size DESC LIMIT 50""",
            (parent_folder_id,),
        )

    # File counts per child folder (direct files only, for display)
    child_ids = [r["id"] for r in children_rows]
    file_counts = {}
    if child_ids:
        placeholders = ",".join("?" * len(child_ids))
        fc_rows = await db.execute_fetchall(
            f"""SELECT folder_id, COUNT(*) AS cnt
                FROM files WHERE folder_id IN ({placeholders})
                GROUP BY folder_id""",
            child_ids,
        )
        file_counts = {r["folder_id"]: r["cnt"] for r in fc_rows}

    # Build breadcrumb
    breadcrumb = [{"id": f"loc-{location_id}", "name": loc_name}]
    if parent_folder_id is not None:
        ancestors = await db.execute_fetchall(
            """WITH RECURSIVE anc(id, parent_id, name, depth) AS (
                   SELECT id, parent_id, name, 0 FROM folders WHERE id = ?
                   UNION ALL
                   SELECT f.id, f.parent_id, f.name, a.depth + 1
                   FROM folders f JOIN anc a ON f.id = a.parent_id
               )
               SELECT id, name FROM anc ORDER BY depth DESC""",
            (parent_folder_id,),
        )
        for a in ancestors:
            breadcrumb.append({"id": a["id"], "name": a["name"]})

    # Fetch sizes from stats.db for treemap children
    tm_ids = [r["id"] for r in children_rows]
    tm_sizes: dict[int, int] = {}
    if tm_ids:
        async with read_stats() as sdb:
            ph = ",".join("?" for _ in tm_ids)
            fs_rows = await sdb.execute_fetchall(
                f"SELECT folder_id, total_size FROM folder_stats "
                f"WHERE folder_id IN ({ph})",
                tm_ids,
            )
            tm_sizes = {r["folder_id"]: r["total_size"] or 0 for r in fs_rows}

    # Build children list
    children = []
    for r in children_rows:
        children.append(
            {
                "id": r["id"],
                "name": r["name"],
                "totalSize": tm_sizes.get(r["id"], 0),
                "hasChildren": bool(r["has_children"]),
                "fileCount": file_counts.get(r["id"], 0),
            }
        )

    # Total size = sum of children sizes + direct files
    total_size = sum(c["totalSize"] for c in children) + direct_files_size

    # Parent name for display
    parent_name = None
    if parent_folder_id is not None and breadcrumb:
        parent_name = breadcrumb[-1]["name"]

    return {
        "locationId": location_id,
        "locationName": loc_name,
        "parentId": parent_folder_id,
        "parentName": parent_name,
        "breadcrumb": breadcrumb,
        "totalSize": total_size,
        "directFilesSize": direct_files_size,
        "directFilesCount": direct_files_count,
        "directFiles": [
            {"id": f["id"], "name": f["filename"], "size": f["file_size"]}
            for f in top_files
        ],
        "children": children,
    }


async def _cross_location_dir_move(
    db,
    fld,
    src_abs,
    dest_abs,
    src_loc_id,
    dest_loc_id,
    old_prefix,
    new_prefix,
    dest_root,
    desc_folder_rows,
):
    """Copy a folder tree between agents, then delete the source.

    Args:
        db: Read-capable DB connection for querying file rows.
        fld: Dict of the source folder row (id, name, rel_path, etc.).
        src_abs: Absolute path of the source folder on the source agent.
        dest_abs: Absolute path of the destination folder on the dest agent.
        src_loc_id: Source location ID.
        dest_loc_id: Destination location ID.
        old_prefix: Source rel_path prefix for path rewriting.
        new_prefix: Destination rel_path prefix for path rewriting.
        dest_root: Destination location root_path.
        desc_folder_rows: Pre-fetched descendant folder rows (id, rel_path).

    Raises:
        Exception: Re-raised from fs operations after cleaning up the partial
            destination. Source is left untouched on failure.

    Side effects:
        Creates folders and copies files on the destination agent via fs.
        Deletes the source tree on the source agent after all copies succeed.
        Registers/unregisters activity and broadcasts batch_move_progress
        events to the UI.

    Notes:
        Creates the destination folder structure via fs.dir_create, copies every
        file via fs.copy_file (streaming, 1MB chunks, constant memory), then
        deletes the source tree via fs.dir_delete after ALL copies succeed.
        Called only by move_folder() for cross-location moves.
    """
    log = logging.getLogger(__name__)

    # Count total files for progress
    all_folder_ids = [fld["id"]] + [df["id"] for df in desc_folder_rows]
    ph = ",".join("?" for _ in all_folder_ids)
    count_row = await db.execute_fetchall(
        f"SELECT COUNT(*) AS cnt FROM files WHERE folder_id IN ({ph})",
        all_folder_ids,
    )
    total_files = count_row[0]["cnt"] if count_row else 0

    act_name = f"cross-move-{fld['id']}"
    register(act_name, f"Moving {fld['name']}", f"0/{total_files} files")

    try:
        # 1. Create destination root folder
        await fs.dir_create(dest_abs, dest_loc_id)

        # 2. Create subfolder structure (sorted by depth so parents exist first)
        sorted_descs = sorted(desc_folder_rows, key=lambda r: r["rel_path"].count("/"))
        for df in sorted_descs:
            dest_sub_rel = new_prefix + df["rel_path"][len(old_prefix) :]
            dest_sub_abs = os.path.join(dest_root, dest_sub_rel)
            await fs.dir_create(dest_sub_abs, dest_loc_id)

        # 3. Copy every file (skip files missing from disk — catalog-only)
        copied = 0
        skipped = 0
        for fid in all_folder_ids:
            file_rows = await db.execute_fetchall(
                "SELECT id, full_path, rel_path, modified_date FROM files WHERE folder_id = ?",
                (fid,),
            )
            for fr in file_rows:
                dest_file_rel = new_prefix + fr["rel_path"][len(old_prefix) :]
                dest_file_abs = os.path.join(dest_root, dest_file_rel)
                try:
                    await fs.copy_file(
                        fr["full_path"],
                        src_loc_id,
                        dest_file_abs,
                        dest_loc_id,
                        mtime=parse_mtime(fr["modified_date"]),
                    )
                    copied += 1
                except RuntimeError as e:
                    if "404" in str(e):
                        log.warning("Skipping missing file: %s", fr["full_path"])
                        skipped += 1
                    else:
                        raise
                update(act_name, progress=f"{copied}/{total_files} files")
                await broadcast(
                    {
                        "type": "batch_move_progress",
                        "done": copied + skipped,
                        "total": total_files,
                        "name": fld["name"],
                    }
                )

        # 4. All copies succeeded — delete source tree
        await fs.dir_delete(src_abs, src_loc_id)
        log.info(
            "Cross-location dir move complete: %s -> %s (%d files, %d skipped)",
            src_abs,
            dest_abs,
            copied,
            skipped,
        )

    except Exception:
        # Clean up partial destination, leave source untouched
        try:
            await fs.dir_delete(dest_abs, dest_loc_id)
        except Exception:
            pass  # best-effort cleanup
        raise
    finally:
        unregister(act_name)
        await broadcast({"type": "status_bar_idle"})


async def move_folder(
    db, folder_id: int, destination_parent_id: str = None, *, new_name: str = None
):
    """Move and/or rename a folder (and its entire subtree) on disk and in the catalog.

    Args:
        db: Write-capable DB connection (inside db_writer context).
        folder_id: Numeric ID of the folder to move/rename.
        destination_parent_id: Target parent as prefixed string ("loc-N" or
            "fld-N"), or None for rename-only.
        new_name: New folder name, or None to keep the current name.

    Returns:
        dict: Result with id, name, old_name, renamed (bool), moved (bool).

    Raises:
        ValueError: On invalid inputs, offline locations, collisions, or
            cycle detection (moving a folder into its own descendant).

    Side effects:
        Moves/copies files on disk via fs operations. Updates folders and files
        tables (rel_path, full_path, location_id, parent_id). Commits the
        transaction. Triggers post_op_stats for size recalculation and stats
        cache invalidation.

    Notes:
        Cross-location moves are handled by _cross_location_dir_move (copy
        then delete, with rollback on failure). Called from the move-folder
        HTTP endpoint.
    """
    if not destination_parent_id and not new_name:
        raise ValueError("Provide destination_parent_id and/or new_name.")

    # Fetch folder record + location info
    row = await db.execute_fetchall(
        """SELECT f.id, f.name, f.rel_path, f.parent_id, f.location_id,
                  l.root_path
           FROM folders f JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (folder_id,),
    )
    if not row:
        raise ValueError("Folder not found.")
    fld = dict(row[0])
    src_root = fld["root_path"]
    src_loc_id = fld["location_id"]

    effective_name = new_name if new_name else fld["name"]

    # Source location must be online
    if not await fs.dir_exists(src_root, src_loc_id):
        raise ValueError("Source location is offline.")

    # Resolve destination parent
    if destination_parent_id is None:
        # Rename only — stay in current parent
        dest_loc_id = src_loc_id
        dest_root = src_root
        dest_parent_fld_id = fld["parent_id"]
        if dest_parent_fld_id:
            parent_row = await db.execute_fetchall(
                "SELECT rel_path FROM folders WHERE id = ?", (dest_parent_fld_id,)
            )
            dest_parent_rel = parent_row[0]["rel_path"] if parent_row else ""
        else:
            dest_parent_rel = ""
    elif str(destination_parent_id).startswith("loc-"):
        dest_loc_id = parse_location_id(destination_parent_id)
        dest_row = await db.execute_fetchall(
            "SELECT id, root_path FROM locations WHERE id = ?", (dest_loc_id,)
        )
        if not dest_row:
            raise ValueError("Destination location not found.")
        dest_root = dest_row[0]["root_path"]
        dest_parent_fld_id = None
        dest_parent_rel = ""
    elif str(destination_parent_id).startswith("fld-"):
        dest_fld_id = parse_folder_id(destination_parent_id)

        # Cycle check: destination must not be the folder itself or any descendant
        if dest_fld_id == folder_id:
            raise ValueError("Cannot move a folder into itself.")
        desc_rows = await db.execute_fetchall(
            """WITH RECURSIVE desc(id) AS (
                   SELECT ? UNION ALL
                   SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
               )
               SELECT id FROM desc""",
            (folder_id,),
        )
        desc_ids = {r["id"] for r in desc_rows}
        if dest_fld_id in desc_ids:
            raise ValueError("Cannot move a folder into one of its descendants.")

        dest_row = await db.execute_fetchall(
            """SELECT f.id, f.location_id, f.rel_path, l.root_path
               FROM folders f JOIN locations l ON l.id = f.location_id
               WHERE f.id = ?""",
            (dest_fld_id,),
        )
        if not dest_row:
            raise ValueError("Destination folder not found.")
        dest_loc_id = dest_row[0]["location_id"]
        dest_root = dest_row[0]["root_path"]
        dest_parent_fld_id = dest_fld_id
        dest_parent_rel = dest_row[0]["rel_path"]
    else:
        raise ValueError("Invalid destination_parent_id.")

    # Cross-location: check destination location is online
    cross_location = dest_loc_id != src_loc_id
    if cross_location or dest_root != src_root:
        if not await fs.dir_exists(dest_root, dest_loc_id):
            raise ValueError("Destination location is offline.")

    # Build source and destination absolute paths
    src_abs = os.path.join(src_root, fld["rel_path"])
    new_rel = (
        os.path.join(dest_parent_rel, effective_name)
        if dest_parent_rel
        else effective_name
    )
    dest_abs = os.path.join(dest_root, new_rel)

    # No-op check
    if src_abs == dest_abs:
        raise ValueError("Source and destination are the same.")

    # Check collision on disk
    if await fs.path_exists(dest_abs, dest_loc_id):
        raise ValueError("A folder with that name already exists at the destination.")

    # Check collision in DB
    existing = await db.execute_fetchall(
        "SELECT id FROM folders WHERE location_id = ? AND parent_id IS ? AND name = ?",
        (dest_loc_id, dest_parent_fld_id, effective_name),
    )
    if existing:
        raise ValueError("A folder with that name already exists in the catalog.")

    # Compute old/new rel_path prefix for batch updates
    old_prefix = fld["rel_path"]
    new_prefix = new_rel

    # Get all descendant folder IDs BEFORE disk operation (needed by both
    # cross-location copy and DB updates below)
    desc_folder_rows = await db.execute_fetchall(
        """WITH RECURSIVE desc(id) AS (
               SELECT f.id FROM folders f WHERE f.parent_id = ?
               UNION ALL
               SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
           )
           SELECT d.id, fo.rel_path FROM desc d JOIN folders fo ON fo.id = d.id""",
        (folder_id,),
    )

    # Move on disk
    if cross_location:
        # Check if both locations are on the same agent
        agent_rows = await db.execute_fetchall(
            "SELECT id, agent_id FROM locations WHERE id IN (?, ?)",
            (src_loc_id, dest_loc_id),
        )
        agent_map = {r["id"]: r["agent_id"] for r in agent_rows}
        src_agent = agent_map.get(src_loc_id)
        dst_agent = agent_map.get(dest_loc_id)
        same_agent = src_agent is not None and src_agent == dst_agent

        if same_agent:
            # Same agent — direct move via agent filesystem
            await fs.dir_move(src_abs, dest_abs, src_loc_id)
        else:
            # Different agents — stream through server
            await _cross_location_dir_move(
                db,
                fld,
                src_abs,
                dest_abs,
                src_loc_id,
                dest_loc_id,
                old_prefix,
                new_prefix,
                dest_root,
                desc_folder_rows,
            )
    else:
        await fs.dir_move(src_abs, dest_abs, src_loc_id)

    # Update moved folder
    new_hidden = 1 if effective_name.startswith(".") else 0
    await db.execute(
        "UPDATE folders SET parent_id = ?, name = ?, rel_path = ?, location_id = ?, hidden = ? WHERE id = ?",
        (
            dest_parent_fld_id,
            effective_name,
            new_rel,
            dest_loc_id,
            new_hidden,
            folder_id,
        ),
    )

    # Update descendant folders: replace rel_path prefix
    for df in desc_folder_rows:
        new_desc_rel = new_prefix + df["rel_path"][len(old_prefix) :]
        params = (
            [new_desc_rel, dest_loc_id, df["id"]]
            if cross_location
            else [new_desc_rel, src_loc_id, df["id"]]
        )
        await db.execute(
            "UPDATE folders SET rel_path = ?, location_id = ? WHERE id = ?",
            params,
        )

    # Update all files in folder + descendants
    all_folder_ids = [folder_id] + [df["id"] for df in desc_folder_rows]
    for fid in all_folder_ids:
        file_rows = await db.execute_fetchall(
            "SELECT id, full_path, rel_path FROM files WHERE folder_id = ?",
            (fid,),
        )
        for fr in file_rows:
            new_file_rel = new_prefix + fr["rel_path"][len(old_prefix) :]
            new_full_path = os.path.join(dest_root, new_file_rel)
            update_params = [new_full_path, new_file_rel]
            if cross_location:
                await db.execute(
                    "UPDATE files SET full_path = ?, rel_path = ?, location_id = ? WHERE id = ?",
                    update_params + [dest_loc_id, fr["id"]],
                )
            else:
                await db.execute(
                    "UPDATE files SET full_path = ?, rel_path = ? WHERE id = ?",
                    update_params + [fr["id"]],
                )

    await db.commit()

    if cross_location:
        await post_op_stats(
            location_ids={src_loc_id, dest_loc_id}, source="move_folder"
        )
    else:
        await post_op_stats(location_ids={src_loc_id}, source="move_folder")

    renamed = effective_name != fld["name"]
    moved = destination_parent_id is not None
    return {
        "id": folder_id,
        "name": effective_name,
        "old_name": fld["name"],
        "renamed": renamed,
        "moved": moved,
    }
