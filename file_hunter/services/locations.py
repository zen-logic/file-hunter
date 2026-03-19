"""Location CRUD and tree building."""

import asyncio
import os
from datetime import datetime


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
    """Build the full tree structure (runs in thread to avoid blocking)."""
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
    from file_hunter.services.settings import get_setting

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"

    locations = await db.execute_fetchall(
        "SELECT id, name, root_path, date_added, date_last_scanned, total_size "
        "FROM locations WHERE name NOT LIKE '__deleting_%' "
        "ORDER BY name COLLATE NOCASE"
    )

    # Root-level folders (parent_id IS NULL) with has_children flag and stored size
    root_folders = await db.execute_fetchall(
        f"""SELECT f.id, f.location_id, f.name, f.total_size, f.hidden, f.dup_exclude,
                  EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
           FROM folders f
           WHERE f.parent_id IS NULL{hidden_filter}
           ORDER BY f.name"""
    )

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

    from file_hunter.extensions import get_agent_location_ids, get_agent_label_prefixes

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
                "totalSize": f["total_size"],
                "children": None,  # not loaded sentinel
            }
            if f["hidden"]:
                child_node["hidden"] = True
            if f["dup_exclude"]:
                child_node["dupExcluded"] = True
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
            "totalSize": loc["total_size"],
            "diskStats": ds,
            "children": children,
        }
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

    from file_hunter.services.settings import get_setting

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"

    placeholders = ",".join("?" * len(folder_ids))
    rows = await db.execute_fetchall(
        f"""SELECT f.id, f.parent_id, f.name, f.total_size, f.hidden, f.dup_exclude,
                   EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
            FROM folders f
            WHERE f.parent_id IN ({placeholders}){hidden_filter}
            ORDER BY f.name""",
        folder_ids,
    )

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
            "totalSize": r["total_size"],
            "children": None,
        }
        if r["hidden"]:
            child_node["hidden"] = True
        if r["dup_exclude"]:
            child_node["dupExcluded"] = True
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
    from file_hunter.services.settings import get_setting

    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    hidden_filter = "" if show_hidden else " AND f.hidden = 0"
    child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"

    children_by_parent = {}

    if parent_ids_to_fetch:
        placeholders = ",".join("?" * len(parent_ids_to_fetch))
        rows = await db.execute_fetchall(
            f"""SELECT f.id, f.parent_id, f.name, f.total_size, f.hidden, f.dup_exclude,
                       EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
                FROM folders f
                WHERE f.parent_id IN ({placeholders}){hidden_filter}
                ORDER BY f.name""",
            parent_ids_to_fetch,
        )

        for r in rows:
            key = f"fld-{r['parent_id']}"
            if key not in children_by_parent:
                children_by_parent[key] = []
            child_node = {
                "id": f"fld-{r['id']}",
                "type": "folder",
                "label": r["name"],
                "hasChildren": bool(r["has_children"]),
                "totalSize": r["total_size"],
                "children": None,
            }
            if r["hidden"]:
                child_node["hidden"] = True
            if r["dup_exclude"]:
                child_node["dupExcluded"] = True
            children_by_parent[key].append(child_node)

    # Also fetch root-level siblings (children of the location)
    root_rows = await db.execute_fetchall(
        f"""SELECT f.id, f.name, f.total_size, f.hidden, f.dup_exclude,
                  EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
           FROM folders f
           WHERE f.location_id = ? AND f.parent_id IS NULL{hidden_filter}
           ORDER BY f.name""",
        (location_id,),
    )

    loc_key = f"loc-{location_id}"
    children_by_parent[loc_key] = []
    for r in root_rows:
        child_node = {
            "id": f"fld-{r['id']}",
            "type": "folder",
            "label": r["name"],
            "hasChildren": bool(r["has_children"]),
            "totalSize": r["total_size"],
            "children": None,
        }
        if r["hidden"]:
            child_node["hidden"] = True
        if r["dup_exclude"]:
            child_node["dupExcluded"] = True
        children_by_parent[loc_key].append(child_node)

    return {
        "locationId": loc_key,
        "path": path,
        "childrenByParent": children_by_parent,
    }


def check_location_online(location_id: int, root_path: str) -> bool:
    """Check if a location is online via agent status (runs in thread)."""
    from file_hunter.services.online_check import agent_online_check

    return agent_online_check({"id": location_id, "root_path": root_path})


async def get_disk_stats(location_id: int, root_path: str) -> dict | None:
    """Return disk stats for a location via its agent.

    Returns {mount, total, free, readonly} or {mount: false}, or None on error.
    """
    from file_hunter.services.online_check import agent_disk_stats

    try:
        return await agent_disk_stats(location_id, root_path)
    except Exception:
        return None


def _check_paths_exist(locations: list) -> list[bool]:
    """Check online status for a list of location dicts (runs in thread).

    Each location must have at least 'id' and 'root_path'.
    All locations are agent-backed — delegates to agent_online_check.
    """
    from file_hunter.services.online_check import agent_online_check

    return [agent_online_check(loc) for loc in locations]


def _build_folder_tree(folders, parent_id):
    """Build nested tree from flat folder list — O(n) via parent_id index."""
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

    parent_id is 'loc-N' or 'fld-N'.
    """
    from file_hunter.services import fs

    # Resolve parent to get location info and path
    if parent_id.startswith("loc-"):
        loc_id = int(parent_id[4:])
        row = await db.execute_fetchall(
            "SELECT id, root_path FROM locations WHERE id = ?", (loc_id,)
        )
        if not row:
            raise ValueError("Location not found.")
        root_path = row[0]["root_path"]
        parent_fld_id = None
        parent_rel = ""
    elif parent_id.startswith("fld-"):
        fld_id = int(parent_id[4:])
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
    now = datetime.now().isoformat(timespec="seconds")
    cursor = await db.execute(
        "INSERT INTO locations (name, root_path, agent_id, date_added) VALUES (?, ?, ?, ?)",
        (name, root_path, agent_id, now),
    )
    await db.commit()
    loc_id = cursor.lastrowid
    online = await asyncio.to_thread(check_location_online, loc_id, root_path)
    return {
        "id": f"loc-{loc_id}",
        "type": "location",
        "label": name,
        "online": online,
        "children": [],
    }


async def rename_location(db, loc_id: int, new_name: str) -> dict | None:
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
    """Update the scan schedule columns for a location."""
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

    # Fetch child folders
    if parent_folder_id is None:
        children_rows = await db.execute_fetchall(
            """SELECT f.id, f.name, f.total_size,
                      EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id) AS has_children
               FROM folders f
               WHERE f.location_id = ? AND f.parent_id IS NULL
               ORDER BY f.name""",
            (location_id,),
        )
        # Direct files at location root (no folder)
        direct_row = await db.execute_fetchall(
            """SELECT COALESCE(SUM(file_size), 0) AS total, COUNT(*) AS cnt
               FROM files WHERE location_id = ? AND folder_id IS NULL""",
            (location_id,),
        )
    else:
        children_rows = await db.execute_fetchall(
            """SELECT f.id, f.name, f.total_size,
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

    # Build children list
    children = []
    for r in children_rows:
        children.append(
            {
                "id": r["id"],
                "name": r["name"],
                "totalSize": r["total_size"],
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


async def move_folder(db, folder_id: int, destination_parent_id: str):
    """Move a folder (and its entire subtree) on disk and in the catalog."""
    from file_hunter.services import fs

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

    # Source location must be online
    if not await fs.dir_exists(src_root, src_loc_id):
        raise ValueError("Source location is offline.")

    # Resolve destination parent
    if destination_parent_id.startswith("loc-"):
        dest_loc_id = int(destination_parent_id[4:])
        dest_row = await db.execute_fetchall(
            "SELECT id, root_path FROM locations WHERE id = ?", (dest_loc_id,)
        )
        if not dest_row:
            raise ValueError("Destination location not found.")
        dest_root = dest_row[0]["root_path"]
        dest_parent_fld_id = None
        dest_parent_rel = ""
    elif destination_parent_id.startswith("fld-"):
        dest_fld_id = int(destination_parent_id[4:])

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
        os.path.join(dest_parent_rel, fld["name"]) if dest_parent_rel else fld["name"]
    )
    dest_abs = os.path.join(dest_root, new_rel)

    # Check collision on disk
    if await fs.path_exists(dest_abs, dest_loc_id):
        raise ValueError("A folder with that name already exists at the destination.")

    # Check collision in DB
    existing = await db.execute_fetchall(
        "SELECT id FROM folders WHERE location_id = ? AND parent_id IS ? AND name = ?",
        (dest_loc_id, dest_parent_fld_id, fld["name"]),
    )
    if existing:
        raise ValueError("A folder with that name already exists in the catalog.")

    # Move on disk via the source location's agent
    await fs.dir_move(src_abs, dest_abs, src_loc_id)

    # Compute old/new rel_path prefix for batch updates
    old_prefix = fld["rel_path"]
    new_prefix = new_rel

    # Get all descendant folder IDs (excluding the moved folder itself)
    desc_folder_rows = await db.execute_fetchall(
        """WITH RECURSIVE desc(id) AS (
               SELECT f.id FROM folders f WHERE f.parent_id = ?
               UNION ALL
               SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
           )
           SELECT d.id, fo.rel_path FROM desc d JOIN folders fo ON fo.id = d.id""",
        (folder_id,),
    )

    # Update moved folder
    await db.execute(
        "UPDATE folders SET parent_id = ?, rel_path = ?, location_id = ? WHERE id = ?",
        (dest_parent_fld_id, new_rel, dest_loc_id, folder_id),
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

    from file_hunter.services.sizes import schedule_size_recalc

    if cross_location:
        schedule_size_recalc(src_loc_id, dest_loc_id)
    else:
        schedule_size_recalc(src_loc_id)

    return {"id": folder_id, "name": fld["name"], "moved": True}
