"""Online status check for agent-backed locations.

Uses the in-memory agent connection state to determine if an agent-backed
location should be considered online, without touching the filesystem.

A DB-backed cache (_all_agent_loc_ids) is loaded at startup so that agent
locations are correctly identified even before any agent connects.
"""

# Persistent cache of ALL agent-backed location IDs (online or offline).
# Loaded from DB at startup, updated on agent connect/disconnect.
_all_agent_loc_ids: set[int] = set()

# location_id -> agent name (for tree label prefixes)
_agent_label_prefixes: dict[int, str] = {}

# Per-location path availability reported by agents: {agent_id: {location_id: bool}}
_agent_location_path_status: dict[int, dict[int, bool]] = {}


async def load_agent_location_ids():
    """Load agent-backed location IDs from the DB into the cache.

    Called once at startup and whenever an agent connects/disconnects.
    """
    from file_hunter.db import get_db

    db = await get_db()
    cursor = await db.execute(
        """SELECT l.id, a.name AS agent_name
           FROM locations l JOIN agents a ON a.id = l.agent_id
           WHERE l.agent_id IS NOT NULL"""
    )
    rows = await cursor.fetchall()
    _all_agent_loc_ids.clear()
    _agent_label_prefixes.clear()
    for row in rows:
        _all_agent_loc_ids.add(row["id"])
        _agent_label_prefixes[row["id"]] = row["agent_name"]


def refresh_agent_location_ids_from_memory(agent_name: str = "", agent_id: int = 0):
    """Update the cache from in-memory WS state (after _sync_agent_locations)."""
    from file_hunter.ws.agent import get_agent_location_ids

    all_agent_locs = get_agent_location_ids()
    if agent_id and agent_id in all_agent_locs:
        location_ids = all_agent_locs[agent_id]
        _all_agent_loc_ids.update(location_ids)
        if agent_name:
            for loc_id in location_ids:
                _agent_label_prefixes[loc_id] = agent_name
    else:
        for location_ids in all_agent_locs.values():
            _all_agent_loc_ids.update(location_ids)


def update_location_path_status(agent_id: int, status_dict: dict[int, bool]):
    """Update per-location path availability for an agent."""
    _agent_location_path_status[agent_id] = status_dict


def clear_location_path_status(agent_id: int):
    """Clear per-location path status when an agent disconnects."""
    _agent_location_path_status.pop(agent_id, None)


def agent_online_check(loc):
    """Return True if this is an agent-backed location with an online agent.

    Returns None if not an agent location (falls through to os.path.isdir).
    Called from a thread — uses only in-memory lookups, no async.
    """
    from file_hunter.ws.agent import get_agent_location_ids, get_online_agent_ids

    loc_id = loc["id"]

    if loc_id in _all_agent_loc_ids:
        for agent_id, location_ids in get_agent_location_ids().items():
            if loc_id in location_ids:
                if agent_id not in get_online_agent_ids():
                    return False
                path_status = _agent_location_path_status.get(agent_id, {})
                return path_status.get(loc_id, True)
        return False

    return None


async def agent_disk_stats(location_id: int, root_path: str) -> dict | None:
    """Get disk stats from the agent for an agent-backed location."""
    if location_id not in _all_agent_loc_ids:
        return None

    from file_hunter.services.agent_ops import dispatch

    try:
        return await dispatch("disk_stats", location_id, path=root_path)
    except Exception:
        return None


def all_agent_location_ids():
    """Return set of all location IDs that are agent-backed (online or offline)."""
    return set(_all_agent_loc_ids)


def agent_label_prefixes():
    """Return {location_id: agent_name} for tree label prefixes."""
    return dict(_agent_label_prefixes)
