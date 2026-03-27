"""Extension registry for pro/plugin packages.

The pro package (file_hunter_pro) calls these functions from its register()
entry point to inject routes, static mounts, and startup hooks into the core
app before Starlette is constructed.

Agent infrastructure (WS handler, scan ingest, content proxy, backfill) lives
in core. The extension hooks below allow Pro to override or extend behaviour
(e.g. multi-agent support). When no override is set, the getters fall back to
core implementations.
"""

from file_hunter.services.agent_ops import dispatch
from file_hunter.services.content_proxy import fetch_agent_bytes, proxy_agent_content
from file_hunter.services.online_check import (
    agent_disk_stats,
    agent_label_prefixes,
    all_agent_location_ids,
)
from file_hunter.services.queue_manager import is_location_running

_extra_routes = []
_extra_startup = []
_static_dirs = {}  # mount_path -> directory
_public_ws_paths = set()  # WS paths that handle their own auth


def add_routes(routes):
    """Append Starlette Route objects to the app route list."""
    _extra_routes.extend(routes)


def add_startup(fn):
    """Register an async callable to run during app startup."""
    _extra_startup.append(fn)


def add_static(path, directory):
    """Register a static file mount (path -> directory)."""
    _static_dirs[path] = directory


def get_routes():
    return list(_extra_routes)


def get_startup_hooks():
    return list(_extra_startup)


def get_static_mounts():
    return dict(_static_dirs)


def add_public_ws_path(path):
    """Register a WebSocket path that bypasses auth (handles its own validation)."""
    _public_ws_paths.add(path)


def get_public_ws_paths():
    return set(_public_ws_paths)


# ---------------------------------------------------------------------------
# Extension hooks — Pro can override; defaults fall back to core
# ---------------------------------------------------------------------------

_scan_trigger_fn = None
_scan_cancel_fn = None
_content_proxy_fn = None
_fetch_bytes_fn = None
_agent_proxy_fn = None
_agent_location_ids_fn = None
_agent_label_prefixes_fn = None
_agent_scanning_fn = None
_disk_stats_fn = None
_location_changed_fn = None
_agent_status_fn = None


def set_scan_trigger(fn):
    """No-op — kept for backward compatibility with old Pro packages."""
    global _scan_trigger_fn
    _scan_trigger_fn = fn


def get_scan_trigger():
    return _scan_trigger_fn


def set_scan_cancel(fn):
    """No-op — kept for backward compatibility with old Pro packages."""
    global _scan_cancel_fn
    _scan_cancel_fn = fn


def get_scan_cancel():
    return _scan_cancel_fn


def set_content_proxy(fn):
    global _content_proxy_fn
    _content_proxy_fn = fn


def get_content_proxy():
    if _content_proxy_fn:
        return _content_proxy_fn
    return proxy_agent_content


def set_fetch_bytes(fn):
    global _fetch_bytes_fn
    _fetch_bytes_fn = fn


def get_fetch_bytes():
    if _fetch_bytes_fn:
        return _fetch_bytes_fn
    return fetch_agent_bytes


def set_agent_proxy(fn):
    global _agent_proxy_fn
    _agent_proxy_fn = fn


def get_agent_proxy():
    if _agent_proxy_fn:
        return _agent_proxy_fn
    return dispatch


def set_agent_location_ids(fn):
    global _agent_location_ids_fn
    _agent_location_ids_fn = fn


def get_agent_location_ids():
    if _agent_location_ids_fn:
        return _agent_location_ids_fn()
    return all_agent_location_ids()


def set_agent_label_prefixes(fn):
    global _agent_label_prefixes_fn
    _agent_label_prefixes_fn = fn


def get_agent_label_prefixes():
    """Return {location_id: agent_name} for agent-backed locations."""
    if _agent_label_prefixes_fn:
        return _agent_label_prefixes_fn()
    return agent_label_prefixes()


def set_agent_scanning(fn):
    global _agent_scanning_fn
    _agent_scanning_fn = fn


def is_agent_scanning(location_id: int) -> bool:
    """Check if an agent is currently scanning this location."""
    if _agent_scanning_fn:
        return _agent_scanning_fn(location_id)
    return is_location_running(location_id)


def set_disk_stats(fn):
    global _disk_stats_fn
    _disk_stats_fn = fn


def get_disk_stats():
    if _disk_stats_fn:
        return _disk_stats_fn
    return agent_disk_stats


def set_location_changed(fn):
    global _location_changed_fn
    _location_changed_fn = fn


def get_location_changed():
    return _location_changed_fn


def set_agent_status(fn):
    global _agent_status_fn
    _agent_status_fn = fn


async def get_agent_status(location_id: int):
    """Return agent activity status, or None if not available."""
    if _agent_status_fn:
        return await _agent_status_fn(location_id)
    try:
        return await dispatch("agent_status", location_id)
    except Exception:
        return None
