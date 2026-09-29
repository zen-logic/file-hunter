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

extra_routes = []
extra_startup = []
extra_shutdown = []
extra_middlewares = []  # ASGI middleware classes, applied outermost-first
static_dirs = {}  # mount_path -> directory
public_paths = set()  # HTTP paths that bypass auth
public_ws_paths = set()  # WS paths that handle their own auth


def add_routes(routes):
    """Append Starlette Route objects to the app route list."""
    extra_routes.extend(routes)


def add_startup(fn):
    """Register an async callable to run during app startup."""
    extra_startup.append(fn)


def add_shutdown(fn):
    """Register an async callable to run during app shutdown."""
    extra_shutdown.append(fn)


def add_static(path, directory):
    """Register a static file mount (path -> directory)."""
    static_dirs[path] = directory


def get_routes():
    return list(extra_routes)


def get_startup_hooks():
    return list(extra_startup)


def get_shutdown_hooks():
    return list(extra_shutdown)


def get_static_mounts():
    return dict(static_dirs)


def add_middleware(cls):
    """Register an ASGI middleware class to wrap the app.

    Middlewares are applied outside AuthMiddleware so they run first.
    Applied in registration order (first registered = outermost).
    """
    extra_middlewares.append(cls)


def get_middlewares():
    return list(extra_middlewares)


def add_public_path(path):
    """Register an HTTP path that bypasses auth."""
    public_paths.add(path)


def get_public_paths():
    return set(public_paths)


def add_public_ws_path(path):
    """Register a WebSocket path that bypasses auth (handles its own validation)."""
    public_ws_paths.add(path)


def get_public_ws_paths():
    return set(public_ws_paths)


# ---------------------------------------------------------------------------
# Extension hooks — Pro can override; defaults fall back to core
# ---------------------------------------------------------------------------

scan_trigger_fn = None
scan_cancel_fn = None
content_proxy_fn = None
fetch_bytes_fn = None
agent_proxy_fn = None
agent_location_ids_fn = None
agent_label_prefixes_fn = None
agent_scanning_fn = None
disk_stats_fn = None
location_changed_fn = None
agent_status_fn = None


def set_scan_trigger(fn):
    """No-op — kept for backward compatibility with old Pro packages."""
    global scan_trigger_fn
    scan_trigger_fn = fn


def get_scan_trigger():
    return scan_trigger_fn


def set_scan_cancel(fn):
    """No-op — kept for backward compatibility with old Pro packages."""
    global scan_cancel_fn
    scan_cancel_fn = fn


def get_scan_cancel():
    return scan_cancel_fn


def set_content_proxy(fn):
    global content_proxy_fn
    content_proxy_fn = fn


def get_content_proxy():
    if content_proxy_fn:
        return content_proxy_fn
    return proxy_agent_content


def set_fetch_bytes(fn):
    global fetch_bytes_fn
    fetch_bytes_fn = fn


def get_fetch_bytes():
    if fetch_bytes_fn:
        return fetch_bytes_fn
    return fetch_agent_bytes


def set_agent_proxy(fn):
    global agent_proxy_fn
    agent_proxy_fn = fn


def get_agent_proxy():
    if agent_proxy_fn:
        return agent_proxy_fn
    return dispatch


def set_agent_location_ids(fn):
    global agent_location_ids_fn
    agent_location_ids_fn = fn


def get_agent_location_ids():
    if agent_location_ids_fn:
        return agent_location_ids_fn()
    return all_agent_location_ids()


def set_agent_label_prefixes(fn):
    global agent_label_prefixes_fn
    agent_label_prefixes_fn = fn


def get_agent_label_prefixes():
    """Return {location_id: agent_name} for agent-backed locations."""
    if agent_label_prefixes_fn:
        return agent_label_prefixes_fn()
    return agent_label_prefixes()


def set_agent_scanning(fn):
    global agent_scanning_fn
    agent_scanning_fn = fn


def is_agent_scanning(location_id: int) -> bool:
    """Check if an agent is currently scanning this location."""
    if agent_scanning_fn:
        return agent_scanning_fn(location_id)
    return is_location_running(location_id)


def set_disk_stats(fn):
    global disk_stats_fn
    disk_stats_fn = fn


def get_disk_stats():
    if disk_stats_fn:
        return disk_stats_fn
    return agent_disk_stats


def set_location_changed(fn):
    global location_changed_fn
    location_changed_fn = fn


def get_location_changed():
    return location_changed_fn


def set_agent_status(fn):
    global agent_status_fn
    agent_status_fn = fn


async def get_agent_status(location_id: int):
    """Return agent activity status, or None if not available."""
    if agent_status_fn:
        return await agent_status_fn(location_id)
    try:
        return await dispatch("agent_status", location_id)
    except Exception:
        return None
