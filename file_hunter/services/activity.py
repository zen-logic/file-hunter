"""Central server activity registry — tracks background operations.

Lightweight in-memory registry. Each subsystem registers when it starts
meaningful work and unregisters when done. A periodic broadcaster pushes
the current state to connected browsers via websocket.
"""

import asyncio
import logging
import time

from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

_activities: dict[str, dict] = {}
_broadcast_task: asyncio.Task | None = None

# Labels for queue operation types
_OP_LABELS = {
    "scan_dir": "Scanning:",
    "backfill_location": "Hashing:",
    "rehash_partial": "Re-hashing:",
    "hash_file": "Hashing file:",
}


def register(name: str, label: str, progress: str | None = None):
    """Register an active background operation."""
    _activities[name] = {
        "label": label,
        "started_at": time.monotonic(),
        "progress": progress,
    }
    _ensure_broadcaster()


def unregister(name: str):
    """Remove a completed background operation."""
    _activities.pop(name, None)


def update(name: str, label: str | None = None, progress: str | None = None):
    """Update progress or label for an active operation."""
    if name not in _activities:
        return
    if label is not None:
        _activities[name]["label"] = label
    if progress is not None:
        _activities[name]["progress"] = progress


def get_all() -> list[dict]:
    """Return all active operations."""
    return [
        {"name": k, "label": v["label"], "progress": v.get("progress")}
        for k, v in _activities.items()
    ]


def count() -> int:
    return len(_activities)


def op_label(op_type: str, location_name: str = "") -> str:
    """Build a human-readable label for a queue operation."""
    prefix = _OP_LABELS.get(op_type, op_type)
    return f"{prefix} {location_name}".strip() if location_name else prefix


def _ensure_broadcaster():
    """Start the periodic broadcaster if not already running."""
    global _broadcast_task
    if _broadcast_task is None or _broadcast_task.done():
        try:
            _broadcast_task = asyncio.create_task(_broadcaster())
        except RuntimeError:
            pass  # no event loop yet


async def _broadcaster():
    """Broadcast activity state every 2 seconds while activities exist."""
    while _activities:
        await broadcast(
            {
                "type": "server_activity",
                "activities": get_all(),
                "count": len(_activities),
            }
        )
        await asyncio.sleep(2)

    # One final broadcast to clear the UI
    await broadcast(
        {
            "type": "server_activity",
            "activities": [],
            "count": 0,
        }
    )
