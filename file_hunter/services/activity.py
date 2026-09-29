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

activities: dict[str, dict] = {}
broadcast_task: asyncio.Task | None = None

# Labels for queue operation types
OP_LABELS = {
    "scan_dir": "Scanning:",
    "backfill_location": "Hashing:",
    "rehash_partial": "Re-hashing:",
    "hash_file": "Hashing file:",
    "transcode": "Converting:",
    "similarity_scan": "Embedding:",
    "embed_file": "Embedding:",
    "extract_markdown": "Extracting:",
}


def register(name: str, label: str, progress: str | None = None):
    """Register an active background operation."""
    activities[name] = {
        "label": label,
        "started_at": time.monotonic(),
        "progress": progress,
    }
    ensure_broadcaster()


def unregister(name: str):
    """Remove a completed background operation."""
    activities.pop(name, None)


def update(name: str, label: str | None = None, progress: str | None = None):
    """Update progress or label for an active operation."""
    if name not in activities:
        return
    if label is not None:
        activities[name]["label"] = label
    if progress is not None:
        activities[name]["progress"] = progress


def get_all() -> list[dict]:
    """Return all active operations."""
    return [
        {"name": k, "label": v["label"], "progress": v.get("progress")}
        for k, v in activities.items()
    ]


def count() -> int:
    return len(activities)


def op_label(op_type: str, location_name: str = "") -> str:
    """Build a human-readable label for a queue operation."""
    prefix = OP_LABELS.get(op_type, op_type)
    return f"{prefix} {location_name}".strip() if location_name else prefix


def ensure_broadcaster():
    """Start the periodic broadcaster if not already running."""
    global broadcast_task
    if broadcast_task is None or broadcast_task.done():
        try:
            broadcast_task = asyncio.create_task(broadcaster())
        except RuntimeError:
            pass  # no event loop yet


async def broadcaster():
    """Broadcast activity state every 2 seconds while activities exist."""
    while activities:
        await broadcast(
            {
                "type": "server_activity",
                "activities": get_all(),
                "count": len(activities),
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
