"""In-memory FIFO scan queue — enqueue locations, drain sequentially.

The queue wraps the existing scanner module without modifying it.  When a
scan finishes (success, cancel, or error), the queue automatically starts
the next pending item.  Agent-backed locations are also queued — the queue
triggers the remote scan and waits for the agent to report completion.

Pending entries are persisted to the settings table so the queue survives
server restarts.
"""

import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, timezone

from file_hunter.db import get_db, execute_write, open_connection
from file_hunter.services.scanner import is_scan_running, register_task, run_scan
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# Pending scan entries awaiting execution
_queue: deque[dict] = deque()

# Monotonic ID so the frontend can address specific pending items
_next_queue_id: int = 1

# Prevents overlapping _drain_queue coroutines
_draining: bool = False

# Tracks whether a queue-launched scan is currently running
_running_location_id: int | None = None

# Prevents _persist_queue during shutdown (preserves queue for restart)
_shutting_down: bool = False

# Signalled by notify_agent_scan_complete() when an agent scan finishes
_agent_scan_event: asyncio.Event | None = None


def _make_entry(
    location_id: int,
    name: str,
    root_path: str,
    scan_path: str | None = None,
    folder_name: str | None = None,
) -> dict:
    global _next_queue_id
    entry = {
        "queue_id": _next_queue_id,
        "location_id": location_id,
        "name": name,
        "root_path": root_path,
        "scan_path": scan_path,
        "folder_name": folder_name,
        "queued_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _next_queue_id += 1
    return entry


def _is_queued(location_id: int) -> bool:
    return any(e["location_id"] == location_id for e in _queue)


def get_queue_state() -> dict:
    """Return serialisable queue state for API / WS."""
    return {
        "running_location_id": _running_location_id,
        "pending": [
            {
                "queue_id": e["queue_id"],
                "location_id": e["location_id"],
                "name": e["name"],
                "folder_name": e.get("folder_name"),
                "queued_at": e["queued_at"],
            }
            for e in _queue
        ],
    }


async def enqueue(
    location_id: int,
    name: str,
    root_path: str,
    scan_path: str | None = None,
    folder_name: str | None = None,
) -> dict:
    """Add a location to the scan queue.

    Returns the queue entry dict.  Raises ValueError if already queued or
    already running.  When *scan_path* is set, the scan will only walk
    that subtree instead of the full location.
    """
    if is_scan_running(location_id) or _running_location_id == location_id:
        raise ValueError(f"Scan already running for '{name}'.")
    if _is_queued(location_id):
        raise ValueError(f"'{name}' is already queued for scanning.")

    entry = _make_entry(location_id, name, root_path, scan_path, folder_name)
    _queue.append(entry)

    # Broadcast immediately so badges appear without waiting for DB persist
    await broadcast(
        {
            "type": "scan_queued",
            "entry": {
                "queue_id": entry["queue_id"],
                "location_id": entry["location_id"],
                "name": entry["name"],
                "queued_at": entry["queued_at"],
            },
            "queue": get_queue_state(),
        }
    )

    await _persist_queue()

    # Kick the pump — will start immediately if nothing is running
    asyncio.ensure_future(_drain_queue())

    return entry


async def dequeue(queue_id: int) -> dict | None:
    """Remove a pending item by queue_id.  Returns the removed entry or None."""
    for i, entry in enumerate(_queue):
        if entry["queue_id"] == queue_id:
            _queue.remove(entry)
            await broadcast(
                {
                    "type": "scan_dequeued",
                    "entry": {
                        "queue_id": entry["queue_id"],
                        "location_id": entry["location_id"],
                        "name": entry["name"],
                    },
                    "queue": get_queue_state(),
                }
            )
            await _persist_queue()
            return entry
    return None


async def _drain_queue():
    """Pop the next pending item and launch a scan.

    Only one instance of this coroutine runs at a time (guarded by
    ``_draining``).  After the scan finishes, ``_run_queued_scan`` calls
    ``_drain_queue`` again so the chain continues.
    """
    global _draining, _running_location_id
    if _draining:
        return
    _draining = True

    try:
        # Wait a tiny moment so broadcast messages from the previous scan
        # (completed/cancelled) are delivered before we start the next one.
        await asyncio.sleep(0.05)

        while _queue:
            # Don't start if a queue-launched scan is still running
            if _running_location_id is not None:
                break

            entry = _queue.popleft()

            from file_hunter.extensions import is_agent_location

            if is_agent_location(entry["location_id"]):
                # Agent location — trigger remote scan via extension hook
                from file_hunter.extensions import get_scan_trigger

                scan_trigger = get_scan_trigger()
                if not scan_trigger:
                    await _skip_entry(entry, "Pro extension not loaded")
                    continue

                scan_path = entry.get("scan_path") or entry["root_path"]
                handled = await scan_trigger(
                    entry["location_id"],
                    entry["name"],
                    entry["root_path"],
                    scan_path,
                )
                if not handled:
                    # Agent busy or offline — put entry back and wait
                    _queue.appendleft(entry)
                    break

                # Mark as running BEFORE broadcast so queue state includes it
                _running_location_id = entry["location_id"]

                # Broadcast updated queue state (item removed from pending)
                await broadcast(
                    {"type": "scan_queue_updated", "queue": get_queue_state()}
                )

                # Wait for agent to report completion
                asyncio.create_task(_run_queued_agent_scan(entry))
                break

            # Local location — verify path is still online
            check_path = entry.get("scan_path") or entry["root_path"]
            online = await asyncio.to_thread(os.path.isdir, check_path)
            if not online:
                await _skip_entry(entry, "Location offline")
                continue

            # Mark as running BEFORE broadcast so queue state includes it
            _running_location_id = entry["location_id"]

            # Broadcast updated queue state (item removed from pending)
            await broadcast({"type": "scan_queue_updated", "queue": get_queue_state()})

            # Launch the scan
            scan_db = await open_connection()
            task = asyncio.create_task(_run_queued_scan(scan_db, entry))
            register_task(entry["location_id"], task)
            # Only one scan at a time — wait for it to finish before
            # popping the next entry.
            break
    finally:
        _draining = False
        await _persist_queue()


async def _run_queued_scan(db, entry: dict):
    """Wrapper around run_scan that re-triggers the queue pump on exit."""
    global _running_location_id
    try:
        await run_scan(
            db,
            entry["location_id"],
            entry["name"],
            entry["root_path"],
            scan_path=entry.get("scan_path"),
        )
    finally:
        _running_location_id = None
        # Always try to drain the next item regardless of outcome
        await _drain_queue()


async def _skip_entry(entry: dict, reason: str):
    """Broadcast a skip notification for a queue entry."""
    await broadcast(
        {
            "type": "scan_queue_skipped",
            "entry": {
                "queue_id": entry["queue_id"],
                "location_id": entry["location_id"],
                "name": entry["name"],
            },
            "reason": reason,
            "queue": get_queue_state(),
        }
    )


async def _run_queued_agent_scan(entry: dict):
    """Track a running agent scan and wait for completion signal."""
    global _running_location_id, _agent_scan_event
    _agent_scan_event = asyncio.Event()
    try:
        await _agent_scan_event.wait()
    finally:
        _running_location_id = None
        _agent_scan_event = None
        await _drain_queue()


def notify_agent_scan_complete(location_id: int):
    """Called by the agent WS handler when a scan finishes (any outcome)."""
    global _running_location_id
    if _running_location_id == location_id:
        if _agent_scan_event:
            _agent_scan_event.set()
        else:
            # Safety net: event missing but location matches — force clear
            _running_location_id = None
            if _queue:
                asyncio.ensure_future(_drain_queue())
    elif _queue:
        # Orphaned scan completed (e.g. post-restart) — retry queued items
        asyncio.ensure_future(_drain_queue())


async def retry_queue():
    """Kick the queue drain — call when an agent comes online."""
    if _queue:
        await _drain_queue()


async def clear_queue():
    """Persist pending items then drop them (used during shutdown)."""
    global _shutting_down
    # Persist before clearing so the queue survives restarts
    await _persist_queue()
    _shutting_down = True
    _queue.clear()


# ---------------------------------------------------------------------------
# Persistence — survive server restarts
# ---------------------------------------------------------------------------


async def _persist_queue():
    """Save pending queue entries to the settings table."""
    if _shutting_down:
        return
    entries = [
        {
            "location_id": e["location_id"],
            "name": e["name"],
            "root_path": e["root_path"],
            "scan_path": e.get("scan_path"),
            "folder_name": e.get("folder_name"),
        }
        for e in _queue
    ]

    async def _write(conn, data):
        await conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            ("scan_queue", data),
        )
        await conn.commit()

    await execute_write(_write, json.dumps(entries))


async def restore_queue():
    """Load persisted queue entries on startup and kick the pump."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT value FROM settings WHERE key = ?", ("scan_queue",)
    )

    if rows and rows[0]["value"]:
        try:
            entries = json.loads(rows[0]["value"])
        except (json.JSONDecodeError, TypeError):
            entries = []

        for e in entries:
            entry = _make_entry(
                e["location_id"],
                e["name"],
                e["root_path"],
                e.get("scan_path"),
                e.get("folder_name"),
            )
            _queue.append(entry)

        if _queue:
            logger.info("Restored %d queued scan(s) from previous session", len(_queue))

    # Recover scans that were running when the server stopped
    interrupted = await db.execute_fetchall(
        "SELECT s.id, s.location_id, l.name, l.root_path "
        "FROM scans s JOIN locations l ON l.id = s.location_id "
        "WHERE s.status = 'running'"
    )

    if interrupted:

        async def _mark_error(conn, scan_ids):
            for sid in scan_ids:
                await conn.execute(
                    "UPDATE scans SET status = 'error', error = 'Server restarted' "
                    "WHERE id = ?",
                    (sid,),
                )
            await conn.commit()

        scan_ids = [r["id"] for r in interrupted]
        await execute_write(_mark_error, scan_ids)

        requeued = 0
        for r in interrupted:
            loc_id = r["location_id"]
            if not _is_queued(loc_id):
                entry = _make_entry(loc_id, r["name"], r["root_path"])
                _queue.appendleft(entry)
                requeued += 1

        if requeued:
            logger.info(
                "Re-queued %d interrupted scan(s) from previous session", requeued
            )

    if _queue:
        asyncio.ensure_future(_drain_queue())
