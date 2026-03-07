"""In-memory scan queue with per-executor concurrency.

Each executor (each connected agent) can run one scan at a time.
Multiple agents can scan concurrently. Pending entries for a busy agent
wait until it finishes, while entries for idle agents start immediately.

All scanning is agent-based — the server does not scan filesystems directly.

Pending entries are persisted to the settings table so the queue survives
server restarts.
"""

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone

from file_hunter.db import get_db, execute_write
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# Pending scan entries awaiting execution
_queue: deque[dict] = deque()

# Monotonic ID so the frontend can address specific pending items
_next_queue_id: int = 1

# Prevents overlapping _drain_queue coroutines
_draining: bool = False

# executor_key -> location_id for currently running scans
# "agent:{agent_id}" for all scans
_running: dict[str, int] = {}

# Prevents _persist_queue during shutdown (preserves queue for restart)
_shutting_down: bool = False

# executor_key -> asyncio.Event, signalled when an agent scan completes
_agent_scan_events: dict[str, asyncio.Event] = {}

# Cache: location_id -> executor_key (populated on demand from DB)
_executor_cache: dict[int, str] = {}


async def _get_executor_key(location_id: int) -> str:
    """Return the executor key for a location: 'agent:{agent_id}'.

    Results are cached in memory for the lifetime of the process.
    """
    cached = _executor_cache.get(location_id)
    if cached is not None:
        return cached

    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
    )
    if row and row[0]["agent_id"]:
        key = f"agent:{row[0]['agent_id']}"
    else:
        # Location not yet assigned to an agent — use a placeholder.
        # The scan will be deferred until the agent connects and syncs.
        key = "unassigned"
    _executor_cache[location_id] = key
    return key


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


def _is_running(location_id: int) -> bool:
    return location_id in _running.values()


def get_queue_state() -> dict:
    """Return serialisable queue state for API / WS."""
    return {
        "running_location_ids": list(_running.values()),
        "running_location_id": next(iter(_running.values()), None),
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

    Returns the queue entry dict. Raises ValueError if already queued or
    already running.
    """
    if _is_running(location_id):
        raise ValueError(f"Scan already running for '{name}'.")
    if _is_queued(location_id):
        raise ValueError(f"'{name}' is already queued for scanning.")

    entry = _make_entry(location_id, name, root_path, scan_path, folder_name)
    _queue.append(entry)

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

    # Kick the pump — will start immediately if executor is idle
    asyncio.ensure_future(_drain_queue())

    return entry


async def dequeue(queue_id: int) -> dict | None:
    """Remove a pending item by queue_id. Returns the removed entry or None."""
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
    """Start scans for all pending items whose executor is idle.

    Only one instance runs at a time (guarded by _draining). After a scan
    finishes, the completion handler calls _drain_queue again.
    """
    global _draining
    if _draining:
        return
    _draining = True

    try:
        await asyncio.sleep(0.05)

        # Single pass: try to start items whose executor is free
        items = list(_queue)
        _queue.clear()
        deferred = deque()

        for entry in items:
            executor = await _get_executor_key(entry["location_id"])

            if executor in _running or executor == "unassigned":
                deferred.append(entry)
                continue

            result = await _try_launch(entry, executor)
            if result == "deferred":
                deferred.append(entry)

        _queue.extend(deferred)
    finally:
        _draining = False
        await _persist_queue()
        await broadcast({"type": "scan_queue_updated", "queue": get_queue_state()})


async def _try_launch(entry: dict, executor: str) -> str:
    """Attempt to launch a scan for *entry*.

    Returns:
        "started"  — scan launched successfully
        "deferred" — agent temporarily unavailable, try again later
        "skipped"  — permanently failed, entry dropped
    """
    from file_hunter.services.scan_trigger import agent_scan_trigger

    location_id = entry["location_id"]
    scan_path = entry.get("scan_path") or entry["root_path"]

    handled = await agent_scan_trigger(
        location_id,
        entry["name"],
        entry["root_path"],
        scan_path,
    )
    if not handled:
        return "deferred"

    _running[executor] = location_id
    await broadcast({"type": "scan_queue_updated", "queue": get_queue_state()})
    asyncio.create_task(_run_queued_agent_scan(entry, executor))
    return "started"


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


async def _run_queued_agent_scan(entry: dict, executor: str):
    """Track a running agent scan and wait for completion signal."""
    event = asyncio.Event()
    _agent_scan_events[executor] = event
    try:
        await event.wait()
    finally:
        _agent_scan_events.pop(executor, None)
        _running.pop(executor, None)
        await _drain_queue()


def notify_agent_scan_complete(location_id: int):
    """Called by the agent WS handler when a scan finishes (any outcome)."""
    target_executor = None
    for executor, running_loc in _running.items():
        if running_loc == location_id:
            target_executor = executor
            break

    if target_executor:
        event = _agent_scan_events.get(target_executor)
        if event:
            event.set()
        else:
            _running.pop(target_executor, None)
            if _queue:
                asyncio.ensure_future(_drain_queue())
    elif _queue:
        asyncio.ensure_future(_drain_queue())


async def retry_queue():
    """Kick the queue drain — call when an agent comes online."""
    if _queue:
        await _drain_queue()


async def clear_queue():
    """Persist pending items then drop them (used during shutdown)."""
    global _shutting_down
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

    # Recover scans that were running or interrupted when the server stopped.
    # Agent scans survive server restarts (the agent keeps scanning), so
    # we only re-queue scans for locations whose agent is not currently
    # scanning. The agent WS handler will finalize ongoing scans when
    # it reconnects.
    interrupted = await db.execute_fetchall(
        "SELECT s.id, s.location_id, l.name, l.root_path, l.agent_id "
        "FROM scans s JOIN locations l ON l.id = s.location_id "
        "WHERE s.status IN ('running', 'interrupted')"
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

        # Mark all interrupted scans as error — the agent will handle its own
        # lifecycle when it reconnects
        scan_ids = [r["id"] for r in interrupted]
        if scan_ids:
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
                "Re-queued %d interrupted scan(s) from previous session",
                requeued,
            )

    if _queue:
        asyncio.ensure_future(_drain_queue())
