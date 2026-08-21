"""Server-side transcode orchestration.

Dispatches transcode jobs to agents via the queue manager and waits for
completion over the agent's WebSocket connection. The agent runs ffmpeg
and sends progress/completion messages which the server relays to the UI
and uses to create catalog entries.
"""

import asyncio
import logging

from file_hunter.services.agent_ops import dispatch
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# Pending completions: keyed by source path -> asyncio.Event + result
_pending: dict[str, dict] = {}


def register_pending(path: str):
    """Register that a transcode is in flight for this path."""
    _pending[path] = {"event": asyncio.Event(), "result": None}


async def wait_for_completion(path: str):
    """Block until the agent reports completion/error for this path."""
    entry = _pending.get(path)
    if not entry:
        return None
    try:
        await entry["event"].wait()
        return entry["result"]
    finally:
        _pending.pop(path, None)


def resolve_pending(path: str, result: dict):
    """Called from the WebSocket handler when transcode_complete/error arrives."""
    entry = _pending.get(path)
    if entry:
        entry["result"] = result
        entry["event"].set()
    else:
        logger.warning("resolve_pending: no pending entry for %s", path)


async def run_transcode(op_id: int, agent_id: int | None, params: dict):
    """Queue handler — dispatch transcode to agent and wait for completion.

    Called by the queue manager as the 'transcode' operation handler.
    """
    file_id = params["file_id"]
    path = params["path"]
    filename = params.get("filename", "")

    await broadcast({
        "type": "transcode_started",
        "fileId": file_id,
        "filename": filename,
    })

    register_pending(path)

    try:
        await dispatch("transcode", params["location_id"], path=path)
    except (ConnectionError, OSError) as e:
        _pending.pop(path, None)
        await broadcast({
            "type": "transcode_error",
            "path": path,
            "fileId": file_id,
            "error": str(e),
        })
        raise

    try:
        result = await wait_for_completion(path)
    except asyncio.CancelledError:
        # Queue manager cancelled us — clean up pending state
        _pending.pop(path, None)
        raise

    if result and result.get("type") == "transcode_error":
        raise OSError(result.get("error", "Transcode failed"))
