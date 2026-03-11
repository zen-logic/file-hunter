import json
from starlette.websockets import WebSocket, WebSocketDisconnect

clients: set[WebSocket] = set()

# Tracked activity states for late-joining / reconnecting browsers.
# Each key holds the most recent broadcast message of that type, or None.
_current_scan_state: dict | None = None
_current_finalizing_state: dict | None = None
_current_backfill_state: dict | None = None


async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        # Send current activity state to newly connected client
        for state in (
            _current_scan_state,
            _current_finalizing_state,
            _current_backfill_state,
        ):
            if state is not None:
                await websocket.send_text(json.dumps(state))

        # Send queue state if items are pending
        from file_hunter.services.queue_manager import get_queue_status

        try:
            queue_items = await get_queue_status()
            if queue_items:
                # Build format the frontend expects
                running_ids = [
                    i.get("location_id")
                    for i in queue_items
                    if i.get("status") == "running" and i.get("location_id")
                ]
                pending = [
                    {
                        "queue_id": i["id"],
                        "location_id": i.get("location_id"),
                        "name": i.get("location_name", ""),
                        "queued_at": i.get("created_at", ""),
                    }
                    for i in queue_items
                    if i.get("status") == "pending"
                ]
                if running_ids or pending:
                    await websocket.send_text(
                        json.dumps(
                            {
                                "type": "scan_queue_updated",
                                "queue": {
                                    "running_location_ids": running_ids,
                                    "running_location_id": running_ids[0]
                                    if running_ids
                                    else None,
                                    "pending": pending,
                                },
                            }
                        )
                    )
        except Exception:
            pass
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        clients.discard(websocket)


async def broadcast(message: dict):
    global _current_scan_state, _current_finalizing_state, _current_backfill_state

    # Track active states for late-joining clients
    msg_type = message.get("type", "")

    if msg_type in ("scan_started", "scan_progress"):
        _current_scan_state = message
    elif msg_type == "scan_finalizing":
        _current_scan_state = None
        _current_finalizing_state = message
    elif msg_type in (
        "scan_completed",
        "scan_cancelled",
        "scan_error",
        "scan_interrupted",
    ):
        _current_scan_state = None
        _current_finalizing_state = None

    if msg_type in ("backfill_started", "backfill_progress"):
        _current_backfill_state = message
    elif msg_type == "backfill_completed":
        _current_backfill_state = None

    data = json.dumps(message)
    disconnected = []
    for ws in list(clients):
        try:
            await ws.send_text(data)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        clients.discard(ws)
