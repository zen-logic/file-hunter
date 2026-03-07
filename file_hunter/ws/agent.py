"""WebSocket handler for agent connections.

Agents connect here to register, send scan results, and receive commands.
Each agent maintains one persistent WebSocket connection.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from urllib.parse import parse_qs

from starlette.websockets import WebSocket, WebSocketDisconnect

from file_hunter.db import get_db, execute_write
from file_hunter.services.auth import verify_password
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast
from file_hunter.services import scan_ingest

logger = logging.getLogger("file_hunter")

# In-memory state for connected agents
_agent_connections: dict[int, WebSocket] = {}  # agent_id -> WebSocket
_agent_tokens: dict[int, str] = {}  # agent_id -> raw token (for HTTP calls)
_agent_info: dict[int, dict] = {}  # agent_id -> {hostname, httpPort, httpHost, os}

# agent_id -> set of location_ids (for online check lookups)
_agent_location_ids: dict[int, set[int]] = {}


def get_agent_connection(agent_id: int) -> WebSocket | None:
    return _agent_connections.get(agent_id)


def get_agent_token(agent_id: int) -> str | None:
    return _agent_tokens.get(agent_id)


def get_agent_info(agent_id: int) -> dict | None:
    return _agent_info.get(agent_id)


def get_online_agent_ids() -> list[int]:
    return list(_agent_connections.keys())


def get_agent_location_ids() -> dict[int, set[int]]:
    """Return the agent_id -> location_ids mapping for all connected agents."""
    return dict(_agent_location_ids)


async def send_to_agent(agent_id: int, msg: dict) -> bool:
    """Send a message to a connected agent. Returns False if not connected."""
    ws = _agent_connections.get(agent_id)
    if not ws:
        return False
    try:
        await ws.send_text(json.dumps(msg))
        logger.info(
            "send_to_agent: sent '%s' to agent #%d", msg.get("type", "?"), agent_id
        )
        return True
    except Exception as e:
        logger.warning("send_to_agent: failed to send to agent #%d: %s", agent_id, e)
        return False


async def _adopt_orphaned_locations(agent_id: int):
    """Find locations with agent_id IS NULL, assign them to this agent, and push to agent config.

    This handles existing local locations that predate the agent-only scanning model.
    Only the file data (files, folders) is preserved — no rescanning needed.
    """
    from file_hunter.services.agent_ops import _resolve_agent

    db = await get_db()
    orphans = await db.execute_fetchall(
        "SELECT id, name, root_path FROM locations WHERE agent_id IS NULL"
    )
    if not orphans:
        return

    logger.info(
        "Agent #%d: adopting %d orphaned locations", agent_id, len(orphans)
    )

    # Update DB: set agent_id on all orphaned locations
    async def _assign(conn, aid, ids):
        placeholders = ",".join("?" * len(ids))
        await conn.execute(
            f"UPDATE locations SET agent_id = ? WHERE id IN ({placeholders})",
            [aid] + ids,
        )
        await conn.commit()

    orphan_ids = [o["id"] for o in orphans]
    await execute_write(_assign, agent_id, orphan_ids)

    # Push each path to the agent's config via HTTP /locations/add
    resolved = _resolve_agent(agent_id)
    if not resolved:
        logger.warning(
            "Agent #%d not reachable via HTTP — orphaned locations assigned in DB "
            "but not pushed to agent config. They will sync on next connect.",
            agent_id,
        )
        return

    host, port, token = resolved
    for o in orphans:
        try:
            from file_hunter.services.agent_ops import _post

            await _post(
                host, port, token, "/locations/add",
                {"name": o["name"], "path": o["root_path"]},
            )
            logger.info(
                "Agent #%d: pushed orphaned location '%s' (%s) to agent config",
                agent_id, o["name"], o["root_path"],
            )
        except Exception as e:
            logger.warning(
                "Agent #%d: failed to push location '%s' to agent: %s",
                agent_id, o["name"], e,
            )

    # Add orphan location IDs to the in-memory set
    if agent_id in _agent_location_ids:
        _agent_location_ids[agent_id].update(orphan_ids)
    else:
        _agent_location_ids[agent_id] = set(orphan_ids)

    invalidate_stats_cache()


async def _sync_agent_locations(agent_id: int, agent_locations: list[dict]):
    """Create or update location records for an agent's configured locations."""
    location_ids = set()

    async def _do_sync(conn, aid, locs):
        nonlocal location_ids
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")

        for loc in locs:
            name = loc.get("name", "")
            path = loc.get("path", "")
            if not name or not path:
                continue

            row = await conn.execute(
                "SELECT id, name FROM locations WHERE agent_id = ? AND root_path = ?",
                (aid, path),
            )
            existing = await row.fetchone()

            if existing:
                loc_id = existing["id"]
                if existing["name"] != name:
                    await conn.execute(
                        "UPDATE locations SET name = ? WHERE id = ?",
                        (name, loc_id),
                    )
            else:
                cursor = await conn.execute(
                    "INSERT INTO locations (name, root_path, agent_id, date_added, total_size) "
                    "VALUES (?, ?, ?, ?, 0)",
                    (name, path, aid, now),
                )
                loc_id = cursor.lastrowid

            location_ids.add(loc_id)

        await conn.commit()

    await execute_write(_do_sync, agent_id, agent_locations)
    _agent_location_ids[agent_id] = location_ids
    invalidate_stats_cache()

    # Build per-location path status from agent-reported online field
    from file_hunter.services.online_check import update_location_path_status

    path_status: dict[int, bool] = {}
    db = await get_db()
    for loc_id in location_ids:
        cursor = await db.execute(
            "SELECT root_path FROM locations WHERE id = ?", (loc_id,)
        )
        row = await cursor.fetchone()
        if row:
            for loc in agent_locations:
                if loc.get("path") == row["root_path"]:
                    path_status[loc_id] = loc.get("online", True)
                    break
            else:
                path_status[loc_id] = True
    update_location_path_status(agent_id, path_status)

    return location_ids


async def _finalize_scan(
    agent_id: int,
    scan_path: str,
    loc_info: tuple[int, str] | None,
    msg: dict,
):
    """Background task: run complete_session, broadcast, and launch backfill."""
    try:
        incremental = msg.get("incremental", False)
        deleted = msg.get("deleted", None)
        files_ingested = await scan_ingest.complete_session(
            agent_id, scan_path, incremental=incremental, deleted=deleted
        )

        msg.pop("deleted", None)  # Don't broadcast large delete lists to UI
        msg["agentId"] = agent_id
        if loc_info:
            msg["locationId"] = loc_info[0]
            msg["location"] = loc_info[1]
        msg.setdefault("duplicatesFound", 0)
        msg.setdefault("staleFiles", 0)
        msg.setdefault("filesSkipped", 0)
        await broadcast(msg)

        if loc_info:
            from file_hunter.services.scan_queue import notify_agent_scan_complete

            notify_agent_scan_complete(loc_info[0])

        if loc_info and files_ingested > 0:
            from file_hunter.services.hash_backfill import run_backfill

            logger.info(
                "Agent #%d scan_completed: launching backfill for "
                "location #%d (%s), %d files ingested",
                agent_id,
                loc_info[0],
                loc_info[1],
                files_ingested,
            )
            asyncio.create_task(run_backfill(agent_id, loc_info[0], loc_info[1]))
        else:
            logger.warning(
                "Agent #%d scan_completed: backfill NOT launched "
                "(loc_info=%s, files_ingested=%d)",
                agent_id,
                loc_info,
                files_ingested,
            )
    except Exception:
        logger.error("Agent #%d _finalize_scan failed", agent_id, exc_info=True)


async def agent_ws_endpoint(websocket: WebSocket):
    """Handle an incoming agent WebSocket connection."""
    qs = websocket.scope.get("query_string", b"").decode()
    params = parse_qs(qs)
    token = params.get("token", [""])[0]

    if not token:
        await websocket.accept()
        await websocket.send_text(
            json.dumps({"type": "error", "error": "Token required."})
        )
        await websocket.close(code=4001)
        return

    await websocket.accept()

    # Wait for register message
    try:
        raw = await websocket.receive_text()
        msg = json.loads(raw)
    except (WebSocketDisconnect, json.JSONDecodeError):
        await websocket.close(code=4001)
        return

    if msg.get("type") != "register":
        await websocket.send_text(
            json.dumps({"type": "error", "error": "Expected register message."})
        )
        await websocket.close(code=4001)
        return

    # Validate token against agents table
    auth_result = await _authenticate_agent(token)
    if not auth_result:
        await websocket.send_text(
            json.dumps({"type": "error", "error": "Invalid agent token."})
        )
        await websocket.close(code=4001)
        return
    agent_id, agent_name = auth_result

    # Store connection state
    hostname = msg.get("hostname", "")
    agent_os = msg.get("os", "")
    http_port = msg.get("httpPort", 8001)
    http_host = msg.get("httpHost", "0.0.0.0")
    client_ip = websocket.client.host if websocket.client else None

    _agent_connections[agent_id] = websocket
    _agent_tokens[agent_id] = token
    _agent_info[agent_id] = {
        "hostname": hostname,
        "httpPort": http_port,
        "httpHost": http_host,
        "clientIp": client_ip,
        "os": agent_os,
    }

    # Update agent status in DB
    now = datetime.now(timezone.utc).isoformat()

    async def _set_online(conn, aid, h, a_os, hp, hh, ts):
        await conn.execute(
            "UPDATE agents SET status = 'online', hostname = ?, os = ?, "
            "http_port = ?, http_host = ?, date_last_seen = ? WHERE id = ?",
            (h, a_os, str(hp), hh, ts, aid),
        )
        await conn.commit()

    await execute_write(
        _set_online, agent_id, hostname, agent_os, http_port, http_host, now
    )

    # Auto-create/update locations from agent's configured location roots
    agent_locations = msg.get("locations", [])
    if agent_locations:
        await _sync_agent_locations(agent_id, agent_locations)
        from file_hunter.services.online_check import (
            refresh_agent_location_ids_from_memory,
        )

        refresh_agent_location_ids_from_memory(agent_name, agent_id)

    # Adopt any existing local locations that have no agent assigned
    await _adopt_orphaned_locations(agent_id)

    # Refresh again after adoption (new location IDs may have been added)
    from file_hunter.services.online_check import load_agent_location_ids

    await load_agent_location_ids()

    # Send registration confirmation
    await websocket.send_text(json.dumps({"type": "registered", "agentId": agent_id}))

    # Broadcast status to UI browsers
    await broadcast({"type": "agent_status", "agentId": agent_id, "status": "online"})
    await broadcast(
        {
            "type": "location_changed",
            "action": "connected",
            "location": {"label": agent_name},
        }
    )

    logger.info(
        "Agent #%d (%s) connected with %d locations",
        agent_id,
        hostname,
        len(agent_locations),
    )

    # --- Clean up stale scan state from before reconnect ---
    agent_scanning = msg.get("scanning", False)
    location_ids = _agent_location_ids.get(agent_id, set())

    if scan_ingest.has_session(agent_id):
        if not agent_scanning:
            loc_info = scan_ingest.get_session_location(agent_id)
            await scan_ingest.cancel_session(agent_id)
            if loc_info:
                from file_hunter.services.scan_queue import notify_agent_scan_complete

                notify_agent_scan_complete(loc_info[0])
                await broadcast(
                    {
                        "type": "scan_cancelled",
                        "locationId": loc_info[0],
                        "location": loc_info[1],
                    }
                )
            logger.info(
                "Cleared stale scan session for agent #%d on reconnect", agent_id
            )

    if not agent_scanning:
        from file_hunter.services.scan_queue import notify_agent_scan_complete

        for loc_id in location_ids:
            notify_agent_scan_complete(loc_id)

    # Kick the scan queue — a queued scan may have been waiting for this agent
    from file_hunter.services.scan_queue import retry_queue

    asyncio.ensure_future(retry_queue())

    # Resume interrupted backfill if one was pending for this agent
    from file_hunter.services.hash_backfill import (
        pop_pending_backfill,
        run_backfill,
    )

    pending_bf = await pop_pending_backfill(agent_id)
    if pending_bf:
        logger.info(
            "Resuming interrupted backfill for location #%d (%s)",
            pending_bf[0],
            pending_bf[1],
        )
        asyncio.create_task(run_backfill(agent_id, pending_bf[0], pending_bf[1]))

    # Message loop
    try:
        async for raw in websocket.iter_text():
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type == "scan_progress":
                if not scan_ingest.has_session(agent_id):
                    await scan_ingest.start_session(agent_id, msg.get("path", ""))
                msg["agentId"] = agent_id
                loc_info = scan_ingest.get_session_location(agent_id)
                if loc_info:
                    msg["locationId"] = loc_info[0]
                    msg["location"] = loc_info[1]
                msg.setdefault("filesFound", 0)
                msg.setdefault("filesHashed", 0)
                msg.setdefault("filesSkipped", 0)
                msg["potentialMatches"] = scan_ingest.get_potential_matches(agent_id)
                await broadcast(msg)

            elif msg_type == "scan_files":
                if not scan_ingest.has_session(agent_id):
                    await scan_ingest.start_session(agent_id, msg.get("path", ""))
                await scan_ingest.ingest_batch(agent_id, msg.get("files", []))
                msg["agentId"] = agent_id
                await broadcast(msg)

            elif msg_type == "scan_completed":
                logger.info(
                    "Agent #%d scan_completed received (path=%s)",
                    agent_id,
                    msg.get("path", ""),
                )
                if not scan_ingest.has_session(agent_id):
                    await scan_ingest.start_session(agent_id, msg.get("path", ""))
                loc_info = scan_ingest.get_session_location(agent_id)
                scan_path = msg.get("path", "")
                asyncio.create_task(_finalize_scan(agent_id, scan_path, loc_info, msg))

            elif msg_type == "scan_cancelled":
                loc_info = scan_ingest.get_session_location(agent_id)
                await scan_ingest.cancel_session(agent_id)
                msg["agentId"] = agent_id
                if loc_info:
                    msg["locationId"] = loc_info[0]
                    msg["location"] = loc_info[1]
                await broadcast(msg)

                if loc_info:
                    from file_hunter.services.scan_queue import (
                        notify_agent_scan_complete,
                    )

                    notify_agent_scan_complete(loc_info[0])

            elif msg_type == "scan_error":
                loc_info = scan_ingest.get_session_location(agent_id)
                await scan_ingest.error_session(agent_id, msg.get("error", ""))
                msg["agentId"] = agent_id
                if loc_info:
                    msg["locationId"] = loc_info[0]
                    msg["location"] = loc_info[1]
                await broadcast(msg)

                if loc_info:
                    from file_hunter.services.scan_queue import (
                        notify_agent_scan_complete,
                    )

                    notify_agent_scan_complete(loc_info[0])
                else:
                    from file_hunter.services.scan_queue import (
                        notify_agent_scan_complete,
                    )

                    for loc_id in _agent_location_ids.get(agent_id, set()):
                        notify_agent_scan_complete(loc_id)

            elif msg_type == "locations_updated":
                agent_locations = msg.get("locations", [])
                if agent_locations:
                    await _sync_agent_locations(agent_id, agent_locations)
                    from file_hunter.services.online_check import (
                        refresh_agent_location_ids_from_memory,
                    )

                    refresh_agent_location_ids_from_memory(agent_name, agent_id)
                    await broadcast(
                        {
                            "type": "location_changed",
                            "action": "added",
                            "location": {"label": agent_name},
                        }
                    )
                    logger.info(
                        "Agent #%d locations updated: %d locations",
                        agent_id,
                        len(agent_locations),
                    )

            else:
                msg["agentId"] = agent_id
                await broadcast(msg)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning("Agent #%d WebSocket error: %s", agent_id, e)
    finally:
        # Cancel any running hash backfill — capture state for re-queue
        from file_hunter.services.hash_backfill import (
            cancel_backfill,
            get_active_backfill_info,
            queue_pending_backfill,
        )

        interrupted_backfill = get_active_backfill_info(agent_id)
        cancel_backfill(agent_id)

        # Clean up any active ingest session
        if scan_ingest.has_session(agent_id):
            await scan_ingest.cancel_session(agent_id)

        # Notify scan queue for any locations this agent owns
        from file_hunter.services.scan_queue import notify_agent_scan_complete

        for loc_id in _agent_location_ids.get(agent_id, set()):
            notify_agent_scan_complete(loc_id)

        # Only clean up if this websocket is still the registered one
        replaced = _agent_connections.get(agent_id) is not websocket

        if replaced:
            logger.info(
                "Agent #%d old connection cleanup skipped — "
                "new connection already registered",
                agent_id,
            )
        else:
            from file_hunter.services.online_check import clear_location_path_status

            clear_location_path_status(agent_id)

            _agent_connections.pop(agent_id, None)
            _agent_tokens.pop(agent_id, None)
            _agent_info.pop(agent_id, None)
            _agent_location_ids.pop(agent_id, None)

            now = datetime.now(timezone.utc).isoformat()

            async def _set_offline(conn, aid, ts):
                await conn.execute(
                    "UPDATE agents SET status = 'offline', date_last_seen = ? WHERE id = ?",
                    (ts, aid),
                )
                await conn.commit()

            await execute_write(_set_offline, agent_id, now)

            await broadcast(
                {"type": "agent_status", "agentId": agent_id, "status": "offline"}
            )
            await broadcast(
                {
                    "type": "location_changed",
                    "action": "disconnected",
                    "location": {"label": agent_name},
                }
            )

            logger.info("Agent #%d disconnected", agent_id)

        # Re-queue interrupted backfill regardless
        if interrupted_backfill:
            await queue_pending_backfill(agent_id, *interrupted_backfill)
            logger.info(
                "Queued interrupted backfill for location #%d (%s)",
                interrupted_backfill[0],
                interrupted_backfill[1],
            )


async def _authenticate_agent(token: str) -> tuple[int, str] | None:
    """Validate a token against all agents. Returns (agent_id, name) or None."""
    db = await get_db()
    cursor = await db.execute("SELECT id, name, token_hash FROM agents")
    rows = await cursor.fetchall()

    for row in rows:
        if verify_password(token, row["token_hash"]):
            return row["id"], row["name"]

    return None
