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

from file_hunter.db import db_writer, read_db, execute_write
from file_hunter.services.auth import verify_password
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# In-memory state for connected agents
_agent_connections: dict[int, WebSocket] = {}  # agent_id -> WebSocket
_agent_tokens: dict[int, str] = {}  # agent_id -> raw token (for HTTP calls)
_agent_info: dict[int, dict] = {}  # agent_id -> {hostname, httpPort, httpHost, os}

# agent_id -> set of location_ids (for online check lookups)
_agent_location_ids: dict[int, set[int]] = {}

# agent_id -> set of capability strings (e.g. "tsv_tree")
_agent_capabilities: dict[int, set[str]] = {}


def get_agent_capabilities(agent_id: int) -> set[str]:
    return _agent_capabilities.get(agent_id, set())


async def _backfill_dup_candidates(agent_id: int, location_id: int):
    """Background task: find files with hash_partial but no hash_fast that
    have size+partial matches elsewhere. Queue them for hashing."""
    from file_hunter.services.activity import register, unregister, update
    from file_hunter.services.dup_counts import hash_candidates_for_location

    async with read_db() as db:
        name_row = await db.execute_fetchall(
            "SELECT name FROM locations WHERE id = ?", (location_id,)
        )
    loc_name = name_row[0]["name"] if name_row else f"location {location_id}"

    act_name = f"backfill-candidates-{location_id}"
    try:
        register(act_name, f"Checking duplicates: {loc_name}")
        total, small, large = await hash_candidates_for_location(
            location_id=location_id,
            agent_id=agent_id,
        )
        if total > 0:
            logger.info(
                "Reconnect backfill for %s: %d candidates (%d small, %d queued)",
                loc_name, total, small, large,
            )
            update(act_name, progress=f"{small} resolved, {large} queued")
        # If large files were queued, drain_pending_hashes will pick them up
        # (already launched as a task in the reconnect flow)
    except Exception:
        logger.exception("Reconnect backfill failed for %s", loc_name)
    finally:
        unregister(act_name)


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

    async with read_db() as db:
        orphans = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE agent_id IS NULL"
        )
    if not orphans:
        return

    logger.info("Agent #%d: adopting %d orphaned locations", agent_id, len(orphans))

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
                host,
                port,
                token,
                "/locations/add",
                {"name": o["name"], "path": o["root_path"]},
            )
            logger.info(
                "Agent #%d: pushed orphaned location '%s' (%s) to agent config",
                agent_id,
                o["name"],
                o["root_path"],
            )
        except Exception as e:
            logger.warning(
                "Agent #%d: failed to push location '%s' to agent: %s",
                agent_id,
                o["name"],
                e,
            )

    # Add orphan location IDs to the in-memory set
    if agent_id in _agent_location_ids:
        _agent_location_ids[agent_id].update(orphan_ids)
    else:
        _agent_location_ids[agent_id] = set(orphan_ids)

    invalidate_stats_cache()


async def _process_pending_deletes(
    agent_id: int, agent_locations: list[dict]
) -> list[dict]:
    """Remove stale locations from agent config on reconnect.

    Checks pending_agent_deletes for this agent, tells the agent to drop
    each path, and filters them from the reported locations so
    _sync_agent_locations won't re-create catalog records.
    """
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT root_path FROM pending_agent_deletes WHERE agent_id = ?",
            (agent_id,),
        )
    if not rows:
        return agent_locations

    from file_hunter.services.agent_ops import delete_agent_location

    pending_paths: set[str] = {r["root_path"] for r in rows}
    cleaned: set[str] = set()

    for path in pending_paths:
        try:
            await delete_agent_location(agent_id, path)
            cleaned.add(path)
            logger.info(
                "Reconnect cleanup: removed '%s' from agent #%d config", path, agent_id
            )
        except Exception:
            logger.warning(
                "Reconnect cleanup: failed to remove '%s' from agent #%d",
                path,
                agent_id,
            )

    if cleaned:
        async with db_writer() as db:
            for path in cleaned:
                await db.execute(
                    "DELETE FROM pending_agent_deletes WHERE agent_id = ? AND root_path = ?",
                    (agent_id, path),
                )

    # Filter all pending paths (cleaned or not) from sync to prevent re-creation
    agent_locations = [
        loc for loc in agent_locations if loc.get("path") not in pending_paths
    ]
    return agent_locations


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
    from file_hunter.services.online_check import (
        _agent_location_path_status,
        update_location_path_status,
    )

    prev_status = _agent_location_path_status.get(agent_id, {})
    path_status: dict[int, bool] = {}
    async with read_db() as db:
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

    # Detect locations that just came online (were offline or unknown before)
    newly_online: set[int] = set()
    for loc_id, online in path_status.items():
        if online and not prev_status.get(loc_id, False):
            newly_online.add(loc_id)

    # Drain any pending consolidation jobs and deferred file ops for online locations
    from file_hunter.services.consolidate import drain_pending_jobs
    from file_hunter.services.deferred_ops import drain_pending_ops

    for loc_id, online in path_status.items():
        if not online:
            continue
        async with read_db() as db:
            cursor = await db.execute(
                "SELECT root_path FROM locations WHERE id = ?", (loc_id,)
            )
            loc_row = await cursor.fetchone()
        if not loc_row:
            continue
        root_path = loc_row["root_path"]

        # Check if there are pending consolidation jobs before draining
        async with read_db() as db:
            pending = await db.execute(
                "SELECT COUNT(*) as cnt FROM consolidation_jobs "
                "WHERE source_location_id = ? AND status = 'pending'",
                (loc_id,),
            )
            cnt_row = await pending.fetchone()
        if cnt_row and cnt_row["cnt"] > 0:
            await drain_pending_jobs(loc_id, root_path)

        # Check if there are pending file ops before draining
        async with read_db() as db:
            pending_ops = await db.execute(
                "SELECT COUNT(*) as cnt FROM pending_file_ops "
                "WHERE location_id = ? AND status = 'pending'",
                (loc_id,),
            )
            ops_row = await pending_ops.fetchone()
        if ops_row and ops_row["cnt"] > 0:
            await drain_pending_ops(loc_id, root_path)

        # Drain pending_hashes (from interrupted import/scan drain)
        async with read_db() as db:
            ph_row = await db.execute_fetchall(
                "SELECT COUNT(*) as cnt FROM pending_hashes "
                "WHERE location_id = ? AND agent_id = ?",
                (loc_id, agent_id),
            )
        if ph_row and ph_row[0]["cnt"] > 0:
            from file_hunter.services.dup_counts import drain_pending_hashes

            loc_name = ""
            async with read_db() as db:
                name_row = await db.execute_fetchall(
                    "SELECT name FROM locations WHERE id = ?", (loc_id,)
                )
            if name_row:
                loc_name = name_row[0]["name"]

            asyncio.create_task(drain_pending_hashes(agent_id, loc_id, loc_name))

        # Queue dup candidates for locations that just came online
        if loc_id in newly_online:
            asyncio.create_task(_backfill_dup_candidates(agent_id, loc_id))

    return location_ids


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
    capabilities = set(msg.get("capabilities", []))
    _agent_capabilities[agent_id] = capabilities
    _agent_info[agent_id] = {
        "hostname": hostname,
        "httpPort": http_port,
        "httpHost": http_host,
        "clientIp": client_ip,
        "os": agent_os,
    }

    from file_hunter.services.agent_ops import open_agent_client
    await open_agent_client(agent_id)

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

    # Process any pending config cleanups from locations deleted while offline
    agent_locations = msg.get("locations", [])
    agent_locations = await _process_pending_deletes(agent_id, agent_locations)

    # Auto-create/update locations from agent's configured location roots
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

    # Broadcast status to UI browsers (include disk stats so bars render
    # even if the page loaded before agents connected)
    raw_loc_ids = list(_agent_location_ids.get(agent_id, set()))
    loc_ids = [f"loc-{lid}" for lid in raw_loc_ids]

    disk_stats_map = {}
    if raw_loc_ids:
        from file_hunter.services.locations import get_disk_stats

        async with read_db() as _db:
            loc_rows = await _db.execute_fetchall(
                f"SELECT id, root_path FROM locations WHERE id IN ({','.join('?' for _ in raw_loc_ids)})",
                raw_loc_ids,
            )
        ds_tasks = [get_disk_stats(r["id"], r["root_path"]) for r in loc_rows]
        ds_results = await asyncio.gather(*ds_tasks, return_exceptions=True)
        for r, ds in zip(loc_rows, ds_results):
            if isinstance(ds, dict):
                disk_stats_map[f"loc-{r['id']}"] = ds

    await broadcast(
        {
            "type": "agent_status",
            "agentId": agent_id,
            "status": "online",
            "locationIds": loc_ids,
            "diskStats": disk_stats_map if disk_stats_map else None,
        }
    )
    await broadcast(
        {
            "type": "location_changed",
            "action": "connected",
            "location": {"label": agent_name},
        }
    )

    cap_label = ", ".join(sorted(capabilities)) if capabilities else "none"
    logger.info(
        "Agent #%d (%s) connected with %d locations — capabilities: %s",
        agent_id,
        hostname,
        len(agent_locations),
        cap_label,
    )

    # Broadcast capabilities to UI for activity panel
    await broadcast(
        {
            "type": "agent_capabilities",
            "agentId": agent_id,
            "agentName": agent_name,
            "hostname": hostname,
            "capabilities": sorted(capabilities),
        }
    )

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
        asyncio.create_task(
            run_backfill(agent_id, pending_bf[0], pending_bf[1], pending_bf[2])
        )

    # Message loop — server drives scanning via HTTP /reconcile now,
    # so we only handle location updates and forward other messages.
    try:
        async for raw in websocket.iter_text():
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type == "locations_updated":
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
    except asyncio.CancelledError:
        logger.info("Agent #%d WebSocket cancelled (shutdown)", agent_id)
    except Exception as e:
        logger.warning("Agent #%d WebSocket error: %s", agent_id, e)
    finally:
        try:
            # Cancel any running hash backfill — capture state for re-queue
            from file_hunter.services.hash_backfill import (
                cancel_backfill,
                get_active_backfill_info,
                queue_pending_backfill,
            )

            interrupted_backfill = get_active_backfill_info(agent_id)
            cancel_backfill(agent_id)

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

                disc_loc_ids = [
                    f"loc-{lid}" for lid in _agent_location_ids.get(agent_id, set())
                ]

                from file_hunter.services.agent_ops import close_agent_client
                await close_agent_client(agent_id)

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
                    {
                        "type": "agent_status",
                        "agentId": agent_id,
                        "status": "offline",
                        "locationIds": disc_loc_ids,
                    }
                )
                await broadcast(
                    {
                        "type": "location_changed",
                        "action": "disconnected",
                        "location": {"label": agent_name},
                    }
                )

                logger.info("Agent #%d disconnected", agent_id)

            # Re-queue interrupted backfill at head so it resumes first
            if interrupted_backfill:
                await queue_pending_backfill(
                    agent_id, *interrupted_backfill, front=True
                )
                logger.info(
                    "Queued interrupted backfill for location #%d (%s)",
                    interrupted_backfill[0],
                    interrupted_backfill[1],
                )
        except asyncio.CancelledError:
            # Shutdown cancelled the cleanup — in-memory state already torn down,
            # DB will be corrected on next startup (_recover_interrupted)
            logger.info("Agent #%d cleanup skipped (shutdown)", agent_id)
            from file_hunter.services.agent_ops import close_agent_client
            await close_agent_client(agent_id)
            _agent_connections.pop(agent_id, None)
            _agent_tokens.pop(agent_id, None)
            _agent_info.pop(agent_id, None)
            _agent_location_ids.pop(agent_id, None)


async def _authenticate_agent(token: str) -> tuple[int, str] | None:
    """Validate a token against all agents. Returns (agent_id, name) or None."""
    async with read_db() as db:
        cursor = await db.execute("SELECT id, name, token_hash FROM agents")
        rows = await cursor.fetchall()

    for row in rows:
        if verify_password(token, row["token_hash"]):
            return row["id"], row["name"]

    return None
