"""Scan trigger and cancel for agent-backed locations.

Sends scan/cancel commands to agents via WebSocket.
"""

import logging

from file_hunter.db import get_db
from file_hunter.ws.agent import send_to_agent, get_online_agent_ids
from file_hunter.services import scan_ingest

logger = logging.getLogger("file_hunter")


async def agent_scan_trigger(location_id, name, root_path, scan_path):
    """Check if this location is agent-backed and trigger a remote scan.

    Returns True if handled (agent scan started), False otherwise.
    """
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
    )
    if not row or not row[0]["agent_id"]:
        logger.warning("Scan trigger: location #%d has no agent_id", location_id)
        return False

    agent_id = row[0]["agent_id"]

    if agent_id not in get_online_agent_ids():
        logger.warning(
            "Scan trigger: agent #%d for location #%d is offline",
            agent_id,
            location_id,
        )
        return False

    if scan_ingest.has_session(agent_id):
        session_loc = scan_ingest.get_session_location(agent_id)
        if session_loc and session_loc[0] == location_id:
            logger.info(
                "Scan trigger: agent #%d already scanning location #%d — piggyback",
                agent_id,
                location_id,
            )
            return True
        logger.info(
            "Scan trigger: agent #%d busy scanning location #%d, can't start #%d",
            agent_id,
            session_loc[0] if session_loc else "?",
            location_id,
        )
        return False

    scan_target = scan_path or root_path
    logger.info("Sending scan command to agent #%d for path: %s", agent_id, scan_target)
    sent = await send_to_agent(
        agent_id,
        {
            "type": "scan",
            "path": scan_target,
            "root_path": root_path,
        },
    )
    if not sent:
        logger.warning("Scan trigger: send_to_agent #%d returned False", agent_id)
        return False

    logger.info(
        "Triggered agent #%d scan for location #%d (%s)",
        agent_id,
        location_id,
        scan_target,
    )
    return True


async def agent_scan_cancel(location_id):
    """Cancel a running scan on an agent-backed location.

    Returns True if handled, False otherwise.
    """
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
    )
    if not row or not row[0]["agent_id"]:
        return False

    agent_id = row[0]["agent_id"]

    if agent_id not in get_online_agent_ids():
        return False

    if not scan_ingest.has_session(agent_id):
        return False

    sent = await send_to_agent(agent_id, {"type": "scan_cancel"})
    if not sent:
        return False

    logger.info(
        "Requested cancel for agent #%d scan (location #%d)", agent_id, location_id
    )
    return True
