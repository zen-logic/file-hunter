"""Filesystem browsing — proxied through the local agent."""

import urllib.parse

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db
from file_hunter.services.agent_ops import _resolve_agent, _get


async def browse(request: Request):
    path = request.query_params.get("path", "").strip()

    # Find the local agent
    async with read_db() as db:
        cursor = await db.execute("SELECT id FROM agents WHERE name = 'Local Agent'")
        row = await cursor.fetchone()
    if not row:
        return json_error("Local agent not configured.", 503)

    resolved = _resolve_agent(row["id"])
    if not resolved:
        return json_error("Local agent is offline.", 503)

    host, port, token = resolved
    qs = f"?path={urllib.parse.quote(path, safe='')}" if path else ""
    try:
        data = await _get(host, port, token, f"/browse-system{qs}", timeout=10.0)
    except Exception as e:
        return json_error(f"Browse failed: {e}", 500)

    return json_ok(data)
