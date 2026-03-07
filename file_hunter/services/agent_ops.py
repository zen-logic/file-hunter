"""Agent filesystem operations proxy — dispatch to remote agent HTTP endpoints.

All filesystem operations on agent-backed locations are routed through
this single dispatch function.
"""

import logging

import httpx

from file_hunter.db import get_db

logger = logging.getLogger("file_hunter")

# Cache location_id -> agent_id to avoid repeated DB lookups
_loc_agent_cache: dict[int, int] = {}


def _resolve_agent(agent_id: int):
    """Return (host, port, token) for an online agent, or None."""
    from file_hunter.ws.agent import (
        get_online_agent_ids,
        get_agent_token,
        get_agent_info,
    )

    if agent_id not in get_online_agent_ids():
        return None
    token = get_agent_token(agent_id)
    info = get_agent_info(agent_id)
    if not token or not info:
        return None
    host = info.get("httpHost", "0.0.0.0")
    port = info.get("httpPort", 8001)
    if host == "0.0.0.0":
        host = info.get("clientIp") or info.get("hostname", "localhost")
    return host, port, token


async def _get_agent_id(location_id: int) -> int:
    """Look up the agent_id for a location, with caching."""
    if location_id in _loc_agent_cache:
        return _loc_agent_cache[location_id]

    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
    )
    if not row or not row[0]["agent_id"]:
        raise ValueError(f"Location {location_id} has no agent_id")
    agent_id = row[0]["agent_id"]
    _loc_agent_cache[location_id] = agent_id
    return agent_id


def invalidate_loc_cache(location_id: int = None):
    """Clear the location->agent cache (e.g. when locations change)."""
    if location_id:
        _loc_agent_cache.pop(location_id, None)
    else:
        _loc_agent_cache.clear()


async def _post(host, port, token, path, body, timeout=60.0):
    """POST JSON to agent and return the parsed data field."""
    url = f"http://{host}:{port}{path}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            url, json=body, headers={"Authorization": f"Bearer {token}"}
        )
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(f"Agent error on {path}: {result.get('error', resp.text)}")
    return result.get("data", {})


async def _get(host, port, token, path, timeout=60.0):
    """GET from agent and return the parsed data field."""
    url = f"http://{host}:{port}{path}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(f"Agent error on {path}: {result.get('error', resp.text)}")
    return result.get("data", {})


async def _upload_multipart(
    host, port, token, dest_dir, filename, file_obj, file_size, on_progress
):
    """Upload a file to agent via multipart POST — raw binary, no base64."""
    import asyncio
    import io

    url = f"http://{host}:{port}/upload"
    total = file_size

    class _TrackedReader(io.RawIOBase):
        def __init__(self, source, total_size):
            self._source = source
            self._total = total_size
            self.bytes_read = 0

        def readable(self):
            return True

        def readinto(self, b):
            data = self._source.read(len(b))
            if not data:
                return 0
            n = len(data)
            b[:n] = data
            self.bytes_read += n
            return n

    reader = _TrackedReader(file_obj, total)

    async def _do_upload():
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(600.0, connect=10.0)
        ) as client:
            resp = await client.post(
                url,
                data={"dest_dir": dest_dir},
                files={"file": (filename, reader)},
                headers={"Authorization": f"Bearer {token}"},
            )
        result = resp.json()
        if not result.get("ok"):
            raise RuntimeError(f"Agent upload error: {result.get('error', resp.text)}")

    if on_progress:
        await on_progress(0, total)
        upload_task = asyncio.create_task(_do_upload())
        while not upload_task.done():
            await asyncio.sleep(0.5)
            if not upload_task.done():
                await on_progress(min(reader.bytes_read, total), total)
        await upload_task
        await on_progress(total, total)
    else:
        await _do_upload()


async def dispatch(operation: str, location_id: int, **kwargs):
    """Route a filesystem operation to the correct agent HTTP endpoint."""
    agent_id = await _get_agent_id(location_id)
    resolved = _resolve_agent(agent_id)
    if not resolved:
        raise ConnectionError(f"Agent for location {location_id} is offline")
    host, port, token = resolved

    if operation == "file_exists":
        data = await _post(host, port, token, "/files/exists", {"path": kwargs["path"]})
        return data.get("is_file", False)

    elif operation == "dir_exists":
        data = await _post(host, port, token, "/files/exists", {"path": kwargs["path"]})
        return data.get("is_dir", False)

    elif operation == "path_exists":
        data = await _post(host, port, token, "/files/exists", {"path": kwargs["path"]})
        return data.get("exists", False)

    elif operation == "file_delete":
        await _post(host, port, token, "/files/delete", {"path": kwargs["path"]})

    elif operation == "file_move":
        await _post(
            host,
            port,
            token,
            "/files/move",
            {
                "path": kwargs["path"],
                "destination": kwargs["destination"],
            },
        )

    elif operation == "file_write":
        body = {"path": kwargs["path"], "content": kwargs["content"]}
        if "encoding" in kwargs:
            body["encoding"] = kwargs["encoding"]
        if kwargs.get("append"):
            body["append"] = True
        await _post(host, port, token, "/files/write", body)

    elif operation == "file_stat":
        data = await _post(host, port, token, "/files/stat", {"path": kwargs["path"]})
        if not data.get("exists"):
            return None
        return {"size": data["size"], "mtime": data["mtime"], "ctime": data["ctime"]}

    elif operation == "file_hash":
        data = await _post(
            host, port, token, "/files/hash", {"path": kwargs["path"]}, timeout=None
        )
        return {"hash_fast": data["hash_fast"], "hash_strong": data["hash_strong"]}

    elif operation == "dir_create":
        await _post(host, port, token, "/folders/create", {"path": kwargs["path"]})

    elif operation == "dir_delete":
        await _post(host, port, token, "/folders/delete", {"path": kwargs["path"]})

    elif operation == "dir_move":
        await _post(
            host,
            port,
            token,
            "/folders/move",
            {
                "path": kwargs["path"],
                "destination": kwargs["destination"],
            },
        )

    elif operation == "dir_exists":
        data = await _post(
            host, port, token, "/folders/exists", {"path": kwargs["path"]}
        )
        return data.get("exists", False)

    elif operation == "agent_status":
        return await _get(host, port, token, "/status", timeout=5.0)

    elif operation == "disk_stats":
        return await _post(
            host, port, token, "/disk-stats", {"path": kwargs["path"]}, timeout=10.0
        )

    elif operation == "_upload_file":
        await _upload_multipart(
            host,
            port,
            token,
            kwargs["dest_dir"],
            kwargs["filename"],
            kwargs["file_obj"],
            kwargs["file_size"],
            kwargs.get("on_progress"),
        )

    else:
        raise ValueError(f"Unknown agent operation: {operation}")


async def rename_agent_location(agent_id: int, root_path: str, new_name: str):
    """Tell an agent to rename a location in its config.json."""
    resolved = _resolve_agent(agent_id)
    if not resolved:
        logger.warning(
            "Agent %d offline — skipping location rename for %s", agent_id, root_path
        )
        return
    host, port, token = resolved
    await _post(
        host,
        port,
        token,
        "/locations/rename",
        {
            "path": root_path,
            "name": new_name,
        },
    )


async def delete_agent_location(agent_id: int, root_path: str, location_id: int = None):
    """Tell an agent to remove a location from its config.json."""
    resolved = _resolve_agent(agent_id)
    if not resolved:
        logger.warning(
            "Agent %d offline — skipping location delete for %s", agent_id, root_path
        )
        return
    host, port, token = resolved
    await _post(host, port, token, "/locations/delete", {"path": root_path})
    if location_id:
        invalidate_loc_cache(location_id)
