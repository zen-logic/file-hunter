"""Agent filesystem operations proxy — dispatch to remote agent HTTP endpoints.

All filesystem operations on agent-backed locations are routed through
this single dispatch function.
"""

import logging

import httpx

from file_hunter.db import read_db

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

    async with read_db() as db:
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
        body = {"path": kwargs["path"]}
        if kwargs.get("strong"):
            body["strong"] = True
        data = await _post(host, port, token, "/files/hash", body, timeout=None)
        result = {"hash_fast": data["hash_fast"]}
        if "hash_strong" in data:
            result["hash_strong"] = data["hash_strong"]
        return result

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


async def hash_partial_batch(agent_id: int, paths: list[str]) -> dict:
    """Call agent /files/hash-partial-batch for a list of file paths."""
    resolved = _resolve_agent(agent_id)
    if not resolved:
        raise ConnectionError(f"Agent {agent_id} is offline")
    host, port, token = resolved
    return await _post(
        host, port, token, "/files/hash-partial-batch", {"paths": paths}, timeout=300.0
    )


async def hash_fast_batch(agent_id: int, paths: list[str]) -> dict:
    """Call agent /files/hash-batch for a list of file paths.

    Returns {"results": [{"path": ..., "hash_fast": ...}], "errors": [...]}.
    Raises ConnectionError if agent is offline.
    """
    resolved = _resolve_agent(agent_id)
    if not resolved:
        raise ConnectionError(f"Agent {agent_id} is offline")
    host, port, token = resolved
    return await _post(
        host, port, token, "/files/hash-batch", {"paths": paths}, timeout=600.0
    )


async def reconcile_directory(
    agent_id: int, path: str, root_path: str, expected: list[dict], cursor=None
) -> dict:
    """Call agent /reconcile endpoint for a single directory.

    Args:
        cursor: pagination offset. 0 = first page (new server, enables pagination).
            None = don't send cursor field (backward compat with old agents).

    Returns dict with keys: unchanged, changed, gone, new, subdirs,
    and when paginating: cursor, total_new, total_changed.
    """
    resolved = _resolve_agent(agent_id)
    if not resolved:
        raise ConnectionError(f"Agent {agent_id} is offline")
    host, port, token = resolved
    body = {"path": path, "root_path": root_path, "expected": expected}
    if cursor is not None:
        body["cursor"] = cursor
    return await _post(
        host,
        port,
        token,
        "/reconcile",
        body,
        timeout=None,
    )


async def stream_copy(
    src_path: str,
    src_loc_id: int,
    dst_path: str,
    dst_loc_id: int,
    on_progress=None,
):
    """Stream a file from one agent to another without buffering in RAM.

    Pipes GET /files/content (source) -> POST /files/stream-write (dest)
    one chunk at a time. on_progress(bytes_sent, total_bytes) called per chunk.
    """
    src_agent_id = await _get_agent_id(src_loc_id)
    dst_agent_id = await _get_agent_id(dst_loc_id)
    src_resolved = _resolve_agent(src_agent_id)
    dst_resolved = _resolve_agent(dst_agent_id)
    if not src_resolved:
        raise ConnectionError(f"Source agent for location {src_loc_id} is offline")
    if not dst_resolved:
        raise ConnectionError(f"Destination agent for location {dst_loc_id} is offline")

    s_host, s_port, s_token = src_resolved
    d_host, d_port, d_token = dst_resolved

    src_url = f"http://{s_host}:{s_port}/files/content"
    dst_url = f"http://{d_host}:{d_port}/files/stream-write"

    src_client = httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10.0))
    try:
        src_req = src_client.build_request(
            "GET",
            src_url,
            params={"path": src_path},
            headers={"Authorization": f"Bearer {s_token}"},
        )
        src_resp = await src_client.send(src_req, stream=True)

        if src_resp.status_code != 200:
            body = (await src_resp.aread())[:500]
            await src_resp.aclose()
            raise RuntimeError(
                f"Source agent returned {src_resp.status_code}: "
                f"{body.decode(errors='replace')}"
            )

        total_bytes = int(src_resp.headers.get("content-length", 0))
        bytes_sent = 0

        async def _pipe_chunks():
            nonlocal bytes_sent
            try:
                async for chunk in src_resp.aiter_bytes(chunk_size=1048576):
                    bytes_sent += len(chunk)
                    if on_progress:
                        await on_progress(bytes_sent, total_bytes)
                    yield chunk
            finally:
                await src_resp.aclose()

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(None, connect=10.0)
        ) as dst_client:
            resp = await dst_client.post(
                dst_url,
                params={"path": dst_path},
                content=_pipe_chunks(),
                headers={
                    "Authorization": f"Bearer {d_token}",
                    "Content-Type": "application/octet-stream",
                },
            )
            result = resp.json()
            if not result.get("ok"):
                raise RuntimeError(
                    f"Destination agent error: {result.get('error', resp.text)}"
                )
    finally:
        await src_client.aclose()


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


def _parse_tsv_line(line: str) -> dict | None:
    """Parse a TSV tree line into a dict.

    Format:
        D\trel_dir
        F\trel_path\tsize\tmtime\tctime\tinode
        P\thashing\ttotal_files
        H\trel_path\thash_partial
        E\ttotal_dirs\ttotal_files
    """
    parts = line.split("\t")
    if not parts:
        return None
    t = parts[0]
    if t == "F" and len(parts) >= 6:
        return {
            "type": "file",
            "rel_path": parts[1],
            "size": int(parts[2]),
            "mtime": parts[3],
            "ctime": parts[4],
            "inode": int(parts[5]),
        }
    if t == "D" and len(parts) >= 2:
        return {"type": "dir", "rel_dir": parts[1]}
    if t == "H" and len(parts) >= 3:
        return {
            "type": "hash",
            "rel_path": parts[1],
            "hash_partial": parts[2],
        }
    if t == "P" and len(parts) >= 3:
        return {"type": "phase", "phase": parts[1], "total": int(parts[2])}
    if t == "E" and len(parts) >= 3:
        return {"type": "end", "dirs": int(parts[1]), "files": int(parts[2])}
    return None


async def stream_tree(agent_id: int, root_path: str, prefix: str | None = None):
    """Stream the full metadata tree from an agent as parsed dicts.

    Each directory arrives as a batch: one "dir" dict followed by all "file"
    dicts for that directory (inode-sorted, with partial hashes).

    Yields dicts: {"type":"dir",...}, {"type":"file",...}, {"type":"end",...}
    Raises ConnectionError if the agent is offline.
    """
    resolved = _resolve_agent(agent_id)
    if not resolved:
        raise ConnectionError(f"Agent {agent_id} is offline")
    host, port, token = resolved

    url = f"http://{host}:{port}/tree"
    body = {"path": root_path}
    if prefix:
        body["prefix"] = prefix

    timeout = httpx.Timeout(1800.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream(
            "POST",
            url,
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        ) as resp:
            if resp.status_code != 200:
                text = ""
                async for chunk in resp.aiter_text():
                    text += chunk
                    if len(text) > 500:
                        break
                raise RuntimeError(
                    f"Agent /tree error {resp.status_code}: {text[:500]}"
                )
            async for line in resp.aiter_lines():
                line = line.strip()
                if not line:
                    continue
                parsed = _parse_tsv_line(line)
                if parsed:
                    yield parsed


async def delete_agent_location(agent_id: int, root_path: str, location_id: int = None):
    """Tell an agent to remove a location from its config.json.

    Raises ConnectionError if the agent is offline so callers (e.g.
    queue_manager) can retry later.
    """
    resolved = _resolve_agent(agent_id)
    if not resolved:
        raise ConnectionError(
            f"Agent {agent_id} offline — cannot delete location {root_path}"
        )
    host, port, token = resolved
    await _post(host, port, token, "/locations/delete", {"path": root_path})
    if location_id:
        invalidate_loc_cache(location_id)
