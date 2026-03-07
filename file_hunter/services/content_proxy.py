"""Proxy file content from agents.

When a file belongs to an agent-backed location, this service fetches it
from the agent's HTTP endpoint and streams it back.
"""

import logging

import httpx
from starlette.responses import StreamingResponse

from file_hunter.db import get_db
from file_hunter.services.agent_ops import _resolve_agent

logger = logging.getLogger("file_hunter")

MIME_MAP = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "webp": "image/webp",
    "svg": "image/svg+xml",
    "bmp": "image/bmp",
    "mp4": "video/mp4",
    "webm": "video/webm",
    "mov": "video/quicktime",
    "avi": "video/x-msvideo",
    "mkv": "video/x-matroska",
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "flac": "audio/flac",
    "ogg": "audio/ogg",
    "aac": "audio/aac",
    "m4a": "audio/mp4",
    "txt": "text/plain",
    "md": "text/plain",
    "csv": "text/plain",
    "json": "text/plain",
    "xml": "text/plain",
    "log": "text/plain",
    "py": "text/plain",
    "js": "text/plain",
    "html": "text/plain",
    "css": "text/plain",
    "pdf": "application/pdf",
    "moved": "text/plain",
    "sources": "text/plain",
}


async def fetch_agent_bytes(full_path, location_id):
    """Fetch raw file bytes from an agent.

    Returns bytes if successful, None if not an agent location or offline.
    """
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
    )
    if not row or not row[0]["agent_id"]:
        return None

    resolved = _resolve_agent(row[0]["agent_id"])
    if not resolved:
        return None
    host, port, token = resolved

    url = f"http://{host}:{port}/files/content"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                url,
                params={"path": full_path},
                headers={"Authorization": f"Bearer {token}"},
            )
        if response.status_code == 200:
            return response.content
        logger.warning(
            "Agent fetch_bytes failed for %s: %d", full_path, response.status_code
        )
        return None
    except httpx.RequestError as e:
        logger.warning("Agent fetch_bytes error for %s: %s", full_path, e)
        return None


async def proxy_agent_content(file_id, full_path, filename, request_headers=None):
    """Proxy file content from an agent for preview/download.

    Returns a StreamingResponse if the file belongs to an online agent,
    or None if not an agent file or agent is offline.
    Supports Range requests for video/audio streaming.
    """
    db = await get_db()

    row = await db.execute_fetchall(
        """SELECT l.agent_id FROM files f
           JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (file_id,),
    )
    if not row or not row[0]["agent_id"]:
        return None

    resolved = _resolve_agent(row[0]["agent_id"])
    if not resolved:
        return None
    host, port, token = resolved

    url = f"http://{host}:{port}/files/content"

    agent_headers = {"Authorization": f"Bearer {token}"}
    if request_headers and "range" in request_headers:
        agent_headers["Range"] = request_headers["range"]

    client = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0))
    try:
        req = client.build_request(
            "GET", url, params={"path": full_path}, headers=agent_headers
        )
        response = await client.send(req, stream=True)

        if response.status_code not in (200, 206):
            body = (await response.aread())[:200]
            await response.aclose()
            await client.aclose()
            logger.warning(
                "Content proxy failed for %s: %d %s",
                full_path,
                response.status_code,
                body.decode(errors="replace"),
            )
            return None

        content_type = response.headers.get("content-type", "")
        if not content_type or content_type == "application/octet-stream":
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            content_type = MIME_MAP.get(ext, "application/octet-stream")

        resp_headers = {}
        for hdr in ("content-range", "accept-ranges", "content-length"):
            if hdr in response.headers:
                resp_headers[hdr] = response.headers[hdr]

        async def stream_chunks():
            try:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk
            finally:
                await response.aclose()
                await client.aclose()

        return StreamingResponse(
            stream_chunks(),
            status_code=response.status_code,
            media_type=content_type,
            headers=resp_headers,
        )

    except httpx.RequestError as e:
        await client.aclose()
        logger.warning("Content proxy error for %s: %s", full_path, e)
        return None
