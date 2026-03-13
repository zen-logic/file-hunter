"""Filesystem abstraction layer — routes all operations through agents.

Every location is agent-backed. Operations are dispatched via the
registered agent_proxy hook.
"""

import os

from file_hunter.extensions import get_agent_proxy


# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------


async def file_exists(path: str, location_id: int) -> bool:
    proxy = get_agent_proxy()
    if proxy:
        return await proxy("file_exists", location_id, path=path)
    return False


async def dir_exists(path: str, location_id: int) -> bool:
    proxy = get_agent_proxy()
    if proxy:
        return await proxy("dir_exists", location_id, path=path)
    return False


async def path_exists(path: str, location_id: int) -> bool:
    proxy = get_agent_proxy()
    if proxy:
        return await proxy("path_exists", location_id, path=path)
    return False


async def file_delete(path: str, location_id: int):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    return await proxy("file_delete", location_id, path=path)


async def file_move(src: str, dest: str, location_id: int):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    return await proxy("file_move", location_id, path=src, destination=dest)


async def file_write_text(path: str, text: str, location_id: int):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    return await proxy("file_write", location_id, path=path, content=text)


async def file_write_bytes(path: str, data: bytes, location_id: int):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    import base64

    return await proxy(
        "file_write",
        location_id,
        path=path,
        content=base64.b64encode(data).decode(),
        encoding="base64",
    )


CHUNK_SIZE = 1024 * 1024  # 1 MB


async def file_write_bytes_chunked(
    path: str, data: bytes, location_id: int, on_progress=None
):
    """Write bytes in chunks with progress callback.

    on_progress(bytes_sent, total_bytes) is called after each chunk.
    Falls back to file_write_bytes for small files.
    """
    if len(data) <= CHUNK_SIZE:
        await file_write_bytes(path, data, location_id)
        if on_progress:
            await on_progress(len(data), len(data))
        return

    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")

    import base64

    total = len(data)
    offset = 0
    first = True

    while offset < total:
        chunk = data[offset : offset + CHUNK_SIZE]
        await proxy(
            "file_write",
            location_id,
            path=path,
            content=base64.b64encode(chunk).decode(),
            encoding="base64",
            append=not first,
        )
        offset += len(chunk)
        first = False
        if on_progress:
            await on_progress(offset, total)


async def file_read_bytes(path: str, location_id: int) -> bytes:
    from file_hunter.extensions import get_fetch_bytes

    fetch = get_fetch_bytes()
    if fetch:
        data = await fetch(path, location_id)
        if data is not None:
            return data
    raise ConnectionError("Agent is offline or proxy not available.")


async def file_stat(path: str, location_id: int) -> dict | None:
    """Return {size, mtime, ctime} or None if path doesn't exist."""
    proxy = get_agent_proxy()
    if proxy:
        return await proxy("file_stat", location_id, path=path)
    return None


async def file_hash(path: str, location_id: int, *, strong: bool = False) -> tuple:
    """Return (hash_fast,) or (hash_fast, hash_strong) if strong=True."""
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    result = await proxy("file_hash", location_id, path=path, strong=strong)
    if strong:
        return result["hash_fast"], result["hash_strong"]
    return (result["hash_fast"],)


# ---------------------------------------------------------------------------
# Directory operations
# ---------------------------------------------------------------------------


async def dir_create(path: str, location_id: int, exist_ok: bool = False):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    return await proxy("dir_create", location_id, path=path)


async def dir_delete(path: str, location_id: int):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    return await proxy("dir_delete", location_id, path=path)


async def dir_move(src: str, dest: str, location_id: int):
    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")
    return await proxy("dir_move", location_id, path=src, destination=dest)


# ---------------------------------------------------------------------------
# Cross-location copy
# ---------------------------------------------------------------------------


async def copy_file(
    src: str, src_loc_id: int, dst: str, dst_loc_id: int, on_progress=None
):
    """Copy a file between agent locations via streaming (constant memory)."""
    from file_hunter.services.agent_ops import stream_copy

    await stream_copy(src, src_loc_id, dst, dst_loc_id, on_progress=on_progress)


# ---------------------------------------------------------------------------
# Composite helpers (stub/sources generation)
# ---------------------------------------------------------------------------


async def write_moved_stub(
    original_path: str, filename: str, dest_path: str, now_iso: str, location_id: int
):
    """Generate .moved stub text, write it, and delete the original."""
    stub_text = (
        f"Consolidated by File Hunter\n"
        f"Original: {filename}\n"
        f"Moved to: {dest_path}\n"
        f"Date: {now_iso}\n"
    )
    stub_path = original_path + ".moved"
    await file_delete(original_path, location_id)
    await file_write_text(stub_path, stub_text, location_id)


async def write_sources_file(
    canonical_path: str, all_copies: list[dict], now_iso: str, location_id: int
):
    """Write a .sources file next to the canonical file."""
    sources_path = canonical_path + ".sources"
    entries = "".join(f"- {c['location_name']}: {c['rel_path']}\n" for c in all_copies)

    existing = await path_exists(sources_path, location_id)
    if existing:
        # Append to existing
        try:
            old_content = (await file_read_bytes(sources_path, location_id)).decode()
        except Exception:
            old_content = ""
        await file_write_text(sources_path, old_content + entries, location_id)
    else:
        text = f"Consolidated by File Hunter\nDate: {now_iso}\n\nSources:\n{entries}"
        await file_write_text(sources_path, text, location_id)


async def write_or_append_sources(
    canonical_path: str,
    src_loc_name: str,
    src_rel_path: str,
    now_iso: str,
    location_id: int,
):
    """Append a single source entry to the .sources file."""
    sources_path = canonical_path + ".sources"
    entry = f"- {src_loc_name}: {src_rel_path}\n"

    existing = await path_exists(sources_path, location_id)
    if existing:
        try:
            old_content = (await file_read_bytes(sources_path, location_id)).decode()
        except Exception:
            old_content = ""
        await file_write_text(sources_path, old_content + entry, location_id)
    else:
        text = f"Consolidated by File Hunter\nDate: {now_iso}\n\nSources:\n{entry}"
        await file_write_text(sources_path, text, location_id)


async def agent_upload_file(
    dest_dir: str,
    filename: str,
    file_obj,
    file_size: int,
    location_id: int,
    on_progress=None,
):
    """Upload a file to an agent via multipart POST to /upload endpoint.

    Streams from file_obj (file-like) — no full-file read into RAM.
    Progress is reported via on_progress(bytes_sent, total_bytes).
    """
    from file_hunter.extensions import get_agent_proxy

    proxy = get_agent_proxy()
    if not proxy:
        raise ConnectionError("Agent proxy not available.")

    result = await proxy(
        "_upload_file",
        location_id,
        dest_dir=dest_dir,
        filename=filename,
        file_obj=file_obj,
        file_size=file_size,
        on_progress=on_progress,
    )
    return result


async def unique_hidden_path(dest_path: str, location_id: int) -> str:
    """Handle collision for hidden files/folders — append .1, .2, etc."""
    if not await path_exists(dest_path, location_id):
        return dest_path
    counter = 1
    while True:
        candidate = f"{dest_path}.{counter}"
        if not await path_exists(candidate, location_id):
            return candidate
        counter += 1


async def unique_dest_path(dest_path: str, location_id: int) -> str:
    """Handle filename collision — append (2), (3), etc."""
    if not await path_exists(dest_path, location_id):
        return dest_path

    base, ext = os.path.splitext(dest_path)
    counter = 2
    while True:
        candidate = f"{base} ({counter}){ext}"
        if not await path_exists(candidate, location_id):
            return candidate
        counter += 1
