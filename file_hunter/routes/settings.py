import asyncio
import logging
import re
from pathlib import Path

from starlette.requests import Request
from starlette.responses import FileResponse
from file_hunter.core import json_error, json_ok, parse_bool, parse_str, read_body
from file_hunter.db import db_writer, read_db, execute_write
from file_hunter.services import settings as settings_svc
from file_hunter.services.queue_manager import running_ops, cancel
from file_hunter.services.similarity import ensure_chromadb
from file_hunter.ws.scan import broadcast
from file_hunter import __version__
from importlib.metadata import version as pkg_version

logger = logging.getLogger("file_hunter")


async def get_settings(request: Request):
    async with read_db() as db:
        all_settings = await settings_svc.get_all_settings(db)
    return json_ok(all_settings)


async def get_version(request: Request):
    try:
        import file_hunter_pro  # noqa: F401

        pro = True
    except ImportError:
        pro = False
    return json_ok({"version": __version__, "pro": pro})


async def get_pro_status(request: Request):
    try:
        from file_hunter_pro import get_features

        try:
            pro_version = pkg_version("file-hunter-pro")
        except Exception:
            pro_version = None
        return json_ok(
            {"active": True, "features": get_features(), "version": pro_version}
        )
    except ImportError:
        return json_ok({"active": False, "features": []})


def builtin_themes_dir():
    return Path(__file__).resolve().parent.parent.parent / "static" / "css" / "themes"


def user_themes_dir():
    d = Path(__file__).resolve().parent.parent.parent / "data" / "themes"
    d.mkdir(parents=True, exist_ok=True)
    return d


async def list_themes(request: Request):
    """GET /api/themes — list available themes with built-in flag."""
    builtin_dir = builtin_themes_dir()
    user_dir = user_themes_dir()

    themes = [{"name": "default", "builtIn": True}]
    if builtin_dir.is_dir():
        for p in sorted(builtin_dir.glob("*.css"), key=lambda p: p.stem):
            themes.append({"name": p.stem, "builtIn": True})
    if user_dir.is_dir():
        for p in sorted(user_dir.glob("*.css"), key=lambda p: p.stem):
            if not any(t["name"] == p.stem for t in themes):
                themes.append({"name": p.stem, "builtIn": False})
    return json_ok(themes)


async def serve_theme_css(request: Request):
    """GET /css/themes/{name} — serve theme CSS, user themes shadow built-in."""
    name = request.path_params.get("name", "")
    if not re.match(r"^[a-z0-9-]+\.css$", name):
        return json_error("Invalid theme name.", 400)

    user_path = user_themes_dir() / name
    if user_path.is_file():
        return FileResponse(str(user_path), media_type="text/css")

    builtin_path = builtin_themes_dir() / name
    if builtin_path.is_file():
        return FileResponse(str(builtin_path), media_type="text/css")

    return json_error("Theme not found.", 404)


async def save_theme(request: Request):
    """POST /api/themes — save a user theme CSS file."""
    body = await read_body(request)
    name = parse_str(body.get("name"), "name").strip()
    css = parse_str(body.get("css"), "css")

    if not name or not css:
        return json_error("Name and css are required.", 400)

    if not re.match(r"^[a-z0-9-]+$", name):
        return json_error(
            "Theme name must be lowercase letters, numbers, and hyphens only.", 400
        )

    if name == "default":
        return json_error("Cannot overwrite the default theme.", 400)

    builtin_path = builtin_themes_dir() / f"{name}.css"
    if builtin_path.exists():
        return json_error(
            "Cannot overwrite a built-in theme. Use Save As with a new name.", 400
        )

    user_dir = user_themes_dir()
    theme_path = user_dir / f"{name}.css"

    overwrite = parse_bool(body.get("overwrite"), "overwrite")
    if theme_path.exists() and not overwrite:
        return json_error(f'Theme "{name}" already exists.', 409)

    theme_path.write_text(css, encoding="utf-8")
    return json_ok({"saved": name})


async def delete_theme(request: Request):
    """DELETE /api/themes/{name} — delete a user theme."""
    name = request.path_params.get("name", "")

    if not name or name == "default":
        return json_error("Cannot delete this theme.", 400)

    user_path = user_themes_dir() / f"{name}.css"
    if not user_path.exists():
        return json_error("Cannot delete a built-in theme.", 400)

    user_path.unlink()
    return json_ok({"deleted": name})


async def update_settings(request: Request):
    body = await read_body(request)

    # Only the settings present in the body are changed
    values = {}
    for key in ("serverName", "license_key"):
        if key in body:
            values[key] = parse_str(body[key], key)
    for key in ("showHiddenFiles", "similaritySearchEnabled"):
        if key in body:
            values[key] = "1" if parse_bool(body[key], key) else "0"
    if "similaritySearchUrl" in body:
        values["similaritySearchUrl"] = parse_str(
            body["similaritySearchUrl"], "similaritySearchUrl"
        ).strip()

    async def update(conn, v):
        for key, value in v.items():
            await settings_svc.set_setting(conn, key, value)

    await execute_write(update, values)

    if values.get("similaritySearchEnabled") == "1":
        await asyncio.to_thread(ensure_chromadb)

    async with read_db() as db:
        all_settings = await settings_svc.get_all_settings(db)
    await broadcast({"type": "settings_changed", "settings": all_settings})
    return json_ok(all_settings)


async def reset_queues(request: Request):
    """POST /api/maintenance/reset-queues — cancel all ops, clear temp DBs, queues, pending hashes."""
    # Cancel all running operations
    cancelled = 0
    for op_id in list(running_ops.keys()):
        await cancel(op_id)
        cancelled += 1

    # Wait briefly for cancellations to complete
    if cancelled:
        await asyncio.sleep(1)

    # Clear temp DBs
    temp_dir = Path(__file__).resolve().parent.parent.parent / "data" / "temp"
    temp_files_removed = 0
    if temp_dir.exists():
        for f in temp_dir.iterdir():
            try:
                f.unlink()
                temp_files_removed += 1
            except OSError:
                pass

    # Clear operation queue, pending hashes, pending backfills, and dead consolidation jobs
    async with db_writer() as db:
        await db.execute("DELETE FROM operation_queue")
    async with db_writer() as db:
        await db.execute("DELETE FROM pending_hashes")
    async with db_writer() as db:
        await db.execute("DELETE FROM pending_backfills")
    async with db_writer() as db:
        await db.execute("DELETE FROM consolidation_jobs WHERE status = 'pending'")

    logger.info(
        "Reset queues: %d ops cancelled, %d temp files removed, queues cleared",
        cancelled,
        temp_files_removed,
    )

    return json_ok(
        {
            "opsCancelled": cancelled,
            "tempFilesRemoved": temp_files_removed,
            "message": "All operations stopped and queues reset.",
        }
    )
