import logging
from pathlib import Path

from starlette.requests import Request
from file_hunter.core import json_ok
from file_hunter.db import db_writer, read_db, execute_write
from file_hunter.services import settings as settings_svc
from file_hunter.ws.scan import broadcast
from file_hunter import __version__

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
        from importlib.metadata import version as pkg_version

        try:
            pro_version = pkg_version("file-hunter-pro")
        except Exception:
            pro_version = None
        return json_ok(
            {"active": True, "features": get_features(), "version": pro_version}
        )
    except ImportError:
        return json_ok({"active": False, "features": []})


async def update_settings(request: Request):
    body = await request.json()

    async def _update(conn, b):
        if "serverName" in b:
            await settings_svc.set_setting(conn, "serverName", b["serverName"])
        if "license_key" in b:
            await settings_svc.set_setting(conn, "license_key", b["license_key"])
        if "showHiddenFiles" in b:
            await settings_svc.set_setting(
                conn, "showHiddenFiles", "1" if b["showHiddenFiles"] else "0"
            )

    await execute_write(_update, body)

    async with read_db() as db:
        all_settings = await settings_svc.get_all_settings(db)
    await broadcast({"type": "settings_changed", "settings": all_settings})
    return json_ok(all_settings)


async def reset_queues(request: Request):
    """POST /api/maintenance/reset-queues — cancel all ops, clear temp DBs, queues, pending hashes."""
    from file_hunter.services.queue_manager import _running_ops, cancel

    # Cancel all running operations
    cancelled = 0
    for op_id in list(_running_ops.keys()):
        await cancel(op_id)
        cancelled += 1

    # Wait briefly for cancellations to complete
    if cancelled:
        import asyncio

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

    # Clear operation queue and pending hashes
    async with db_writer() as db:
        await db.execute("DELETE FROM operation_queue")
    async with db_writer() as db:
        await db.execute("DELETE FROM pending_hashes")

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
