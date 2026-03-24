import shutil
import tempfile
from pathlib import Path

import httpx
from starlette.requests import Request
from file_hunter.core import json_ok, json_error
from file_hunter.services import update as update_svc
from file_hunter.services.restart import schedule_restart


async def check_update(request: Request):
    body = await request.json()
    key = body.get("key", "").strip()
    if not key:
        return json_error("License key is required")
    try:
        result = await update_svc.check_update(key)
    except ValueError as e:
        return json_error(str(e))
    except httpx.HTTPError:
        return json_error("Could not reach update server")
    if result.get("ok"):
        return json_ok(result["data"])
    return json_error(
        result.get("error", "Unknown error"), status=result.get("status", 400)
    )


async def install_update(request: Request):
    body = await request.json()
    key = body.get("key", "").strip()
    if not key:
        return json_error("License key is required")
    try:
        result = await update_svc.install_update(key)
    except ValueError as e:
        return json_error(str(e))
    except httpx.HTTPError:
        return json_error("Could not reach update server")
    except Exception as e:
        return json_error(f"Install failed: {e}")
    if result.get("ok"):
        return json_ok(result["data"])
    return json_error(result.get("error", "Unknown error"))


async def upload_update(request: Request):
    form = await request.form()
    upload_file = form.get("file")
    if (
        not upload_file
        or not hasattr(upload_file, "filename")
        or not upload_file.filename
    ):
        return json_error("No file provided")

    tmp_dir = Path(tempfile.mkdtemp())
    zip_path = tmp_dir / upload_file.filename
    try:
        with open(zip_path, "wb") as f:
            shutil.copyfileobj(upload_file.file, f)
    except Exception as e:
        zip_path.unlink(missing_ok=True)
        tmp_dir.rmdir()
        return json_error(f"Failed to save upload: {e}")

    try:
        result = await update_svc.install_from_zip(zip_path)
    except Exception as e:
        zip_path.unlink(missing_ok=True)
        tmp_dir.rmdir()
        return json_error(f"Install failed: {e}")

    if result.get("ok"):
        return json_ok(result["data"])
    return json_error(result.get("error", "Unknown error"))


async def restart_server(request: Request):
    schedule_restart()
    return json_ok({"message": "Server restarting…"})


async def check_release(request: Request):
    """GET /api/update/check-release — check GitHub for latest version."""
    try:
        result = await update_svc.check_github_release()
    except Exception as e:
        return json_error(f"Could not check for updates: {e}")
    return json_ok(result)


async def apply_release(request: Request):
    """POST /api/update/apply-release — download and install latest from GitHub, then restart."""
    try:
        result = await update_svc.apply_github_update()
    except Exception as e:
        return json_error(f"Update failed: {e}")
    schedule_restart()
    return json_ok({"message": f"Updated to v{result['version']}. Restarting…"})
