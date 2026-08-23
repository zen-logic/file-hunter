import sqlite3

from starlette.requests import Request
from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db, execute_write
from file_hunter.services import applications as app_svc


async def list_applications(request: Request):
    async with read_db() as db:
        apps = await app_svc.get_applications(db)
    return json_ok(apps)


async def create_application(request: Request):
    body = await request.json()
    name = (body.get("name") or "").strip()

    if not name:
        return json_error("Application name is required.")

    try:

        async def _create(conn, n):
            return await app_svc.create_application(conn, n)

        app = await execute_write(_create, name)
        return json_ok(app)
    except sqlite3.IntegrityError:
        return json_error("Application name already exists.")


async def regenerate_application_token(request: Request):
    app_id = int(request.path_params["id"])

    async def _regen(conn, aid):
        return await app_svc.regenerate_token(conn, aid)

    token = await execute_write(_regen, app_id)
    return json_ok({"token": token})


async def delete_application(request: Request):
    app_id = int(request.path_params["id"])

    async def _delete(conn, aid):
        await app_svc.delete_application(conn, aid)

    await execute_write(_delete, app_id)
    return json_ok({"deleted": True})
