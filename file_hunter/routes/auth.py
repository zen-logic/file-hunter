from starlette.requests import Request
from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db, execute_write
from file_hunter.services import auth as auth_svc


async def auth_status(request: Request):
    async with read_db() as db:
        count = await auth_svc.user_count(db)
        from file_hunter.services.settings import get_setting

        server_name = await get_setting(db, "serverName") or ""
    return json_ok({"needsSetup": count == 0, "serverName": server_name})


async def auth_setup(request: Request):
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    display_name = (body.get("displayName") or "").strip()

    if not username or not password:
        return json_error("Username and password are required.")

    async with read_db() as db:
        count = await auth_svc.user_count(db)
    if count > 0:
        return json_error("Setup already completed.", status=403)

    async def _create(conn, u, p, d):
        user = await auth_svc.create_user(conn, u, p, d)
        token = await auth_svc.create_session(conn, user["id"])
        return {"token": token, "user": user}

    result = await execute_write(_create, username, password, display_name)
    return json_ok(result)


async def auth_login(request: Request):
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    if not username or not password:
        return json_error("Username and password are required.")

    async with read_db() as db:
        user = await auth_svc.authenticate(db, username, password)
    if not user:
        return json_error("Invalid username or password.", status=401)

    async def _session(conn, uid):
        return await auth_svc.create_session(conn, uid)

    token = await execute_write(_session, user["id"])
    return json_ok({"token": token, "user": user})


async def auth_logout(request: Request):
    auth_header = request.headers.get("authorization", "")
    token = (
        auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else ""
    )
    if token:

        async def _delete(conn, t):
            await auth_svc.delete_session(conn, t)

        await execute_write(_delete, token)
    return json_ok({"loggedOut": True})


async def auth_me(request: Request):
    user = request.scope.get("user")
    if not user:
        return json_error("Not authenticated.", status=401)
    return json_ok(user)


async def list_users(request: Request):
    async with read_db() as db:
        users = await auth_svc.get_users(db)
    return json_ok(users)


async def create_user(request: Request):
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    display_name = (body.get("displayName") or "").strip()

    if not username or not password:
        return json_error("Username and password are required.")

    import sqlite3

    try:

        async def _create(conn, u, p, d):
            return await auth_svc.create_user(conn, u, p, d)

        user = await execute_write(_create, username, password, display_name)
        return json_ok(user)
    except sqlite3.IntegrityError:
        return json_error("Username already exists.")


async def update_user(request: Request):
    user_id = int(request.path_params["id"])
    body = await request.json()

    kwargs = {}
    if "username" in body:
        val = (body["username"] or "").strip()
        if not val:
            return json_error("Username cannot be empty.")
        kwargs["username"] = val
    if "password" in body:
        if not body["password"]:
            return json_error("Password cannot be empty.")
        kwargs["password"] = body["password"]
    if "displayName" in body:
        kwargs["display_name"] = (body["displayName"] or "").strip()

    if not kwargs:
        return json_error("Nothing to update.")

    import sqlite3

    try:

        async def _update(conn, uid, **kw):
            await auth_svc.update_user(conn, uid, **kw)

        await execute_write(_update, user_id, **kwargs)
        return json_ok({"updated": True})
    except sqlite3.IntegrityError:
        return json_error("Username already exists.")


async def delete_user(request: Request):
    user_id = int(request.path_params["id"])
    current_user = request.scope.get("user")
    if current_user and current_user["id"] == user_id:
        return json_error("Cannot delete your own account.")

    async def _delete(conn, uid):
        await auth_svc.delete_user(conn, uid)

    await execute_write(_delete, user_id)
    return json_ok({"deleted": True})
