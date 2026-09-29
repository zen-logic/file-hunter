import sqlite3

from starlette.requests import Request
from file_hunter.core import json_error, json_ok, parse_str, read_body
from file_hunter.db import read_db, execute_write
from file_hunter.services import auth as auth_svc
from file_hunter.services.settings import get_setting


async def auth_status(request: Request):
    async with read_db() as db:
        count = await auth_svc.user_count(db)
        server_name = await get_setting(db, "serverName") or ""
    return json_ok({"needsSetup": count == 0, "serverName": server_name})


async def auth_setup(request: Request):
    body = await read_body(request)
    username = parse_str(body.get("username"), "username").strip()
    password = parse_str(body.get("password"), "password")
    display_name = parse_str(body.get("displayName"), "displayName").strip()

    if not username or not password:
        return json_error("Username and password are required.")

    async with read_db() as db:
        count = await auth_svc.user_count(db)
    if count > 0:
        return json_error("Setup already completed.", status=403)

    async def create(conn, u, p, d):
        user = await auth_svc.create_user(conn, u, p, d)
        token = await auth_svc.create_session(conn, user["id"])
        return {"token": token, "user": user}

    result = await execute_write(create, username, password, display_name)
    return json_ok(result)


async def auth_login(request: Request):
    body = await read_body(request)
    username = parse_str(body.get("username"), "username").strip()
    password = parse_str(body.get("password"), "password")

    if not username or not password:
        return json_error("Username and password are required.")

    async with read_db() as db:
        user = await auth_svc.authenticate(db, username, password)
    if not user:
        return json_error("Invalid username or password.", status=401)

    async def session(conn, uid):
        return await auth_svc.create_session(conn, uid)

    token = await execute_write(session, user["id"])
    return json_ok({"token": token, "user": user})


async def auth_logout(request: Request):
    auth_header = request.headers.get("authorization", "")
    token = (
        auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else ""
    )
    if token:

        async def delete(conn, t):
            await auth_svc.delete_session(conn, t)

        await execute_write(delete, token)
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
    body = await read_body(request)
    username = parse_str(body.get("username"), "username").strip()
    password = parse_str(body.get("password"), "password")
    display_name = parse_str(body.get("displayName"), "displayName").strip()

    if not username or not password:
        return json_error("Username and password are required.")

    try:

        async def create(conn, u, p, d):
            return await auth_svc.create_user(conn, u, p, d)

        user = await execute_write(create, username, password, display_name)
        return json_ok(user)
    except sqlite3.IntegrityError:
        return json_error("Username already exists.")


async def update_user(request: Request):
    user_id = int(request.path_params["id"])
    body = await read_body(request)

    kwargs = {}
    if "username" in body:
        val = parse_str(body["username"], "username").strip()
        if not val:
            return json_error("Username cannot be empty.")
        kwargs["username"] = val
    if "password" in body:
        password = parse_str(body["password"], "password")
        if not password:
            return json_error("Password cannot be empty.")
        kwargs["password"] = password
    if "displayName" in body:
        kwargs["display_name"] = parse_str(body["displayName"], "displayName").strip()

    if not kwargs:
        return json_error("Nothing to update.")

    try:

        async def update(conn, uid, **kw):
            await auth_svc.update_user(conn, uid, **kw)

        await execute_write(update, user_id, **kwargs)
        return json_ok({"updated": True})
    except sqlite3.IntegrityError:
        return json_error("Username already exists.")


async def delete_user(request: Request):
    user_id = int(request.path_params["id"])
    current_user = request.scope.get("user")
    if current_user and current_user["id"] == user_id:
        return json_error("Cannot delete your own account.")

    async def delete(conn, uid):
        await auth_svc.delete_user(conn, uid)

    await execute_write(delete, user_id)
    return json_ok({"deleted": True})
