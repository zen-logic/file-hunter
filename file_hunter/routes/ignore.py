from starlette.requests import Request
from file_hunter.core import json_ok, json_error
from file_hunter.db import read_db, execute_write
from file_hunter.services import ignore as ignore_svc


async def list_ignore_rules(request: Request):
    async with read_db() as db:
        rules = await ignore_svc.list_ignore_rules(db)
    return json_ok(rules)


async def create_ignore_rule(request: Request):
    body = await request.json()
    filename = body.get("filename")
    file_size = body.get("file_size")
    location_id = body.get("location_id")  # None = global

    if not filename or file_size is None:
        return json_error("filename and file_size are required")

    async def _insert(conn, fn, fs, lid):
        return await ignore_svc.add_ignore_rule(conn, fn, fs, lid)

    rule = await execute_write(_insert, filename, file_size, location_id)
    if rule is None:
        return json_error("This ignore rule already exists", status=409)
    return json_ok(rule)


async def delete_ignore_rule(request: Request):
    rule_id = request.path_params["id"]

    async def _delete(conn, rid):
        return await ignore_svc.remove_ignore_rule(conn, rid)

    deleted = await execute_write(_delete, rule_id)
    if not deleted:
        return json_error("Rule not found", status=404)
    return json_ok({})


async def check_ignore(request: Request):
    filename = request.query_params.get("filename")
    file_size = request.query_params.get("file_size")
    location_id = request.query_params.get("location_id")

    if not filename or not file_size:
        return json_error("filename and file_size are required")

    file_size = int(file_size)
    location_id = int(location_id) if location_id else None

    async with read_db() as db:
        rule = await ignore_svc.check_file_ignored(db, filename, file_size, location_id)
    return json_ok({"ignored": rule is not None, "rule": rule})


async def count_ignore_matches(request: Request):
    filename = request.query_params.get("filename")
    file_size = request.query_params.get("file_size")
    location_id = request.query_params.get("location_id")

    if not filename or not file_size:
        return json_error("filename and file_size are required")

    file_size = int(file_size)
    location_id = int(location_id) if location_id else None

    async with read_db() as db:
        count = await ignore_svc.count_matching_files(db, filename, file_size, location_id)
    return json_ok({"count": count})
