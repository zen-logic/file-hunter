from starlette.requests import Request
from file_hunter.core import BadRequest, json_error, json_ok, parse_int, parse_str, read_body
from file_hunter.db import read_db, execute_write
from file_hunter.services import ignore as ignore_svc


async def list_ignore_rules(request: Request):
    async with read_db() as db:
        rules = await ignore_svc.list_ignore_rules(db)
    return json_ok(rules)


async def create_ignore_rule(request: Request):
    body = await read_body(request)
    filename = parse_str(body.get("filename"), "filename")
    file_size = parse_int(body.get("file_size"), "file_size", None, minimum=0)
    location_id = parse_int(body.get("location_id"), "location_id", None)  # None = global

    if not filename or file_size is None:
        return json_error("filename and file_size are required")

    async def insert(conn, fn, fs, lid):
        return await ignore_svc.add_ignore_rule(conn, fn, fs, lid)

    rule = await execute_write(insert, filename, file_size, location_id)
    if rule is None:
        return json_error("This ignore rule already exists", status=409)
    return json_ok(rule)


async def delete_ignore_rule(request: Request):
    rule_id = request.path_params["id"]

    async def delete(conn, rid):
        return await ignore_svc.remove_ignore_rule(conn, rid)

    deleted = await execute_write(delete, rule_id)
    if not deleted:
        return json_error("Rule not found", status=404)
    return json_ok({})


def ignore_match_params(request):
    """(filename, file_size, location_id) from the query; BadRequest if the
    name or size is missing."""
    filename = request.query_params.get("filename")
    file_size = request.query_params.get("file_size")
    location_id = request.query_params.get("location_id")

    if not filename or not file_size:
        raise BadRequest("filename and file_size are required")

    file_size = parse_int(file_size, "file_size")
    location_id = parse_int(location_id, "location_id", None)
    return filename, file_size, location_id


async def check_ignore(request: Request):
    filename, file_size, location_id = ignore_match_params(request)

    async with read_db() as db:
        rule = await ignore_svc.check_file_ignored(db, filename, file_size, location_id)
    return json_ok({"ignored": rule is not None, "rule": rule})


async def count_ignore_matches(request: Request):
    filename, file_size, location_id = ignore_match_params(request)

    async with read_db() as db:
        count = await ignore_svc.count_matching_files(
            db, filename, file_size, location_id
        )
    return json_ok({"count": count})
