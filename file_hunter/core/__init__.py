import asyncio
import math
import re

from starlette.responses import JSONResponse

# Re-export from file_hunter_core so existing imports continue to work
from file_hunter_core.classify import classify_file, format_size  # noqa: F401


def json_ok(data, status=200) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data}, status_code=status)


async def cancel_task(task):
    """Cancel task, if it's still running, and wait for it to finish."""
    if task and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


def json_error(message: str, status=400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message}, status_code=status)


class RequestError(Exception):
    """Raised from a route; the app answers with json_error(message, status)."""

    status = 400


class BadRequest(RequestError):
    """Request input that can't be used: 400."""


class NotFound(RequestError):
    """The requested record doesn't exist: 404."""

    status = 404


REQUIRED = object()


def parse_int(value, name, default=REQUIRED, *, prefix=None, minimum=None):
    """A whole number from request input (query string or JSON body).

    prefix ("loc-", "fld-") is stripped first. Missing or blank gives
    default, or BadRequest when there's no default; anything that isn't a
    whole number, or is below minimum, is a BadRequest.
    """
    if value is None or str(value).strip() == "":
        if default is REQUIRED:
            raise BadRequest(f"{name} is required.")
        return default
    text = str(value).strip()
    if prefix and text.startswith(prefix):
        text = text[len(prefix):]
    try:
        number = int(text)
    except ValueError:
        raise BadRequest(f"{name} must be a whole number.") from None
    if minimum is not None and number < minimum:
        raise BadRequest(f"{name} must be at least {minimum}.")
    return number


def parse_float(value, name, default=REQUIRED):
    """A finite number from request input; missing or blank gives default."""
    if value is None or str(value).strip() == "":
        if default is REQUIRED:
            raise BadRequest(f"{name} is required.")
        return default
    try:
        number = float(str(value).strip())
    except ValueError:
        raise BadRequest(f"{name} must be a number.") from None
    if not math.isfinite(number):
        raise BadRequest(f"{name} must be a number.")
    return number


def parse_str(value, name, default=""):
    """Text from request input; missing (None) gives default."""
    if value is None:
        return default
    if not isinstance(value, str):
        raise BadRequest(f"{name} must be text.")
    return value


def parse_bool(value, name, default=False):
    """true/false from a JSON body; missing (None) gives default."""
    if value is None:
        return default
    if not isinstance(value, bool):
        raise BadRequest(f"{name} must be true or false.")
    return value


NODE_ID = re.compile(r"(loc|fld)-\d+")


def parse_node_id(value, name, default=REQUIRED):
    """A location or folder id, "loc-N" or "fld-N"; missing or blank gives
    default, or BadRequest when there's no default."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        if default is REQUIRED:
            raise BadRequest(f"{name} is required.")
        return default
    if not isinstance(value, str) or not NODE_ID.fullmatch(value.strip()):
        raise BadRequest(f"{name} must be a location or folder id.")
    return value.strip()


def parse_int_array(value, name, default=None) -> list[int]:
    """A JSON array of whole numbers; missing (None) gives default."""
    if value is None:
        return [] if default is None else default
    if not isinstance(value, list):
        raise BadRequest(f"{name} must be a list.")
    if any(isinstance(v, bool) for v in value):
        raise BadRequest(f"{name} must be whole numbers.")
    return [parse_int(v, name) for v in value]


def parse_str_array(value, name, default=None) -> list[str]:
    """A JSON array of text; missing (None) gives default."""
    if value is None:
        return [] if default is None else default
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise BadRequest(f"{name} must be a list of text.")
    return value


def parse_tag_input(value, name):
    """Tags as sent by the UI: comma-separated text or a list of text."""
    if isinstance(value, list):
        return parse_str_array(value, name)
    return parse_str(value, name, None)


async def read_body(request) -> dict:
    """The request's JSON body, which must be an object."""
    try:
        body = await request.json()
    except ValueError:
        raise BadRequest("Request body must be JSON.") from None
    if not isinstance(body, dict):
        raise BadRequest("Request body must be a JSON object.")
    return body


def parse_int_list(value, name) -> list[int]:
    """Comma-separated whole numbers; blank entries are skipped."""
    return [parse_int(x, name) for x in str(value or "").split(",") if x.strip()]


class ProgressTracker(dict):
    """Dict subclass for pollable progress state.

    Drop-in replacement for the module-level progress dicts used by
    import, dup exclude, and repair.  Inherits from dict so
    all existing access patterns work unchanged:
        progress["status"] = "running"
        progress.update(status="error", error=str(e))
        dict(progress)  # snapshot for JSON response
    """

    def __init__(self, **fields):
        super().__init__(status="idle", error=None, **fields)
        self.defaults = dict(self)

    def reset(self):
        """Restore all fields to their initial values."""
        self.update(self.defaults)

    def snapshot(self) -> dict:
        """Return a plain dict copy (for JSON serialization)."""
        return dict(self)

    @property
    def is_running(self) -> bool:
        return self["status"] not in ("idle", "complete", "error")
