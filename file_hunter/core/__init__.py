from starlette.responses import JSONResponse

# Re-export from file_hunter_core so existing imports continue to work
from file_hunter_core.classify import classify_file, format_size  # noqa: F401


def json_ok(data, status=200) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data}, status_code=status)


def json_error(message: str, status=400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message}, status_code=status)


class ProgressTracker(dict):
    """Dict subclass for pollable progress state.

    Drop-in replacement for the module-level _progress dicts used by
    import, fast scan, dup exclude, and repair.  Inherits from dict so
    all existing access patterns work unchanged:
        _progress["status"] = "running"
        _progress.update(status="error", error=str(e))
        dict(_progress)  # snapshot for JSON response
    """

    def __init__(self, **fields):
        super().__init__(status="idle", error=None, **fields)
        self._defaults = dict(self)

    def reset(self):
        """Restore all fields to their initial values."""
        self.update(self._defaults)

    def snapshot(self) -> dict:
        """Return a plain dict copy (for JSON serialization)."""
        return dict(self)

    @property
    def is_running(self) -> bool:
        return self["status"] not in ("idle", "complete", "error")
