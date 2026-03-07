from urllib.parse import parse_qs
from starlette.responses import JSONResponse
from file_hunter.db import get_db
from file_hunter.services.auth import validate_session
from file_hunter.extensions import get_public_ws_paths

# Paths that do not require authentication
_PUBLIC_PATHS = {
    "/api/auth/status",
    "/api/auth/setup",
    "/api/auth/login",
    "/api/pro/status",
}


class AuthMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            path = scope.get("path", "")

            # Static files — anything not starting with /api/
            if not path.startswith("/api/"):
                await self.app(scope, receive, send)
                return

            # Public auth endpoints
            if path in _PUBLIC_PATHS:
                await self.app(scope, receive, send)
                return

            # Require Bearer token (header or query parameter)
            headers = dict(scope.get("headers", []))
            auth = headers.get(b"authorization", b"").decode()
            token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""

            # Fall back to ?token= query parameter (for <img src> etc.)
            if not token:
                qs = scope.get("query_string", b"").decode()
                params = parse_qs(qs)
                token = params.get("token", [""])[0]

            if not token:
                response = JSONResponse(
                    {"ok": False, "error": "Authentication required."},
                    status_code=401,
                )
                await response(scope, receive, send)
                return

            db = await get_db()
            user = await validate_session(db, token)
            if not user:
                response = JSONResponse(
                    {"ok": False, "error": "Invalid or expired session."},
                    status_code=401,
                )
                await response(scope, receive, send)
                return

            scope["user"] = user
            await self.app(scope, receive, send)

        elif scope["type"] == "websocket":
            # Agent WS and extension-registered WS paths handle their own auth
            ws_path = scope.get("path", "")
            if ws_path == "/ws/agent" or ws_path in get_public_ws_paths():
                await self.app(scope, receive, send)
                return

            # Extract token from query string
            qs = scope.get("query_string", b"").decode()
            params = parse_qs(qs)
            token = params.get("token", [""])[0]

            if not token:
                await self._reject_ws(scope, receive, send, 4001)
                return

            db = await get_db()
            user = await validate_session(db, token)
            if not user:
                await self._reject_ws(scope, receive, send, 4001)
                return

            scope["user"] = user
            await self.app(scope, receive, send)

        else:
            await self.app(scope, receive, send)

    async def _reject_ws(self, scope, receive, send, code):
        """Accept then immediately close the WebSocket with an error code."""
        await send({"type": "websocket.accept"})
        await send({"type": "websocket.close", "code": code})
