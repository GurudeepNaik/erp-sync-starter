"""Vercel entry point for the chaos playground: the FastAPI app as one Python function.

Each run opens its own schema, so the database must be reachable without a transaction pooler
(search_path is a session setting): prefer Neon's non-pooling URL.
"""

import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from erpsync.playground import create_playground_app


def _database_url() -> str | None:
    for name in (
        "POSTGRES_URL_NON_POOLING",
        "DATABASE_URL_UNPOOLED",
        "DATABASE_URL",
        "POSTGRES_URL",
    ):
        if os.environ.get(name):
            return os.environ[name]
    return None


def _restore_path(inner: Any) -> Any:
    """vercel.json rewrites /api/<path> to this function as /api/index?__path=<path>. Put the
    original path back before FastAPI routes the request."""

    async def asgi(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http" and scope["path"].rstrip("/") in ("/api/index", "/api/index.py"):
            query = parse_qsl(scope.get("query_string", b"").decode(), keep_blank_values=True)
            original = [v for k, v in query if k == "__path"]
            if original:
                path = "/api/" + original[0].lstrip("/")
                rest = urlencode([(k, v) for k, v in query if k != "__path"])
                scope = {
                    **scope,
                    "path": path,
                    "raw_path": path.encode(),
                    "query_string": rest.encode(),
                }
        await inner(scope, receive, send)

    return asgi


_api = create_playground_app(_database_url(), budget_s=45)

_dashboard = Path(__file__).resolve().parent.parent / "public"
if _dashboard.is_dir():
    _api.mount("/", StaticFiles(directory=_dashboard, html=True), name="dashboard")

app = _restore_path(_api)
