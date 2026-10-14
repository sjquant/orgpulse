from __future__ import annotations

import json
from collections import Counter
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from orgpulse.common.config import Config
from orgpulse.common.models import PullRequest
from orgpulse.libs.metrics import Grain, build_report
from orgpulse.libs.store import Store

STATIC_DIR = Path(__file__).parent / "static"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/app.css": ("app.css", "text/css; charset=utf-8"),
    "/favicon.svg": ("favicon.svg", "image/svg+xml"),
}
MAX_PERIODS = 260


class BadRequest(ValueError):
    """Raised when report query parameters are invalid."""


def make_server(config: Config, host: str, port: int) -> ThreadingHTTPServer:
    """Build the local report server; call `serve_forever()` to run it."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            url = urlsplit(self.path)
            if url.path in STATIC_FILES:
                filename, content_type = STATIC_FILES[url.path]
                self._send(
                    HTTPStatus.OK, (STATIC_DIR / filename).read_bytes(), content_type
                )
            elif url.path == "/api/meta":
                self._send_json(HTTPStatus.OK, meta(config))
            elif url.path == "/api/report":
                try:
                    payload = report(config, parse_qs(url.query))
                except BadRequest as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return
                self._send_json(HTTPStatus.OK, payload)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def log_message(self, format: str, *args: Any) -> None:
            pass

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode()
            self._send(status, body, "application/json; charset=utf-8")

        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return ThreadingHTTPServer((host, port), Handler)


def meta(config: Config) -> dict[str, Any]:
    """Describe who and what can be selected in the report UI."""

    with Store(config.database) as store:
        pull_requests = _pull_requests(config, store)
        synced_at = store.last_synced_at()
    merged_counts = Counter(pr.author for pr in pull_requests if pr.merged_at)
    authors = sorted(
        {pr.author for pr in pull_requests},
        key=lambda login: (-merged_counts[login], login.lower()),
    )
    return {
        "org": config.org,
        "locale": config.locale,
        "synced_at": synced_at.isoformat() if synced_at else None,
        "people": authors,
        "groups": {name: list(members) for name, members in config.groups.items()},
    }


def report(config: Config, query: dict[str, list[str]]) -> dict[str, Any]:
    """Build the report for the people, grain, and range named in the query."""

    with Store(config.database) as store:
        pull_requests = _pull_requests(config, store)
    try:
        grain = Grain(_param(query, "grain", Grain.WEEK.value))
        count = int(_param(query, "count", "13"))
        until = date.fromisoformat(_param(query, "until", _today(config).isoformat()))
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if not 1 <= count <= MAX_PERIODS:
        raise BadRequest(f"count must be between 1 and {MAX_PERIODS}")
    return build_report(
        pull_requests,
        people=_people(config, query),
        grain=grain,
        count=count,
        until=until,
        timezone=config.timezone,
        ai=config.ai,
    )


def _pull_requests(config: Config, store: Store) -> list[PullRequest]:
    return [
        pr for pr in store.pull_requests() if config.includes_repository(pr.repository)
    ]


def _people(config: Config, query: dict[str, list[str]]) -> list[str] | None:
    if "group" in query:
        group = _param(query, "group", "")
        if group not in config.groups:
            raise BadRequest(f"unknown group: {group}")
        return list(config.groups[group])
    if "people" in query:
        return [login for login in _param(query, "people", "").split(",") if login]
    return None


def _param(query: dict[str, list[str]], name: str, default: str) -> str:
    return query.get(name, [default])[0]


def _today(config: Config) -> date:
    return datetime.now(config.timezone).date()
