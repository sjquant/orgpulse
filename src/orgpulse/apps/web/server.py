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
from orgpulse.libs.metrics import Grain, group_report, person_report
from orgpulse.libs.store import Store

STATIC_DIR = Path(__file__).parent / "static"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/app.css": ("app.css", "text/css; charset=utf-8"),
    "/favicon.svg": ("favicon.svg", "image/svg+xml"),
}
MAX_PERIODS = 52


class _BadRequest(ValueError):
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
                self._send_json(HTTPStatus.OK, _meta(config))
            elif url.path == "/api/report":
                try:
                    payload = _report(config, parse_qs(url.query))
                except _BadRequest as exc:
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


def _meta(config: Config) -> dict[str, Any]:
    """Describe the groups and people the report UI can show."""

    with Store(config.database) as store:
        pull_requests = _pull_requests(config, store)
        synced_at = store.last_synced_at()
    people = config.people
    if people is None:
        merged_counts = Counter(pr.author for pr in pull_requests if pr.merged_at)
        people = tuple(
            sorted(
                merged_counts, key=lambda login: (-merged_counts[login], login.lower())
            )
        )
    return {
        "org": config.org,
        "locale": config.locale,
        "synced_at": synced_at.isoformat() if synced_at else None,
        "groups": [
            {"name": name, "members": list(members)}
            for name, members in config.groups.items()
        ],
        "people": list(people),
    }


def _report(config: Config, query: dict[str, list[str]]) -> dict[str, Any]:
    """Build the group, person, or everyone report named in the query."""

    try:
        grain = Grain(_param(query, "grain", Grain.WEEK.value))
        count = int(_param(query, "count", "12"))
        until = date.fromisoformat(_param(query, "until", _today(config).isoformat()))
    except ValueError as exc:
        raise _BadRequest(str(exc)) from exc
    if not 1 <= count <= MAX_PERIODS:
        raise _BadRequest(f"count must be between 1 and {MAX_PERIODS}")
    group = query.get("group", [None])[0]
    if group is not None and group not in config.groups:
        raise _BadRequest(f"unknown group: {group}")
    window = {
        "grain": grain,
        "count": count,
        "until": until,
        "since": config.since,
        "timezone": config.timezone,
    }
    with Store(config.database) as store:
        pull_requests = _pull_requests(config, store)
    if person := query.get("person", [""])[0]:
        team = config.team_of(person)
        return {
            "scope": {"kind": "person", "name": person, "team": team},
            **person_report(
                pull_requests,
                login=person,
                team=config.groups[team] if team else config.people,
                **window,
            ),
        }
    return {
        "scope": {"kind": "group" if group else "everyone", "name": group},
        **group_report(
            pull_requests,
            people=config.groups[group] if group else config.people,
            **window,
        ),
    }


def _pull_requests(config: Config, store: Store) -> list[PullRequest]:
    return [
        pr for pr in store.pull_requests() if config.includes_repository(pr.repository)
    ]


def _param(query: dict[str, list[str]], name: str, default: str) -> str:
    return query.get(name, [default])[0]


def _today(config: Config) -> date:
    return datetime.now(config.timezone).date()
