from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime, timezone
from functools import cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from urllib.parse import parse_qs, urlsplit

from orgpulse.common.config import Config
from orgpulse.common.errors import OrgpulseError
from orgpulse.libs.metrics import Grain, group_report, person_report
from orgpulse.libs.store import Store

STATIC_DIR = Path(__file__).parent / "static"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/app.css": ("app.css", "text/css; charset=utf-8"),
    "/favicon.svg": ("favicon.svg", "image/svg+xml"),
}
MAX_PERIODS = {Grain.WEEK: 156, Grain.MONTH: 36}
MAX_CUSTOM_DAYS = 1096


class _BadRequest(ValueError):
    """Raised when report query parameters are invalid."""


SyncRunner = Callable[[Callable[[str, int], None]], int]
AvatarLoader = Callable[[tuple[str, ...]], dict[str, str]]


class _SyncJob:
    """One background writer per dashboard; readers keep using committed data."""

    def __init__(self, runner: SyncRunner | None) -> None:
        self.runner = runner
        self.lock = Lock()
        self.status: dict[str, Any] = {
            "available": runner is not None,
            "state": "idle",
            "repositories": 0,
            "processed_prs": 0,
            "last_repository": None,
            "error": None,
            "started_at": None,
            "finished_at": None,
        }

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return dict(self.status)

    def start(self) -> bool:
        with self.lock:
            if self.runner is None or self.status["state"] == "running":
                return False
            self.status.update(
                state="running",
                repositories=0,
                processed_prs=0,
                last_repository=None,
                error=None,
                started_at=datetime.now(timezone.utc).isoformat(),
                finished_at=None,
            )
            Thread(target=self._run, daemon=True).start()
            return True

    def _progress(self, repository: str, count: int) -> None:
        with self.lock:
            self.status["repositories"] += 1
            self.status["processed_prs"] += count
            self.status["last_repository"] = repository

    def _run(self) -> None:
        try:
            assert self.runner is not None
            count = self.runner(self._progress)
            with self.lock:
                self.status.update(state="success", processed_prs=count)
        except Exception as exc:
            # Unexpected exception details may contain configuration or credentials.
            error = (
                str(exc)
                if isinstance(exc, OrgpulseError)
                else "Sync failed. Please retry."
            )
            with self.lock:
                self.status.update(state="error", error=error)
        finally:
            with self.lock:
                self.status["finished_at"] = datetime.now(timezone.utc).isoformat()


def make_server(
    config: Config,
    host: str,
    port: int,
    *,
    sync_runner: SyncRunner | None = None,
    avatar_loader: AvatarLoader | None = None,
) -> ThreadingHTTPServer:
    """Build the local report server; CLI supplies feature workflow callbacks."""
    job = _SyncJob(sync_runner)

    @cache
    def profiles(people: tuple[str, ...]) -> dict[str, str]:
        if avatar_loader is None:
            return {}
        try:
            return avatar_loader(people)
        except OrgpulseError:
            return {}  # Profile lookup must not prevent offline reporting.

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            url = urlsplit(self.path)
            if url.path in STATIC_FILES:
                filename, content_type = STATIC_FILES[url.path]
                self._send(
                    HTTPStatus.OK, (STATIC_DIR / filename).read_bytes(), content_type
                )
            elif url.path == "/api/meta":
                meta = _meta(config)
                meta["avatars"] = profiles(tuple(meta["people"]))
                self._send_json(HTTPStatus.OK, meta)
            elif url.path == "/api/sync":
                self._send_json(HTTPStatus.OK, job.snapshot())
            elif url.path == "/api/report":
                try:
                    payload = _report(config, parse_qs(url.query))
                except _BadRequest as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return
                self._send_json(HTTPStatus.OK, payload)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_POST(self) -> None:
            if urlsplit(self.path).path != "/api/sync":
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            # A page on another origin must not be able to trigger local GitHub work.
            assert isinstance(self.server, ThreadingHTTPServer)
            authority = self.headers.get("Host", "")
            expected = {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }
            origin = self.headers.get("Origin")
            if (
                authority not in expected
                or (origin and origin != f"http://{authority}")
                or self.headers.get("Sec-Fetch-Site") == "cross-site"
            ):
                self._send_json(
                    HTTPStatus.FORBIDDEN, {"error": "same-origin requests only"}
                )
                return
            if self.headers.get_content_type() != "application/json":
                self._send_json(
                    HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                    {"error": "application/json required"},
                )
                return
            status = job.snapshot()
            if not status["available"]:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE, {"error": "sync is not configured"}
                )
            elif not job.start():
                self._send_json(
                    HTTPStatus.CONFLICT, {"error": "sync is already running"}
                )
            else:
                self._send_json(HTTPStatus.ACCEPTED, job.snapshot())

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
        synced_at = store.last_synced_at()
        people = config.members(None)
        if people is None:
            merged = Counter()
            for author, repository, count in store.merged_counts():
                if config.includes_repository(repository):
                    merged[author] += count
            people = sorted(merged, key=lambda login: (-merged[login], login.lower()))
    return {
        "org": config.org,
        "locale": config.locale,
        "synced_at": synced_at.isoformat() if synced_at else None,
        "today": datetime.now(config.timezone).date().isoformat(),
        "groups": list(config.groups),
        "people": list(people),
    }


def _report(config: Config, query: dict[str, list[str]]) -> dict[str, Any]:
    """Build the group, person, or everyone report named in the query."""

    today = datetime.now(config.timezone).date()
    try:
        grain = Grain(_param(query, "grain", Grain.WEEK.value))
        count = int(_param(query, "count", "12"))
        until = date.fromisoformat(_param(query, "until", today.isoformat()))
        start = date.fromisoformat(query["from"][0]) if "from" in query else None
    except ValueError as exc:
        raise _BadRequest(str(exc)) from exc
    if until > today:
        raise _BadRequest("until must not be in the future")
    if start is not None:
        if "until" not in query:
            raise _BadRequest("custom ranges require both from and until")
        if start > until:
            raise _BadRequest("from must be on or before until")
        if (until - start).days + 1 > MAX_CUSTOM_DAYS:
            raise _BadRequest("custom ranges must be at most 3 years (1096 days)")
    elif not 1 <= count <= MAX_PERIODS[grain]:
        raise _BadRequest(f"count must be between 1 and {MAX_PERIODS[grain]}")
    group = _param(query, "group", "") or None
    if group is not None and group not in config.groups:
        raise _BadRequest(f"unknown group: {group}")
    person = _param(query, "person", "")
    window = {
        "grain": grain,
        "count": count,
        "until": until,
        "since": config.since,
        "timezone": config.timezone,
        "start": start,
    }
    with Store(config.database) as store:
        pull_requests = [
            pr
            for pr in store.pull_requests()
            if config.includes_repository(pr.repository)
        ]
    if person:
        team = config.team_of(person)
        return {
            "scope": {"kind": "person", "name": person, "team": team},
            **person_report(
                pull_requests, login=person, team=config.members(team), **window
            ),
        }
    return {
        "scope": {"kind": "group" if group else "everyone", "name": group},
        **group_report(pull_requests, people=config.members(group), **window),
    }


def _param(query: dict[str, list[str]], name: str, default: str) -> str:
    return query.get(name, [default])[0]
