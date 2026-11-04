from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime, timezone
from functools import cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit
from zipfile import ZIP_DEFLATED, ZipFile

from orgpulse.common.config import Config
from orgpulse.common.errors import OrgpulseError
from orgpulse.common.models import PullRequest, Review
from orgpulse.libs.html_export import inline_github_avatar, render_report_html
from orgpulse.libs.metrics import (
    DISTRIBUTION_METRICS,
    Grain,
    group_report,
    person_report,
)
from orgpulse.libs.review_metrics import add_review_metrics
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
            outcome = {"state": "success", "processed_prs": self.runner(self._progress)}
        except Exception as exc:
            error = (
                str(exc)
                if isinstance(exc, OrgpulseError)
                else "Sync failed. Please retry."
            )
            outcome = {"state": "error", "error": error}
        with self.lock:
            self.status.update(
                **outcome, finished_at=datetime.now(timezone.utc).isoformat()
            )


def make_server(
    config: Config,
    host: str,
    port: int,
    *,
    sync_runner: SyncRunner | None = None,
    avatar_loader: AvatarLoader | None = None,
) -> ThreadingHTTPServer:
    """Serve local reports and optional GitHub refreshes."""
    job = _SyncJob(sync_runner)

    @cache
    def profiles(people: tuple[str, ...]) -> dict[str, str]:
        if avatar_loader is None or not people:
            return {}
        try:
            return avatar_loader(people)
        except OrgpulseError:
            return {}  # Profile lookup must not prevent offline reporting.

    class Handler(BaseHTTPRequestHandler):
        def _local_host(self) -> bool:
            assert isinstance(self.server, ThreadingHTTPServer)
            return self.headers.get("Host", "").lower() in {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }

        def do_GET(self) -> None:
            if not self._local_host():
                self._send_json(
                    HTTPStatus.FORBIDDEN, {"error": "localhost requests only"}
                )
                return
            url = urlsplit(self.path)
            if url.path in STATIC_FILES:
                filename, content_type = STATIC_FILES[url.path]
                self._send(
                    HTTPStatus.OK, (STATIC_DIR / filename).read_bytes(), content_type
                )
            elif url.path == "/api/meta":
                self._send_json(HTTPStatus.OK, _meta(config))
            elif url.path == "/api/profiles":
                people = _meta(config)["people"]
                self._send_json(HTTPStatus.OK, {"avatars": profiles(tuple(people))})
            elif url.path == "/api/sync":
                self._send_json(HTTPStatus.OK, job.snapshot())
            elif url.path == "/api/export":
                try:
                    body, filename, content_type = export_reports(
                        config, parse_qs(url.query), profiles
                    )
                except _BadRequest as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return
                self._send(HTTPStatus.OK, body, content_type, filename=filename)
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
            authority = self.headers.get("Host", "")
            origin = self.headers.get("Origin")
            if (
                not self._local_host()
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

        def _send(
            self,
            status: HTTPStatus,
            body: bytes,
            content_type: str,
            *,
            filename: str | None = None,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            if filename is not None:
                self.send_header(
                    "Content-Disposition", f'attachment; filename="{filename}"'
                )
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
            for review in store.reviews():
                if config.includes_repository(review.repository):
                    merged.setdefault(review.reviewer, 0)
            people = sorted(merged, key=lambda login: (-merged[login], login.lower()))
    return {
        "org": config.org,
        "locale": config.locale,
        "synced_at": synced_at.isoformat() if synced_at else None,
        "today": datetime.now(config.timezone).date().isoformat(),
        "groups": list(config.groups),
        "people": list(people),
    }


def _report(
    config: Config,
    query: dict[str, list[str]],
    *,
    pull_requests: list[PullRequest] | None = None,
    reviews: list[Review] | None = None,
    review_available: bool | None = None,
) -> dict[str, Any]:
    """Build the group, person, or everyone report named in the query."""

    today = datetime.now(config.timezone).date()
    team_statistic = _param(query, "team_statistic", "mean")
    if team_statistic not in ("mean", "median"):
        raise _BadRequest("team_statistic must be mean or median")
    raw_analysis = _param(query, "analysis", "{}")
    if len(raw_analysis) > 2048:
        raise _BadRequest("analysis options are too long")
    try:
        analysis = json.loads(raw_analysis)
    except ValueError as exc:
        raise _BadRequest("invalid analysis options") from exc
    if not isinstance(analysis, dict) or any(
        metric not in DISTRIBUTION_METRICS for metric in analysis
    ):
        raise _BadRequest("analysis only supports distribution metrics")
    for options in analysis.values():
        if not isinstance(options, dict) or set(options) - {
            "statistic",
            "exclude_outliers",
        }:
            raise _BadRequest("invalid analysis settings")
        if options.get("statistic", "median") not in ("mean", "median"):
            raise _BadRequest("statistic must be mean or median")
        if not isinstance(options.get("exclude_outliers", False), bool):
            raise _BadRequest("exclude_outliers must be a boolean")
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
    window: dict[str, Any] = {
        "grain": grain,
        "count": count,
        "until": until,
        "since": config.since,
        "timezone": config.timezone,
        "start": start,
        "analysis": analysis,
        "team_statistic": team_statistic,
    }
    if pull_requests is None or reviews is None:
        with Store(config.database) as store:
            pull_requests = (
                store.pull_requests() if pull_requests is None else pull_requests
            )
            reviews = store.reviews() if reviews is None else reviews
            required = [
                name
                for name in store.repository_names()
                if config.includes_repository(name)
            ]
            review_available = bool(required) and all(
                store.reviews_synced_at(name, since=config.since) for name in required
            )
    pull_requests = [
        pr for pr in pull_requests if config.includes_repository(pr.repository)
    ]
    reviews = [
        review for review in reviews if config.includes_repository(review.repository)
    ]
    team = config.team_of(person) if person else group
    members = config.members(team)

    def build(members: tuple[str, ...] | None) -> dict[str, Any]:
        if person:
            return {
                "scope": {"kind": "person", "name": person, "team": team},
                **person_report(pull_requests, login=person, team=members, **window),
            }
        return {
            "scope": {"kind": "group" if group else "everyone", "name": group},
            **group_report(pull_requests, people=members, **window),
        }

    report = build(members)
    if members is None:
        first = date.fromisoformat(report["periods"][0]["start"])
        last = date.fromisoformat(report["periods"][-1]["end"])
        # With no configured roster, infer people only from activity in this range.
        names = [
            pr.author
            for pr in pull_requests
            if first <= pr.merged_at.astimezone(config.timezone).date() <= last
        ]
        names += [
            review.reviewer
            for review in reviews
            if first <= review.submitted_at.astimezone(config.timezone).date() <= last
        ]
        unique: dict[str, str] = {}
        for name in names:
            unique.setdefault(name.lower(), name)
        members = tuple(unique.values())
        report = build(members)
    add_review_metrics(
        report,
        reviews,
        people=members,
        timezone=config.timezone,
        since=config.since,
        available=bool(review_available),
        options=analysis.get("review_response", {}),
        team_statistic=team_statistic,
    )
    return report


def export_reports(
    config: Config,
    query: dict[str, list[str]],
    profiles: AvatarLoader,
) -> tuple[bytes, str, str]:
    """Export a scope report or personal reports from one data snapshot."""
    frozen_query = {key: list(value) for key, value in query.items()}
    frozen_query.setdefault("until", [datetime.now(config.timezone).date().isoformat()])
    with Store(config.database) as store:
        pull_requests = store.pull_requests()
        reviews = store.reviews()
        required = [
            name
            for name in store.repository_names()
            if config.includes_repository(name)
        ]
        review_available = bool(required) and all(
            store.reviews_synced_at(name, since=config.since) for name in required
        )
    _report(
        config,
        frozen_query,
        pull_requests=pull_requests,
        reviews=reviews,
        review_available=review_available,
    )  # Validate before image requests.
    meta = _meta(config)
    person = _param(query, "person", "")
    output_format = _param(query, "format", "html" if person else "zip")
    if output_format not in ("html", "zip") or (person and output_format != "html"):
        raise _BadRequest("format must be html, or zip for personal reports of a group")
    people = (person,) if person else config.members(_param(query, "group", "") or None)
    if people is None:
        people = tuple(meta["people"])
    unique_people: dict[str, str] = {}
    for login in people:
        unique_people.setdefault(login.lower(), login)
    people = tuple(unique_people.values())
    if (not people and output_format == "zip") or any(
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", login) for login in people
    ):
        raise _BadRequest("a valid GitHub person is required for each report")
    avatar_sources = profiles(tuple(people))
    assets = {
        name: (STATIC_DIR / name).read_text()
        for name in ("index.html", "app.css", "app.js")
    }
    created_at = datetime.now(config.timezone).isoformat()

    def document(
        report_query: dict[str, list[str]], name: str, members: tuple[str, ...]
    ) -> tuple[str, bytes]:
        reports = {}
        # Each metric can independently select one of four shared variants.
        for statistic in ("mean", "median"):
            for exclude in (False, True):
                options = {
                    metric: {"statistic": statistic, "exclude_outliers": exclude}
                    for metric in DISTRIBUTION_METRICS
                }
                reports[f"{statistic}:{int(exclude)}"] = _report(
                    config,
                    {**report_query, "analysis": [json.dumps(options)]},
                    pull_requests=pull_requests,
                    reviews=reviews,
                    review_available=review_available,
                )
        start = reports["median:0"]["periods"][0]["start"]
        end = frozen_query["until"][0]
        avatars = {}
        for login in members:
            avatar = inline_github_avatar(avatar_sources.get(login.lower()))
            if avatar:
                avatars[login.lower()] = avatar
        snapshot_meta = {
            **meta,
            "people": list(members),
            "avatars": avatars,
            "snapshot": {
                "person": _param(report_query, "person", "") or None,
                "name": name,
                "kind": reports["median:0"]["scope"]["kind"],
                "from": start,
                "until": end,
                "timezone": str(config.timezone),
                "created_at": created_at,
                "history_since": config.since.isoformat(),
            },
        }
        snapshot = {
            "meta": snapshot_meta,
            "query": urlencode(report_query, doseq=True),
            "reports": reports,
        }
        html = render_report_html(
            snapshot,
            template=assets["index.html"],
            styles=assets["app.css"],
            script=assets["app.js"],
            icon=(STATIC_DIR / "favicon.svg").read_bytes(),
        )
        org = re.sub(r"[^A-Za-z0-9-]", "_", config.org)
        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", name)
        return f"orgpulse-{org}-{safe_name}-{start}_to_{end}.html", html.encode()

    if output_format == "html":
        name = person or _param(query, "group", "") or "everyone"
        filename, body = document(frozen_query, name, people)
        return body, filename, "text/html; charset=utf-8"
    files = []
    for login in people:
        person_query = {**frozen_query, "person": [login], "format": ["html"]}
        person_query.pop("group", None)
        files.append(document(person_query, login, (login,)))
    archive = BytesIO()
    with ZipFile(archive, "w", compression=ZIP_DEFLATED) as bundle:
        for filename, body in files:
            bundle.writestr(filename, body)
    return archive.getvalue(), "orgpulse-person-reports.zip", "application/zip"


def _param(query: dict[str, list[str]], name: str, default: str) -> str:
    return query.get(name, [default])[0]
