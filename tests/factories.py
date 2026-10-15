from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from orgpulse.common.config import Config
from orgpulse.common.models import PullRequest


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def make_config(tmp_path: Path, **overrides: Any) -> Config:
    """A UTC config syncing from 2026-01-01 into a database under `tmp_path`."""

    base = Config(
        org="acme",
        database=tmp_path / "orgpulse.db",
        since=date(2026, 1, 1),
        timezone=timezone.utc,
        locale="en",
    )
    return replace(base, **overrides)


def merged_pr(
    author: str,
    merged_at: str,
    *,
    hours_open: float = 2,
    number: int = 0,
    **overrides: Any,
) -> PullRequest:
    """A merged PR opened `hours_open` before `merged_at` and reviewed one hour after opening."""

    merged = utc(merged_at)
    created = merged - timedelta(hours=hours_open)
    fields: dict[str, Any] = {
        "repository": "acme/api",
        "number": number,
        "title": f"Change {number}",
        "url": f"https://github.com/acme/api/pull/{number}",
        "author": author,
        "created_at": created,
        "merged_at": merged,
        "ready_at": created,
        "first_review_at": created + timedelta(hours=1),
        "additions": 10,
        "deletions": 0,
    }
    fields.update(overrides)
    return PullRequest(**fields)
