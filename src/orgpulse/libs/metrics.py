from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from enum import StrEnum
from typing import Any

from orgpulse.common.models import PullRequest

DAYS_PER_WEEK = 7
PANEL_RECENT_LIMIT = 3
PERSON_RECENT_LIMIT = 10
# A person's median, or a single PR, this far past the team is flagged.
OUTLIER_FACTOR = 1.5
# Metrics where a higher value is worse; throughput is never flagged.
FLAGGED_METRICS = ("size", "lead_time", "review_wait")


class Grain(StrEnum):
    WEEK = "week"
    MONTH = "month"

    def start_of(self, day: date) -> date:
        if self is Grain.WEEK:
            return day - timedelta(days=day.weekday())
        return day.replace(day=1)

    def next_start(self, start: date) -> date:
        if self is Grain.WEEK:
            return start + timedelta(days=DAYS_PER_WEEK)
        return (start.replace(day=28) + timedelta(days=4)).replace(day=1)


@dataclass(frozen=True)
class _Period:
    start: date
    end: date
    # Elapsed share of the period as of the report date; 1.0 once it has ended.
    coverage: float


@dataclass(frozen=True)
class _Merged:
    pull_request: PullRequest
    login: str
    merged_on: date
    size: int
    lead_time: float
    review_wait: float | None


class _Items:
    """Merged PRs sorted by local merge date for fast range lookups."""

    def __init__(self, items: list[_Merged]) -> None:
        self.items = sorted(items, key=lambda item: item.merged_on)
        self._days = [item.merged_on for item in self.items]

    def between(self, start: date, end: date) -> list[_Merged]:
        return self.items[
            bisect_left(self._days, start) : bisect_right(self._days, end)
        ]


def group_report(
    pull_requests: list[PullRequest],
    *,
    people: Sequence[str] | None,
    grain: Grain,
    count: int,
    until: date,
    since: date,
    timezone: tzinfo,
) -> dict[str, Any]:
    """Trend, previous-range comparison, and per-person rows for a set of people.

    `people=None` means everyone who merged a PR in the range.
    """

    merged = _merged(pull_requests, timezone)
    periods = _periods(grain, count, until)
    previous = _periods(grain, count, periods[0].start - timedelta(days=1))
    logins = _logins(people, merged, periods)
    team = _Items([item for item in merged if item.login in logins])
    headcount = len(logins)
    current = _summary(team, periods, headcount, since)
    thresholds = _p90s(team.between(periods[0].start, periods[-1].end))
    rows = []
    for login, name in logins.items():
        own = _Items([item for item in team.items if item.login == login])
        stats = _summary(own, periods, 1, since)
        rows.append(
            {
                "login": name,
                **(stats or _empty()),
                "flags": _flags(stats, current, OUTLIER_FACTOR),
                "recent": _recent(own, periods, thresholds, PANEL_RECENT_LIMIT),
            }
        )
    rows.sort(key=lambda row: (-row["merged"], row["login"].lower()))
    return {
        "headcount": headcount,
        "periods": [
            {
                **_period_json(period),
                "current": _summary(team, [period], headcount, since),
                "previous": _summary(team, [earlier], headcount, since),
            }
            for period, earlier in zip(periods, previous, strict=True)
        ],
        "summary": {
            "current": current,
            "previous": _comparable(team, previous, headcount, since),
        },
        "people": rows,
    }


def person_report(
    pull_requests: list[PullRequest],
    *,
    login: str,
    team: Sequence[str] | None,
    grain: Grain,
    count: int,
    until: date,
    since: date,
    timezone: tzinfo,
) -> dict[str, Any]:
    """One person's trend and recent PRs, each set against their team."""

    merged = _merged(pull_requests, timezone)
    periods = _periods(grain, count, until)
    previous = _periods(grain, count, periods[0].start - timedelta(days=1))
    team_logins = _logins(team, merged, periods)
    team_items = _Items([item for item in merged if item.login in team_logins])
    own = _Items([item for item in merged if item.login == login.lower()])
    team_size = len(team_logins)
    thresholds = _p90s(team_items.between(periods[0].start, periods[-1].end))
    return {
        "periods": [
            {
                **_period_json(period),
                "current": _summary(own, [period], 1, since),
                "team": _summary(team_items, [period], team_size, since),
            }
            for period in periods
        ],
        "summary": {
            "current": _summary(own, periods, 1, since),
            "previous": _comparable(own, previous, 1, since),
            "team": _summary(team_items, periods, team_size, since),
        },
        "recent": _recent(own, periods, thresholds, PERSON_RECENT_LIMIT),
    }


def _merged(pull_requests: list[PullRequest], timezone: tzinfo) -> list[_Merged]:
    items = []
    for pr in pull_requests:
        if pr.merged_at is None:
            continue
        items.append(
            _Merged(
                pull_request=pr,
                login=pr.author.lower(),
                merged_on=pr.merged_at.astimezone(timezone).date(),
                size=pr.changed_lines,
                lead_time=_hours(pr.created_at, pr.merged_at),
                review_wait=(
                    _hours(pr.ready_at, pr.first_review_at)
                    if pr.ready_at and pr.first_review_at
                    else None
                ),
            )
        )
    return items


def _periods(grain: Grain, count: int, until: date) -> list[_Period]:
    starts = [grain.start_of(until)]
    while len(starts) < count:
        starts.append(grain.start_of(starts[-1] - timedelta(days=1)))
    periods = []
    for start in reversed(starts):
        end = grain.next_start(start) - timedelta(days=1)
        length = (end - start).days + 1
        elapsed = (min(until, end) - start).days + 1
        periods.append(_Period(start=start, end=end, coverage=elapsed / length))
    return periods


def _logins(
    people: Sequence[str] | None, merged: list[_Merged], periods: list[_Period]
) -> dict[str, str]:
    """Map lowercased logins in scope to their display spelling."""

    if people is not None:
        return {login.lower(): login for login in people}
    start, end = periods[0].start, periods[-1].end
    return {
        item.login: item.pull_request.author
        for item in merged
        if start <= item.merged_on <= end
    }


def _summary(
    items: _Items, periods: list[_Period], headcount: int, since: date
) -> dict[str, Any] | None:
    """Throughput and medians over the periods synced data covers; None if none."""

    covered = [period for period in periods if period.end >= since]
    if not covered:
        return None
    merged = [
        item for period in covered for item in items.between(period.start, period.end)
    ]
    person_periods = headcount * sum(period.coverage for period in covered)
    return {
        "merged": len(merged),
        "merged_per_person": (
            round(len(merged) / person_periods, 2) if person_periods else None
        ),
        "size": _percentile([item.size for item in merged], 0.5),
        "lead_time": _percentile([item.lead_time for item in merged], 0.5),
        "review_wait": _percentile(_review_waits(merged), 0.5),
    }


def _comparable(
    items: _Items, periods: list[_Period], headcount: int, since: date
) -> dict[str, Any] | None:
    # A range reaching before `since` would compare against unsynced history.
    if periods[0].start < since:
        return None
    return _summary(items, periods, headcount, since)


def _empty() -> dict[str, Any]:
    return {
        "merged": 0,
        "merged_per_person": None,
        "size": None,
        "lead_time": None,
        "review_wait": None,
    }


def _flags(
    stats: dict[str, Any] | None, baseline: dict[str, Any] | None, factor: float
) -> list[str]:
    if stats is None or baseline is None:
        return []
    return [
        metric
        for metric in FLAGGED_METRICS
        if baseline[metric] and _exceeds(stats[metric], baseline[metric] * factor)
    ]


def _p90s(items: list[_Merged]) -> dict[str, float | None]:
    return {
        "size": _percentile([item.size for item in items], 0.9),
        "lead_time": _percentile([item.lead_time for item in items], 0.9),
        "review_wait": _percentile(_review_waits(items), 0.9),
    }


def _recent(
    items: _Items,
    periods: list[_Period],
    thresholds: dict[str, float | None],
    limit: int,
) -> list[dict[str, Any]]:
    """The latest merged PRs, flagging values past the team's 90th percentile."""

    latest = items.between(periods[0].start, periods[-1].end)[::-1][:limit]
    rows = []
    for item in latest:
        pr = item.pull_request
        values = {
            "size": item.size,
            "lead_time": round(item.lead_time, 2),
            "review_wait": (
                round(item.review_wait, 2) if item.review_wait is not None else None
            ),
        }
        rows.append(
            {
                "title": pr.title,
                "url": pr.url,
                "repository": pr.repository,
                "number": pr.number,
                "merged_on": item.merged_on.isoformat(),
                **values,
                "flags": [
                    metric
                    for metric in FLAGGED_METRICS
                    if _exceeds(values[metric], thresholds[metric])
                ],
            }
        )
    return rows


def _exceeds(value: float | None, limit: float | None) -> bool:
    return value is not None and limit is not None and value > limit


def _period_json(period: _Period) -> dict[str, Any]:
    return {
        "start": period.start.isoformat(),
        "end": period.end.isoformat(),
        "partial": period.coverage < 1,
    }


def _review_waits(items: list[_Merged]) -> list[float]:
    return [item.review_wait for item in items if item.review_wait is not None]


def _hours(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() / 3600)


def _percentile(values: list[float] | list[int], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower, upper = math.floor(position), math.ceil(position)
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(value, 2)
