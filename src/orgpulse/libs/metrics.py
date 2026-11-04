from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from enum import StrEnum
from statistics import mean, median, quantiles
from typing import Any

from orgpulse.common.models import PullRequest

DAYS_PER_WEEK = 7
PANEL_RECENT_LIMIT = 3
PERSON_RECENT_LIMIT = 10
# Medians reported per scope; higher is worse for each, so each can be flagged.
METRICS = ("size", "lead_time", "review_wait")
DISTRIBUTION_METRICS = (*METRICS, "commits", "review_response")
# A person's median this far past the team's median is flagged.
OUTLIER_FACTOR = 1.5
# A single PR past this team percentile is flagged.
OUTLIER_QUANTILE = 0.9


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
    # Synced history reaches into the period / covers all of it.
    synced: bool
    complete: bool


@dataclass(frozen=True)
class _Merged:
    pull_request: PullRequest
    login: str
    merged_on: date
    size: int
    lead_time: float
    review_wait: float | None
    commits: int | None


class _Items:
    """Merged PRs sorted by local merge date for fast range lookups."""

    def __init__(self, items: list[_Merged]) -> None:
        self.items = sorted(items, key=lambda item: item.merged_on)
        self._days = [item.merged_on for item in self.items]

    def within(self, periods: list[_Period]) -> list[_Merged]:
        start, end = periods[0].start, periods[-1].end
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
    start: date | None = None,
    analysis: dict[str, dict[str, Any]] | None = None,
    team_statistic: str = "mean",
) -> dict[str, Any]:
    """Trend against the previous range, plus a row per person against the team.

    `people=None` means everyone who merged a PR in the range.
    """

    periods = _periods(grain, count, until, since, start=start)
    if start is None:
        previous = _periods(grain, count, periods[0].start - timedelta(days=1), since)
    else:
        # Compare custom windows by equal elapsed days, aligned with current bins.
        offset = until - start + timedelta(days=1)
        previous = [
            _period(grain, period.start - offset, period.end - offset, since)
            for period in periods
        ]
    merged = _merged(pull_requests, timezone)
    logins = _logins(people, merged, periods)
    by_login: dict[str, list[_Merged]] = {login: [] for login in logins}
    for item in merged:
        if item.login in by_login:
            by_login[item.login].append(item)
    team = _Items([item for items in by_login.values() for item in items])
    headcount = len(logins)
    current = _summary(team, periods, headcount, analysis)
    person_limits = {
        metric: current[metric] * OUTLIER_FACTOR
        if current and current[metric] is not None
        else None
        for metric in METRICS
    }
    pr_limits = _percentiles(team.within(periods), OUTLIER_QUANTILE)
    rows = []
    for login, name in logins.items():
        own = _Items(by_login[login])
        stats = _summary(own, periods, 1, analysis) or _empty()
        rows.append(
            {
                "login": name,
                **stats,
                "flags": _flagged(stats, person_limits),
                "recent": _recent(own, periods, pr_limits, PANEL_RECENT_LIMIT),
            }
        )
    rows.sort(key=lambda row: (-row["merged"], row["login"].lower()))
    return {
        "team_summary": _team_summary(
            team, periods, tuple(logins), analysis, team_statistic
        ),
        "headcount": headcount,
        "periods": [
            {
                **_period_json(period),
                "compare_start": earlier.start.isoformat(),
                "compare_end": earlier.end.isoformat(),
                "current": _summary(team, [period], headcount, analysis),
                "compare": _summary(team, [earlier], headcount, analysis),
            }
            for period, earlier in zip(periods, previous, strict=True)
        ],
        "summary": {
            "current": current,
            # A range reaching before `since` would compare against unsynced history.
            "compare": (
                _summary(team, previous, headcount, analysis)
                if previous[0].complete
                else None
            ),
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
    start: date | None = None,
    analysis: dict[str, dict[str, Any]] | None = None,
    team_statistic: str = "mean",
) -> dict[str, Any]:
    """One person's trend and recent PRs, each set against their team."""

    periods = _periods(grain, count, until, since, start=start)
    merged = _merged(pull_requests, timezone)
    team_logins = _logins(team, merged, periods)
    team_items = _Items([item for item in merged if item.login in team_logins])
    own = _Items([item for item in merged if item.login == login.lower()])
    team_size = len(team_logins)
    return {
        "team_size": team_size,
        "periods": [
            {
                **_period_json(period),
                "current": _summary(own, [period], 1, analysis),
                "compare": _team_summary(
                    team_items, [period], tuple(team_logins), analysis, team_statistic
                ),
            }
            for period in periods
        ],
        "summary": {
            "current": _summary(own, periods, 1, analysis),
            "compare": _team_summary(
                team_items, periods, tuple(team_logins), analysis, team_statistic
            ),
        },
        "recent": _recent(
            own,
            periods,
            _percentiles(team_items.within(periods), OUTLIER_QUANTILE),
            PERSON_RECENT_LIMIT,
        ),
    }


def _periods(
    grain: Grain, count: int, until: date, since: date, *, start: date | None = None
) -> list[_Period]:
    if start is None:
        starts = [grain.start_of(until)]
        while len(starts) < count:
            starts.append(grain.start_of(starts[-1] - timedelta(days=1)))
        start = starts[-1]
    periods = []
    cursor = start
    while cursor <= until:
        end = min(grain.next_start(grain.start_of(cursor)) - timedelta(days=1), until)
        periods.append(_period(grain, cursor, end, since))
        cursor = end + timedelta(days=1)
    return periods


def _period(grain: Grain, start: date, end: date, since: date) -> _Period:
    # Shifted comparison bins may cross calendar-month boundaries.
    coverage = 0.0
    cursor = start
    while cursor <= end:
        bucket_start = grain.start_of(cursor)
        next_start = grain.next_start(bucket_start)
        stop = min(end + timedelta(days=1), next_start)
        coverage += (stop - cursor).days / (next_start - bucket_start).days
        cursor = stop
    return _Period(
        start=start,
        end=end,
        coverage=coverage,
        synced=end >= since,
        complete=start >= since,
    )


def _merged(pull_requests: list[PullRequest], timezone: tzinfo) -> list[_Merged]:
    return [
        _Merged(
            pull_request=pr,
            login=pr.author.lower(),
            merged_on=pr.merged_at.astimezone(timezone).date(),
            size=pr.changed_lines,
            commits=pr.commits,
            lead_time=_hours(pr.created_at, pr.merged_at),
            review_wait=(
                _hours(pr.ready_at, pr.first_review_at) if pr.first_review_at else None
            ),
        )
        for pr in pull_requests
    ]


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


def _team_summary(
    items: _Items,
    periods: list[_Period],
    people: Sequence[str],
    analysis: dict[str, dict[str, Any]] | None = None,
    statistic: str = "mean",
) -> dict[str, Any] | None:
    """Compare person-level rates and volumes across the configured roster."""
    stats = _summary(items, periods, len(people), analysis)
    if stats is None:
        return None
    covered = [period for period in periods if period.synced]
    elapsed = sum(period.coverage for period in covered)
    per_person = {
        login: {
            "merged_per_person": 0.0,
            "total_lines": 0,
            "added_lines": 0,
            "deleted_lines": 0,
        }
        for login in people
    }
    for item in items.within(covered):
        own = per_person[item.login]
        own["merged_per_person"] += 1 / elapsed
        own["total_lines"] += item.size
        own["added_lines"] += item.pull_request.additions
        own["deleted_lines"] += item.pull_request.deletions
    benchmarks = {
        name: {
            metric: round(aggregate([own[metric] for own in per_person.values()]), 2)
            if per_person
            else None
            for metric in (
                "merged_per_person",
                "total_lines",
                "added_lines",
                "deleted_lines",
            )
        }
        for name, aggregate in (("mean", mean), ("median", median))
    }
    stats["team_benchmarks"] = benchmarks
    stats.update(benchmarks[statistic])
    return stats


def _summary(
    items: _Items,
    periods: list[_Period],
    headcount: int,
    analysis: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Throughput and medians over the synced periods; None if none are synced."""

    covered = [period for period in periods if period.synced]
    if not covered:
        return None
    merged = items.within(covered)
    person_periods = headcount * sum(period.coverage for period in covered)
    distributions = {
        metric: _distribution(merged, metric, (analysis or {}).get(metric, {}))
        for metric in (*METRICS, "commits")
    }
    return {
        "merged": len(merged),
        "total_lines": sum(item.size for item in merged),
        "added_lines": sum(item.pull_request.additions for item in merged),
        "deleted_lines": sum(item.pull_request.deletions for item in merged),
        "analysis_samples": {
            metric: result[1] for metric, result in distributions.items()
        },
        "merged_per_person": (
            round(len(merged) / person_periods, 2) if person_periods else None
        ),
        **{metric: result[0] for metric, result in distributions.items()},
    }


def _distribution(
    items: list[_Merged], metric: str, options: dict[str, Any]
) -> tuple[float | None, dict[str, int]]:
    values = [value for item in items if (value := getattr(item, metric)) is not None]
    total = len(values)
    if options.get("exclude_outliers") and total >= 4:
        q1, _, q3 = quantiles(values, n=4, method="inclusive")
        spread = q3 - q1
        values = [
            value for value in values if q1 - 1.5 * spread <= value <= q3 + 1.5 * spread
        ]
    statistic = options.get("statistic", "mean" if metric == "commits" else "median")
    value = (
        (mean(values) if statistic == "mean" else median(values)) if values else None
    )
    return (round(value, 2) if value is not None else None), {
        "total": total,
        "analyzed": len(values),
        "excluded": total - len(values),
        "missing": len(items) - total,
    }


def _empty() -> dict[str, Any]:
    return {
        "merged": 0,
        "merged_per_person": None,
        "total_lines": 0,
        "added_lines": 0,
        "deleted_lines": 0,
        "analysis_samples": {},
        **dict.fromkeys(DISTRIBUTION_METRICS),
    }


def _flagged(values: dict[str, Any], limits: dict[str, float | None]) -> list[str]:
    return [
        metric
        for metric in METRICS
        if values[metric] is not None
        and limits[metric] is not None
        and values[metric] > limits[metric]
    ]


def _recent(
    items: _Items,
    periods: list[_Period],
    limits: dict[str, float | None],
    limit: int,
) -> list[dict[str, Any]]:
    """The latest merged PRs in the range, newest first, with flagged values."""

    rows = []
    for item in items.within(periods)[::-1][:limit]:
        pr = item.pull_request
        values = {
            metric: None if value is None else round(value, 2)
            for metric in METRICS
            for value in [getattr(item, metric)]
        }
        rows.append(
            {
                "title": pr.title,
                "url": pr.url,
                "repository": pr.repository,
                "number": pr.number,
                "commits": pr.commits,
                **values,
                "flags": _flagged(values, limits),
            }
        )
    return rows


def _percentiles(items: list[_Merged], quantile: float) -> dict[str, float | None]:
    return {
        metric: _percentile(
            [value for item in items if (value := getattr(item, metric)) is not None],
            quantile,
        )
        for metric in METRICS
    }


def _period_json(period: _Period) -> dict[str, Any]:
    return {
        "start": period.start.isoformat(),
        "end": period.end.isoformat(),
        "partial": period.coverage < 1 - 1e-9,
    }


def _hours(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() / 3600)


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower, upper = math.floor(position), math.ceil(position)
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(value, 2)
