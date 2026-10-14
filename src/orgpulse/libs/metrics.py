from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from enum import StrEnum
from typing import Any

from orgpulse.common.config import AiConfig
from orgpulse.common.models import PullRequest

DAYS_PER_WEEK = 7


class Grain(StrEnum):
    WEEK = "week"
    MONTH = "month"

    def start_of(self, day: date) -> date:
        if self is Grain.WEEK:
            return day - timedelta(days=day.weekday())
        return day.replace(day=1)

    def key(self, start: date) -> str:
        if self is Grain.WEEK:
            year, week, _ = start.isocalendar()
            return f"{year}-W{week:02d}"
        return start.strftime("%Y-%m")


@dataclass(frozen=True)
class Period:
    key: str
    start: date
    end: date
    # Elapsed share of the period as of the report date; 1.0 once it has ended.
    coverage: float


@dataclass(frozen=True)
class _Dated:
    pull_request: PullRequest
    login: str
    created_on: date
    merged_on: date | None


def build_report(
    pull_requests: list[PullRequest],
    *,
    people: Collection[str] | None,
    grain: Grain,
    count: int,
    until: date,
    timezone: tzinfo,
    ai: AiConfig | None,
) -> dict[str, Any]:
    """Compute per-period trends, range summaries, per-person rows, and AI impact.

    `people=None` selects every author. Merged-PR metrics are bucketed by merge
    date and opened counts by creation date, both in `timezone`.
    """

    wanted = {login.lower() for login in people} if people is not None else None
    selected = [
        _Dated(
            pull_request=pull_request,
            login=pull_request.author.lower(),
            created_on=_local_date(pull_request.created_at, timezone),
            merged_on=(
                _local_date(pull_request.merged_at, timezone)
                if pull_request.merged_at
                else None
            ),
        )
        for pull_request in pull_requests
        if wanted is None or pull_request.author.lower() in wanted
    ]
    periods = _periods(grain, count, until)
    previous = _periods(grain, count, periods[0].start - timedelta(days=1))
    return {
        "periods": [
            {
                "key": period.key,
                "start": period.start.isoformat(),
                "end": period.end.isoformat(),
                "partial": period.coverage < 1,
                **_range_stats(selected, [period]),
            }
            for period in periods
        ],
        "summary": {
            "current": _range_stats(selected, periods),
            "previous": _range_stats(selected, previous),
        },
        "people": _people_rows(selected, periods, people),
        "ai": _ai_comparison(selected, ai, until) if ai is not None else None,
    }


def _local_date(value: datetime, timezone: tzinfo) -> date:
    return value.astimezone(timezone).date()


def _periods(grain: Grain, count: int, until: date) -> list[Period]:
    starts = [grain.start_of(until)]
    while len(starts) < count:
        starts.append(grain.start_of(starts[-1] - timedelta(days=1)))
    periods = []
    for start in reversed(starts):
        end = _next_start(grain, start) - timedelta(days=1)
        length = (end - start).days + 1
        elapsed = (min(until, end) - start).days + 1
        periods.append(
            Period(
                key=grain.key(start), start=start, end=end, coverage=elapsed / length
            )
        )
    return periods


def _next_start(grain: Grain, start: date) -> date:
    if grain is Grain.WEEK:
        return start + timedelta(days=DAYS_PER_WEEK)
    return (start.replace(day=28) + timedelta(days=4)).replace(day=1)


def _range_stats(selected: list[_Dated], periods: list[Period]) -> dict[str, Any]:
    merged: list[PullRequest] = []
    opened = 0
    authors: set[str] = set()
    person_periods = 0.0
    for period in periods:
        period_merged = [
            item
            for item in selected
            if _within(item.merged_on, period.start, period.end)
        ]
        period_opened = [
            item
            for item in selected
            if _within(item.created_on, period.start, period.end)
        ]
        period_authors = {item.login for item in period_merged + period_opened}
        merged.extend(item.pull_request for item in period_merged)
        opened += len(period_opened)
        authors |= period_authors
        person_periods += len(period_authors) * period.coverage
    return _stats(
        merged, opened=opened, authors=len(authors), person_periods=person_periods
    )


def _people_rows(
    selected: list[_Dated],
    periods: list[Period],
    people: Collection[str] | None,
) -> list[dict[str, Any]]:
    start, end = periods[0].start, periods[-1].end
    elapsed_periods = sum(period.coverage for period in periods)
    by_login: dict[str, list[_Dated]] = defaultdict(list)
    names: dict[str, str] = {login.lower(): login for login in people or ()}
    for item in selected:
        by_login[item.login].append(item)
        names[item.login] = item.pull_request.author
    rows = []
    for login, name in names.items():
        items = by_login.get(login, [])
        merged = [
            item.pull_request for item in items if _within(item.merged_on, start, end)
        ]
        opened = sum(_within(item.created_on, start, end) for item in items)
        if people is None and not merged and not opened:
            continue
        rows.append(
            {
                "login": name,
                **_stats(
                    merged, opened=opened, authors=1, person_periods=elapsed_periods
                ),
            }
        )
    return sorted(rows, key=lambda row: (-row["merged"], row["login"].lower()))


def _ai_comparison(
    selected: list[_Dated],
    ai: AiConfig,
    until: date,
) -> dict[str, Any]:
    """Compare each person's window before adoption with their most recent window.

    People without merged PRs in both windows are excluded, so joiners and
    leavers do not skew the comparison.
    """

    window = timedelta(days=ai.window_days)
    by_login: dict[str, list[_Dated]] = defaultdict(list)
    for item in selected:
        by_login[item.login].append(item)
    pooled: dict[str, list[Any]] = {"before": [], "after": []}
    weeks = {"before": 0.0, "after": 0.0}
    rows = []
    excluded = []
    for login, items in sorted(by_login.items()):
        name = items[0].pull_request.author
        adopted_on = ai.adopted_on_for(login)
        after_start = max(adopted_on, until - window + timedelta(days=1))
        windows = {
            "before": (adopted_on - window, adopted_on - timedelta(days=1)),
            "after": (after_start, until),
        }
        merged = {
            side: [item for item in items if _within(item.merged_on, *bounds)]
            for side, bounds in windows.items()
        }
        if after_start > until or not merged["before"] or not merged["after"]:
            excluded.append(name)
            continue
        row: dict[str, Any] = {"login": name, "adopted_on": adopted_on.isoformat()}
        for side, (start, end) in windows.items():
            side_weeks = ((end - start).days + 1) / DAYS_PER_WEEK
            opened = [item for item in items if _within(item.created_on, start, end)]
            row[side] = _stats(
                [item.pull_request for item in merged[side]],
                opened=len(opened),
                authors=1,
                person_periods=side_weeks,
            )
            pooled[side].append((merged[side], opened))
            weeks[side] += side_weeks
        rows.append(row)
    return {
        "adopted_on": ai.adopted_on.isoformat(),
        "window_days": ai.window_days,
        "people": rows,
        "excluded": excluded,
        **{
            side: _stats(
                [item.pull_request for merged, _ in pooled[side] for item in merged],
                opened=sum(len(opened) for _, opened in pooled[side]),
                authors=len(rows),
                person_periods=weeks[side],
            )
            for side in ("before", "after")
        },
    }


def _within(day: date | None, start: date, end: date) -> bool:
    return day is not None and start <= day <= end


def _stats(
    merged: list[PullRequest],
    *,
    opened: int,
    authors: int,
    person_periods: float,
) -> dict[str, Any]:
    """Summarize merged pull requests; durations are in hours."""

    sizes = [pull_request.changed_lines for pull_request in merged]
    commits = [pull_request.commits for pull_request in merged]
    lead_times = [_hours(pr.created_at, pr.merged_at) for pr in merged]
    cycle_times = [
        _hours(pr.first_commit_at, pr.merged_at) for pr in merged if pr.first_commit_at
    ]
    reviewed = [pr for pr in merged if pr.first_review_at is not None]
    review_waits = [
        _hours(pr.ready_at or pr.created_at, pr.first_review_at) for pr in reviewed
    ]
    return {
        "authors": authors,
        "merged": len(merged),
        "opened": opened,
        "merged_per_person": _ratio(len(merged), person_periods) or 0.0,
        "opened_per_person": _ratio(opened, person_periods) or 0.0,
        "size_median": _percentile(sizes, 0.5),
        "size_p90": _percentile(sizes, 0.9),
        "commits_median": _percentile(commits, 0.5),
        "commits_p90": _percentile(commits, 0.9),
        "lead_time_median": _percentile(lead_times, 0.5),
        "lead_time_p90": _percentile(lead_times, 0.9),
        "cycle_time_median": _percentile(cycle_times, 0.5),
        "cycle_time_p90": _percentile(cycle_times, 0.9),
        "review_wait_median": _percentile(review_waits, 0.5),
        "review_wait_p90": _percentile(review_waits, 0.9),
        "unreviewed_share": _ratio(100 * (len(merged) - len(reviewed)), len(merged)),
    }


def _hours(start: datetime | None, end: datetime | None) -> float:
    assert start is not None and end is not None
    return max(0.0, (end - start).total_seconds() / 3600)


def _ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 2) if denominator else None


def _percentile(values: list[float] | list[int], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower, upper = math.floor(position), math.ceil(position)
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(value, 2)
