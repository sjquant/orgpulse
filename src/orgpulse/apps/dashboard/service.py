from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from pydantic import ValidationError

from orgpulse.common.config import build_run_config
from orgpulse.common.errors import (
    AuthResolutionError,
    GitHubApiError,
    OrgTargetingError,
)
from orgpulse.common.models import (
    DashboardAuthorPayload,
    DashboardAuthorThroughputPointPayload,
    DashboardChartsPayload,
    DashboardInsightPayload,
    DashboardOverviewPayload,
    DashboardPullRequestPayload,
    DashboardRepositoryPayload,
    DashboardRepositoryThroughputPointPayload,
    DashboardReviewerPayload,
    DashboardReviewerTrendPayload,
    DashboardReviewLatencyPointPayload,
    DashboardReviewStatePayload,
    DashboardSizeBucketPayload,
    DashboardSourcePayload,
    DashboardTimeSeriesPointPayload,
    PeriodGrain,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportLocale,
    RunManifest,
    RunMode,
)
from orgpulse.libs.metrics.review_latency import (
    approval_hours as calculate_approval_hours,
)
from orgpulse.libs.metrics.review_latency import (
    hours_between as _hours_between,
)
from orgpulse.libs.metrics.review_latency import (
    review_cycle_markers,
)
from orgpulse.libs.reporting.contracts import build_time_anchor_context
from orgpulse.libs.reporting.dashboard_html import (
    prepare_dashboard_payload,
    render_dashboard_html,
)
from orgpulse.libs.snapshots.person_source import PersonSnapshotSource, ReviewFact
from orgpulse.libs.snapshots.refresh import execute_source_refresh
from orgpulse.libs.snapshots.source import LocalSnapshotSource, read_snapshot_csv_rows


@dataclass(frozen=True)
class PullRequestReview:
    """Represent one normalized review event attached to a pull request."""

    review_id: int
    author_login: str
    state: str
    submitted_at: datetime


@dataclass(frozen=True)
class PullRequestTimelineEvent:
    """Represent one normalized timeline event attached to a pull request."""

    event_id: int
    event: str
    created_at: datetime | None
    requested_reviewer_login: str | None
    requested_team_name: str | None


@dataclass(frozen=True)
class PullRequestSnapshot:
    """Represent one dashboard-oriented pull request snapshot."""

    repository_full_name: str
    number: int
    title: str
    author_login: str
    state: str
    draft: bool
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    merged_at: datetime | None
    html_url: str
    additions: int
    deletions: int
    changed_files: int
    changed_lines: int
    commits: int
    review_count: int
    approval_count: int
    changes_requested_count: int
    comment_review_count: int
    reviewer_count: int
    first_review_at: datetime | None
    first_review_hours: float | None
    approval_hours: float | None
    merge_hours: float | None
    close_hours: float | None
    review_rounds: int
    review_requested_at: datetime | None
    review_ready_at: datetime
    size_bucket: str
    reviews: tuple[PullRequestReview, ...]


DASHBOARD_PULL_REQUEST_FIELDNAMES = (
    "repository_full_name",
    "pull_request_number",
    "title",
    "author_login",
    "state",
    "created_at",
    "updated_at",
    "closed_at",
    "merged_at",
    "html_url",
    "additions",
    "deletions",
    "changed_files",
    "changed_lines",
    "commits",
    "review_count",
    "approval_count",
    "changes_requested_count",
    "comment_review_count",
    "reviewer_count",
    "first_review_hours",
    "approval_hours",
    "merge_hours",
    "close_hours",
    "review_rounds",
    "review_ready_at",
    "review_requested_at",
    "size_bucket",
)


def generate_dashboard_report(
    *,
    org: str,
    since: date,
    until: date,
    source_output_dir: Path,
    output_dir: Path,
    base_name: str,
    refresh: bool,
    distribution_percentile: int,
    locale: ReportLocale | str | None = None,
) -> dict[str, Any]:
    """Generate dashboard artifacts from local source data.

    Args:
        org: Target organization login.
        since: Inclusive lower date bound for the rendered dashboard.
        until: Inclusive upper date bound for the rendered dashboard.
        source_output_dir: Directory containing normalized local source outputs.
        output_dir: Directory that should receive rendered dashboard artifacts.
        base_name: Base filename for generated dashboard artifacts.
        refresh: Whether to refresh the open source period before rendering.
        distribution_percentile: Percentile cutoff applied to latency metrics.

    Returns:
        A mapping of generated dashboard artifact paths.
    """

    source_manifest = LocalSnapshotSource().try_load_dashboard_manifest(
        org=org,
        output_dir=source_output_dir,
    )
    try:
        if refresh:
            _refresh_local_source_outputs(
                org=org,
                as_of=date.today(),
                source_output_dir=source_output_dir,
                source_manifest=source_manifest,
            )
    except (AuthResolutionError, GitHubApiError, OrgTargetingError) as exc:
        raise RuntimeError(f"failed to refresh local source outputs: {exc}") from exc
    try:
        payload = build_dashboard_payload_from_local_outputs(
            org=org,
            since=since,
            until=until,
            source_output_dir=source_output_dir,
        )
        if locale is None:
            return _write_outputs(
                output_dir=output_dir,
                base_name=base_name,
                payload=payload,
                distribution_percentile=distribution_percentile,
            )
        return _write_outputs(
            output_dir=output_dir,
            base_name=base_name,
            payload=payload,
            distribution_percentile=distribution_percentile,
            locale=locale,
        )
    except ValidationError as exc:
        raise RuntimeError(f"dashboard payload validation failed: {exc}") from exc


def _refresh_local_source_outputs(
    *,
    org: str,
    as_of: date,
    source_output_dir: Path,
    source_manifest: RunManifest | None,
) -> None:
    try:
        config = build_run_config(
            org=org,
            as_of=as_of,
            mode=RunMode.INCREMENTAL,
            include_repos=(
                list(source_manifest.include_repos)
                if source_manifest is not None
                else None
            ),
            exclude_repos=(
                list(source_manifest.exclude_repos)
                if source_manifest is not None
                else None
            ),
            output_dir=source_output_dir,
        )
    except ValidationError as exc:
        raise RuntimeError(f"invalid run configuration: {exc}") from exc

    execute_source_refresh(config)


def build_dashboard_payload_from_local_outputs(
    *,
    org: str,
    since: date,
    until: date,
    source_output_dir: Path,
) -> DashboardSourcePayload:
    """Build a typed dashboard payload from stored local run outputs.

    Args:
        org: Target organization login.
        since: Inclusive lower date bound for the dashboard window.
        until: Inclusive upper date bound for the dashboard window.
        source_output_dir: Directory containing normalized local source outputs.

    Returns:
        A validated source payload for dashboard rendering.
    """

    source = LocalSnapshotSource().load_dashboard_source(
        org=org,
        output_dir=source_output_dir,
        since=since,
        until=until,
    )
    review_facts = _load_local_review_facts(source.raw_snapshot)
    snapshots = _load_local_snapshots(
        periods=source.raw_snapshot.periods,
        since=since,
        until=until,
    )
    return _build_dashboard_payload(
        org=org,
        since=since,
        until=until,
        source_as_of=source.manifest.last_successful_run.as_of,
        snapshots=snapshots,
        review_facts=review_facts,
    )


def _load_local_review_facts(
    raw_snapshot: RawSnapshotWriteResult,
) -> tuple[ReviewFact, ...]:
    person_snapshot = PersonSnapshotSource().load(raw_snapshot)
    return tuple(
        review
        for pull_request in person_snapshot.pull_requests
        for review in pull_request.reviews
    )


def _load_local_snapshots(
    *,
    periods: tuple[RawSnapshotPeriod, ...],
    since: date,
    until: date,
) -> list[PullRequestSnapshot]:
    snapshots: list[PullRequestSnapshot] = []
    for period in periods:
        if period.end_date < since or period.start_date > until:
            continue
        snapshots.extend(
            _period_snapshots(
                period=period,
                since=since,
                until=until,
            )
        )
    return sorted(
        snapshots,
        key=lambda snapshot: (
            snapshot.created_at,
            snapshot.repository_full_name,
            snapshot.number,
        ),
    )


def _period_snapshots(
    *,
    period: RawSnapshotPeriod,
    since: date,
    until: date,
) -> list[PullRequestSnapshot]:
    pull_request_rows = read_snapshot_csv_rows(period.pull_requests_path)
    review_rows = read_snapshot_csv_rows(period.reviews_path)
    timeline_rows = read_snapshot_csv_rows(period.timeline_events_path)
    reviews_by_pull_request = _reviews_by_pull_request(review_rows)
    timeline_events_by_pull_request = _timeline_events_by_pull_request(timeline_rows)
    snapshots: list[PullRequestSnapshot] = []
    for pull_request_row in pull_request_rows:
        created_at = _parse_datetime(pull_request_row["created_at"])
        if created_at.date() < since or created_at.date() > until:
            continue
        pull_request_key = _pull_request_key(
            repository_full_name=pull_request_row["repository_full_name"],
            pull_request_number=pull_request_row["pull_request_number"],
        )
        snapshots.append(
            _snapshot_from_local_rows(
                pull_request_row,
                reviews=reviews_by_pull_request.get(pull_request_key, []),
                timeline_events=timeline_events_by_pull_request.get(
                    pull_request_key,
                    [],
                ),
            )
        )
    return snapshots


def _reviews_by_pull_request(
    review_rows: tuple[dict[str, str], ...],
) -> dict[tuple[str, int], list[PullRequestReview]]:
    grouped: dict[tuple[str, int], list[PullRequestReview]] = defaultdict(list)
    for review_row in review_rows:
        submitted_at = _parse_optional_datetime(review_row["submitted_at"])
        if submitted_at is None:
            continue
        grouped[
            _pull_request_key(
                repository_full_name=review_row["repository_full_name"],
                pull_request_number=review_row["pull_request_number"],
            )
        ].append(
            PullRequestReview(
                review_id=int(review_row["review_id"]),
                author_login=review_row["author_login"] or "ghost",
                state=review_row["state"],
                submitted_at=submitted_at,
            )
        )
    for reviews in grouped.values():
        reviews.sort(key=lambda review: (review.submitted_at, review.review_id))
    return grouped


def _timeline_events_by_pull_request(
    timeline_rows: tuple[dict[str, str], ...],
) -> dict[tuple[str, int], list[PullRequestTimelineEvent]]:
    grouped: dict[tuple[str, int], list[PullRequestTimelineEvent]] = defaultdict(list)
    for timeline_row in timeline_rows:
        grouped[
            _pull_request_key(
                repository_full_name=timeline_row["repository_full_name"],
                pull_request_number=timeline_row["pull_request_number"],
            )
        ].append(
            PullRequestTimelineEvent(
                event_id=int(timeline_row["event_id"]),
                event=timeline_row["event"],
                created_at=_parse_optional_datetime(timeline_row["created_at"]),
                requested_reviewer_login=(
                    timeline_row["requested_reviewer_login"] or None
                ),
                requested_team_name=timeline_row["requested_team_name"] or None,
            )
        )
    for timeline_events in grouped.values():
        timeline_events.sort(
            key=lambda event: (
                event.created_at.isoformat() if event.created_at is not None else "",
                event.event,
            )
        )
    return grouped


def _pull_request_key(
    *,
    repository_full_name: str,
    pull_request_number: str,
) -> tuple[str, int]:
    return repository_full_name, int(pull_request_number)


def _parse_optional_datetime(value: str | None) -> datetime | None:
    if value in {None, "", "0001-01-01T00:00:00Z", "0001-01-01T00:00:00+00:00"}:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _snapshot_from_local_rows(
    pull_request_row: dict[str, str],
    *,
    reviews: list[PullRequestReview],
    timeline_events: list[PullRequestTimelineEvent],
) -> PullRequestSnapshot:
    created_at = _parse_datetime(pull_request_row["created_at"])
    updated_at = _parse_datetime(pull_request_row["updated_at"])
    closed_at = _parse_optional_datetime(pull_request_row["closed_at"])
    merged_at = _parse_optional_datetime(pull_request_row["merged_at"])
    additions = int(pull_request_row["additions"])
    deletions = int(pull_request_row["deletions"])
    draft = _parse_bool(pull_request_row["draft"])
    review_markers = review_cycle_markers(
        author_login=pull_request_row["author_login"] or "ghost",
        draft=draft,
        created_at=created_at,
        timeline_events=timeline_events,
        reviews=reviews,
    )
    return PullRequestSnapshot(
        repository_full_name=pull_request_row["repository_full_name"],
        number=int(pull_request_row["pull_request_number"]),
        title=pull_request_row["title"],
        author_login=pull_request_row["author_login"] or "ghost",
        state=pull_request_row["state"],
        draft=draft,
        created_at=created_at,
        updated_at=updated_at,
        closed_at=closed_at,
        merged_at=merged_at,
        html_url=pull_request_row["html_url"],
        additions=additions,
        deletions=deletions,
        changed_files=int(pull_request_row["changed_files"]),
        changed_lines=additions + deletions,
        commits=int(pull_request_row["commits"]),
        review_count=len(reviews),
        approval_count=sum(1 for review in reviews if review.state == "APPROVED"),
        changes_requested_count=sum(
            1 for review in reviews if review.state == "CHANGES_REQUESTED"
        ),
        comment_review_count=sum(
            1 for review in reviews if review.state == "COMMENTED"
        ),
        reviewer_count=len({review.author_login for review in reviews}),
        first_review_at=review_markers.first_review_at,
        first_review_hours=_hours_between(
            review_markers.review_started_at,
            review_markers.first_review_at,
        ),
        approval_hours=calculate_approval_hours(
            author_login=pull_request_row["author_login"] or "ghost",
            draft=draft,
            created_at=created_at,
            timeline_events=timeline_events,
            reviews=reviews,
        ),
        merge_hours=_hours_between(created_at, merged_at),
        close_hours=_hours_between(created_at, closed_at),
        review_rounds=max(
            1,
            len(
                {
                    review.author_login
                    for review in reviews
                    if review.state in {"APPROVED", "CHANGES_REQUESTED", "COMMENTED"}
                }
            ),
        )
        if reviews
        else 0,
        review_requested_at=review_markers.review_requested_at,
        review_ready_at=review_markers.review_ready_at or created_at,
        size_bucket=_size_bucket(additions + deletions),
        reviews=tuple(reviews),
    )


def _build_dashboard_payload(
    *,
    org: str,
    since: date,
    until: date,
    source_as_of: date | None = None,
    snapshots: list[PullRequestSnapshot],
    review_facts: tuple[ReviewFact, ...],
) -> DashboardSourcePayload:
    total_pull_requests = len(snapshots)
    created_series = _time_series(
        snapshots=snapshots,
        date_selector=lambda snapshot: snapshot.created_at.date(),
    )
    merged_series = _time_series(
        snapshots=[
            snapshot for snapshot in snapshots if snapshot.merged_at is not None
        ],
        date_selector=lambda snapshot: snapshot.merged_at.date(),
    )
    review_series = _time_series(
        snapshots=[
            snapshot for snapshot in snapshots if snapshot.first_review_at is not None
        ],
        date_selector=lambda snapshot: snapshot.first_review_at.date(),
    )
    author_rows = _author_rows(
        snapshots,
        total_pull_requests=total_pull_requests,
    )
    reviewer_rows = _reviewer_rows(
        review_facts,
        since=since,
        until=until,
    )
    reviewer_weekly_trends = _reviewer_trend_rows(
        review_facts,
        grain=PeriodGrain.WEEK,
        since=since,
        until=until,
    )
    reviewer_monthly_trends = _reviewer_trend_rows(
        review_facts,
        grain=PeriodGrain.MONTH,
        since=since,
        until=until,
    )
    repository_rows = _repository_rows(
        snapshots,
        total_pull_requests=total_pull_requests,
    )
    size_bucket_rows = _size_bucket_rows(snapshots)
    review_state_rows = _review_state_rows(snapshots)
    overview = _overview_summary(
        org=org,
        since=since,
        until=until,
        source_as_of=source_as_of or until,
        snapshots=snapshots,
        author_rows=author_rows,
        reviewer_rows=reviewer_rows,
        repository_rows=repository_rows,
    )
    return DashboardSourcePayload(
        overview=overview,
        insights=_insights(
            overview=overview,
            author_rows=author_rows,
            reviewer_rows=reviewer_rows,
            repository_rows=repository_rows,
            size_bucket_rows=size_bucket_rows,
        ),
        charts=DashboardChartsPayload(
            created_series=created_series,
            merged_series=merged_series,
            review_series=review_series,
            author_throughput=[
                DashboardAuthorThroughputPointPayload(
                    label=row.author_login,
                    pull_requests=row.pull_requests,
                    merged_pull_requests=row.merged_pull_requests,
                    changed_lines=row.changed_lines,
                )
                for row in author_rows[:8]
            ],
            review_latency_by_author=[
                DashboardReviewLatencyPointPayload(
                    label=row.author_login,
                    median_first_review_hours=row.median_first_review_hours,
                    median_approval_hours=row.median_approval_hours,
                )
                for row in author_rows
                if row.median_first_review_hours is not None
                or row.median_approval_hours is not None
            ][:8],
            repository_throughput=[
                DashboardRepositoryThroughputPointPayload(
                    label=row.repository_full_name,
                    pull_requests=row.pull_requests,
                    merged_pull_requests=row.merged_pull_requests,
                )
                for row in repository_rows[:10]
            ],
            size_bucket_latency=size_bucket_rows,
        ),
        authors=author_rows,
        reviewers=reviewer_rows,
        repositories=repository_rows,
        size_buckets=size_bucket_rows,
        review_state_rows=review_state_rows,
        reviewer_weekly_trends=reviewer_weekly_trends,
        reviewer_monthly_trends=reviewer_monthly_trends,
        pull_requests=[_snapshot_row(snapshot) for snapshot in snapshots],
    )


def _write_outputs(
    *,
    output_dir: Path,
    base_name: str,
    payload: DashboardSourcePayload,
    distribution_percentile: int,
    locale: ReportLocale | str | None = None,
) -> dict[str, Any]:
    payload_data = payload.model_dump(mode="json")
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{base_name}.json"
    csv_path = output_dir / f"{base_name}-prs.csv"
    html_path = output_dir / f"{base_name}.html"
    json_path.write_text(
        json.dumps(payload_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        rows = payload_data["pull_requests"]
        writer = csv.DictWriter(handle, fieldnames=DASHBOARD_PULL_REQUEST_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    html_path.write_text(
        render_dashboard_html(
            prepare_dashboard_payload(
                payload,
                distribution_percentile=distribution_percentile,
            ),
            locale=locale,
        ),
        encoding="utf-8",
    )
    return {
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "html_path": str(html_path),
        "pull_requests": payload_data["overview"]["pull_requests"],
        "distribution_percentile": distribution_percentile,
    }


def _time_series(
    *,
    snapshots: list[PullRequestSnapshot],
    date_selector,
) -> list[DashboardTimeSeriesPointPayload]:
    counts = Counter(date_selector(snapshot).isoformat() for snapshot in snapshots)
    return [
        DashboardTimeSeriesPointPayload(date=label, count=counts[label])
        for label in sorted(counts)
    ]


def _overview_summary(
    *,
    org: str,
    since: date,
    until: date,
    source_as_of: date,
    snapshots: list[PullRequestSnapshot],
    author_rows: list[DashboardAuthorPayload],
    reviewer_rows: list[DashboardReviewerPayload],
    repository_rows: list[DashboardRepositoryPayload],
) -> DashboardOverviewPayload:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    approval_values = [
        snapshot.approval_hours
        for snapshot in snapshots
        if snapshot.approval_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours
        for snapshot in snapshots
        if snapshot.merge_hours is not None
    ]
    close_values = [
        snapshot.close_hours
        for snapshot in snapshots
        if snapshot.close_hours is not None
    ]
    return DashboardOverviewPayload(
        org=org,
        time_anchor="created_at",
        time_anchor_context=build_time_anchor_context("created_at"),
        since=since.isoformat(),
        until=until.isoformat(),
        source_as_of=source_as_of.isoformat(),
        generated_at=datetime.now(UTC).isoformat(),
        pull_requests=len(snapshots),
        merged_pull_requests=sum(1 for snapshot in snapshots if snapshot.merged_at),
        open_pull_requests=sum(1 for snapshot in snapshots if snapshot.state == "open"),
        repositories=len(repository_rows),
        authors=len(author_rows),
        review_submissions=sum(row.review_submissions for row in reviewer_rows),
        unique_reviewers=len(reviewer_rows),
        total_changed_lines=sum(snapshot.changed_lines for snapshot in snapshots),
        total_commits=sum(snapshot.commits for snapshot in snapshots),
        median_first_review_hours=_round(_median_or_none(first_review_values)),
        median_approval_hours=_round(_median_or_none(approval_values)),
        median_merge_hours=_round(_median_or_none(merge_values)),
        median_close_hours=_round(_median_or_none(close_values)),
        average_reviews_per_pr=_round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        average_changed_lines_per_pr=_round(
            sum(snapshot.changed_lines for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        review_coverage_pct=_round(
            (
                sum(1 for snapshot in snapshots if snapshot.review_count > 0)
                / len(snapshots)
                * 100
            )
            if snapshots
            else None
        ),
        merge_rate_pct=_round(
            (
                sum(1 for snapshot in snapshots if snapshot.merged_at)
                / len(snapshots)
                * 100
            )
            if snapshots
            else None
        ),
        top_repository=repository_rows[0].repository_full_name
        if repository_rows
        else None,
        top_author=author_rows[0].author_login if author_rows else None,
        open_week=source_as_of < _week_end(until),
        open_week_key=_week_key(until) if source_as_of < _week_end(until) else None,
        open_month=source_as_of < _month_end(until),
        open_month_key=until.strftime("%Y-%m")
        if source_as_of < _month_end(until)
        else None,
    )


def _week_key(current: date) -> str:
    iso_year, iso_week, _iso_weekday = current.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _week_end(current: date) -> date:
    return current + timedelta(days=6 - current.weekday())


def _month_end(current: date) -> date:
    return PeriodGrain.MONTH.end_for(current)


def _author_rows(
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> list[DashboardAuthorPayload]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.author_login].append(snapshot)
    rows = [
        _author_row(
            author_login,
            grouped_snapshots,
            total_pull_requests=total_pull_requests,
        )
        for author_login, grouped_snapshots in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (-row.pull_requests, -row.changed_lines, row.author_login),
    )


def _reviewer_rows(
    review_facts: tuple[ReviewFact, ...],
    *,
    since: date,
    until: date,
) -> list[DashboardReviewerPayload]:
    review_counts: Counter[str] = Counter()
    reviewed_line_totals: Counter[str] = Counter()
    approval_counts: Counter[str] = Counter()
    change_request_counts: Counter[str] = Counter()
    comment_counts: Counter[str] = Counter()
    prs_reviewed: dict[str, set[str]] = defaultdict(set)
    authors_supported: dict[str, set[str]] = defaultdict(set)
    month_span_count = PeriodGrain.MONTH.count_periods(since, until)
    for review in review_facts:
        if review.author_login is None or review.submitted_at is None:
            continue
        if _same_login(review.author_login, review.pull_request_author_login):
            continue
        if review.submitted_at.date() < since or review.submitted_at.date() > until:
            continue
        pull_request_key = f"{review.repository_full_name}#{review.pull_request_number}"
        reviewer_login = review.author_login
        review_counts[reviewer_login] += 1
        if pull_request_key not in prs_reviewed[reviewer_login]:
            prs_reviewed[reviewer_login].add(pull_request_key)
            reviewed_line_totals[reviewer_login] += review.pull_request_changed_lines
        if review.pull_request_author_login is not None:
            authors_supported[reviewer_login].add(review.pull_request_author_login)
        if review.state == "APPROVED":
            approval_counts[reviewer_login] += 1
        if review.state == "CHANGES_REQUESTED":
            change_request_counts[reviewer_login] += 1
        if review.state == "COMMENTED":
            comment_counts[reviewer_login] += 1
    rows = []
    for reviewer_login in review_counts:
        rows.append(
            DashboardReviewerPayload(
                reviewer_login=reviewer_login,
                review_submissions=review_counts[reviewer_login],
                pull_requests_reviewed=len(prs_reviewed[reviewer_login]),
                reviewed_lines=reviewed_line_totals[reviewer_login],
                pull_requests_reviewed_per_month=_round(
                    len(prs_reviewed[reviewer_login]) / month_span_count
                ),
                reviewed_lines_per_month=_round(
                    reviewed_line_totals[reviewer_login] / month_span_count
                ),
                approvals=approval_counts[reviewer_login],
                changes_requested=change_request_counts[reviewer_login],
                comments=comment_counts[reviewer_login],
                authors_supported=len(authors_supported[reviewer_login]),
            )
        )
    return sorted(
        rows,
        key=lambda row: (
            -row.pull_requests_reviewed,
            -row.review_submissions,
            row.reviewer_login,
        ),
    )


def _reviewer_trend_rows(
    review_facts: tuple[ReviewFact, ...],
    *,
    grain: PeriodGrain,
    since: date,
    until: date,
) -> list[DashboardReviewerTrendPayload]:
    review_counts: Counter[tuple[str, str]] = Counter()
    reviewed_line_totals: Counter[tuple[str, str]] = Counter()
    prs_reviewed: dict[tuple[str, str], set[str]] = defaultdict(set)
    for review in review_facts:
        if review.author_login is None or review.submitted_at is None:
            continue
        if _same_login(review.author_login, review.pull_request_author_login):
            continue
        submitted_date = review.submitted_at.date()
        if submitted_date < since or submitted_date > until:
            continue
        period_key = _review_period_key(submitted_date, grain=grain)
        reviewer_period_key = (review.author_login, period_key)
        pull_request_key = f"{review.repository_full_name}#{review.pull_request_number}"
        review_counts[reviewer_period_key] += 1
        if pull_request_key in prs_reviewed[reviewer_period_key]:
            continue
        prs_reviewed[reviewer_period_key].add(pull_request_key)
        reviewed_line_totals[reviewer_period_key] += review.pull_request_changed_lines
    return [
        DashboardReviewerTrendPayload(
            reviewer_login=reviewer_login,
            period_key=period_key,
            review_submissions_given=review_counts[(reviewer_login, period_key)],
            pull_requests_reviewed=len(prs_reviewed[(reviewer_login, period_key)]),
            reviewed_lines=reviewed_line_totals[(reviewer_login, period_key)],
        )
        for reviewer_login, period_key in sorted(review_counts)
    ]


def _review_period_key(
    submitted_date: date,
    *,
    grain: PeriodGrain,
) -> str:
    if grain is PeriodGrain.MONTH:
        return submitted_date.strftime("%Y-%m")
    return _week_key(submitted_date)


def _same_login(
    left: str | None,
    right: str | None,
) -> bool:
    if left is None or right is None:
        return False
    return left.lower() == right.lower()


def _repository_rows(
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> list[DashboardRepositoryPayload]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.repository_full_name].append(snapshot)
    rows = [
        _repository_row(
            repository_full_name,
            grouped_snapshots,
            total_pull_requests=total_pull_requests,
        )
        for repository_full_name, grouped_snapshots in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (
            -row.pull_requests,
            -row.changed_lines,
            row.repository_full_name,
        ),
    )


def _size_bucket_rows(
    snapshots: list[PullRequestSnapshot],
) -> list[DashboardSizeBucketPayload]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.size_bucket].append(snapshot)
    ordered_buckets = ["XS", "S", "M", "L", "XL"]
    rows = []
    for bucket in ordered_buckets:
        bucket_snapshots = grouped.get(bucket, [])
        rows.append(
            DashboardSizeBucketPayload(
                bucket=bucket,
                pull_requests=len(bucket_snapshots),
                median_changed_lines=_round(
                    _median_or_none(
                        [snapshot.changed_lines for snapshot in bucket_snapshots]
                    )
                ),
                median_first_review_hours=_round(
                    _median_or_none(
                        [
                            snapshot.first_review_hours
                            for snapshot in bucket_snapshots
                            if snapshot.first_review_hours is not None
                        ]
                    )
                ),
                median_approval_hours=_round(
                    _median_or_none(
                        [
                            snapshot.approval_hours
                            for snapshot in bucket_snapshots
                            if snapshot.approval_hours is not None
                        ]
                    )
                ),
                median_merge_hours=_round(
                    _median_or_none(
                        [
                            snapshot.merge_hours
                            for snapshot in bucket_snapshots
                            if snapshot.merge_hours is not None
                        ]
                    )
                ),
                average_reviews_per_pr=_round(
                    (
                        sum(snapshot.review_count for snapshot in bucket_snapshots)
                        / len(bucket_snapshots)
                    )
                    if bucket_snapshots
                    else None
                ),
            )
        )
    return rows


def _review_state_rows(
    snapshots: list[PullRequestSnapshot],
) -> list[DashboardReviewStatePayload]:
    states = Counter()
    for snapshot in snapshots:
        states["APPROVED"] += snapshot.approval_count
        states["CHANGES_REQUESTED"] += snapshot.changes_requested_count
        states["COMMENTED"] += snapshot.comment_review_count
    total = sum(states.values())
    return [
        DashboardReviewStatePayload(
            state=state,
            count=count,
            share_pct=_round((count / total) * 100 if total else None),
        )
        for state, count in sorted(states.items(), key=lambda item: (-item[1], item[0]))
    ]


def _insights(
    *,
    overview: DashboardOverviewPayload,
    author_rows: list[DashboardAuthorPayload],
    reviewer_rows: list[DashboardReviewerPayload],
    repository_rows: list[DashboardRepositoryPayload],
    size_bucket_rows: list[DashboardSizeBucketPayload],
) -> list[DashboardInsightPayload]:
    insights: list[DashboardInsightPayload] = []
    if repository_rows:
        top_repo = repository_rows[0]
        insights.append(
            DashboardInsightPayload(
                title="Throughput concentration",
                body=(
                    f"{top_repo.repository_full_name} accounted for "
                    f"{top_repo.pull_requests} PRs and "
                    f"{top_repo.share_of_prs_pct}% of total flow."
                ),
            )
        )
    fast_review_authors = [
        row
        for row in author_rows
        if row.pull_requests >= 20 and row.median_first_review_hours is not None
    ]
    if fast_review_authors:
        fastest_author = min(
            fast_review_authors,
            key=lambda row: row.median_first_review_hours or 0,
        )
        insights.append(
            DashboardInsightPayload(
                title="Fastest first review",
                body=(
                    f"{fastest_author.author_login} had the fastest median first review "
                    f"among authors with 20+ PRs at "
                    f"{fastest_author.median_first_review_hours} hours."
                ),
            )
        )
    if reviewer_rows:
        top_reviewer = reviewer_rows[0]
        insights.append(
            DashboardInsightPayload(
                title="Review load",
                body=(
                    f"{top_reviewer.reviewer_login} submitted "
                    f"{top_reviewer.review_submissions} reviews across "
                    f"{top_reviewer.pull_requests_reviewed} PRs."
                ),
            )
        )
    large_buckets = [row for row in size_bucket_rows if row.bucket in {"L", "XL"}]
    if large_buckets:
        slowest_bucket = max(
            large_buckets,
            key=lambda row: row.median_first_review_hours or -1,
        )
        if slowest_bucket.median_first_review_hours is not None:
            insights.append(
                DashboardInsightPayload(
                    title="First-review size penalty",
                    body=(
                        f"{slowest_bucket.bucket} PRs waited "
                        f"{slowest_bucket.median_first_review_hours} hours median "
                        f"for first review."
                    ),
                )
            )
    insights.append(
        DashboardInsightPayload(
            title="Review coverage",
            body=(
                f"{overview.review_coverage_pct}% of PRs received at least one review "
                "submission in the selected window."
            ),
        )
    )
    return insights


def _author_row(
    author_login: str,
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> DashboardAuthorPayload:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    approval_values = [
        snapshot.approval_hours
        for snapshot in snapshots
        if snapshot.approval_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours
        for snapshot in snapshots
        if snapshot.merge_hours is not None
    ]
    return DashboardAuthorPayload(
        author_login=author_login,
        pull_requests=len(snapshots),
        merged_pull_requests=sum(1 for snapshot in snapshots if snapshot.merged_at),
        open_pull_requests=sum(1 for snapshot in snapshots if snapshot.state == "open"),
        changed_lines=sum(snapshot.changed_lines for snapshot in snapshots),
        commits=sum(snapshot.commits for snapshot in snapshots),
        review_submissions_received=sum(
            snapshot.review_count for snapshot in snapshots
        ),
        average_reviews_per_pr=_round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        median_first_review_hours=_round(_median_or_none(first_review_values)),
        median_approval_hours=_round(_median_or_none(approval_values)),
        median_merge_hours=_round(_median_or_none(merge_values)),
        median_changed_lines=_round(
            _median_or_none([snapshot.changed_lines for snapshot in snapshots])
        ),
        share_of_prs_pct=_round(
            (len(snapshots) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    )


def _repository_row(
    repository_full_name: str,
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> DashboardRepositoryPayload:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    approval_values = [
        snapshot.approval_hours
        for snapshot in snapshots
        if snapshot.approval_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours
        for snapshot in snapshots
        if snapshot.merge_hours is not None
    ]
    return DashboardRepositoryPayload(
        repository_full_name=repository_full_name,
        pull_requests=len(snapshots),
        merged_pull_requests=sum(1 for snapshot in snapshots if snapshot.merged_at),
        open_pull_requests=sum(1 for snapshot in snapshots if snapshot.state == "open"),
        authors=len({snapshot.author_login for snapshot in snapshots}),
        changed_lines=sum(snapshot.changed_lines for snapshot in snapshots),
        review_submissions=sum(snapshot.review_count for snapshot in snapshots),
        average_reviews_per_pr=_round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        median_first_review_hours=_round(_median_or_none(first_review_values)),
        median_approval_hours=_round(_median_or_none(approval_values)),
        median_merge_hours=_round(_median_or_none(merge_values)),
        share_of_prs_pct=_round(
            (len(snapshots) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    )


def _snapshot_row(snapshot: PullRequestSnapshot) -> DashboardPullRequestPayload:
    return DashboardPullRequestPayload(
        repository_full_name=snapshot.repository_full_name,
        pull_request_number=snapshot.number,
        title=snapshot.title,
        author_login=snapshot.author_login,
        state=snapshot.state,
        created_at=snapshot.created_at.isoformat(),
        updated_at=snapshot.updated_at.isoformat(),
        closed_at=None
        if snapshot.closed_at is None
        else snapshot.closed_at.isoformat(),
        merged_at=None
        if snapshot.merged_at is None
        else snapshot.merged_at.isoformat(),
        html_url=snapshot.html_url,
        additions=snapshot.additions,
        deletions=snapshot.deletions,
        changed_files=snapshot.changed_files,
        changed_lines=snapshot.changed_lines,
        commits=snapshot.commits,
        review_count=snapshot.review_count,
        approval_count=snapshot.approval_count,
        changes_requested_count=snapshot.changes_requested_count,
        comment_review_count=snapshot.comment_review_count,
        reviewer_count=snapshot.reviewer_count,
        first_review_hours=snapshot.first_review_hours,
        approval_hours=snapshot.approval_hours,
        merge_hours=snapshot.merge_hours,
        close_hours=snapshot.close_hours,
        review_rounds=snapshot.review_rounds,
        review_ready_at=snapshot.review_ready_at.isoformat(),
        review_requested_at=(
            None
            if snapshot.review_requested_at is None
            else snapshot.review_requested_at.isoformat()
        ),
        size_bucket=snapshot.size_bucket,
    )


def _parse_datetime(value: str) -> datetime:
    parsed = _parse_optional_datetime(value)
    if parsed is None:
        raise ValueError(f"expected an ISO datetime, received {value!r}")
    return parsed


def _parse_bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _size_bucket(changed_lines: int) -> str:
    if changed_lines <= 100:
        return "XS"
    if changed_lines <= 400:
        return "S"
    if changed_lines <= 1000:
        return "M"
    if changed_lines <= 3000:
        return "L"
    return "XL"


def _median_or_none(values: list[float | int]) -> float | None:
    normalized = [float(value) for value in values]
    if not normalized:
        return None
    return float(median(normalized))


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


if __name__ == "__main__":
    raise SystemExit(
        "orgpulse.apps.dashboard.service is no longer executable as a module. "
        "Use `orgpulse dashboard`."
    )
