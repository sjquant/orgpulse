from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from github import Auth, Github
from pydantic import ValidationError
from render_manual_org_dashboard import (
    prepare_manual_dashboard_payload,
    render_manual_dashboard_html,
)

from orgpulse.cli import (
    _build_metric_outputs,
    _write_org_summary,
    build_run_config,
)
from orgpulse.cli import (
    _write_outputs as _write_run_outputs,
)
from orgpulse.errors import AuthResolutionError, GitHubApiError, OrgTargetingError
from orgpulse.github_auth import GitHubAuthService, resolve_auth_token
from orgpulse.ingestion import GitHubIngestionService
from orgpulse.models import RawSnapshotPeriod, ReportingPeriod, RunManifest, RunMode


@dataclass(frozen=True)
class PullRequestReview:
    author_login: str
    state: str
    submitted_at: datetime


@dataclass(frozen=True)
class PullRequestTimelineEvent:
    event: str
    created_at: datetime | None
    requested_reviewer_login: str | None


@dataclass(frozen=True)
class PullRequestSnapshot:
    repository_full_name: str
    number: int
    title: str
    author_login: str
    state: str
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
    merge_hours: float | None
    close_hours: float | None
    review_rounds: int
    review_requested_at: datetime | None
    review_ready_at: datetime
    size_bucket: str
    reviews: tuple[PullRequestReview, ...]


def main() -> None:
    args = _parse_args()
    since = date.fromisoformat(args.since)
    until = date.fromisoformat(args.until)
    source_manifest = _try_load_source_manifest(
        org=args.org,
        source_output_dir=args.source_output_dir,
    )
    try:
        if args.refresh:
            _refresh_local_source_outputs(
                org=args.org,
                as_of=date.today(),
                source_output_dir=args.source_output_dir,
                source_manifest=source_manifest,
            )
    except (AuthResolutionError, GitHubApiError, OrgTargetingError) as exc:
        raise RuntimeError(f"failed to refresh local source outputs: {exc}") from exc
    payload = build_manual_dashboard_payload_from_local_outputs(
        org=args.org,
        since=since,
        until=until,
        source_output_dir=args.source_output_dir,
    )
    _write_outputs(
        output_dir=args.output_dir,
        base_name=args.base_name,
        payload=payload,
        distribution_percentile=args.distribution_percentile,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a manual org dashboard from local orgpulse raw snapshots. "
            "By default this refreshes the current open period incrementally first."
        ),
    )
    parser.add_argument("--org", required=True)
    parser.add_argument("--since", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--until", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--base-name", required=True)
    parser.add_argument(
        "--source-output-dir",
        type=Path,
        default=Path("output"),
        help="Root orgpulse output directory used for manifest and raw snapshots.",
    )
    parser.add_argument(
        "--refresh",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Refresh local orgpulse outputs incrementally before rendering.",
    )
    parser.add_argument(
        "--distribution-percentile",
        type=int,
        default=100,
        choices=(95, 99, 100),
        help="Upper-tail percentile retained for distribution-based metrics in HTML output.",
    )
    return parser.parse_args()


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

    resolved_token = resolve_auth_token(config)
    github_client = Github(auth=Auth.Token(resolved_token.token))
    GitHubAuthService(github_client, resolved_token.source).validate_access(config)
    ingestion_service = GitHubIngestionService(github_client)
    inventory = ingestion_service.load_repository_inventory(config)
    collection = ingestion_service.fetch_pull_requests(config, inventory)
    (
        raw_snapshot,
        raw_snapshot_skipped_reason,
        manifest,
        _manifest_skipped_reason,
    ) = _write_run_outputs(
        config,
        len(inventory.repositories),
        collection,
    )
    (
        _repo_summary,
        _repo_summary_skipped_reason,
        org_metrics,
        org_metrics_skipped_reason,
        _metric_validation,
        _metric_validation_skipped_reason,
    ) = _build_metric_outputs(
        config,
        manifest=manifest,
        raw_snapshot=raw_snapshot,
        raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
    )
    _write_org_summary(
        config,
        org_metrics=org_metrics,
        org_metrics_skipped_reason=org_metrics_skipped_reason,
        refreshed_period_keys=()
        if raw_snapshot is None
        else tuple(period.key for period in raw_snapshot.periods),
    )
    if not collection.failures:
        ingestion_service.clear_checkpoint(config)


def build_manual_dashboard_payload_from_local_outputs(
    *,
    org: str,
    since: date,
    until: date,
    source_output_dir: Path,
) -> dict[str, Any]:
    manifest = _load_source_manifest(
        org=org,
        source_output_dir=source_output_dir,
    )
    period_index = _snapshot_period_index(manifest)
    _validate_local_source_coverage(
        manifest=manifest,
        period_index=period_index,
        since=since,
        until=until,
    )
    snapshots = _load_local_snapshots(
        period_index=period_index,
        since=since,
        until=until,
    )
    return _build_dashboard_payload(
        org=org,
        since=since,
        until=until,
        snapshots=snapshots,
    )


def _load_source_manifest(
    *,
    org: str,
    source_output_dir: Path,
) -> RunManifest:
    manifest_path = (
        source_output_dir
        / "manifest"
        / "month"
        / "created_at"
        / "manifest.json"
    )
    if not manifest_path.exists():
        raise RuntimeError(
            "local manual dashboard source is missing: "
            f"{manifest_path}. Run `orgpulse run --org {org}` first."
        )
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"local manifest is unreadable: {manifest_path}") from exc
    manifest = RunManifest.model_validate(payload)
    if manifest.target_org.lower() != org.lower():
        raise RuntimeError(
            "local manifest org does not match the requested org: "
            f"expected {org}, found {manifest.target_org}"
        )
    return manifest


def _try_load_source_manifest(
    *,
    org: str,
    source_output_dir: Path,
) -> RunManifest | None:
    manifest_path = (
        source_output_dir
        / "manifest"
        / "month"
        / "created_at"
        / "manifest.json"
    )
    if not manifest_path.exists():
        return None
    return _load_source_manifest(
        org=org,
        source_output_dir=source_output_dir,
    )


def _snapshot_period_index(
    manifest: RunManifest,
) -> dict[str, RawSnapshotPeriod]:
    return {
        period.key: _build_snapshot_period(
            manifest.raw_snapshot_root_dir,
            period,
        )
        for period in (*manifest.locked_periods, *manifest.refreshed_periods)
    }


def _build_snapshot_period(
    root_dir: Path,
    period: ReportingPeriod | RawSnapshotPeriod,
) -> RawSnapshotPeriod:
    if isinstance(period, RawSnapshotPeriod):
        return period
    period_dir = root_dir / period.key
    return RawSnapshotPeriod(
        key=period.key,
        start_date=period.start_date,
        end_date=period.end_date,
        closed=period.closed,
        directory=period_dir,
        pull_requests_path=period_dir / "pull_requests.csv",
        pull_request_count=0,
        reviews_path=period_dir / "pull_request_reviews.csv",
        review_count=0,
        timeline_events_path=period_dir / "pull_request_timeline_events.csv",
        timeline_event_count=0,
    )


def _validate_local_source_coverage(
    *,
    manifest: RunManifest,
    period_index: dict[str, RawSnapshotPeriod],
    since: date,
    until: date,
) -> None:
    if until > manifest.last_successful_run.as_of:
        raise RuntimeError(
            "local source outputs are stale for the requested window: "
            f"latest local as-of is {manifest.last_successful_run.as_of.isoformat()}, "
            f"but --until is {until.isoformat()}."
        )
    expected_keys = _period_keys_for_window(since=since, until=until)
    missing_keys = [key for key in expected_keys if key not in period_index]
    if missing_keys:
        raise RuntimeError(
            "local source outputs do not cover the requested historical window. "
            f"Missing periods: {', '.join(missing_keys)}. "
            "Run a full rebuild or period backfill before rendering this dashboard."
        )


def _period_keys_for_window(
    *,
    since: date,
    until: date,
) -> list[str]:
    keys: list[str] = []
    cursor = since.replace(day=1)
    until_month_start = until.replace(day=1)
    while cursor <= until_month_start:
        keys.append(cursor.strftime("%Y-%m"))
        cursor = _next_month_start(cursor)
    return keys


def _next_month_start(current: date) -> date:
    next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return next_month


def _load_local_snapshots(
    *,
    period_index: dict[str, RawSnapshotPeriod],
    since: date,
    until: date,
) -> list[PullRequestSnapshot]:
    snapshots: list[PullRequestSnapshot] = []
    for period_key in _period_keys_for_window(since=since, until=until):
        period = period_index[period_key]
        pull_request_rows = _read_csv_rows(period.pull_requests_path)
        review_rows = _read_csv_rows(period.reviews_path)
        timeline_rows = _read_csv_rows(period.timeline_events_path)
        reviews_by_pull_request = _reviews_by_pull_request(review_rows)
        timeline_events_by_pull_request = _timeline_events_by_pull_request(timeline_rows)
        for pull_request_row in pull_request_rows:
            created_at = _parse_datetime(pull_request_row["created_at"])
            if created_at.date() < since or created_at.date() > until:
                continue
            pull_request_key = _pull_request_key(
                repository_full_name=pull_request_row["repository_full_name"],
                pull_request_number=pull_request_row["pull_request_number"],
            )
            reviews = reviews_by_pull_request.get(pull_request_key, [])
            timeline_events = timeline_events_by_pull_request.get(pull_request_key, [])
            snapshots.append(
                _snapshot_from_local_rows(
                    pull_request_row,
                    reviews=reviews,
                    timeline_events=timeline_events,
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


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise RuntimeError(f"local source snapshot file is missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _reviews_by_pull_request(
    review_rows: list[dict[str, str]],
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
                author_login=review_row["author_login"] or "ghost",
                state=review_row["state"],
                submitted_at=submitted_at,
            )
        )
    for reviews in grouped.values():
        reviews.sort(key=lambda review: review.submitted_at)
    return grouped


def _timeline_events_by_pull_request(
    timeline_rows: list[dict[str, str]],
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
                event=timeline_row["event"],
                created_at=_parse_optional_datetime(timeline_row["created_at"]),
                requested_reviewer_login=(
                    timeline_row["requested_reviewer_login"] or None
                ),
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
    review_ready_at, review_requested_at, first_review_at = _review_cycle_markers(
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
        comment_review_count=sum(1 for review in reviews if review.state == "COMMENTED"),
        reviewer_count=len({review.author_login for review in reviews}),
        first_review_at=first_review_at,
        first_review_hours=_hours_between(
            review_requested_at or review_ready_at,
            first_review_at,
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
        review_requested_at=review_requested_at,
        review_ready_at=review_ready_at,
        size_bucket=_size_bucket(additions + deletions),
        reviews=tuple(reviews),
    )


def _build_dashboard_payload(
    *,
    org: str,
    since: date,
    until: date,
    snapshots: list[PullRequestSnapshot],
) -> dict[str, Any]:
    total_pull_requests = len(snapshots)
    created_series = _time_series(
        snapshots=snapshots,
        date_selector=lambda snapshot: snapshot.created_at.date(),
    )
    merged_series = _time_series(
        snapshots=[snapshot for snapshot in snapshots if snapshot.merged_at is not None],
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
    reviewer_rows = _reviewer_rows(snapshots)
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
        snapshots=snapshots,
        author_rows=author_rows,
        reviewer_rows=reviewer_rows,
        repository_rows=repository_rows,
    )
    return {
        "overview": overview,
        "insights": _insights(
            overview=overview,
            author_rows=author_rows,
            reviewer_rows=reviewer_rows,
            repository_rows=repository_rows,
            size_bucket_rows=size_bucket_rows,
        ),
        "charts": {
            "created_series": created_series,
            "merged_series": merged_series,
            "review_series": review_series,
            "author_throughput": [
                {
                    "label": row["author_login"],
                    "pull_requests": row["pull_requests"],
                    "merged_pull_requests": row["merged_pull_requests"],
                    "changed_lines": row["changed_lines"],
                }
                for row in author_rows[:8]
            ],
            "review_latency_by_author": [
                {
                    "label": row["author_login"],
                    "median_first_review_hours": row["median_first_review_hours"],
                }
                for row in author_rows
                if row["median_first_review_hours"] is not None
            ][:8],
            "repository_throughput": [
                {
                    "label": row["repository_full_name"],
                    "pull_requests": row["pull_requests"],
                    "merged_pull_requests": row["merged_pull_requests"],
                }
                for row in repository_rows[:10]
            ],
            "size_bucket_latency": size_bucket_rows,
        },
        "authors": author_rows,
        "reviewers": reviewer_rows,
        "repositories": repository_rows,
        "size_buckets": size_bucket_rows,
        "review_state_rows": review_state_rows,
        "pull_requests": [_snapshot_row(snapshot) for snapshot in snapshots],
    }


def _write_outputs(
    *,
    output_dir: Path,
    base_name: str,
    payload: dict[str, Any],
    distribution_percentile: int,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{base_name}.json"
    csv_path = output_dir / f"{base_name}-prs.csv"
    html_path = output_dir / f"{base_name}.html"
    json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        rows = payload["pull_requests"]
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    html_path.write_text(
        render_manual_dashboard_html(
            prepare_manual_dashboard_payload(
                payload,
                distribution_percentile=distribution_percentile,
            )
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "json_path": str(json_path),
                "csv_path": str(csv_path),
                "html_path": str(html_path),
                "pull_requests": payload["overview"]["pull_requests"],
                "distribution_percentile": distribution_percentile,
            },
            ensure_ascii=False,
        )
    )


def _review_cycle_markers(
    *,
    created_at: datetime,
    timeline_events: list[PullRequestTimelineEvent],
    reviews: list[PullRequestReview],
) -> tuple[datetime, datetime | None, datetime | None]:
    review_ready_at = created_at
    review_requested_at: datetime | None = None
    first_review_at: datetime | None = None
    markers = [
        ("event", event.created_at, event)
        for event in timeline_events
        if event.created_at is not None
    ] + [
        ("review", review.submitted_at, review)
        for review in reviews
    ]
    markers.sort(
        key=lambda marker: (
            marker[1].isoformat(),
            0 if marker[0] == "event" else 1,
        )
    )
    for marker_type, marker_at, marker in markers:
        if marker_type == "event":
            event = marker
            if event.event in {"converted_to_draft", "ready_for_review"}:
                review_ready_at = marker_at
                review_requested_at = None
                first_review_at = None
                continue
            if (
                event.event == "review_requested"
                and marker_at >= review_ready_at
                and first_review_at is None
                and review_requested_at is None
            ):
                review_requested_at = marker_at
            continue
        if marker_at >= review_ready_at and first_review_at is None:
            first_review_at = marker_at
    return review_ready_at, review_requested_at, first_review_at


def _time_series(
    *,
    snapshots: list[PullRequestSnapshot],
    date_selector,
) -> list[dict[str, Any]]:
    counts = Counter(date_selector(snapshot).isoformat() for snapshot in snapshots)
    return [
        {"date": label, "count": counts[label]}
        for label in sorted(counts)
    ]


def _overview_summary(
    *,
    org: str,
    since: date,
    until: date,
    snapshots: list[PullRequestSnapshot],
    author_rows: list[dict[str, Any]],
    reviewer_rows: list[dict[str, Any]],
    repository_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours for snapshot in snapshots if snapshot.merge_hours is not None
    ]
    close_values = [
        snapshot.close_hours for snapshot in snapshots if snapshot.close_hours is not None
    ]
    return {
        "org": org,
        "time_anchor": "created_at",
        "since": since.isoformat(),
        "until": until.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "pull_requests": len(snapshots),
        "merged_pull_requests": sum(1 for snapshot in snapshots if snapshot.merged_at),
        "open_pull_requests": sum(1 for snapshot in snapshots if snapshot.state == "open"),
        "repositories": len(repository_rows),
        "authors": len(author_rows),
        "review_submissions": sum(snapshot.review_count for snapshot in snapshots),
        "unique_reviewers": len(reviewer_rows),
        "total_changed_lines": sum(snapshot.changed_lines for snapshot in snapshots),
        "total_commits": sum(snapshot.commits for snapshot in snapshots),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "median_close_hours": _round(_median_or_none(close_values)),
        "average_reviews_per_pr": _round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "average_changed_lines_per_pr": _round(
            sum(snapshot.changed_lines for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "review_coverage_pct": _round(
            (
                sum(1 for snapshot in snapshots if snapshot.review_count > 0)
                / len(snapshots)
                * 100
            )
            if snapshots
            else None
        ),
        "merge_rate_pct": _round(
            (
                sum(1 for snapshot in snapshots if snapshot.merged_at)
                / len(snapshots)
                * 100
            )
            if snapshots
            else None
        ),
        "top_repository": repository_rows[0]["repository_full_name"] if repository_rows else None,
        "top_author": author_rows[0]["author_login"] if author_rows else None,
    }


def _author_rows(
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> list[dict[str, Any]]:
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
        key=lambda row: (-row["pull_requests"], -row["changed_lines"], row["author_login"]),
    )


def _reviewer_rows(snapshots: list[PullRequestSnapshot]) -> list[dict[str, Any]]:
    review_records: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    review_counts: Counter[str] = Counter()
    approval_counts: Counter[str] = Counter()
    change_request_counts: Counter[str] = Counter()
    comment_counts: Counter[str] = Counter()
    prs_reviewed: dict[str, set[str]] = defaultdict(set)
    for snapshot in snapshots:
        for review in snapshot.reviews:
            review_records[review.author_login].append(snapshot)
            review_counts[review.author_login] += 1
            prs_reviewed[review.author_login].add(
                f"{snapshot.repository_full_name}#{snapshot.number}"
            )
            if review.state == "APPROVED":
                approval_counts[review.author_login] += 1
            if review.state == "CHANGES_REQUESTED":
                change_request_counts[review.author_login] += 1
            if review.state == "COMMENTED":
                comment_counts[review.author_login] += 1
    rows = []
    for reviewer_login, reviewer_snapshots in review_records.items():
        rows.append(
            {
                "reviewer_login": reviewer_login,
                "review_submissions": review_counts[reviewer_login],
                "pull_requests_reviewed": len(prs_reviewed[reviewer_login]),
                "approvals": approval_counts[reviewer_login],
                "changes_requested": change_request_counts[reviewer_login],
                "comments": comment_counts[reviewer_login],
                "authors_supported": len(
                    {snapshot.author_login for snapshot in reviewer_snapshots}
                ),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            -row["pull_requests_reviewed"],
            -row["review_submissions"],
            row["reviewer_login"],
        ),
    )


def _repository_rows(
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> list[dict[str, Any]]:
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
            -row["pull_requests"],
            -row["changed_lines"],
            row["repository_full_name"],
        ),
    )


def _size_bucket_rows(snapshots: list[PullRequestSnapshot]) -> list[dict[str, Any]]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.size_bucket].append(snapshot)
    ordered_buckets = ["XS", "S", "M", "L", "XL"]
    rows = []
    for bucket in ordered_buckets:
        bucket_snapshots = grouped.get(bucket, [])
        rows.append(
            {
                "bucket": bucket,
                "pull_requests": len(bucket_snapshots),
                "median_changed_lines": _round(
                    _median_or_none([snapshot.changed_lines for snapshot in bucket_snapshots])
                ),
                "median_first_review_hours": _round(
                    _median_or_none(
                        [
                            snapshot.first_review_hours
                            for snapshot in bucket_snapshots
                            if snapshot.first_review_hours is not None
                        ]
                    )
                ),
                "median_merge_hours": _round(
                    _median_or_none(
                        [
                            snapshot.merge_hours
                            for snapshot in bucket_snapshots
                            if snapshot.merge_hours is not None
                        ]
                    )
                ),
                "average_reviews_per_pr": _round(
                    (
                        sum(snapshot.review_count for snapshot in bucket_snapshots)
                        / len(bucket_snapshots)
                    )
                    if bucket_snapshots
                    else None
                ),
            }
        )
    return rows


def _review_state_rows(snapshots: list[PullRequestSnapshot]) -> list[dict[str, Any]]:
    states = Counter()
    for snapshot in snapshots:
        states["APPROVED"] += snapshot.approval_count
        states["CHANGES_REQUESTED"] += snapshot.changes_requested_count
        states["COMMENTED"] += snapshot.comment_review_count
    total = sum(states.values())
    return [
        {
            "state": state,
            "count": count,
            "share_pct": _round((count / total) * 100 if total else None),
        }
        for state, count in sorted(states.items(), key=lambda item: (-item[1], item[0]))
    ]


def _insights(
    *,
    overview: dict[str, Any],
    author_rows: list[dict[str, Any]],
    reviewer_rows: list[dict[str, Any]],
    repository_rows: list[dict[str, Any]],
    size_bucket_rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    insights: list[dict[str, str]] = []
    if repository_rows:
        top_repo = repository_rows[0]
        insights.append(
            {
                "title": "Throughput concentration",
                "body": (
                    f"{top_repo['repository_full_name']} accounted for "
                    f"{top_repo['pull_requests']} PRs and "
                    f"{top_repo['share_of_prs_pct']}% of total flow."
                ),
            }
        )
    fast_review_authors = [
        row
        for row in author_rows
        if row["pull_requests"] >= 20 and row["median_first_review_hours"] is not None
    ]
    if fast_review_authors:
        fastest_author = min(
            fast_review_authors,
            key=lambda row: row["median_first_review_hours"],
        )
        insights.append(
            {
                "title": "Fastest review entry",
                "body": (
                    f"{fastest_author['author_login']} had the fastest median first review "
                    f"among authors with 20+ PRs at "
                    f"{fastest_author['median_first_review_hours']} hours."
                ),
            }
        )
    if reviewer_rows:
        top_reviewer = reviewer_rows[0]
        insights.append(
            {
                "title": "Review load",
                "body": (
                    f"{top_reviewer['reviewer_login']} submitted "
                    f"{top_reviewer['review_submissions']} reviews across "
                    f"{top_reviewer['pull_requests_reviewed']} PRs."
                ),
            }
        )
    large_buckets = [row for row in size_bucket_rows if row["bucket"] in {"L", "XL"}]
    if large_buckets:
        slowest_bucket = max(
            large_buckets,
            key=lambda row: row["median_first_review_hours"] or -1,
        )
        if slowest_bucket["median_first_review_hours"] is not None:
            insights.append(
                {
                    "title": "Size penalty",
                    "body": (
                        f"{slowest_bucket['bucket']} PRs waited "
                        f"{slowest_bucket['median_first_review_hours']} hours median "
                        f"for first review."
                    ),
                }
            )
    insights.append(
        {
            "title": "Review coverage",
            "body": (
                f"{overview['review_coverage_pct']}% of PRs received at least one review "
                f"submission in the selected window."
            ),
        }
    )
    return insights


def _author_row(
    author_login: str,
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> dict[str, Any]:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours for snapshot in snapshots if snapshot.merge_hours is not None
    ]
    return {
        "author_login": author_login,
        "pull_requests": len(snapshots),
        "merged_pull_requests": sum(1 for snapshot in snapshots if snapshot.merged_at),
        "open_pull_requests": sum(1 for snapshot in snapshots if snapshot.state == "open"),
        "changed_lines": sum(snapshot.changed_lines for snapshot in snapshots),
        "commits": sum(snapshot.commits for snapshot in snapshots),
        "review_submissions_received": sum(snapshot.review_count for snapshot in snapshots),
        "average_reviews_per_pr": _round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "median_changed_lines": _round(
            _median_or_none([snapshot.changed_lines for snapshot in snapshots])
        ),
        "share_of_prs_pct": _round(
            (len(snapshots) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    }


def _repository_row(
    repository_full_name: str,
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> dict[str, Any]:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours for snapshot in snapshots if snapshot.merge_hours is not None
    ]
    return {
        "repository_full_name": repository_full_name,
        "pull_requests": len(snapshots),
        "merged_pull_requests": sum(1 for snapshot in snapshots if snapshot.merged_at),
        "open_pull_requests": sum(1 for snapshot in snapshots if snapshot.state == "open"),
        "authors": len({snapshot.author_login for snapshot in snapshots}),
        "changed_lines": sum(snapshot.changed_lines for snapshot in snapshots),
        "review_submissions": sum(snapshot.review_count for snapshot in snapshots),
        "average_reviews_per_pr": _round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "share_of_prs_pct": _round(
            (len(snapshots) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    }


def _snapshot_row(snapshot: PullRequestSnapshot) -> dict[str, Any]:
    return {
        "repository_full_name": snapshot.repository_full_name,
        "pull_request_number": snapshot.number,
        "title": snapshot.title,
        "author_login": snapshot.author_login,
        "state": snapshot.state,
        "created_at": snapshot.created_at.isoformat(),
        "updated_at": snapshot.updated_at.isoformat(),
        "closed_at": None if snapshot.closed_at is None else snapshot.closed_at.isoformat(),
        "merged_at": None if snapshot.merged_at is None else snapshot.merged_at.isoformat(),
        "html_url": snapshot.html_url,
        "additions": snapshot.additions,
        "deletions": snapshot.deletions,
        "changed_files": snapshot.changed_files,
        "changed_lines": snapshot.changed_lines,
        "commits": snapshot.commits,
        "review_count": snapshot.review_count,
        "approval_count": snapshot.approval_count,
        "changes_requested_count": snapshot.changes_requested_count,
        "comment_review_count": snapshot.comment_review_count,
        "reviewer_count": snapshot.reviewer_count,
        "first_review_hours": snapshot.first_review_hours,
        "merge_hours": snapshot.merge_hours,
        "close_hours": snapshot.close_hours,
        "review_rounds": snapshot.review_rounds,
        "review_ready_at": snapshot.review_ready_at.isoformat(),
        "review_requested_at": (
            None
            if snapshot.review_requested_at is None
            else snapshot.review_requested_at.isoformat()
        ),
        "size_bucket": snapshot.size_bucket,
    }


def _parse_datetime(value: str) -> datetime:
    parsed = _parse_optional_datetime(value)
    if parsed is None:
        raise ValueError(f"expected an ISO datetime, received {value!r}")
    return parsed


def _hours_between(start_at: datetime | None, end_at: datetime | None) -> float | None:
    if start_at is None or end_at is None:
        return None
    return round((end_at - start_at).total_seconds() / 3600, 2)


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
    main()
