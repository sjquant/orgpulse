from __future__ import annotations

import csv
import json
from datetime import date, datetime, timedelta
from pathlib import Path

from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    PULL_REQUEST_REVIEW_FIELDNAMES,
    PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
)
from orgpulse.models import (
    LastSuccessfulRun,
    ManifestWatermarks,
    PeriodGrain,
    RawSnapshotPeriod,
    ReportingPeriod,
    RunManifest,
    RunMode,
    RunScope,
    TimeAnchor,
)


def expected_time_anchor_context(time_anchor: str = "created_at") -> dict[str, str]:
    return {
        "field": time_anchor,
        "scope": f"pull_request.{time_anchor}",
        "description": (
            "All counts and summaries in this file are grouped by "
            f"pull_request.{time_anchor}."
        ),
    }


def expected_period_state(
    *,
    grain: str = "month",
    closed: bool,
    observed_through_date: str,
) -> dict[str, object]:
    status = "closed" if closed else "open"
    return {
        "status": status,
        "label": f"{status} {grain}",
        "is_open": not closed,
        "is_closed": closed,
        "is_partial": not closed,
        "observed_through_date": observed_through_date,
        "open_week": not closed and grain == "week",
        "open_month": not closed and grain == "month",
    }


def write_dashboard_source_period(
    *,
    period_dir: Path,
    pull_request_rows: list[dict[str, str]],
    review_rows: list[dict[str, str]],
    timeline_rows: list[dict[str, str]],
) -> None:
    period_dir.mkdir(parents=True, exist_ok=True)
    write_dashboard_csv(
        path=period_dir / "pull_requests.csv",
        fieldnames=PULL_REQUEST_FIELDNAMES,
        rows=pull_request_rows,
    )
    write_dashboard_csv(
        path=period_dir / "pull_request_reviews.csv",
        fieldnames=PULL_REQUEST_REVIEW_FIELDNAMES,
        rows=review_rows,
    )
    write_dashboard_csv(
        path=period_dir / "pull_request_timeline_events.csv",
        fieldnames=PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
        rows=timeline_rows,
    )


def write_dashboard_csv(
    *,
    path: Path,
    fieldnames: tuple[str, ...],
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_dashboard_source_manifest(
    *,
    source_output_dir: Path,
    refreshed_period_keys: tuple[str, ...],
    locked_period_keys: tuple[str, ...],
    as_of: str,
    period_grain: PeriodGrain = PeriodGrain.MONTH,
    target_org: str = "acme",
    collection_window_start_date: str = "2026-03-01",
    completed_at: str | None = None,
    repository_count: int = 1,
    pull_request_count: int = 1,
    latest_refreshed_period_end_date: str | None = None,
    latest_locked_period_end_date: str | None = None,
    count_snapshot_rows: bool = False,
) -> None:
    raw_root_dir = source_output_dir / "raw" / period_grain.value / "created_at"
    manifest = RunManifest(
        target_org=target_org,
        period_grain=period_grain,
        time_anchor=TimeAnchor.CREATED_AT,
        include_repos=(),
        exclude_repos=(),
        raw_snapshot_root_dir=raw_root_dir,
        refreshed_periods=tuple(
            dashboard_raw_snapshot_period(
                raw_root_dir=raw_root_dir,
                key=key,
                closed=False,
                period_grain=period_grain,
                count_rows=count_snapshot_rows,
            )
            for key in refreshed_period_keys
        ),
        locked_periods=tuple(
            dashboard_reporting_period(
                key=key,
                closed=True,
                period_grain=period_grain,
            )
            for key in locked_period_keys
        ),
        watermarks=ManifestWatermarks(
            collection_window_start_date=date.fromisoformat(collection_window_start_date),
            collection_window_end_date=date.fromisoformat(as_of),
            latest_refreshed_period_end_date=date.fromisoformat(
                latest_refreshed_period_end_date or as_of
            ),
            latest_locked_period_end_date=(
                None
                if latest_locked_period_end_date is None
                else date.fromisoformat(latest_locked_period_end_date)
            ),
        ),
        last_successful_run=LastSuccessfulRun(
            completed_at=datetime.fromisoformat(
                completed_at or f"{as_of}T12:00:00+00:00"
            ),
            as_of=date.fromisoformat(as_of),
            mode=RunMode.INCREMENTAL,
            refresh_scope=RunScope.OPEN_PERIOD,
            repository_count=repository_count,
            pull_request_count=pull_request_count,
        ),
    )
    manifest_dir = source_output_dir / "manifest" / period_grain.value / "created_at"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "manifest.json").write_text(
        json.dumps(manifest.model_dump(mode="json")),
        encoding="utf-8",
    )


def dashboard_reporting_period(
    *,
    key: str,
    closed: bool,
    period_grain: PeriodGrain = PeriodGrain.MONTH,
) -> ReportingPeriod:
    start_date, end_date = dashboard_period_dates(
        key=key,
        period_grain=period_grain,
    )
    return ReportingPeriod(
        grain=period_grain,
        key=key,
        start_date=start_date,
        end_date=end_date,
        closed=closed,
    )


def dashboard_raw_snapshot_period(
    *,
    raw_root_dir: Path,
    key: str,
    closed: bool,
    period_grain: PeriodGrain = PeriodGrain.MONTH,
    count_rows: bool = False,
) -> RawSnapshotPeriod:
    reporting_period = dashboard_reporting_period(
        key=key,
        closed=closed,
        period_grain=period_grain,
    )
    period_dir = raw_root_dir / key
    return RawSnapshotPeriod(
        key=reporting_period.key,
        start_date=reporting_period.start_date,
        end_date=reporting_period.end_date,
        closed=reporting_period.closed,
        directory=period_dir,
        pull_requests_path=period_dir / "pull_requests.csv",
        pull_request_count=(
            dashboard_csv_row_count(period_dir / "pull_requests.csv")
            if count_rows
            else 0
        ),
        reviews_path=period_dir / "pull_request_reviews.csv",
        review_count=(
            dashboard_csv_row_count(period_dir / "pull_request_reviews.csv")
            if count_rows
            else 0
        ),
        timeline_events_path=period_dir / "pull_request_timeline_events.csv",
        timeline_event_count=(
            dashboard_csv_row_count(period_dir / "pull_request_timeline_events.csv")
            if count_rows
            else 0
        ),
    )


def dashboard_period_dates(
    *,
    key: str,
    period_grain: PeriodGrain = PeriodGrain.MONTH,
) -> tuple[date, date]:
    if period_grain is PeriodGrain.MONTH:
        start_date = date.fromisoformat(f"{key}-01")
        next_period_start = (start_date.replace(day=28) + timedelta(days=4)).replace(
            day=1
        )
        return start_date, next_period_start - timedelta(days=1)
    iso_year, iso_week = key.split("-W", maxsplit=1)
    start_date = datetime.fromisocalendar(int(iso_year), int(iso_week), 1).date()
    return start_date, start_date + timedelta(days=6)


def dashboard_csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def dashboard_pull_request_row(
    *,
    period_key: str,
    repository_full_name: str,
    pull_request_number: int,
    author_login: str,
    created_at: str,
    updated_at: str,
    closed_at: str | None,
    merged_at: str | None,
    additions: int,
    deletions: int,
    changed_files: int,
    commits: int,
) -> dict[str, str]:
    return {
        "period_key": period_key,
        "repository_full_name": repository_full_name,
        "pull_request_number": str(pull_request_number),
        "title": f"PR {pull_request_number}",
        "state": "closed" if closed_at is not None else "open",
        "draft": "false",
        "merged": "true" if merged_at is not None else "false",
        "author_login": author_login,
        "created_at": created_at,
        "updated_at": updated_at,
        "closed_at": closed_at or "",
        "merged_at": merged_at or "",
        "additions": str(additions),
        "deletions": str(deletions),
        "changed_files": str(changed_files),
        "commits": str(commits),
        "html_url": f"https://example.test/pr/{pull_request_number}",
    }


def dashboard_review_row(
    *,
    period_key: str,
    repository_full_name: str,
    pull_request_number: int,
    review_id: int,
    author_login: str,
    submitted_at: str,
) -> dict[str, str]:
    return {
        "period_key": period_key,
        "repository_full_name": repository_full_name,
        "pull_request_number": str(pull_request_number),
        "review_id": str(review_id),
        "state": "APPROVED",
        "author_login": author_login,
        "submitted_at": submitted_at,
        "commit_id": "",
    }


def dashboard_timeline_event_row(
    *,
    period_key: str,
    repository_full_name: str,
    pull_request_number: int,
    event_id: int,
    event: str,
    created_at: str,
    requested_reviewer_login: str,
) -> dict[str, str]:
    return {
        "period_key": period_key,
        "repository_full_name": repository_full_name,
        "pull_request_number": str(pull_request_number),
        "event_id": str(event_id),
        "event": event,
        "actor_login": "maintainer",
        "created_at": created_at,
        "requested_reviewer_login": requested_reviewer_login,
        "requested_team_name": "",
    }
