from __future__ import annotations

# ruff: noqa: F401
import csv
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from orgpulse import dashboard as _dashboard_module
from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    PULL_REQUEST_REVIEW_FIELDNAMES,
    PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
    NormalizedRawSnapshotWriter,
)
from orgpulse.metrics import (
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.models import (
    CollectionWindow,
    LastSuccessfulRun,
    ManifestWatermarks,
    MetricValueSummary,
    OrganizationMetricCollection,
    OrganizationMetricPeriod,
    OrganizationMetricRollup,
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunConfig,
    RunManifest,
    RunMode,
    RunScope,
)
from orgpulse.reporting.analysis_report import (
    build_organization_report_payload,
    render_organization_report_html,
)
from orgpulse.reporting.dashboard_html import (
    prepare_dashboard_payload,
    render_dashboard_artifact,
    render_dashboard_html,
)
from orgpulse.reporting.run_outputs import (
    MANIFEST_FILENAME,
    ORG_SUMMARY_DIRNAME,
    REQUIRED_RAW_SNAPSHOT_HEADERS,
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)

from .dashboard_source import (
    dashboard_pull_request_row as _manual_dashboard_pull_request_row,
)
from .dashboard_source import (
    dashboard_review_row as _manual_dashboard_review_row,
)
from .dashboard_source import (
    dashboard_timeline_event_row as _manual_dashboard_timeline_event_row,
)
from .dashboard_source import (
    expected_period_state as _expected_period_state,
)
from .dashboard_source import (
    expected_time_anchor_context as _expected_time_anchor_context,
)
from .dashboard_source import (
    write_dashboard_source_manifest as _shared_write_dashboard_source_manifest,
)
from .dashboard_source import (
    write_dashboard_source_period as _write_manual_dashboard_source_period,
)

build_dashboard_payload_from_local_outputs = _dashboard_module.build_dashboard_payload_from_local_outputs

__all__ = [
    "csv",
    "json",
    "subprocess",
    "sys",
    "date",
    "datetime",
    "timedelta",
    "Path",
    "Any",
    "pytest",
    "_dashboard_module",
    "PULL_REQUEST_FIELDNAMES",
    "PULL_REQUEST_REVIEW_FIELDNAMES",
    "PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES",
    "NormalizedRawSnapshotWriter",
    "PullRequestMetricCollectionBuilder",
    "RepositoryMetricCollectionBuilder",
    "CollectionWindow",
    "LastSuccessfulRun",
    "ManifestWatermarks",
    "MetricValueSummary",
    "OrganizationMetricCollection",
    "OrganizationMetricPeriod",
    "OrganizationMetricRollup",
    "PullRequestCollection",
    "PullRequestRecord",
    "PullRequestReviewRecord",
    "RawSnapshotPeriod",
    "RawSnapshotWriteResult",
    "ReportingPeriod",
    "RunConfig",
    "RunManifest",
    "RunMode",
    "RunScope",
    "build_organization_report_payload",
    "render_organization_report_html",
    "prepare_dashboard_payload",
    "render_dashboard_artifact",
    "render_dashboard_html",
    "MANIFEST_FILENAME",
    "ORG_SUMMARY_DIRNAME",
    "REQUIRED_RAW_SNAPSHOT_HEADERS",
    "OrgSummaryWriter",
    "RepositorySummaryCsvWriter",
    "RunManifestWriter",
    "build_dashboard_payload_from_local_outputs",
    "_manual_dashboard_pull_request_row",
    "_manual_dashboard_review_row",
    "_manual_dashboard_timeline_event_row",
    "_expected_period_state",
    "_expected_time_anchor_context",
    "_write_manual_dashboard_source_manifest",
    "_write_manual_dashboard_source_period",
    "_manual_pull_request",
]


def _write_manual_dashboard_source_manifest(
    *,
    source_output_dir: Path,
    refreshed_period_keys: tuple[str, ...],
    locked_period_keys: tuple[str, ...],
    as_of: str,
) -> None:
    _shared_write_dashboard_source_manifest(
        source_output_dir=source_output_dir,
        refreshed_period_keys=refreshed_period_keys,
        locked_period_keys=locked_period_keys,
        as_of=as_of,
        collection_window_start_date="2026-04-01",
        repository_count=2,
        pull_request_count=2,
        latest_refreshed_period_end_date="2026-04-30",
        latest_locked_period_end_date="2026-03-31" if locked_period_keys else None,
        count_snapshot_rows=True,
    )


def _manual_pull_request(
    *,
    repository_full_name: str,
    pull_request_number: int,
    author_login: str,
    created_at: str,
    merged_at: str | None,
    changed_lines: int,
    additions: int,
    deletions: int,
    first_review_hours: float | None,
    merge_hours: float | None,
    size_bucket: str,
) -> dict[str, object]:
    """Build a normalized manual dashboard pull request payload for tests."""
    closed_at = merged_at
    return {
        "repository_full_name": repository_full_name,
        "pull_request_number": pull_request_number,
        "title": f"PR {pull_request_number}",
        "author_login": author_login,
        "state": "closed" if merged_at else "open",
        "created_at": created_at,
        "updated_at": merged_at or created_at,
        "closed_at": closed_at,
        "merged_at": merged_at,
        "html_url": f"https://example.test/pr/{pull_request_number}",
        "additions": additions,
        "deletions": deletions,
        "changed_files": 1,
        "changed_lines": changed_lines,
        "commits": 1,
        "review_count": 1,
        "approval_count": 1,
        "changes_requested_count": 0,
        "comment_review_count": 0,
        "reviewer_count": 1,
        "first_review_hours": first_review_hours,
        "merge_hours": merge_hours,
        "close_hours": merge_hours,
        "review_rounds": 1,
        "review_ready_at": created_at,
        "review_requested_at": created_at,
        "size_bucket": size_bucket,
    }
