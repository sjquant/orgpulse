from __future__ import annotations

# ruff: noqa: F401
import csv
import json
import re
import shutil
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from orgpulse.apps.dashboard import service as dashboard_module
from orgpulse.cli import app
from orgpulse.common.config import build_run_config
from orgpulse.common.errors import AuthResolutionError, GitHubApiError
from orgpulse.common.models import (
    AuthSource,
    CollectionWindow,
    GitHubTargetContext,
    PeriodGrain,
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RepositoryCollectionFailure,
    RepositoryInventory,
    RepositoryInventoryItem,
    ResolvedToken,
    RunMode,
    RunScope,
    TimeAnchor,
)
from orgpulse.libs.github.auth import GitHubAuthService
from orgpulse.libs.github.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    NormalizedRawSnapshotWriter,
)
from orgpulse.libs.metrics.service import (
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.libs.output_store.run_outputs import (
    REPOSITORY_SUMMARY_CSV_FIELDNAMES,
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)
from orgpulse.libs.reporting.dashboard_html import prepare_dashboard_payload

from .dashboard_source import dashboard_pull_request_row as _dashboard_pull_request_row
from .dashboard_source import dashboard_review_row as _dashboard_review_row
from .dashboard_source import expected_period_state as _expected_period_state
from .dashboard_source import (
    expected_time_anchor_context as _expected_time_anchor_context,
)
from .dashboard_source import (
    write_dashboard_source_manifest as _shared_write_dashboard_source_manifest,
)
from .dashboard_source import (
    write_dashboard_source_period as _write_dashboard_source_period,
)
from .mocks import (
    FakeCliIngestionService,
    FakeCliManifestWriter,
    FakeCliOrgSummaryWriter,
    FakeCliRepositorySummaryWriter,
    FakeCliSnapshotWriter,
    UnexpectedManifestWriter,
    UnexpectedOrgSummaryWriter,
    UnexpectedRepositorySummaryWriter,
    UnexpectedSnapshotWriter,
)

__all__ = [
    "app",
    "build_run_config",
    "pytest",
    "CliRunner",
    "ValidationError",
    "create_autospec",
    "dashboard_module",
    "AuthResolutionError",
    "GitHubApiError",
    "GitHubAuthService",
    "NormalizedRawSnapshotWriter",
    "OrganizationMetricCollectionBuilder",
    "PullRequestMetricCollectionBuilder",
    "RepositoryMetricCollectionBuilder",
    "CollectionWindow",
    "PullRequestCollection",
    "PullRequestRecord",
    "PullRequestReviewRecord",
    "PullRequestTimelineEventRecord",
    "RepositoryInventory",
    "RepositoryInventoryItem",
    "RepositoryCollectionFailure",
    "PeriodGrain",
    "RunMode",
    "RunScope",
    "TimeAnchor",
    "AuthSource",
    "GitHubTargetContext",
    "ResolvedToken",
    "REPOSITORY_SUMMARY_CSV_FIELDNAMES",
    "OrgSummaryWriter",
    "PULL_REQUEST_FIELDNAMES",
    "RepositorySummaryCsvWriter",
    "RunManifestWriter",
    "prepare_dashboard_payload",
    "csv",
    "json",
    "re",
    "shutil",
    "StringIO",
    "Path",
    "datetime",
    "runner",
    "github_auth_service",
    "FakeCliIngestionService",
    "FakeCliSnapshotWriter",
    "FakeCliManifestWriter",
    "FakeCliOrgSummaryWriter",
    "FakeCliRepositorySummaryWriter",
    "UnexpectedSnapshotWriter",
    "UnexpectedManifestWriter",
    "UnexpectedOrgSummaryWriter",
    "UnexpectedRepositorySummaryWriter",
    "_write_dashboard_source_manifest",
    "_dashboard_pull_request_row",
    "_dashboard_review_row",
    "_expected_period_state",
    "_expected_time_anchor_context",
    "_write_dashboard_source_period",
    "_configure_production_cli_runtime",
]


@pytest.fixture(scope="module")
def runner() -> CliRunner:
    """Provide a reusable Typer CLI runner for pytest-based tests."""
    return CliRunner()


def _write_dashboard_source_manifest(
    *,
    source_output_dir: Path,
    refreshed_period_keys: tuple[str, ...],
    locked_period_keys: tuple[str, ...],
    as_of: str,
    period_grain: PeriodGrain = PeriodGrain.MONTH,
) -> None:
    _shared_write_dashboard_source_manifest(
        source_output_dir=source_output_dir,
        refreshed_period_keys=refreshed_period_keys,
        locked_period_keys=locked_period_keys,
        as_of=as_of,
        period_grain=period_grain,
        collection_window_start_date="2026-03-01",
        completed_at="2026-04-18T00:00:00+00:00",
    )


@pytest.fixture
def github_auth_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the GitHub auth boundary for CLI tests that focus on config behavior."""
    github_auth_service = create_autospec(
        GitHubAuthService, instance=True, spec_set=True
    )
    github_auth_service.validate_access.side_effect = lambda config: (
        GitHubTargetContext(
            auth_source=AuthSource.GH_TOKEN,
            viewer_login="test-user",
            organization_login=config.org,
        )
    )

    monkeypatch.setattr(
        "orgpulse.libs.snapshots.refresh.resolve_auth_token",
        lambda config: ResolvedToken(source=AuthSource.GH_TOKEN, token="env-token"),
    )
    monkeypatch.setattr("orgpulse.libs.snapshots.refresh.Github", lambda auth: object())
    monkeypatch.setattr("orgpulse.libs.snapshots.refresh.Auth.Token", lambda token: object())
    monkeypatch.setattr(
        "orgpulse.libs.snapshots.refresh.GitHubAuthService",
        lambda github_client, auth_source: github_auth_service,
    )
    monkeypatch.setattr(
        "orgpulse.libs.snapshots.refresh.GitHubIngestionService",
        lambda github_client: FakeCliIngestionService(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.NormalizedRawSnapshotWriter",
        lambda: FakeCliSnapshotWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.RunManifestWriter",
        lambda: FakeCliManifestWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.OrgSummaryWriter",
        lambda: FakeCliOrgSummaryWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.RepositorySummaryCsvWriter",
        lambda: FakeCliRepositorySummaryWriter(),
    )


def _configure_production_cli_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    collection: PullRequestCollection,
) -> None:
    inventory = RepositoryInventory(
        organization_login="acme",
        repositories=(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.snapshots.refresh.GitHubIngestionService",
        lambda github_client: FakeCliIngestionService(
            inventory=inventory,
            collection=collection,
        ),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.NormalizedRawSnapshotWriter",
        lambda: NormalizedRawSnapshotWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.RunManifestWriter",
        lambda: RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        ),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.OrgSummaryWriter",
        lambda: OrgSummaryWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.libs.output_store.pipeline.RepositorySummaryCsvWriter",
        lambda: RepositorySummaryCsvWriter(),
    )
