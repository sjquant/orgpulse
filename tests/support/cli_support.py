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

from orgpulse import dashboard as dashboard_module
from orgpulse.cli import app, build_run_config
from orgpulse.errors import AuthResolutionError, GitHubApiError
from orgpulse.github_auth import GitHubAuthService
from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    NormalizedRawSnapshotWriter,
)
from orgpulse.metrics import (
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.models import (
    AuthSource,
    CollectionWindow,
    GitHubTargetContext,
    LastSuccessfulRun,
    ManifestWatermarks,
    ManifestWriteResult,
    OrgSummaryPeriodWriteResult,
    OrgSummaryWriteResult,
    PeriodGrain,
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RawSnapshotWriteResult,
    RepositoryCollectionFailure,
    RepositoryInventory,
    RepositorySummaryCsvWriteResult,
    ResolvedToken,
    RunManifest,
    RunMode,
    RunScope,
    TimeAnchor,
)
from orgpulse.reporting.run_outputs import (
    REPOSITORY_SUMMARY_CSV_FIELDNAMES,
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)

from .dashboard_source import (
    dashboard_pull_request_row as _dashboard_pull_request_row,
)
from .dashboard_source import (
    dashboard_review_row as _dashboard_review_row,
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
    write_dashboard_source_period as _write_dashboard_source_period,
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
    "RepositoryCollectionFailure",
    "PeriodGrain",
    "RunMode",
    "RunScope",
    "TimeAnchor",
    "AuthSource",
    "GitHubTargetContext",
    "ResolvedToken",
    "LastSuccessfulRun",
    "ManifestWatermarks",
    "ManifestWriteResult",
    "OrgSummaryPeriodWriteResult",
    "OrgSummaryWriteResult",
    "RawSnapshotWriteResult",
    "RepositorySummaryCsvWriteResult",
    "RunManifest",
    "REPOSITORY_SUMMARY_CSV_FIELDNAMES",
    "OrgSummaryWriter",
    "PULL_REQUEST_FIELDNAMES",
    "RepositorySummaryCsvWriter",
    "RunManifestWriter",
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
        "orgpulse.cli.resolve_auth_token",
        lambda config: ResolvedToken(source=AuthSource.GH_TOKEN, token="env-token"),
    )
    monkeypatch.setattr("orgpulse.cli.Github", lambda auth: object())
    monkeypatch.setattr("orgpulse.cli.Auth.Token", lambda token: object())
    monkeypatch.setattr(
        "orgpulse.cli.GitHubAuthService",
        lambda github_client, auth_source: github_auth_service,
    )
    monkeypatch.setattr(
        "orgpulse.cli.GitHubIngestionService",
        lambda github_client: FakeCliIngestionService(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.NormalizedRawSnapshotWriter",
        lambda: FakeCliSnapshotWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.RunManifestWriter",
        lambda: FakeCliManifestWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.OrgSummaryWriter",
        lambda: FakeCliOrgSummaryWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.RepositorySummaryCsvWriter",
        lambda: FakeCliRepositorySummaryWriter(),
    )


class FakeCliIngestionService:
    def __init__(
        self,
        *,
        inventory: RepositoryInventory | None = None,
        collection: PullRequestCollection | None = None,
    ) -> None:
        self._inventory = inventory
        self._collection = collection

    def load_repository_inventory(self, config) -> RepositoryInventory:
        return (
            self._inventory
            if self._inventory is not None
            else RepositoryInventory(
                organization_login=config.org,
                repositories=(),
            )
        )

    def fetch_pull_requests(
        self,
        config,
        inventory: RepositoryInventory,
    ) -> PullRequestCollection:
        return (
            self._collection
            if self._collection is not None
            else PullRequestCollection(
                window=config.collection_window,
                pull_requests=(),
                failures=(),
            )
        )

    def clear_checkpoint(self, config) -> None:
        """Mirror the production ingestion interface without touching fixture state."""


class FakeCliSnapshotWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
    ) -> RawSnapshotWriteResult:
        return RawSnapshotWriteResult(
            root_dir=config.output_dir / "raw" / config.period.value,
            periods=(),
        )


class FakeCliManifestWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        *,
        repository_count: int,
    ) -> ManifestWriteResult:
        return ManifestWriteResult(
            path=config.output_dir / "manifest" / config.period.value / "manifest.json",
            index_path=config.output_dir / "manifest" / config.period.value / "index.json",
            readme_path=config.output_dir / "manifest" / config.period.value / "README.md",
            manifest=RunManifest(
                target_org=config.org,
                period_grain=config.period,
                time_anchor=config.time_anchor,
                include_repos=config.include_repos,
                exclude_repos=config.exclude_repos,
                raw_snapshot_root_dir=raw_snapshot.root_dir,
                refreshed_periods=raw_snapshot.periods,
                locked_periods=(),
                watermarks=ManifestWatermarks(
                    collection_window_start_date=collection.window.start_date,
                    collection_window_end_date=collection.window.end_date,
                    latest_refreshed_period_end_date=None,
                    latest_locked_period_end_date=None,
                ),
                last_successful_run=LastSuccessfulRun(
                    completed_at=datetime.fromisoformat("2026-04-18T00:00:00+00:00"),
                    as_of=config.as_of,
                    mode=config.mode,
                    refresh_scope=config.refresh_scope,
                    repository_count=repository_count,
                    pull_request_count=len(collection.pull_requests),
                ),
            ),
        )


class FakeCliOrgSummaryWriter:
    def write(
        self,
        config,
        org_metrics,
        *,
        refreshed_period_keys,
    ) -> OrgSummaryWriteResult:
        return OrgSummaryWriteResult(
            target_org=org_metrics.target_org,
            root_dir=config.output_dir / "org_summary" / config.period.value,
            contract_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "contract.json",
            index_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "index.json",
            readme_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "README.md",
            latest_directory=config.output_dir
            / "org_summary"
            / config.period.value
            / "latest",
            latest_markdown_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "latest"
            / "summary.md",
            latest_json_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "latest"
            / "summary.json",
            periods=tuple(
                OrgSummaryPeriodWriteResult(
                    key=period.key,
                    start_date=period.start_date,
                    end_date=period.end_date,
                    closed=period.closed,
                    directory=config.output_dir
                    / "org_summary"
                    / config.period.value
                    / period.key,
                    markdown_path=config.output_dir
                    / "org_summary"
                    / config.period.value
                    / period.key
                    / "summary.md",
                    json_path=config.output_dir
                    / "org_summary"
                    / config.period.value
                    / period.key
                    / "summary.json",
                )
                for period in org_metrics.periods
            ),
        )


class FakeCliRepositorySummaryWriter:
    def write(
        self,
        config,
        repository_metrics,
        *,
        refreshed_period_keys,
    ) -> RepositorySummaryCsvWriteResult:
        return RepositorySummaryCsvWriteResult(
            root_dir=config.output_dir / "repo_summary" / config.period.value,
            contract_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "contract.json",
            index_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "index.json",
            readme_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "README.md",
            latest_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "latest"
            / "repo_summary.csv",
            periods=(),
        )


class UnexpectedSnapshotWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
    ) -> RawSnapshotWriteResult:
        raise AssertionError("snapshot writer should not run")


class UnexpectedManifestWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        *,
        repository_count: int,
    ) -> None:
        raise AssertionError("manifest writer should not run")


class UnexpectedOrgSummaryWriter:
    def write(
        self,
        config,
        org_metrics,
        *,
        refreshed_period_keys,
    ) -> None:
        raise AssertionError("org summary writer should not run")


class UnexpectedRepositorySummaryWriter:
    def write(
        self,
        config,
        repository_metrics,
        *,
        refreshed_period_keys,
    ) -> None:
        raise AssertionError("repo summary writer should not run")


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
        "orgpulse.cli.GitHubIngestionService",
        lambda github_client: FakeCliIngestionService(
            inventory=inventory,
            collection=collection,
        ),
    )
    monkeypatch.setattr(
        "orgpulse.cli.NormalizedRawSnapshotWriter",
        lambda: NormalizedRawSnapshotWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.RunManifestWriter",
        lambda: RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        ),
    )
    monkeypatch.setattr(
        "orgpulse.cli.OrgSummaryWriter",
        lambda: OrgSummaryWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.RepositorySummaryCsvWriter",
        lambda: RepositorySummaryCsvWriter(),
    )
