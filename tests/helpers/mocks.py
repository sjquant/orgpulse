from __future__ import annotations

from datetime import datetime

from orgpulse.common.models import (
    LastSuccessfulRun,
    ManifestWatermarks,
    ManifestWriteResult,
    OrgSummaryPeriodWriteResult,
    OrgSummaryWriteResult,
    PullRequestCollection,
    RawSnapshotWriteResult,
    RepositoryInventory,
    RepositorySummaryCsvWriteResult,
    RunManifest,
)
from orgpulse.libs.github.ingestion import PullRequestFetchProgress


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
        *,
        progress_callback=None,
    ) -> PullRequestCollection:
        if progress_callback is not None:
            total_repositories = len(inventory.repositories)
            progress_callback(
                PullRequestFetchProgress(
                    phase="start",
                    repository_full_name=None,
                    repository_index=None,
                    total_repositories=total_repositories,
                    completed_repositories=0,
                    progress_percent=0.0 if total_repositories else 100.0,
                    cached_pull_request_count=0,
                    fetched_pull_request_count=0,
                    repository_pull_request_count=0,
                    failure_count=0,
                )
            )
            for index, repository in enumerate(inventory.repositories, start=1):
                repository_pull_request_count = sum(
                    1
                    for pull_request in (
                        () if self._collection is None else self._collection.pull_requests
                    )
                    if pull_request.repository_full_name == repository.full_name
                )
                progress_callback(
                    PullRequestFetchProgress(
                        phase="repository_completed",
                        repository_full_name=repository.full_name,
                        repository_index=index,
                        total_repositories=total_repositories,
                        completed_repositories=index,
                        progress_percent=round((index / total_repositories) * 100, 1),
                        cached_pull_request_count=0,
                        fetched_pull_request_count=repository_pull_request_count,
                        repository_pull_request_count=repository_pull_request_count,
                        failure_count=0,
                    )
                )
            progress_callback(
                PullRequestFetchProgress(
                    phase="finish",
                    repository_full_name=None,
                    repository_index=None,
                    total_repositories=total_repositories,
                    completed_repositories=total_repositories,
                    progress_percent=100.0,
                    cached_pull_request_count=0,
                    fetched_pull_request_count=0,
                    repository_pull_request_count=0,
                    failure_count=0,
                )
            )
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
