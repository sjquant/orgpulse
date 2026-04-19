from __future__ import annotations

import csv
import json
import shutil
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    PULL_REQUEST_REVIEW_FIELDNAMES,
    PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
)
from orgpulse.models import (
    LastSuccessfulRun,
    ManifestWatermarks,
    ManifestWriteResult,
    MetricValueSummary,
    OrganizationMetricCollection,
    OrganizationMetricPeriod,
    OrgSummaryPeriodWriteResult,
    OrgSummaryWriteResult,
    PullRequestCollection,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RepositoryMetricCollection,
    RepositoryMetricPeriod,
    RepositoryMetricRollup,
    RepositorySummaryCsvPeriod,
    RepositorySummaryCsvWriteResult,
    RunConfig,
    RunManifest,
    RunMode,
    canonicalize_repo_filter,
)

MANIFEST_FILENAME = "manifest.json"
MANIFEST_DIRNAME = "manifest"

ORG_SUMMARY_DIRNAME = "org_summary"
ORG_SUMMARY_CONTRACT_FILENAME = "contract.json"
ORG_SUMMARY_JSON_FILENAME = "summary.json"
ORG_SUMMARY_MARKDOWN_FILENAME = "summary.md"

REPOSITORY_SUMMARY_CSV_DIRNAME = "repo_summary"
REPOSITORY_SUMMARY_CSV_FILENAME = "repo_summary.csv"
REPOSITORY_SUMMARY_CSV_FIELDNAMES = (
    "period_key",
    "period_start_date",
    "period_end_date",
    "period_closed",
    "repository_full_name",
    "pull_request_count",
    "merged_pull_request_count",
    "active_author_count",
    "merged_pull_requests_per_active_author",
    "time_to_merge_count",
    "time_to_merge_total_seconds",
    "time_to_merge_average_seconds",
    "time_to_merge_median_seconds",
    "time_to_first_review_count",
    "time_to_first_review_total_seconds",
    "time_to_first_review_average_seconds",
    "time_to_first_review_median_seconds",
    "additions_count",
    "additions_total",
    "additions_average",
    "additions_median",
    "deletions_count",
    "deletions_total",
    "deletions_average",
    "deletions_median",
    "changed_lines_count",
    "changed_lines_total",
    "changed_lines_average",
    "changed_lines_median",
    "changed_files_count",
    "changed_files_total",
    "changed_files_average",
    "changed_files_median",
    "commits_count",
    "commits_total",
    "commits_average",
    "commits_median",
)

REQUIRED_RAW_SNAPSHOT_HEADERS = {
    "pull_requests.csv": ",".join(PULL_REQUEST_FIELDNAMES),
    "pull_request_reviews.csv": ",".join(PULL_REQUEST_REVIEW_FIELDNAMES),
    "pull_request_timeline_events.csv": ",".join(
        PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES
    ),
}


class RepositorySummaryCsvWriter:
    """Persist repo-level metric rollups into deterministic period-scoped CSV files."""

    def write(
        self,
        config: RunConfig,
        repository_metrics: RepositoryMetricCollection,
        *,
        refreshed_period_keys: tuple[str, ...],
    ) -> RepositorySummaryCsvWriteResult:
        root_dir = self._root_dir(config.output_dir, config.period.value)
        refreshed_periods = tuple(
            period
            for period in repository_metrics.periods
            if period.key in set(refreshed_period_keys)
        )
        _prune_stale_period_directories(
            config=config,
            root_dir=root_dir,
            active_period_keys=tuple(period.key for period in refreshed_periods),
        )
        return RepositorySummaryCsvWriteResult(
            root_dir=root_dir,
            periods=tuple(
                self._write_period_summary(root_dir, metric_period)
                for metric_period in refreshed_periods
            ),
        )

    def _write_period_summary(
        self,
        root_dir: Path,
        metric_period: RepositoryMetricPeriod,
    ) -> RepositorySummaryCsvPeriod:
        path = root_dir / metric_period.key / REPOSITORY_SUMMARY_CSV_FILENAME
        _write_csv_file(
            path=path,
            fieldnames=REPOSITORY_SUMMARY_CSV_FIELDNAMES,
            rows=[
                self._csv_row(metric_period, repository)
                for repository in metric_period.repositories
            ],
        )
        return RepositorySummaryCsvPeriod(
            key=metric_period.key,
            start_date=metric_period.start_date,
            end_date=metric_period.end_date,
            closed=metric_period.closed,
            path=path,
            repository_count=len(metric_period.repositories),
        )

    def _csv_row(
        self,
        metric_period: RepositoryMetricPeriod,
        repository: RepositoryMetricRollup,
    ) -> dict[str, object]:
        return {
            "period_key": metric_period.key,
            "period_start_date": metric_period.start_date.isoformat(),
            "period_end_date": metric_period.end_date.isoformat(),
            "period_closed": str(metric_period.closed).lower(),
            "repository_full_name": repository.repository_full_name,
            "pull_request_count": repository.pull_request_count,
            "merged_pull_request_count": repository.merged_pull_request_count,
            "active_author_count": repository.active_author_count,
            "merged_pull_requests_per_active_author": repository.merged_pull_requests_per_active_author,
            "time_to_merge_count": repository.time_to_merge_seconds.count,
            "time_to_merge_total_seconds": repository.time_to_merge_seconds.total,
            "time_to_merge_average_seconds": repository.time_to_merge_seconds.average,
            "time_to_merge_median_seconds": repository.time_to_merge_seconds.median,
            "time_to_first_review_count": repository.time_to_first_review_seconds.count,
            "time_to_first_review_total_seconds": repository.time_to_first_review_seconds.total,
            "time_to_first_review_average_seconds": repository.time_to_first_review_seconds.average,
            "time_to_first_review_median_seconds": repository.time_to_first_review_seconds.median,
            "additions_count": repository.additions.count,
            "additions_total": repository.additions.total,
            "additions_average": repository.additions.average,
            "additions_median": repository.additions.median,
            "deletions_count": repository.deletions.count,
            "deletions_total": repository.deletions.total,
            "deletions_average": repository.deletions.average,
            "deletions_median": repository.deletions.median,
            "changed_lines_count": repository.changed_lines.count,
            "changed_lines_total": repository.changed_lines.total,
            "changed_lines_average": repository.changed_lines.average,
            "changed_lines_median": repository.changed_lines.median,
            "changed_files_count": repository.changed_files.count,
            "changed_files_total": repository.changed_files.total,
            "changed_files_average": repository.changed_files.average,
            "changed_files_median": repository.changed_files.median,
            "commits_count": repository.commits.count,
            "commits_total": repository.commits.total,
            "commits_average": repository.commits.average,
            "commits_median": repository.commits.median,
        }

    def _root_dir(self, output_dir: Path, period_grain: str) -> Path:
        return output_dir / REPOSITORY_SUMMARY_CSV_DIRNAME / period_grain


class OrgSummaryWriter:
    """Persist deterministic org-level summary files for each reporting period."""

    def write(
        self,
        config: RunConfig,
        org_metrics: OrganizationMetricCollection,
        *,
        refreshed_period_keys: tuple[str, ...],
    ) -> OrgSummaryWriteResult:
        root_dir = self._root_dir(config.output_dir, config.period.value)
        refreshed_periods = tuple(
            period
            for period in org_metrics.periods
            if period.key in set(refreshed_period_keys)
        )
        contract_path = root_dir / ORG_SUMMARY_CONTRACT_FILENAME
        contract = self._contract_payload(config)
        _prune_period_directories_for_contract_change(
            root_dir=root_dir,
            contract_path=contract_path,
            contract=contract,
        )
        _prune_stale_period_directories(
            config=config,
            root_dir=root_dir,
            active_period_keys=tuple(period.key for period in refreshed_periods),
        )
        _write_json_file(contract_path, contract)
        return OrgSummaryWriteResult(
            target_org=org_metrics.target_org,
            root_dir=root_dir,
            contract_path=contract_path,
            periods=tuple(
                self._write_period_summary(
                    config=config,
                    root_dir=root_dir,
                    period=period,
                    target_org=org_metrics.target_org,
                )
                for period in refreshed_periods
            ),
        )

    def _write_period_summary(
        self,
        *,
        config: RunConfig,
        root_dir: Path,
        period: OrganizationMetricPeriod,
        target_org: str,
    ) -> OrgSummaryPeriodWriteResult:
        period_dir = root_dir / period.key
        markdown_path = period_dir / ORG_SUMMARY_MARKDOWN_FILENAME
        json_path = period_dir / ORG_SUMMARY_JSON_FILENAME
        _write_json_file(
            json_path,
            self._json_payload(config=config, period=period, target_org=target_org),
        )
        _write_text_file(
            markdown_path,
            self._markdown_document(
                config=config,
                period=period,
                target_org=target_org,
            ),
        )
        return OrgSummaryPeriodWriteResult(
            key=period.key,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=period.closed,
            directory=period_dir,
            markdown_path=markdown_path,
            json_path=json_path,
        )

    def _json_payload(
        self,
        *,
        config: RunConfig,
        period: OrganizationMetricPeriod,
        target_org: str,
    ) -> dict[str, object]:
        include_repos = _canonical_repo_filters(config.include_repos, org=config.org)
        exclude_repos = _canonical_repo_filters(config.exclude_repos, org=config.org)
        return {
            "target_org": target_org,
            "period_grain": config.period.value,
            "include_repos": list(include_repos),
            "exclude_repos": list(exclude_repos),
            "period": {
                "key": period.key,
                "start_date": period.start_date.isoformat(),
                "end_date": period.end_date.isoformat(),
                "closed": period.closed,
            },
            "summary": period.summary.model_dump(mode="json"),
        }

    def _markdown_document(
        self,
        *,
        config: RunConfig,
        period: OrganizationMetricPeriod,
        target_org: str,
    ) -> str:
        include_repos = _canonical_repo_filters(config.include_repos, org=config.org)
        exclude_repos = _canonical_repo_filters(config.exclude_repos, org=config.org)
        summary = period.summary
        return "\n".join(
            (
                f"# Organization Summary: {target_org} {period.key}",
                "",
                f"- Target org: {target_org}",
                f"- Period grain: {config.period.value}",
                f"- Period key: {period.key}",
                f"- Include repos: {_repo_filters_text(include_repos, empty='all')}",
                f"- Exclude repos: {_repo_filters_text(exclude_repos, empty='none')}",
                f"- Period start: {period.start_date.isoformat()}",
                f"- Period end: {period.end_date.isoformat()}",
                f"- Closed: {_bool_text(period.closed)}",
                "",
                "## Totals",
                "",
                f"- Repository count: {summary.repository_count}",
                f"- Pull request count: {summary.pull_request_count}",
                f"- Merged pull request count: {summary.merged_pull_request_count}",
                f"- Active author count: {summary.active_author_count}",
                (
                    "- Merged pull requests per active author: "
                    f"{_float_text(summary.merged_pull_requests_per_active_author)}"
                ),
                "",
                "## Value Summaries",
                "",
                "| Metric | Count | Total | Average | Median |",
                "| --- | ---: | ---: | ---: | ---: |",
                _summary_row("Time to merge (seconds)", summary.time_to_merge_seconds),
                _summary_row(
                    "Time to first review (seconds)",
                    summary.time_to_first_review_seconds,
                ),
                _summary_row("Additions", summary.additions),
                _summary_row("Deletions", summary.deletions),
                _summary_row("Changed lines", summary.changed_lines),
                _summary_row("Changed files", summary.changed_files),
                _summary_row("Commits", summary.commits),
                "",
            )
        )

    def _contract_payload(
        self,
        config: RunConfig,
    ) -> dict[str, object]:
        return {
            "target_org": config.org,
            "period_grain": config.period.value,
            "include_repos": list(
                _canonical_repo_filters(config.include_repos, org=config.org)
            ),
            "exclude_repos": list(
                _canonical_repo_filters(config.exclude_repos, org=config.org)
            ),
        }

    def _root_dir(self, output_dir: Path, period_grain: str) -> Path:
        return output_dir / ORG_SUMMARY_DIRNAME / period_grain


class RunManifestWriter:
    """Persist run metadata that catalogs refreshed and locked raw snapshot periods."""

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._now = now or self._current_time

    def write(
        self,
        config: RunConfig,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        *,
        repository_count: int,
    ) -> ManifestWriteResult:
        manifest = self._build_manifest(
            config=config,
            collection=collection,
            raw_snapshot=raw_snapshot,
            repository_count=repository_count,
        )
        path = self._manifest_path(config.output_dir, config.period.value)
        self._write_manifest_file(path, manifest)
        return ManifestWriteResult(path=path, manifest=manifest)

    def _build_manifest(
        self,
        *,
        config: RunConfig,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        repository_count: int,
    ) -> RunManifest:
        locked_periods = self._build_locked_periods(
            config=config,
            raw_snapshot=raw_snapshot,
        )
        return RunManifest(
            target_org=config.org,
            period_grain=config.period,
            include_repos=config.include_repos,
            exclude_repos=config.exclude_repos,
            raw_snapshot_root_dir=raw_snapshot.root_dir,
            refreshed_periods=raw_snapshot.periods,
            locked_periods=locked_periods,
            watermarks=self._build_watermarks(
                collection=collection,
                raw_snapshot=raw_snapshot,
                locked_periods=locked_periods,
            ),
            last_successful_run=self._build_last_successful_run(
                config=config,
                collection=collection,
                repository_count=repository_count,
            ),
        )

    def _build_locked_periods(
        self,
        *,
        config: RunConfig,
        raw_snapshot: RawSnapshotWriteResult,
    ) -> tuple[ReportingPeriod, ...]:
        carried_locked_periods = self._load_carried_locked_periods(
            config=config,
            manifest_path=self._manifest_path(config.output_dir, config.period.value),
            raw_snapshot_root_dir=raw_snapshot.root_dir,
        )
        refreshed_closed_periods = tuple(
            ReportingPeriod(
                grain=config.period,
                start_date=period.start_date,
                end_date=period.end_date,
                key=period.key,
                closed=period.end_date < config.active_period.start_date,
            )
            for period in raw_snapshot.periods
            if period.end_date < config.active_period.start_date
        )
        locked_periods_by_key = {
            period.key: period
            for period in (*carried_locked_periods, *refreshed_closed_periods)
            if self._period_snapshot_is_complete(raw_snapshot.root_dir / period.key)
        }
        return tuple(
            locked_periods_by_key[key] for key in sorted(locked_periods_by_key.keys())
        )

    def _load_carried_locked_periods(
        self,
        *,
        config: RunConfig,
        manifest_path: Path,
        raw_snapshot_root_dir: Path,
    ) -> tuple[ReportingPeriod, ...]:
        manifest = self._load_existing_manifest(manifest_path)
        if manifest is None:
            return ()
        if not self._manifest_matches_run_contract(
            config=config,
            manifest=manifest,
            raw_snapshot_root_dir=raw_snapshot_root_dir,
        ):
            return ()
        prior_refreshed_closed_periods = tuple(
            ReportingPeriod(
                grain=config.period,
                start_date=period.start_date,
                end_date=period.end_date,
                key=period.key,
                closed=period.end_date < config.active_period.start_date,
            )
            for period in manifest.refreshed_periods
            if period.end_date < config.active_period.start_date
        )
        prior_closed_periods = tuple(
            period
            for period in (*manifest.locked_periods, *prior_refreshed_closed_periods)
            if period.end_date < config.active_period.start_date
        )
        return tuple(
            period
            for period in prior_closed_periods
            if self._period_snapshot_is_complete(raw_snapshot_root_dir / period.key)
        )

    def _load_existing_manifest(
        self,
        manifest_path: Path,
    ) -> RunManifest | None:
        if not manifest_path.exists():
            return None
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        try:
            return RunManifest.model_validate(payload)
        except Exception:
            return None

    def _manifest_matches_run_contract(
        self,
        *,
        config: RunConfig,
        manifest: RunManifest,
        raw_snapshot_root_dir: Path,
    ) -> bool:
        return (
            manifest.target_org.lower() == config.org.lower()
            and manifest.period_grain == config.period
            and _canonical_repo_filters(manifest.include_repos, org=config.org)
            == _canonical_repo_filters(config.include_repos, org=config.org)
            and _canonical_repo_filters(manifest.exclude_repos, org=config.org)
            == _canonical_repo_filters(config.exclude_repos, org=config.org)
            and manifest.raw_snapshot_root_dir == raw_snapshot_root_dir
        )

    def _build_watermarks(
        self,
        *,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        locked_periods: tuple[ReportingPeriod, ...],
    ) -> ManifestWatermarks:
        return ManifestWatermarks(
            collection_window_start_date=collection.window.start_date,
            collection_window_end_date=collection.window.end_date,
            latest_refreshed_period_end_date=self._latest_period_end_date(
                raw_snapshot.periods
            ),
            latest_locked_period_end_date=self._latest_period_end_date(locked_periods),
        )

    def _build_last_successful_run(
        self,
        *,
        config: RunConfig,
        collection: PullRequestCollection,
        repository_count: int,
    ) -> LastSuccessfulRun:
        return LastSuccessfulRun(
            completed_at=self._now(),
            as_of=config.as_of,
            mode=config.mode,
            refresh_scope=config.refresh_scope,
            repository_count=repository_count,
            pull_request_count=len(collection.pull_requests),
        )

    def _latest_period_end_date(
        self,
        periods: Sequence[RawSnapshotPeriod | ReportingPeriod],
    ) -> date | None:
        if not periods:
            return None
        return max(period.end_date for period in periods)

    def _manifest_path(self, output_dir: Path, period_grain: str) -> Path:
        return output_dir / MANIFEST_DIRNAME / period_grain / MANIFEST_FILENAME

    def _period_snapshot_is_complete(self, period_dir: Path) -> bool:
        return period_dir.is_dir() and all(
            self._csv_has_expected_header(period_dir / filename, expected_header)
            for filename, expected_header in REQUIRED_RAW_SNAPSHOT_HEADERS.items()
        )

    def _csv_has_expected_header(self, path: Path, expected_header: str) -> bool:
        if not path.is_file():
            return False
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                header = handle.readline().strip()
        except OSError:
            return False
        return header == expected_header

    def _write_manifest_file(self, path: Path, manifest: RunManifest) -> None:
        _write_json_file(path, manifest.model_dump(mode="json"))

    def _current_time(self) -> datetime:
        return datetime.now(UTC).replace(microsecond=0)


def _write_csv_file(
    *,
    path: Path,
    fieldnames: tuple[str, ...],
    rows: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json_file(
    path: Path,
    payload: dict[str, object],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _write_text_file(
    path: Path,
    document: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(document, encoding="utf-8", newline="\n")


def _prune_stale_period_directories(
    *,
    config: RunConfig,
    root_dir: Path,
    active_period_keys: tuple[str, ...],
) -> None:
    if config.mode is not RunMode.FULL or not root_dir.exists():
        return
    active_period_key_set = set(active_period_keys)
    for child in root_dir.iterdir():
        if not child.is_dir() or child.name in active_period_key_set:
            continue
        shutil.rmtree(child)


def _prune_period_directories_for_contract_change(
    *,
    root_dir: Path,
    contract_path: Path,
    contract: dict[str, object],
) -> None:
    if _load_json_payload(contract_path) == contract:
        return
    if not root_dir.exists():
        return
    for child in root_dir.iterdir():
        if child == contract_path or child.name == ORG_SUMMARY_CONTRACT_FILENAME:
            continue
        if child.is_dir():
            shutil.rmtree(child)


def _load_json_payload(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _summary_row(
    label: str,
    summary: MetricValueSummary,
) -> str:
    return (
        f"| {label} | {summary.count} | {summary.total} | "
        f"{_float_text(summary.average)} | {_float_text(summary.median)} |"
    )


def _float_text(
    value: float | None,
) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _bool_text(
    value: bool,
) -> str:
    return "true" if value else "false"


def _repo_filters_text(
    repo_filters: tuple[str, ...],
    *,
    empty: str,
) -> str:
    if not repo_filters:
        return empty
    return ", ".join(repo_filters)


def _canonical_repo_filters(
    repo_filters: tuple[str, ...],
    *,
    org: str,
) -> tuple[str, ...]:
    return tuple(
        sorted(canonicalize_repo_filter(repo_filter, org=org) for repo_filter in repo_filters)
    )
