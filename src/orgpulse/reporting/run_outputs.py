from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TypeVar, cast

from pydantic import BaseModel

from orgpulse.files import atomic_write_csv, atomic_write_json, atomic_write_text
from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    PULL_REQUEST_REVIEW_FIELDNAMES,
    PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
)
from orgpulse.models import (
    LastSuccessfulRun,
    ManifestHistoryPayload,
    ManifestIndexLatestPayload,
    ManifestIndexPayload,
    ManifestPeriodPayload,
    ManifestWatermarks,
    ManifestWriteResult,
    MetricValueSummary,
    OrganizationMetricCollection,
    OrganizationMetricPeriod,
    OrgSummaryContractPayload,
    OrgSummaryHistoryEntryPayload,
    OrgSummaryIndexPayload,
    OrgSummaryJsonPayload,
    OrgSummaryLatestPayload,
    OrgSummaryPeriodPayload,
    OrgSummaryPeriodWriteResult,
    OrgSummaryWriteResult,
    PeriodStatePayload,
    PullRequestCollection,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RepositoryMetricCollection,
    RepositoryMetricPeriod,
    RepositoryMetricRollup,
    RepositorySummaryContractPayload,
    RepositorySummaryCsvPeriod,
    RepositorySummaryCsvWriteResult,
    RepositorySummaryHistoryEntryPayload,
    RepositorySummaryIndexPayload,
    RepositorySummaryLatestPayload,
    RunConfig,
    RunManifest,
    RunMode,
    TimeAnchorContextPayload,
    canonicalize_repo_filter,
)
from orgpulse.reporting.contracts import (
    build_anchored_metric_label as _shared_anchored_metric_label,
)
from orgpulse.reporting.contracts import (
    build_period_state_payload as _shared_period_state_payload,
)
from orgpulse.reporting.contracts import (
    build_time_anchor_context as _shared_time_anchor_context,
)
from orgpulse.reporting.contracts import (
    time_anchor_scope as _shared_time_anchor_scope,
)

HistoryEntryPayload = (
    RepositorySummaryHistoryEntryPayload
    | OrgSummaryHistoryEntryPayload
    | dict[str, object]
)
HistoryEntryT = TypeVar(
    "HistoryEntryT",
    RepositorySummaryHistoryEntryPayload,
    OrgSummaryHistoryEntryPayload,
)

MANIFEST_FILENAME = "manifest.json"
MANIFEST_DIRNAME = "manifest"

INDEX_FILENAME = "index.json"
README_FILENAME = "README.md"
LATEST_DIRNAME = "latest"

ORG_SUMMARY_DIRNAME = "org_summary"
SUMMARY_CONTRACT_FILENAME = "contract.json"
ORG_SUMMARY_JSON_FILENAME = "summary.json"
ORG_SUMMARY_MARKDOWN_FILENAME = "summary.md"

REPOSITORY_SUMMARY_CSV_DIRNAME = "repo_summary"
REPOSITORY_SUMMARY_CSV_FILENAME = "repo_summary.csv"
REPOSITORY_SUMMARY_CSV_FIELDNAMES = (
    "period_key",
    "period_grain",
    "time_anchor",
    "time_anchor_scope",
    "period_start_date",
    "period_end_date",
    "period_status",
    "period_label",
    "period_open",
    "period_open_week",
    "period_open_month",
    "period_closed",
    "period_partial",
    "period_observed_through_date",
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
        root_dir = self._root_dir(
            config.output_dir,
            config.period.value,
            config.time_anchor.value,
        )
        refreshed_periods = tuple(
            period
            for period in repository_metrics.periods
            if period.key in set(refreshed_period_keys)
        )
        contract_path = root_dir / SUMMARY_CONTRACT_FILENAME
        index_path = root_dir / INDEX_FILENAME
        readme_path = root_dir / README_FILENAME
        contract = self._contract_payload(config)
        _prune_output_entries_for_contract_change(
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
        period_results = tuple(
            self._write_period_summary(
                config=config,
                root_dir=root_dir,
                metric_period=metric_period,
            )
            for metric_period in refreshed_periods
        )
        history_entries = self._history_entries(
            config=config,
            root_dir=root_dir,
            periods=repository_metrics.periods,
            index_path=index_path,
        )
        latest_path = self._write_latest_summary(
            root_dir=root_dir,
            history_entries=history_entries,
        )
        _write_json_file(
            index_path,
            self._index_payload(config=config, history_entries=history_entries),
        )
        _write_text_file(
            readme_path,
            self._readme_document(config=config, history_entries=history_entries),
        )
        return RepositorySummaryCsvWriteResult(
            root_dir=root_dir,
            contract_path=contract_path,
            index_path=index_path,
            readme_path=readme_path,
            latest_path=latest_path,
            periods=period_results,
        )

    def _write_period_summary(
        self,
        *,
        config: RunConfig,
        root_dir: Path,
        metric_period: RepositoryMetricPeriod,
    ) -> RepositorySummaryCsvPeriod:
        path = root_dir / metric_period.key / REPOSITORY_SUMMARY_CSV_FILENAME
        _write_csv_file(
            path=path,
            fieldnames=REPOSITORY_SUMMARY_CSV_FIELDNAMES,
            rows=[
                self._csv_row(config, metric_period, repository)
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
        config: RunConfig,
        metric_period: RepositoryMetricPeriod,
        repository: RepositoryMetricRollup,
    ) -> dict[str, object]:
        period_state = _period_state_payload(
            period_grain=config.period.value,
            start_date=metric_period.start_date,
            end_date=metric_period.end_date,
            closed=metric_period.closed,
            as_of=config.as_of,
        )
        return {
            "period_key": metric_period.key,
            "period_grain": config.period.value,
            "time_anchor": config.time_anchor.value,
            "time_anchor_scope": _time_anchor_scope(config.time_anchor.value),
            "period_start_date": metric_period.start_date.isoformat(),
            "period_end_date": metric_period.end_date.isoformat(),
            "period_status": period_state.status,
            "period_label": period_state.label,
            "period_open": str(period_state.is_open).lower(),
            "period_open_week": str(period_state.open_week).lower(),
            "period_open_month": str(period_state.open_month).lower(),
            "period_closed": str(metric_period.closed).lower(),
            "period_partial": str(period_state.is_partial).lower(),
            "period_observed_through_date": period_state.observed_through_date,
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

    def _history_entries(
        self,
        *,
        config: RunConfig,
        root_dir: Path,
        periods: tuple[RepositoryMetricPeriod, ...],
        index_path: Path,
    ) -> tuple[RepositorySummaryHistoryEntryPayload, ...]:
        saved_history = _load_index_history(index_path)
        history_entries: list[RepositorySummaryHistoryEntryPayload] = []
        period_index = {period.key: period for period in periods}
        for period_key in _history_period_keys(root_dir):
            if period_key in period_index:
                period = period_index[period_key]
                period_state = _period_state_payload(
                    period_grain=config.period.value,
                    start_date=period.start_date,
                    end_date=period.end_date,
                    closed=period.closed,
                    as_of=config.as_of,
                )
                history_entries.append(
                    RepositorySummaryHistoryEntryPayload(
                        key=period.key,
                        start_date=period.start_date.isoformat(),
                        end_date=period.end_date.isoformat(),
                        closed=period.closed,
                        **period_state.model_dump(mode="json"),
                        path=_relative_path(
                            root_dir,
                            root_dir / period.key / REPOSITORY_SUMMARY_CSV_FILENAME,
                        ),
                    )
                )
                continue
            saved_entry = saved_history.get(period_key)
            if saved_entry is not None:
                history_entries.append(
                    _validate_saved_history_entry(
                        RepositorySummaryHistoryEntryPayload,
                        saved_entry,
                    )
                )
        return tuple(sorted(history_entries, key=_history_entry_sort_key))

    def _write_latest_summary(
        self,
        *,
        root_dir: Path,
        history_entries: tuple[RepositorySummaryHistoryEntryPayload, ...],
    ) -> Path | None:
        latest_entry = _latest_history_entry(history_entries)
        if latest_entry is None:
            _remove_directory(root_dir / LATEST_DIRNAME)
            return None
        latest_dir = root_dir / LATEST_DIRNAME
        _reset_directory(latest_dir)
        source_path = root_dir / latest_entry.path
        latest_path = latest_dir / REPOSITORY_SUMMARY_CSV_FILENAME
        shutil.copyfile(source_path, latest_path)
        return latest_path

    def _index_payload(
        self,
        *,
        config: RunConfig,
        history_entries: tuple[RepositorySummaryHistoryEntryPayload, ...],
    ) -> RepositorySummaryIndexPayload:
        latest_entry = _latest_history_entry(history_entries)
        latest_payload = None
        if latest_entry is not None:
            root_dir = self._root_dir(
                config.output_dir,
                config.period.value,
                config.time_anchor.value,
            )
            latest_payload = RepositorySummaryLatestPayload(
                key=latest_entry.key,
                start_date=latest_entry.start_date,
                end_date=latest_entry.end_date,
                closed=latest_entry.closed,
                status=latest_entry.status,
                label=latest_entry.label,
                is_open=latest_entry.is_open,
                is_closed=latest_entry.is_closed,
                is_partial=latest_entry.is_partial,
                observed_through_date=latest_entry.observed_through_date,
                open_week=latest_entry.open_week,
                open_month=latest_entry.open_month,
                path=_relative_path(
                    root_dir,
                    root_dir / LATEST_DIRNAME / REPOSITORY_SUMMARY_CSV_FILENAME,
                ),
                source_path=latest_entry.path,
            )
        return RepositorySummaryIndexPayload(
            target_org=config.org,
            period_grain=config.period.value,
            time_anchor=config.time_anchor.value,
            time_anchor_context=_time_anchor_context(config.time_anchor.value),
            include_repos=list(
                _canonical_repo_filters(config.include_repos, org=config.org)
            ),
            exclude_repos=list(
                _canonical_repo_filters(config.exclude_repos, org=config.org)
            ),
            latest=latest_payload,
            history=list(history_entries),
        )

    def _readme_document(
        self,
        *,
        config: RunConfig,
        history_entries: tuple[RepositorySummaryHistoryEntryPayload, ...],
    ) -> str:
        latest_entry = _latest_history_entry(history_entries)
        history_rows = (
            [
                "| Period | Start | End | Status | Partial | Observed Through | CSV |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
            + [
                (
                    f"| {entry.key} | {entry.start_date} | {entry.end_date} | "
                    f"{entry.label} | {_bool_text(entry.is_partial)} | "
                    f"{entry.observed_through_date} | {entry.path} |"
                )
                for entry in history_entries
            ]
            if history_entries
            else ["No history files are available."]
        )
        latest_lines = (
            [
                f"- Latest period: {latest_entry.key}",
                f"- Latest period status: {latest_entry.label}",
                f"- Latest period partial: {_bool_text(latest_entry.is_partial)}",
                (
                    "- Latest period observed through: "
                    f"{latest_entry.observed_through_date}"
                ),
                (
                    "- Latest CSV: "
                    f"{LATEST_DIRNAME}/{REPOSITORY_SUMMARY_CSV_FILENAME}"
                ),
            ]
            if latest_entry is not None
            else [
                "- Latest period: none",
                "- Latest period status: none",
                "- Latest period partial: none",
                "- Latest period observed through: none",
                "- Latest CSV: none",
            ]
        )
        return "\n".join(
            (
                f"# Repository Summary Index: {config.org} {config.period.value}",
                "",
                f"- Target org: {config.org}",
                f"- Period grain: {config.period.value}",
                f"- Time anchor: {config.time_anchor.value}",
                (
                    "- Summary scope: "
                    f"{_time_anchor_context(config.time_anchor.value).description}"
                ),
                f"- Include repos: {_repo_filters_text(_canonical_repo_filters(config.include_repos, org=config.org), empty='all')}",
                f"- Exclude repos: {_repo_filters_text(_canonical_repo_filters(config.exclude_repos, org=config.org), empty='none')}",
                *latest_lines,
                "",
                "## History",
                "",
                *history_rows,
                "",
            )
        )

    def _contract_payload(
        self,
        config: RunConfig,
    ) -> RepositorySummaryContractPayload:
        return RepositorySummaryContractPayload(
            target_org=config.org,
            period_grain=config.period.value,
            time_anchor=config.time_anchor.value,
            time_anchor_context=_time_anchor_context(config.time_anchor.value),
            period_state_fields=[
                "status",
                "label",
                "is_open",
                "is_closed",
                "is_partial",
                "observed_through_date",
                "open_week",
                "open_month",
            ],
            include_repos=list(
                _canonical_repo_filters(config.include_repos, org=config.org)
            ),
            exclude_repos=list(
                _canonical_repo_filters(config.exclude_repos, org=config.org)
            ),
        )

    def _root_dir(
        self,
        output_dir: Path,
        period_grain: str,
        time_anchor: str,
    ) -> Path:
        return output_dir / REPOSITORY_SUMMARY_CSV_DIRNAME / period_grain / time_anchor


class OrgSummaryWriter:
    """Persist deterministic org-level summary files for each reporting period."""

    def write(
        self,
        config: RunConfig,
        org_metrics: OrganizationMetricCollection,
        *,
        refreshed_period_keys: tuple[str, ...],
    ) -> OrgSummaryWriteResult:
        root_dir = self._root_dir(
            config.output_dir,
            config.period.value,
            config.time_anchor.value,
        )
        refreshed_periods = tuple(
            period for period in org_metrics.periods if period.key in set(refreshed_period_keys)
        )
        contract_path = root_dir / SUMMARY_CONTRACT_FILENAME
        index_path = root_dir / INDEX_FILENAME
        readme_path = root_dir / README_FILENAME
        contract = self._contract_payload(config)
        _prune_output_entries_for_contract_change(
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
        period_results = tuple(
            self._write_period_summary(
                config=config,
                root_dir=root_dir,
                period=period,
                target_org=org_metrics.target_org,
            )
            for period in refreshed_periods
        )
        history_entries = self._history_entries(
            config=config,
            root_dir=root_dir,
            periods=org_metrics.periods,
            index_path=index_path,
        )
        latest_directory, latest_markdown_path, latest_json_path = self._write_latest_summary(
            root_dir=root_dir,
            history_entries=history_entries,
        )
        _write_json_file(
            index_path,
            self._index_payload(
                config=config,
                history_entries=history_entries,
                latest_markdown_path=latest_markdown_path,
                latest_json_path=latest_json_path,
            ),
        )
        _write_text_file(
            readme_path,
            self._readme_document(config=config, history_entries=history_entries),
        )
        return OrgSummaryWriteResult(
            target_org=org_metrics.target_org,
            root_dir=root_dir,
            contract_path=contract_path,
            index_path=index_path,
            readme_path=readme_path,
            latest_directory=latest_directory,
            latest_markdown_path=latest_markdown_path,
            latest_json_path=latest_json_path,
            periods=period_results,
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
    ) -> OrgSummaryJsonPayload:
        include_repos = _canonical_repo_filters(config.include_repos, org=config.org)
        exclude_repos = _canonical_repo_filters(config.exclude_repos, org=config.org)
        period_state = _period_state_payload(
            period_grain=config.period.value,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=period.closed,
            as_of=config.as_of,
        )
        return OrgSummaryJsonPayload(
            target_org=target_org,
            period_grain=config.period.value,
            time_anchor=config.time_anchor.value,
            time_anchor_context=_time_anchor_context(config.time_anchor.value),
            include_repos=list(include_repos),
            exclude_repos=list(exclude_repos),
            period=OrgSummaryPeriodPayload(
                key=period.key,
                start_date=period.start_date.isoformat(),
                end_date=period.end_date.isoformat(),
                closed=period.closed,
                **period_state.model_dump(mode="json"),
            ),
            summary_labels={
                "pull_request_count": _anchored_metric_label(
                    "Pull request count",
                    config.time_anchor.value,
                ),
                "merged_pull_request_count": _anchored_metric_label(
                    "Merged pull request count",
                    config.time_anchor.value,
                ),
                "value_summaries": (
                    "Value summaries grouped by "
                    f"{_time_anchor_scope(config.time_anchor.value)}"
                ),
            },
            summary=period.summary.model_dump(mode="json"),
        )

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
        period_state = _period_state_payload(
            period_grain=config.period.value,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=period.closed,
            as_of=config.as_of,
        )
        return "\n".join(
            (
                (
                    f"# Organization Summary: {target_org} {period.key} "
                    f"({_time_anchor_scope(config.time_anchor.value)})"
                ),
                "",
                f"- Target org: {target_org}",
                f"- Period grain: {config.period.value}",
                f"- Time anchor: {config.time_anchor.value}",
                (
                    "- Summary scope: "
                    f"{_time_anchor_context(config.time_anchor.value).description}"
                ),
                f"- Period key: {period.key}",
                f"- Include repos: {_repo_filters_text(include_repos, empty='all')}",
                f"- Exclude repos: {_repo_filters_text(exclude_repos, empty='none')}",
                f"- Period start: {period.start_date.isoformat()}",
                f"- Period end: {period.end_date.isoformat()}",
                f"- Period status: {period_state.label}",
                f"- Partial period: {_bool_text(period_state.is_partial)}",
                (
                    "- Observed through: "
                    f"{period_state.observed_through_date}"
                ),
                "",
                "## Totals",
                "",
                f"- Repository count: {summary.repository_count}",
                (
                    f"- {_anchored_metric_label('Pull request count', config.time_anchor.value)}: "
                    f"{summary.pull_request_count}"
                ),
                (
                    f"- {_anchored_metric_label('Merged pull request count', config.time_anchor.value)}: "
                    f"{summary.merged_pull_request_count}"
                ),
                f"- Active author count: {summary.active_author_count}",
                (
                    "- Merged pull requests per active author: "
                    f"{_float_text(summary.merged_pull_requests_per_active_author)}"
                ),
                "",
                (
                    "## Value Summaries "
                    f"({_time_anchor_scope(config.time_anchor.value)})"
                ),
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

    def _history_entries(
        self,
        *,
        config: RunConfig,
        root_dir: Path,
        periods: tuple[OrganizationMetricPeriod, ...],
        index_path: Path,
    ) -> tuple[OrgSummaryHistoryEntryPayload, ...]:
        saved_history = _load_index_history(index_path)
        history_entries: list[OrgSummaryHistoryEntryPayload] = []
        period_index = {period.key: period for period in periods}
        for period_key in _history_period_keys(root_dir):
            if period_key in period_index:
                period = period_index[period_key]
                period_state = _period_state_payload(
                    period_grain=config.period.value,
                    start_date=period.start_date,
                    end_date=period.end_date,
                    closed=period.closed,
                    as_of=config.as_of,
                )
                history_entries.append(
                    OrgSummaryHistoryEntryPayload(
                        key=period.key,
                        start_date=period.start_date.isoformat(),
                        end_date=period.end_date.isoformat(),
                        closed=period.closed,
                        **period_state.model_dump(mode="json"),
                        markdown_path=_relative_path(
                            root_dir,
                            root_dir / period.key / ORG_SUMMARY_MARKDOWN_FILENAME,
                        ),
                        json_path=_relative_path(
                            root_dir,
                            root_dir / period.key / ORG_SUMMARY_JSON_FILENAME,
                        ),
                    )
                )
                continue
            saved_entry = saved_history.get(period_key)
            if saved_entry is not None:
                history_entries.append(
                    _validate_saved_history_entry(
                        OrgSummaryHistoryEntryPayload,
                        saved_entry,
                    )
                )
        return tuple(sorted(history_entries, key=_history_entry_sort_key))

    def _write_latest_summary(
        self,
        *,
        root_dir: Path,
        history_entries: tuple[OrgSummaryHistoryEntryPayload, ...],
    ) -> tuple[Path | None, Path | None, Path | None]:
        latest_entry = _latest_history_entry(history_entries)
        if latest_entry is None:
            _remove_directory(root_dir / LATEST_DIRNAME)
            return None, None, None
        latest_dir = root_dir / LATEST_DIRNAME
        _reset_directory(latest_dir)
        source_markdown_path = root_dir / latest_entry.markdown_path
        source_json_path = root_dir / latest_entry.json_path
        latest_markdown_path = latest_dir / ORG_SUMMARY_MARKDOWN_FILENAME
        latest_json_path = latest_dir / ORG_SUMMARY_JSON_FILENAME
        shutil.copyfile(source_markdown_path, latest_markdown_path)
        shutil.copyfile(source_json_path, latest_json_path)
        return latest_dir, latest_markdown_path, latest_json_path

    def _index_payload(
        self,
        *,
        config: RunConfig,
        history_entries: tuple[OrgSummaryHistoryEntryPayload, ...],
        latest_markdown_path: Path | None,
        latest_json_path: Path | None,
    ) -> OrgSummaryIndexPayload:
        latest_entry = _latest_history_entry(history_entries)
        latest_payload = None
        if (
            latest_entry is not None
            and latest_markdown_path is not None
            and latest_json_path is not None
        ):
            root_dir = self._root_dir(
                config.output_dir,
                config.period.value,
                config.time_anchor.value,
            )
            latest_payload = OrgSummaryLatestPayload(
                key=latest_entry.key,
                start_date=latest_entry.start_date,
                end_date=latest_entry.end_date,
                closed=latest_entry.closed,
                status=latest_entry.status,
                label=latest_entry.label,
                is_open=latest_entry.is_open,
                is_closed=latest_entry.is_closed,
                is_partial=latest_entry.is_partial,
                observed_through_date=latest_entry.observed_through_date,
                open_week=latest_entry.open_week,
                open_month=latest_entry.open_month,
                markdown_path=_relative_path(root_dir, latest_markdown_path),
                json_path=_relative_path(root_dir, latest_json_path),
                source_markdown_path=latest_entry.markdown_path,
                source_json_path=latest_entry.json_path,
            )
        return OrgSummaryIndexPayload(
            target_org=config.org,
            period_grain=config.period.value,
            time_anchor=config.time_anchor.value,
            time_anchor_context=_time_anchor_context(config.time_anchor.value),
            include_repos=list(
                _canonical_repo_filters(config.include_repos, org=config.org)
            ),
            exclude_repos=list(
                _canonical_repo_filters(config.exclude_repos, org=config.org)
            ),
            latest=latest_payload,
            history=list(history_entries),
        )

    def _readme_document(
        self,
        *,
        config: RunConfig,
        history_entries: tuple[OrgSummaryHistoryEntryPayload, ...],
    ) -> str:
        latest_entry = _latest_history_entry(history_entries)
        history_rows = (
            [
                (
                    "| Period | Start | End | Status | Partial | "
                    "Observed Through | JSON | Markdown |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
            + [
                (
                    f"| {entry.key} | {entry.start_date} | {entry.end_date} | "
                    f"{entry.label} | {_bool_text(entry.is_partial)} | "
                    f"{entry.observed_through_date} | {entry.json_path} | "
                    f"{entry.markdown_path} |"
                )
                for entry in history_entries
            ]
            if history_entries
            else ["No history files are available."]
        )
        latest_lines = (
            [
                f"- Latest period: {latest_entry.key}",
                f"- Latest period status: {latest_entry.label}",
                f"- Latest period partial: {_bool_text(latest_entry.is_partial)}",
                (
                    "- Latest period observed through: "
                    f"{latest_entry.observed_through_date}"
                ),
                f"- Latest JSON: {LATEST_DIRNAME}/{ORG_SUMMARY_JSON_FILENAME}",
                f"- Latest Markdown: {LATEST_DIRNAME}/{ORG_SUMMARY_MARKDOWN_FILENAME}",
            ]
            if latest_entry is not None
            else [
                "- Latest period: none",
                "- Latest period status: none",
                "- Latest period partial: none",
                "- Latest period observed through: none",
                "- Latest JSON: none",
                "- Latest Markdown: none",
            ]
        )
        return "\n".join(
            (
                f"# Organization Summary Index: {config.org} {config.period.value}",
                "",
                f"- Target org: {config.org}",
                f"- Period grain: {config.period.value}",
                f"- Time anchor: {config.time_anchor.value}",
                (
                    "- Summary scope: "
                    f"{_time_anchor_context(config.time_anchor.value).description}"
                ),
                f"- Include repos: {_repo_filters_text(_canonical_repo_filters(config.include_repos, org=config.org), empty='all')}",
                f"- Exclude repos: {_repo_filters_text(_canonical_repo_filters(config.exclude_repos, org=config.org), empty='none')}",
                *latest_lines,
                "",
                "## History",
                "",
                *history_rows,
                "",
            )
        )

    def _contract_payload(
        self,
        config: RunConfig,
    ) -> OrgSummaryContractPayload:
        return OrgSummaryContractPayload(
            target_org=config.org,
            period_grain=config.period.value,
            time_anchor=config.time_anchor.value,
            time_anchor_context=_time_anchor_context(config.time_anchor.value),
            include_repos=list(
                _canonical_repo_filters(config.include_repos, org=config.org)
            ),
            exclude_repos=list(
                _canonical_repo_filters(config.exclude_repos, org=config.org)
            ),
        )

    def _root_dir(
        self,
        output_dir: Path,
        period_grain: str,
        time_anchor: str,
    ) -> Path:
        return output_dir / ORG_SUMMARY_DIRNAME / period_grain / time_anchor


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
        path = self._manifest_path(
            config.output_dir,
            config.period.value,
            config.time_anchor.value,
        )
        index_path = path.parent / INDEX_FILENAME
        readme_path = path.parent / README_FILENAME
        self._write_manifest_file(path, manifest)
        _write_json_file(index_path, self._index_payload(manifest))
        _write_text_file(readme_path, self._readme_document(manifest))
        return ManifestWriteResult(
            path=path,
            index_path=index_path,
            readme_path=readme_path,
            manifest=manifest,
        )

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
            time_anchor=config.time_anchor,
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
            manifest_path=self._manifest_path(
                config.output_dir,
                config.period.value,
                config.time_anchor.value,
            ),
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
            and manifest.time_anchor == config.time_anchor
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

    def _index_payload(
        self,
        manifest: RunManifest,
    ) -> ManifestIndexPayload:
        locked_period_keys = {period.key for period in manifest.locked_periods}
        return ManifestIndexPayload(
            target_org=manifest.target_org,
            period_grain=manifest.period_grain.value,
            time_anchor=manifest.time_anchor.value,
            time_anchor_context=_time_anchor_context(manifest.time_anchor.value),
            include_repos=list(
                _canonical_repo_filters(manifest.include_repos, org=manifest.target_org)
            ),
            exclude_repos=list(
                _canonical_repo_filters(manifest.exclude_repos, org=manifest.target_org)
            ),
            latest=ManifestIndexLatestPayload(
                manifest_path=MANIFEST_FILENAME,
                completed_at=manifest.last_successful_run.completed_at.isoformat(),
                as_of=manifest.last_successful_run.as_of.isoformat(),
                mode=manifest.last_successful_run.mode.value,
                refresh_scope=manifest.last_successful_run.refresh_scope.value,
            ),
            history=ManifestHistoryPayload(
                refreshed_periods=[
                    _manifest_period_payload(
                        period,
                        period_grain=manifest.period_grain.value,
                        closed=period.key in locked_period_keys,
                        as_of=manifest.last_successful_run.as_of,
                    )
                    for period in manifest.refreshed_periods
                ],
                locked_periods=[
                    _manifest_period_payload(
                        period,
                        period_grain=manifest.period_grain.value,
                        closed=period.closed,
                        as_of=manifest.last_successful_run.as_of,
                    )
                    for period in manifest.locked_periods
                ],
            ),
            watermarks=manifest.watermarks.model_dump(mode="json"),
        )

    def _readme_document(
        self,
        manifest: RunManifest,
    ) -> str:
        locked_period_keys = {period.key for period in manifest.locked_periods}
        refreshed_rows = _manifest_period_table_rows(
            manifest.refreshed_periods,
            period_grain=manifest.period_grain.value,
            as_of=manifest.last_successful_run.as_of,
            closed_period_keys=locked_period_keys,
        )
        locked_rows = _manifest_period_table_rows(
            manifest.locked_periods,
            period_grain=manifest.period_grain.value,
            as_of=manifest.last_successful_run.as_of,
            closed_period_keys=locked_period_keys,
        )
        return "\n".join(
            (
                f"# Manifest Index: {manifest.target_org} {manifest.period_grain.value}",
                "",
                f"- Target org: {manifest.target_org}",
                f"- Period grain: {manifest.period_grain.value}",
                f"- Time anchor: {manifest.time_anchor.value}",
                (
                    "- Summary scope: "
                    f"{_time_anchor_context(manifest.time_anchor.value).description}"
                ),
                f"- Include repos: {_repo_filters_text(_canonical_repo_filters(manifest.include_repos, org=manifest.target_org), empty='all')}",
                f"- Exclude repos: {_repo_filters_text(_canonical_repo_filters(manifest.exclude_repos, org=manifest.target_org), empty='none')}",
                f"- Latest manifest: {MANIFEST_FILENAME}",
                f"- Completed at: {manifest.last_successful_run.completed_at.isoformat()}",
                f"- As of: {manifest.last_successful_run.as_of.isoformat()}",
                f"- Mode: {manifest.last_successful_run.mode.value}",
                f"- Refresh scope: {manifest.last_successful_run.refresh_scope.value}",
                "",
                "## Refreshed Periods",
                "",
                *refreshed_rows,
                "",
                "## Locked Periods",
                "",
                *locked_rows,
                "",
            )
        )

    def _manifest_path(
        self,
        output_dir: Path,
        period_grain: str,
        time_anchor: str,
    ) -> Path:
        return output_dir / MANIFEST_DIRNAME / period_grain / time_anchor / MANIFEST_FILENAME

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
        _write_json_file(path, self._manifest_payload(manifest))

    def _manifest_payload(
        self,
        manifest: RunManifest,
    ) -> dict[str, object]:
        locked_period_keys = {period.key for period in manifest.locked_periods}
        payload = manifest.model_dump(mode="json")
        payload["time_anchor_context"] = _time_anchor_context(
            manifest.time_anchor.value
        ).model_dump(mode="json")
        payload["refreshed_periods"] = [
            {
                **period,
                **_period_state_payload(
                    period_grain=manifest.period_grain.value,
                    start_date=date.fromisoformat(str(period["start_date"])),
                    end_date=date.fromisoformat(str(period["end_date"])),
                    closed=bool(period["key"] in locked_period_keys),
                    as_of=manifest.last_successful_run.as_of,
                ).model_dump(mode="json"),
            }
            for period in payload["refreshed_periods"]
        ]
        payload["locked_periods"] = [
            {
                **period,
                **_period_state_payload(
                    period_grain=manifest.period_grain.value,
                    start_date=date.fromisoformat(str(period["start_date"])),
                    end_date=date.fromisoformat(str(period["end_date"])),
                    closed=bool(period["closed"]),
                    as_of=manifest.last_successful_run.as_of,
                ).model_dump(mode="json"),
            }
            for period in payload["locked_periods"]
        ]
        return payload

    def _current_time(self) -> datetime:
        return datetime.now(UTC).replace(microsecond=0)


def _write_csv_file(
    *,
    path: Path,
    fieldnames: tuple[str, ...],
    rows: list[dict[str, object]],
) -> None:
    atomic_write_csv(path=path, fieldnames=fieldnames, rows=rows)


def _write_json_file(
    path: Path,
    payload: object,
) -> None:
    atomic_write_json(path, _json_ready_payload(payload))


def _write_text_file(
    path: Path,
    document: str,
) -> None:
    atomic_write_text(path, document)


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


def _prune_output_entries_for_contract_change(
    *,
    root_dir: Path,
    contract_path: Path,
    contract: object,
) -> None:
    if _load_json_payload(contract_path) == _json_ready_payload(contract):
        return
    if not root_dir.exists():
        return
    for child in root_dir.iterdir():
        if child == contract_path or child.name == contract_path.name:
            continue
        if child.is_dir():
            shutil.rmtree(child)
            continue
        child.unlink(missing_ok=True)


def _load_json_payload(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _load_index_history(index_path: Path) -> dict[str, dict[str, object]]:
    payload = _load_json_payload(index_path)
    if payload is None:
        return {}
    history = payload.get("history")
    if not isinstance(history, list):
        return {}
    entries: dict[str, dict[str, object]] = {}
    for entry in history:
        if not isinstance(entry, dict):
            continue
        typed_entry = cast(dict[str, object], entry)
        key = typed_entry.get("key")
        if not isinstance(key, str):
            continue
        entries[key] = typed_entry
    return entries


def _history_period_keys(root_dir: Path) -> tuple[str, ...]:
    if not root_dir.exists():
        return ()
    return tuple(
        child.name
        for child in sorted(root_dir.iterdir(), key=lambda path: path.name)
        if child.is_dir() and child.name != LATEST_DIRNAME
    )


def _validate_saved_history_entry(
    model_class: type[HistoryEntryT],
    payload: dict[str, object],
) -> HistoryEntryT:
    filtered_payload = {
        key: value
        for key, value in payload.items()
        if key in model_class.model_fields
    }
    return model_class.model_validate(filtered_payload)


def _history_entry_sort_key(entry: object) -> tuple[date, str]:
    if isinstance(entry, dict):
        typed_entry = cast(dict[str, object], entry)
        return date.fromisoformat(str(typed_entry["start_date"])), str(
            typed_entry["key"]
        )
    typed_entry = cast(
        RepositorySummaryHistoryEntryPayload | OrgSummaryHistoryEntryPayload,
        entry,
    )
    return date.fromisoformat(str(typed_entry.start_date)), str(typed_entry.key)


def _latest_history_entry(
    history_entries: Sequence[HistoryEntryT],
) -> HistoryEntryT | None:
    if not history_entries:
        return None
    return max(history_entries, key=_history_entry_sort_key)


def _relative_path(root_dir: Path, path: Path) -> str:
    return path.relative_to(root_dir).as_posix()


def _reset_directory(path: Path) -> None:
    _remove_directory(path)
    path.mkdir(parents=True, exist_ok=True)


def _remove_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _manifest_period_payload(
    period: RawSnapshotPeriod | ReportingPeriod,
    *,
    period_grain: str,
    closed: bool,
    as_of: date,
) -> ManifestPeriodPayload:
    return ManifestPeriodPayload(
        key=period.key,
        start_date=period.start_date.isoformat(),
        end_date=period.end_date.isoformat(),
        closed=closed,
        **_period_state_payload(
            period_grain=period_grain,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=closed,
            as_of=as_of,
        ).model_dump(mode="json"),
    )


def _manifest_period_table_rows(
    periods: Sequence[RawSnapshotPeriod | ReportingPeriod],
    *,
    period_grain: str,
    as_of: date,
    closed_period_keys: set[str],
) -> list[str]:
    if not periods:
        return ["No periods are available."]
    rows = [
        "| Period | Start | End | Status | Partial | Observed Through |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for period in periods:
        period_state = _period_state_payload(
            period_grain=period_grain,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=period.key in closed_period_keys,
            as_of=as_of,
        )
        rows.append(
            (
                f"| {period.key} | {period.start_date.isoformat()} | "
                f"{period.end_date.isoformat()} | "
                f"{period_state.label} | "
                f"{_bool_text(period_state.is_partial)} | "
                f"{period_state.observed_through_date} |"
            )
        )
    return rows


def _json_ready_payload(
    payload: object,
) -> dict[str, object]:
    serialized_payload = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    return cast(dict[str, object], serialized_payload)


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


def _time_anchor_context(
    time_anchor: str,
) -> TimeAnchorContextPayload:
    return _shared_time_anchor_context(time_anchor)


def _time_anchor_scope(
    time_anchor: str,
) -> str:
    return _shared_time_anchor_scope(time_anchor)


def _anchored_metric_label(
    label: str,
    time_anchor: str,
) -> str:
    return _shared_anchored_metric_label(label, time_anchor)


def _period_state_payload(
    *,
    period_grain: str,
    start_date: date,
    end_date: date,
    closed: bool,
    as_of: date,
) -> PeriodStatePayload:
    return _shared_period_state_payload(
        period_grain=period_grain,
        start_date=start_date,
        end_date=end_date,
        closed=closed,
        as_of=as_of,
    )


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
        sorted(
            canonicalize_repo_filter(repo_filter, org=org)
            for repo_filter in repo_filters
        )
    )
