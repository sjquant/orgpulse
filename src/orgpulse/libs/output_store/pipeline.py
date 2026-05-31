from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from orgpulse.common.models import (
    ManifestWriteResult,
    MetricValidationCollection,
    OrganizationMetricCollection,
    OrgSummaryWriteResult,
    PullRequestCollection,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RepositorySummaryCsvWriteResult,
    RunConfig,
    RunManifest,
)
from orgpulse.libs.github.ingestion import (
    PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME,
    PULL_REQUEST_SNAPSHOT_FILENAME,
    PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME,
    NormalizedRawSnapshotWriter,
)
from orgpulse.libs.metrics.service import (
    MetricValidationCollectionBuilder,
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.libs.output_store.run_outputs import (
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)


@dataclass(frozen=True)
class OutputPipelineResult:
    raw_snapshot: RawSnapshotWriteResult | None
    raw_snapshot_skipped_reason: str | None
    manifest: ManifestWriteResult | None
    manifest_skipped_reason: str | None
    repo_summary: RepositorySummaryCsvWriteResult | None
    repo_summary_skipped_reason: str | None
    org_metrics: OrganizationMetricCollection | None
    org_metrics_skipped_reason: str | None
    org_summary: OrgSummaryWriteResult | None
    org_summary_skipped_reason: str | None
    metric_validation: MetricValidationCollection | None
    metric_validation_skipped_reason: str | None

    def to_payload_fields(self) -> dict[str, object]:
        return {
            "raw_snapshot": None
            if self.raw_snapshot is None
            else self.raw_snapshot.model_dump(mode="json"),
            "raw_snapshot_skipped_reason": self.raw_snapshot_skipped_reason,
            "manifest": None
            if self.manifest is None
            else self.manifest.manifest.model_dump(mode="json"),
            "manifest_path": None if self.manifest is None else str(self.manifest.path),
            "manifest_skipped_reason": self.manifest_skipped_reason,
            "repo_summary": None
            if self.repo_summary is None
            else self.repo_summary.model_dump(mode="json"),
            "repo_summary_skipped_reason": self.repo_summary_skipped_reason,
            "org_metrics": None
            if self.org_metrics is None
            else self.org_metrics.model_dump(mode="json"),
            "org_metrics_skipped_reason": self.org_metrics_skipped_reason,
            "org_summary": None
            if self.org_summary is None
            else self.org_summary.model_dump(mode="json"),
            "org_summary_skipped_reason": self.org_summary_skipped_reason,
            "metric_validation": None
            if self.metric_validation is None
            else self.metric_validation.model_dump(mode="json"),
            "metric_validation_skipped_reason": self.metric_validation_skipped_reason,
        }


def execute_output_pipeline(
    config: RunConfig,
    *,
    repository_count: int,
    collection: PullRequestCollection,
) -> OutputPipelineResult:
    raw_snapshot, raw_snapshot_skipped_reason = _write_raw_snapshot(
        config,
        collection,
    )
    manifest, manifest_skipped_reason = _write_manifest(
        config,
        repository_count=repository_count,
        collection=collection,
        raw_snapshot=raw_snapshot,
        raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
    )
    (
        repo_summary,
        repo_summary_skipped_reason,
        org_metrics,
        org_metrics_skipped_reason,
        metric_validation,
        metric_validation_skipped_reason,
    ) = _build_metric_outputs(
        config,
        manifest=manifest,
        raw_snapshot=raw_snapshot,
        raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
    )
    org_summary, org_summary_skipped_reason = _write_org_summary(
        config,
        org_metrics=org_metrics,
        org_metrics_skipped_reason=org_metrics_skipped_reason,
        refreshed_period_keys=()
        if raw_snapshot is None
        else tuple(period.key for period in raw_snapshot.periods),
    )
    return OutputPipelineResult(
        raw_snapshot=raw_snapshot,
        raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
        manifest=manifest,
        manifest_skipped_reason=manifest_skipped_reason,
        repo_summary=repo_summary,
        repo_summary_skipped_reason=repo_summary_skipped_reason,
        org_metrics=org_metrics,
        org_metrics_skipped_reason=org_metrics_skipped_reason,
        org_summary=org_summary,
        org_summary_skipped_reason=org_summary_skipped_reason,
        metric_validation=metric_validation,
        metric_validation_skipped_reason=metric_validation_skipped_reason,
    )


def _write_raw_snapshot(
    config: RunConfig,
    collection: PullRequestCollection,
) -> tuple[RawSnapshotWriteResult | None, str | None]:
    if collection.failures:
        return None, "repository_collection_failures"
    return NormalizedRawSnapshotWriter().write(config, collection), None


def _write_manifest(
    config: RunConfig,
    *,
    repository_count: int,
    collection: PullRequestCollection,
    raw_snapshot: RawSnapshotWriteResult | None,
    raw_snapshot_skipped_reason: str | None,
) -> tuple[ManifestWriteResult | None, str | None]:
    if raw_snapshot is None:
        return None, raw_snapshot_skipped_reason
    return (
        RunManifestWriter().write(
            config,
            collection,
            raw_snapshot,
            repository_count=repository_count,
        ),
        None,
    )


def _build_metric_outputs(
    config: RunConfig,
    *,
    manifest: ManifestWriteResult | None,
    raw_snapshot: RawSnapshotWriteResult | None,
    raw_snapshot_skipped_reason: str | None,
) -> tuple[
    RepositorySummaryCsvWriteResult | None,
    str | None,
    OrganizationMetricCollection | None,
    str | None,
    MetricValidationCollection | None,
    str | None,
]:
    if raw_snapshot is None or manifest is None:
        return (
            None,
            raw_snapshot_skipped_reason,
            None,
            raw_snapshot_skipped_reason,
            None,
            raw_snapshot_skipped_reason,
        )
    metric_snapshot = _build_metric_snapshot(
        manifest=manifest.manifest,
        refreshed_snapshot=raw_snapshot,
    )
    pull_request_metrics = PullRequestMetricCollectionBuilder().build(
        config,
        metric_snapshot,
    )
    repository_metrics = RepositoryMetricCollectionBuilder().build(
        config,
        pull_request_metrics,
    )
    repo_summary = RepositorySummaryCsvWriter().write(
        config,
        repository_metrics,
        refreshed_period_keys=tuple(period.key for period in raw_snapshot.periods),
    )
    org_metrics = OrganizationMetricCollectionBuilder().build(
        config,
        pull_request_metrics,
    )
    metric_validation = MetricValidationCollectionBuilder().build(
        config,
        raw_snapshot=metric_snapshot,
        pull_request_metrics=pull_request_metrics,
        org_metrics=org_metrics,
    )
    return repo_summary, None, org_metrics, None, metric_validation, None


def _write_org_summary(
    config: RunConfig,
    *,
    org_metrics: OrganizationMetricCollection | None,
    org_metrics_skipped_reason: str | None,
    refreshed_period_keys: tuple[str, ...],
) -> tuple[OrgSummaryWriteResult | None, str | None]:
    if org_metrics is None:
        return None, org_metrics_skipped_reason
    return (
        OrgSummaryWriter().write(
            config,
            org_metrics,
            refreshed_period_keys=refreshed_period_keys,
        ),
        None,
    )


def _build_metric_snapshot(
    *,
    manifest: RunManifest,
    refreshed_snapshot: RawSnapshotWriteResult,
) -> RawSnapshotWriteResult:
    period_index = {
        period.key: period for period in refreshed_snapshot.periods
    }
    for locked_period in manifest.locked_periods:
        period_index.setdefault(
            locked_period.key,
            _build_snapshot_period(manifest.raw_snapshot_root_dir, locked_period),
        )
    return RawSnapshotWriteResult(
        root_dir=manifest.raw_snapshot_root_dir,
        periods=tuple(
            period_index[key]
            for key in sorted(
                period_index.keys(),
                key=lambda period_key: (
                    period_index[period_key].start_date,
                    period_key,
                ),
            )
        ),
    )


def _build_snapshot_period(
    root_dir: Path,
    period: ReportingPeriod,
) -> RawSnapshotPeriod:
    period_dir = root_dir / period.key
    pull_requests_path = period_dir / PULL_REQUEST_SNAPSHOT_FILENAME
    reviews_path = period_dir / PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME
    timeline_events_path = period_dir / PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME
    return RawSnapshotPeriod(
        key=period.key,
        start_date=period.start_date,
        end_date=period.end_date,
        closed=period.closed,
        directory=period_dir,
        pull_requests_path=pull_requests_path,
        pull_request_count=_count_snapshot_rows(pull_requests_path),
        reviews_path=reviews_path,
        review_count=_count_snapshot_rows(reviews_path),
        timeline_events_path=timeline_events_path,
        timeline_event_count=_count_snapshot_rows(timeline_events_path),
    )


def _count_snapshot_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            return max(sum(1 for _ in handle) - 1, 0)
    except OSError:
        return 0
