from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

from orgpulse.config import get_settings
from orgpulse.ingestion import NormalizedRawSnapshotWriter
from orgpulse.metrics import (
    MetricValidationCollectionBuilder,
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.models import (
    MetricValidationCollection,
    OrganizationMetricCollection,
    PullRequestCollection,
    PullRequestMetricCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RawSnapshotWriteResult,
    RepositoryMetricCollection,
    RunConfig,
)


@dataclass(frozen=True)
class MetricPipelineResult:
    """Capture the public metric outputs produced by the test harness."""

    config: RunConfig
    raw_snapshot: RawSnapshotWriteResult
    pull_request_metrics: PullRequestMetricCollection
    repository_metrics: RepositoryMetricCollection
    org_metrics: OrganizationMetricCollection


class MetricTestHarness:
    """Exercise the metrics pipeline through the production snapshot contract."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def build_run_config(self, **overrides: object) -> RunConfig:
        payload: dict[str, object] = {
            "org": "acme",
            "output_dir": self._output_dir,
        }
        payload.update(overrides)
        return RunConfig.model_validate(payload)

    def build_pipeline(
        self,
        *,
        pull_requests: tuple[PullRequestRecord, ...],
        **config_overrides: object,
    ) -> MetricPipelineResult:
        config = self.build_run_config(**config_overrides)
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=pull_requests,
        )
        pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            config,
            raw_snapshot,
        )
        repository_metrics = RepositoryMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )
        org_metrics = OrganizationMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )
        return MetricPipelineResult(
            config=config,
            raw_snapshot=raw_snapshot,
            pull_request_metrics=pull_request_metrics,
            repository_metrics=repository_metrics,
            org_metrics=org_metrics,
        )

    def build_validation(
        self,
        pipeline: MetricPipelineResult,
        *,
        raw_snapshot: RawSnapshotWriteResult | None = None,
        pull_request_metrics: PullRequestMetricCollection | None = None,
        org_metrics: OrganizationMetricCollection | None = None,
    ) -> MetricValidationCollection:
        return MetricValidationCollectionBuilder().build(
            pipeline.config,
            raw_snapshot=(
                pipeline.raw_snapshot if raw_snapshot is None else raw_snapshot
            ),
            pull_request_metrics=(
                pipeline.pull_request_metrics
                if pull_request_metrics is None
                else pull_request_metrics
            ),
            org_metrics=pipeline.org_metrics if org_metrics is None else org_metrics,
        )

    def _write_raw_snapshot(
        self,
        config: RunConfig,
        *,
        pull_requests: tuple[PullRequestRecord, ...],
    ) -> RawSnapshotWriteResult:
        return NormalizedRawSnapshotWriter().write(
            config,
            PullRequestCollection(
                window=config.collection_window,
                pull_requests=pull_requests,
                failures=(),
            ),
        )


@pytest.fixture(autouse=True)
def reset_settings_cache() -> Iterator[None]:
    """Clear cached settings around each test so env overrides remain deterministic."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def metric_harness(tmp_path: Path) -> MetricTestHarness:
    """Provide a reusable black-box harness for the public metrics pipeline."""
    return MetricTestHarness(output_dir=tmp_path)


@pytest.fixture
def pull_request_factory() -> Callable[..., PullRequestRecord]:
    """Build pull request records with stable defaults for metrics fixtures."""
    default_payload = {
        "repository_full_name": "acme/api",
        "number": 1,
        "title": "Default pull request",
        "state": "open",
        "draft": False,
        "merged": False,
        "author_login": "alice",
        "created_at": datetime.fromisoformat("2026-04-10T09:00:00"),
        "updated_at": datetime.fromisoformat("2026-04-10T12:00:00"),
        "closed_at": None,
        "merged_at": None,
        "additions": 8,
        "deletions": 2,
        "changed_files": 2,
        "commits": 1,
        "html_url": "https://example.test/pr/1",
        "reviews": (),
        "timeline_events": (),
    }

    def factory(**overrides: object) -> PullRequestRecord:
        payload = {**default_payload, **overrides}
        return PullRequestRecord.model_validate(payload)

    return factory


@pytest.fixture
def review_factory() -> Callable[..., PullRequestReviewRecord]:
    """Build review records with stable defaults for metrics fixtures."""
    default_payload = {
        "review_id": 1,
        "state": "APPROVED",
        "author_login": "reviewer-a",
        "submitted_at": datetime.fromisoformat("2026-04-10T12:00:00"),
        "commit_id": "commit-1",
    }

    def factory(**overrides: object) -> PullRequestReviewRecord:
        payload = {**default_payload, **overrides}
        return PullRequestReviewRecord.model_validate(payload)

    return factory


@pytest.fixture
def timeline_event_factory() -> Callable[..., PullRequestTimelineEventRecord]:
    """Build timeline events with stable defaults for metrics fixtures."""
    default_payload = {
        "event_id": 1,
        "event": "review_requested",
        "actor_login": "alice",
        "created_at": datetime.fromisoformat("2026-04-10T10:00:00"),
        "requested_reviewer_login": "reviewer-a",
        "requested_team_name": None,
    }

    def factory(**overrides: object) -> PullRequestTimelineEventRecord:
        payload = {**default_payload, **overrides}
        return PullRequestTimelineEventRecord.model_validate(payload)

    return factory
