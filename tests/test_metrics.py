from __future__ import annotations

from datetime import datetime

import pytest

from orgpulse.ingestion import NormalizedRawSnapshotWriter
from orgpulse.metrics import (
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
)
from orgpulse.models import (
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RunConfig,
    RunMode,
)


class TestPullRequestMetricCollectionBuilder:
    def test_builds_review_and_merge_timing_from_normalized_raw_snapshots(
        self,
        tmp_path,
    ) -> None:
        """Build timing facts from normalized raw snapshots for draft resets and review-request cycles."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            mode=RunMode.FULL,
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=21,
                    title="Stabilize review timing",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-06T18:30:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T18:30:00"),
                    merged_at=datetime.fromisoformat("2026-04-06T18:00:00"),
                    additions=30,
                    deletions=12,
                    changed_files=5,
                    commits=4,
                    html_url="https://example.test/pr/21",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=501,
                            state="APPROVED",
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-06T18:30:00"
                            ),
                            commit_id="commit-501",
                        ),
                    ),
                    timeline_events=(
                        PullRequestTimelineEventRecord(
                            event_id=601,
                            event="review_requested",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-05T10:00:00"),
                            requested_reviewer_login="reviewer-a",
                            requested_team_name=None,
                        ),
                        PullRequestTimelineEventRecord(
                            event_id=602,
                            event="review_request_removed",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-05T11:00:00"),
                            requested_reviewer_login="reviewer-a",
                            requested_team_name=None,
                        ),
                        PullRequestTimelineEventRecord(
                            event_id=603,
                            event="converted_to_draft",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-05T12:00:00"),
                            requested_reviewer_login=None,
                            requested_team_name=None,
                        ),
                        PullRequestTimelineEventRecord(
                            event_id=604,
                            event="ready_for_review",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-05T15:00:00"),
                            requested_reviewer_login=None,
                            requested_team_name=None,
                        ),
                        PullRequestTimelineEventRecord(
                            event_id=605,
                            event="review_requested",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-05T16:00:00"),
                            requested_reviewer_login="reviewer-b",
                            requested_team_name=None,
                        ),
                    ),
                ),
            ),
        )

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        assert [period.key for period in result.periods] == ["2026-04"]
        metric = result.periods[0].pull_request_metrics[0]
        assert metric.review_ready_at == datetime.fromisoformat("2026-04-05T15:00:00")
        assert metric.review_requested_at == datetime.fromisoformat(
            "2026-04-05T16:00:00"
        )
        assert metric.review_started_at == datetime.fromisoformat(
            "2026-04-05T16:00:00"
        )
        assert metric.first_review_submitted_at == datetime.fromisoformat(
            "2026-04-06T18:30:00"
        )
        assert metric.time_to_first_review_seconds == 95_400
        assert metric.time_to_merge_seconds == 118_800
        assert metric.changed_lines == 42

    def test_normalizes_missing_review_and_merge_data_to_null(
        self,
        tmp_path,
    ) -> None:
        """Normalize missing review and merge timing inputs to null instead of zero-like values."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=34,
                    title="Leave timing fields empty",
                    state="open",
                    draft=False,
                    merged=False,
                    author_login=None,
                    created_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-12T11:00:00"),
                    closed_at=None,
                    merged_at=None,
                    additions=8,
                    deletions=3,
                    changed_files=2,
                    commits=1,
                    html_url="https://example.test/pr/34",
                ),
            ),
        )

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        metric = result.periods[0].pull_request_metrics[0]
        assert metric.author_login is None
        assert metric.review_ready_at == datetime.fromisoformat("2026-04-10T09:00:00")
        assert metric.review_requested_at is None
        assert metric.review_started_at == datetime.fromisoformat("2026-04-10T09:00:00")
        assert metric.first_review_submitted_at is None
        assert metric.time_to_first_review_seconds is None
        assert metric.merged_at is None
        assert metric.time_to_merge_seconds is None

    def test_ignores_reviews_that_arrive_before_the_normalized_review_window(
        self,
        tmp_path,
    ) -> None:
        """Ignore reviews that land before a draft PR becomes reviewable again."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=44,
                    title="Skip early draft review",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    additions=10,
                    deletions=2,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/44",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=701,
                            state="COMMENTED",
                            author_login="reviewer-a",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-10T10:00:00"
                            ),
                            commit_id="commit-701",
                        ),
                        PullRequestReviewRecord(
                            review_id=702,
                            state="APPROVED",
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-11T12:00:00"
                            ),
                            commit_id="commit-702",
                        ),
                    ),
                    timeline_events=(
                        PullRequestTimelineEventRecord(
                            event_id=801,
                            event="converted_to_draft",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-10T09:30:00"),
                            requested_reviewer_login=None,
                            requested_team_name=None,
                        ),
                        PullRequestTimelineEventRecord(
                            event_id=802,
                            event="ready_for_review",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                            requested_reviewer_login=None,
                            requested_team_name=None,
                        ),
                    ),
                ),
            ),
        )

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        metric = result.periods[0].pull_request_metrics[0]
        assert metric.review_ready_at == datetime.fromisoformat("2026-04-11T09:00:00")
        assert metric.first_review_submitted_at == datetime.fromisoformat(
            "2026-04-11T12:00:00"
        )
        assert metric.time_to_first_review_seconds == 10_800

    def test_keeps_review_timing_null_for_pull_requests_created_as_drafts(
        self,
        tmp_path,
    ) -> None:
        """Keep review timing null until a draft-created pull request becomes reviewable."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=45,
                    title="Start life as a draft",
                    state="open",
                    draft=True,
                    merged=False,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-12T11:00:00"),
                    closed_at=None,
                    merged_at=None,
                    additions=6,
                    deletions=1,
                    changed_files=1,
                    commits=1,
                    html_url="https://example.test/pr/45",
                ),
            ),
        )

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        metric = result.periods[0].pull_request_metrics[0]
        assert metric.review_ready_at is None
        assert metric.review_requested_at is None
        assert metric.review_started_at is None
        assert metric.first_review_submitted_at is None
        assert metric.time_to_first_review_seconds is None

    def test_ignores_pre_ready_reviews_for_pull_requests_opened_as_drafts(
        self,
        tmp_path,
    ) -> None:
        """Ignore reviews that arrive before the first ready transition for a draft-created pull request."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=46,
                    title="Become ready later",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    additions=9,
                    deletions=2,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/46",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=703,
                            state="COMMENTED",
                            author_login="reviewer-a",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-10T10:00:00"
                            ),
                            commit_id="commit-703",
                        ),
                        PullRequestReviewRecord(
                            review_id=704,
                            state="APPROVED",
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-11T12:00:00"
                            ),
                            commit_id="commit-704",
                        ),
                    ),
                    timeline_events=(
                        PullRequestTimelineEventRecord(
                            event_id=803,
                            event="ready_for_review",
                            actor_login="alice",
                            created_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                            requested_reviewer_login=None,
                            requested_team_name=None,
                        ),
                    ),
                ),
            ),
        )

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        metric = result.periods[0].pull_request_metrics[0]
        assert metric.review_ready_at == datetime.fromisoformat("2026-04-11T09:00:00")
        assert metric.review_started_at == datetime.fromisoformat(
            "2026-04-11T09:00:00"
        )
        assert metric.first_review_submitted_at == datetime.fromisoformat(
            "2026-04-11T12:00:00"
        )
        assert metric.time_to_first_review_seconds == 10_800

    def test_preserves_incremental_period_scope_from_raw_snapshot_output(
        self,
        tmp_path,
    ) -> None:
        """Preserve incremental run scope by building metrics only for the active raw snapshot period."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            mode=RunMode.INCREMENTAL,
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=55,
                    title="Current-period refresh only",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    additions=4,
                    deletions=1,
                    changed_files=1,
                    commits=1,
                    html_url="https://example.test/pr/55",
                ),
            ),
        )

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        assert [period.key for period in result.periods] == ["2026-04"]
        assert result.periods[0].closed is False
        assert [metric.pull_request_number for metric in result.periods[0].pull_request_metrics] == [55]

    def test_builds_empty_backfill_periods_from_header_only_raw_snapshots(
        self,
        tmp_path,
    ) -> None:
        """Build empty metric periods for requested backfill windows even when raw snapshots have only headers."""
        # Given
        config = self._build_run_config(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(config, pull_requests=())

        # When
        result = PullRequestMetricCollectionBuilder().build(config, raw_snapshot)

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert result.periods[0].closed is True
        assert result.periods[1].closed is True
        assert result.periods[0].pull_request_metrics == ()
        assert result.periods[1].pull_request_metrics == ()

    def _build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for metric integration tests."""
        return RunConfig.model_validate({"org": "acme", **overrides})

    def _write_raw_snapshot(
        self,
        config: RunConfig,
        *,
        pull_requests: tuple[PullRequestRecord, ...],
    ):
        """Write raw snapshot fixtures through the production snapshot writer."""
        return NormalizedRawSnapshotWriter().write(
            config,
            PullRequestCollection(
                window=config.collection_window,
                pull_requests=pull_requests,
                failures=(),
            ),
        )


class TestOrganizationMetricCollectionBuilder:
    def test_builds_org_rollups_across_multiple_repositories(
        self,
        tmp_path,
    ) -> None:
        """Build org-level rollups from multiple repository metric fact streams in the same period."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=21,
                    title="Ship API endpoint",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="Alice",
                    created_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    additions=10,
                    deletions=4,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/21",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=501,
                            state="APPROVED",
                            author_login="reviewer-a",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-05T12:00:00"
                            ),
                            commit_id="commit-501",
                        ),
                    ),
                ),
                PullRequestRecord(
                    repository_full_name="acme/web",
                    number=31,
                    title="Ship dashboard layout",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-08T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-09T13:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-09T13:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-09T12:00:00"),
                    additions=20,
                    deletions=10,
                    changed_files=4,
                    commits=5,
                    html_url="https://example.test/pr/31",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=601,
                            state="APPROVED",
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-08T15:00:00"
                            ),
                            commit_id="commit-601",
                        ),
                    ),
                ),
                PullRequestRecord(
                    repository_full_name="acme/docs",
                    number=41,
                    title="Clarify onboarding steps",
                    state="open",
                    draft=False,
                    merged=False,
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-10T16:00:00"),
                    closed_at=None,
                    merged_at=None,
                    additions=5,
                    deletions=1,
                    changed_files=1,
                    commits=1,
                    html_url="https://example.test/pr/41",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=701,
                            state="COMMENTED",
                            author_login="reviewer-c",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-10T11:00:00"
                            ),
                            commit_id="commit-701",
                        ),
                    ),
                ),
            ),
        )
        pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            config,
            raw_snapshot,
        )

        # When
        result = OrganizationMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )

        # Then
        assert result.target_org == "acme"
        assert [period.key for period in result.periods] == ["2026-04"]
        summary = result.periods[0].summary
        assert summary.repository_count == 3
        assert summary.pull_request_count == 3
        assert summary.merged_pull_request_count == 2
        assert summary.active_author_count == 2
        assert summary.merged_pull_requests_per_active_author == 1.0
        assert summary.time_to_merge_seconds.count == 2
        assert summary.time_to_merge_seconds.total == 194_400
        assert summary.time_to_merge_seconds.average == 97_200.0
        assert summary.time_to_merge_seconds.median == 97_200.0
        assert summary.time_to_first_review_seconds.count == 3
        assert summary.time_to_first_review_seconds.total == 39_600
        assert summary.time_to_first_review_seconds.average == 13_200.0
        assert summary.time_to_first_review_seconds.median == 10_800.0
        assert summary.additions.total == 35
        assert summary.additions.average == pytest.approx(35 / 3)
        assert summary.additions.median == 10.0
        assert summary.changed_lines.total == 50
        assert summary.changed_lines.average == pytest.approx(50 / 3)
        assert summary.changed_lines.median == 14.0
        assert summary.commits.total == 8
        assert summary.commits.average == pytest.approx(8 / 3)
        assert summary.commits.median == 2.0

    def test_preserves_empty_backfill_periods_in_org_rollups(
        self,
        tmp_path,
    ) -> None:
        """Preserve explicit backfill periods in org rollups even when the normalized snapshot only contains headers."""
        # Given
        config = self._build_run_config(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(config, pull_requests=())
        pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            config,
            raw_snapshot,
        )

        # When
        result = OrganizationMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert result.periods[0].closed is True
        assert result.periods[1].closed is True
        assert result.periods[0].summary.repository_count == 0
        assert result.periods[0].summary.pull_request_count == 0
        assert result.periods[0].summary.merged_pull_request_count == 0
        assert result.periods[0].summary.active_author_count == 0
        assert result.periods[0].summary.merged_pull_requests_per_active_author is None
        assert result.periods[0].summary.time_to_merge_seconds.average is None
        assert result.periods[1].summary.repository_count == 0
        assert result.periods[1].summary.pull_request_count == 0
        assert result.periods[1].summary.time_to_first_review_seconds.median is None

    def _build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for org rollup integration tests."""
        return RunConfig.model_validate({"org": "acme", **overrides})

    def _write_raw_snapshot(
        self,
        config: RunConfig,
        *,
        pull_requests: tuple[PullRequestRecord, ...],
    ):
        """Write raw snapshot fixtures through the production snapshot writer."""
        return NormalizedRawSnapshotWriter().write(
            config,
            PullRequestCollection(
                window=config.collection_window,
                pull_requests=pull_requests,
                failures=(),
            ),
        )
