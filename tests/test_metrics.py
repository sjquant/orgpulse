from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pytest

from orgpulse.models import (
    OrganizationMetricCollection,
    PullRequestMetricCollection,
    PullRequestRecord,
    RawSnapshotWriteResult,
    RunMode,
)


@dataclass(frozen=True)
class MetricSummaryExpectation:
    """Describe an aggregate summary asserted by fixture-driven tests."""

    count: int
    total: int
    average: float | None
    median: float | None


@dataclass(frozen=True)
class ReviewTimingExpectation:
    """Describe the normalized timing facts expected for a pull request."""

    author_login: str | None
    review_ready_at: datetime | None
    review_requested_at: datetime | None
    review_started_at: datetime | None
    first_review_submitted_at: datetime | None
    merged_at: datetime | None
    time_to_first_review_seconds: int | None
    time_to_merge_seconds: int | None
    changed_lines: int


@dataclass(frozen=True)
class ReviewTimingCase:
    """Bundle a pull request fixture with its expected timing outputs."""

    pull_request: PullRequestRecord
    expected: ReviewTimingExpectation


@dataclass(frozen=True)
class RepositoryRollupExpectation:
    """Describe the repo-level aggregation expected for a fixture set."""

    repository_full_name: str
    pull_request_count: int
    merged_pull_request_count: int
    active_author_count: int
    merged_pull_requests_per_active_author: float | None
    time_to_merge_seconds: MetricSummaryExpectation
    time_to_first_review_seconds: MetricSummaryExpectation
    additions: MetricSummaryExpectation
    deletions: MetricSummaryExpectation
    changed_lines: MetricSummaryExpectation
    changed_files: MetricSummaryExpectation
    commits: MetricSummaryExpectation


@dataclass(frozen=True)
class OrganizationRollupExpectation:
    """Describe the org-level aggregation expected for a fixture set."""

    repository_count: int
    pull_request_count: int
    merged_pull_request_count: int
    active_author_count: int
    merged_pull_requests_per_active_author: float | None
    time_to_merge_seconds: MetricSummaryExpectation
    time_to_first_review_seconds: MetricSummaryExpectation
    additions: MetricSummaryExpectation
    deletions: MetricSummaryExpectation
    changed_lines: MetricSummaryExpectation
    changed_files: MetricSummaryExpectation
    commits: MetricSummaryExpectation


@dataclass(frozen=True)
class AggregationCase:
    """Bundle multi-PR fixtures with expected repo and org rollups."""

    pull_requests: tuple[PullRequestRecord, ...]
    expected_repositories: tuple[RepositoryRollupExpectation, ...]
    expected_org_summary: OrganizationRollupExpectation


@dataclass(frozen=True)
class ValidationCase:
    """Bundle metric outputs and expected validation results."""

    pipeline: object
    raw_snapshot: RawSnapshotWriteResult
    pull_request_metrics: PullRequestMetricCollection
    org_metrics: OrganizationMetricCollection
    expected_valid: bool
    expected_issue_codes: tuple[str, ...]


class TestPullRequestMetricCollectionBuilder:
    def test_normalizes_review_timing_cases(
        self,
        metric_harness,
        review_timing_case,
    ) -> None:
        """Normalize review timing fixtures through the public raw snapshot contract."""
        # Given
        pipeline = metric_harness.build_pipeline(
            pull_requests=(review_timing_case.pull_request,),
            as_of="2026-04-18",
            mode=RunMode.FULL,
        )

        # When
        period = pipeline.pull_request_metrics.periods[0]
        metric = period.pull_request_metrics[0]

        # Then
        assert [metric_period.key for metric_period in pipeline.pull_request_metrics.periods] == [
            "2026-04"
        ]
        assert period.closed is False
        assert metric.author_login == review_timing_case.expected.author_login
        assert metric.review_ready_at == review_timing_case.expected.review_ready_at
        assert metric.review_requested_at == review_timing_case.expected.review_requested_at
        assert metric.review_started_at == review_timing_case.expected.review_started_at
        assert (
            metric.first_review_submitted_at
            == review_timing_case.expected.first_review_submitted_at
        )
        assert metric.merged_at == review_timing_case.expected.merged_at
        assert (
            metric.time_to_first_review_seconds
            == review_timing_case.expected.time_to_first_review_seconds
        )
        assert metric.time_to_merge_seconds == review_timing_case.expected.time_to_merge_seconds
        assert metric.changed_lines == review_timing_case.expected.changed_lines

    def test_preserves_incremental_period_scope_from_raw_snapshot_output(
        self,
        metric_harness,
        pull_request_factory,
    ) -> None:
        """Preserve incremental run scope by building metrics only for the active raw snapshot period."""
        # Given
        pipeline = metric_harness.build_pipeline(
            as_of="2026-04-18",
            mode=RunMode.INCREMENTAL,
            pull_requests=(
                pull_request_factory(
                    number=55,
                    title="Current-period refresh only",
                    state="closed",
                    merged=True,
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
        result = pipeline.pull_request_metrics

        # Then
        assert [period.key for period in result.periods] == ["2026-04"]
        assert result.periods[0].closed is False
        assert [
            metric.pull_request_number for metric in result.periods[0].pull_request_metrics
        ] == [55]

    def test_builds_empty_backfill_periods_from_header_only_raw_snapshots(
        self,
        metric_harness,
    ) -> None:
        """Build empty metric periods for requested backfill windows even when raw snapshots have only headers."""
        # Given
        pipeline = metric_harness.build_pipeline(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            pull_requests=(),
        )

        # When
        result = pipeline.pull_request_metrics

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert result.periods[0].closed is True
        assert result.periods[1].closed is True
        assert result.periods[0].pull_request_metrics == ()
        assert result.periods[1].pull_request_metrics == ()


class TestRepositoryMetricCollectionBuilder:
    def test_builds_repository_rollups_from_fixture_matrix(
        self,
        metric_harness,
        aggregation_case,
    ) -> None:
        """Build repo rollups from fixture-driven pull request aggregates."""
        # Given
        pipeline = metric_harness.build_pipeline(
            as_of="2026-04-18",
            pull_requests=aggregation_case.pull_requests,
        )

        # When
        repositories = pipeline.repository_metrics.periods[0].repositories

        # Then
        assert [rollup.repository_full_name for rollup in repositories] == [
            expectation.repository_full_name
            for expectation in aggregation_case.expected_repositories
        ]
        for rollup, expectation in zip(
            repositories,
            aggregation_case.expected_repositories,
            strict=True,
        ):
            assert rollup.pull_request_count == expectation.pull_request_count
            assert (
                rollup.merged_pull_request_count
                == expectation.merged_pull_request_count
            )
            assert rollup.active_author_count == expectation.active_author_count
            assert (
                rollup.merged_pull_requests_per_active_author
                == expectation.merged_pull_requests_per_active_author
            )
            _assert_metric_summary(
                rollup.time_to_merge_seconds,
                expectation.time_to_merge_seconds,
            )
            _assert_metric_summary(
                rollup.time_to_first_review_seconds,
                expectation.time_to_first_review_seconds,
            )
            _assert_metric_summary(rollup.additions, expectation.additions)
            _assert_metric_summary(rollup.deletions, expectation.deletions)
            _assert_metric_summary(rollup.changed_lines, expectation.changed_lines)
            _assert_metric_summary(rollup.changed_files, expectation.changed_files)
            _assert_metric_summary(rollup.commits, expectation.commits)


class TestOrganizationMetricCollectionBuilder:
    def test_builds_org_rollups_from_fixture_matrix(
        self,
        metric_harness,
        aggregation_case,
    ) -> None:
        """Build org rollups from the same fixture-driven aggregate inputs."""
        # Given
        pipeline = metric_harness.build_pipeline(
            as_of="2026-04-18",
            pull_requests=aggregation_case.pull_requests,
        )

        # When
        summary = pipeline.org_metrics.periods[0].summary

        # Then
        assert pipeline.org_metrics.target_org == "acme"
        assert [period.key for period in pipeline.org_metrics.periods] == ["2026-04"]
        assert summary.repository_count == aggregation_case.expected_org_summary.repository_count
        assert summary.pull_request_count == aggregation_case.expected_org_summary.pull_request_count
        assert (
            summary.merged_pull_request_count
            == aggregation_case.expected_org_summary.merged_pull_request_count
        )
        assert summary.active_author_count == aggregation_case.expected_org_summary.active_author_count
        assert (
            summary.merged_pull_requests_per_active_author
            == aggregation_case.expected_org_summary.merged_pull_requests_per_active_author
        )
        _assert_metric_summary(
            summary.time_to_merge_seconds,
            aggregation_case.expected_org_summary.time_to_merge_seconds,
        )
        _assert_metric_summary(
            summary.time_to_first_review_seconds,
            aggregation_case.expected_org_summary.time_to_first_review_seconds,
        )
        _assert_metric_summary(
            summary.additions,
            aggregation_case.expected_org_summary.additions,
        )
        _assert_metric_summary(
            summary.deletions,
            aggregation_case.expected_org_summary.deletions,
        )
        _assert_metric_summary(
            summary.changed_lines,
            aggregation_case.expected_org_summary.changed_lines,
        )
        _assert_metric_summary(
            summary.changed_files,
            aggregation_case.expected_org_summary.changed_files,
        )
        _assert_metric_summary(
            summary.commits,
            aggregation_case.expected_org_summary.commits,
        )

    def test_preserves_empty_backfill_periods_in_org_rollups(
        self,
        metric_harness,
    ) -> None:
        """Preserve explicit backfill periods in org rollups even when the normalized snapshot only contains headers."""
        # Given
        pipeline = metric_harness.build_pipeline(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            pull_requests=(),
        )

        # When
        result = pipeline.org_metrics

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


class TestMetricValidationCollectionBuilder:
    def test_validates_clean_repo_and_org_totals_from_metric_outputs(
        self,
        metric_harness,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Validate raw counts plus repo and org totals for a clean fixture-driven snapshot."""
        # Given
        aggregation_case = _build_aggregation_case(
            "multi_repo_rollups",
            pull_request_factory=pull_request_factory,
            review_factory=review_factory,
        )
        pipeline = metric_harness.build_pipeline(
            as_of="2026-04-18",
            pull_requests=aggregation_case.pull_requests,
        )

        # When
        result = metric_harness.build_validation(pipeline)

        # Then
        assert result.target_org == "acme"
        assert [period.key for period in result.periods] == ["2026-04"]
        period = result.periods[0]
        assert period.valid is True
        assert period.raw_pull_request_count == 4
        assert period.raw_review_count == 3
        assert period.raw_timeline_event_count == 0
        assert [
            summary.repository_full_name for summary in period.repository_summaries
        ] == [
            "acme/api",
            "acme/docs",
            "acme/web",
        ]
        assert period.org_summary.repository_count == 3
        assert period.org_summary.pull_request_count == 4
        assert period.org_summary.merged_pull_request_count == 2
        assert period.org_summary.time_to_merge_count == 2
        assert period.org_summary.time_to_first_review_count == 3
        assert period.issues == ()

    def test_preserves_empty_backfill_periods_in_validation_results(
        self,
        metric_harness,
    ) -> None:
        """Preserve explicit empty backfill periods when validating historical windows."""
        # Given
        pipeline = metric_harness.build_pipeline(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            pull_requests=(),
        )

        # When
        result = metric_harness.build_validation(pipeline)

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert [period.closed for period in result.periods] == [True, True]
        assert [period.raw_pull_request_count for period in result.periods] == [0, 0]
        assert [period.org_summary.pull_request_count for period in result.periods] == [
            0,
            0,
        ]
        assert all(period.valid for period in result.periods)
        assert all(period.issues == () for period in result.periods)

    def test_flags_validation_edge_cases(
        self,
        metric_harness,
        validation_case,
    ) -> None:
        """Flag validation edge cases from fixture-driven public metric outputs."""
        # Given
        case = validation_case

        # When
        result = metric_harness.build_validation(
            case.pipeline,
            raw_snapshot=case.raw_snapshot,
            pull_request_metrics=case.pull_request_metrics,
            org_metrics=case.org_metrics,
        )

        # Then
        period = result.periods[0]
        assert period.valid is validation_case.expected_valid
        assert [issue.code for issue in period.issues] == list(
            validation_case.expected_issue_codes
        )


@pytest.fixture(
    params=(
        "draft_reset_request_cycle",
        "missing_review_merge_data",
        "ignore_pre_ready_review_after_draft_reset",
        "draft_created_stays_null",
        "ignore_pre_ready_review_for_draft_created_pr",
    )
)
def review_timing_case(
    request: pytest.FixtureRequest,
    pull_request_factory,
    review_factory,
    timeline_event_factory,
) -> ReviewTimingCase:
    """Build timing fixtures from a shared table of public PR inputs."""
    # Given
    case_name = str(request.param)

    # When
    case = _build_review_timing_case(
        case_name,
        pull_request_factory=pull_request_factory,
        review_factory=review_factory,
        timeline_event_factory=timeline_event_factory,
    )

    # Then
    return case


@pytest.fixture(params=("multi_repo_rollups", "no_active_authors"))
def aggregation_case(
    request: pytest.FixtureRequest,
    pull_request_factory,
    review_factory,
) -> AggregationCase:
    """Build aggregation fixtures from a shared table of PR collections."""
    # Given
    case_name = str(request.param)

    # When
    case = _build_aggregation_case(
        case_name,
        pull_request_factory=pull_request_factory,
        review_factory=review_factory,
    )

    # Then
    return case


@pytest.fixture(
    params=(
        "raw_count_mismatch",
        "review_start_missing",
        "merge_timing_mismatch",
        "org_rollup_mismatch",
    )
)
def validation_case(
    request: pytest.FixtureRequest,
    metric_harness,
    pull_request_factory,
    review_factory,
) -> ValidationCase:
    """Build validation fixtures from a shared table of output mutations."""
    # Given
    case_name = str(request.param)

    # When
    case = _build_validation_case(
        case_name,
        metric_harness=metric_harness,
        pull_request_factory=pull_request_factory,
        review_factory=review_factory,
    )

    # Then
    return case


def _assert_metric_summary(actual, expected: MetricSummaryExpectation) -> None:
    assert actual.count == expected.count
    assert actual.total == expected.total
    if expected.average is None:
        assert actual.average is None
    else:
        assert actual.average == pytest.approx(expected.average)
    if expected.median is None:
        assert actual.median is None
    else:
        assert actual.median == pytest.approx(expected.median)


def _build_review_timing_case(
    case_name: str,
    *,
    pull_request_factory,
    review_factory,
    timeline_event_factory,
) -> ReviewTimingCase:
    match case_name:
        case "draft_reset_request_cycle":
            return ReviewTimingCase(
                pull_request=pull_request_factory(
                    number=21,
                    title="Stabilize review timing",
                    state="closed",
                    merged=True,
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
                        review_factory(
                            review_id=501,
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-06T18:30:00"
                            ),
                            commit_id="commit-501",
                        ),
                    ),
                    timeline_events=(
                        timeline_event_factory(
                            event_id=601,
                            created_at=datetime.fromisoformat("2026-04-05T10:00:00"),
                            requested_reviewer_login="reviewer-a",
                        ),
                        timeline_event_factory(
                            event_id=602,
                            event="review_request_removed",
                            created_at=datetime.fromisoformat("2026-04-05T11:00:00"),
                            requested_reviewer_login="reviewer-a",
                        ),
                        timeline_event_factory(
                            event_id=603,
                            event="converted_to_draft",
                            created_at=datetime.fromisoformat("2026-04-05T12:00:00"),
                            requested_reviewer_login=None,
                        ),
                        timeline_event_factory(
                            event_id=604,
                            event="ready_for_review",
                            created_at=datetime.fromisoformat("2026-04-05T15:00:00"),
                            requested_reviewer_login=None,
                        ),
                        timeline_event_factory(
                            event_id=605,
                            created_at=datetime.fromisoformat("2026-04-05T16:00:00"),
                            requested_reviewer_login="reviewer-b",
                        ),
                    ),
                ),
                expected=ReviewTimingExpectation(
                    author_login="alice",
                    review_ready_at=datetime.fromisoformat("2026-04-05T15:00:00"),
                    review_requested_at=datetime.fromisoformat("2026-04-05T16:00:00"),
                    review_started_at=datetime.fromisoformat("2026-04-05T16:00:00"),
                    first_review_submitted_at=datetime.fromisoformat(
                        "2026-04-06T18:30:00"
                    ),
                    merged_at=datetime.fromisoformat("2026-04-06T18:00:00"),
                    time_to_first_review_seconds=95_400,
                    time_to_merge_seconds=118_800,
                    changed_lines=42,
                ),
            )
        case "missing_review_merge_data":
            return ReviewTimingCase(
                pull_request=pull_request_factory(
                    number=34,
                    title="Leave timing fields empty",
                    author_login=None,
                    updated_at=datetime.fromisoformat("2026-04-12T11:00:00"),
                    additions=8,
                    deletions=3,
                    changed_files=2,
                    html_url="https://example.test/pr/34",
                ),
                expected=ReviewTimingExpectation(
                    author_login=None,
                    review_ready_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    review_requested_at=None,
                    review_started_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    first_review_submitted_at=None,
                    merged_at=None,
                    time_to_first_review_seconds=None,
                    time_to_merge_seconds=None,
                    changed_lines=11,
                ),
            )
        case "ignore_pre_ready_review_after_draft_reset":
            return ReviewTimingCase(
                pull_request=pull_request_factory(
                    number=44,
                    title="Skip early draft review",
                    state="closed",
                    merged=True,
                    updated_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    additions=10,
                    deletions=2,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/44",
                    reviews=(
                        review_factory(
                            review_id=701,
                            state="COMMENTED",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-10T10:00:00"
                            ),
                            commit_id="commit-701",
                        ),
                        review_factory(
                            review_id=702,
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-11T12:00:00"
                            ),
                            commit_id="commit-702",
                        ),
                    ),
                    timeline_events=(
                        timeline_event_factory(
                            event_id=801,
                            event="converted_to_draft",
                            created_at=datetime.fromisoformat("2026-04-10T09:30:00"),
                            requested_reviewer_login=None,
                        ),
                        timeline_event_factory(
                            event_id=802,
                            event="ready_for_review",
                            created_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                            requested_reviewer_login=None,
                        ),
                    ),
                ),
                expected=ReviewTimingExpectation(
                    author_login="alice",
                    review_ready_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                    review_requested_at=None,
                    review_started_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                    first_review_submitted_at=datetime.fromisoformat(
                        "2026-04-11T12:00:00"
                    ),
                    merged_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    time_to_first_review_seconds=10_800,
                    time_to_merge_seconds=100_800,
                    changed_lines=12,
                ),
            )
        case "draft_created_stays_null":
            return ReviewTimingCase(
                pull_request=pull_request_factory(
                    number=45,
                    title="Start life as a draft",
                    draft=True,
                    additions=6,
                    deletions=1,
                    changed_files=1,
                    html_url="https://example.test/pr/45",
                ),
                expected=ReviewTimingExpectation(
                    author_login="alice",
                    review_ready_at=None,
                    review_requested_at=None,
                    review_started_at=None,
                    first_review_submitted_at=None,
                    merged_at=None,
                    time_to_first_review_seconds=None,
                    time_to_merge_seconds=None,
                    changed_lines=7,
                ),
            )
        case "ignore_pre_ready_review_for_draft_created_pr":
            return ReviewTimingCase(
                pull_request=pull_request_factory(
                    number=46,
                    title="Become ready later",
                    state="closed",
                    merged=True,
                    updated_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    additions=9,
                    deletions=2,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/46",
                    reviews=(
                        review_factory(
                            review_id=703,
                            state="COMMENTED",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-10T10:00:00"
                            ),
                            commit_id="commit-703",
                        ),
                        review_factory(
                            review_id=704,
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-11T12:00:00"
                            ),
                            commit_id="commit-704",
                        ),
                    ),
                    timeline_events=(
                        timeline_event_factory(
                            event_id=803,
                            event="ready_for_review",
                            created_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                            requested_reviewer_login=None,
                        ),
                    ),
                ),
                expected=ReviewTimingExpectation(
                    author_login="alice",
                    review_ready_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                    review_requested_at=None,
                    review_started_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                    first_review_submitted_at=datetime.fromisoformat(
                        "2026-04-11T12:00:00"
                    ),
                    merged_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                    time_to_first_review_seconds=10_800,
                    time_to_merge_seconds=100_800,
                    changed_lines=11,
                ),
            )
    raise AssertionError(f"unknown review timing case: {case_name}")


def _build_aggregation_case(
    case_name: str,
    *,
    pull_request_factory,
    review_factory,
) -> AggregationCase:
    match case_name:
        case "multi_repo_rollups":
            pull_requests = (
                pull_request_factory(
                    number=21,
                    title="Ship API endpoint",
                    state="closed",
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
                        review_factory(
                            review_id=501,
                            submitted_at=datetime.fromisoformat(
                                "2026-04-05T12:00:00"
                            ),
                            commit_id="commit-501",
                        ),
                    ),
                ),
                pull_request_factory(
                    number=22,
                    title="Refine API validation",
                    state="open",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-07T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-07T14:00:00"),
                    closed_at=None,
                    merged_at=None,
                    additions=6,
                    deletions=2,
                    changed_files=1,
                    commits=1,
                    html_url="https://example.test/pr/22",
                    reviews=(
                        review_factory(
                            review_id=502,
                            state="COMMENTED",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-07T11:00:00"
                            ),
                            commit_id="commit-502",
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/docs",
                    number=41,
                    title="Clarify onboarding steps",
                    author_login=None,
                    additions=5,
                    deletions=1,
                    changed_files=1,
                    html_url="https://example.test/pr/41",
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=31,
                    title="Ship dashboard layout",
                    state="closed",
                    merged=True,
                    author_login="bob",
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
                        review_factory(
                            review_id=601,
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-08T15:00:00"
                            ),
                            commit_id="commit-601",
                        ),
                    ),
                ),
            )
            return AggregationCase(
                pull_requests=pull_requests,
                expected_repositories=(
                    RepositoryRollupExpectation(
                        repository_full_name="acme/api",
                        pull_request_count=2,
                        merged_pull_request_count=1,
                        active_author_count=1,
                        merged_pull_requests_per_active_author=1.0,
                        time_to_merge_seconds=MetricSummaryExpectation(
                            count=1,
                            total=97_200,
                            average=97_200.0,
                            median=97_200.0,
                        ),
                        time_to_first_review_seconds=MetricSummaryExpectation(
                            count=2,
                            total=18_000,
                            average=9_000.0,
                            median=9_000.0,
                        ),
                        additions=MetricSummaryExpectation(2, 16, 8.0, 8.0),
                        deletions=MetricSummaryExpectation(2, 6, 3.0, 3.0),
                        changed_lines=MetricSummaryExpectation(2, 22, 11.0, 11.0),
                        changed_files=MetricSummaryExpectation(2, 3, 1.5, 1.5),
                        commits=MetricSummaryExpectation(2, 3, 1.5, 1.5),
                    ),
                    RepositoryRollupExpectation(
                        repository_full_name="acme/docs",
                        pull_request_count=1,
                        merged_pull_request_count=0,
                        active_author_count=0,
                        merged_pull_requests_per_active_author=None,
                        time_to_merge_seconds=MetricSummaryExpectation(0, 0, None, None),
                        time_to_first_review_seconds=MetricSummaryExpectation(
                            0,
                            0,
                            None,
                            None,
                        ),
                        additions=MetricSummaryExpectation(1, 5, 5.0, 5.0),
                        deletions=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                        changed_lines=MetricSummaryExpectation(1, 6, 6.0, 6.0),
                        changed_files=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                        commits=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                    ),
                    RepositoryRollupExpectation(
                        repository_full_name="acme/web",
                        pull_request_count=1,
                        merged_pull_request_count=1,
                        active_author_count=1,
                        merged_pull_requests_per_active_author=1.0,
                        time_to_merge_seconds=MetricSummaryExpectation(
                            count=1,
                            total=97_200,
                            average=97_200.0,
                            median=97_200.0,
                        ),
                        time_to_first_review_seconds=MetricSummaryExpectation(
                            count=1,
                            total=21_600,
                            average=21_600.0,
                            median=21_600.0,
                        ),
                        additions=MetricSummaryExpectation(1, 20, 20.0, 20.0),
                        deletions=MetricSummaryExpectation(1, 10, 10.0, 10.0),
                        changed_lines=MetricSummaryExpectation(1, 30, 30.0, 30.0),
                        changed_files=MetricSummaryExpectation(1, 4, 4.0, 4.0),
                        commits=MetricSummaryExpectation(1, 5, 5.0, 5.0),
                    ),
                ),
                expected_org_summary=OrganizationRollupExpectation(
                    repository_count=3,
                    pull_request_count=4,
                    merged_pull_request_count=2,
                    active_author_count=2,
                    merged_pull_requests_per_active_author=1.0,
                    time_to_merge_seconds=MetricSummaryExpectation(
                        count=2,
                        total=194_400,
                        average=97_200.0,
                        median=97_200.0,
                    ),
                    time_to_first_review_seconds=MetricSummaryExpectation(
                        count=3,
                        total=39_600,
                        average=13_200.0,
                        median=10_800.0,
                    ),
                    additions=MetricSummaryExpectation(4, 41, 10.25, 8.0),
                    deletions=MetricSummaryExpectation(4, 17, 4.25, 3.0),
                    changed_lines=MetricSummaryExpectation(4, 58, 14.5, 11.0),
                    changed_files=MetricSummaryExpectation(4, 8, 2.0, 1.5),
                    commits=MetricSummaryExpectation(4, 9, 2.25, 1.5),
                ),
            )
        case "no_active_authors":
            pull_requests = (
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=51,
                    author_login=None,
                    additions=4,
                    deletions=1,
                    changed_files=1,
                    html_url="https://example.test/pr/51",
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=61,
                    author_login=None,
                    additions=2,
                    deletions=2,
                    changed_files=1,
                    html_url="https://example.test/pr/61",
                ),
            )
            return AggregationCase(
                pull_requests=pull_requests,
                expected_repositories=(
                    RepositoryRollupExpectation(
                        repository_full_name="acme/api",
                        pull_request_count=1,
                        merged_pull_request_count=0,
                        active_author_count=0,
                        merged_pull_requests_per_active_author=None,
                        time_to_merge_seconds=MetricSummaryExpectation(0, 0, None, None),
                        time_to_first_review_seconds=MetricSummaryExpectation(
                            0,
                            0,
                            None,
                            None,
                        ),
                        additions=MetricSummaryExpectation(1, 4, 4.0, 4.0),
                        deletions=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                        changed_lines=MetricSummaryExpectation(1, 5, 5.0, 5.0),
                        changed_files=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                        commits=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                    ),
                    RepositoryRollupExpectation(
                        repository_full_name="acme/web",
                        pull_request_count=1,
                        merged_pull_request_count=0,
                        active_author_count=0,
                        merged_pull_requests_per_active_author=None,
                        time_to_merge_seconds=MetricSummaryExpectation(0, 0, None, None),
                        time_to_first_review_seconds=MetricSummaryExpectation(
                            0,
                            0,
                            None,
                            None,
                        ),
                        additions=MetricSummaryExpectation(1, 2, 2.0, 2.0),
                        deletions=MetricSummaryExpectation(1, 2, 2.0, 2.0),
                        changed_lines=MetricSummaryExpectation(1, 4, 4.0, 4.0),
                        changed_files=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                        commits=MetricSummaryExpectation(1, 1, 1.0, 1.0),
                    ),
                ),
                expected_org_summary=OrganizationRollupExpectation(
                    repository_count=2,
                    pull_request_count=2,
                    merged_pull_request_count=0,
                    active_author_count=0,
                    merged_pull_requests_per_active_author=None,
                    time_to_merge_seconds=MetricSummaryExpectation(0, 0, None, None),
                    time_to_first_review_seconds=MetricSummaryExpectation(
                        0,
                        0,
                        None,
                        None,
                    ),
                    additions=MetricSummaryExpectation(2, 6, 3.0, 3.0),
                    deletions=MetricSummaryExpectation(2, 3, 1.5, 1.5),
                    changed_lines=MetricSummaryExpectation(2, 9, 4.5, 4.5),
                    changed_files=MetricSummaryExpectation(2, 2, 1.0, 1.0),
                    commits=MetricSummaryExpectation(2, 2, 1.0, 1.0),
                ),
            )
    raise AssertionError(f"unknown aggregation case: {case_name}")


def _build_validation_case(
    case_name: str,
    *,
    metric_harness,
    pull_request_factory,
    review_factory,
) -> ValidationCase:
    pipeline = metric_harness.build_pipeline(
        as_of="2026-04-18",
        pull_requests=(
            pull_request_factory(
                number=51,
                title="Keep counts aligned",
                state="closed",
                merged=True,
                created_at=datetime.fromisoformat("2026-04-09T09:00:00"),
                updated_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                closed_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                merged_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                additions=4,
                deletions=1,
                changed_files=1,
                html_url="https://example.test/pr/51",
                reviews=(
                    review_factory(
                        review_id=801,
                        submitted_at=datetime.fromisoformat("2026-04-09T12:00:00"),
                        commit_id="commit-801",
                    ),
                ),
            ),
            pull_request_factory(
                repository_full_name="acme/web",
                number=61,
                title="Keep org rollups aligned",
                state="closed",
                merged=True,
                author_login="bob",
                created_at=datetime.fromisoformat("2026-04-11T09:00:00"),
                updated_at=datetime.fromisoformat("2026-04-12T13:00:00"),
                closed_at=datetime.fromisoformat("2026-04-12T13:00:00"),
                merged_at=datetime.fromisoformat("2026-04-12T12:00:00"),
                additions=9,
                deletions=2,
                changed_files=2,
                commits=2,
                html_url="https://example.test/pr/61",
                reviews=(
                    review_factory(
                        review_id=802,
                        author_login="reviewer-b",
                        submitted_at=datetime.fromisoformat("2026-04-11T13:00:00"),
                        commit_id="commit-802",
                    ),
                ),
            ),
        ),
    )
    match case_name:
        case "raw_count_mismatch":
            raw_period = pipeline.raw_snapshot.periods[0].model_copy(
                update={"pull_request_count": 3}
            )
            raw_snapshot = pipeline.raw_snapshot.model_copy(
                update={"periods": (raw_period,)}
            )
            return ValidationCase(
                pipeline=pipeline,
                raw_snapshot=raw_snapshot,
                pull_request_metrics=pipeline.pull_request_metrics,
                org_metrics=pipeline.org_metrics,
                expected_valid=False,
                expected_issue_codes=("raw_pull_request_count_mismatch",),
            )
        case "review_start_missing":
            pull_request_metrics = _replace_first_metric(
                pipeline.pull_request_metrics,
                review_started_at=None,
            )
            return ValidationCase(
                pipeline=pipeline,
                raw_snapshot=pipeline.raw_snapshot,
                pull_request_metrics=pull_request_metrics,
                org_metrics=pipeline.org_metrics,
                expected_valid=False,
                expected_issue_codes=("review_submission_missing_review_start",),
            )
        case "merge_timing_mismatch":
            pull_request_metrics = _replace_first_metric(
                pipeline.pull_request_metrics,
                time_to_merge_seconds=90_001,
            )
            return ValidationCase(
                pipeline=pipeline,
                raw_snapshot=pipeline.raw_snapshot,
                pull_request_metrics=pull_request_metrics,
                org_metrics=pipeline.org_metrics,
                expected_valid=False,
                expected_issue_codes=("merged_pr_merge_timing_mismatch",),
            )
        case "org_rollup_mismatch":
            org_period = pipeline.org_metrics.periods[0]
            org_metrics = pipeline.org_metrics.model_copy(
                update={
                    "periods": (
                        org_period.model_copy(
                            update={
                                "summary": org_period.summary.model_copy(
                                    update={
                                        "pull_request_count": org_period.summary.pull_request_count
                                        + 1
                                    }
                                )
                            }
                        ),
                    )
                }
            )
            return ValidationCase(
                pipeline=pipeline,
                raw_snapshot=pipeline.raw_snapshot,
                pull_request_metrics=pipeline.pull_request_metrics,
                org_metrics=org_metrics,
                expected_valid=False,
                expected_issue_codes=("pull_request_count_mismatch",),
            )
    raise AssertionError(f"unknown validation case: {case_name}")


def _replace_first_metric(
    pull_request_metrics: PullRequestMetricCollection,
    **updates: object,
) -> PullRequestMetricCollection:
    period = pull_request_metrics.periods[0]
    replacement_metric = period.pull_request_metrics[0].model_copy(update=updates)
    replacement_period = period.model_copy(
        update={
            "pull_request_metrics": (
                replacement_metric,
                *period.pull_request_metrics[1:],
            )
        }
    )
    return pull_request_metrics.model_copy(update={"periods": (replacement_period,)})
