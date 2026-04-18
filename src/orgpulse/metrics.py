from __future__ import annotations

import csv
from datetime import datetime
from statistics import fmean, median

from orgpulse.models import (
    MetricValueSummary,
    OrganizationMetricCollection,
    OrganizationMetricPeriod,
    OrganizationMetricRollup,
    PullRequestMetricCollection,
    PullRequestMetricPeriod,
    PullRequestMetricRecord,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    RunConfig,
)

PullRequestKey = tuple[str, int]


class PullRequestMetricCollectionBuilder:
    """Build per-PR metric facts from the normalized raw snapshot layer."""

    def build(
        self,
        config: RunConfig,
        raw_snapshot: RawSnapshotWriteResult,
    ) -> PullRequestMetricCollection:
        return PullRequestMetricCollection(
            periods=tuple(
                self._build_metric_period(config, snapshot_period)
                for snapshot_period in raw_snapshot.periods
            )
        )

    def _build_metric_period(
        self,
        config: RunConfig,
        snapshot_period: RawSnapshotPeriod,
    ) -> PullRequestMetricPeriod:
        pull_requests = self._load_pull_requests(snapshot_period)
        return PullRequestMetricPeriod(
            key=snapshot_period.key,
            start_date=snapshot_period.start_date,
            end_date=snapshot_period.end_date,
            closed=snapshot_period.end_date < config.active_period.start_date,
            pull_request_metrics=tuple(
                self._build_metric_record(snapshot_period.key, pull_request)
                for pull_request in pull_requests
            ),
        )

    def _load_pull_requests(
        self,
        snapshot_period: RawSnapshotPeriod,
    ) -> tuple[PullRequestRecord, ...]:
        review_index = self._load_review_index(snapshot_period)
        timeline_event_index = self._load_timeline_event_index(snapshot_period)
        pull_requests = [
            self._build_pull_request_record(
                row,
                review_index=review_index,
                timeline_event_index=timeline_event_index,
            )
            for row in self._read_rows(snapshot_period.pull_requests_path)
        ]
        return tuple(
            sorted(
                pull_requests,
                key=lambda pull_request: (
                    pull_request.repository_full_name,
                    pull_request.updated_at,
                    pull_request.number,
                ),
            )
        )

    def _load_review_index(
        self,
        snapshot_period: RawSnapshotPeriod,
    ) -> dict[PullRequestKey, tuple[PullRequestReviewRecord, ...]]:
        reviews_by_pull_request: dict[PullRequestKey, list[PullRequestReviewRecord]] = {}
        for row in self._read_rows(snapshot_period.reviews_path):
            pull_request_key = self._pull_request_key(row)
            reviews_by_pull_request.setdefault(pull_request_key, []).append(
                PullRequestReviewRecord(
                    review_id=int(row["review_id"]),
                    state=row["state"],
                    author_login=self._optional_str(row["author_login"]),
                    submitted_at=self._optional_datetime(row["submitted_at"]),
                    commit_id=self._optional_str(row["commit_id"]),
                )
            )
        return {
            pull_request_key: tuple(
                sorted(
                    reviews,
                    key=lambda review: (
                        review.submitted_at.isoformat() if review.submitted_at else "",
                        review.review_id,
                    ),
                )
            )
            for pull_request_key, reviews in reviews_by_pull_request.items()
        }

    def _load_timeline_event_index(
        self,
        snapshot_period: RawSnapshotPeriod,
    ) -> dict[PullRequestKey, tuple[PullRequestTimelineEventRecord, ...]]:
        timeline_events_by_pull_request: dict[
            PullRequestKey, list[PullRequestTimelineEventRecord]
        ] = {}
        for row in self._read_rows(snapshot_period.timeline_events_path):
            pull_request_key = self._pull_request_key(row)
            timeline_events_by_pull_request.setdefault(pull_request_key, []).append(
                PullRequestTimelineEventRecord(
                    event_id=int(row["event_id"]),
                    event=row["event"],
                    actor_login=self._optional_str(row["actor_login"]),
                    created_at=self._optional_datetime(row["created_at"]),
                    requested_reviewer_login=self._optional_str(
                        row["requested_reviewer_login"]
                    ),
                    requested_team_name=self._optional_str(row["requested_team_name"]),
                )
            )
        return {
            pull_request_key: tuple(
                sorted(
                    timeline_events,
                    key=lambda timeline_event: (
                        timeline_event.created_at.isoformat()
                        if timeline_event.created_at
                        else "",
                        timeline_event.event,
                        timeline_event.event_id,
                    ),
                )
            )
            for pull_request_key, timeline_events in timeline_events_by_pull_request.items()
        }

    def _build_pull_request_record(
        self,
        row: dict[str, str],
        *,
        review_index: dict[PullRequestKey, tuple[PullRequestReviewRecord, ...]],
        timeline_event_index: dict[
            PullRequestKey, tuple[PullRequestTimelineEventRecord, ...]
        ],
    ) -> PullRequestRecord:
        pull_request_key = self._pull_request_key(row)
        return PullRequestRecord(
            repository_full_name=row["repository_full_name"],
            number=int(row["pull_request_number"]),
            title=row["title"],
            state=row["state"],
            draft=self._parse_bool(row["draft"]),
            merged=self._parse_bool(row["merged"]),
            author_login=self._optional_str(row["author_login"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            closed_at=self._optional_datetime(row["closed_at"]),
            merged_at=self._optional_datetime(row["merged_at"]),
            additions=int(row["additions"]),
            deletions=int(row["deletions"]),
            changed_files=int(row["changed_files"]),
            commits=int(row["commits"]),
            html_url=row["html_url"],
            reviews=review_index.get(pull_request_key, ()),
            timeline_events=timeline_event_index.get(pull_request_key, ()),
        )

    def _build_metric_record(
        self,
        period_key: str,
        pull_request: PullRequestRecord,
    ) -> PullRequestMetricRecord:
        (
            review_ready_at,
            review_requested_at,
            review_started_at,
            first_review_submitted_at,
        ) = self._review_timing_state(pull_request)
        return PullRequestMetricRecord(
            period_key=period_key,
            repository_full_name=pull_request.repository_full_name,
            pull_request_number=pull_request.number,
            author_login=pull_request.author_login,
            merged=pull_request.merged,
            created_at=pull_request.created_at,
            review_ready_at=review_ready_at,
            review_requested_at=review_requested_at,
            review_started_at=review_started_at,
            first_review_submitted_at=first_review_submitted_at,
            time_to_first_review_seconds=self._seconds_between(
                review_started_at,
                first_review_submitted_at,
            ),
            merged_at=pull_request.merged_at,
            time_to_merge_seconds=self._time_to_merge_seconds(pull_request),
            additions=pull_request.additions,
            deletions=pull_request.deletions,
            changed_lines=pull_request.additions + pull_request.deletions,
            changed_files=pull_request.changed_files,
            commits=pull_request.commits,
        )

    def _review_timing_state(
        self,
        pull_request: PullRequestRecord,
    ) -> tuple[datetime | None, datetime | None, datetime | None, datetime | None]:
        for review_submitted_at in self._review_submission_times(pull_request):
            review_ready_at = self._review_ready_at(pull_request, review_submitted_at)
            review_requested_at = self._review_requested_at(
                pull_request,
                review_submitted_at,
            )
            review_started_at = self._review_started_at(
                review_ready_at,
                review_requested_at,
            )
            if self._seconds_between(review_started_at, review_submitted_at) is None:
                continue
            return (
                review_ready_at,
                review_requested_at,
                review_started_at,
                review_submitted_at,
            )

        reference_at = pull_request.updated_at
        review_ready_at = self._review_ready_at(pull_request, reference_at)
        review_requested_at = self._review_requested_at(pull_request, reference_at)
        review_started_at = self._review_started_at(
            review_ready_at,
            review_requested_at,
        )
        return review_ready_at, review_requested_at, review_started_at, None

    def _review_submission_times(
        self,
        pull_request: PullRequestRecord,
    ) -> tuple[datetime, ...]:
        submission_times: list[datetime] = []
        for review in pull_request.reviews:
            if review.submitted_at is None:
                continue
            if (
                pull_request.author_login is not None
                and review.author_login == pull_request.author_login
            ):
                continue
            submission_times.append(review.submitted_at)
        return tuple(submission_times)

    def _review_ready_at(
        self,
        pull_request: PullRequestRecord,
        reference_at: datetime,
    ) -> datetime | None:
        review_ready_at = self._initial_review_ready_at(pull_request)
        for timeline_event in pull_request.timeline_events:
            if timeline_event.created_at is None:
                continue
            if timeline_event.created_at > reference_at:
                break
            if timeline_event.event == "converted_to_draft":
                review_ready_at = None
            elif timeline_event.event == "ready_for_review":
                review_ready_at = timeline_event.created_at
        return review_ready_at

    def _initial_review_ready_at(
        self,
        pull_request: PullRequestRecord,
    ) -> datetime | None:
        first_transition_event = self._first_draft_transition_event(pull_request)
        if first_transition_event == "ready_for_review":
            return None
        if first_transition_event == "converted_to_draft":
            return pull_request.created_at
        if pull_request.draft:
            return None
        return pull_request.created_at

    def _first_draft_transition_event(
        self,
        pull_request: PullRequestRecord,
    ) -> str | None:
        for timeline_event in pull_request.timeline_events:
            if timeline_event.created_at is None:
                continue
            if timeline_event.event in {"converted_to_draft", "ready_for_review"}:
                return timeline_event.event
        return None

    def _review_requested_at(
        self,
        pull_request: PullRequestRecord,
        reference_at: datetime,
    ) -> datetime | None:
        active_requests: set[str] = set()
        review_requested_at: datetime | None = None
        for timeline_event in pull_request.timeline_events:
            if timeline_event.created_at is None:
                continue
            if timeline_event.created_at > reference_at:
                break
            if timeline_event.event == "converted_to_draft":
                active_requests.clear()
                review_requested_at = None
                continue
            if timeline_event.event == "review_requested":
                request_key = self._request_key(timeline_event)
                if request_key in active_requests:
                    continue
                if not active_requests:
                    review_requested_at = timeline_event.created_at
                active_requests.add(request_key)
                continue
            if timeline_event.event == "review_request_removed":
                request_key = self._request_key(timeline_event)
                active_requests.discard(request_key)
                if not active_requests:
                    review_requested_at = None
        return review_requested_at

    def _review_started_at(
        self,
        review_ready_at: datetime | None,
        review_requested_at: datetime | None,
    ) -> datetime | None:
        if review_ready_at is None:
            return None
        if review_requested_at is None:
            return review_ready_at
        return max(review_ready_at, review_requested_at)

    def _time_to_merge_seconds(
        self,
        pull_request: PullRequestRecord,
    ) -> int | None:
        if not pull_request.merged:
            return None
        return self._seconds_between(pull_request.created_at, pull_request.merged_at)

    def _seconds_between(
        self,
        start_at: datetime | None,
        end_at: datetime | None,
    ) -> int | None:
        if start_at is None or end_at is None:
            return None
        if end_at < start_at:
            return None
        return int((end_at - start_at).total_seconds())

    def _request_key(
        self,
        timeline_event: PullRequestTimelineEventRecord,
    ) -> str:
        if timeline_event.requested_reviewer_login is not None:
            return f"user:{timeline_event.requested_reviewer_login.lower()}"
        if timeline_event.requested_team_name is not None:
            return f"team:{timeline_event.requested_team_name.lower()}"
        return f"event:{timeline_event.event_id}"

    def _read_rows(
        self,
        path,
    ) -> list[dict[str, str]]:
        if not path.exists():
            return []
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def _pull_request_key(
        self,
        row: dict[str, str],
    ) -> PullRequestKey:
        return (row["repository_full_name"], int(row["pull_request_number"]))

    def _parse_bool(
        self,
        value: str,
    ) -> bool:
        return value.strip().lower() == "true"

    def _optional_datetime(
        self,
        value: str,
    ) -> datetime | None:
        normalized = self._optional_str(value)
        if normalized is None:
            return None
        return datetime.fromisoformat(normalized)

    def _optional_str(
        self,
        value: str,
    ) -> str | None:
        normalized = value.strip()
        if not normalized:
            return None
        return normalized


class OrganizationMetricCollectionBuilder:
    """Build org-level rollups from periodized pull request metric facts."""

    def build(
        self,
        config: RunConfig,
        pull_request_metrics: PullRequestMetricCollection,
    ) -> OrganizationMetricCollection:
        return OrganizationMetricCollection(
            target_org=config.org,
            periods=tuple(
                self._build_metric_period(metric_period)
                for metric_period in pull_request_metrics.periods
            ),
        )

    def _build_metric_period(
        self,
        metric_period: PullRequestMetricPeriod,
    ) -> OrganizationMetricPeriod:
        summary = self._build_rollup(metric_period.pull_request_metrics)
        return OrganizationMetricPeriod(
            key=metric_period.key,
            start_date=metric_period.start_date,
            end_date=metric_period.end_date,
            closed=metric_period.closed,
            summary=summary,
        )

    def _build_rollup(
        self,
        pull_request_metrics: tuple[PullRequestMetricRecord, ...],
    ) -> OrganizationMetricRollup:
        merged_pull_requests = tuple(
            metric for metric in pull_request_metrics if metric.merged
        )
        active_author_count = self._active_author_count(pull_request_metrics)
        merged_pull_request_count = len(merged_pull_requests)
        return OrganizationMetricRollup(
            repository_count=len(
                {metric.repository_full_name for metric in pull_request_metrics}
            ),
            pull_request_count=len(pull_request_metrics),
            merged_pull_request_count=merged_pull_request_count,
            active_author_count=active_author_count,
            merged_pull_requests_per_active_author=self._per_active_author(
                merged_pull_request_count,
                active_author_count,
            ),
            time_to_merge_seconds=self._build_summary(
                tuple(
                    metric.time_to_merge_seconds
                    for metric in merged_pull_requests
                    if metric.time_to_merge_seconds is not None
                )
            ),
            time_to_first_review_seconds=self._build_summary(
                tuple(
                    metric.time_to_first_review_seconds
                    for metric in pull_request_metrics
                    if metric.time_to_first_review_seconds is not None
                )
            ),
            additions=self._build_summary(
                tuple(metric.additions for metric in pull_request_metrics)
            ),
            deletions=self._build_summary(
                tuple(metric.deletions for metric in pull_request_metrics)
            ),
            changed_lines=self._build_summary(
                tuple(metric.changed_lines for metric in pull_request_metrics)
            ),
            changed_files=self._build_summary(
                tuple(metric.changed_files for metric in pull_request_metrics)
            ),
            commits=self._build_summary(
                tuple(metric.commits for metric in pull_request_metrics)
            ),
        )

    def _active_author_count(
        self,
        pull_request_metrics: tuple[PullRequestMetricRecord, ...],
    ) -> int:
        return len(
            {
                metric.author_login.lower()
                for metric in pull_request_metrics
                if metric.author_login is not None
            }
        )

    def _per_active_author(
        self,
        merged_pull_request_count: int,
        active_author_count: int,
    ) -> float | None:
        if active_author_count == 0:
            return None
        return merged_pull_request_count / active_author_count

    def _build_summary(
        self,
        values: tuple[int, ...],
    ) -> MetricValueSummary:
        if not values:
            return MetricValueSummary(
                count=0,
                total=0,
                average=None,
                median=None,
            )
        return MetricValueSummary(
            count=len(values),
            total=sum(values),
            average=float(fmean(values)),
            median=float(median(values)),
        )
