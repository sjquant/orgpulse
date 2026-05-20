from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from orgpulse.models import (
    RawSnapshotPeriod,
    ReportingPeriod,
    RunManifest,
)
from orgpulse.raw_snapshot_source import read_snapshot_csv_rows

PullRequestKey = tuple[str, str]


@dataclass(frozen=True)
class ReviewFact:
    """Store one review row with the pull request context needed for person metrics."""

    repository_full_name: str
    pull_request_number: str
    state: str
    author_login: str | None
    submitted_at: datetime | None
    pull_request_author_login: str | None


@dataclass(frozen=True)
class TimelineEventFact:
    """Store one timeline event row needed for first-review latency."""

    event_id: str
    event: str
    created_at: datetime | None
    requested_reviewer_login: str | None
    requested_team_name: str | None


@dataclass(frozen=True)
class PullRequestFact:
    """Store one pull request row with attached review and timeline facts."""

    period_key: str
    repository_full_name: str
    pull_request_number: str
    title: str
    state: str
    draft: bool
    merged: bool
    author_login: str | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    merged_at: datetime | None
    additions: int
    deletions: int
    changed_files: int
    commits: int
    html_url: str
    reviews: tuple[ReviewFact, ...]
    timeline_events: tuple[TimelineEventFact, ...]

    @property
    def key(self) -> PullRequestKey:
        return self.repository_full_name, self.pull_request_number

    @property
    def changed_lines(self) -> int:
        return self.additions + self.deletions


class PersonSnapshotSource:
    """Load person metric facts from normalized local snapshot CSV files."""

    def load_snapshot_periods(
        self,
        manifest: RunManifest,
    ) -> tuple[RawSnapshotPeriod, ...]:
        period_index = {
            period.key: self._snapshot_period(manifest.raw_snapshot_root_dir, period)
            for period in (*manifest.locked_periods, *manifest.refreshed_periods)
        }
        return tuple(
            period_index[key]
            for key in sorted(
                period_index,
                key=lambda period_key: (
                    period_index[period_key].start_date,
                    period_key,
                ),
            )
        )

    def _snapshot_period(
        self,
        root_dir: Path,
        period: ReportingPeriod | RawSnapshotPeriod,
    ) -> RawSnapshotPeriod:
        period_dir = root_dir / period.key
        return RawSnapshotPeriod(
            key=period.key,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=period.closed,
            directory=period_dir,
            pull_requests_path=period_dir / "pull_requests.csv",
            pull_request_count=0,
            reviews_path=period_dir / "pull_request_reviews.csv",
            review_count=0,
            timeline_events_path=period_dir / "pull_request_timeline_events.csv",
            timeline_event_count=0,
        )

    def load_pull_requests(
        self,
        periods: tuple[RawSnapshotPeriod, ...],
    ) -> tuple[PullRequestFact, ...]:
        pull_requests: list[PullRequestFact] = []
        for period in periods:
            pull_request_rows = read_snapshot_csv_rows(period.pull_requests_path)
            review_index = self._review_index(period)
            timeline_event_index = self._timeline_event_index(period)
            for row in pull_request_rows:
                pull_request_key = self._pull_request_key(row)
                pull_requests.append(
                    self._pull_request_fact(
                        period_key=period.key,
                        row=row,
                        reviews=tuple(review_index.get(pull_request_key, ())),
                        timeline_events=tuple(
                            timeline_event_index.get(pull_request_key, ())
                        ),
                    )
                )
        return tuple(
            sorted(
                pull_requests,
                key=lambda pull_request: (
                    pull_request.created_at,
                    pull_request.repository_full_name,
                    pull_request.pull_request_number,
                ),
            )
        )

    def _review_index(
        self,
        period: RawSnapshotPeriod,
    ) -> dict[PullRequestKey, list[ReviewFact]]:
        pull_request_authors = {
            self._pull_request_key(row): self._optional_str(row["author_login"])
            for row in read_snapshot_csv_rows(period.pull_requests_path)
        }
        reviews_by_pull_request: dict[PullRequestKey, list[ReviewFact]] = defaultdict(
            list
        )
        for row in read_snapshot_csv_rows(period.reviews_path):
            pull_request_key = self._pull_request_key(row)
            reviews_by_pull_request[pull_request_key].append(
                ReviewFact(
                    repository_full_name=row["repository_full_name"],
                    pull_request_number=row["pull_request_number"],
                    state=row["state"],
                    author_login=self._optional_str(row["author_login"]),
                    submitted_at=self._optional_datetime(row["submitted_at"]),
                    pull_request_author_login=pull_request_authors.get(
                        pull_request_key
                    ),
                )
            )
        for reviews in reviews_by_pull_request.values():
            reviews.sort(
                key=lambda review: (
                    review.submitted_at.isoformat() if review.submitted_at else "",
                    review.author_login or "",
                    review.state,
                )
            )
        return reviews_by_pull_request

    def _timeline_event_index(
        self,
        period: RawSnapshotPeriod,
    ) -> dict[PullRequestKey, list[TimelineEventFact]]:
        events_by_pull_request: dict[PullRequestKey, list[TimelineEventFact]] = (
            defaultdict(list)
        )
        for row in read_snapshot_csv_rows(period.timeline_events_path):
            events_by_pull_request[self._pull_request_key(row)].append(
                TimelineEventFact(
                    event_id=row["event_id"],
                    event=row["event"],
                    created_at=self._optional_datetime(row["created_at"]),
                    requested_reviewer_login=self._optional_str(
                        row["requested_reviewer_login"]
                    ),
                    requested_team_name=self._optional_str(row["requested_team_name"]),
                )
            )
        for events in events_by_pull_request.values():
            events.sort(
                key=lambda event: (
                    event.created_at.isoformat() if event.created_at else "",
                    event.event,
                    event.event_id,
                )
            )
        return events_by_pull_request

    def _pull_request_fact(
        self,
        *,
        period_key: str,
        row: dict[str, str],
        reviews: tuple[ReviewFact, ...],
        timeline_events: tuple[TimelineEventFact, ...],
    ) -> PullRequestFact:
        return PullRequestFact(
            period_key=period_key,
            repository_full_name=row["repository_full_name"],
            pull_request_number=row["pull_request_number"],
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
            reviews=reviews,
            timeline_events=timeline_events,
        )

    def _pull_request_key(
        self,
        row: dict[str, str],
    ) -> PullRequestKey:
        return row["repository_full_name"], row["pull_request_number"]

    def _parse_bool(
        self,
        value: str,
    ) -> bool:
        return value.strip().lower() == "true"

    def _optional_datetime(
        self,
        value: str | None,
    ) -> datetime | None:
        normalized = self._optional_str(value)
        if normalized is None:
            return None
        return datetime.fromisoformat(normalized)

    def _optional_str(
        self,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        return normalized
