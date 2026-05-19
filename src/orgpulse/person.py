from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from io import StringIO
from pathlib import Path
from statistics import median
from typing import Annotated, Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup, escape
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

from orgpulse.config import get_settings
from orgpulse.distribution import trim_upper_tail, validate_distribution_percentile
from orgpulse.errors import AnalysisInputError
from orgpulse.models import (
    OrgSlug,
    PeriodGrain,
    RawSnapshotPeriod,
    ReportingPeriod,
    RepoSlug,
    RunManifest,
    TimeAnchor,
    canonicalize_repo_filter,
    repo_filter_matches,
)
from orgpulse.raw_snapshot_source import read_snapshot_csv_rows

Login = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
PullRequestKey = tuple[str, str]


class PersonExportFormat(StrEnum):
    """Enumerate supported person metric serialization formats."""

    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"
    HTML = "html"


class PersonConfig(BaseModel):
    """Capture validated settings for a person metrics extraction run."""

    model_config = ConfigDict(frozen=True)

    org: OrgSlug
    login: Login
    output_dir: Path = Field(default_factory=lambda: Path("output"))
    grain: PeriodGrain = PeriodGrain.MONTH
    time_anchor: TimeAnchor = TimeAnchor.CREATED_AT
    since: date | None = None
    until: date | None = None
    distribution_percentile: int = 100
    export_format: PersonExportFormat = PersonExportFormat.JSON
    include_repos: tuple[RepoSlug, ...] = ()
    exclude_repos: tuple[RepoSlug, ...] = ()

    @field_validator("include_repos", "exclude_repos", mode="before")
    @classmethod
    def normalize_repo_filters(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            items = [value]
        elif isinstance(value, Iterable):
            items = list(value)
        else:
            items = [value]

        org = info.data.get("org")
        deduped: list[str] = []
        seen: set[str] = set()
        for item in items:
            cleaned = str(item).strip()
            if not cleaned:
                continue
            canonical = canonicalize_repo_filter(cleaned, org=org)
            if canonical in seen:
                continue
            deduped.append(cleaned)
            seen.add(canonical)
        return tuple(deduped)

    @field_validator("output_dir", mode="before")
    @classmethod
    def normalize_output_dir(cls, value: object) -> Path:
        if value is None:
            return Path("output")
        return Path(str(value)).expanduser()

    @field_validator("until")
    @classmethod
    def validate_until(
        cls,
        value: date | None,
        info: ValidationInfo,
    ) -> date | None:
        since = info.data.get("since")
        if since is not None and value is not None and value < since:
            raise ValueError("--until must be on or after --since")
        return value

    @field_validator("distribution_percentile")
    @classmethod
    def validate_distribution_percentile_value(
        cls,
        value: int,
    ) -> int:
        return validate_distribution_percentile(value)

    @model_validator(mode="after")
    def validate_repo_filters(self) -> "PersonConfig":
        for repo_filter in (*self.include_repos, *self.exclude_repos):
            if "/" not in repo_filter:
                continue
            owner, _name = repo_filter.split("/", 1)
            if owner.lower() != self.org.lower():
                raise ValueError(
                    f"repo filter owner must match target org '{self.org}': {repo_filter}"
                )

        include_index = {
            canonicalize_repo_filter(repo_filter, org=self.org)
            for repo_filter in self.include_repos
        }
        overlapping = [
            repo_filter
            for repo_filter in self.exclude_repos
            if canonicalize_repo_filter(repo_filter, org=self.org) in include_index
        ]
        if overlapping:
            overlap = ", ".join(sorted(overlapping))
            raise ValueError(
                f"repo filters overlap across include and exclude lists: {overlap}"
            )
        return self


class PersonSummary(BaseModel):
    """Summarize authored pull request performance for one person."""

    model_config = ConfigDict(frozen=True)

    authored_pull_request_count: int
    merged_pull_request_count: int
    open_pull_request_count: int
    merge_rate_pct: float | None
    changed_lines_total: int
    commits_total: int
    reviews_received: int
    review_coverage_pct: float | None
    median_first_review_hours: float | None
    median_merge_hours: float | None


class PersonReviewerSummary(BaseModel):
    """Summarize review activity submitted by one person."""

    model_config = ConfigDict(frozen=True)

    review_submissions: int
    pull_requests_reviewed: int
    approvals: int
    changes_requested: int
    comments: int
    authors_supported: int
    repositories_reviewed: int


class PersonPeriodRow(BaseModel):
    """Store one period row for person performance tracking."""

    model_config = ConfigDict(frozen=True)

    period_key: str
    period_start_date: date
    period_end_date: date
    authored_pull_request_count: int
    merged_pull_request_count: int
    open_pull_request_count: int
    changed_lines_total: int
    commits_total: int
    reviews_received: int
    review_submissions_given: int
    pull_requests_reviewed: int
    approvals_given: int
    changes_requested_given: int
    comments_given: int


class PersonRepositoryRow(BaseModel):
    """Store one repository row for person performance tracking."""

    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    authored_pull_request_count: int
    merged_pull_request_count: int
    open_pull_request_count: int
    changed_lines_total: int
    commits_total: int
    reviews_received: int
    review_submissions_given: int
    pull_requests_reviewed: int


class PersonMetricsResult(BaseModel):
    """Store the fully rendered outcome of a person metrics extraction."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    login: str
    source_manifest_path: Path
    output_dir: Path
    grain: PeriodGrain
    time_anchor: TimeAnchor
    since: date | None
    until: date | None
    distribution_percentile: int
    summary: PersonSummary
    reviewer_summary: PersonReviewerSummary
    period_rows: tuple[PersonPeriodRow, ...]
    repository_rows: tuple[PersonRepositoryRow, ...]
    export_format: PersonExportFormat


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


class PersonMetricsService:
    """Build a focused person performance extract from normalized local snapshots."""

    def extract(
        self,
        config: PersonConfig,
    ) -> PersonMetricsResult:
        manifest_path, manifest = self._load_manifest(config)
        periods = self._load_snapshot_periods(manifest)
        pull_requests = self._filter_pull_requests_by_repository(
            config,
            self._load_pull_requests(periods),
        )
        authored_pull_requests = self._authored_pull_requests(config, pull_requests)
        review_submissions = self._review_submissions(config, pull_requests)
        period_rows = self._period_rows(
            config=config,
            periods=periods,
            authored_pull_requests=authored_pull_requests,
            review_submissions=review_submissions,
        )
        repository_rows = self._repository_rows(
            authored_pull_requests=authored_pull_requests,
            review_submissions=review_submissions,
        )
        return PersonMetricsResult(
            target_org=manifest.target_org,
            login=config.login,
            source_manifest_path=manifest_path,
            output_dir=config.output_dir,
            grain=config.grain,
            time_anchor=config.time_anchor,
            since=config.since,
            until=config.until,
            distribution_percentile=config.distribution_percentile,
            summary=self._summary(
                authored_pull_requests,
                distribution_percentile=config.distribution_percentile,
            ),
            reviewer_summary=self._reviewer_summary(review_submissions),
            period_rows=period_rows,
            repository_rows=repository_rows,
            export_format=config.export_format,
        )

    def _load_manifest(
        self,
        config: PersonConfig,
    ) -> tuple[Path, RunManifest]:
        manifest_path = (
            config.output_dir
            / "manifest"
            / config.grain.value
            / config.time_anchor.value
            / "manifest.json"
        )
        if not manifest_path.exists():
            raise AnalysisInputError(
                "person metrics input is missing: "
                f"{manifest_path}. Run `orgpulse run` for this grain and time anchor first."
            )
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AnalysisInputError(
                f"person metrics manifest is unreadable: {manifest_path}"
            ) from exc

        manifest = RunManifest.model_validate(payload)
        if manifest.target_org.lower() != config.org.lower():
            raise AnalysisInputError(
                "person metrics manifest org does not match the requested org: "
                f"expected {config.org}, found {manifest.target_org}"
            )
        if (
            config.until is not None
            and config.until > manifest.last_successful_run.as_of
        ):
            raise AnalysisInputError(
                "local person metrics source is stale for the requested window: "
                f"latest local as-of is {manifest.last_successful_run.as_of.isoformat()}, "
                f"but --until is {config.until.isoformat()}."
            )
        return manifest_path, manifest

    def _load_snapshot_periods(
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

    def _load_pull_requests(
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
        reviews_by_pull_request: dict[PullRequestKey, list[ReviewFact]] = defaultdict(list)
        for row in read_snapshot_csv_rows(period.reviews_path):
            pull_request_key = self._pull_request_key(row)
            reviews_by_pull_request[pull_request_key].append(
                ReviewFact(
                    repository_full_name=row["repository_full_name"],
                    pull_request_number=row["pull_request_number"],
                    state=row["state"],
                    author_login=self._optional_str(row["author_login"]),
                    submitted_at=self._optional_datetime(row["submitted_at"]),
                    pull_request_author_login=pull_request_authors.get(pull_request_key),
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
        events_by_pull_request: dict[PullRequestKey, list[TimelineEventFact]] = defaultdict(list)
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

    def _authored_pull_requests(
        self,
        config: PersonConfig,
        pull_requests: tuple[PullRequestFact, ...],
    ) -> tuple[PullRequestFact, ...]:
        login = config.login.lower()
        return tuple(
            pull_request
            for pull_request in pull_requests
            if (pull_request.author_login or "").lower() == login
            and self._in_window(
                self._anchor_datetime(config.time_anchor, pull_request).date(),
                since=config.since,
                until=config.until,
            )
        )

    def _filter_pull_requests_by_repository(
        self,
        config: PersonConfig,
        pull_requests: tuple[PullRequestFact, ...],
    ) -> tuple[PullRequestFact, ...]:
        return tuple(
            pull_request
            for pull_request in pull_requests
            if self._repository_in_scope(config, pull_request.repository_full_name)
        )

    def _repository_in_scope(
        self,
        config: PersonConfig,
        repository_full_name: str,
    ) -> bool:
        repository_name = repository_full_name.split("/", maxsplit=1)[-1]
        if config.include_repos and not any(
            repo_filter_matches(
                repo_filter,
                full_name=repository_full_name,
                name=repository_name,
                org=config.org,
            )
            for repo_filter in config.include_repos
        ):
            return False
        return not any(
            repo_filter_matches(
                repo_filter,
                full_name=repository_full_name,
                name=repository_name,
                org=config.org,
            )
            for repo_filter in config.exclude_repos
        )

    def _review_submissions(
        self,
        config: PersonConfig,
        pull_requests: tuple[PullRequestFact, ...],
    ) -> tuple[ReviewFact, ...]:
        login = config.login.lower()
        reviews: list[ReviewFact] = []
        for pull_request in pull_requests:
            for review in pull_request.reviews:
                if review.submitted_at is None:
                    continue
                if (review.author_login or "").lower() != login:
                    continue
                if not self._in_window(
                    review.submitted_at.date(),
                    since=config.since,
                    until=config.until,
                ):
                    continue
                reviews.append(review)
        return tuple(
            sorted(
                reviews,
                key=lambda review: (
                    review.submitted_at or datetime.min,
                    review.repository_full_name,
                    review.pull_request_number,
                ),
            )
        )

    def _period_rows(
        self,
        *,
        config: PersonConfig,
        periods: tuple[RawSnapshotPeriod, ...],
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> tuple[PersonPeriodRow, ...]:
        authored_by_period: dict[str, list[PullRequestFact]] = defaultdict(list)
        for pull_request in authored_pull_requests:
            authored_by_period[
                config.grain.key_for(
                    self._anchor_datetime(config.time_anchor, pull_request).date()
                )
            ].append(pull_request)
        reviews_by_period: dict[str, list[ReviewFact]] = defaultdict(list)
        for review in review_submissions:
            if review.submitted_at is not None:
                reviews_by_period[config.grain.key_for(review.submitted_at.date())].append(
                    review
                )
        period_catalog = self._period_catalog(
            config,
            periods,
            activity_period_keys=(
                *authored_by_period.keys(),
                *reviews_by_period.keys(),
            ),
        )
        return tuple(
            self._period_row(
                period=period,
                authored_pull_requests=tuple(authored_by_period.get(period.key, ())),
                review_submissions=tuple(reviews_by_period.get(period.key, ())),
            )
            for period in period_catalog
        )

    def _period_catalog(
        self,
        config: PersonConfig,
        periods: tuple[RawSnapshotPeriod, ...],
        *,
        activity_period_keys: tuple[str, ...],
    ) -> tuple[RawSnapshotPeriod, ...]:
        period_index = {
            period.key: period
            for period in periods
            if self._period_overlaps_window(
                period,
                since=config.since,
                until=config.until,
            )
        }
        for period_key in activity_period_keys:
            period_index.setdefault(
                period_key,
                self._synthetic_activity_period(config.grain, period_key),
            )
        return tuple(
            period_index[key]
            for key in sorted(
                period_index,
                key=lambda key: (period_index[key].start_date, key),
            )
        )

    def _synthetic_activity_period(
        self,
        grain: PeriodGrain,
        period_key: str,
    ) -> RawSnapshotPeriod:
        start_date = grain.start_for_key(period_key)
        return RawSnapshotPeriod(
            key=period_key,
            start_date=start_date,
            end_date=grain.end_for(start_date),
            closed=True,
            directory=Path(),
            pull_requests_path=Path(),
            pull_request_count=0,
            reviews_path=Path(),
            review_count=0,
            timeline_events_path=Path(),
            timeline_event_count=0,
        )

    def _period_row(
        self,
        *,
        period: RawSnapshotPeriod,
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> PersonPeriodRow:
        return PersonPeriodRow(
            period_key=period.key,
            period_start_date=period.start_date,
            period_end_date=period.end_date,
            authored_pull_request_count=len(authored_pull_requests),
            merged_pull_request_count=sum(
                1 for pull_request in authored_pull_requests if pull_request.merged
            ),
            open_pull_request_count=sum(
                1 for pull_request in authored_pull_requests if pull_request.state == "open"
            ),
            changed_lines_total=sum(
                pull_request.changed_lines for pull_request in authored_pull_requests
            ),
            commits_total=sum(pull_request.commits for pull_request in authored_pull_requests),
            reviews_received=sum(
                self._review_count(pull_request) for pull_request in authored_pull_requests
            ),
            review_submissions_given=len(review_submissions),
            pull_requests_reviewed=len(
                {self._review_pull_request_key(review) for review in review_submissions}
            ),
            approvals_given=sum(1 for review in review_submissions if review.state == "APPROVED"),
            changes_requested_given=sum(
                1 for review in review_submissions if review.state == "CHANGES_REQUESTED"
            ),
            comments_given=sum(1 for review in review_submissions if review.state == "COMMENTED"),
        )

    def _repository_rows(
        self,
        *,
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> tuple[PersonRepositoryRow, ...]:
        authored_by_repository: dict[str, list[PullRequestFact]] = defaultdict(list)
        for pull_request in authored_pull_requests:
            authored_by_repository[pull_request.repository_full_name].append(pull_request)
        reviews_by_repository: dict[str, list[ReviewFact]] = defaultdict(list)
        for review in review_submissions:
            reviews_by_repository[review.repository_full_name].append(review)
        repository_names = sorted(
            {*authored_by_repository.keys(), *reviews_by_repository.keys()}
        )
        return tuple(
            self._repository_row(
                repository_full_name=repository_name,
                authored_pull_requests=tuple(authored_by_repository.get(repository_name, ())),
                review_submissions=tuple(reviews_by_repository.get(repository_name, ())),
            )
            for repository_name in repository_names
        )

    def _repository_row(
        self,
        *,
        repository_full_name: str,
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> PersonRepositoryRow:
        return PersonRepositoryRow(
            repository_full_name=repository_full_name,
            authored_pull_request_count=len(authored_pull_requests),
            merged_pull_request_count=sum(
                1 for pull_request in authored_pull_requests if pull_request.merged
            ),
            open_pull_request_count=sum(
                1 for pull_request in authored_pull_requests if pull_request.state == "open"
            ),
            changed_lines_total=sum(
                pull_request.changed_lines for pull_request in authored_pull_requests
            ),
            commits_total=sum(pull_request.commits for pull_request in authored_pull_requests),
            reviews_received=sum(
                self._review_count(pull_request) for pull_request in authored_pull_requests
            ),
            review_submissions_given=len(review_submissions),
            pull_requests_reviewed=len(
                {self._review_pull_request_key(review) for review in review_submissions}
            ),
        )

    def _summary(
        self,
        authored_pull_requests: tuple[PullRequestFact, ...],
        *,
        distribution_percentile: int,
    ) -> PersonSummary:
        merged_pull_request_count = sum(
            1 for pull_request in authored_pull_requests if pull_request.merged
        )
        return PersonSummary(
            authored_pull_request_count=len(authored_pull_requests),
            merged_pull_request_count=merged_pull_request_count,
            open_pull_request_count=sum(
                1 for pull_request in authored_pull_requests if pull_request.state == "open"
            ),
            merge_rate_pct=self._percentage(
                merged_pull_request_count,
                len(authored_pull_requests),
            ),
            changed_lines_total=sum(
                pull_request.changed_lines for pull_request in authored_pull_requests
            ),
            commits_total=sum(pull_request.commits for pull_request in authored_pull_requests),
            reviews_received=sum(
                self._review_count(pull_request) for pull_request in authored_pull_requests
            ),
            review_coverage_pct=self._percentage(
                sum(
                    1
                    for pull_request in authored_pull_requests
                    if self._review_count(pull_request) > 0
                ),
                len(authored_pull_requests),
            ),
            median_first_review_hours=self._median_metric(
                tuple(
                    first_review_hours
                    for pull_request in authored_pull_requests
                    if (
                        first_review_hours := self._first_review_hours(pull_request)
                    )
                    is not None
                ),
                distribution_percentile=distribution_percentile,
            ),
            median_merge_hours=self._median_metric(
                tuple(
                    merge_hours
                    for pull_request in authored_pull_requests
                    if (merge_hours := self._merge_hours(pull_request)) is not None
                ),
                distribution_percentile=distribution_percentile,
            ),
        )

    def _reviewer_summary(
        self,
        review_submissions: tuple[ReviewFact, ...],
    ) -> PersonReviewerSummary:
        return PersonReviewerSummary(
            review_submissions=len(review_submissions),
            pull_requests_reviewed=len(
                {self._review_pull_request_key(review) for review in review_submissions}
            ),
            approvals=sum(1 for review in review_submissions if review.state == "APPROVED"),
            changes_requested=sum(
                1 for review in review_submissions if review.state == "CHANGES_REQUESTED"
            ),
            comments=sum(1 for review in review_submissions if review.state == "COMMENTED"),
            authors_supported=len(
                {
                    review.pull_request_author_login
                    for review in review_submissions
                    if review.pull_request_author_login is not None
                }
            ),
            repositories_reviewed=len(
                {review.repository_full_name for review in review_submissions}
            ),
        )

    def _anchor_datetime(
        self,
        time_anchor: TimeAnchor,
        pull_request: PullRequestFact,
    ) -> datetime:
        if time_anchor is TimeAnchor.CREATED_AT:
            return pull_request.created_at
        if time_anchor is TimeAnchor.UPDATED_AT:
            return pull_request.updated_at
        if pull_request.merged_at is None:
            return datetime.min
        return pull_request.merged_at

    def _first_review_hours(
        self,
        pull_request: PullRequestFact,
    ) -> float | None:
        for review in pull_request.reviews:
            if review.submitted_at is None:
                continue
            if (
                pull_request.author_login is not None
                and review.author_login == pull_request.author_login
            ):
                continue
            review_started_at = self._review_started_at(pull_request, review.submitted_at)
            return self._hours_between(review_started_at, review.submitted_at)
        return None

    def _review_started_at(
        self,
        pull_request: PullRequestFact,
        reference_at: datetime,
    ) -> datetime | None:
        review_ready_at = self._review_ready_at(pull_request, reference_at)
        review_requested_at = self._review_requested_at(pull_request, reference_at)
        if review_ready_at is None:
            return None
        if review_requested_at is None:
            return review_ready_at
        return max(review_ready_at, review_requested_at)

    def _review_ready_at(
        self,
        pull_request: PullRequestFact,
        reference_at: datetime,
    ) -> datetime | None:
        review_ready_at = self._initial_review_ready_at(pull_request)
        for event in pull_request.timeline_events:
            if event.created_at is None:
                continue
            if event.created_at > reference_at:
                break
            if event.event == "converted_to_draft":
                review_ready_at = None
            elif event.event == "ready_for_review":
                review_ready_at = event.created_at
        return review_ready_at

    def _initial_review_ready_at(
        self,
        pull_request: PullRequestFact,
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
        pull_request: PullRequestFact,
    ) -> str | None:
        for event in pull_request.timeline_events:
            if event.created_at is None:
                continue
            if event.event in {"converted_to_draft", "ready_for_review"}:
                return event.event
        return None

    def _review_requested_at(
        self,
        pull_request: PullRequestFact,
        reference_at: datetime,
    ) -> datetime | None:
        active_requests: set[str] = set()
        review_requested_at: datetime | None = None
        for event in pull_request.timeline_events:
            if event.created_at is None:
                continue
            if event.created_at > reference_at:
                break
            if event.event == "converted_to_draft":
                active_requests.clear()
                review_requested_at = None
                continue
            if event.event == "review_requested":
                request_key = self._request_key(event)
                if request_key not in active_requests and not active_requests:
                    review_requested_at = event.created_at
                active_requests.add(request_key)
                continue
            if event.event == "review_request_removed":
                active_requests.discard(self._request_key(event))
                if not active_requests:
                    review_requested_at = None
        return review_requested_at

    def _request_key(
        self,
        event: TimelineEventFact,
    ) -> str:
        if event.requested_reviewer_login is not None:
            return f"user:{event.requested_reviewer_login.lower()}"
        if event.requested_team_name is not None:
            return f"team:{event.requested_team_name.lower()}"
        return f"event:{event.event_id}"

    def _merge_hours(
        self,
        pull_request: PullRequestFact,
    ) -> float | None:
        if not pull_request.merged:
            return None
        return self._hours_between(pull_request.created_at, pull_request.merged_at)

    def _hours_between(
        self,
        start_at: datetime | None,
        end_at: datetime | None,
    ) -> float | None:
        if start_at is None or end_at is None or end_at < start_at:
            return None
        return _round_metric((end_at - start_at).total_seconds() / 3600)

    def _review_count(
        self,
        pull_request: PullRequestFact,
    ) -> int:
        return sum(1 for review in pull_request.reviews if review.submitted_at is not None)

    def _period_overlaps_window(
        self,
        period: RawSnapshotPeriod,
        *,
        since: date | None,
        until: date | None,
    ) -> bool:
        if since is not None and period.end_date < since:
            return False
        if until is not None and period.start_date > until:
            return False
        return True

    def _in_window(
        self,
        value: date,
        *,
        since: date | None,
        until: date | None,
    ) -> bool:
        if value == date.min:
            return False
        if since is not None and value < since:
            return False
        if until is not None and value > until:
            return False
        return True

    def _percentage(
        self,
        numerator: int,
        denominator: int,
    ) -> float | None:
        if denominator == 0:
            return None
        return _round_metric(numerator / denominator * 100)

    def _median_metric(
        self,
        values: tuple[float, ...],
        *,
        distribution_percentile: int,
    ) -> float | None:
        trimmed_values = trim_upper_tail(values, percentile=distribution_percentile)
        if not trimmed_values:
            return None
        return _round_metric(float(median(trimmed_values)))

    def _review_pull_request_key(
        self,
        review: ReviewFact,
    ) -> PullRequestKey:
        return review.repository_full_name, review.pull_request_number

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


def build_person_config(
    *,
    org: str | None = None,
    login: str,
    output_dir: Path | None = None,
    grain: PeriodGrain | None = None,
    time_anchor: TimeAnchor | None = None,
    since: date | str | None = None,
    until: date | str | None = None,
    distribution_percentile: int | None = None,
    export_format: PersonExportFormat | None = None,
    include_repos: list[str] | None = None,
    exclude_repos: list[str] | None = None,
) -> PersonConfig:
    """Build person metric settings from CLI inputs and application defaults."""

    settings = get_settings()
    payload: dict[str, object] = {
        "org": settings.org if org is None else org,
        "login": login,
        "output_dir": settings.output_dir if output_dir is None else output_dir,
        "grain": settings.period if grain is None else grain,
        "time_anchor": settings.time_anchor if time_anchor is None else time_anchor,
        "export_format": (
            PersonExportFormat.JSON if export_format is None else export_format
        ),
    }
    if since is not None:
        payload["since"] = since
    if until is not None:
        payload["until"] = until
    if distribution_percentile is not None:
        payload["distribution_percentile"] = distribution_percentile
    if include_repos is not None:
        payload["include_repos"] = include_repos
    if exclude_repos is not None:
        payload["exclude_repos"] = exclude_repos
    return PersonConfig.model_validate(payload)


def render_person_metrics_result(
    result: PersonMetricsResult,
) -> str:
    """Render a person metrics result into the requested export format."""

    if result.export_format is PersonExportFormat.CSV:
        return _render_csv(result)
    if result.export_format is PersonExportFormat.MARKDOWN:
        return _render_markdown(result)
    if result.export_format is PersonExportFormat.HTML:
        return _render_html(result)
    return _render_json(result)


def _render_csv(
    result: PersonMetricsResult,
) -> str:
    fieldnames = tuple(PersonPeriodRow.model_fields.keys())
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in result.period_rows:
        writer.writerow(
            {
                key: "" if value is None else value
                for key, value in row.model_dump(mode="json").items()
            }
        )
    return buffer.getvalue().rstrip("\n")


def _render_markdown(
    result: PersonMetricsResult,
) -> str:
    summary = result.summary
    reviewer = result.reviewer_summary
    lines = [
        f"# orgpulse person metrics: {result.login}",
        "",
        f"- Organization: {result.target_org}",
        f"- Grain: {result.grain.value}",
        f"- Time anchor: {result.time_anchor.value}",
        f"- Since: {result.since.isoformat() if result.since is not None else 'all'}",
        f"- Until: {result.until.isoformat() if result.until is not None else 'all'}",
        f"- Distribution percentile: {result.distribution_percentile}",
        "",
        "## Summary",
        "",
        f"- Authored PRs: {summary.authored_pull_request_count}",
        f"- Merged PRs: {summary.merged_pull_request_count}",
        f"- Open PRs: {summary.open_pull_request_count}",
        f"- Merge rate: {_markdown_number(summary.merge_rate_pct)}%",
        f"- Changed lines: {summary.changed_lines_total}",
        f"- Commits: {summary.commits_total}",
        f"- Reviews received: {summary.reviews_received}",
        f"- Review coverage: {_markdown_number(summary.review_coverage_pct)}%",
        f"- Median first review hours: {_markdown_number(summary.median_first_review_hours)}",
        f"- Median merge hours: {_markdown_number(summary.median_merge_hours)}",
        "",
        "## Reviewer Activity",
        "",
        f"- Reviews submitted: {reviewer.review_submissions}",
        f"- PRs reviewed: {reviewer.pull_requests_reviewed}",
        f"- Approvals: {reviewer.approvals}",
        f"- Changes requested: {reviewer.changes_requested}",
        f"- Comments: {reviewer.comments}",
        f"- Authors supported: {reviewer.authors_supported}",
        f"- Repositories reviewed: {reviewer.repositories_reviewed}",
        "",
        "## Periods",
        "",
        "| Period | Authored PRs | Merged | Open | Changed Lines | Commits | Reviews Received | Reviews Given | PRs Reviewed |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.period_rows:
        lines.append(
            "| "
            f"{row.period_key} | "
            f"{row.authored_pull_request_count} | "
            f"{row.merged_pull_request_count} | "
            f"{row.open_pull_request_count} | "
            f"{row.changed_lines_total} | "
            f"{row.commits_total} | "
            f"{row.reviews_received} | "
            f"{row.review_submissions_given} | "
            f"{row.pull_requests_reviewed} |"
        )
    lines.extend(
        [
            "",
            "## Repositories",
            "",
            "| Repository | Authored PRs | Merged | Open | Changed Lines | Reviews Given | PRs Reviewed |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.repository_rows:
        lines.append(
            "| "
            f"{row.repository_full_name} | "
            f"{row.authored_pull_request_count} | "
            f"{row.merged_pull_request_count} | "
            f"{row.open_pull_request_count} | "
            f"{row.changed_lines_total} | "
            f"{row.review_submissions_given} | "
            f"{row.pull_requests_reviewed} |"
        )
    return "\n".join(lines)


def _render_json(
    result: PersonMetricsResult,
) -> str:
    return json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True)


def _render_html(
    result: PersonMetricsResult,
) -> str:
    top_repository_rows = _top_repository_rows(result.repository_rows)
    period_recent_rows, period_older_rows = _split_recent_rows(
        result.period_rows,
        recent_count=6,
    )
    repository_top_rows, repository_rest_rows = _split_ranked_repository_rows(
        result.repository_rows,
        top_count=10,
    )
    window_label = _window_label(result)
    report_payload = _html_report_payload(result)
    template = _template_environment().get_template("person_report.html.j2")
    return template.render(
        result=result,
        summary=result.summary,
        reviewer_summary=result.reviewer_summary,
        period_recent_rows=period_recent_rows,
        period_older_rows=period_older_rows,
        repository_top_rows=repository_top_rows,
        repository_rest_rows=repository_rest_rows,
        top_repository_rows=top_repository_rows,
        period_total_count=len(result.period_rows),
        repository_total_count=len(result.repository_rows),
        report_payload=report_payload,
        window_label=window_label,
    )


def _split_recent_rows(
    rows: tuple[PersonPeriodRow, ...],
    *,
    recent_count: int,
) -> tuple[tuple[PersonPeriodRow, ...], tuple[PersonPeriodRow, ...]]:
    if len(rows) <= recent_count:
        return tuple(reversed(rows)), ()
    return tuple(reversed(rows[-recent_count:])), tuple(reversed(rows[:-recent_count]))


def _split_ranked_repository_rows(
    rows: tuple[PersonRepositoryRow, ...],
    *,
    top_count: int,
) -> tuple[tuple[PersonRepositoryRow, ...], tuple[PersonRepositoryRow, ...]]:
    ranked_rows = _ranked_repository_rows(rows)
    return ranked_rows[:top_count], ranked_rows[top_count:]


def _ranked_repository_rows(
    rows: tuple[PersonRepositoryRow, ...],
) -> tuple[PersonRepositoryRow, ...]:
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                -row.authored_pull_request_count,
                -row.review_submissions_given,
                -row.changed_lines_total,
                row.repository_full_name,
            ),
        )
    )


def _top_repository_rows(
    rows: tuple[PersonRepositoryRow, ...],
) -> tuple[PersonRepositoryRow, ...]:
    return _ranked_repository_rows(rows)[:6]


def _window_label(
    result: PersonMetricsResult,
) -> str:
    since = result.since.isoformat() if result.since is not None else "all"
    until = result.until.isoformat() if result.until is not None else "all"
    return f"{since} to {until}"


def _html_report_payload(
    result: PersonMetricsResult,
) -> dict[str, Any]:
    payload = result.model_dump(mode="json")
    payload.pop("source_manifest_path", None)
    payload.pop("output_dir", None)
    return payload


def _template_environment() -> Environment:
    environment = Environment(
        loader=FileSystemLoader(
            str(Path(__file__).resolve().parent / "templates")
        ),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )
    environment.filters["intfmt"] = _format_integer
    environment.filters["numfmt"] = _format_number
    environment.filters["duration"] = _format_duration
    environment.filters["pctfmt"] = _format_percent
    environment.filters["json_script"] = _json_script
    return environment


def _markdown_number(
    value: float | None,
) -> str:
    if value is None:
        return "-"
    return str(_display_value(value))


def _format_integer(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return f"{int(float(value)):,}"


def _format_number(value: Any) -> str:
    if value is None or value == "":
        return "-"
    number = float(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def _format_duration(value: Any) -> str:
    if value is None or value == "":
        return "-"
    hours = float(value)
    if hours < 1:
        return f"{round(hours * 60):,} min"
    if hours >= 24:
        return f"{hours / 24:,.1f} d"
    return f"{hours:,.1f} h"


def _format_percent(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return f"{_format_number(value)}%"


def _json_script(value: Any) -> Markup:
    return Markup(escape(json.dumps(value, ensure_ascii=False)))


def _display_value(
    value: object,
) -> object:
    if value is None:
        return "-"
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _round_metric(
    value: float | None,
) -> float | None:
    if value is None:
        return None
    return round(value, 2)
