from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from statistics import median
from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

from orgpulse.common.config import get_settings
from orgpulse.common.distribution import (
    trim_upper_tail,
    upper_percentile_threshold,
    validate_distribution_percentile,
)
from orgpulse.common.models import (
    OrgSlug,
    PeriodGrain,
    RawSnapshotPeriod,
    ReportLocale,
    RepoSlug,
    TimeAnchor,
    canonicalize_repo_filter,
    repo_filter_matches,
)
from orgpulse.libs.metrics.review_latency import (
    approval_hours as calculate_approval_hours,
)
from orgpulse.libs.metrics.review_latency import (
    first_review_hours as calculate_first_review_hours,
)
from orgpulse.libs.metrics.review_latency import (
    hours_between,
)
from orgpulse.libs.reporting.contracts import build_period_state_payload
from orgpulse.libs.snapshots.person_source import (
    PersonSnapshotSource,
    PullRequestFact,
    PullRequestKey,
    ReviewFact,
)
from orgpulse.libs.snapshots.source import LocalSnapshotSource

Login = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


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
    locale: ReportLocale = Field(default=ReportLocale.EN, exclude=True)
    include_repos: tuple[RepoSlug, ...] = ()
    exclude_repos: tuple[RepoSlug, ...] = ()
    include_org_trends: bool = False

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
    median_approval_hours: float | None
    median_merge_hours: float | None


class PersonReviewerSummary(BaseModel):
    """Summarize review activity submitted by one person."""

    model_config = ConfigDict(frozen=True)

    review_submissions: int
    pull_requests_reviewed: int
    reviewed_lines: int
    pull_requests_reviewed_per_month: float | None
    reviewed_lines_per_month: float | None
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
    status: str
    label: str
    is_open: bool
    is_closed: bool
    is_partial: bool
    observed_through_date: str
    open_week: bool
    open_month: bool
    authored_pull_request_count: int
    merged_pull_request_count: int
    open_pull_request_count: int
    changed_lines_total: int
    commits_total: int
    reviews_received: int
    median_first_review_hours: float | None
    median_approval_hours: float | None
    median_merge_hours: float | None
    review_submissions_given: int
    pull_requests_reviewed: int
    reviewed_lines: int
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
    median_first_review_hours: float | None
    median_approval_hours: float | None
    median_merge_hours: float | None
    review_submissions_given: int
    pull_requests_reviewed: int
    reviewed_lines: int


class OrgTrendRow(BaseModel):
    """Store one org-wide period row for person report comparison."""

    model_config = ConfigDict(frozen=True)

    period_key: str
    period_start_date: date
    period_end_date: date
    status: str
    label: str
    is_open: bool
    is_closed: bool
    is_partial: bool
    observed_through_date: str
    open_week: bool
    open_month: bool
    pull_requests: int
    merged_pull_requests: int
    open_pull_requests: int
    active_authors: int
    active_reviewers: int
    changed_lines: int
    authored_pull_request_count: int
    changed_lines_total: int
    commits_total: int
    pull_requests_per_active_author: float | None
    changed_lines_per_active_author: float | None
    review_submissions: int
    review_submissions_given: int
    pull_requests_reviewed: int
    reviewed_lines: int
    median_first_review_hours: float | None
    median_approval_hours: float | None
    median_merge_hours: float | None


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
    weekly_period_rows: tuple[PersonPeriodRow, ...]
    monthly_period_rows: tuple[PersonPeriodRow, ...]
    org_weekly_trend_rows: tuple[OrgTrendRow, ...] | None = None
    org_monthly_trend_rows: tuple[OrgTrendRow, ...] | None = None
    repository_rows: tuple[PersonRepositoryRow, ...]
    export_format: PersonExportFormat
    locale: ReportLocale = Field(default=ReportLocale.EN, exclude=True)
    include_org_trends: bool = False

    @model_validator(mode="after")
    def validate_org_trend_rows(self) -> "PersonMetricsResult":
        if self.include_org_trends:
            if (
                self.org_weekly_trend_rows is None
                or self.org_monthly_trend_rows is None
            ):
                raise ValueError(
                    "org trend rows are required when include_org_trends is true"
                )
            return self
        if (
            self.org_weekly_trend_rows is not None
            or self.org_monthly_trend_rows is not None
        ):
            raise ValueError(
                "org trend rows must be omitted when include_org_trends is false"
            )
        return self


class PersonMetricsService:
    """Build a focused person performance extract from normalized local snapshots."""

    def extract(
        self,
        config: PersonConfig,
    ) -> PersonMetricsResult:
        source = LocalSnapshotSource().load_person_metrics_source(
            org=config.org,
            output_dir=config.output_dir,
            grain=config.grain,
            time_anchor=config.time_anchor,
            until=config.until,
        )
        snapshot = PersonSnapshotSource().load(source.raw_snapshot)
        pull_requests = self._filter_pull_requests_by_repository(
            config,
            snapshot.pull_requests,
        )
        authored_pull_requests = self._authored_pull_requests(config, pull_requests)
        review_submissions = self._review_submissions(config, pull_requests)
        period_rows = self._period_rows(
            config=config,
            grain=config.grain,
            periods=snapshot.periods,
            source_as_of=source.manifest.last_successful_run.as_of,
            authored_pull_requests=authored_pull_requests,
            review_submissions=review_submissions,
        )
        weekly_period_rows = self._period_rows(
            config=config,
            grain=PeriodGrain.WEEK,
            periods=snapshot.periods,
            source_as_of=source.manifest.last_successful_run.as_of,
            authored_pull_requests=authored_pull_requests,
            review_submissions=review_submissions,
        )
        monthly_period_rows = self._period_rows(
            config=config,
            grain=PeriodGrain.MONTH,
            periods=snapshot.periods,
            source_as_of=source.manifest.last_successful_run.as_of,
            authored_pull_requests=authored_pull_requests,
            review_submissions=review_submissions,
        )
        org_weekly_trend_rows, org_monthly_trend_rows = self._org_trend_result_rows(
            config=config,
            periods=snapshot.periods,
            source_as_of=source.manifest.last_successful_run.as_of,
            pull_requests=pull_requests,
        )
        repository_rows = self._repository_rows(
            config=config,
            authored_pull_requests=authored_pull_requests,
            review_submissions=review_submissions,
        )
        return PersonMetricsResult(
            target_org=source.manifest.target_org,
            login=config.login,
            source_manifest_path=source.manifest_path,
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
            reviewer_summary=self._reviewer_summary(
                review_submissions,
                month_count=self._review_month_count(config, monthly_period_rows),
            ),
            period_rows=period_rows,
            weekly_period_rows=weekly_period_rows,
            monthly_period_rows=monthly_period_rows,
            org_weekly_trend_rows=org_weekly_trend_rows,
            org_monthly_trend_rows=org_monthly_trend_rows,
            repository_rows=repository_rows,
            export_format=config.export_format,
            locale=config.locale,
            include_org_trends=config.include_org_trends,
        )

    def _org_trend_result_rows(
        self,
        *,
        config: PersonConfig,
        periods: tuple[RawSnapshotPeriod, ...],
        source_as_of: date,
        pull_requests: tuple[PullRequestFact, ...],
    ) -> tuple[tuple[OrgTrendRow, ...] | None, tuple[OrgTrendRow, ...] | None]:
        if not config.include_org_trends:
            return None, None

        org_pull_requests = self._org_pull_requests(config, pull_requests)
        org_review_submissions = self._org_review_submissions(config, pull_requests)
        return (
            self._org_trend_rows(
                config=config,
                grain=PeriodGrain.WEEK,
                periods=periods,
                source_as_of=source_as_of,
                pull_requests=org_pull_requests,
                review_submissions=org_review_submissions,
            ),
            self._org_trend_rows(
                config=config,
                grain=PeriodGrain.MONTH,
                periods=periods,
                source_as_of=source_as_of,
                pull_requests=org_pull_requests,
                review_submissions=org_review_submissions,
            ),
        )

    def _org_pull_requests(
        self,
        config: PersonConfig,
        pull_requests: tuple[PullRequestFact, ...],
    ) -> tuple[PullRequestFact, ...]:
        return tuple(
            pull_request
            for pull_request in pull_requests
            if self._in_window(
                self._anchor_datetime(config.time_anchor, pull_request).date(),
                since=config.since,
                until=config.until,
            )
        )

    def _org_review_submissions(
        self,
        config: PersonConfig,
        pull_requests: tuple[PullRequestFact, ...],
    ) -> tuple[ReviewFact, ...]:
        reviews: list[ReviewFact] = []
        for pull_request in pull_requests:
            for review in pull_request.reviews:
                if review.submitted_at is None:
                    continue
                if self._same_login(
                    review.pull_request_author_login, review.author_login
                ):
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
                    review.review_id,
                ),
            )
        )

    def _org_trend_rows(
        self,
        *,
        config: PersonConfig,
        grain: PeriodGrain,
        periods: tuple[RawSnapshotPeriod, ...],
        source_as_of: date,
        pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> tuple[OrgTrendRow, ...]:
        pull_requests_by_period: dict[str, list[PullRequestFact]] = defaultdict(list)
        for pull_request in pull_requests:
            pull_requests_by_period[
                grain.key_for(
                    self._anchor_datetime(config.time_anchor, pull_request).date()
                )
            ].append(pull_request)
        reviews_by_period: dict[str, list[ReviewFact]] = defaultdict(list)
        for review in review_submissions:
            if review.submitted_at is not None:
                reviews_by_period[grain.key_for(review.submitted_at.date())].append(
                    review
                )
        period_catalog = self._period_catalog(
            config,
            grain,
            periods,
            source_as_of=source_as_of,
            activity_period_keys=(
                *pull_requests_by_period.keys(),
                *reviews_by_period.keys(),
            ),
        )
        changed_lines_threshold = self._changed_lines_threshold(
            pull_requests,
            distribution_percentile=config.distribution_percentile,
        )
        return tuple(
            self._org_trend_row(
                config=config,
                grain=grain,
                period=period,
                source_as_of=source_as_of,
                pull_requests=tuple(pull_requests_by_period.get(period.key, ())),
                review_submissions=tuple(reviews_by_period.get(period.key, ())),
                changed_lines_threshold=changed_lines_threshold,
            )
            for period in period_catalog
        )

    def _changed_lines_threshold(
        self,
        pull_requests: tuple[PullRequestFact, ...],
        *,
        distribution_percentile: int,
    ) -> float | None:
        return upper_percentile_threshold(
            tuple(pull_request.changed_lines for pull_request in pull_requests),
            percentile=distribution_percentile,
        )

    def _org_trend_row(
        self,
        *,
        config: PersonConfig,
        grain: PeriodGrain,
        period: RawSnapshotPeriod,
        source_as_of: date,
        pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
        changed_lines_threshold: float | None,
    ) -> OrgTrendRow:
        period_state = build_period_state_payload(
            period_grain=grain.value,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=self._period_closed(period, source_as_of=source_as_of),
            as_of=source_as_of,
            since=config.since,
            until=config.until,
        )
        pull_request_count = len(pull_requests)
        review_submission_count = len(review_submissions)
        active_authors = len(
            {
                pull_request.author_login.lower()
                for pull_request in pull_requests
                if pull_request.author_login is not None
            }
        )
        active_reviewers = len(
            {
                review.author_login.lower()
                for review in review_submissions
                if review.author_login is not None
            }
        )
        changed_lines = self._trimmed_changed_lines(
            pull_requests,
            threshold=changed_lines_threshold,
        )
        return OrgTrendRow(
            period_key=period.key,
            period_start_date=period.start_date,
            period_end_date=period.end_date,
            status=period_state.status,
            label=period_state.label,
            is_open=period_state.is_open,
            is_closed=period_state.is_closed,
            is_partial=period_state.is_partial,
            observed_through_date=period_state.observed_through_date,
            open_week=period_state.open_week,
            open_month=period_state.open_month,
            pull_requests=pull_request_count,
            merged_pull_requests=sum(
                1 for pull_request in pull_requests if pull_request.merged
            ),
            open_pull_requests=sum(
                1 for pull_request in pull_requests if pull_request.state == "open"
            ),
            active_authors=active_authors,
            active_reviewers=active_reviewers,
            changed_lines=changed_lines,
            authored_pull_request_count=pull_request_count,
            changed_lines_total=changed_lines,
            commits_total=sum(pull_request.commits for pull_request in pull_requests),
            pull_requests_per_active_author=_round_metric(
                pull_request_count / active_authors if active_authors else None
            ),
            changed_lines_per_active_author=_round_metric(
                changed_lines / active_authors if active_authors else None
            ),
            review_submissions=sum(
                self._review_count(pull_request) for pull_request in pull_requests
            ),
            review_submissions_given=review_submission_count,
            pull_requests_reviewed=len(
                {self._review_pull_request_key(review) for review in review_submissions}
            ),
            reviewed_lines=self._reviewed_lines(review_submissions),
            median_first_review_hours=self._median_metric(
                tuple(
                    first_review_hours
                    for pull_request in pull_requests
                    if (first_review_hours := self._first_review_hours(pull_request))
                    is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            median_approval_hours=self._median_metric(
                tuple(
                    approval_hours
                    for pull_request in pull_requests
                    if (approval_hours := self._approval_hours(pull_request))
                    is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            median_merge_hours=self._median_metric(
                tuple(
                    merge_hours
                    for pull_request in pull_requests
                    if (merge_hours := self._merge_hours(pull_request)) is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
        )

    def _trimmed_changed_lines(
        self,
        pull_requests: tuple[PullRequestFact, ...],
        *,
        threshold: float | None,
    ) -> int:
        return sum(
            pull_request.changed_lines
            for pull_request in pull_requests
            if threshold is None or pull_request.changed_lines <= threshold
        )

    def _review_month_count(
        self,
        config: PersonConfig,
        monthly_period_rows: tuple[PersonPeriodRow, ...],
    ) -> int:
        if config.since is not None and config.until is not None:
            return PeriodGrain.MONTH.count_periods(config.since, config.until)
        return max(1, len(monthly_period_rows))

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
                if self._same_login(review.pull_request_author_login, login):
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
        grain: PeriodGrain,
        periods: tuple[RawSnapshotPeriod, ...],
        source_as_of: date,
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> tuple[PersonPeriodRow, ...]:
        authored_by_period: dict[str, list[PullRequestFact]] = defaultdict(list)
        for pull_request in authored_pull_requests:
            authored_by_period[
                grain.key_for(
                    self._anchor_datetime(config.time_anchor, pull_request).date()
                )
            ].append(pull_request)
        reviews_by_period: dict[str, list[ReviewFact]] = defaultdict(list)
        for review in review_submissions:
            if review.submitted_at is not None:
                reviews_by_period[grain.key_for(review.submitted_at.date())].append(
                    review
                )
        period_catalog = self._period_catalog(
            config,
            grain,
            periods,
            source_as_of=source_as_of,
            activity_period_keys=(
                *authored_by_period.keys(),
                *reviews_by_period.keys(),
            ),
        )
        return tuple(
            self._period_row(
                config=config,
                grain=grain,
                period=period,
                source_as_of=source_as_of,
                authored_pull_requests=tuple(authored_by_period.get(period.key, ())),
                review_submissions=tuple(reviews_by_period.get(period.key, ())),
            )
            for period in period_catalog
        )

    def _period_catalog(
        self,
        config: PersonConfig,
        grain: PeriodGrain,
        periods: tuple[RawSnapshotPeriod, ...],
        *,
        source_as_of: date,
        activity_period_keys: tuple[str, ...],
    ) -> tuple[RawSnapshotPeriod, ...]:
        if grain is not config.grain:
            return self._derived_period_catalog(
                config,
                grain,
                periods,
                source_as_of=source_as_of,
                activity_period_keys=activity_period_keys,
            )
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
                self._synthetic_activity_period(grain, period_key),
            )
        return tuple(
            period_index[key]
            for key in sorted(
                period_index,
                key=lambda key: (period_index[key].start_date, key),
            )
        )

    def _derived_period_catalog(
        self,
        config: PersonConfig,
        grain: PeriodGrain,
        periods: tuple[RawSnapshotPeriod, ...],
        *,
        source_as_of: date,
        activity_period_keys: tuple[str, ...],
    ) -> tuple[RawSnapshotPeriod, ...]:
        window_periods = tuple(
            period
            for period in periods
            if self._period_overlaps_window(
                period,
                since=config.since,
                until=config.until,
            )
        )
        boundary_dates = [
            boundary
            for period in window_periods
            for boundary in (period.start_date, period.end_date)
        ]
        boundary_dates.extend(
            grain.start_for_key(period_key) for period_key in activity_period_keys
        )
        if not boundary_dates:
            return ()

        source_start_date = min(boundary_dates)
        source_end_date = min(max(boundary_dates), source_as_of)
        start_date = max(source_start_date, config.since or source_start_date)
        end_date = min(source_end_date, config.until or source_end_date)
        period_start = grain.start_for(start_date)
        period_index: dict[str, RawSnapshotPeriod] = {}
        while period_start <= end_date:
            period_key = grain.key_for(period_start)
            period_index[period_key] = self._synthetic_activity_period(
                grain,
                period_key,
            )
            period_start = grain.end_for(period_start) + timedelta(days=1)
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
            closed=False,
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
        config: PersonConfig,
        grain: PeriodGrain,
        period: RawSnapshotPeriod,
        source_as_of: date,
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> PersonPeriodRow:
        period_state = build_period_state_payload(
            period_grain=grain.value,
            start_date=period.start_date,
            end_date=period.end_date,
            closed=self._period_closed(period, source_as_of=source_as_of),
            as_of=source_as_of,
            since=config.since,
            until=config.until,
        )
        return PersonPeriodRow(
            period_key=period.key,
            period_start_date=period.start_date,
            period_end_date=period.end_date,
            status=period_state.status,
            label=period_state.label,
            is_open=period_state.is_open,
            is_closed=period_state.is_closed,
            is_partial=period_state.is_partial,
            observed_through_date=period_state.observed_through_date,
            open_week=period_state.open_week,
            open_month=period_state.open_month,
            authored_pull_request_count=len(authored_pull_requests),
            merged_pull_request_count=sum(
                1 for pull_request in authored_pull_requests if pull_request.merged
            ),
            open_pull_request_count=sum(
                1
                for pull_request in authored_pull_requests
                if pull_request.state == "open"
            ),
            changed_lines_total=sum(
                pull_request.changed_lines for pull_request in authored_pull_requests
            ),
            commits_total=sum(
                pull_request.commits for pull_request in authored_pull_requests
            ),
            reviews_received=sum(
                self._review_count(pull_request)
                for pull_request in authored_pull_requests
            ),
            median_first_review_hours=self._median_metric(
                tuple(
                    first_review_hours
                    for pull_request in authored_pull_requests
                    if (first_review_hours := self._first_review_hours(pull_request))
                    is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            median_approval_hours=self._median_metric(
                tuple(
                    approval_hours
                    for pull_request in authored_pull_requests
                    if (approval_hours := self._approval_hours(pull_request))
                    is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            median_merge_hours=self._median_metric(
                tuple(
                    merge_hours
                    for pull_request in authored_pull_requests
                    if (merge_hours := self._merge_hours(pull_request)) is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            review_submissions_given=len(review_submissions),
            pull_requests_reviewed=len(
                {self._review_pull_request_key(review) for review in review_submissions}
            ),
            reviewed_lines=self._reviewed_lines(review_submissions),
            approvals_given=sum(
                1 for review in review_submissions if review.state == "APPROVED"
            ),
            changes_requested_given=sum(
                1
                for review in review_submissions
                if review.state == "CHANGES_REQUESTED"
            ),
            comments_given=sum(
                1 for review in review_submissions if review.state == "COMMENTED"
            ),
        )

    def _period_closed(
        self,
        period: RawSnapshotPeriod,
        *,
        source_as_of: date,
    ) -> bool:
        if period.closed:
            return True
        return source_as_of >= period.end_date

    def _repository_rows(
        self,
        *,
        config: PersonConfig,
        authored_pull_requests: tuple[PullRequestFact, ...],
        review_submissions: tuple[ReviewFact, ...],
    ) -> tuple[PersonRepositoryRow, ...]:
        authored_by_repository: dict[str, list[PullRequestFact]] = defaultdict(list)
        for pull_request in authored_pull_requests:
            authored_by_repository[pull_request.repository_full_name].append(
                pull_request
            )
        reviews_by_repository: dict[str, list[ReviewFact]] = defaultdict(list)
        for review in review_submissions:
            reviews_by_repository[review.repository_full_name].append(review)
        repository_names = sorted(
            {*authored_by_repository.keys(), *reviews_by_repository.keys()}
        )
        return tuple(
            self._repository_row(
                config=config,
                repository_full_name=repository_name,
                authored_pull_requests=tuple(
                    authored_by_repository.get(repository_name, ())
                ),
                review_submissions=tuple(
                    reviews_by_repository.get(repository_name, ())
                ),
            )
            for repository_name in repository_names
        )

    def _repository_row(
        self,
        *,
        config: PersonConfig,
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
                1
                for pull_request in authored_pull_requests
                if pull_request.state == "open"
            ),
            changed_lines_total=sum(
                pull_request.changed_lines for pull_request in authored_pull_requests
            ),
            commits_total=sum(
                pull_request.commits for pull_request in authored_pull_requests
            ),
            reviews_received=sum(
                self._review_count(pull_request)
                for pull_request in authored_pull_requests
            ),
            median_first_review_hours=self._median_metric(
                tuple(
                    first_review_hours
                    for pull_request in authored_pull_requests
                    if (first_review_hours := self._first_review_hours(pull_request))
                    is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            median_approval_hours=self._median_metric(
                tuple(
                    approval_hours
                    for pull_request in authored_pull_requests
                    if (approval_hours := self._approval_hours(pull_request))
                    is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            median_merge_hours=self._median_metric(
                tuple(
                    merge_hours
                    for pull_request in authored_pull_requests
                    if (merge_hours := self._merge_hours(pull_request)) is not None
                ),
                distribution_percentile=config.distribution_percentile,
            ),
            review_submissions_given=len(review_submissions),
            pull_requests_reviewed=len(
                {self._review_pull_request_key(review) for review in review_submissions}
            ),
            reviewed_lines=self._reviewed_lines(review_submissions),
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
                1
                for pull_request in authored_pull_requests
                if pull_request.state == "open"
            ),
            merge_rate_pct=self._percentage(
                merged_pull_request_count,
                len(authored_pull_requests),
            ),
            changed_lines_total=sum(
                pull_request.changed_lines for pull_request in authored_pull_requests
            ),
            commits_total=sum(
                pull_request.commits for pull_request in authored_pull_requests
            ),
            reviews_received=sum(
                self._review_count(pull_request)
                for pull_request in authored_pull_requests
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
                    if (first_review_hours := self._first_review_hours(pull_request))
                    is not None
                ),
                distribution_percentile=distribution_percentile,
            ),
            median_approval_hours=self._median_metric(
                tuple(
                    approval_hours
                    for pull_request in authored_pull_requests
                    if (approval_hours := self._approval_hours(pull_request))
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
        *,
        month_count: int,
    ) -> PersonReviewerSummary:
        pull_requests_reviewed = len(
            {self._review_pull_request_key(review) for review in review_submissions}
        )
        reviewed_lines = self._reviewed_lines(review_submissions)
        return PersonReviewerSummary(
            review_submissions=len(review_submissions),
            pull_requests_reviewed=pull_requests_reviewed,
            reviewed_lines=reviewed_lines,
            pull_requests_reviewed_per_month=_round_metric(
                pull_requests_reviewed / month_count
            ),
            reviewed_lines_per_month=_round_metric(reviewed_lines / month_count),
            approvals=sum(
                1 for review in review_submissions if review.state == "APPROVED"
            ),
            changes_requested=sum(
                1
                for review in review_submissions
                if review.state == "CHANGES_REQUESTED"
            ),
            comments=sum(
                1 for review in review_submissions if review.state == "COMMENTED"
            ),
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

    def _reviewed_lines(
        self,
        review_submissions: tuple[ReviewFact, ...],
    ) -> int:
        reviewed_pull_requests: dict[PullRequestKey, int] = {}
        for review in review_submissions:
            reviewed_pull_requests.setdefault(
                self._review_pull_request_key(review),
                review.pull_request_changed_lines,
            )
        return sum(reviewed_pull_requests.values())

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
        return calculate_first_review_hours(
            author_login=pull_request.author_login,
            draft=pull_request.draft,
            created_at=pull_request.created_at,
            timeline_events=pull_request.timeline_events,
            reviews=pull_request.reviews,
        )

    def _approval_hours(
        self,
        pull_request: PullRequestFact,
    ) -> float | None:
        return calculate_approval_hours(
            author_login=pull_request.author_login,
            draft=pull_request.draft,
            created_at=pull_request.created_at,
            timeline_events=pull_request.timeline_events,
            reviews=pull_request.reviews,
        )

    def _merge_hours(
        self,
        pull_request: PullRequestFact,
    ) -> float | None:
        if not pull_request.merged:
            return None
        return hours_between(pull_request.created_at, pull_request.merged_at)

    def _review_count(
        self,
        pull_request: PullRequestFact,
    ) -> int:
        return sum(
            1 for review in pull_request.reviews if review.submitted_at is not None
        )

    def _same_login(
        self,
        left: str | None,
        right: str | None,
    ) -> bool:
        if left is None or right is None:
            return False
        return left.lower() == right.lower()

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
    locale: ReportLocale | str | None = None,
    include_repos: list[str] | None = None,
    exclude_repos: list[str] | None = None,
    include_org_trends: bool = False,
) -> PersonConfig:
    """Build person metric settings from CLI inputs and application defaults."""

    settings = get_settings()
    resolved_export_format = (
        PersonExportFormat.JSON if export_format is None else export_format
    )
    payload: dict[str, object] = {
        "org": settings.org if org is None else org,
        "login": login,
        "output_dir": settings.output_dir if output_dir is None else output_dir,
        "grain": PeriodGrain.MONTH if grain is None else grain,
        "time_anchor": settings.time_anchor if time_anchor is None else time_anchor,
        "export_format": resolved_export_format,
        "locale": (
            (settings.locale if locale is None else locale)
            if resolved_export_format is PersonExportFormat.HTML
            else ReportLocale.EN
        ),
        "include_org_trends": include_org_trends,
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


def _round_metric(
    value: float | None,
) -> float | None:
    if value is None:
        return None
    return round(value, 2)
