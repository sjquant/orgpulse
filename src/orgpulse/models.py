from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Callable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)

ORG_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38}[A-Za-z0-9])?$"
REPO_PATTERN = r"^(?:[A-Za-z0-9_.-]+/)?[A-Za-z0-9_.-]+$"

OrgSlug = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, pattern=ORG_PATTERN)
]
RepoSlug = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, pattern=REPO_PATTERN)
]


class PeriodGrain(StrEnum):
    WEEK = "week"
    MONTH = "month"

    def start_for(self, value: date) -> date:
        if self is PeriodGrain.MONTH:
            return value.replace(day=1)
        return value - timedelta(days=value.weekday())

    def end_for(self, value: date) -> date:
        start_date = self.start_for(value)
        if self is PeriodGrain.MONTH:
            return _month_end(start_date)
        return start_date + timedelta(days=6)

    def is_period_start(self, value: date) -> bool:
        return value == self.start_for(value)

    def is_period_end(self, value: date) -> bool:
        return value == self.end_for(value)

    def key_for(self, value: date) -> str:
        if self is PeriodGrain.MONTH:
            return _period_key_for_month(value)
        return _period_key_for_week(value)

    def count_periods(self, start_date: date, end_date: date) -> int:
        if self is PeriodGrain.MONTH:
            return _count_periods(start_date, end_date, _next_month_start)
        return _count_periods(start_date, end_date, _next_week_start)


class RunMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


class RunScope(StrEnum):
    FULL_HISTORY = "full_history"
    OPEN_PERIOD = "open_period"
    BOUNDED_BACKFILL = "bounded_backfill"


class AuthSource(StrEnum):
    GH_TOKEN = "GH_TOKEN"
    GH_CLI = "gh"


class ReportingPeriod(BaseModel):
    model_config = ConfigDict(frozen=True)

    grain: PeriodGrain
    start_date: date
    end_date: date
    key: str
    closed: bool


class PeriodRange(BaseModel):
    model_config = ConfigDict(frozen=True)

    grain: PeriodGrain
    start_date: date
    end_date: date
    period_count: int


class CollectionWindow(BaseModel):
    model_config = ConfigDict(frozen=True)

    scope: RunScope
    start_date: date | None
    end_date: date


class RepositoryInventoryItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    full_name: str
    default_branch: str
    private: bool
    archived: bool
    disabled: bool


class RepositoryInventory(BaseModel):
    model_config = ConfigDict(frozen=True)

    organization_login: str
    repositories: tuple[RepositoryInventoryItem, ...]


class RepositoryCollectionFailure(BaseModel):
    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    operation: str
    status_code: int
    retriable: bool
    message: str


class PullRequestReviewRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    review_id: int
    state: str
    author_login: str | None
    submitted_at: datetime | None
    commit_id: str | None


class PullRequestTimelineEventRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: int
    event: str
    actor_login: str | None
    created_at: datetime | None
    requested_reviewer_login: str | None
    requested_team_name: str | None


class PullRequestRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    number: int
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
    reviews: tuple[PullRequestReviewRecord, ...] = ()
    timeline_events: tuple[PullRequestTimelineEventRecord, ...] = ()


class PullRequestCollection(BaseModel):
    model_config = ConfigDict(frozen=True)

    window: CollectionWindow
    pull_requests: tuple[PullRequestRecord, ...]
    failures: tuple[RepositoryCollectionFailure, ...]


class RawSnapshotPeriod(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    directory: Path
    pull_requests_path: Path
    pull_request_count: int
    reviews_path: Path
    review_count: int
    timeline_events_path: Path
    timeline_event_count: int


class RawSnapshotWriteResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    root_dir: Path
    periods: tuple[RawSnapshotPeriod, ...]


class CheckpointPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    resume_from_checkpoint: bool
    persist_checkpoint: bool
    overwrite_checkpoint: bool


class LockPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    skip_locked_periods: bool
    refresh_locked_periods: bool
    lock_closed_periods_on_success: bool


class RunConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    org: OrgSlug
    github_token: SecretStr | None = Field(
        default=None,
        exclude=True,
        repr=False,
    )
    as_of: date = Field(default_factory=date.today)
    period: PeriodGrain = PeriodGrain.MONTH
    mode: RunMode = RunMode.INCREMENTAL
    output_dir: Path = Field(default_factory=lambda: Path("output"))
    include_repos: tuple[RepoSlug, ...] = ()
    exclude_repos: tuple[RepoSlug, ...] = ()
    backfill_start: date | None = None
    backfill_end: date | None = None

    @field_validator("include_repos", "exclude_repos", mode="before")
    @classmethod
    def normalize_repo_filters(
        cls, value: Any, info: ValidationInfo
    ) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            items = [value]
        else:
            items = list(value)

        org = info.data.get("org")
        deduped: list[str] = []
        seen: set[str] = set()
        for item in items:
            cleaned = item.strip()
            canonical = canonicalize_repo_filter(cleaned, org=org)
            if cleaned and canonical not in seen:
                deduped.append(cleaned)
                seen.add(canonical)
        return tuple(deduped)

    @field_validator("output_dir", mode="before")
    @classmethod
    def normalize_output_dir(cls, value: Any) -> Path:
        if value is None:
            return Path("output")
        return Path(value).expanduser()

    @model_validator(mode="after")
    def validate_cross_field_constraints(self) -> "RunConfig":
        overlapping = _find_overlapping_repo_filters(
            org=self.org,
            include_repos=self.include_repos,
            exclude_repos=self.exclude_repos,
        )
        if overlapping:
            overlap = ", ".join(sorted(overlapping))
            raise ValueError(
                f"repo filters overlap across include and exclude lists: {overlap}"
            )

        for repo_filter in (*self.include_repos, *self.exclude_repos):
            if "/" not in repo_filter:
                continue
            owner, _ = repo_filter.split("/", 1)
            if owner.lower() != self.org.lower():
                raise ValueError(
                    f"repo filter owner must match target org '{self.org}': {repo_filter}"
                )

        has_backfill_bounds = (
            self.backfill_start is not None or self.backfill_end is not None
        )
        if self.mode is RunMode.BACKFILL:
            if self.backfill_start is None or self.backfill_end is None:
                raise ValueError(
                    "backfill mode requires both --backfill-start and --backfill-end"
                )
            if self.backfill_start > self.backfill_end:
                raise ValueError("--backfill-start must be on or before --backfill-end")
            if not self.period.is_period_start(self.backfill_start):
                raise ValueError(
                    "backfill start must align to the selected period boundary"
                )
            if not self.period.is_period_end(self.backfill_end):
                raise ValueError(
                    "backfill end must align to the selected period boundary"
                )
            if self.backfill_end >= self.active_period.start_date:
                raise ValueError(
                    "backfill range must end before the current open period begins"
                )
        elif has_backfill_bounds:
            raise ValueError(
                "backfill date bounds are only valid when --mode backfill is selected"
            )

        return self

    @computed_field(return_type=RunScope)
    @property
    def refresh_scope(self) -> RunScope:
        if self.mode is RunMode.FULL:
            return RunScope.FULL_HISTORY
        if self.mode is RunMode.BACKFILL:
            return RunScope.BOUNDED_BACKFILL
        return RunScope.OPEN_PERIOD

    @computed_field(return_type=ReportingPeriod)
    @property
    def active_period(self) -> ReportingPeriod:
        start_date = self.period.start_for(self.as_of)
        end_date = self.period.end_for(self.as_of)
        return ReportingPeriod(
            grain=self.period,
            start_date=start_date,
            end_date=end_date,
            key=self.period.key_for(start_date),
            closed=False,
        )

    @computed_field(return_type=PeriodRange | None)
    @property
    def requested_range(self) -> PeriodRange | None:
        if self.mode is not RunMode.BACKFILL:
            return None
        assert self.backfill_start is not None
        assert self.backfill_end is not None
        return PeriodRange(
            grain=self.period,
            start_date=self.backfill_start,
            end_date=self.backfill_end,
            period_count=self.period.count_periods(
                self.backfill_start, self.backfill_end
            ),
        )

    @computed_field(return_type=CollectionWindow)
    @property
    def collection_window(self) -> CollectionWindow:
        if self.mode is RunMode.FULL:
            return CollectionWindow(
                scope=self.refresh_scope,
                start_date=None,
                end_date=self.as_of,
            )
        if self.mode is RunMode.BACKFILL:
            assert self.backfill_start is not None
            assert self.backfill_end is not None
            return CollectionWindow(
                scope=self.refresh_scope,
                start_date=self.backfill_start,
                end_date=self.backfill_end,
            )
        return CollectionWindow(
            scope=self.refresh_scope,
            start_date=self.active_period.start_date,
            end_date=self.as_of,
        )

    @computed_field(return_type=CheckpointPolicy)
    @property
    def checkpoint_policy(self) -> CheckpointPolicy:
        if self.mode is RunMode.FULL:
            return CheckpointPolicy(
                resume_from_checkpoint=False,
                persist_checkpoint=True,
                overwrite_checkpoint=True,
            )
        if self.mode is RunMode.BACKFILL:
            return CheckpointPolicy(
                resume_from_checkpoint=False,
                persist_checkpoint=False,
                overwrite_checkpoint=False,
            )
        return CheckpointPolicy(
            resume_from_checkpoint=True,
            persist_checkpoint=True,
            overwrite_checkpoint=False,
        )

    @computed_field(return_type=LockPolicy)
    @property
    def lock_policy(self) -> LockPolicy:
        if self.mode is RunMode.INCREMENTAL:
            return LockPolicy(
                skip_locked_periods=True,
                refresh_locked_periods=False,
                lock_closed_periods_on_success=True,
            )
        return LockPolicy(
            skip_locked_periods=False,
            refresh_locked_periods=True,
            lock_closed_periods_on_success=True,
        )


def _period_key_for_month(value: date) -> str:
    return value.strftime("%Y-%m")


def _period_key_for_week(value: date) -> str:
    iso_year, iso_week, _ = value.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _month_end(value: date) -> date:
    return value.replace(day=monthrange(value.year, value.month)[1])


def _next_month_start(value: date) -> date:
    year = value.year + (value.month // 12)
    month = 1 if value.month == 12 else value.month + 1
    return date(year, month, 1)


def _next_week_start(value: date) -> date:
    return value + timedelta(days=7)


def _count_periods(
    start_date: date, end_date: date, next_period_start: Callable[[date], date]
) -> int:
    count = 0
    current = start_date
    while current <= end_date:
        count += 1
        current = next_period_start(current)
    return count


def _find_overlapping_repo_filters(
    *,
    org: str,
    include_repos: tuple[str, ...],
    exclude_repos: tuple[str, ...],
) -> tuple[str, ...]:
    include_index = {
        canonicalize_repo_filter(repo_filter, org=org): repo_filter
        for repo_filter in include_repos
    }
    overlapping: list[str] = []
    seen: set[str] = set()
    for repo_filter in exclude_repos:
        canonical = canonicalize_repo_filter(repo_filter, org=org)
        if canonical not in include_index or canonical in seen:
            continue
        overlapping.append(include_index[canonical])
        seen.add(canonical)
    return tuple(overlapping)


def canonicalize_repo_filter(value: str, *, org: str | None = None) -> str:
    normalized = value.strip().lower()
    if "/" in normalized or org is None:
        return normalized
    return f"{org.lower()}/{normalized}"


def repo_filter_matches(
    repo_filter: str, *, full_name: str, name: str, org: str
) -> bool:
    canonical_filter = canonicalize_repo_filter(repo_filter, org=org)
    return canonical_filter in {
        canonicalize_repo_filter(full_name),
        canonicalize_repo_filter(name, org=org),
    }


class ResolvedToken(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: AuthSource
    token: str


class GitHubTargetContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    auth_source: AuthSource
    viewer_login: str
    organization_login: str
