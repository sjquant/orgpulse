from __future__ import annotations

import csv
import shutil
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TypeVar, cast

from github import Github, GithubException

from orgpulse.errors import GitHubApiError
from orgpulse.models import (
    CollectionWindow,
    PeriodGrain,
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RepositoryCollectionFailure,
    RepositoryInventory,
    RepositoryInventoryItem,
    RunConfig,
    RunMode,
    repo_filter_matches,
)
from orgpulse.types.github import (
    GitHubActorLike,
    GitHubIngestionClientLike,
    GitHubOrganizationLike,
    GitHubPullRequestLike,
    GitHubRepositoryLike,
    GitHubTeamLike,
)

DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
RAW_SNAPSHOT_DIRNAME = "raw"
PULL_REQUEST_SNAPSHOT_FILENAME = "pull_requests.csv"
PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME = "pull_request_reviews.csv"
PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME = "pull_request_timeline_events.csv"
PULL_REQUEST_FIELDNAMES = (
    "period_key",
    "repository_full_name",
    "pull_request_number",
    "title",
    "state",
    "draft",
    "merged",
    "author_login",
    "created_at",
    "updated_at",
    "closed_at",
    "merged_at",
    "additions",
    "deletions",
    "changed_files",
    "commits",
    "html_url",
)
PULL_REQUEST_REVIEW_FIELDNAMES = (
    "period_key",
    "repository_full_name",
    "pull_request_number",
    "review_id",
    "state",
    "author_login",
    "submitted_at",
    "commit_id",
)
PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES = (
    "period_key",
    "repository_full_name",
    "pull_request_number",
    "event_id",
    "event",
    "actor_login",
    "created_at",
    "requested_reviewer_login",
    "requested_team_name",
)
FIRST_REVIEW_TIMELINE_EVENTS = frozenset(
    {
        "converted_to_draft",
        "ready_for_review",
        "review_request_removed",
        "review_requested",
    }
)
SnapshotRow = dict[str, str | int | bool | None]
T = TypeVar("T")


class NormalizedRawSnapshotWriter:
    """Persist enriched pull request records into period-partitioned raw snapshots."""

    def write(
        self,
        config: RunConfig,
        collection: PullRequestCollection,
    ) -> RawSnapshotWriteResult:
        root_dir = self._raw_snapshot_root_dir(config.output_dir, config.period.value)
        snapshot_periods = self._build_snapshot_periods(config, collection)
        self._prune_stale_period_directories(
            config=config,
            root_dir=root_dir,
            snapshot_periods=snapshot_periods,
        )
        grouped_pull_requests = self._group_pull_requests_by_period(
            config,
            collection.pull_requests,
        )
        periods = tuple(
            self._write_period_snapshot(
                root_dir=root_dir,
                snapshot_period=snapshot_period,
                pull_requests=grouped_pull_requests.get(snapshot_period.key, ()),
            )
            for snapshot_period in snapshot_periods
        )
        return RawSnapshotWriteResult(root_dir=root_dir, periods=periods)

    def _build_snapshot_periods(
        self,
        config: RunConfig,
        collection: PullRequestCollection,
    ) -> tuple[ReportingPeriod, ...]:
        if config.mode is RunMode.INCREMENTAL:
            return (config.active_period,)
        if config.mode is RunMode.BACKFILL:
            assert config.backfill_start is not None
            assert config.backfill_end is not None
            return self._build_periods_for_explicit_range(
                grain=config.period,
                start_date=config.backfill_start,
                end_date=config.backfill_end,
                active_period_start=config.active_period.start_date,
            )
        periods = self._build_periods_from_pull_requests(config, collection)
        if periods:
            return periods
        return (config.active_period,)

    def _build_periods_for_explicit_range(
        self,
        *,
        grain: PeriodGrain,
        start_date: date,
        end_date: date,
        active_period_start: date,
    ) -> tuple[ReportingPeriod, ...]:
        periods: list[ReportingPeriod] = []
        current = start_date
        while current <= end_date:
            period_end = grain.end_for(current)
            periods.append(
                ReportingPeriod(
                    grain=grain,
                    start_date=current,
                    end_date=period_end,
                    key=grain.key_for(current),
                    closed=period_end < active_period_start,
                )
            )
            current = self._next_period_start(grain, current)
        return tuple(periods)

    def _build_periods_from_pull_requests(
        self,
        config: RunConfig,
        collection: PullRequestCollection,
    ) -> tuple[ReportingPeriod, ...]:
        start_dates = sorted(
            {
                config.period.start_for(pull_request.updated_at.date())
                for pull_request in collection.pull_requests
            }
        )
        return tuple(
            ReportingPeriod(
                grain=config.period,
                start_date=start_date,
                end_date=config.period.end_for(start_date),
                key=config.period.key_for(start_date),
                closed=config.period.end_for(start_date)
                < config.active_period.start_date,
            )
            for start_date in start_dates
        )

    def _prune_stale_period_directories(
        self,
        *,
        config: RunConfig,
        root_dir: Path,
        snapshot_periods: tuple[ReportingPeriod, ...],
    ) -> None:
        if config.mode is not RunMode.FULL or not root_dir.exists():
            return
        active_period_keys = {
            snapshot_period.key for snapshot_period in snapshot_periods
        }
        for child in root_dir.iterdir():
            if not child.is_dir() or child.name in active_period_keys:
                continue
            shutil.rmtree(child)

    def _write_period_snapshot(
        self,
        *,
        root_dir: Path,
        snapshot_period: ReportingPeriod,
        pull_requests: tuple[PullRequestRecord, ...],
    ) -> RawSnapshotPeriod:
        period_dir = root_dir / snapshot_period.key
        pull_requests_path = period_dir / PULL_REQUEST_SNAPSHOT_FILENAME
        reviews_path = period_dir / PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME
        timeline_events_path = (
            period_dir / PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME
        )
        pull_request_rows = [
            self._pull_request_row(snapshot_period.key, pull_request)
            for pull_request in pull_requests
        ]
        review_rows = [
            review_row
            for pull_request in pull_requests
            for review_row in self._review_rows_for_pull_request(
                snapshot_period.key, pull_request
            )
        ]
        timeline_event_rows = [
            timeline_event_row
            for pull_request in pull_requests
            for timeline_event_row in self._timeline_event_rows_for_pull_request(
                snapshot_period.key, pull_request
            )
        ]
        self._write_rows(
            path=pull_requests_path,
            fieldnames=PULL_REQUEST_FIELDNAMES,
            rows=pull_request_rows,
        )
        self._write_rows(
            path=reviews_path,
            fieldnames=PULL_REQUEST_REVIEW_FIELDNAMES,
            rows=review_rows,
        )
        self._write_rows(
            path=timeline_events_path,
            fieldnames=PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
            rows=timeline_event_rows,
        )
        return RawSnapshotPeriod(
            key=snapshot_period.key,
            start_date=snapshot_period.start_date,
            end_date=snapshot_period.end_date,
            directory=period_dir,
            pull_requests_path=pull_requests_path,
            pull_request_count=len(pull_request_rows),
            reviews_path=reviews_path,
            review_count=len(review_rows),
            timeline_events_path=timeline_events_path,
            timeline_event_count=len(timeline_event_rows),
        )

    def _raw_snapshot_root_dir(self, output_dir: Path, period_grain: str) -> Path:
        return output_dir / RAW_SNAPSHOT_DIRNAME / period_grain

    def _group_pull_requests_by_period(
        self,
        config: RunConfig,
        pull_requests: tuple[PullRequestRecord, ...],
    ) -> dict[str, tuple[PullRequestRecord, ...]]:
        grouped_pull_requests: dict[str, list[PullRequestRecord]] = {}
        for pull_request in pull_requests:
            period_key = config.period.key_for(pull_request.updated_at.date())
            grouped_pull_requests.setdefault(period_key, []).append(pull_request)
        return {
            period_key: tuple(period_pull_requests)
            for period_key, period_pull_requests in grouped_pull_requests.items()
        }

    def _pull_request_row(
        self, period_key: str, pull_request: PullRequestRecord
    ) -> SnapshotRow:
        return {
            "period_key": period_key,
            "repository_full_name": pull_request.repository_full_name,
            "pull_request_number": pull_request.number,
            "title": pull_request.title,
            "state": pull_request.state,
            "draft": pull_request.draft,
            "merged": pull_request.merged,
            "author_login": pull_request.author_login,
            "created_at": self._serialize_datetime(pull_request.created_at),
            "updated_at": self._serialize_datetime(pull_request.updated_at),
            "closed_at": self._serialize_datetime(pull_request.closed_at),
            "merged_at": self._serialize_datetime(pull_request.merged_at),
            "additions": pull_request.additions,
            "deletions": pull_request.deletions,
            "changed_files": pull_request.changed_files,
            "commits": pull_request.commits,
            "html_url": pull_request.html_url,
        }

    def _review_rows_for_pull_request(
        self,
        period_key: str,
        pull_request: PullRequestRecord,
    ) -> tuple[SnapshotRow, ...]:
        return tuple(
            {
                "period_key": period_key,
                "repository_full_name": pull_request.repository_full_name,
                "pull_request_number": pull_request.number,
                "review_id": review.review_id,
                "state": review.state,
                "author_login": review.author_login,
                "submitted_at": self._serialize_datetime(review.submitted_at),
                "commit_id": review.commit_id,
            }
            for review in pull_request.reviews
        )

    def _timeline_event_rows_for_pull_request(
        self,
        period_key: str,
        pull_request: PullRequestRecord,
    ) -> tuple[SnapshotRow, ...]:
        return tuple(
            {
                "period_key": period_key,
                "repository_full_name": pull_request.repository_full_name,
                "pull_request_number": pull_request.number,
                "event_id": timeline_event.event_id,
                "event": timeline_event.event,
                "actor_login": timeline_event.actor_login,
                "created_at": self._serialize_datetime(timeline_event.created_at),
                "requested_reviewer_login": timeline_event.requested_reviewer_login,
                "requested_team_name": timeline_event.requested_team_name,
            }
            for timeline_event in pull_request.timeline_events
        )

    def _write_rows(
        self,
        *,
        path: Path,
        fieldnames: tuple[str, ...],
        rows: list[SnapshotRow],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

    def _serialize_datetime(self, value: datetime | None) -> str:
        if value is None:
            return ""
        return value.isoformat()

    def _next_period_start(
        self,
        grain: PeriodGrain,
        current: date,
    ) -> date:
        if grain is PeriodGrain.MONTH:
            return (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        return current + timedelta(days=7)


class GitHubIngestionService:
    """Load repositories and pull requests with retry and partial-failure handling."""

    def __init__(
        self,
        github_client: Github | GitHubIngestionClientLike,
        *,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._github_client = github_client
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._sleep = sleep
        self._now = now

    def load_repository_inventory(self, config: RunConfig) -> RepositoryInventory:
        """Load and filter repositories for the target organization."""
        try:
            organization = self._load_organization(config.org)
            repositories = self._load_repository_inventory_items(organization, config)
        except GithubException as exc:
            raise GitHubApiError(
                f"GitHub API request failed while loading repositories for '{config.org}': {self._message_for_exception(exc)}"
            ) from exc

        return RepositoryInventory(
            organization_login=organization.login,
            repositories=repositories,
        )

    def _load_organization(self, org: str) -> GitHubOrganizationLike:
        return cast(
            GitHubOrganizationLike,
            self._run_github_operation(
                call=lambda: self._github_client.get_organization(org),
            ),
        )

    def _load_repository_inventory_items(
        self,
        organization: GitHubOrganizationLike,
        config: RunConfig,
    ) -> tuple[RepositoryInventoryItem, ...]:
        def load_repositories() -> tuple[RepositoryInventoryItem, ...]:
            repositories: list[RepositoryInventoryItem] = []
            for repository in organization.get_repos(
                type="all", sort="full_name", direction="asc"
            ):
                if not self._repo_is_selected(
                    config, repository.full_name, repository.name
                ):
                    continue
                repositories.append(self._build_repository_inventory_item(repository))
            return tuple(
                sorted(repositories, key=lambda repository: repository.full_name)
            )

        return self._run_github_operation(
            call=load_repositories,
        )

    def _repo_is_selected(self, config: RunConfig, full_name: str, name: str) -> bool:
        if config.include_repos and not any(
            self._matches_repo_filter(repo_filter, full_name, name, config.org)
            for repo_filter in config.include_repos
        ):
            return False
        if any(
            self._matches_repo_filter(repo_filter, full_name, name, config.org)
            for repo_filter in config.exclude_repos
        ):
            return False
        return True

    def _matches_repo_filter(
        self, repo_filter: str, full_name: str, name: str, org: str
    ) -> bool:
        return repo_filter_matches(
            repo_filter,
            full_name=full_name,
            name=name,
            org=org,
        )

    def _build_repository_inventory_item(
        self, repository: GitHubRepositoryLike
    ) -> RepositoryInventoryItem:
        return RepositoryInventoryItem(
            name=repository.name,
            full_name=repository.full_name,
            default_branch=repository.default_branch,
            private=repository.private,
            archived=repository.archived,
            disabled=repository.disabled,
        )

    def fetch_pull_requests(
        self,
        config: RunConfig,
        inventory: RepositoryInventory,
    ) -> PullRequestCollection:
        """Fetch pull requests for the configured collection window across repositories."""
        pull_requests: list[PullRequestRecord] = []
        failures: list[RepositoryCollectionFailure] = []
        window = config.collection_window

        for repository in inventory.repositories:
            try:
                pull_requests.extend(
                    self._fetch_repository_pull_requests(
                        repository_full_name=repository.full_name,
                        window=window,
                    )
                )
            except GithubException as exc:
                failures.append(
                    self._build_collection_failure(
                        repository_full_name=repository.full_name,
                        operation="pull_requests",
                        exc=exc,
                    )
                )

        return PullRequestCollection(
            window=window,
            pull_requests=tuple(
                sorted(
                    pull_requests,
                    key=lambda pull_request: (
                        pull_request.repository_full_name,
                        pull_request.updated_at,
                        pull_request.number,
                    ),
                )
            ),
            failures=tuple(failures),
        )

    def _fetch_repository_pull_requests(
        self,
        *,
        repository_full_name: str,
        window: CollectionWindow,
    ) -> tuple[PullRequestRecord, ...]:
        repository = self._load_repository(repository_full_name)
        return self._load_pull_requests(repository, window)

    def _load_repository(self, repository_full_name: str) -> GitHubRepositoryLike:
        return cast(
            GitHubRepositoryLike,
            self._run_github_operation(
                call=lambda: self._github_client.get_repo(repository_full_name),
            ),
        )

    def _load_pull_requests(
        self,
        repository: GitHubRepositoryLike,
        window: CollectionWindow,
    ) -> tuple[PullRequestRecord, ...]:
        pull_requests = self._load_pull_request_nodes(repository, window)
        return tuple(
            self._build_pull_request_record(
                repository_full_name=repository.full_name,
                pull_request=pull_request,
            )
            for pull_request in pull_requests
        )

    def _load_pull_request_nodes(
        self,
        repository: GitHubRepositoryLike,
        window: CollectionWindow,
    ) -> tuple[GitHubPullRequestLike, ...]:
        def collect_pull_requests() -> tuple[GitHubPullRequestLike, ...]:
            pull_requests: list[GitHubPullRequestLike] = []
            for pull_request in repository.get_pulls(
                state="all", sort="updated", direction="desc"
            ):
                if not self._pull_request_is_within_window(
                    pull_request.updated_at.date(), window
                ):
                    if self._should_stop_loading_pull_requests(
                        updated_on=pull_request.updated_at.date(),
                        window=window,
                    ):
                        break
                    continue
                pull_requests.append(pull_request)
            return tuple(pull_requests)

        return self._run_github_operation(
            call=collect_pull_requests,
        )

    def _pull_request_is_within_window(
        self, updated_on: date, window: CollectionWindow
    ) -> bool:
        if updated_on > window.end_date:
            return False
        if window.start_date is None:
            return True
        return updated_on >= window.start_date

    def _should_stop_loading_pull_requests(
        self,
        *,
        updated_on: date,
        window: CollectionWindow,
    ) -> bool:
        return window.start_date is not None and updated_on < window.start_date

    def _build_pull_request_record(
        self,
        *,
        repository_full_name: str,
        pull_request: GitHubPullRequestLike,
    ) -> PullRequestRecord:
        author_login = self._login_for(pull_request.user)
        reviews = self._load_pull_request_reviews(pull_request)
        timeline_events = self._load_pull_request_timeline_events(pull_request)

        return PullRequestRecord(
            repository_full_name=repository_full_name,
            number=pull_request.number,
            title=pull_request.title,
            state=pull_request.state,
            draft=pull_request.draft,
            merged=pull_request.merged,
            author_login=author_login,
            created_at=pull_request.created_at,
            updated_at=pull_request.updated_at,
            closed_at=pull_request.closed_at,
            merged_at=pull_request.merged_at,
            additions=pull_request.additions,
            deletions=pull_request.deletions,
            changed_files=pull_request.changed_files,
            commits=pull_request.commits,
            html_url=pull_request.html_url,
            reviews=reviews,
            timeline_events=timeline_events,
        )

    def _load_pull_request_reviews(
        self,
        pull_request: GitHubPullRequestLike,
    ) -> tuple[PullRequestReviewRecord, ...]:
        def collect_reviews() -> tuple[PullRequestReviewRecord, ...]:
            reviews = [
                PullRequestReviewRecord(
                    review_id=review.id,
                    state=review.state,
                    author_login=self._login_for(review.user),
                    submitted_at=review.submitted_at,
                    commit_id=review.commit_id,
                )
                for review in pull_request.get_reviews()
            ]
            return tuple(
                sorted(
                    reviews,
                    key=lambda review: (
                        review.submitted_at.isoformat() if review.submitted_at else "",
                        review.review_id,
                    ),
                )
            )

        return self._run_github_operation(
            call=collect_reviews,
        )

    def _load_pull_request_timeline_events(
        self,
        pull_request: GitHubPullRequestLike,
    ) -> tuple[PullRequestTimelineEventRecord, ...]:
        def collect_timeline_events() -> tuple[PullRequestTimelineEventRecord, ...]:
            issue = pull_request.as_issue()
            timeline_events = [
                PullRequestTimelineEventRecord(
                    event_id=timeline_event.id,
                    event=timeline_event.event,
                    actor_login=self._login_for(timeline_event.actor),
                    created_at=timeline_event.created_at,
                    requested_reviewer_login=self._login_for(
                        timeline_event.requested_reviewer
                    ),
                    requested_team_name=self._team_name_for(
                        timeline_event.requested_team
                    ),
                )
                for timeline_event in issue.get_timeline()
                if timeline_event.event in FIRST_REVIEW_TIMELINE_EVENTS
            ]
            return tuple(
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

        return self._run_github_operation(
            call=collect_timeline_events,
        )

    def _login_for(self, actor: GitHubActorLike | None) -> str | None:
        if actor is None:
            return None
        return actor.login

    def _team_name_for(self, team: GitHubTeamLike | None) -> str | None:
        if team is None:
            return None
        return team.name

    def _build_collection_failure(
        self,
        *,
        repository_full_name: str,
        operation: str,
        exc: GithubException,
    ) -> RepositoryCollectionFailure:
        return RepositoryCollectionFailure(
            repository_full_name=repository_full_name,
            operation=operation,
            status_code=exc.status,
            retriable=self._should_retry(exc),
            message=self._message_for_exception(exc),
        )

    def _run_github_operation(
        self,
        *,
        call: Callable[[], T],
    ) -> T:
        attempt = 0
        while True:
            try:
                return call()
            except GithubException as exc:
                if attempt >= self._max_retries or not self._should_retry(exc):
                    raise
                self._sleep_for_retry(exc, attempt)
                attempt += 1

    def _sleep_for_retry(self, exc: GithubException, attempt: int) -> None:
        self._sleep(self._retry_after_seconds(exc, attempt))

    def _should_retry(self, exc: GithubException) -> bool:
        if exc.status in {500, 502, 503, 504, 429}:
            return True
        if exc.status != 403:
            return False
        message = self._message_for_exception(exc).lower()
        return "rate limit" in message or "abuse" in message

    def _retry_after_seconds(self, exc: GithubException, attempt: int) -> float:
        headers = {key.lower(): value for key, value in (exc.headers or {}).items()}
        retry_after = headers.get("retry-after")
        if retry_after is not None:
            return max(float(retry_after), 0.0)

        rate_limit_reset = headers.get("x-ratelimit-reset")
        if rate_limit_reset is not None:
            return max(float(rate_limit_reset) - self._now(), 0.0)

        return self._retry_backoff_seconds * (2**attempt)

    def _message_for_exception(self, exc: GithubException) -> str:
        data = exc.data
        if isinstance(data, dict):
            message = data.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()
        if isinstance(data, str) and data.strip():
            return data.strip()
        return str(exc)


__all__ = [
    "GitHubIngestionService",
    "NormalizedRawSnapshotWriter",
    "PullRequestCollection",
    "PullRequestRecord",
    "RawSnapshotWriteResult",
    "RepositoryCollectionFailure",
    "RepositoryInventory",
    "RepositoryInventoryItem",
]
