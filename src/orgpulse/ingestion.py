from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
import time
from typing import Any, TypeVar

from github import Github, GithubException
from pydantic import BaseModel, ConfigDict

from orgpulse.errors import GitHubApiError
from orgpulse.models import CollectionWindow, RunConfig

DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
T = TypeVar("T")


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


class PullRequestCollection(BaseModel):
    model_config = ConfigDict(frozen=True)

    window: CollectionWindow
    pull_requests: tuple[PullRequestRecord, ...]
    failures: tuple[RepositoryCollectionFailure, ...]


class GitHubIngestionService:
    """Load repositories and pull requests with retry and partial-failure handling."""

    def __init__(
        self,
        github_client: Github,
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

    def _load_organization(self, org: str) -> Any:
        return self._run_github_operation(
            call=lambda: self._github_client.get_organization(org),
        )

    def _load_repository_inventory_items(
        self,
        organization: Any,
        config: RunConfig,
    ) -> tuple[RepositoryInventoryItem, ...]:
        def load_repositories() -> tuple[RepositoryInventoryItem, ...]:
            repositories: list[RepositoryInventoryItem] = []
            for repository in organization.get_repos(type="all", sort="full_name", direction="asc"):
                if not self._repo_is_selected(config, repository.full_name, repository.name):
                    continue
                repositories.append(self._build_repository_inventory_item(repository))
            return tuple(sorted(repositories, key=lambda repository: repository.full_name))

        return self._run_github_operation(
            call=load_repositories,
        )

    def _repo_is_selected(self, config: RunConfig, full_name: str, name: str) -> bool:
        if config.include_repos and not any(
            self._matches_repo_filter(repo_filter, full_name, name)
            for repo_filter in config.include_repos
        ):
            return False
        if any(
            self._matches_repo_filter(repo_filter, full_name, name)
            for repo_filter in config.exclude_repos
        ):
            return False
        return True

    def _matches_repo_filter(self, repo_filter: str, full_name: str, name: str) -> bool:
        normalized_filter = repo_filter.lower()
        return normalized_filter in {full_name.lower(), name.lower()}

    def _build_repository_inventory_item(self, repository: Any) -> RepositoryInventoryItem:
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

    def _load_repository(self, repository_full_name: str) -> Any:
        return self._run_github_operation(
            call=lambda: self._github_client.get_repo(repository_full_name),
        )

    def _load_pull_requests(
        self,
        repository: Any,
        window: CollectionWindow,
    ) -> tuple[PullRequestRecord, ...]:
        def collect_pull_requests() -> tuple[PullRequestRecord, ...]:
            pull_requests: list[PullRequestRecord] = []
            for pull_request in repository.get_pulls(state="all", sort="updated", direction="desc"):
                if not self._pull_request_is_within_window(pull_request.updated_at.date(), window):
                    if self._should_stop_loading_pull_requests(
                        updated_on=pull_request.updated_at.date(),
                        window=window,
                    ):
                        break
                    continue
                pull_requests.append(
                    self._build_pull_request_record(
                        repository_full_name=repository.full_name,
                        pull_request=pull_request,
                    )
                )
            return tuple(pull_requests)

        return self._run_github_operation(
            call=collect_pull_requests,
        )

    def _pull_request_is_within_window(self, updated_on: date, window: CollectionWindow) -> bool:
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
        pull_request: Any,
    ) -> PullRequestRecord:
        author = getattr(pull_request, "user", None)
        author_login = None if author is None else getattr(author, "login", None)

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
        )

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
    "PullRequestCollection",
    "PullRequestRecord",
    "RepositoryCollectionFailure",
    "RepositoryInventory",
    "RepositoryInventoryItem",
]
