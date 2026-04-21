from __future__ import annotations

import hashlib
import json
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar, cast

from github import Github, GithubException
from requests.exceptions import RequestException

from orgpulse.errors import GitHubApiError
from orgpulse.files import atomic_write_csv, atomic_write_json
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
    TimeAnchor,
    canonicalize_repo_filter,
    repo_filter_matches,
)
from orgpulse.types.github import (
    GitHubActorLike,
    GitHubIngestionClientLike,
    GitHubOrganizationLike,
    GitHubPullRequestLike,
    GitHubRepositoryLike,
    GitHubTeamLike,
    GraphQLRequesterLike,
)

DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
RAW_SNAPSHOT_DIRNAME = "raw"
CHECKPOINT_DIRNAME = "checkpoints"
CHECKPOINT_MANIFEST_FILENAME = "manifest.json"
CHECKPOINT_REPOSITORY_DIRNAME = "repositories"
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
PULL_REQUEST_GRAPHQL_QUERY_TEMPLATE = """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(
      first: 50
      after: $after
      orderBy: {field: __ORDER_FIELD__, direction: DESC}
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        updatedAt
        createdAt
        closedAt
        mergedAt
        state
        isDraft
        additions
        deletions
        changedFiles
        commits {
          totalCount
        }
        url
        author {
          login
        }
        reviews(first: 50) {
          pageInfo {
            hasNextPage
          }
          nodes {
            databaseId
            state
            submittedAt
            author {
              login
            }
            commit {
              oid
            }
          }
        }
        timelineItems(
          first: 50
          itemTypes: [
            REVIEW_REQUESTED_EVENT
            REVIEW_REQUEST_REMOVED_EVENT
            READY_FOR_REVIEW_EVENT
            CONVERT_TO_DRAFT_EVENT
          ]
        ) {
          pageInfo {
            hasNextPage
          }
          nodes {
            __typename
            ... on ReviewRequestedEvent {
              id
              createdAt
              actor {
                login
              }
              requestedReviewer {
                __typename
                ... on User {
                  login
                }
                ... on Team {
                  name
                }
              }
            }
            ... on ReviewRequestRemovedEvent {
              id
              createdAt
              actor {
                login
              }
              requestedReviewer {
                __typename
                ... on User {
                  login
                }
                ... on Team {
                  name
                }
              }
            }
            ... on ReadyForReviewEvent {
              id
              createdAt
              actor {
                login
              }
            }
            ... on ConvertToDraftEvent {
              id
              createdAt
              actor {
                login
              }
            }
          }
        }
      }
    }
  }
}
"""
SnapshotRow = dict[str, str | int | bool | None]
T = TypeVar("T")


@dataclass(frozen=True)
class _CollectionCheckpoint:
    pull_requests: tuple[PullRequestRecord, ...]
    completed_repositories: frozenset[str]


class NormalizedRawSnapshotWriter:
    """Persist enriched pull request records into period-partitioned raw snapshots."""

    def write(
        self,
        config: RunConfig,
        collection: PullRequestCollection,
    ) -> RawSnapshotWriteResult:
        root_dir = self._raw_snapshot_root_dir(
            config.output_dir,
            config.period.value,
            config.time_anchor.value,
        )
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
            periods = self._build_periods_from_pull_requests(config, collection)
            if not periods:
                return (config.active_period,)
            period_index = {period.key: period for period in periods}
            period_index.setdefault(config.active_period.key, config.active_period)
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
                config.period.start_for(anchor_at.date())
                for pull_request in collection.pull_requests
                if (
                    anchor_at := self._anchor_datetime(
                        config.time_anchor,
                        pull_request,
                    )
                )
                is not None
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

    def _raw_snapshot_root_dir(
        self,
        output_dir: Path,
        period_grain: str,
        time_anchor: str,
    ) -> Path:
        return output_dir / RAW_SNAPSHOT_DIRNAME / period_grain / time_anchor

    def _group_pull_requests_by_period(
        self,
        config: RunConfig,
        pull_requests: tuple[PullRequestRecord, ...],
    ) -> dict[str, tuple[PullRequestRecord, ...]]:
        grouped_pull_requests: dict[str, list[PullRequestRecord]] = {}
        for pull_request in pull_requests:
            anchor_at = self._anchor_datetime(config.time_anchor, pull_request)
            if anchor_at is None:
                continue
            period_key = config.period.key_for(anchor_at.date())
            grouped_pull_requests.setdefault(period_key, []).append(pull_request)
        return {
            period_key: tuple(period_pull_requests)
            for period_key, period_pull_requests in grouped_pull_requests.items()
        }

    def _anchor_datetime(
        self,
        time_anchor: TimeAnchor,
        pull_request: PullRequestRecord,
    ) -> datetime | None:
        return time_anchor.pull_request_datetime(pull_request)

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
        atomic_write_csv(path=path, fieldnames=fieldnames, rows=rows)

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
        requester = github_client.requester
        self._graphql_requester: GraphQLRequesterLike | None = requester
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
        except RequestException as exc:
            raise GitHubApiError(
                f"GitHub API request failed while loading repositories for '{config.org}': {exc}"
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
        checkpoint = self._load_collection_checkpoint(config)
        pull_requests = list(checkpoint.pull_requests)
        failures: list[RepositoryCollectionFailure] = []
        window = config.collection_window

        for repository in inventory.repositories:
            if repository.full_name in checkpoint.completed_repositories:
                continue
            try:
                repository_pull_requests = self._fetch_repository_pull_requests(
                    config=config,
                    repository_full_name=repository.full_name,
                    window=window,
                )
                pull_requests.extend(repository_pull_requests)
                self._save_collection_checkpoint(
                    config,
                    repository_full_name=repository.full_name,
                    pull_requests=repository_pull_requests,
                )
            except (GithubException, RequestException) as exc:
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
                        self._anchor_datetime(
                            config.time_anchor,
                            pull_request,
                        )
                        or pull_request.updated_at,
                        pull_request.number,
                    ),
                )
            ),
            failures=tuple(failures),
        )

    def clear_checkpoint(self, config: RunConfig) -> None:
        """Delete any repo-scoped checkpoint state for the current run contract."""
        checkpoint_root_dir = self._checkpoint_root_dir(config)
        if checkpoint_root_dir.exists():
            shutil.rmtree(checkpoint_root_dir)

    def _load_collection_checkpoint(
        self,
        config: RunConfig,
    ) -> _CollectionCheckpoint:
        if config.checkpoint_policy.overwrite_checkpoint:
            self.clear_checkpoint(config)
            return _CollectionCheckpoint(
                pull_requests=(),
                completed_repositories=frozenset(),
            )
        if not config.checkpoint_policy.resume_from_checkpoint:
            return _CollectionCheckpoint(
                pull_requests=(),
                completed_repositories=frozenset(),
            )
        manifest_payload = self._load_checkpoint_manifest_payload(config)
        if manifest_payload is None:
            return _CollectionCheckpoint(
                pull_requests=(),
                completed_repositories=frozenset(),
            )
        if manifest_payload.get("contract") != self._checkpoint_contract(config):
            self.clear_checkpoint(config)
            return _CollectionCheckpoint(
                pull_requests=(),
                completed_repositories=frozenset(),
            )
        completed_repositories = self._completed_checkpoint_repositories(
            config,
            manifest_payload,
        )
        pull_requests: list[PullRequestRecord] = []
        for repository_full_name in completed_repositories:
            checkpoint_pull_requests = self._load_checkpoint_pull_requests(
                config,
                repository_full_name=repository_full_name,
            )
            if checkpoint_pull_requests is None:
                continue
            pull_requests.extend(checkpoint_pull_requests)
        return _CollectionCheckpoint(
            pull_requests=tuple(pull_requests),
            completed_repositories=frozenset(completed_repositories),
        )

    def _save_collection_checkpoint(
        self,
        config: RunConfig,
        *,
        repository_full_name: str,
        pull_requests: tuple[PullRequestRecord, ...],
    ) -> None:
        if not config.checkpoint_policy.persist_checkpoint:
            return
        repository_path = self._checkpoint_repository_path(
            config,
            repository_full_name=repository_full_name,
        )
        atomic_write_json(
            repository_path,
            {
                "repository_full_name": repository_full_name,
                "pull_requests": [
                    pull_request.model_dump(mode="json")
                    for pull_request in pull_requests
                ],
            },
        )
        completed_repositories = self._completed_checkpoint_repositories(
            config,
            self._load_checkpoint_manifest_payload(config),
        )
        completed_repository_set = set(completed_repositories)
        completed_repository_set.add(repository_full_name)
        atomic_write_json(
            self._checkpoint_manifest_path(config),
            {
                "contract": self._checkpoint_contract(config),
                "completed_repositories": sorted(completed_repository_set),
            },
        )

    def _load_checkpoint_manifest_payload(
        self,
        config: RunConfig,
    ) -> dict[str, object] | None:
        manifest_path = self._checkpoint_manifest_path(config)
        if not manifest_path.exists():
            return None
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        return cast(dict[str, object], payload)

    def _completed_checkpoint_repositories(
        self,
        config: RunConfig,
        manifest_payload: dict[str, object] | None,
    ) -> tuple[str, ...]:
        if manifest_payload is None:
            return ()
        repository_names = manifest_payload.get("completed_repositories")
        if not isinstance(repository_names, list):
            return ()
        valid_repositories: list[str] = []
        for repository_name in repository_names:
            if not isinstance(repository_name, str):
                continue
            if self._load_checkpoint_pull_requests(
                config,
                repository_full_name=repository_name,
            ) is None:
                continue
            valid_repositories.append(repository_name)
        return tuple(sorted(set(valid_repositories)))

    def _load_checkpoint_pull_requests(
        self,
        config: RunConfig,
        *,
        repository_full_name: str,
    ) -> tuple[PullRequestRecord, ...] | None:
        repository_path = self._checkpoint_repository_path(
            config,
            repository_full_name=repository_full_name,
        )
        if not repository_path.exists():
            return None
        try:
            payload = json.loads(repository_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("repository_full_name") != repository_full_name:
            return None
        pull_request_payloads = payload.get("pull_requests")
        if not isinstance(pull_request_payloads, list):
            return None
        try:
            return tuple(
                PullRequestRecord.model_validate(pull_request_payload)
                for pull_request_payload in pull_request_payloads
            )
        except Exception:
            return None

    def _checkpoint_root_dir(
        self,
        config: RunConfig,
    ) -> Path:
        return (
            config.output_dir
            / CHECKPOINT_DIRNAME
            / config.period.value
            / config.time_anchor.value
            / config.mode.value
            / config.org
        )

    def _checkpoint_manifest_path(
        self,
        config: RunConfig,
    ) -> Path:
        return self._checkpoint_root_dir(config) / CHECKPOINT_MANIFEST_FILENAME

    def _checkpoint_repository_path(
        self,
        config: RunConfig,
        *,
        repository_full_name: str,
    ) -> Path:
        return (
            self._checkpoint_root_dir(config)
            / CHECKPOINT_REPOSITORY_DIRNAME
            / f"{self._stable_repo_checkpoint_key(repository_full_name)}.json"
        )

    def _checkpoint_contract(
        self,
        config: RunConfig,
    ) -> dict[str, object]:
        return {
            "target_org": config.org,
            "period_grain": config.period.value,
            "time_anchor": config.time_anchor.value,
            "mode": config.mode.value,
            "include_repos": self._canonical_checkpoint_repo_filters(
                config.include_repos,
                org=config.org,
            ),
            "exclude_repos": self._canonical_checkpoint_repo_filters(
                config.exclude_repos,
                org=config.org,
            ),
            "collection_window": self._checkpoint_collection_window_contract(config),
        }

    def _canonical_checkpoint_repo_filters(
        self,
        repo_filters: tuple[str, ...],
        *,
        org: str,
    ) -> list[str]:
        return sorted(
            canonicalize_repo_filter(repo_filter, org=org)
            for repo_filter in repo_filters
        )

    def _checkpoint_collection_window_contract(
        self,
        config: RunConfig,
    ) -> dict[str, object]:
        if config.mode is RunMode.INCREMENTAL:
            return {
                "scope": config.collection_window.scope.value,
                "start_date": None
                if config.collection_window.start_date is None
                else config.collection_window.start_date.isoformat(),
            }
        return config.collection_window.model_dump(mode="json")

    def _stable_repo_checkpoint_key(
        self,
        repository_full_name: str,
    ) -> str:
        return hashlib.blake2b(
            repository_full_name.encode("utf-8"),
            digest_size=8,
        ).hexdigest()

    def _fetch_repository_pull_requests(
        self,
        *,
        config: RunConfig,
        repository_full_name: str,
        window: CollectionWindow,
    ) -> tuple[PullRequestRecord, ...]:
        repository = self._load_repository(repository_full_name)
        return self._load_pull_requests(config, repository, window)

    def _load_repository(self, repository_full_name: str) -> GitHubRepositoryLike:
        return cast(
            GitHubRepositoryLike,
            self._run_github_operation(
                call=lambda: self._github_client.get_repo(repository_full_name),
            ),
        )

    def _load_pull_requests(
        self,
        config: RunConfig,
        repository: GitHubRepositoryLike,
        window: CollectionWindow,
    ) -> tuple[PullRequestRecord, ...]:
        if self._graphql_requester is not None:
            return self._load_pull_requests_via_graphql(config, repository, window)

        pull_requests = self._load_pull_request_nodes(config, repository, window)
        return tuple(
            self._build_pull_request_record(
                repository_full_name=repository.full_name,
                pull_request=pull_request,
            )
            for pull_request in pull_requests
        )

    def _load_pull_requests_via_graphql(
        self,
        config: RunConfig,
        repository: GitHubRepositoryLike,
        window: CollectionWindow,
    ) -> tuple[PullRequestRecord, ...]:
        owner, repository_name = repository.full_name.split("/", 1)
        collection_time_anchor = self._collection_time_anchor(config)
        pull_request_nodes = self._load_pull_request_nodes_via_graphql(
            collection_time_anchor=collection_time_anchor,
            owner=owner,
            repository_name=repository_name,
            window=window,
        )
        return tuple(
            self._build_graphql_pull_request_record(
                repository=repository,
                pull_request_node=pull_request_node,
            )
            for pull_request_node in pull_request_nodes
        )

    def _load_pull_request_nodes_via_graphql(
        self,
        *,
        collection_time_anchor: TimeAnchor,
        owner: str,
        repository_name: str,
        window: CollectionWindow,
    ) -> tuple[dict[str, Any], ...]:
        pull_requests: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            response = self._run_github_operation(
                call=lambda: self._graphql_query(
                    time_anchor=collection_time_anchor,
                    owner=owner,
                    repository_name=repository_name,
                    cursor=cursor,
                ),
            )
            pull_request_connection = response["data"]["repository"]["pullRequests"]
            stop_loading = False
            for pull_request_node in pull_request_connection["nodes"]:
                anchor_on = self._graphql_anchor_date(
                    collection_time_anchor,
                    pull_request_node,
                )
                if not self._pull_request_is_within_window(anchor_on, window):
                    if self._should_stop_loading_pull_requests(
                        time_anchor=collection_time_anchor,
                        anchor_on=anchor_on,
                        window=window,
                    ):
                        stop_loading = True
                        break
                    continue
                pull_requests.append(pull_request_node)

            if stop_loading or not pull_request_connection["pageInfo"]["hasNextPage"]:
                break
            cursor = pull_request_connection["pageInfo"]["endCursor"]

        return tuple(pull_requests)

    def _graphql_query(
        self,
        *,
        time_anchor: TimeAnchor,
        owner: str,
        repository_name: str,
        cursor: str | None,
    ) -> dict[str, Any]:
        assert self._graphql_requester is not None
        graphql_query = PULL_REQUEST_GRAPHQL_QUERY_TEMPLATE.replace(
            "__ORDER_FIELD__",
            time_anchor.github_graphql_order_field(),
        )
        _, response = self._graphql_requester.graphql_query(
            graphql_query,
            {
                "owner": owner,
                "name": repository_name,
                "after": cursor,
            },
        )
        return response

    def _build_graphql_pull_request_record(
        self,
        *,
        repository: GitHubRepositoryLike,
        pull_request_node: dict[str, Any],
    ) -> PullRequestRecord:
        if self._graphql_pull_request_requires_rest_fallback(pull_request_node):
            pull_request = self._load_pull_request_by_number(
                repository,
                pull_request_number=pull_request_node["number"],
            )
            return self._build_pull_request_record(
                repository_full_name=repository.full_name,
                pull_request=pull_request,
            )

        merged_at = self._optional_graphql_datetime(pull_request_node["mergedAt"])
        state, merged = self._graphql_pull_request_state(
            pull_request_node["state"],
            merged_at=merged_at,
        )
        return PullRequestRecord(
            repository_full_name=repository.full_name,
            number=pull_request_node["number"],
            title=pull_request_node["title"],
            state=state,
            draft=pull_request_node["isDraft"],
            merged=merged,
            author_login=self._graphql_actor_login(pull_request_node.get("author")),
            created_at=self._parse_graphql_datetime(pull_request_node["createdAt"]),
            updated_at=self._parse_graphql_datetime(pull_request_node["updatedAt"]),
            closed_at=self._optional_graphql_datetime(pull_request_node["closedAt"]),
            merged_at=merged_at,
            additions=pull_request_node["additions"],
            deletions=pull_request_node["deletions"],
            changed_files=pull_request_node["changedFiles"],
            commits=pull_request_node["commits"]["totalCount"],
            html_url=pull_request_node["url"],
            reviews=self._build_graphql_reviews(pull_request_node["reviews"]["nodes"]),
            timeline_events=self._build_graphql_timeline_events(
                pull_request_node["timelineItems"]["nodes"]
            ),
        )

    def _graphql_pull_request_requires_rest_fallback(
        self,
        pull_request_node: dict[str, Any],
    ) -> bool:
        return (
            pull_request_node["reviews"]["pageInfo"]["hasNextPage"]
            or pull_request_node["timelineItems"]["pageInfo"]["hasNextPage"]
        )

    def _load_pull_request_by_number(
        self,
        repository: GitHubRepositoryLike,
        *,
        pull_request_number: int,
    ) -> GitHubPullRequestLike:
        return self._run_github_operation(
            call=lambda: repository.get_pull(pull_request_number),
        )

    def _graphql_pull_request_state(
        self,
        graphql_state: str,
        *,
        merged_at: datetime | None,
    ) -> tuple[str, bool]:
        if merged_at is not None or graphql_state == "MERGED":
            return "closed", True
        if graphql_state == "OPEN":
            return "open", False
        return "closed", False

    def _build_graphql_reviews(
        self,
        review_nodes: list[dict[str, Any]],
    ) -> tuple[PullRequestReviewRecord, ...]:
        reviews = [
            PullRequestReviewRecord(
                review_id=review_node["databaseId"],
                state=review_node["state"],
                author_login=self._graphql_actor_login(review_node.get("author")),
                submitted_at=self._optional_graphql_datetime(
                    review_node["submittedAt"]
                ),
                commit_id=self._graphql_commit_oid(review_node.get("commit")),
            )
            for review_node in review_nodes
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

    def _graphql_commit_oid(
        self,
        commit_node: dict[str, Any] | None,
    ) -> str | None:
        if commit_node is None:
            return None
        commit_oid = commit_node.get("oid")
        if isinstance(commit_oid, str):
            return commit_oid
        return None

    def _build_graphql_timeline_events(
        self,
        timeline_nodes: list[dict[str, Any]],
    ) -> tuple[PullRequestTimelineEventRecord, ...]:
        timeline_events = [
            PullRequestTimelineEventRecord(
                event_id=self._graphql_timeline_event_id(timeline_node),
                event=self._graphql_timeline_event_name(timeline_node["__typename"]),
                actor_login=self._graphql_actor_login(timeline_node.get("actor")),
                created_at=self._optional_graphql_datetime(timeline_node["createdAt"]),
                requested_reviewer_login=self._graphql_requested_reviewer_login(
                    timeline_node.get("requestedReviewer")
                ),
                requested_team_name=self._graphql_requested_team_name(
                    timeline_node.get("requestedReviewer")
                ),
            )
            for timeline_node in timeline_nodes
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

    def _graphql_timeline_event_id(
        self,
        timeline_node: dict[str, Any],
    ) -> int:
        node_id = timeline_node.get("id")
        if isinstance(node_id, str):
            return self._stable_int_id(node_id)
        event_signature = "|".join(
            str(value)
            for value in (
                timeline_node.get("__typename"),
                timeline_node.get("createdAt"),
                self._graphql_actor_login(timeline_node.get("actor")),
                self._graphql_requested_reviewer_login(
                    timeline_node.get("requestedReviewer")
                ),
                self._graphql_requested_team_name(timeline_node.get("requestedReviewer")),
            )
        )
        return self._stable_int_id(event_signature)

    def _stable_int_id(
        self,
        value: str,
    ) -> int:
        return int.from_bytes(
            hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest(),
            byteorder="big",
        )

    def _graphql_timeline_event_name(
        self,
        typename: str,
    ) -> str:
        if typename == "ReviewRequestedEvent":
            return "review_requested"
        if typename == "ReviewRequestRemovedEvent":
            return "review_request_removed"
        if typename == "ReadyForReviewEvent":
            return "ready_for_review"
        if typename == "ConvertToDraftEvent":
            return "converted_to_draft"
        raise AssertionError(f"unsupported GraphQL timeline event type: {typename}")

    def _graphql_actor_login(
        self,
        actor_node: dict[str, Any] | None,
    ) -> str | None:
        if actor_node is None:
            return None
        actor_login = actor_node.get("login")
        if isinstance(actor_login, str):
            return actor_login
        return None

    def _graphql_requested_reviewer_login(
        self,
        reviewer_node: dict[str, Any] | None,
    ) -> str | None:
        if reviewer_node is None or reviewer_node.get("__typename") != "User":
            return None
        reviewer_login = reviewer_node.get("login")
        if isinstance(reviewer_login, str):
            return reviewer_login
        return None

    def _graphql_requested_team_name(
        self,
        reviewer_node: dict[str, Any] | None,
    ) -> str | None:
        if reviewer_node is None or reviewer_node.get("__typename") != "Team":
            return None
        team_name = reviewer_node.get("name")
        if isinstance(team_name, str):
            return team_name
        return None

    def _parse_graphql_datetime(
        self,
        value: str,
    ) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _optional_graphql_datetime(
        self,
        value: str | None,
    ) -> datetime | None:
        if value is None:
            return None
        return self._parse_graphql_datetime(value)

    def _load_pull_request_nodes(
        self,
        config: RunConfig,
        repository: GitHubRepositoryLike,
        window: CollectionWindow,
    ) -> tuple[GitHubPullRequestLike, ...]:
        collection_time_anchor = self._collection_time_anchor(config)

        def collect_pull_requests() -> tuple[GitHubPullRequestLike, ...]:
            pull_requests: list[GitHubPullRequestLike] = []
            for pull_request in repository.get_pulls(
                state="all",
                sort=collection_time_anchor.github_rest_sort(),
                direction="desc",
            ):
                anchor_on = self._anchor_date(collection_time_anchor, pull_request)
                if not self._pull_request_is_within_window(anchor_on, window):
                    if self._should_stop_loading_pull_requests(
                        time_anchor=collection_time_anchor,
                        anchor_on=anchor_on,
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
        self,
        anchor_on: date | None,
        window: CollectionWindow,
    ) -> bool:
        if anchor_on is None:
            return False
        if anchor_on > window.end_date:
            return False
        if window.start_date is None:
            return True
        return anchor_on >= window.start_date

    def _should_stop_loading_pull_requests(
        self,
        *,
        time_anchor: TimeAnchor,
        anchor_on: date | None,
        window: CollectionWindow,
    ) -> bool:
        return (
            time_anchor.supports_early_stop()
            and window.start_date is not None
            and anchor_on is not None
            and anchor_on < window.start_date
        )

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

    def _anchor_datetime(
        self,
        time_anchor: TimeAnchor,
        pull_request: PullRequestRecord,
    ) -> datetime | None:
        return time_anchor.pull_request_datetime(pull_request)

    def _collection_time_anchor(
        self,
        config: RunConfig,
    ) -> TimeAnchor:
        if config.mode is RunMode.INCREMENTAL:
            return TimeAnchor.UPDATED_AT
        return config.time_anchor

    def _anchor_date(
        self,
        time_anchor: TimeAnchor,
        pull_request: GitHubPullRequestLike,
    ) -> date | None:
        if time_anchor is TimeAnchor.CREATED_AT:
            return pull_request.created_at.date()
        if time_anchor is TimeAnchor.UPDATED_AT:
            return pull_request.updated_at.date()
        if pull_request.merged_at is None:
            return None
        return pull_request.merged_at.date()

    def _graphql_anchor_date(
        self,
        time_anchor: TimeAnchor,
        pull_request_node: dict[str, Any],
    ) -> date | None:
        if time_anchor is TimeAnchor.CREATED_AT:
            return self._parse_graphql_datetime(pull_request_node["createdAt"]).date()
        if time_anchor is TimeAnchor.UPDATED_AT:
            return self._parse_graphql_datetime(pull_request_node["updatedAt"]).date()
        merged_at = self._optional_graphql_datetime(pull_request_node["mergedAt"])
        if merged_at is None:
            return None
        return merged_at.date()

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
                    requested_reviewer_login=self._requested_reviewer_login_for(
                        timeline_event
                    ),
                    requested_team_name=self._requested_team_name_for(timeline_event),
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

    def _requested_reviewer_login_for(
        self,
        timeline_event,
    ) -> str | None:
        requested_reviewer = getattr(timeline_event, "requested_reviewer", None)
        if requested_reviewer is not None:
            return self._login_for(requested_reviewer)

        raw_data = getattr(timeline_event, "raw_data", {})
        if not isinstance(raw_data, dict):
            return None

        requested_reviewer_data = raw_data.get("requested_reviewer")
        if not isinstance(requested_reviewer_data, dict):
            return None

        reviewer_login = requested_reviewer_data.get("login")
        if isinstance(reviewer_login, str):
            return reviewer_login
        return None

    def _requested_team_name_for(
        self,
        timeline_event,
    ) -> str | None:
        requested_team = getattr(timeline_event, "requested_team", None)
        if requested_team is not None:
            return self._team_name_for(requested_team)

        raw_data = getattr(timeline_event, "raw_data", {})
        if not isinstance(raw_data, dict):
            return None

        requested_team_data = raw_data.get("requested_team")
        if not isinstance(requested_team_data, dict):
            return None

        team_name = requested_team_data.get("name")
        if isinstance(team_name, str):
            return team_name
        return None

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
        exc: GithubException | RequestException,
    ) -> RepositoryCollectionFailure:
        if isinstance(exc, RequestException):
            return RepositoryCollectionFailure(
                repository_full_name=repository_full_name,
                operation=operation,
                status_code=0,
                retriable=True,
                message=str(exc),
            )
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
            except RequestException:
                if attempt >= self._max_retries:
                    raise
                self._sleep(self._retry_backoff_seconds * (2**attempt))
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
