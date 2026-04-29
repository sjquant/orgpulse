from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Protocol


class GitHubActorLike(Protocol):
    """Describe the subset of a GitHub actor object used by ingestion."""

    login: str


class GitHubTeamLike(Protocol):
    """Describe the subset of a GitHub team object used by ingestion."""

    name: str


class GitHubReviewLike(Protocol):
    """Describe the GitHub review attributes consumed during normalization."""

    id: int
    state: str
    user: GitHubActorLike | None
    submitted_at: datetime | None
    commit_id: str | None


class GitHubTimelineEventLike(Protocol):
    """Describe the GitHub timeline attributes consumed during normalization."""

    id: int
    event: str
    actor: GitHubActorLike | None
    created_at: datetime | None
    raw_data: dict[str, object]


class GitHubIssueLike(Protocol):
    """Describe the issue API surface needed to load pull request timelines."""

    def get_timeline(self) -> Iterable[GitHubTimelineEventLike]: ...


class GraphQLRequesterLike(Protocol):
    """Describe the GraphQL requester surface used for repository backfill."""

    def graphql_query(
        self,
        query: str,
        variables: dict[str, object],
    ) -> tuple[dict[str, object], dict[str, object]]: ...


class GitHubPullRequestLike(Protocol):
    """Describe the pull request surface used by ingestion."""

    number: int
    title: str
    state: str
    draft: bool
    merged: bool
    user: GitHubActorLike | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    merged_at: datetime | None
    additions: int
    deletions: int
    changed_files: int
    commits: int
    html_url: str

    def get_reviews(self) -> Iterable[GitHubReviewLike]: ...

    def as_issue(self) -> GitHubIssueLike: ...


class GitHubRepositoryLike(Protocol):
    """Describe the repository surface used by ingestion."""

    name: str
    full_name: str
    default_branch: str
    private: bool
    archived: bool
    disabled: bool

    def get_pulls(
        self,
        *,
        state: str,
        sort: str,
        direction: str,
    ) -> Iterable[GitHubPullRequestLike]: ...

    def get_pull(self, number: int) -> GitHubPullRequestLike: ...


class GitHubOrganizationLike(Protocol):
    """Describe the organization surface used by ingestion."""

    login: str

    def get_repos(
        self,
        *,
        type: str,
        sort: str,
        direction: str,
    ) -> Iterable[GitHubRepositoryLike]: ...


class GitHubIngestionClientLike(Protocol):
    """Describe the GitHub client surface required for ingestion flows."""

    requester: GraphQLRequesterLike | None

    def get_organization(self, org: str) -> GitHubOrganizationLike: ...

    def get_repo(self, full_name: str) -> GitHubRepositoryLike: ...


class GitHubAuthClientLike(Protocol):
    """Describe the GitHub client surface required for auth validation."""

    def get_user(self) -> GitHubActorLike: ...

    def get_organization(self, org: str) -> GitHubOrganizationLike: ...
