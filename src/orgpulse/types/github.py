from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Protocol


class GitHubActorLike(Protocol):
    login: str


class GitHubTeamLike(Protocol):
    name: str


class GitHubReviewLike(Protocol):
    id: int
    state: str
    user: GitHubActorLike | None
    submitted_at: datetime | None
    commit_id: str | None


class GitHubTimelineEventLike(Protocol):
    id: int
    event: str
    actor: GitHubActorLike | None
    created_at: datetime | None
    requested_reviewer: GitHubActorLike | None
    requested_team: GitHubTeamLike | None


class GitHubIssueLike(Protocol):
    def get_timeline(self) -> Iterable[GitHubTimelineEventLike]: ...


class GitHubPullRequestLike(Protocol):
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


class GitHubOrganizationLike(Protocol):
    login: str

    def get_repos(
        self,
        *,
        type: str,
        sort: str,
        direction: str,
    ) -> Iterable[GitHubRepositoryLike]: ...


class GitHubIngestionClientLike(Protocol):
    def get_organization(self, org: str) -> GitHubOrganizationLike: ...

    def get_repo(self, full_name: str) -> GitHubRepositoryLike: ...


class GitHubAuthClientLike(Protocol):
    def get_user(self) -> GitHubActorLike: ...

    def get_organization(self, org: str) -> GitHubOrganizationLike: ...
