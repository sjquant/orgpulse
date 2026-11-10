from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PullRequest:
    """A merged pull request for author-side flow and volume metrics."""

    repository: str
    number: int
    title: str
    url: str
    author: str
    created_at: datetime
    ready_at: datetime
    first_review_at: datetime | None
    merged_at: datetime
    additions: int
    deletions: int
    commits: int | None = None

    @property
    def changed_lines(self) -> int:
        return self.additions + self.deletions


@dataclass(frozen=True)
class Review:
    """A submitted human review, optionally answering an individual request."""

    repository: str
    number: int
    author: str
    title: str
    url: str
    reviewer: str
    submitted_at: datetime
    requested_at: datetime | None = None
