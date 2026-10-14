from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PullRequest:
    """One pull request with the timestamps and size every metric reads."""

    repository: str
    number: int
    title: str
    url: str
    author: str
    created_at: datetime
    updated_at: datetime
    merged_at: datetime | None
    closed_at: datetime | None
    ready_at: datetime | None
    first_commit_at: datetime | None
    first_review_at: datetime | None
    additions: int
    deletions: int
    commits: int

    @property
    def changed_lines(self) -> int:
        return self.additions + self.deletions
