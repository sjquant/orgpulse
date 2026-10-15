from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PullRequest:
    """One merged pull request with the timestamps and size every metric reads."""

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

    @property
    def changed_lines(self) -> int:
        return self.additions + self.deletions
