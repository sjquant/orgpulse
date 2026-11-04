from __future__ import annotations

import sqlite3
from dataclasses import astuple, fields
from datetime import date, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from orgpulse.common.models import PullRequest, Review

SCHEMA = """
CREATE TABLE IF NOT EXISTS pull_requests (
    repository TEXT NOT NULL,
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    author TEXT NOT NULL,
    created_at TEXT NOT NULL,
    ready_at TEXT NOT NULL,
    first_review_at TEXT,
    merged_at TEXT NOT NULL,
    additions INTEGER NOT NULL,
    deletions INTEGER NOT NULL,
    commits INTEGER,
    PRIMARY KEY (repository, number)
);
CREATE TABLE IF NOT EXISTS review_activity (
    repository TEXT NOT NULL, number INTEGER NOT NULL, author TEXT NOT NULL,
    title TEXT NOT NULL, url TEXT NOT NULL, reviewer TEXT NOT NULL,
    submitted_at TEXT NOT NULL, requested_at TEXT,
    PRIMARY KEY (repository, number, reviewer, submitted_at)
);
CREATE TABLE IF NOT EXISTS review_sync (
    repository TEXT PRIMARY KEY, since TEXT NOT NULL, synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS repositories (
    full_name TEXT PRIMARY KEY,
    since TEXT NOT NULL,
    synced_at TEXT NOT NULL
);
"""
COLUMNS = tuple(field.name for field in fields(PullRequest))
DATETIME_COLUMNS = frozenset(name for name in COLUMNS if name.endswith("_at"))


class Store:
    """SQLite-backed pull request store with per-repository sync watermarks."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.executescript(SCHEMA)
        if "commits" not in {
            row[1]
            for row in self._connection.execute("PRAGMA table_info(pull_requests)")
        }:
            with self._connection:
                self._connection.execute("BEGIN IMMEDIATE")
                if "commits" not in {
                    row[1]
                    for row in self._connection.execute(
                        "PRAGMA table_info(pull_requests)"
                    )
                }:
                    self._connection.execute(
                        "ALTER TABLE pull_requests ADD COLUMN commits INTEGER"
                    )

    def __enter__(self) -> Store:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._connection.close()

    def synced_at(self, full_name: str, *, since: date) -> datetime | None:
        """Return the last sync time if it covered history back to `since`."""

        row = self._connection.execute(
            "SELECT since, synced_at FROM repositories WHERE full_name = ?",
            (full_name,),
        ).fetchone()
        if row is None or date.fromisoformat(row[0]) > since:
            return None
        return datetime.fromisoformat(row[1])

    def last_synced_at(self) -> datetime | None:
        row = self._connection.execute(
            "SELECT MAX(synced_at) FROM repositories"
        ).fetchone()
        return datetime.fromisoformat(row[0]) if row[0] else None

    def save(
        self,
        full_name: str,
        pull_requests: list[PullRequest],
        *,
        since: date,
        synced_at: datetime,
    ) -> None:
        placeholders = ", ".join("?" for _ in COLUMNS)
        with self._connection:
            self._connection.executemany(
                f"INSERT OR REPLACE INTO pull_requests ({', '.join(COLUMNS)}) "
                f"VALUES ({placeholders})",
                [_row(pull_request) for pull_request in pull_requests],
            )
            self._connection.execute(
                "INSERT OR REPLACE INTO repositories VALUES (?, ?, ?)",
                (full_name, since.isoformat(), synced_at.isoformat()),
            )

    def repository_names(self) -> list[str]:
        return [
            row[0]
            for row in self._connection.execute("SELECT full_name FROM repositories")
        ]

    def reviews_synced_at(self, repository: str, *, since: date) -> datetime | None:
        row = self._connection.execute(
            "SELECT since, synced_at FROM review_sync WHERE repository = ?",
            (repository,),
        ).fetchone()
        return (
            datetime.fromisoformat(row[1])
            if row and date.fromisoformat(row[0]) <= since
            else None
        )

    def save_reviews(
        self,
        repository: str,
        records: list[tuple[int, list[Review]]],
        *,
        since: date,
        synced_at: datetime,
    ) -> None:
        with self._connection:
            for number, reviews in records:
                self._connection.execute(
                    "DELETE FROM review_activity WHERE repository = ? AND number = ?",
                    (repository, number),
                )
                self._connection.executemany(
                    "INSERT OR REPLACE INTO review_activity VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            review.repository,
                            review.number,
                            review.author,
                            review.title,
                            review.url,
                            review.reviewer,
                            review.submitted_at.isoformat(),
                            review.requested_at.isoformat()
                            if review.requested_at
                            else None,
                        )
                        for review in reviews
                    ],
                )
            self._connection.execute(
                "INSERT OR REPLACE INTO review_sync VALUES (?, ?, ?)",
                (repository, since.isoformat(), synced_at.isoformat()),
            )

    def reviews(self) -> list[Review]:
        return [
            Review(
                repository=row[0],
                number=row[1],
                author=row[2],
                title=row[3],
                url=row[4],
                reviewer=row[5],
                submitted_at=datetime.fromisoformat(row[6]),
                requested_at=datetime.fromisoformat(row[7]) if row[7] else None,
            )
            for row in self._connection.execute(
                "SELECT repository, number, author, title, url, reviewer, submitted_at, requested_at FROM review_activity"
            )
        ]

    def missing_commit_counts(self) -> dict[str, list[int]]:
        missing: dict[str, list[int]] = {}
        for repository, number in self._connection.execute(
            "SELECT repository, number FROM pull_requests WHERE commits IS NULL"
        ):
            missing.setdefault(repository, []).append(number)
        return missing

    def save_commit_counts(self, repository: str, counts: dict[int, int]) -> None:
        with self._connection:
            self._connection.executemany(
                "UPDATE pull_requests SET commits = ? WHERE repository = ? AND number = ?",
                [(count, repository, number) for number, count in counts.items()],
            )

    def merged_counts(self) -> list[tuple[str, str, int]]:
        """Merged PR counts per (author, repository)."""

        return self._connection.execute(
            "SELECT author, repository, COUNT(*) FROM pull_requests "
            "GROUP BY author, repository"
        ).fetchall()

    def pull_requests(self) -> list[PullRequest]:
        rows = self._connection.execute(
            f"SELECT {', '.join(COLUMNS)} FROM pull_requests"
        )
        return [_pull_request(row) for row in rows]


def _pull_request(row: tuple[Any, ...]) -> PullRequest:
    values: dict[str, Any] = dict(zip(COLUMNS, row, strict=True))
    for name in DATETIME_COLUMNS:
        if values[name] is not None:
            values[name] = datetime.fromisoformat(values[name])
    return PullRequest(**values)


def _row(pull_request: PullRequest) -> tuple[object, ...]:
    return tuple(
        value.isoformat() if isinstance(value, datetime) else value
        for value in astuple(pull_request)
    )
