from __future__ import annotations

import sqlite3
from dataclasses import astuple, fields
from datetime import date, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from orgpulse.common.models import PullRequest

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
    PRIMARY KEY (repository, number)
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
