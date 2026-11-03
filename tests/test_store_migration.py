import sqlite3
from datetime import date, datetime, timezone

from orgpulse.libs.store import Store


def test_existing_database_migrates_without_losing_rows_or_watermarks(tmp_path):
    path = tmp_path / "old.db"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE pull_requests (repository TEXT NOT NULL, number INTEGER NOT NULL, title TEXT NOT NULL, url TEXT NOT NULL, author TEXT NOT NULL, created_at TEXT NOT NULL, ready_at TEXT NOT NULL, first_review_at TEXT, merged_at TEXT NOT NULL, additions INTEGER NOT NULL, deletions INTEGER NOT NULL, PRIMARY KEY(repository,number))"
        )
        db.execute(
            "INSERT INTO pull_requests VALUES ('acme/api', 1, 'Old PR', 'https://example.test/1', 'alice', '2026-06-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00', NULL, '2026-06-02T00:00:00+00:00', 12, 3)"
        )
        db.execute(
            "CREATE TABLE repositories (full_name TEXT PRIMARY KEY, since TEXT NOT NULL, synced_at TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO repositories VALUES ('acme/api','2026-01-01','2026-06-03T00:00:00+00:00')"
        )
    with Store(path) as store:
        assert store.pull_requests()[0].commits is None
        assert store.missing_commit_counts() == {"acme/api": [1]}
        store.save_commit_counts("acme/api", {1: 7})
        pr = store.pull_requests()[0]
        assert (pr.title, pr.changed_lines, pr.commits) == ("Old PR", 15, 7)
        assert store.synced_at("acme/api", since=date(2026, 1, 1)) == datetime(
            2026, 6, 3, tzinfo=timezone.utc
        )
    with Store(path) as store:
        assert not store.missing_commit_counts()
