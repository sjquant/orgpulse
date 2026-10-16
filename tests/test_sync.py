from datetime import date, datetime, timedelta, timezone

from orgpulse.apps.sync import sync
from orgpulse.libs.store import Store
from tests.factories import make_config, utc


class FakeGitHub:
    """Serves canned GraphQL pages and records which pull request pages were read."""

    def __init__(self, repositories, pages):
        self.repositories = repositories
        self.pages = pages
        self.calls = []

    def __call__(self, query, variables):
        if "organization(" in query:
            return {
                "organization": {
                    "repositories": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {"nameWithOwner": name, "pushedAt": pushed_at}
                            for name, pushed_at in self.repositories
                        ],
                    }
                }
            }
        full_name = f"{variables['owner']}/{variables['name']}"
        page = int(variables["after"] or 0)
        self.calls.append((full_name, page))
        pages = self.pages[full_name]
        return {
            "repository": {
                "pullRequests": {
                    "pageInfo": {
                        "hasNextPage": page + 1 < len(pages),
                        "endCursor": str(page + 1),
                    },
                    "nodes": pages[page],
                }
            }
        }


def pr_node(
    number,
    *,
    author="alice",
    author_type="User",
    created="2026-06-01T00:00:00Z",
    updated="2026-06-02T00:00:00Z",
    merged="2026-06-02T00:00:00Z",
    ready=None,
    reviews=(),
):
    return {
        "number": number,
        "title": f"Change {number}",
        "url": f"https://github.com/acme/api/pull/{number}",
        "createdAt": created,
        "updatedAt": updated,
        "mergedAt": merged,
        "additions": 5,
        "deletions": 3,
        "author": {"__typename": author_type, "login": author} if author else None,
        "reviews": {
            "nodes": [
                {"submittedAt": at, "author": {"__typename": kind, "login": login}}
                for login, kind, at in reviews
            ]
        },
        "timelineItems": {"nodes": [{"createdAt": ready}] if ready else []},
    }


def _stored(config):
    with Store(config.database) as store:
        return {pr.number: pr for pr in store.pull_requests()}


def _recent(minutes_ago=0):
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()


def test_sync_stores_human_pull_requests_with_review_timing(tmp_path):
    """Sync keeps merged human PRs created since `since` in included, recently pushed
    repos; first review ignores self and bot reviews, ready time follows the
    ready-for-review event, and a deleted author is stored as "ghost"."""

    # Given
    config = make_config(tmp_path, exclude_repos=("legacy",))
    github = FakeGitHub(
        repositories=[
            ("acme/api", "2026-06-02T00:00:00Z"),
            ("acme/legacy", "2026-06-02T00:00:00Z"),
            ("acme/old", "2025-06-01T00:00:00Z"),
        ],
        pages={
            "acme/api": [
                [
                    pr_node(
                        1,
                        reviews=[
                            ("alice", "User", "2026-06-01T01:00:00Z"),
                            ("copilot", "Bot", "2026-06-01T02:00:00Z"),
                            ("bob", "User", "2026-06-01T05:00:00Z"),
                        ],
                    ),
                    pr_node(2, author="dependabot", author_type="Bot"),
                    pr_node(3, merged=None),
                    pr_node(4, ready="2026-06-01T03:00:00Z"),
                    pr_node(6, author=None),
                ],
                [
                    pr_node(
                        5,
                        created="2025-12-01T00:00:00Z",
                        updated="2026-02-01T00:00:00Z",
                    )
                ],
            ],
        },
    )

    # When
    saved = sync(config, github)

    # Then
    stored = _stored(config)
    assert saved == 3
    assert sorted(stored) == [1, 4, 6]
    assert stored[1].first_review_at == utc("2026-06-01T05:00:00")
    assert stored[1].title == "Change 1"
    assert stored[6].author == "ghost"
    assert stored[4].ready_at == utc("2026-06-01T03:00:00")
    assert {name for name, _ in github.calls} == {"acme/api"}


def test_resync_stops_at_previously_synced_updates(tmp_path):
    """A second sync reads pull requests newest-update first, rereads updates from
    shortly before the previous sync, and stops at older updates, keeping earlier rows."""

    # Given: a first sync stored PR 1
    config = make_config(tmp_path)
    repositories = [("acme/api", _recent())]
    sync(config, FakeGitHub(repositories, {"acme/api": [[pr_node(1)]]}))
    github = FakeGitHub(
        repositories,
        {
            "acme/api": [
                [
                    pr_node(2, updated=_recent()),
                    pr_node(3, updated=_recent(minutes_ago=5)),
                    pr_node(1, updated=_recent(minutes_ago=60)),
                ],
                [pr_node(9)],
            ]
        },
    )

    # When
    sync(config, github)

    # Then
    assert github.calls == [("acme/api", 0)]
    assert sorted(_stored(config)) == [1, 2, 3]


def test_moving_since_earlier_refetches_full_history(tmp_path):
    """Lowering `since` below what was synced before ignores the previous sync
    point so older pull requests are backfilled."""

    # Given: a first sync covered history from 2026-01-01
    config = make_config(tmp_path)
    repositories = [("acme/api", _recent())]
    sync(config, FakeGitHub(repositories, {"acme/api": [[pr_node(1)]]}))
    older = pr_node(7, created="2025-06-01T00:00:00Z", updated="2025-06-02T00:00:00Z")

    # When
    sync(
        make_config(tmp_path, since=date(2025, 1, 1)),
        FakeGitHub(repositories, {"acme/api": [[pr_node(1)], [older]]}),
    )

    # Then
    assert sorted(_stored(config)) == [1, 7]
