import json
from datetime import timedelta
from unittest.mock import Mock
from urllib.parse import parse_qs
from zoneinfo import ZoneInfo

from orgpulse.apps.sync import sync
from orgpulse.apps.web.server import _report, export_reports
from orgpulse.common.models import Review
from orgpulse.libs.github import iter_review_activity
from orgpulse.libs.store import Store
from tests.factories import make_config, merged_pr, utc
from tests.test_html_export import parse_snapshot
from tests.test_sync import FakeGitHub, pr_node


def connection(nodes, cursor=None):
    return {
        "nodes": nodes,
        "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
    }


def event(kind, at, login="bob", reviewer_type="User"):
    return {
        "__typename": kind,
        "createdAt": at,
        "requestedReviewer": {"__typename": reviewer_type, "login": login},
    }


def review(login, at, kind="User"):
    return {"author": {"login": login, "__typename": kind}, "submittedAt": at}


def test_review_collection_paginates_matches_re_requests_and_ignores_bots_and_self():
    node = pr_node(1, merged=None)
    node["reviews"] = connection(
        [review("Bob", "2026-06-04T04:00:00Z")], "more-reviews"
    )
    node["timelineItems"] = connection(
        [event("ReviewRequestedEvent", "2026-06-02T00:00:00Z")], "more-events"
    )
    request = Mock(
        side_effect=[
            {"repository": {"pullRequests": connection([node], "more-prs")}},
            {
                "repository": {
                    "pullRequest": {
                        "reviews": connection(
                            [
                                review("bob", "2026-06-04T05:00:00Z"),
                                review("charlie", "2026-06-04T06:00:00Z"),
                                review("alice", "2026-06-04T07:00:00Z"),
                                review("robot", "2026-06-04T07:00:00Z", "Bot"),
                                review("pending", None),
                            ]
                        )
                    }
                }
            },
            {
                "repository": {
                    "pullRequest": {
                        "timelineItems": connection(
                            [
                                event(
                                    "ReviewRequestRemovedEvent", "2026-06-02T12:00:00Z"
                                ),
                                event("ReviewRequestedEvent", "2026-06-03T04:00:00Z"),
                                event(
                                    "ReviewRequestRemovedEvent", "2026-06-04T04:00:00Z"
                                ),
                                event(
                                    "ReviewRequestedEvent",
                                    "2026-06-03T04:00:00Z",
                                    "team",
                                    "Team",
                                ),
                            ]
                        )
                    }
                }
            },
            {
                "repository": {
                    "pullRequests": connection(
                        [pr_node(2, created="2024-01-01T00:00:00Z")]
                    )
                }
            },
        ]
    )
    records = list(
        iter_review_activity(
            request,
            "acme/api",
            updated_since=utc("2026-01-01"),
            created_since=utc("2026-01-01"),
        )
    )
    assert [number for number, _ in records] == [1]
    reviews = records[0][1]
    assert [row.reviewer.lower() for row in reviews] == ["bob", "bob", "charlie"]
    assert reviews[0].requested_at == utc("2026-06-03T04:00:00")
    assert reviews[1].requested_at is None and reviews[2].requested_at is None
    assert request.call_args_list[1].args[1]["reviewAfter"] == "more-reviews"
    assert request.call_args_list[2].args[1]["eventAfter"] == "more-events"
    assert request.call_args_list[3].args[1]["after"] == "more-prs"


def test_review_metrics_use_submission_dates_distinct_pairs_and_known_response_samples(
    tmp_path,
):
    config = make_config(
        tmp_path,
        groups={"team": ("bob", "charlie", "inactive")},
        timezone=ZoneInfo("Asia/Seoul"),
    )
    reviews = [
        Review(
            "acme/api",
            1,
            "alice",
            "Open PR",
            "https://example.com/1",
            "bob",
            utc("2026-06-03T02:00:00"),
            utc("2026-06-03T00:00:00"),
        ),
        Review(
            "acme/api",
            1,
            "alice",
            "Open PR",
            "https://example.com/1",
            "bob",
            utc("2026-06-04T02:00:00"),
        ),
        Review(
            "acme/api",
            2,
            "alice",
            "Voluntary",
            "https://example.com/2",
            "bob",
            utc("2026-06-30T14:59:59"),
        ),
        Review(
            "acme/api",
            3,
            "alice",
            "July",
            "https://example.com/3",
            "bob",
            utc("2026-06-30T15:00:00"),
        ),
        Review(
            "acme/api",
            1,
            "alice",
            "Open PR",
            "https://example.com/1",
            "charlie",
            utc("2026-06-04T02:00:00"),
        ),
        Review(
            "acme/api",
            1,
            "alice",
            "Open PR",
            "https://example.com/1",
            "outside",
            utc("2026-06-04T02:00:00"),
        ),
    ]
    with Store(config.database) as store:
        store.save("acme/api", [], since=config.since, synced_at=utc("2026-07-01"))
        store.save_reviews(
            "acme/api",
            [
                (number, [review for review in reviews if review.number == number])
                for number in (1, 2, 3)
            ],
            since=config.since,
            synced_at=utc("2026-07-01"),
        )
    query = {
        "group": ["team"],
        "grain": ["month"],
        "count": ["1"],
        "until": ["2026-06-30"],
    }
    group = _report(config, query)
    assert group["review_data_available"]
    assert group["summary"]["current"]["reviewed_prs"] == 3
    assert [(row["login"], row["reviewed_prs"]) for row in group["people"]] == [
        ("bob", 2),
        ("charlie", 1),
        ("inactive", 0),
    ]
    person = _report(config, {**query, "person": ["bob"]})
    stats = person["summary"]["current"]
    assert stats["reviewed_prs"] == 2 and stats["review_response"] == 2
    assert stats["analysis_samples"]["review_response"] == {
        "total": 1,
        "analyzed": 1,
        "excluded": 0,
        "missing": 1,
    }
    assert person["summary"]["compare"]["reviewed_prs"] == 1
    assert len(person["review_recent"]) == 2
    assert person["summary"]["current"]["merged"] == 0
    body, _, mime = export_reports(
        config, {**query, "format": ["html"]}, lambda people: {}
    )
    frozen, external = parse_snapshot(body.decode())
    assert not external and mime.startswith("text/html")
    assert frozen["reports"]["median:0"]["summary"]["current"]["reviewed_prs"] == 3
    assert frozen["reports"]["mean:1"]["people"][0]["reviewed_prs"] == 2
    assert parse_qs(frozen["query"])["group"] == ["team"]


def test_missing_review_history_stays_unknown_and_response_outliers_do_not_change_counts(
    tmp_path,
):
    config = make_config(tmp_path, groups={"team": ("bob",)})
    query = {
        "person": ["bob"],
        "grain": ["month"],
        "count": ["1"],
        "until": ["2026-06-30"],
    }
    assert _report(config, query)["summary"]["current"]["reviewed_prs"] is None
    records = [
        Review(
            "acme/api",
            n,
            "alice",
            str(n),
            f"https://example.com/{n}",
            "bob",
            utc("2026-06-10T12:00:00"),
            utc("2026-06-10T12:00:00") - timedelta(hours=hours),
        )
        for n, hours in enumerate([1, 1, 2, 2, 100])
    ]
    with Store(config.database) as store:
        store.save("acme/api", [], since=config.since, synced_at=utc("2026-07-01"))
        store.save_reviews(
            "acme/api",
            [(row.number, [row]) for row in records],
            since=config.since,
            synced_at=utc("2026-07-01"),
        )
    filtered = _report(
        config,
        {
            **query,
            "analysis": [
                json.dumps(
                    {"review_response": {"statistic": "mean", "exclude_outliers": True}}
                )
            ],
        },
    )["summary"]["current"]
    assert filtered["reviewed_prs"] == 5
    assert filtered["review_response"] == 1.5
    assert filtered["analysis_samples"]["review_response"]["excluded"] == 1
    with Store(config.database) as store:
        store.save_reviews(
            "acme/api", [(0, [])], since=config.since, synced_at=utc("2026-07-02")
        )
        assert all(row.number != 0 for row in store.reviews())


def test_sync_backfills_review_history_independently_of_author_watermark(tmp_path):
    config = make_config(tmp_path)
    with Store(config.database) as store:
        store.save(
            "acme/api",
            [merged_pr("alice", "2026-06-02T00:00:00", number=1, commits=3)],
            since=config.since,
            synced_at=utc("2026-06-20"),
        )
    node = pr_node(
        1,
        updated="2026-06-03T00:00:00Z",
        reviews=[("bob", "User", "2026-06-02T00:00:00Z")],
    )
    fake = FakeGitHub([("acme/api", "2026-06-21T00:00:00Z")], {"acme/api": [[node]]})
    sync(config, fake)
    with Store(config.database) as store:
        assert len(store.reviews()) == 1
        assert store.reviews_synced_at("acme/api", since=config.since) is not None
        assert store.pull_requests()[0].commits == 3


def test_review_only_roster_and_custom_previous_window(tmp_path):
    config = make_config(tmp_path)
    review = Review(
        "acme/api",
        1,
        "alice",
        "Not merged",
        "https://example.com/1",
        "bob",
        utc("2026-06-10T02:00:00"),
    )
    with Store(config.database) as store:
        store.save(
            "acme/api",
            [merged_pr("historical", "2026-02-01T00:00:00")],
            since=config.since,
            synced_at=utc("2026-07-01"),
        )
        store.save_reviews(
            "acme/api", [(1, [review])], since=config.since, synced_at=utc("2026-07-01")
        )
    active = _report(config, {"from": ["2026-06-01"], "until": ["2026-06-14"]})
    assert [row["login"] for row in active["people"]] == ["bob"]
    assert active["summary"]["current"]["reviewed_prs"] == 1
    # Configured rosters keep zero-activity people, so previous-range reviews remain comparable.
    from dataclasses import replace

    configured = replace(config, groups={"team": ("bob",)})
    later = _report(
        configured, {"group": ["team"], "from": ["2026-06-15"], "until": ["2026-06-30"]}
    )
    assert later["summary"]["current"]["reviewed_prs"] == 0
    assert later["summary"]["compare"]["reviewed_prs"] == 1
