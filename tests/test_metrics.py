from datetime import date, timezone

from orgpulse.common.config import AiConfig
from orgpulse.libs.metrics import Grain, build_report
from tests.factories import merged_pr


def _report(pull_requests, **overrides):
    options = {
        "people": None,
        "grain": Grain.WEEK,
        "count": 2,
        "until": date(2026, 6, 14),
        "timezone": timezone.utc,
        "ai": None,
    }
    options.update(overrides)
    return build_report(pull_requests, **options)


def test_weekly_throughput_is_per_active_person():
    """Weekly rows bucket merged PRs by merge week and divide by the people active
    that week; the summary compares the range with the same-length range before it."""

    # Given
    pull_requests = [
        merged_pr("alice", "2026-06-02T12:00:00", number=1),
        merged_pr("alice", "2026-06-02T13:00:00", number=2),
        merged_pr("bob", "2026-06-03T12:00:00", number=3),
        merged_pr("alice", "2026-06-09T12:00:00", number=4),
        merged_pr("carol", "2026-05-20T12:00:00", number=5),
    ]

    # When
    report = _report(pull_requests)

    # Then
    assert [p["key"] for p in report["periods"]] == ["2026-W23", "2026-W24"]
    assert [
        (p["merged"], p["authors"], p["merged_per_person"]) for p in report["periods"]
    ] == [
        (3, 2, 1.5),
        (1, 1, 1.0),
    ]
    assert report["summary"]["current"]["merged_per_person"] == 1.33
    assert report["summary"]["previous"]["merged"] == 1
    assert [
        (row["login"], row["merged"], row["merged_per_person"])
        for row in report["people"]
    ] == [
        ("alice", 3, 1.5),
        ("bob", 1, 0.5),
    ]


def test_selected_people_match_case_insensitively_and_idle_people_are_listed():
    """Selecting people limits every metric to their PRs, matches logins
    case-insensitively, and still lists a selected person with no PRs."""

    # Given
    pull_requests = [
        merged_pr("Alice", "2026-06-02T12:00:00", number=1),
        merged_pr("bob", "2026-06-03T12:00:00", number=2),
    ]

    # When
    report = _report(pull_requests, people=["alice", "dave"])

    # Then
    assert report["summary"]["current"]["merged"] == 1
    assert [(row["login"], row["merged"]) for row in report["people"]] == [
        ("Alice", 1),
        ("dave", 0),
    ]


def test_size_time_and_review_distributions():
    """Size, lead time, cycle time, and review wait are reported as median and p90,
    and PRs merged without a review are counted as an unreviewed share."""

    # Given
    pull_requests = [
        merged_pr("alice", "2026-06-10T12:00:00", number=1, hours_open=1, additions=10),
        merged_pr("alice", "2026-06-10T12:00:00", number=2, hours_open=2, additions=20),
        merged_pr("alice", "2026-06-10T12:00:00", number=3, hours_open=3, additions=30),
        merged_pr("alice", "2026-06-10T12:00:00", number=4, hours_open=4, additions=40),
        merged_pr(
            "alice",
            "2026-06-10T12:00:00",
            number=5,
            hours_open=10,
            additions=100,
            first_review_at=None,
        ),
    ]

    # When
    current = _report(pull_requests)["summary"]["current"]

    # Then
    assert (current["size_median"], current["size_p90"]) == (30, 76)
    assert (current["lead_time_median"], current["lead_time_p90"]) == (3, 7.6)
    assert current["cycle_time_median"] == 4
    assert current["review_wait_median"] == 1
    assert current["unreviewed_share"] == 20


def test_ai_comparison_uses_each_persons_adoption_date():
    """Each person's window before their adoption date is compared with the most
    recent window, and people missing PRs in either window are excluded."""

    # Given
    ai = AiConfig(
        adopted_on=date(2026, 3, 1),
        window_days=28,
        adopted_on_by_person={"bob": date(2026, 4, 1)},
    )
    pull_requests = [
        merged_pr("alice", "2026-02-10T12:00:00", number=1),
        merged_pr("alice", "2026-02-11T12:00:00", number=2),
        merged_pr("bob", "2026-03-10T12:00:00", number=3),
        merged_pr("bob", "2026-03-11T12:00:00", number=4),
        *(
            merged_pr("alice", f"2026-06-0{day}T12:00:00", number=10 + day)
            for day in range(1, 5)
        ),
        *(
            merged_pr("bob", f"2026-06-0{day}T12:00:00", number=20 + day)
            for day in range(1, 5)
        ),
        merged_pr("carol", "2026-06-05T12:00:00", number=30),
    ]

    # When
    comparison = _report(pull_requests, ai=ai)["ai"]

    # Then
    assert comparison["before"]["merged_per_person"] == 0.5
    assert comparison["after"]["merged_per_person"] == 1.0
    assert [(row["login"], row["adopted_on"]) for row in comparison["people"]] == [
        ("alice", "2026-03-01"),
        ("bob", "2026-04-01"),
    ]
    assert comparison["excluded"] == ["carol"]
