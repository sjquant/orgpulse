from dataclasses import replace
from datetime import date, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from orgpulse.libs.metrics import Grain, group_report, person_report
from tests.factories import merged_pr


def test_group_throughput_is_per_person_per_elapsed_period():
    """Weekly throughput divides merged PRs by the group's size and by how much of
    each week has elapsed, buckets by local merge date, and lines each week up with
    the matching week of the previous range."""

    # Given: a Wednesday report date in Seoul, so the last week is 3/7 elapsed
    pull_requests = [
        merged_pr("alice", "2026-06-01T12:00:00", number=1),
        # Sunday 16:00 UTC is Monday 01:00 in Seoul.
        merged_pr("bob", "2026-06-07T16:00:00", number=2),
        merged_pr("alice", "2026-06-09T03:00:00", number=3),
        merged_pr("carol", "2026-06-02T12:00:00", number=4),
        merged_pr("alice", "2026-05-27T12:00:00", number=5),
    ]

    # When
    report = group_report(
        pull_requests,
        **_window(
            people=["alice", "bob"],
            until=date(2026, 6, 10),
            timezone=ZoneInfo("Asia/Seoul"),
        ),
    )

    # Then
    periods = report["periods"]
    assert [p["start"] for p in periods] == ["2026-06-01", "2026-06-08"]
    assert [p["partial"] for p in periods] == [False, True]
    assert [p["current"]["merged_per_person"] for p in periods] == [0.5, 2.33]
    assert [p["compare"]["merged"] for p in periods] == [0, 1]
    assert report["summary"]["current"]["merged_per_person"] == 1.05
    assert report["summary"]["compare"]["merged_per_person"] == 0.25
    assert [(row["login"], row["merged_per_person"]) for row in report["people"]] == [
        ("alice", 1.4),
        ("bob", 0.7),
    ]


def test_months_before_since_have_no_data():
    """Months that end before `since` report no data instead of zeros, and the
    previous-range comparison is withheld when it reaches before `since`."""

    # Given
    pull_requests = [
        merged_pr("alice", "2026-02-10T12:00:00", number=1),
        merged_pr("alice", "2026-03-05T12:00:00", number=2),
    ]

    # When
    report = group_report(
        pull_requests,
        **_window(
            people=["alice"],
            grain=Grain.MONTH,
            count=3,
            until=date(2026, 3, 15),
            since=date(2026, 2, 1),
        ),
    )

    # Then
    periods = report["periods"]
    assert [p["start"] for p in periods] == ["2026-01-01", "2026-02-01", "2026-03-01"]
    assert periods[0]["current"] is None
    assert [p["current"]["merged"] for p in periods[1:]] == [1, 1]
    assert report["summary"]["current"]["merged_per_person"] == 1.35
    assert report["summary"]["compare"] is None


def test_review_wait_starts_when_ready_and_skips_unreviewed_prs():
    """Size and time to merge are medians over merged PRs; review wait starts when
    a PR became ready for review and skips PRs merged without a review."""

    # Given
    reviewed = merged_pr("alice", "2026-06-10T12:00:00", number=1, hours_open=1)
    readied_late = merged_pr(
        "alice", "2026-06-10T12:00:00", number=2, hours_open=3, additions=30
    )
    readied_late = replace(
        readied_late,
        ready_at=readied_late.created_at + timedelta(hours=2),
        first_review_at=readied_late.created_at + timedelta(hours=2.5),
    )
    unreviewed = merged_pr(
        "alice",
        "2026-06-10T12:00:00",
        number=3,
        hours_open=5,
        additions=50,
        first_review_at=None,
    )

    # When
    current = group_report(
        [reviewed, readied_late, unreviewed], **_window(people=None)
    )["summary"]["current"]

    # Then
    assert current["merged"] == 3
    assert (current["size"], current["lead_time"]) == (30, 3)
    assert current["review_wait"] == 0.75


def test_person_is_compared_with_their_team():
    """A person's view carries their team's values, lists their newest PRs first,
    and flags PRs past the team's 90th percentile; the group view flags a person
    whose median is over 1.5x the team's."""

    # Given: bob's newest PR is ten times the team's usual size
    pull_requests = [
        *(
            merged_pr("alice", f"2026-06-0{day}T12:00:00", number=day)
            for day in (2, 3, 4, 5)
        ),
        merged_pr("bob", "2026-06-08T12:00:00", number=10),
        merged_pr("bob", "2026-06-09T12:00:00", number=11, additions=100),
    ]

    # When
    person = person_report(pull_requests, **_window(login="Bob", team=["alice", "bob"]))
    group = group_report(pull_requests, **_window(people=["alice", "bob"]))

    # Then
    assert person["summary"]["current"]["size"] == 55
    assert person["summary"]["compare"]["size"] == 10
    assert [(pr["number"], pr["flags"]) for pr in person["recent"]] == [
        (11, ["size"]),
        (10, []),
    ]
    assert {row["login"]: row["flags"] for row in group["people"]} == {
        "alice": [],
        "bob": ["size"],
    }


def _window(**overrides: Any) -> dict[str, Any]:
    return {
        "grain": Grain.WEEK,
        "count": 2,
        "until": date(2026, 6, 14),
        "since": date(2026, 1, 1),
        "timezone": timezone.utc,
        **overrides,
    }
