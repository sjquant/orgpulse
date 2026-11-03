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


def test_custom_dates_include_only_local_boundary_days_and_compare_equal_days():
    # June 2 16:00 UTC is June 3 in Seoul; June 9 15:00 UTC is June 10.
    prs = [
        merged_pr("alice", stamp, number=i)
        for i, stamp in enumerate(
            [
                "2026-05-27T12:00:00",
                "2026-06-02T12:00:00",
                "2026-06-02T16:00:00",
                "2026-06-09T14:59:00",
                "2026-06-09T15:00:00",
            ],
            1,
        )
    ]
    window = _window(
        start=date(2026, 6, 3), until=date(2026, 6, 9), timezone=ZoneInfo("Asia/Seoul")
    )
    group = group_report(prs, people=["alice"], **window)
    assert group["summary"]["current"]["merged"] == 2
    assert group["summary"]["current"]["merged_per_person"] == 2
    assert group["summary"]["compare"]["merged"] == 2
    assert [p["current"]["merged"] for p in group["periods"]] == [1, 1]
    person = person_report(prs, login="alice", team=["alice"], **window)
    assert [pr["number"] for pr in person["recent"]] == [4, 3]


def test_custom_month_range_uses_fractional_calendar_months():
    prs = [merged_pr("alice", "2026-04-20T12:00:00")]
    report = group_report(
        prs,
        **_window(
            people=["alice"],
            grain=Grain.MONTH,
            start=date(2026, 4, 15),
            until=date(2026, 5, 10),
        ),
    )
    assert report["summary"]["current"]["merged_per_person"] == round(
        1 / (16 / 30 + 10 / 31), 2
    )
    assert [p["start"] for p in report["periods"]] == ["2026-04-15", "2026-05-01"]


def test_report_until_excludes_later_merges_in_the_same_bucket():
    prs = [merged_pr("alice", "2026-06-11T12:00:00")]
    report = group_report(prs, **_window(people=["alice"], until=date(2026, 6, 10)))
    assert report["summary"]["current"]["merged"] == 0


def test_in_progress_month_chart_projects_counts_at_the_current_pace():
    prs = [merged_pr("alice", "2026-11-02T12:00:00", number=i) for i in range(6)]
    window = _window(grain=Grain.MONTH, count=1, until=date(2026, 11, 3))
    group = group_report(prs, people=["alice", "bob"], **window)
    assert group["periods"][0]["current"]["merged"] == 6
    assert group["periods"][0]["current"]["merged_per_person"] == 30
    person = person_report(prs, login="alice", team=["alice", "bob"], **window)
    assert person["periods"][0]["current"]["merged_per_person"] == 60
    assert person["periods"][0]["compare"]["merged_per_person"] == 30
    # Actual totals remain available separately from projected period rates.
    assert person["periods"][0]["current"]["merged"] == 6
    assert person["summary"]["current"]["merged_per_person"] == 60


def test_mean_median_and_iqr_are_metric_local_and_preserve_actual_totals():
    prs = [
        merged_pr(
            "alice", "2026-06-10T12:00:00", number=i, commits=commits, additions=lines
        )
        for i, (commits, lines) in enumerate(
            [(1, 10), (2, 10), (3, 10), (100, 1000), (None, 5)]
        )
    ]
    default = group_report(prs, **_window(people=["alice"]))["summary"]["current"]
    assert default["commits"] == 26.5
    assert default["total_lines"] == 1035
    assert default["analysis_samples"]["commits"] == {
        "total": 4,
        "analyzed": 4,
        "excluded": 0,
        "missing": 1,
    }
    median = person_report(
        prs,
        **_window(
            login="alice", team=["alice"], analysis={"commits": {"statistic": "median"}}
        ),
    )["summary"]["current"]
    assert median["commits"] == 2.5
    filtered = group_report(
        prs,
        **_window(
            people=["alice"],
            analysis={"commits": {"statistic": "mean", "exclude_outliers": True}},
        ),
    )["summary"]["current"]
    assert filtered["commits"] == 2
    assert filtered["analysis_samples"]["commits"]["excluded"] == 1
    assert filtered["total_lines"] == default["total_lines"]
    assert filtered["size"] == default["size"]
    assert filtered["merged"] == default["merged"]
    assert filtered["added_lines"] == 1035 and filtered["deleted_lines"] == 0


def test_unknown_commits_are_not_zero_and_small_samples_are_not_trimmed():
    unknown = group_report(
        [merged_pr("alice", "2026-06-10T12:00:00")], **_window(people=["alice"])
    )["summary"]["current"]
    assert unknown["commits"] is None
    assert unknown["analysis_samples"]["commits"]["missing"] == 1
    prs = [
        merged_pr("alice", "2026-06-10T12:00:00", number=i, commits=n)
        for i, n in enumerate([1, 2, 100])
    ]
    stats = group_report(
        prs,
        **_window(people=["alice"], analysis={"commits": {"exclude_outliers": True}}),
    )["summary"]["current"]
    assert stats["analysis_samples"]["commits"]["excluded"] == 0
    assert stats["commits"] == round(103 / 3, 2)


def test_time_and_size_statistic_switches_and_total_lines_use_merge_window():
    prs = [
        merged_pr(
            "alice",
            "2026-06-10T12:00:00",
            number=i,
            hours_open=hours,
            additions=lines,
            deletions=2,
        )
        for i, (hours, lines) in enumerate([(1, 10), (2, 20), (9, 90)])
    ]
    stats = group_report(
        prs,
        **_window(
            people=["alice"],
            analysis={
                "lead_time": {"statistic": "mean"},
                "size": {"statistic": "mean"},
            },
        ),
    )["summary"]["current"]
    assert stats["lead_time"] == 4
    assert stats["size"] == 42
    assert stats["total_lines"] == 126
    assert stats["added_lines"] == 120 and stats["deleted_lines"] == 6


def test_person_changed_lines_compare_with_team_per_member_average():
    prs = [
        merged_pr("alice", "2026-06-10T12:00:00", number=1, additions=80, deletions=20),
        merged_pr("bob", "2026-06-10T12:00:00", number=2, additions=250, deletions=50),
    ]
    report = person_report(
        prs, **_window(login="alice", team=["alice", "bob", "carol", "dave"])
    )
    assert report["team_size"] == 4
    assert report["summary"]["current"]["total_lines"] == 100
    assert report["summary"]["compare"]["total_lines"] == 100
    assert report["summary"]["compare"]["added_lines"] == 82.5
    assert report["summary"]["compare"]["deleted_lines"] == 17.5
    assert report["periods"][-1]["compare"]["total_lines"] == 100
    group = group_report(prs, **_window(people=["alice", "bob", "carol", "dave"]))
    assert group["summary"]["current"]["total_lines"] == 400


def test_person_changed_lines_without_a_team_has_no_average_baseline():
    report = person_report(
        [merged_pr("alice", "2026-06-10T12:00:00")], **_window(login="alice", team=[])
    )
    assert report["team_size"] == 0
    assert report["summary"]["compare"]["total_lines"] is None
