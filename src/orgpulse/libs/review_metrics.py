from __future__ import annotations

from datetime import date, tzinfo
from statistics import mean, median, quantiles
from typing import Any

from orgpulse.common.models import Review


def add_review_metrics(
    report: dict[str, Any],
    reviews: list[Review],
    *,
    people: tuple[str, ...],
    timezone: tzinfo,
    since: date,
    available: bool,
    options: dict[str, Any],
    team_statistic: str,
) -> None:
    """Count distinct reviewer/PR pairs by review date and describe requested response waits."""
    person = report["scope"]["kind"] == "person"
    login = report["scope"]["name"].lower() if person else None
    roster = {name.lower() for name in people}
    scoped = [review for review in reviews if review.reviewer.lower() in roster]
    own = (
        [review for review in reviews if review.reviewer.lower() == login]
        if person
        else scoped
    )
    dates = report["periods"]
    start, end = (
        date.fromisoformat(dates[0]["start"]),
        date.fromisoformat(dates[-1]["end"]),
    )

    def summary(
        records: list[Review], first: date, last: date, *, team: bool = False
    ) -> dict[str, Any]:
        ready = available and last >= since
        window = [
            review
            for review in records
            if max(first, since)
            <= review.submitted_at.astimezone(timezone).date()
            <= last
        ]
        keys = {
            (review.reviewer.lower(), review.repository, review.number)
            for review in window
        }
        # One requested response per reviewer/PR in the selected range.
        answers: dict[tuple[str, str, int], Review] = {}
        for review in sorted(window, key=lambda review: review.submitted_at):
            if review.requested_at is not None:
                answers.setdefault(
                    (review.reviewer.lower(), review.repository, review.number), review
                )
        waits = [
            (review.submitted_at - review.requested_at).total_seconds() / 3600
            for review in answers.values()
            if review.requested_at is not None
        ]
        known = len(waits)
        if options.get("exclude_outliers") and known >= 4:
            q1, _, q3 = quantiles(waits, n=4, method="inclusive")
            waits = [
                value
                for value in waits
                if q1 - 1.5 * (q3 - q1) <= value <= q3 + 1.5 * (q3 - q1)
            ]
        aggregate = mean if options.get("statistic") == "mean" else median
        benchmarks = {}
        count = len(keys)
        if team:
            counts = [sum(key[0] == name for key in keys) for name in roster]
            benchmarks = {
                name: round(aggregate(counts), 2) if counts and ready else None
                for name, aggregate in (("mean", mean), ("median", median))
            }
            count = benchmarks[team_statistic]
        return {
            "reviewed_prs": count if ready else None,
            "benchmarks": benchmarks,
            "review_response": round(aggregate(waits), 2) if waits and ready else None,
            "sample": {
                "total": known,
                "analyzed": len(waits),
                "excluded": known - len(waits),
                "missing": len(keys) - known,
            },
        }

    def apply(
        target: dict[str, Any] | None,
        records: list[Review],
        first: date,
        last: date,
        *,
        team: bool = False,
    ) -> None:
        if target is None:
            return
        values = summary(records, first, last, team=team)
        target.update({key: values[key] for key in ("reviewed_prs", "review_response")})
        target["analysis_samples"]["review_response"] = values["sample"]
        if team and "team_benchmarks" in target:
            for name, count in values["benchmarks"].items():
                target["team_benchmarks"][name]["reviewed_prs"] = count

    def recent(records: list[Review]) -> list[dict[str, Any]]:
        rows = {}
        for review in sorted(
            records, key=lambda review: review.submitted_at, reverse=True
        ):
            if (
                max(start, since)
                <= review.submitted_at.astimezone(timezone).date()
                <= end
            ):
                rows.setdefault(
                    (review.repository, review.number),
                    {
                        "repository": review.repository,
                        "number": review.number,
                        "title": review.title,
                        "url": review.url,
                        "author": review.author,
                        "reviewed_at": review.submitted_at.isoformat(),
                    },
                )
        return list(rows.values())[:10]

    for period in dates:
        first, last = (
            date.fromisoformat(period["start"]),
            date.fromisoformat(period["end"]),
        )
        apply(period["current"], own, first, last)
        if person:
            apply(period["compare"], scoped, first, last, team=True)
        elif period["compare"] is not None:
            prior_first = date.fromisoformat(period["compare_start"])
            prior_last = date.fromisoformat(period["compare_end"])
            apply(period["compare"], scoped, prior_first, prior_last)
    apply(report["summary"]["current"], own, start, end)
    if person:
        apply(report["summary"]["compare"], scoped, start, end, team=True)
    elif report["summary"]["compare"] is not None:
        first = date.fromisoformat(dates[0]["compare_start"])
        last = date.fromisoformat(dates[-1]["compare_end"])
        apply(report["summary"]["compare"], scoped, first, last)
    if not person:
        apply(report["team_summary"], scoped, start, end, team=True)
        for row in report["people"]:
            records = [
                review
                for review in scoped
                if review.reviewer.lower() == row["login"].lower()
            ]
            apply(row, records, start, end)
            row["review_recent"] = recent(records)
    report["review_recent"] = recent(own) if person else []
    report["review_data_available"] = available
