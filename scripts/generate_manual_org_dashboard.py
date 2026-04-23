from __future__ import annotations

import argparse
import csv
import http.client
import json
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from render_manual_org_dashboard import (
    prepare_manual_dashboard_payload,
    render_manual_dashboard_html,
)

MAX_SEARCH_PAGE_SIZE = 100
MAX_WORKERS = 12


@dataclass(frozen=True)
class PullRequestSeed:
    repository_full_name: str
    number: int
    title: str
    author_login: str
    state: str
    created_at: datetime
    closed_at: datetime | None
    html_url: str


@dataclass(frozen=True)
class PullRequestReview:
    author_login: str
    state: str
    submitted_at: datetime


@dataclass(frozen=True)
class PullRequestTimelineEvent:
    event: str
    created_at: datetime | None
    requested_reviewer_login: str | None


@dataclass(frozen=True)
class PullRequestSnapshot:
    repository_full_name: str
    number: int
    title: str
    author_login: str
    state: str
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    merged_at: datetime | None
    html_url: str
    additions: int
    deletions: int
    changed_files: int
    changed_lines: int
    commits: int
    review_count: int
    approval_count: int
    changes_requested_count: int
    comment_review_count: int
    reviewer_count: int
    first_review_at: datetime | None
    first_review_hours: float | None
    merge_hours: float | None
    close_hours: float | None
    review_rounds: int
    review_requested_at: datetime | None
    review_ready_at: datetime
    size_bucket: str
    reviews: tuple[PullRequestReview, ...]


def main() -> None:
    args = _parse_args()
    token = _resolve_token()
    seeds = _collect_pull_request_seeds(
        token=token,
        org=args.org,
        since=date.fromisoformat(args.since),
        until=date.fromisoformat(args.until),
    )
    snapshots = _hydrate_pull_request_snapshots(token=token, seeds=seeds)
    payload = _build_dashboard_payload(
        org=args.org,
        since=date.fromisoformat(args.since),
        until=date.fromisoformat(args.until),
        snapshots=snapshots,
    )
    _write_outputs(
        output_dir=args.output_dir,
        base_name=args.base_name,
        payload=payload,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a dark-mode GitHub organization productivity dashboard.",
    )
    parser.add_argument("--org", required=True)
    parser.add_argument("--since", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--until", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--base-name", required=True)
    return parser.parse_args()


def _resolve_token() -> str:
    command = ["gh", "auth", "token"]
    token = __import__("subprocess").run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not token:
        raise RuntimeError("unable to resolve GitHub auth token from gh")
    return token


def _collect_pull_request_seeds(
    *,
    token: str,
    org: str,
    since: date,
    until: date,
) -> list[PullRequestSeed]:
    seeds: list[PullRequestSeed] = []
    for window_start, window_end in _month_windows(since, until):
        query = f"org:{org} is:pr created:{window_start.isoformat()}..{window_end.isoformat()}"
        page = 1
        while True:
            payload = _github_get_json(
                token=token,
                url="https://api.github.com/search/issues",
                query_params={
                    "q": query,
                    "sort": "created",
                    "order": "desc",
                    "per_page": str(MAX_SEARCH_PAGE_SIZE),
                    "page": str(page),
                },
            )
            items = payload["items"]
            if not items:
                break
            seeds.extend(_build_seed(item) for item in items)
            if len(items) < MAX_SEARCH_PAGE_SIZE:
                break
            page += 1
    unique: dict[tuple[str, int], PullRequestSeed] = {}
    for seed in seeds:
        unique[(seed.repository_full_name, seed.number)] = seed
    return sorted(
        unique.values(),
        key=lambda seed: (seed.created_at, seed.repository_full_name, seed.number),
    )


def _hydrate_pull_request_snapshots(
    *,
    token: str,
    seeds: list[PullRequestSeed],
) -> list[PullRequestSnapshot]:
    snapshots: list[PullRequestSnapshot] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_hydrate_pull_request_snapshot, token, seed): seed
            for seed in seeds
        }
        for future in as_completed(futures):
            snapshots.append(future.result())
    return sorted(
        snapshots,
        key=lambda snapshot: (
            snapshot.created_at,
            snapshot.repository_full_name,
            snapshot.number,
        ),
    )


def _hydrate_pull_request_snapshot(
    token: str,
    seed: PullRequestSeed,
) -> PullRequestSnapshot:
    detail = _github_get_json(
        token=token,
        url=f"https://api.github.com/repos/{seed.repository_full_name}/pulls/{seed.number}",
    )
    reviews = _load_reviews(
        token=token,
        repository_full_name=seed.repository_full_name,
        number=seed.number,
    )
    timeline_events = _load_timeline_events(
        token=token,
        repository_full_name=seed.repository_full_name,
        number=seed.number,
    )
    review_ready_at, review_requested_at, first_review_at = _review_cycle_markers(
        created_at=seed.created_at,
        timeline_events=timeline_events,
        reviews=reviews,
    )
    merged_at = _parse_datetime(detail.get("merged_at"))
    closed_at = _parse_datetime(detail.get("closed_at"))
    return PullRequestSnapshot(
        repository_full_name=seed.repository_full_name,
        number=seed.number,
        title=seed.title,
        author_login=seed.author_login,
        state=str(detail["state"]),
        created_at=_parse_datetime(detail["created_at"]),
        updated_at=_parse_datetime(detail["updated_at"]),
        closed_at=closed_at,
        merged_at=merged_at,
        html_url=str(detail["html_url"]),
        additions=int(detail["additions"]),
        deletions=int(detail["deletions"]),
        changed_files=int(detail["changed_files"]),
        changed_lines=int(detail["additions"]) + int(detail["deletions"]),
        commits=int(detail["commits"]),
        review_count=len(reviews),
        approval_count=sum(1 for review in reviews if review.state == "APPROVED"),
        changes_requested_count=sum(
            1 for review in reviews if review.state == "CHANGES_REQUESTED"
        ),
        comment_review_count=sum(1 for review in reviews if review.state == "COMMENTED"),
        reviewer_count=len({review.author_login for review in reviews}),
        first_review_at=first_review_at,
        first_review_hours=_hours_between(
            review_requested_at or review_ready_at,
            first_review_at,
        ),
        merge_hours=_hours_between(seed.created_at, merged_at),
        close_hours=_hours_between(seed.created_at, closed_at),
        review_rounds=max(
            1,
            len(
                {
                    review.author_login
                    for review in reviews
                    if review.state in {"APPROVED", "CHANGES_REQUESTED", "COMMENTED"}
                }
            ),
        )
        if reviews
        else 0,
        review_requested_at=review_requested_at,
        review_ready_at=review_ready_at,
        size_bucket=_size_bucket(int(detail["additions"]) + int(detail["deletions"])),
        reviews=tuple(reviews),
    )


def _build_dashboard_payload(
    *,
    org: str,
    since: date,
    until: date,
    snapshots: list[PullRequestSnapshot],
) -> dict[str, Any]:
    total_pull_requests = len(snapshots)
    created_series = _time_series(
        snapshots=snapshots,
        date_selector=lambda snapshot: snapshot.created_at.date(),
    )
    merged_series = _time_series(
        snapshots=[snapshot for snapshot in snapshots if snapshot.merged_at is not None],
        date_selector=lambda snapshot: snapshot.merged_at.date(),
    )
    review_series = _time_series(
        snapshots=[
            snapshot for snapshot in snapshots if snapshot.first_review_at is not None
        ],
        date_selector=lambda snapshot: snapshot.first_review_at.date(),
    )
    author_rows = _author_rows(
        snapshots,
        total_pull_requests=total_pull_requests,
    )
    reviewer_rows = _reviewer_rows(snapshots)
    repository_rows = _repository_rows(
        snapshots,
        total_pull_requests=total_pull_requests,
    )
    size_bucket_rows = _size_bucket_rows(snapshots)
    review_state_rows = _review_state_rows(snapshots)
    overview = _overview_summary(
        org=org,
        since=since,
        until=until,
        snapshots=snapshots,
        author_rows=author_rows,
        reviewer_rows=reviewer_rows,
        repository_rows=repository_rows,
    )
    return {
        "overview": overview,
        "insights": _insights(
            overview=overview,
            author_rows=author_rows,
            reviewer_rows=reviewer_rows,
            repository_rows=repository_rows,
            size_bucket_rows=size_bucket_rows,
        ),
        "charts": {
            "created_series": created_series,
            "merged_series": merged_series,
            "review_series": review_series,
            "author_throughput": [
                {
                    "label": row["author_login"],
                    "pull_requests": row["pull_requests"],
                    "merged_pull_requests": row["merged_pull_requests"],
                    "changed_lines": row["changed_lines"],
                }
                for row in author_rows[:8]
            ],
            "review_latency_by_author": [
                {
                    "label": row["author_login"],
                    "median_first_review_hours": row["median_first_review_hours"],
                }
                for row in author_rows
                if row["median_first_review_hours"] is not None
            ][:8],
            "repository_throughput": [
                {
                    "label": row["repository_full_name"],
                    "pull_requests": row["pull_requests"],
                    "merged_pull_requests": row["merged_pull_requests"],
                }
                for row in repository_rows[:10]
            ],
            "size_bucket_latency": size_bucket_rows,
        },
        "authors": author_rows,
        "reviewers": reviewer_rows,
        "repositories": repository_rows,
        "size_buckets": size_bucket_rows,
        "review_state_rows": review_state_rows,
        "pull_requests": [_snapshot_row(snapshot) for snapshot in snapshots],
    }


def _write_outputs(
    *,
    output_dir: Path,
    base_name: str,
    payload: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{base_name}.json"
    csv_path = output_dir / f"{base_name}-prs.csv"
    html_path = output_dir / f"{base_name}.html"
    json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        rows = payload["pull_requests"]
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    html_path.write_text(
        render_manual_dashboard_html(prepare_manual_dashboard_payload(payload)),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "json_path": str(json_path),
                "csv_path": str(csv_path),
                "html_path": str(html_path),
                "pull_requests": payload["overview"]["pull_requests"],
            },
            ensure_ascii=False,
        )
    )


def _build_seed(item: dict[str, Any]) -> PullRequestSeed:
    return PullRequestSeed(
        repository_full_name=_repository_full_name(item["repository_url"]),
        number=int(item["number"]),
        title=str(item["title"]),
        author_login=str(item.get("user", {}).get("login") or "ghost"),
        state=str(item["state"]),
        created_at=_parse_datetime(item["created_at"]),
        closed_at=_parse_datetime(item.get("closed_at")),
        html_url=str(item["html_url"]),
    )


def _month_windows(start_date: date, end_date: date) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    cursor = start_date.replace(day=1)
    while cursor <= end_date:
        next_month = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        windows.append((max(cursor, start_date), min(month_end, end_date)))
        cursor = next_month
    return windows


def _load_reviews(
    *,
    token: str,
    repository_full_name: str,
    number: int,
) -> list[PullRequestReview]:
    rows = _github_get_paginated_json(
        token=token,
        url=f"https://api.github.com/repos/{repository_full_name}/pulls/{number}/reviews",
    )
    reviews = [
        PullRequestReview(
            author_login=str(row.get("user", {}).get("login") or "ghost"),
            state=str(row["state"]),
            submitted_at=_parse_datetime(row["submitted_at"]),
        )
        for row in rows
        if row.get("submitted_at")
    ]
    return sorted(reviews, key=lambda review: review.submitted_at)


def _load_timeline_events(
    *,
    token: str,
    repository_full_name: str,
    number: int,
) -> list[PullRequestTimelineEvent]:
    rows = _github_get_paginated_json(
        token=token,
        url=f"https://api.github.com/repos/{repository_full_name}/issues/{number}/timeline",
        accept="application/vnd.github+json",
    )
    events = [
        PullRequestTimelineEvent(
            event=str(row["event"]),
            created_at=_parse_datetime(row.get("created_at")),
            requested_reviewer_login=_requested_reviewer_login(row),
        )
        for row in rows
    ]
    return sorted(
        events,
        key=lambda event: (
            event.created_at.isoformat() if event.created_at is not None else "",
            event.event,
        ),
    )


def _review_cycle_markers(
    *,
    created_at: datetime,
    timeline_events: list[PullRequestTimelineEvent],
    reviews: list[PullRequestReview],
) -> tuple[datetime, datetime | None, datetime | None]:
    review_ready_at = created_at
    review_requested_at: datetime | None = None
    first_review_at: datetime | None = None
    markers = [
        ("event", event.created_at, event)
        for event in timeline_events
        if event.created_at is not None
    ] + [
        ("review", review.submitted_at, review)
        for review in reviews
    ]
    markers.sort(
        key=lambda marker: (
            marker[1].isoformat(),
            0 if marker[0] == "event" else 1,
        )
    )
    for marker_type, marker_at, marker in markers:
        if marker_type == "event":
            event = marker
            if event.event in {"converted_to_draft", "ready_for_review"}:
                review_ready_at = marker_at
                review_requested_at = None
                first_review_at = None
                continue
            if (
                event.event == "review_requested"
                and marker_at >= review_ready_at
                and first_review_at is None
                and review_requested_at is None
            ):
                review_requested_at = marker_at
            continue
        if marker_at >= review_ready_at and first_review_at is None:
            first_review_at = marker_at
    return review_ready_at, review_requested_at, first_review_at


def _time_series(
    *,
    snapshots: list[PullRequestSnapshot],
    date_selector,
) -> list[dict[str, Any]]:
    counts = Counter(date_selector(snapshot).isoformat() for snapshot in snapshots)
    return [
        {"date": label, "count": counts[label]}
        for label in sorted(counts)
    ]


def _overview_summary(
    *,
    org: str,
    since: date,
    until: date,
    snapshots: list[PullRequestSnapshot],
    author_rows: list[dict[str, Any]],
    reviewer_rows: list[dict[str, Any]],
    repository_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours for snapshot in snapshots if snapshot.merge_hours is not None
    ]
    close_values = [
        snapshot.close_hours for snapshot in snapshots if snapshot.close_hours is not None
    ]
    return {
        "org": org,
        "time_anchor": "created_at",
        "since": since.isoformat(),
        "until": until.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "pull_requests": len(snapshots),
        "merged_pull_requests": sum(1 for snapshot in snapshots if snapshot.merged_at),
        "open_pull_requests": sum(1 for snapshot in snapshots if snapshot.state == "open"),
        "repositories": len(repository_rows),
        "authors": len(author_rows),
        "review_submissions": sum(snapshot.review_count for snapshot in snapshots),
        "unique_reviewers": len(reviewer_rows),
        "total_changed_lines": sum(snapshot.changed_lines for snapshot in snapshots),
        "total_commits": sum(snapshot.commits for snapshot in snapshots),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "median_close_hours": _round(_median_or_none(close_values)),
        "average_reviews_per_pr": _round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "average_changed_lines_per_pr": _round(
            sum(snapshot.changed_lines for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "review_coverage_pct": _round(
            (
                sum(1 for snapshot in snapshots if snapshot.review_count > 0)
                / len(snapshots)
                * 100
            )
            if snapshots
            else None
        ),
        "merge_rate_pct": _round(
            (
                sum(1 for snapshot in snapshots if snapshot.merged_at)
                / len(snapshots)
                * 100
            )
            if snapshots
            else None
        ),
        "top_repository": repository_rows[0]["repository_full_name"] if repository_rows else None,
        "top_author": author_rows[0]["author_login"] if author_rows else None,
    }


def _author_rows(
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.author_login].append(snapshot)
    rows = [
        _author_row(
            author_login,
            grouped_snapshots,
            total_pull_requests=total_pull_requests,
        )
        for author_login, grouped_snapshots in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (-row["pull_requests"], -row["changed_lines"], row["author_login"]),
    )


def _reviewer_rows(snapshots: list[PullRequestSnapshot]) -> list[dict[str, Any]]:
    review_records: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    review_counts: Counter[str] = Counter()
    approval_counts: Counter[str] = Counter()
    change_request_counts: Counter[str] = Counter()
    comment_counts: Counter[str] = Counter()
    prs_reviewed: dict[str, set[str]] = defaultdict(set)
    for snapshot in snapshots:
        for review in snapshot.reviews:
            review_records[review.author_login].append(snapshot)
            review_counts[review.author_login] += 1
            prs_reviewed[review.author_login].add(
                f"{snapshot.repository_full_name}#{snapshot.number}"
            )
            if review.state == "APPROVED":
                approval_counts[review.author_login] += 1
            if review.state == "CHANGES_REQUESTED":
                change_request_counts[review.author_login] += 1
            if review.state == "COMMENTED":
                comment_counts[review.author_login] += 1
    rows = []
    for reviewer_login, reviewer_snapshots in review_records.items():
        rows.append(
            {
                "reviewer_login": reviewer_login,
                "review_submissions": review_counts[reviewer_login],
                "pull_requests_reviewed": len(prs_reviewed[reviewer_login]),
                "approvals": approval_counts[reviewer_login],
                "changes_requested": change_request_counts[reviewer_login],
                "comments": comment_counts[reviewer_login],
                "authors_supported": len(
                    {snapshot.author_login for snapshot in reviewer_snapshots}
                ),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            -row["review_submissions"],
            -row["pull_requests_reviewed"],
            row["reviewer_login"],
        ),
    )


def _repository_rows(
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.repository_full_name].append(snapshot)
    rows = [
        _repository_row(
            repository_full_name,
            grouped_snapshots,
            total_pull_requests=total_pull_requests,
        )
        for repository_full_name, grouped_snapshots in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (
            -row["pull_requests"],
            -row["changed_lines"],
            row["repository_full_name"],
        ),
    )


def _size_bucket_rows(snapshots: list[PullRequestSnapshot]) -> list[dict[str, Any]]:
    grouped: dict[str, list[PullRequestSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.size_bucket].append(snapshot)
    ordered_buckets = ["XS", "S", "M", "L", "XL"]
    rows = []
    for bucket in ordered_buckets:
        bucket_snapshots = grouped.get(bucket, [])
        rows.append(
            {
                "bucket": bucket,
                "pull_requests": len(bucket_snapshots),
                "median_changed_lines": _round(
                    _median_or_none([snapshot.changed_lines for snapshot in bucket_snapshots])
                ),
                "median_first_review_hours": _round(
                    _median_or_none(
                        [
                            snapshot.first_review_hours
                            for snapshot in bucket_snapshots
                            if snapshot.first_review_hours is not None
                        ]
                    )
                ),
                "median_merge_hours": _round(
                    _median_or_none(
                        [
                            snapshot.merge_hours
                            for snapshot in bucket_snapshots
                            if snapshot.merge_hours is not None
                        ]
                    )
                ),
                "average_reviews_per_pr": _round(
                    (
                        sum(snapshot.review_count for snapshot in bucket_snapshots)
                        / len(bucket_snapshots)
                    )
                    if bucket_snapshots
                    else None
                ),
            }
        )
    return rows


def _review_state_rows(snapshots: list[PullRequestSnapshot]) -> list[dict[str, Any]]:
    states = Counter()
    for snapshot in snapshots:
        states["APPROVED"] += snapshot.approval_count
        states["CHANGES_REQUESTED"] += snapshot.changes_requested_count
        states["COMMENTED"] += snapshot.comment_review_count
    total = sum(states.values())
    return [
        {
            "state": state,
            "count": count,
            "share_pct": _round((count / total) * 100 if total else None),
        }
        for state, count in sorted(states.items(), key=lambda item: (-item[1], item[0]))
    ]


def _insights(
    *,
    overview: dict[str, Any],
    author_rows: list[dict[str, Any]],
    reviewer_rows: list[dict[str, Any]],
    repository_rows: list[dict[str, Any]],
    size_bucket_rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    insights: list[dict[str, str]] = []
    if repository_rows:
        top_repo = repository_rows[0]
        insights.append(
            {
                "title": "Throughput concentration",
                "body": (
                    f"{top_repo['repository_full_name']} accounted for "
                    f"{top_repo['pull_requests']} PRs and "
                    f"{top_repo['share_of_prs_pct']}% of total flow."
                ),
            }
        )
    fast_review_authors = [
        row
        for row in author_rows
        if row["pull_requests"] >= 20 and row["median_first_review_hours"] is not None
    ]
    if fast_review_authors:
        fastest_author = min(
            fast_review_authors,
            key=lambda row: row["median_first_review_hours"],
        )
        insights.append(
            {
                "title": "Fastest review entry",
                "body": (
                    f"{fastest_author['author_login']} had the fastest median first review "
                    f"among authors with 20+ PRs at "
                    f"{fastest_author['median_first_review_hours']} hours."
                ),
            }
        )
    if reviewer_rows:
        top_reviewer = reviewer_rows[0]
        insights.append(
            {
                "title": "Review load",
                "body": (
                    f"{top_reviewer['reviewer_login']} submitted "
                    f"{top_reviewer['review_submissions']} reviews across "
                    f"{top_reviewer['pull_requests_reviewed']} PRs."
                ),
            }
        )
    large_buckets = [row for row in size_bucket_rows if row["bucket"] in {"L", "XL"}]
    if large_buckets:
        slowest_bucket = max(
            large_buckets,
            key=lambda row: row["median_first_review_hours"] or -1,
        )
        if slowest_bucket["median_first_review_hours"] is not None:
            insights.append(
                {
                    "title": "Size penalty",
                    "body": (
                        f"{slowest_bucket['bucket']} PRs waited "
                        f"{slowest_bucket['median_first_review_hours']} hours median "
                        f"for first review."
                    ),
                }
            )
    insights.append(
        {
            "title": "Review coverage",
            "body": (
                f"{overview['review_coverage_pct']}% of PRs received at least one review "
                f"submission in the selected window."
            ),
        }
    )
    return insights


def _author_row(
    author_login: str,
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> dict[str, Any]:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours for snapshot in snapshots if snapshot.merge_hours is not None
    ]
    return {
        "author_login": author_login,
        "pull_requests": len(snapshots),
        "merged_pull_requests": sum(1 for snapshot in snapshots if snapshot.merged_at),
        "open_pull_requests": sum(1 for snapshot in snapshots if snapshot.state == "open"),
        "changed_lines": sum(snapshot.changed_lines for snapshot in snapshots),
        "commits": sum(snapshot.commits for snapshot in snapshots),
        "review_submissions_received": sum(snapshot.review_count for snapshot in snapshots),
        "average_reviews_per_pr": _round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "median_changed_lines": _round(
            _median_or_none([snapshot.changed_lines for snapshot in snapshots])
        ),
        "share_of_prs_pct": _round(
            (len(snapshots) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    }


def _repository_row(
    repository_full_name: str,
    snapshots: list[PullRequestSnapshot],
    *,
    total_pull_requests: int,
) -> dict[str, Any]:
    first_review_values = [
        snapshot.first_review_hours
        for snapshot in snapshots
        if snapshot.first_review_hours is not None
    ]
    merge_values = [
        snapshot.merge_hours for snapshot in snapshots if snapshot.merge_hours is not None
    ]
    return {
        "repository_full_name": repository_full_name,
        "pull_requests": len(snapshots),
        "merged_pull_requests": sum(1 for snapshot in snapshots if snapshot.merged_at),
        "open_pull_requests": sum(1 for snapshot in snapshots if snapshot.state == "open"),
        "authors": len({snapshot.author_login for snapshot in snapshots}),
        "changed_lines": sum(snapshot.changed_lines for snapshot in snapshots),
        "review_submissions": sum(snapshot.review_count for snapshot in snapshots),
        "average_reviews_per_pr": _round(
            sum(snapshot.review_count for snapshot in snapshots) / len(snapshots)
            if snapshots
            else None
        ),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "share_of_prs_pct": _round(
            (len(snapshots) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    }


def _snapshot_row(snapshot: PullRequestSnapshot) -> dict[str, Any]:
    return {
        "repository_full_name": snapshot.repository_full_name,
        "pull_request_number": snapshot.number,
        "title": snapshot.title,
        "author_login": snapshot.author_login,
        "state": snapshot.state,
        "created_at": snapshot.created_at.isoformat(),
        "updated_at": snapshot.updated_at.isoformat(),
        "closed_at": None if snapshot.closed_at is None else snapshot.closed_at.isoformat(),
        "merged_at": None if snapshot.merged_at is None else snapshot.merged_at.isoformat(),
        "html_url": snapshot.html_url,
        "additions": snapshot.additions,
        "deletions": snapshot.deletions,
        "changed_files": snapshot.changed_files,
        "changed_lines": snapshot.changed_lines,
        "commits": snapshot.commits,
        "review_count": snapshot.review_count,
        "approval_count": snapshot.approval_count,
        "changes_requested_count": snapshot.changes_requested_count,
        "comment_review_count": snapshot.comment_review_count,
        "reviewer_count": snapshot.reviewer_count,
        "first_review_hours": snapshot.first_review_hours,
        "merge_hours": snapshot.merge_hours,
        "close_hours": snapshot.close_hours,
        "review_rounds": snapshot.review_rounds,
        "review_ready_at": snapshot.review_ready_at.isoformat(),
        "review_requested_at": (
            None
            if snapshot.review_requested_at is None
            else snapshot.review_requested_at.isoformat()
        ),
        "size_bucket": snapshot.size_bucket,
    }


def _github_get_paginated_json(
    *,
    token: str,
    url: str,
    accept: str = "application/vnd.github+json",
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    next_url: str | None = f"{url}?per_page=100"
    while next_url is not None:
        payload, headers = _github_get_json_with_headers(
            token=token,
            url=next_url,
            accept=accept,
        )
        items.extend(payload)
        next_url = _next_link(headers.get("Link"))
    return items


def _github_get_json(
    *,
    token: str,
    url: str,
    query_params: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload, _headers = _github_get_json_with_headers(
        token=token,
        url=url,
        query_params=query_params,
    )
    return payload


def _github_get_json_with_headers(
    *,
    token: str,
    url: str,
    query_params: dict[str, str] | None = None,
    accept: str = "application/vnd.github+json",
) -> tuple[Any, dict[str, str]]:
    final_url = url
    if query_params:
        final_url = f"{url}?{urlencode(query_params)}"
    for attempt in range(5):
        request = Request(
            final_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": accept,
                "User-Agent": "orgpulse-manual-dashboard",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urlopen(request) as response:  # noqa: S310
                return (
                    json.loads(response.read().decode("utf-8")),
                    dict(response.headers.items()),
                )
        except (http.client.RemoteDisconnected, URLError):
            if attempt == 4:
                raise
            time.sleep(1.5 * (attempt + 1))
        except HTTPError:
            raise
    raise RuntimeError(f"unreachable retry loop for {final_url}")


def _next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        section = part.strip().split(";")
        if len(section) < 2:
            continue
        if section[1].strip() == 'rel="next"':
            return section[0].strip()[1:-1]
    return None


def _repository_full_name(repository_url: str) -> str:
    path = urlparse(repository_url).path.strip("/")
    owner, repo = path.split("/")[-2:]
    return f"{owner}/{repo}"


def _requested_reviewer_login(row: dict[str, Any]) -> str | None:
    reviewer = row.get("requested_reviewer")
    if not isinstance(reviewer, dict):
        return None
    login = reviewer.get("login")
    if isinstance(login, str):
        return login
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if value in {None, "", "0001-01-01T00:00:00Z", "0001-01-01T00:00:00+00:00"}:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _hours_between(start_at: datetime | None, end_at: datetime | None) -> float | None:
    if start_at is None or end_at is None:
        return None
    return round((end_at - start_at).total_seconds() / 3600, 2)


def _size_bucket(changed_lines: int) -> str:
    if changed_lines <= 100:
        return "XS"
    if changed_lines <= 400:
        return "S"
    if changed_lines <= 1000:
        return "M"
    if changed_lines <= 3000:
        return "L"
    return "XL"


def _median_or_none(values: list[float | int]) -> float | None:
    normalized = [float(value) for value in values]
    if not normalized:
        return None
    return float(median(normalized))


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)

if __name__ == "__main__":
    main()
