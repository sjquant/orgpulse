from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup, escape

from orgpulse.distribution import (
    trim_upper_tail,
    upper_percentile_threshold,
    validate_distribution_percentile,
)
from orgpulse.models import (
    DashboardPreparedPayload,
    DashboardSourcePayload,
)

AUTHOR_ROSTER_LIMIT = 12
LEADERBOARD_LIMIT = 10
WEEKLY_RECENT_TREND_COUNT = 12
MONTHLY_RECENT_TREND_COUNT = 6


def render_dashboard_artifact(
    *,
    input_json: Path,
    output_html: Path,
    distribution_percentile: int,
) -> dict[str, str | int]:
    payload = _load_payload(
        input_json,
        distribution_percentile=distribution_percentile,
    )
    html = _render_html(payload)
    output_html.write_text(html, encoding="utf-8")
    return {
        "input_json": str(input_json),
        "output_html": str(output_html),
        "distribution_percentile": distribution_percentile,
    }


def _load_payload(
    path: Path,
    *,
    distribution_percentile: int,
) -> DashboardPreparedPayload:
    return prepare_dashboard_payload(
        DashboardSourcePayload.model_validate(
            json.loads(path.read_text(encoding="utf-8"))
        ),
        distribution_percentile=distribution_percentile,
    )


def render_dashboard_html(
    payload: DashboardPreparedPayload | dict[str, Any],
) -> str:
    prepared_payload = _validate_prepared_payload(payload)
    template = _template_environment().get_template("org_dashboard.html.j2")
    return template.render(
        overview=prepared_payload.overview,
        authors=prepared_payload.authors,
        authors_roster_top=prepared_payload.authors_roster_top,
        authors_roster_rest=prepared_payload.authors_roster_rest,
        reviewers=prepared_payload.reviewers,
        reviewers_top=prepared_payload.reviewers_top,
        reviewers_rest=prepared_payload.reviewers_rest,
        repositories_top=prepared_payload.repositories_top,
        repositories_rest=prepared_payload.repositories_rest,
        size_buckets=prepared_payload.size_buckets,
        weekly_trends=prepared_payload.weekly_trends,
        monthly_trends=prepared_payload.monthly_trends,
        weekly_trends_recent=prepared_payload.weekly_trends_recent,
        weekly_trends_older=prepared_payload.weekly_trends_older,
        monthly_trends_recent=prepared_payload.monthly_trends_recent,
        monthly_trends_older=prepared_payload.monthly_trends_older,
        methodology=prepared_payload.methodology,
        reference_summary=prepared_payload.reference_summary,
        size_diagnostic=prepared_payload.size_diagnostic,
        default_author=prepared_payload.default_author,
        author_details_json=Markup(prepared_payload.author_details_json),
    )


def _render_html(payload: DashboardPreparedPayload) -> str:
    return render_dashboard_html(payload)


def prepare_dashboard_payload(
    payload: DashboardSourcePayload | dict[str, Any],
    *,
    distribution_percentile: int = 100,
) -> DashboardPreparedPayload:
    validate_distribution_percentile(distribution_percentile)
    normalized_payload = _validate_source_payload(payload).model_dump(mode="json")
    pull_requests = normalized_payload["pull_requests"]
    distribution_thresholds = _build_distribution_thresholds(
        pull_requests,
        distribution_percentile=distribution_percentile,
    )
    normalized_payload.update(
        _build_dashboard_sections(
            normalized_payload,
            pull_requests=pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
    )
    normalized_payload["distribution_percentile"] = distribution_percentile
    normalized_payload["overview"] = _build_team_normalized_overview(
        normalized_payload["overview"],
        monthly_trends=normalized_payload["monthly_trends"],
    )
    _attach_dashboard_slices(normalized_payload)
    normalized_payload["methodology"] = _build_methodology(normalized_payload)
    normalized_payload["reference_summary"] = _build_reference_summary(normalized_payload)
    normalized_payload["size_diagnostic"] = _build_size_diagnostic(normalized_payload["size_buckets"])
    normalized_payload["default_author"] = (
        normalized_payload["authors"][0]["author_login"] if normalized_payload["authors"] else None
    )
    normalized_payload["author_details_json"] = _build_author_details_json(
        authors=normalized_payload["authors"],
        reviewers=normalized_payload["reviewers"],
        pull_requests=pull_requests,
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    return DashboardPreparedPayload.model_validate(normalized_payload)


def _validate_source_payload(
    payload: DashboardSourcePayload | dict[str, Any],
) -> DashboardSourcePayload:
    if isinstance(payload, DashboardSourcePayload):
        return payload
    return DashboardSourcePayload.model_validate(payload)


def _validate_prepared_payload(
    payload: DashboardPreparedPayload | dict[str, Any],
) -> DashboardPreparedPayload:
    if isinstance(payload, DashboardPreparedPayload):
        return payload
    return DashboardPreparedPayload.model_validate(payload)


def _build_dashboard_sections(
    payload: dict[str, Any],
    *,
    pull_requests: list[dict[str, Any]],
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> dict[str, Any]:
    return {
        "overview": _build_overview(
            payload,
            pull_requests=pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        ),
        "authors": _build_author_rows(
            pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        ),
        "reviewers": _sort_reviewers(payload["reviewers"]),
        "repositories": _build_repository_rows(
            pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        ),
        "size_buckets": _build_size_bucket_rows(
            pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        ),
        "weekly_trends": _build_trend_rows(
            pull_requests,
            grain="week",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        ),
        "monthly_trends": _build_trend_rows(
            pull_requests,
            grain="month",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        ),
    }


def _attach_dashboard_slices(payload: dict[str, Any]) -> None:
    payload["authors_roster_top"] = payload["authors"][:AUTHOR_ROSTER_LIMIT]
    payload["authors_roster_rest"] = payload["authors"][AUTHOR_ROSTER_LIMIT:]
    payload["reviewers_top"] = payload["reviewers"][:LEADERBOARD_LIMIT]
    payload["reviewers_rest"] = payload["reviewers"][LEADERBOARD_LIMIT:]
    payload["repositories_top"] = payload["repositories"][:LEADERBOARD_LIMIT]
    payload["repositories_rest"] = payload["repositories"][LEADERBOARD_LIMIT:]
    payload["weekly_trends_recent"], payload["weekly_trends_older"] = _split_recent_rows(
        payload["weekly_trends"],
        recent_count=WEEKLY_RECENT_TREND_COUNT,
    )
    payload["monthly_trends_recent"], payload["monthly_trends_older"] = _split_recent_rows(
        payload["monthly_trends"],
        recent_count=MONTHLY_RECENT_TREND_COUNT,
    )


def _build_author_details_json(
    *,
    authors: list[dict[str, Any]],
    reviewers: list[dict[str, Any]],
    pull_requests: list[dict[str, Any]],
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> str:
    author_details = _build_author_details(
        authors=authors,
        reviewers=reviewers,
        pull_requests=pull_requests,
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    return json.dumps(
        author_details,
        ensure_ascii=False,
    ).replace("</script>", "<\\/script>")


def _sort_reviewers(reviewers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        reviewers,
        key=lambda reviewer: (
            -int(reviewer.get("pull_requests_reviewed", 0)),
            -int(reviewer.get("review_submissions", 0)),
            str(reviewer.get("reviewer_login", "")),
        ),
    )


def _build_overview(
    payload: dict[str, Any],
    *,
    pull_requests: list[dict[str, Any]],
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> dict[str, Any]:
    source_overview = payload["overview"]
    changed_lines = _summary(
        _trimmed_values(
            pull_requests,
            "changed_lines",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
    )
    commits = _summary(
        _trimmed_values(
            pull_requests,
            "commits",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
    )
    first_review_values = _trimmed_values(
        pull_requests,
        "first_review_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    merge_values = _trimmed_values(
        pull_requests,
        "merge_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    close_values = _trimmed_values(
        pull_requests,
        "close_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    return {
        **source_overview,
        "generated_at": source_overview["generated_at"],
        "pull_requests": len(pull_requests),
        "merged_pull_requests": sum(
            1 for pull_request in pull_requests if pull_request["merged_at"] is not None
        ),
        "open_pull_requests": sum(
            1 for pull_request in pull_requests if pull_request["state"] == "open"
        ),
        "repositories": len(
            {pull_request["repository_full_name"] for pull_request in pull_requests}
        ),
        "authors": len({pull_request["author_login"] for pull_request in pull_requests}),
        "review_submissions": sum(
            int(pull_request["review_count"]) for pull_request in pull_requests
        ),
        "total_changed_lines": _as_int(changed_lines["total"]),
        "total_commits": _as_int(commits["total"]),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "median_close_hours": _round(_median_or_none(close_values)),
        "average_reviews_per_pr": _round(
            (
                sum(int(pull_request["review_count"]) for pull_request in pull_requests)
                / len(pull_requests)
            )
            if pull_requests
            else None
        ),
        "average_changed_lines_per_pr": _round(changed_lines["average"]),
        "review_coverage_pct": _round(
            (
                sum(1 for pull_request in pull_requests if pull_request["review_count"] > 0)
                / len(pull_requests)
                * 100
            )
            if pull_requests
            else None
        ),
        "review_sla_24h_pct": _round(
            (
                sum(
                    1
                    for pull_request in pull_requests
                    if pull_request["first_review_hours"] is not None
                    and float(pull_request["first_review_hours"]) <= 24
                )
                / len(pull_requests)
                * 100
            )
            if pull_requests
            else None
        ),
        "stale_open_pull_requests": _stale_open_pull_requests(
            pull_requests,
            as_of=source_overview["until"],
            threshold_hours=72,
        ),
        "merge_rate_pct": _round(
            (
                sum(1 for pull_request in pull_requests if pull_request["merged_at"])
                / len(pull_requests)
                * 100
            )
            if pull_requests
            else None
        ),
        "distribution_percentile": distribution_percentile,
    }


def _build_author_rows(
    pull_requests: list[dict[str, Any]],
    *,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pull_request in pull_requests:
        grouped[str(pull_request["author_login"])].append(pull_request)
    total_pull_requests = len(pull_requests)
    rows = [
        _author_row(
            author_login,
            author_pull_requests,
            total_pull_requests=total_pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        for author_login, author_pull_requests in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (-row["pull_requests"], -row["changed_lines"], row["author_login"]),
    )


def _author_row(
    author_login: str,
    pull_requests: list[dict[str, Any]],
    *,
    total_pull_requests: int,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> dict[str, Any]:
    changed_lines = _summary(
        _trimmed_values(
            pull_requests,
            "changed_lines",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
    )
    commits = _summary(
        _trimmed_values(
            pull_requests,
            "commits",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
    )
    first_review_values = _trimmed_values(
        pull_requests,
        "first_review_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    merge_values = _trimmed_values(
        pull_requests,
        "merge_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    return {
        "author_login": author_login,
        "pull_requests": len(pull_requests),
        "merged_pull_requests": sum(
            1 for pull_request in pull_requests if pull_request["merged_at"]
        ),
        "open_pull_requests": sum(
            1 for pull_request in pull_requests if pull_request["state"] == "open"
        ),
        "changed_lines": _as_int(changed_lines["total"]),
        "commits": _as_int(commits["total"]),
        "review_submissions_received": sum(
            int(pull_request["review_count"]) for pull_request in pull_requests
        ),
        "average_reviews_per_pr": _round(
            (
                sum(int(pull_request["review_count"]) for pull_request in pull_requests)
                / len(pull_requests)
            )
            if pull_requests
            else None
        ),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "median_changed_lines": _round(changed_lines["median"]),
        "share_of_prs_pct": _round(
            (len(pull_requests) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    }


def _build_repository_rows(
    pull_requests: list[dict[str, Any]],
    *,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pull_request in pull_requests:
        grouped[str(pull_request["repository_full_name"])].append(pull_request)
    total_pull_requests = len(pull_requests)
    rows = [
        _repository_row(
            repository_full_name,
            repository_pull_requests,
            total_pull_requests=total_pull_requests,
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        for repository_full_name, repository_pull_requests in grouped.items()
    ]
    return sorted(
        rows,
        key=lambda row: (
            -row["pull_requests"],
            -row["changed_lines"],
            row["repository_full_name"],
        ),
    )


def _repository_row(
    repository_full_name: str,
    pull_requests: list[dict[str, Any]],
    *,
    total_pull_requests: int,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> dict[str, Any]:
    changed_lines = _summary(
        _trimmed_values(
            pull_requests,
            "changed_lines",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
    )
    first_review_values = _trimmed_values(
        pull_requests,
        "first_review_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    merge_values = _trimmed_values(
        pull_requests,
        "merge_hours",
        distribution_percentile=distribution_percentile,
        distribution_thresholds=distribution_thresholds,
    )
    return {
        "repository_full_name": repository_full_name,
        "pull_requests": len(pull_requests),
        "merged_pull_requests": sum(
            1 for pull_request in pull_requests if pull_request["merged_at"]
        ),
        "open_pull_requests": sum(
            1 for pull_request in pull_requests if pull_request["state"] == "open"
        ),
        "authors": len({pull_request["author_login"] for pull_request in pull_requests}),
        "changed_lines": _as_int(changed_lines["total"]),
        "review_submissions": sum(
            int(pull_request["review_count"]) for pull_request in pull_requests
        ),
        "average_reviews_per_pr": _round(
            (
                sum(int(pull_request["review_count"]) for pull_request in pull_requests)
                / len(pull_requests)
            )
            if pull_requests
            else None
        ),
        "median_first_review_hours": _round(_median_or_none(first_review_values)),
        "median_merge_hours": _round(_median_or_none(merge_values)),
        "share_of_prs_pct": _round(
            (len(pull_requests) / total_pull_requests * 100)
            if total_pull_requests
            else None
        ),
    }


def _build_size_bucket_rows(
    pull_requests: list[dict[str, Any]],
    *,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pull_request in pull_requests:
        grouped[str(pull_request["size_bucket"])].append(pull_request)
    rows: list[dict[str, Any]] = []
    for bucket in ("XS", "S", "M", "L", "XL"):
        bucket_pull_requests = grouped.get(bucket, [])
        changed_lines = _trimmed_values(
            bucket_pull_requests,
            "changed_lines",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        first_review_values = _trimmed_values(
            bucket_pull_requests,
            "first_review_hours",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        merge_values = _trimmed_values(
            bucket_pull_requests,
            "merge_hours",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        rows.append(
            {
                "bucket": bucket,
                "pull_requests": len(bucket_pull_requests),
                "median_changed_lines": _round(_median_or_none(changed_lines)),
                "median_first_review_hours": _round(
                    _median_or_none(first_review_values)
                ),
                "median_merge_hours": _round(_median_or_none(merge_values)),
                "average_reviews_per_pr": _round(
                    (
                        sum(
                            int(pull_request["review_count"])
                            for pull_request in bucket_pull_requests
                        )
                        / len(bucket_pull_requests)
                    )
                    if bucket_pull_requests
                    else None
                ),
            }
        )
    return rows


def _build_trend_rows(
    pull_requests: list[dict[str, Any]],
    *,
    grain: str,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pull_request in pull_requests:
        created_at = _parse_datetime(pull_request["created_at"])
        period_key = _period_key(created_at, grain=grain)
        grouped[period_key].append(pull_request)
    rows: list[dict[str, Any]] = []
    previous_pull_requests: int | None = None
    previous_changed_lines: int | None = None
    for period_key in sorted(grouped):
        period_rows = grouped[period_key]
        pull_request_count = len(period_rows)
        changed_lines = _summary(
            _trimmed_values(
                period_rows,
                "changed_lines",
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            )
        )
        review_submissions = sum(
            int(pull_request["review_count"]) for pull_request in period_rows
        )
        active_authors = len(
            {
                str(pull_request["author_login"])
                for pull_request in period_rows
                if pull_request["author_login"]
            }
        )
        merged_count = sum(
            1 for pull_request in period_rows if pull_request["merged_at"] is not None
        )
        open_count = sum(
            1 for pull_request in period_rows if pull_request["state"] == "open"
        )
        first_review_values = _trimmed_values(
            period_rows,
            "first_review_hours",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        merge_values = _trimmed_values(
            period_rows,
            "merge_hours",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        row = {
            "period_key": period_key,
            "pull_requests": pull_request_count,
            "merged_pull_requests": merged_count,
            "open_pull_requests": open_count,
            "active_authors": active_authors,
            "changed_lines": _as_int(changed_lines["total"]),
            "review_submissions": review_submissions,
            "pull_requests_per_active_author": _round(
                pull_request_count / active_authors if active_authors else None
            ),
            "changed_lines_per_active_author": _round(
                _as_int(changed_lines["total"]) / active_authors
                if active_authors
                else None
            ),
            "average_reviews_per_pr": _round(
                review_submissions / pull_request_count if pull_request_count else None
            ),
            "median_first_review_hours": _round(
                float(median(first_review_values)) if first_review_values else None
            ),
            "median_merge_hours": _round(
                float(median(merge_values)) if merge_values else None
            ),
            "pull_request_delta": (
                pull_request_count - previous_pull_requests
                if previous_pull_requests is not None
                else None
            ),
            "changed_lines_delta": (
                _as_int(changed_lines["total"]) - previous_changed_lines
                if previous_changed_lines is not None
                else None
            ),
        }
        rows.append(row)
        previous_pull_requests = pull_request_count
        previous_changed_lines = _as_int(changed_lines["total"])
    return rows


def _build_team_normalized_overview(
    overview: dict[str, Any],
    *,
    monthly_trends: list[dict[str, Any]],
) -> dict[str, Any]:
    active_author_values = [
        int(row["active_authors"])
        for row in monthly_trends
        if row.get("active_authors")
    ]
    average_active_authors_per_month = _round(
        sum(active_author_values) / len(active_author_values)
        if active_author_values
        else None
    )
    return {
        **overview,
        "average_active_authors_per_month": average_active_authors_per_month,
        "latest_active_authors": (
            int(monthly_trends[-1]["active_authors"])
            if monthly_trends and monthly_trends[-1].get("active_authors") is not None
            else None
        ),
        "pull_requests_per_active_author": _round(
            float(overview["pull_requests"]) / average_active_authors_per_month
            if average_active_authors_per_month
            else None
        ),
        "changed_lines_per_active_author": _round(
            float(overview["total_changed_lines"]) / average_active_authors_per_month
            if average_active_authors_per_month
            else None
        ),
        "review_submissions_per_reviewer": _round(
            float(overview["review_submissions"]) / float(overview["unique_reviewers"])
            if overview.get("unique_reviewers")
            else None
        ),
    }


def _build_author_details(
    *,
    authors: list[dict[str, Any]],
    reviewers: list[dict[str, Any]],
    pull_requests: list[dict[str, Any]],
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pull_request in pull_requests:
        grouped[str(pull_request["author_login"])].append(pull_request)
    reviewer_by_login = {
        str(reviewer["reviewer_login"]): reviewer for reviewer in reviewers
    }
    details: dict[str, Any] = {}
    for author in authors:
        author_login = author["author_login"]
        author_pull_requests = grouped.get(author_login, [])
        reviewer = reviewer_by_login.get(author_login, {})
        repository_counter = Counter(
            str(pull_request["repository_full_name"]) for pull_request in author_pull_requests
        )
        size_counter = Counter(
            str(pull_request["size_bucket"]) for pull_request in author_pull_requests
        )
        changed_lines = _summary(
            _trimmed_values(
                author_pull_requests,
                "changed_lines",
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            )
        )
        commits = _summary(
            _trimmed_values(
                author_pull_requests,
                "commits",
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            )
        )
        details[author_login] = {
            "summary": {
                **author,
                "average_changed_lines_per_pr": _round(changed_lines["average"]),
                "average_commits_per_pr": _round(commits["average"]),
                "merge_rate_pct": _round(
                    (
                        sum(
                            1
                            for pull_request in author_pull_requests
                            if pull_request["merged_at"] is not None
                        )
                        / len(author_pull_requests)
                        * 100
                    )
                    if author_pull_requests
                    else None
                ),
                "review_submissions_given": int(reviewer.get("review_submissions", 0)),
                "pull_requests_reviewed": int(reviewer.get("pull_requests_reviewed", 0)),
                "approvals_given": int(reviewer.get("approvals", 0)),
                "changes_requested_given": int(reviewer.get("changes_requested", 0)),
                "review_comments_given": int(reviewer.get("comments", 0)),
                "authors_supported": int(reviewer.get("authors_supported", 0)),
            },
            "top_repositories": [
                {
                    "repository_full_name": repository_full_name,
                    "pull_requests": count,
                    "changed_lines": _as_int(
                        _summary(
                            _trimmed_values(
                                [
                                    pull_request
                                    for pull_request in author_pull_requests
                                    if pull_request["repository_full_name"]
                                    == repository_full_name
                                ],
                                "changed_lines",
                                distribution_percentile=distribution_percentile,
                                distribution_thresholds=distribution_thresholds,
                            )
                        )["total"]
                    ),
                }
                for repository_full_name, count in sorted(
                    repository_counter.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:5]
            ],
            "size_mix": _build_author_size_mix_rows(
                author_pull_requests,
                size_counter=size_counter,
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            ),
            "weekly_trends": _build_trend_rows(
                author_pull_requests,
                grain="week",
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            ),
            "monthly_trends": _build_trend_rows(
                author_pull_requests,
                grain="month",
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            ),
        }
    return details


def _build_author_size_mix_rows(
    pull_requests: list[dict[str, Any]],
    *,
    size_counter: Counter[str],
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in ("XS", "S", "M", "L", "XL"):
        bucket_pull_requests = [
            pull_request
            for pull_request in pull_requests
            if pull_request["size_bucket"] == bucket
        ]
        changed_lines = _summary(
            _trimmed_values(
                bucket_pull_requests,
                "changed_lines",
                distribution_percentile=distribution_percentile,
                distribution_thresholds=distribution_thresholds,
            )
        )
        first_review_values = _trimmed_values(
            bucket_pull_requests,
            "first_review_hours",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        merge_values = _trimmed_values(
            bucket_pull_requests,
            "merge_hours",
            distribution_percentile=distribution_percentile,
            distribution_thresholds=distribution_thresholds,
        )
        review_submissions = sum(
            int(pull_request["review_count"]) for pull_request in bucket_pull_requests
        )
        pull_request_count = size_counter.get(bucket, 0)
        rows.append(
            {
                "bucket": bucket,
                "pull_requests": pull_request_count,
                "changed_lines": _as_int(changed_lines["total"]),
                "median_first_review_hours": _round(_median_or_none(first_review_values)),
                "median_merge_hours": _round(_median_or_none(merge_values)),
                "average_reviews_per_pr": _round(
                    review_submissions / pull_request_count
                    if pull_request_count
                    else None
                ),
            }
        )
    return rows


def _build_insights(payload: dict[str, Any]) -> list[dict[str, str]]:
    overview = payload["overview"]
    authors = payload["authors"]
    reviewers = payload["reviewers"]
    repositories = payload["repositories"]
    size_buckets = payload["size_buckets"]
    insights: list[dict[str, str]] = [
        {
            "title": "Distribution cutoff",
            "body": (
                "Distribution-based metrics keep values at or below the "
                f"{overview['distribution_percentile']}th percentile while "
                "throughput counts stay unchanged."
            ),
        }
    ]
    if repositories:
        top_repository = repositories[0]
        insights.append(
            {
                "title": "Throughput concentration",
                "body": (
                    f"{top_repository['repository_full_name']} accounted for "
                    f"{_format_integer(top_repository['pull_requests'])} PRs and "
                    f"{top_repository['share_of_prs_pct']}% of total flow."
                ),
            }
        )
    fast_review_authors = [
        row
        for row in authors
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
                    f"{fastest_author['author_login']} had the fastest median first "
                    "review among authors with 20+ PRs at "
                    f"{_format_duration(fastest_author['median_first_review_hours'])}."
                ),
            }
        )
    if reviewers:
        top_reviewer = reviewers[0]
        insights.append(
            {
                "title": "Review load",
                "body": (
                    f"{top_reviewer['reviewer_login']} submitted "
                    f"{_format_integer(top_reviewer['review_submissions'])} reviews "
                    f"across {_format_integer(top_reviewer['pull_requests_reviewed'])} PRs."
                ),
            }
        )
    large_buckets = [
        row
        for row in size_buckets
        if row["bucket"] in {"L", "XL"} and row["median_first_review_hours"] is not None
    ]
    if large_buckets:
        slowest_bucket = max(
            large_buckets,
            key=lambda row: row["median_first_review_hours"],
        )
        insights.append(
            {
                "title": "Size penalty",
                "body": (
                    f"{slowest_bucket['bucket']} PRs waited "
                    f"{_format_duration(slowest_bucket['median_first_review_hours'])} median "
                    "for first review."
                ),
            }
        )
    insights.append(
        {
            "title": "Review coverage",
            "body": (
                f"{overview['review_coverage_pct']}% of PRs received at least one "
                "review submission in the selected window."
            ),
        }
    )
    return insights[:5]


def _build_methodology(payload: dict[str, Any]) -> dict[str, Any]:
    overview = payload["overview"]
    return {
        "window": f"{overview['since']} to {overview['until']}",
        "anchor": overview["time_anchor"],
        "distribution_percentile": overview["distribution_percentile"],
        "generated_at": overview["generated_at"],
    }


def _build_reference_summary(payload: dict[str, Any]) -> dict[str, Any]:
    overview = payload["overview"]
    authors = payload["authors"]
    reviewers = payload["reviewers"]
    repositories = payload["repositories"]
    return {
        "author_roster_coverage_pct": _coverage_share(
            authors,
            value_key="pull_requests",
            total=float(overview["pull_requests"]),
            top_n=12,
        ),
        "reviewers_top_coverage_pct": _coverage_share(
            reviewers,
            value_key="pull_requests_reviewed",
            total=float(sum(int(row["pull_requests_reviewed"]) for row in reviewers)),
            top_n=10,
        ),
        "repositories_top_coverage_pct": _coverage_share(
            repositories,
            value_key="pull_requests",
            total=float(overview["pull_requests"]),
            top_n=10,
        ),
        "top3_author_share_pct": _coverage_share(
            authors,
            value_key="pull_requests",
            total=float(overview["pull_requests"]),
            top_n=3,
        ),
        "top3_repository_share_pct": _coverage_share(
            repositories,
            value_key="pull_requests",
            total=float(overview["pull_requests"]),
            top_n=3,
        ),
        "weekly_hidden_count": len(payload["weekly_trends_older"]),
        "monthly_hidden_count": len(payload["monthly_trends_older"]),
        "author_reference_count": len(payload["authors"]),
    }


def _build_size_diagnostic(size_buckets: list[dict[str, Any]]) -> dict[str, Any]:
    rows_with_latency = [
        row
        for row in size_buckets
        if row["pull_requests"] and row["median_first_review_hours"] is not None
    ]
    if not rows_with_latency:
        return {
            "headline": "No review-latency size signal available in this window.",
            "supporting": "This window does not contain enough reviewed PR size data to compare latency by bucket.",
        }
    slowest_row = max(rows_with_latency, key=lambda row: row["median_first_review_hours"])
    fastest_row = min(rows_with_latency, key=lambda row: row["median_first_review_hours"])
    gap = None
    if (
        slowest_row["median_first_review_hours"] is not None
        and fastest_row["median_first_review_hours"] is not None
    ):
        gap = _round(
            float(slowest_row["median_first_review_hours"])
            - float(fastest_row["median_first_review_hours"])
        )
    return {
        "headline": (
            f"{slowest_row['bucket']} PRs waited the longest for first review at "
            f"{_format_duration(slowest_row['median_first_review_hours'])} median."
        ),
        "supporting": (
            "Compared with "
            f"{fastest_row['bucket']} at {_format_duration(fastest_row['median_first_review_hours'])}, "
            f"the gap is {_format_duration(gap) if gap is not None else '-'}."
        ),
    }


def _split_recent_rows(
    rows: list[dict[str, Any]],
    *,
    recent_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if len(rows) <= recent_count:
        return list(reversed(rows)), []
    return list(reversed(rows[-recent_count:])), list(reversed(rows[:-recent_count]))


def _coverage_share(
    rows: list[dict[str, Any]],
    *,
    value_key: str,
    total: float,
    top_n: int,
) -> float | None:
    if not rows or total <= 0:
        return None
    covered = sum(float(row.get(value_key, 0) or 0) for row in rows[:top_n])
    return _round(covered / total * 100)


def _stale_open_pull_requests(
    pull_requests: list[dict[str, Any]],
    *,
    as_of: str,
    threshold_hours: int,
) -> int:
    as_of_datetime = datetime.fromisoformat(f"{as_of}T23:59:59+00:00")
    return sum(
        1
        for pull_request in pull_requests
        if pull_request["state"] == "open"
        and (
            as_of_datetime - _parse_datetime(str(pull_request["created_at"]))
        ).total_seconds()
        / 3600
        >= threshold_hours
    )


def _trimmed_values(
    pull_requests: list[dict[str, Any]],
    key: str,
    *,
    distribution_percentile: int,
    distribution_thresholds: dict[str, float | None],
) -> list[float]:
    threshold = distribution_thresholds.get(key)
    values = []
    for pull_request in pull_requests:
        raw_value = pull_request[key]
        if raw_value is None:
            continue
        numeric_value = float(raw_value)
        if threshold is not None and numeric_value > threshold:
            continue
        values.append(numeric_value)
    return list(values if distribution_percentile < 100 else trim_upper_tail(values, percentile=distribution_percentile))


def _build_distribution_thresholds(
    pull_requests: list[dict[str, Any]],
    *,
    distribution_percentile: int,
) -> dict[str, float | None]:
    metric_keys = (
        "changed_lines",
        "commits",
        "first_review_hours",
        "merge_hours",
        "close_hours",
    )
    thresholds: dict[str, float | None] = {}
    for metric_key in metric_keys:
        values = [
            float(pull_request[metric_key])
            for pull_request in pull_requests
            if pull_request[metric_key] is not None
        ]
        if distribution_percentile == 100:
            thresholds[metric_key] = (
                max(values) if values else None
            )
            continue
        thresholds[metric_key] = upper_percentile_threshold(
            values,
            percentile=distribution_percentile,
        )
    return thresholds


def _summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "count": 0,
            "total": 0,
            "average": None,
            "median": None,
        }
    return {
        "count": len(values),
        "total": sum(values),
        "average": sum(values) / len(values),
        "median": float(median(values)),
    }


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _period_key(value: datetime, *, grain: str) -> str:
    if grain == "month":
        return value.strftime("%Y-%m")
    iso_year, iso_week, _iso_weekday = value.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _as_int(value: int | float | None) -> int:
    if value is None:
        return 0
    return int(value)


def _template_environment() -> Environment:
    environment = Environment(
        loader=FileSystemLoader(
            str(Path(__file__).resolve().parents[1] / "templates")
        ),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )
    environment.filters["intfmt"] = _format_integer
    environment.filters["numfmt"] = _format_number
    environment.filters["duration"] = _format_duration
    environment.filters["deltafmt"] = _format_delta
    return environment


def _format_integer(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return f"{int(float(value)):,}"


def _format_number(value: Any) -> str:
    if value is None or value == "":
        return "-"
    number = float(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def _format_duration(value: Any) -> str:
    if value is None or value == "":
        return "-"
    hours = float(value)
    if hours < 1:
        return f"{round(hours * 60):,} min"
    if hours >= 24:
        return f"{hours / 24:,.1f} d"
    return f"{hours:,.1f} h"


def _format_delta(value: Any) -> Markup:
    if value is None or value == "":
        return Markup('<span class="delta-badge neutral">-</span>')
    number = float(value)
    css_class = "positive" if number > 0 else "negative" if number < 0 else "neutral"
    sign = "+" if number > 0 else ""
    text = f"{sign}{_format_number(number)}"
    return Markup(
        f'<span class="delta-badge {css_class}">{escape(text)}</span>',
    )


if __name__ == "__main__":
    raise SystemExit(
        "orgpulse.reporting.dashboard_html is no longer executable as a module. "
        "Use `orgpulse dashboard-render`."
    )
