from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup


def main() -> None:
    args = _parse_args()
    payload = _load_payload(args.input_json)
    html = _render_html(payload)
    args.output_html.write_text(html, encoding="utf-8")
    print(
        json.dumps(
            {
                "input_json": str(args.input_json),
                "output_html": str(args.output_html),
            },
            ensure_ascii=False,
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the manual org dashboard HTML from an existing JSON payload.",
    )
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--output-html", type=Path, required=True)
    return parser.parse_args()


def _load_payload(path: Path) -> dict[str, Any]:
    return _prepare_payload(json.loads(path.read_text(encoding="utf-8")))


def _render_html(payload: dict[str, Any]) -> str:
    template = _template_environment().get_template("manual_org_dashboard.html.j2")
    return template.render(
        overview=payload["overview"],
        insights=payload["insights"],
        authors=payload["authors"],
        reviewers=payload["reviewers"],
        repositories_top=payload["repositories_top"],
        repositories_rest=payload["repositories_rest"],
        size_buckets=payload["size_buckets"],
        review_state_rows=payload["review_state_rows"],
        weekly_trends=payload["weekly_trends"],
        monthly_trends=payload["monthly_trends"],
        default_author=payload["default_author"],
        author_details_json=Markup(payload["author_details_json"]),
    )


def _prepare_payload(payload: dict[str, Any]) -> dict[str, Any]:
    pull_requests = payload["pull_requests"]
    payload["repositories_top"] = payload["repositories"][:10]
    payload["repositories_rest"] = payload["repositories"][10:]
    payload["weekly_trends"] = _build_trend_rows(pull_requests, grain="week")
    payload["monthly_trends"] = _build_trend_rows(pull_requests, grain="month")
    payload["default_author"] = (
        payload["authors"][0]["author_login"] if payload["authors"] else None
    )
    author_details = _build_author_details(
        authors=payload["authors"],
        pull_requests=pull_requests,
    )
    payload["author_details_json"] = json.dumps(
        author_details,
        ensure_ascii=False,
    ).replace("</script>", "<\\/script>")
    return payload


def _build_trend_rows(
    pull_requests: list[dict[str, Any]],
    *,
    grain: str,
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
        changed_lines = sum(
            int(pull_request["changed_lines"]) for pull_request in period_rows
        )
        review_submissions = sum(
            int(pull_request["review_count"]) for pull_request in period_rows
        )
        merged_count = sum(
            1 for pull_request in period_rows if pull_request["merged_at"] is not None
        )
        open_count = sum(
            1 for pull_request in period_rows if pull_request["state"] == "open"
        )
        first_review_values = [
            float(pull_request["first_review_hours"])
            for pull_request in period_rows
            if pull_request["first_review_hours"] is not None
        ]
        merge_values = [
            float(pull_request["merge_hours"])
            for pull_request in period_rows
            if pull_request["merge_hours"] is not None
        ]
        row = {
            "period_key": period_key,
            "pull_requests": pull_request_count,
            "merged_pull_requests": merged_count,
            "open_pull_requests": open_count,
            "changed_lines": changed_lines,
            "review_submissions": review_submissions,
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
                changed_lines - previous_changed_lines
                if previous_changed_lines is not None
                else None
            ),
        }
        rows.append(row)
        previous_pull_requests = pull_request_count
        previous_changed_lines = changed_lines
    return rows


def _build_author_details(
    *,
    authors: list[dict[str, Any]],
    pull_requests: list[dict[str, Any]],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pull_request in pull_requests:
        grouped[str(pull_request["author_login"])].append(pull_request)
    details: dict[str, Any] = {}
    for author in authors:
        author_login = author["author_login"]
        author_pull_requests = grouped.get(author_login, [])
        repository_counter = Counter(
            str(pull_request["repository_full_name"]) for pull_request in author_pull_requests
        )
        size_counter = Counter(
            str(pull_request["size_bucket"]) for pull_request in author_pull_requests
        )
        details[author_login] = {
            "summary": {
                **author,
                "average_changed_lines_per_pr": _round(
                    (
                        sum(int(pull_request["changed_lines"]) for pull_request in author_pull_requests)
                        / len(author_pull_requests)
                    )
                    if author_pull_requests
                    else None
                ),
                "average_commits_per_pr": _round(
                    (
                        sum(int(pull_request["commits"]) for pull_request in author_pull_requests)
                        / len(author_pull_requests)
                    )
                    if author_pull_requests
                    else None
                ),
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
            },
            "top_repositories": [
                {
                    "repository_full_name": repository_full_name,
                    "pull_requests": count,
                    "changed_lines": sum(
                        int(pull_request["changed_lines"])
                        for pull_request in author_pull_requests
                        if pull_request["repository_full_name"] == repository_full_name
                    ),
                }
                for repository_full_name, count in sorted(
                    repository_counter.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:5]
            ],
            "size_mix": [
                {
                    "bucket": bucket,
                    "pull_requests": size_counter.get(bucket, 0),
                }
                for bucket in ("XS", "S", "M", "L", "XL")
            ],
            "weekly_trends": _build_trend_rows(author_pull_requests, grain="week"),
            "monthly_trends": _build_trend_rows(author_pull_requests, grain="month"),
        }
    return details


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


def _template_environment() -> Environment:
    return Environment(
        loader=FileSystemLoader(
            str(Path(__file__).resolve().parents[1] / "src" / "orgpulse" / "templates")
        ),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )


if __name__ == "__main__":
    main()
