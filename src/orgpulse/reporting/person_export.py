from __future__ import annotations

import csv
import json
from io import StringIO

from orgpulse.person import (
    PersonExportFormat,
    PersonMetricsResult,
    PersonPeriodRow,
)
from orgpulse.reporting.person_report import render_person_report_html


def render_person_metrics_result(
    result: PersonMetricsResult,
) -> str:
    """Render a person metrics result into the requested export format."""

    if result.export_format is PersonExportFormat.CSV:
        return _render_csv(result)
    if result.export_format is PersonExportFormat.MARKDOWN:
        return _render_markdown(result)
    if result.export_format is PersonExportFormat.HTML:
        return render_person_report_html(result)
    return _render_json(result)


def _render_csv(
    result: PersonMetricsResult,
) -> str:
    fieldnames = tuple(PersonPeriodRow.model_fields.keys())
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in result.period_rows:
        writer.writerow(
            {
                key: "" if value is None else value
                for key, value in row.model_dump(mode="json").items()
            }
        )
    return buffer.getvalue().rstrip("\n")


def _render_markdown(
    result: PersonMetricsResult,
) -> str:
    summary = result.summary
    reviewer = result.reviewer_summary
    lines = [
        f"# orgpulse person metrics: {result.login}",
        "",
        f"- Organization: {result.target_org}",
        f"- Grain: {result.grain.value}",
        f"- Time anchor: {result.time_anchor.value}",
        f"- Since: {result.since.isoformat() if result.since is not None else 'all'}",
        f"- Until: {result.until.isoformat() if result.until is not None else 'all'}",
        f"- Distribution percentile: {result.distribution_percentile}",
        "",
        "## Summary",
        "",
        f"- Authored PRs: {summary.authored_pull_request_count}",
        f"- Merged PRs: {summary.merged_pull_request_count}",
        f"- Open PRs: {summary.open_pull_request_count}",
        f"- Merge rate: {_markdown_number(summary.merge_rate_pct)}%",
        f"- Changed lines: {summary.changed_lines_total}",
        f"- Commits: {summary.commits_total}",
        f"- Reviews received: {summary.reviews_received}",
        f"- Review coverage: {_markdown_number(summary.review_coverage_pct)}%",
        f"- Median first review hours: {_markdown_number(summary.median_first_review_hours)}",
        f"- Median approval hours: {_markdown_number(summary.median_approval_hours)}",
        f"- Median merge hours: {_markdown_number(summary.median_merge_hours)}",
        "",
        "## Reviewer Activity",
        "",
        f"- Reviews submitted: {reviewer.review_submissions}",
        f"- PRs reviewed: {reviewer.pull_requests_reviewed}",
        f"- PRs reviewed per month: {_markdown_number(reviewer.pull_requests_reviewed_per_month)}",
        f"- Reviewed lines: {reviewer.reviewed_lines}",
        f"- Reviewed lines per month: {_markdown_number(reviewer.reviewed_lines_per_month)}",
        f"- Approvals: {reviewer.approvals}",
        f"- Changes requested: {reviewer.changes_requested}",
        f"- Comments: {reviewer.comments}",
        f"- Authors supported: {reviewer.authors_supported}",
        f"- Repositories reviewed: {reviewer.repositories_reviewed}",
        "",
        "## Periods",
        "",
        "| Period | Authored PRs | Merged | Open | Changed Lines | Commits | Reviews Received | Approval Hours | Reviews Given | PRs Reviewed | Reviewed Lines |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.period_rows:
        lines.append(
            "| "
            f"{row.period_key} | "
            f"{row.authored_pull_request_count} | "
            f"{row.merged_pull_request_count} | "
            f"{row.open_pull_request_count} | "
            f"{row.changed_lines_total} | "
            f"{row.commits_total} | "
            f"{row.reviews_received} | "
            f"{_markdown_number(row.median_approval_hours)} | "
            f"{row.review_submissions_given} | "
            f"{row.pull_requests_reviewed} | "
            f"{row.reviewed_lines} |"
        )
    lines.extend(
        [
            "",
            "## Repositories",
            "",
            "| Repository | Authored PRs | Merged | Open | Changed Lines | Reviews Given | PRs Reviewed | Reviewed Lines |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.repository_rows:
        lines.append(
            "| "
            f"{row.repository_full_name} | "
            f"{row.authored_pull_request_count} | "
            f"{row.merged_pull_request_count} | "
            f"{row.open_pull_request_count} | "
            f"{row.changed_lines_total} | "
            f"{row.review_submissions_given} | "
            f"{row.pull_requests_reviewed} | "
            f"{row.reviewed_lines} |"
        )
    return "\n".join(lines)


def _render_json(
    result: PersonMetricsResult,
) -> str:
    return json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True)


def _markdown_number(
    value: float | None,
) -> str:
    if value is None:
        return "-"
    return str(_display_value(value))


def _display_value(
    value: object,
) -> object:
    if value is None:
        return "-"
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value
