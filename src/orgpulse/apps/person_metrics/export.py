from __future__ import annotations

import csv
import json
from io import StringIO

from orgpulse.apps.person_metrics.report import render_person_report_html
from orgpulse.apps.person_metrics.service import (
    OrgTrendRow,
    PersonExportFormat,
    PersonMetricsResult,
    PersonPeriodRow,
)


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
    fieldnames = ("period_grain", *PersonPeriodRow.model_fields.keys())
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for period_grain, rows in (
        ("week", result.weekly_period_rows),
        ("month", result.monthly_period_rows),
    ):
        for row in rows:
            writer.writerow(
                {
                    "period_grain": period_grain,
                    **{
                        key: "" if value is None else value
                        for key, value in row.model_dump(mode="json").items()
                    },
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
        f"- Source grain: {result.grain.value}",
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
        "## Weekly Periods",
        "",
        "| Period | Authored PRs | Merged | Open | Changed Lines | Commits | Reviews Received | Approval Hours | Reviews Given | PRs Reviewed | Reviewed Lines |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.weekly_period_rows:
        lines.append(_markdown_period_row(row))
    lines.extend(
        [
            "",
            "## Monthly Periods",
            "",
            "| Period | Authored PRs | Merged | Open | Changed Lines | Commits | Reviews Received | Approval Hours | Reviews Given | PRs Reviewed | Reviewed Lines |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in result.monthly_period_rows:
        lines.append(_markdown_period_row(row))
    lines.extend(
        [
            "",
            "## Repositories",
            "",
            "| Repository | Authored PRs | Merged | Open | Changed Lines | Approval Hours | Reviews Given | PRs Reviewed | Reviewed Lines |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
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
            f"{_markdown_number(row.median_approval_hours)} | "
            f"{row.review_submissions_given} | "
            f"{row.pull_requests_reviewed} | "
            f"{row.reviewed_lines} |"
        )
    if result.include_org_trends:
        lines.extend(_render_org_trends_markdown(result))
    return "\n".join(lines)


def _markdown_period_row(
    row: PersonPeriodRow,
) -> str:
    return (
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


def _render_org_trends_markdown(
    result: PersonMetricsResult,
) -> list[str]:
    lines = [
        "",
        "## Org Trends",
        "",
        "### Weekly",
        "",
        *_render_org_trend_table(result.org_weekly_trend_rows or ()),
        "",
        "### Monthly",
        "",
        *_render_org_trend_table(result.org_monthly_trend_rows or ()),
    ]
    return lines


def _render_org_trend_table(
    rows: tuple[OrgTrendRow, ...],
) -> list[str]:
    lines = [
        "| Period | PRs | Merged | Open | Active Authors | Changed Lines | PRs / Active Author | Lines / Active Author | Reviews | Approval Hours | First Review Hours | Merge Hours |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.period_key} | "
            f"{row.pull_requests} | "
            f"{row.merged_pull_requests} | "
            f"{row.open_pull_requests} | "
            f"{row.active_authors} | "
            f"{row.changed_lines} | "
            f"{_markdown_number(row.pull_requests_per_active_author)} | "
            f"{_markdown_number(row.changed_lines_per_active_author)} | "
            f"{row.review_submissions} | "
            f"{_markdown_number(row.median_approval_hours)} | "
            f"{_markdown_number(row.median_first_review_hours)} | "
            f"{_markdown_number(row.median_merge_hours)} |"
        )
    return lines


def _render_json(
    result: PersonMetricsResult,
) -> str:
    return json.dumps(_json_payload(result), indent=2, sort_keys=True)


def _json_payload(
    result: PersonMetricsResult,
) -> dict[str, object]:
    payload = result.model_dump(mode="json")
    if not result.include_org_trends:
        payload.pop("include_org_trends", None)
        payload.pop("org_weekly_trend_rows", None)
        payload.pop("org_monthly_trend_rows", None)
    return payload


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
