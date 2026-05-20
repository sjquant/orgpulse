from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from orgpulse.person import (
    PersonExportFormat,
    PersonMetricsResult,
    PersonPeriodRow,
    PersonRepositoryRow,
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
        return _render_html(result)
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
        f"- Median merge hours: {_markdown_number(summary.median_merge_hours)}",
        "",
        "## Reviewer Activity",
        "",
        f"- Reviews submitted: {reviewer.review_submissions}",
        f"- PRs reviewed: {reviewer.pull_requests_reviewed}",
        f"- Approvals: {reviewer.approvals}",
        f"- Changes requested: {reviewer.changes_requested}",
        f"- Comments: {reviewer.comments}",
        f"- Authors supported: {reviewer.authors_supported}",
        f"- Repositories reviewed: {reviewer.repositories_reviewed}",
        "",
        "## Periods",
        "",
        "| Period | Authored PRs | Merged | Open | Changed Lines | Commits | Reviews Received | Reviews Given | PRs Reviewed |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
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
            f"{row.review_submissions_given} | "
            f"{row.pull_requests_reviewed} |"
        )
    lines.extend(
        [
            "",
            "## Repositories",
            "",
            "| Repository | Authored PRs | Merged | Open | Changed Lines | Reviews Given | PRs Reviewed |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
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
            f"{row.pull_requests_reviewed} |"
        )
    return "\n".join(lines)


def _render_json(
    result: PersonMetricsResult,
) -> str:
    return json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True)


def _render_html(
    result: PersonMetricsResult,
) -> str:
    progressive_chunk_size = 5
    top_repository_rows = _top_repository_rows(result.repository_rows)
    period_recent_rows, period_older_rows = _split_recent_rows(
        result.period_rows,
        recent_count=progressive_chunk_size,
    )
    repository_top_rows, repository_rest_rows = _split_ranked_repository_rows(
        result.repository_rows,
        top_count=progressive_chunk_size,
    )
    window_label = _window_label(result)
    report_payload = _html_report_payload(result)
    template = _template_environment().get_template("person_report.html.j2")
    return template.render(
        result=result,
        summary=result.summary,
        reviewer_summary=result.reviewer_summary,
        period_recent_rows=period_recent_rows,
        period_older_rows=period_older_rows,
        repository_top_rows=repository_top_rows,
        repository_rest_rows=repository_rest_rows,
        top_repository_rows=top_repository_rows,
        period_total_count=len(result.period_rows),
        repository_total_count=len(result.repository_rows),
        progressive_chunk_size=progressive_chunk_size,
        report_payload=report_payload,
        window_label=window_label,
    )


def _split_recent_rows(
    rows: tuple[PersonPeriodRow, ...],
    *,
    recent_count: int,
) -> tuple[tuple[PersonPeriodRow, ...], tuple[PersonPeriodRow, ...]]:
    if len(rows) <= recent_count:
        return tuple(reversed(rows)), ()
    return tuple(reversed(rows[-recent_count:])), tuple(reversed(rows[:-recent_count]))


def _split_ranked_repository_rows(
    rows: tuple[PersonRepositoryRow, ...],
    *,
    top_count: int,
) -> tuple[tuple[PersonRepositoryRow, ...], tuple[PersonRepositoryRow, ...]]:
    ranked_rows = _ranked_repository_rows(rows)
    return ranked_rows[:top_count], ranked_rows[top_count:]


def _ranked_repository_rows(
    rows: tuple[PersonRepositoryRow, ...],
) -> tuple[PersonRepositoryRow, ...]:
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                -row.authored_pull_request_count,
                -row.review_submissions_given,
                -row.changed_lines_total,
                row.repository_full_name,
            ),
        )
    )


def _top_repository_rows(
    rows: tuple[PersonRepositoryRow, ...],
) -> tuple[PersonRepositoryRow, ...]:
    return _ranked_repository_rows(rows)[:6]


def _window_label(
    result: PersonMetricsResult,
) -> str:
    since = result.since.isoformat() if result.since is not None else "all"
    until = result.until.isoformat() if result.until is not None else "all"
    return f"{since} to {until}"


def _html_report_payload(
    result: PersonMetricsResult,
) -> dict[str, Any]:
    payload = result.model_dump(mode="json")
    payload.pop("source_manifest_path", None)
    payload.pop("output_dir", None)
    return payload


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
    environment.filters["pctfmt"] = _format_percent
    environment.filters["json_script"] = _json_script
    return environment


def _markdown_number(
    value: float | None,
) -> str:
    if value is None:
        return "-"
    return str(_display_value(value))


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


def _format_percent(value: Any) -> str:
    if value is None or value == "":
        return "-"
    return f"{_format_number(value)}%"


def _json_script(value: Any) -> Markup:
    payload = json.dumps(value, ensure_ascii=False).replace("</", "<\\/")
    return Markup(
        payload.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("'", "\\u0027")
    )


def _display_value(
    value: object,
) -> object:
    if value is None:
        return "-"
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value
