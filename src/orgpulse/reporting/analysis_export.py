from __future__ import annotations

import csv
import json
from io import StringIO

from orgpulse.analysis import (
    AnalysisExportFormat,
    AnalysisInputError,
    AnalysisResult,
    AnalysisRow,
)
from orgpulse.reporting.analysis_report import render_analysis_report_html


def render_analysis_result(
    result: AnalysisResult,
) -> str:
    """Render an analysis result into the requested export format.

    Args:
        result: Fully computed analysis result.

    Returns:
        A serialized export document.
    """

    if result.export_format is AnalysisExportFormat.CSV:
        return _render_csv(result)
    if result.export_format is AnalysisExportFormat.MARKDOWN:
        return _render_markdown(result)
    if result.export_format is AnalysisExportFormat.HTML:
        return _render_html(result)
    return _render_json(result)


def _render_csv(
    result: AnalysisResult,
) -> str:
    fieldnames = tuple(AnalysisRow.model_fields.keys())
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in result.rows:
        writer.writerow(
            {
                key: "" if value is None else value
                for key, value in row.model_dump(mode="json").items()
            }
        )
    return buffer.getvalue().rstrip("\n")


def _render_markdown(
    result: AnalysisResult,
) -> str:
    lines = [
        f"# orgpulse analysis: {result.target_org}",
        "",
        f"- Grain: {result.grain.value}",
        f"- Grouping: {result.grouping.value}",
        f"- Time anchor: {result.time_anchor.value}",
        f"- Since: {result.since.isoformat() if result.since is not None else 'all'}",
        f"- Until: {result.until.isoformat() if result.until is not None else 'all'}",
        f"- Distribution percentile: {result.distribution_percentile}",
        f"- Matched pull requests: {result.matched_pull_request_count}",
        f"- Top N: {result.top_n if result.top_n is not None else 'all'}",
        "",
        "| Group | Period | PRs | Merged PRs | Active Authors | Avg Merge Seconds | Avg First Review Seconds |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.rows:
        lines.append(
            "| "
            f"{row.group_value} | "
            f"{row.period_key or '-'} | "
            f"{row.pull_request_count} | "
            f"{row.merged_pull_request_count} | "
            f"{row.active_author_count} | "
            f"{_markdown_number(row.time_to_merge_average_seconds)} | "
            f"{_markdown_number(row.time_to_first_review_average_seconds)} |"
        )
    return "\n".join(lines)


def _render_json(
    result: AnalysisResult,
) -> str:
    return json.dumps(
        result.model_dump(mode="json"),
        indent=2,
        sort_keys=True,
    )


def _render_html(
    result: AnalysisResult,
) -> str:
    if result.report_payload is None:
        raise AnalysisInputError("analysis report payload is unavailable")
    return render_analysis_report_html(result.report_payload)


def _markdown_number(
    value: float | None,
) -> str:
    if value is None:
        return "-"
    normalized = float(value)
    if normalized.is_integer():
        return str(int(normalized))
    return f"{normalized:.2f}"
