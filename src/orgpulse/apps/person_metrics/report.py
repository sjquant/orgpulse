from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from orgpulse.apps.person_metrics.service import (
    PersonMetricsResult,
    PersonPeriodRow,
    PersonRepositoryRow,
)


def render_person_report_html(
    result: PersonMetricsResult,
) -> str:
    """Render a person metrics result as an HTML report."""

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
    period_status_label = _period_status_label(result.period_rows)
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
        period_status_label=period_status_label,
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


def _period_status_label(
    rows: tuple[PersonPeriodRow, ...],
) -> str:
    if any(row.open_month for row in rows):
        return "open month"
    if any(row.open_week for row in rows):
        return "open week"
    if any(row.is_partial for row in rows):
        return "partial window"
    return "closed window"


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
            str(Path(__file__).resolve().parents[2] / "templates")
        ),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )
    environment.filters["intfmt"] = _format_integer
    environment.filters["numfmt"] = _format_number
    environment.filters["duration"] = _format_duration
    environment.filters["pctfmt"] = _format_percent
    environment.filters["json_script"] = _json_script
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
