from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from orgpulse.apps.person_metrics.service import (
    OrgTrendRow,
    PersonMetricsResult,
    PersonPeriodRow,
    PersonRepositoryRow,
)
from orgpulse.common.models import ReportLocale
from orgpulse.libs.reporting.i18n import (
    format_duration,
    format_integer,
    format_number,
    format_percent,
    metric_label_html,
    metric_text,
    period_state_text,
    report_i18n_json,
    report_text,
    resolve_report_locale,
)


def render_person_report_html(
    result: PersonMetricsResult,
    *,
    locale: ReportLocale | str | None = None,
) -> str:
    """Render a person metrics result as an HTML report."""

    resolved_locale = resolve_report_locale(locale)
    progressive_chunk_size = 5
    top_repository_rows = _top_repository_rows(result.repository_rows)
    repository_top_rows, repository_rest_rows = _split_ranked_repository_rows(
        result.repository_rows,
        top_count=progressive_chunk_size,
    )
    weekly_period_rows_recent, weekly_period_rows_older = _split_recent_rows(
        result.weekly_period_rows,
        recent_count=progressive_chunk_size,
    )
    monthly_period_rows_recent, monthly_period_rows_older = _split_recent_rows(
        result.monthly_period_rows,
        recent_count=progressive_chunk_size,
    )
    window_label = _window_label(result)
    report_payload = _html_report_payload(result)
    template = _template_environment(resolved_locale).get_template(
        "person_report.html.j2"
    )
    return template.render(
        locale=resolved_locale.value,
        t=lambda key: report_text(resolved_locale, key),
        metric_label=lambda key, fallback=None: metric_label_html(
            resolved_locale,
            key,
            fallback=fallback,
        ),
        metric_text=lambda key, fallback=None: metric_text(
            resolved_locale,
            key,
            fallback=fallback,
        ),
        period_state=lambda row: period_state_text(resolved_locale, row),
        report_i18n_json=report_i18n_json(resolved_locale),
        result=result,
        summary=result.summary,
        reviewer_summary=result.reviewer_summary,
        repository_top_rows=repository_top_rows,
        repository_rest_rows=repository_rest_rows,
        weekly_period_rows_recent=weekly_period_rows_recent,
        weekly_period_rows_older=weekly_period_rows_older,
        monthly_period_rows_recent=monthly_period_rows_recent,
        monthly_period_rows_older=monthly_period_rows_older,
        top_repository_rows=top_repository_rows,
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
    if not result.include_org_trends:
        payload.pop("include_org_trends", None)
        payload.pop("org_weekly_trend_rows", None)
        payload.pop("org_monthly_trend_rows", None)
        payload.pop("org_weekly_comparison_rows", None)
        payload.pop("org_monthly_comparison_rows", None)
    else:
        payload["org_weekly_comparison_rows"] = _org_comparison_rows(
            result.org_weekly_trend_rows or (),
        )
        payload["org_monthly_comparison_rows"] = _org_comparison_rows(
            result.org_monthly_trend_rows or (),
        )
    return payload


def _org_comparison_rows(
    rows: tuple[OrgTrendRow, ...],
) -> list[dict[str, Any]]:
    return [_org_comparison_row(row) for row in rows]


def _org_comparison_row(
    row: OrgTrendRow,
) -> dict[str, Any]:
    payload = row.model_dump(mode="json")
    payload["pull_requests"] = row.pull_requests_per_active_author
    payload["merged_pull_requests"] = _per_person(
        row.merged_pull_requests,
        row.active_authors,
    )
    payload["open_pull_requests"] = _per_person(
        row.open_pull_requests,
        row.active_authors,
    )
    payload["changed_lines"] = row.changed_lines_per_active_author
    payload["authored_pull_request_count"] = row.pull_requests_per_active_author
    payload["changed_lines_total"] = row.changed_lines_per_active_author
    payload["commits_total"] = _per_person(row.commits_total, row.active_authors)
    payload["review_submissions"] = _per_person(
        row.review_submissions,
        row.active_reviewers,
    )
    payload["review_submissions_given"] = _per_person(
        row.review_submissions_given,
        row.active_reviewers,
    )
    payload["pull_requests_reviewed"] = _per_person(
        row.pull_requests_reviewed,
        row.active_reviewers,
    )
    payload["reviewed_lines"] = _per_person(row.reviewed_lines, row.active_reviewers)
    return payload


def _per_person(
    value: int,
    people_count: int,
) -> float | None:
    if people_count == 0:
        return None
    return round(value / people_count, 2)


def _template_environment(locale: ReportLocale) -> Environment:
    environment = Environment(
        loader=FileSystemLoader(str(Path(__file__).resolve().parents[2] / "templates")),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )
    environment.filters["intfmt"] = lambda value: format_integer(value, locale)
    environment.filters["numfmt"] = lambda value: format_number(value, locale)
    environment.filters["duration"] = lambda value: format_duration(value, locale)
    environment.filters["pctfmt"] = lambda value: format_percent(value, locale)
    environment.filters["json_script"] = _json_script
    return environment


def _json_script(value: Any) -> Markup:
    payload = json.dumps(value, ensure_ascii=False).replace("</", "<\\/")
    return Markup(
        payload.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("'", "\\u0027")
    )
