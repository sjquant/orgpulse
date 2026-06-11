from __future__ import annotations

import json
from typing import Any

from markupsafe import Markup, escape

from orgpulse.common.config import get_settings
from orgpulse.common.models import ReportLocale


def resolve_report_locale(locale: ReportLocale | str | None = None) -> ReportLocale:
    """Resolve an explicit or environment-backed report locale."""

    if locale is None:
        return get_settings().locale
    return ReportLocale(locale)


def report_i18n_payload(locale: ReportLocale | str | None = None) -> dict[str, Any]:
    """Return JSON-serializable translation data for report templates."""

    resolved_locale = resolve_report_locale(locale)
    return {
        "locale": resolved_locale.value,
        "static": _STATIC[resolved_locale],
        "metrics": _METRICS[resolved_locale],
    }


def report_i18n_json(locale: ReportLocale | str | None = None) -> Markup:
    """Return an escaped JSON translation payload for inline report scripts."""

    payload = json.dumps(
        report_i18n_payload(locale),
        ensure_ascii=False,
        sort_keys=True,
    ).replace("</", "<\\/")
    return Markup(
        payload.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("'", "\\u0027")
    )


def report_text(
    locale: ReportLocale | str | None,
    key: str,
) -> str:
    """Return a translated static report string."""

    resolved_locale = resolve_report_locale(locale)
    return _STATIC[resolved_locale].get(key, _STATIC[ReportLocale.EN].get(key, key))


def metric_text(
    locale: ReportLocale | str | None,
    key: str,
    *,
    fallback: str | None = None,
) -> str:
    """Return a translated metric label."""

    resolved_locale = resolve_report_locale(locale)
    return (
        _METRICS[resolved_locale].get(key, {}).get("label")
        or _METRICS[ReportLocale.EN].get(key, {}).get("label")
        or fallback
        or key.replace("_", " ")
    )


def metric_description(
    locale: ReportLocale | str | None,
    key: str,
    *,
    fallback: str | None = None,
) -> str:
    """Return a translated metric description."""

    resolved_locale = resolve_report_locale(locale)
    return (
        _METRICS[resolved_locale].get(key, {}).get("description")
        or _METRICS[ReportLocale.EN].get(key, {}).get("description")
        or fallback
        or metric_text(resolved_locale, key)
    )


def metric_label_html(
    locale: ReportLocale | str | None,
    key: str,
    *,
    fallback: str | None = None,
) -> Markup:
    """Render an accessible metric label with a native tooltip fallback."""

    label = metric_text(locale, key, fallback=fallback)
    description = metric_description(locale, key, fallback=label)
    return Markup(
        '<span class="metric-label-with-tooltip">'
        f"<span>{escape(label)}</span>"
        '<span class="metric-tooltip" tabindex="0" role="note" '
        f'title="{escape(description)}" aria-label="{escape(description)}">?</span>'
        "</span>"
    )


def format_integer(
    value: Any,
    locale: ReportLocale | str | None = None,
) -> str:
    """Format an integer metric for the selected report locale."""

    if value is None or value == "":
        return "-"
    return f"{int(float(value)):,}"


def format_number(
    value: Any,
    locale: ReportLocale | str | None = None,
) -> str:
    """Format a numeric metric for the selected report locale."""

    if value is None or value == "":
        return "-"
    number = float(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def format_duration(
    value: Any,
    locale: ReportLocale | str | None = None,
) -> str:
    """Format an hour-based duration for the selected report locale."""

    if value is None or value == "":
        return "-"
    resolved_locale = resolve_report_locale(locale)
    hours = float(value)
    if hours < 1:
        minutes = format_integer(round(hours * 60), resolved_locale)
        return f"{minutes}분" if resolved_locale is ReportLocale.KO else f"{minutes} min"
    if hours >= 24:
        days = format_number(hours / 24, resolved_locale)
        return f"{days}일" if resolved_locale is ReportLocale.KO else f"{days} d"
    formatted_hours = format_number(hours, resolved_locale)
    return f"{formatted_hours}시간" if resolved_locale is ReportLocale.KO else f"{formatted_hours} h"


def format_percent(
    value: Any,
    locale: ReportLocale | str | None = None,
) -> str:
    """Format a percent value for the selected report locale."""

    if value is None or value == "":
        return "-"
    return f"{format_number(value, locale)}%"


_STATIC: dict[ReportLocale, dict[str, str]] = {
    ReportLocale.EN: {
        "analysis.title": "orgpulse analysis report",
        "analysis.eyebrow": "Diagnostics and visualization",
        "analysis.view": "View",
        "analysis.metric": "Metric",
        "analysis.period": "Period",
        "analysis.top_series": "Top series",
        "analysis.series_filter": "Series filter",
        "analysis.series_filter_placeholder": "repo or author name",
        "analysis.single_series_focus": "Single-series focus",
        "analysis.trend_view": "Trend view",
        "analysis.trend": "Trend",
        "analysis.selected_period": "Selected period",
        "analysis.spike_diagnostics": "Spike diagnostics",
        "analysis.ranking": "Ranking",
        "analysis.current_ranking": "Current ranking",
        "dashboard.title_suffix": "productivity leaderboard",
        "dashboard.eyebrow": "Engineering Productivity Leaderboard",
        "dashboard.heading_suffix": "review and throughput board",
        "dashboard.subtitle": "Leaderboard view for the selected window. The page focuses on who shipped, who reviewed, where flow concentrated, and how review latency moved with PR size, with chart tabs for period and person-level inspection.",
        "person.title_prefix": "orgpulse person metrics",
        "person.eyebrow": "Person Performance Extract",
        "person.heading_suffix": "performance",
        "person.subtitle_suffix": "Review-given activity is counted by review submission time.",
        "theme.color": "Color theme",
        "theme.dark": "Dark theme",
        "theme.light": "Light theme",
        "common.window": "Window",
        "common.organization": "Organization",
        "common.source_grain": "Source grain",
        "common.distribution_cutoff": "Distribution cutoff",
        "common.open_period_label": "Open period",
        "common.closed_window": "closed window",
        "common.weekly": "Weekly",
        "common.monthly": "Monthly",
        "common.weekly_report": "Weekly report",
        "common.monthly_report": "Monthly report",
        "common.week": "Week",
        "common.month": "Month",
        "common.state": "State",
        "common.repository": "Repository",
        "common.repositories": "Repositories",
        "common.methodology": "Methodology",
        "common.charts": "Charts",
        "common.reference": "Reference",
        "common.trends": "Trends",
        "common.people": "People",
        "common.diagnostics": "Diagnostics",
        "common.selected_metric": "Selected metric",
        "common.open_period_legend": "Yellow band = open period",
        "common.selected_period": "Selected period",
        "common.selected_value": "Selected value",
        "common.window_average": "Window average",
        "common.period_state": "Period state",
        "common.no_period_data": "No period data available.",
        "common.no_metric_data": "No data for this metric.",
        "common.no_view_data": "No data for this view.",
        "common.open_week": "open week",
        "common.open_month": "open month",
        "common.partial_period": "partial period",
        "common.closed_period": "closed period",
        "common.open_period": "open period",
    },
    ReportLocale.KO: {
        "analysis.title": "orgpulse 분석 보고서",
        "analysis.eyebrow": "진단 및 시각화",
        "analysis.view": "보기",
        "analysis.metric": "지표",
        "analysis.period": "기간",
        "analysis.top_series": "상위 시리즈",
        "analysis.series_filter": "시리즈 필터",
        "analysis.series_filter_placeholder": "저장소 또는 작성자 이름",
        "analysis.single_series_focus": "단일 시리즈 집중",
        "analysis.trend_view": "추세 보기",
        "analysis.trend": "추세",
        "analysis.selected_period": "선택한 기간",
        "analysis.spike_diagnostics": "스파이크 진단",
        "analysis.ranking": "순위",
        "analysis.current_ranking": "현재 순위",
        "dashboard.title_suffix": "생산성 리더보드",
        "dashboard.eyebrow": "엔지니어링 생산성 리더보드",
        "dashboard.heading_suffix": "리뷰 및 처리량 보드",
        "dashboard.subtitle": "선택한 기간의 리더보드입니다. 누가 배포했고, 누가 리뷰했으며, 흐름이 어디에 집중되었고 PR 크기에 따라 리뷰 지연이 어떻게 달라졌는지 확인합니다.",
        "person.title_prefix": "orgpulse 개인 지표",
        "person.eyebrow": "개인 성과 추출",
        "person.heading_suffix": "성과",
        "person.subtitle_suffix": "제공한 리뷰 활동은 리뷰 제출 시간을 기준으로 집계합니다.",
        "theme.color": "색상 테마",
        "theme.dark": "어두운 테마",
        "theme.light": "밝은 테마",
        "common.window": "기간",
        "common.organization": "조직",
        "common.source_grain": "소스 단위",
        "common.distribution_cutoff": "분포 절단값",
        "common.open_period_label": "열린 기간",
        "common.closed_window": "닫힌 기간",
        "common.weekly": "주간",
        "common.monthly": "월간",
        "common.weekly_report": "주간 보고서",
        "common.monthly_report": "월간 보고서",
        "common.week": "주",
        "common.month": "월",
        "common.state": "상태",
        "common.repository": "저장소",
        "common.repositories": "저장소",
        "common.methodology": "방법론",
        "common.charts": "차트",
        "common.reference": "참고",
        "common.trends": "추세",
        "common.people": "사람",
        "common.diagnostics": "진단",
        "common.selected_metric": "선택한 지표",
        "common.open_period_legend": "노란색 구간 = 열린 기간",
        "common.selected_period": "선택한 기간",
        "common.selected_value": "선택값",
        "common.window_average": "기간 평균",
        "common.period_state": "기간 상태",
        "common.no_period_data": "기간 데이터가 없습니다.",
        "common.no_metric_data": "이 지표의 데이터가 없습니다.",
        "common.no_view_data": "이 보기의 데이터가 없습니다.",
        "common.open_week": "열린 주",
        "common.open_month": "열린 월",
        "common.partial_period": "부분 기간",
        "common.closed_period": "닫힌 기간",
        "common.open_period": "열린 기간",
    },
}


_METRICS: dict[ReportLocale, dict[str, dict[str, str]]] = {
    ReportLocale.EN: {
        "pull_requests": {
            "label": "PR throughput",
            "description": "Pull requests counted in the selected reporting window.",
        },
        "pull_request_count": {
            "label": "Pull requests",
            "description": "Pull requests counted in the selected reporting window.",
        },
        "authored_pull_request_count": {
            "label": "Authored PRs",
            "description": "Pull requests authored by the selected person.",
        },
        "merged_pull_requests": {
            "label": "Merged",
            "description": "Pull requests merged during the selected window.",
        },
        "merged_pull_request_count": {
            "label": "Merged pull requests",
            "description": "Pull requests that were merged.",
        },
        "open_pull_requests": {
            "label": "Open",
            "description": "Pull requests still open at the reporting cutoff.",
        },
        "active_authors": {
            "label": "Active authors",
            "description": "Unique authors with pull requests in the selected window.",
        },
        "active_author_count": {
            "label": "Active authors",
            "description": "Unique authors with pull requests in the selected window.",
        },
        "pull_requests_per_active_author": {
            "label": "PRs per active author",
            "description": "Pull request volume normalized by active author count.",
        },
        "merged_pull_requests_per_active_author": {
            "label": "Merged PRs / active author",
            "description": "Merged pull request volume normalized by active author count.",
        },
        "changed_lines": {
            "label": "Changed lines",
            "description": "Added plus deleted lines across pull requests.",
        },
        "changed_lines_total": {
            "label": "Changed lines",
            "description": "Added plus deleted lines across authored pull requests.",
        },
        "total_changed_lines": {
            "label": "Changed lines",
            "description": "Added plus deleted lines across pull requests.",
        },
        "changed_lines_per_active_author": {
            "label": "Lines / Active Author",
            "description": "Changed lines normalized by active author count.",
        },
        "commits_total": {
            "label": "Commits",
            "description": "Total commits across authored pull requests.",
        },
        "review_coverage_pct": {
            "label": "Review coverage",
            "description": "Share of pull requests that received at least one review.",
        },
        "review_submissions": {
            "label": "Reviews",
            "description": "Review submissions recorded in the selected window.",
        },
        "review_submissions_given": {
            "label": "Reviews given",
            "description": "Review submissions made by the selected person.",
        },
        "pull_requests_reviewed": {
            "label": "Reviewed PRs",
            "description": "Distinct pull requests reviewed by the selected person.",
        },
        "reviewed_lines": {
            "label": "Reviewed lines",
            "description": "Changed lines on pull requests reviewed by the selected person.",
        },
        "reviewed_lines_per_month": {
            "label": "Reviewed lines / month",
            "description": "Reviewed lines normalized by month in the selected window.",
        },
        "reviews_per_pr": {
            "label": "Reviews / PR",
            "description": "Average number of review submissions per pull request.",
        },
        "average_reviews_per_pr": {
            "label": "Reviews / PR",
            "description": "Average number of review submissions per pull request.",
        },
        "merge_rate_pct": {
            "label": "Merge rate",
            "description": "Share of authored pull requests that were merged.",
        },
        "median_merge_hours": {
            "label": "Median merge",
            "description": "Median time from pull request creation to merge.",
        },
        "median_time_to_merge_hours": {
            "label": "Median time to merge",
            "description": "Median time from pull request creation to merge.",
        },
        "time_to_merge_median_hours": {
            "label": "Median time to merge",
            "description": "Median time from pull request creation to merge.",
        },
        "median_first_review_hours": {
            "label": "Median first review",
            "description": "Median time from becoming reviewable to first review.",
        },
        "median_time_to_first_review_hours": {
            "label": "Median time to first review",
            "description": "Median time from becoming reviewable to first review.",
        },
        "time_to_first_review_median_hours": {
            "label": "Median time to first review",
            "description": "Median time from becoming reviewable to first review.",
        },
        "median_approval_hours": {
            "label": "Median approval time",
            "description": "Median time from becoming reviewable to final approval.",
        },
        "review_sla_24h_pct": {
            "label": "Review SLA",
            "description": "Share of pull requests with first review within 24 hours.",
        },
        "stale_open_pull_requests": {
            "label": "Open backlog risk",
            "description": "Open pull requests older than 72 hours at the reporting cutoff.",
        },
        "average_changed_lines_per_pr": {
            "label": "Avg lines / PR",
            "description": "Average changed lines per pull request.",
        },
        "repository_share": {
            "label": "PR share",
            "description": "Share of organization pull requests for this repository or author.",
        },
        "authors": {
            "label": "Authors",
            "description": "Unique pull request authors in the selected row.",
        },
    },
    ReportLocale.KO: {
        "pull_requests": {
            "label": "PR 처리량",
            "description": "선택한 보고 기간에 집계된 풀 리퀘스트 수입니다.",
        },
        "pull_request_count": {
            "label": "풀 리퀘스트",
            "description": "선택한 보고 기간에 집계된 풀 리퀘스트 수입니다.",
        },
        "authored_pull_request_count": {
            "label": "작성한 PR",
            "description": "선택한 사람이 작성한 풀 리퀘스트 수입니다.",
        },
        "merged_pull_requests": {
            "label": "머지됨",
            "description": "선택한 기간에 머지된 풀 리퀘스트 수입니다.",
        },
        "merged_pull_request_count": {
            "label": "머지된 풀 리퀘스트",
            "description": "머지된 상태의 풀 리퀘스트 수입니다.",
        },
        "open_pull_requests": {
            "label": "열림",
            "description": "보고 기준 시점에 아직 열려 있는 풀 리퀘스트 수입니다.",
        },
        "active_authors": {
            "label": "활성 작성자",
            "description": "선택한 기간에 풀 리퀘스트를 작성한 고유 작성자 수입니다.",
        },
        "active_author_count": {
            "label": "활성 작성자",
            "description": "선택한 기간에 풀 리퀘스트를 작성한 고유 작성자 수입니다.",
        },
        "pull_requests_per_active_author": {
            "label": "활성 작성자당 PR",
            "description": "PR 처리량을 활성 작성자 수로 나눈 값입니다.",
        },
        "merged_pull_requests_per_active_author": {
            "label": "활성 작성자당 머지 PR",
            "description": "머지된 PR 수를 활성 작성자 수로 나눈 값입니다.",
        },
        "changed_lines": {
            "label": "변경 라인",
            "description": "풀 리퀘스트의 추가 라인과 삭제 라인을 합산한 값입니다.",
        },
        "changed_lines_total": {
            "label": "변경 라인",
            "description": "작성한 풀 리퀘스트의 추가 라인과 삭제 라인을 합산한 값입니다.",
        },
        "total_changed_lines": {
            "label": "변경 라인",
            "description": "풀 리퀘스트의 추가 라인과 삭제 라인을 합산한 값입니다.",
        },
        "changed_lines_per_active_author": {
            "label": "활성 작성자당 라인",
            "description": "변경 라인을 활성 작성자 수로 나눈 값입니다.",
        },
        "commits_total": {
            "label": "커밋",
            "description": "작성한 풀 리퀘스트의 총 커밋 수입니다.",
        },
        "review_coverage_pct": {
            "label": "리뷰 커버리지",
            "description": "하나 이상의 리뷰를 받은 풀 리퀘스트 비율입니다.",
        },
        "review_submissions": {
            "label": "리뷰",
            "description": "선택한 기간에 제출된 리뷰 수입니다.",
        },
        "review_submissions_given": {
            "label": "제공한 리뷰",
            "description": "선택한 사람이 제출한 리뷰 수입니다.",
        },
        "pull_requests_reviewed": {
            "label": "리뷰한 PR",
            "description": "선택한 사람이 리뷰한 고유 풀 리퀘스트 수입니다.",
        },
        "reviewed_lines": {
            "label": "리뷰한 라인",
            "description": "선택한 사람이 리뷰한 풀 리퀘스트의 변경 라인 수입니다.",
        },
        "reviewed_lines_per_month": {
            "label": "월별 리뷰 라인",
            "description": "선택한 기간의 월 단위 평균 리뷰 라인 수입니다.",
        },
        "reviews_per_pr": {
            "label": "PR당 리뷰",
            "description": "풀 리퀘스트당 평균 리뷰 제출 수입니다.",
        },
        "average_reviews_per_pr": {
            "label": "PR당 리뷰",
            "description": "풀 리퀘스트당 평균 리뷰 제출 수입니다.",
        },
        "merge_rate_pct": {
            "label": "머지율",
            "description": "작성한 풀 리퀘스트 중 머지된 비율입니다.",
        },
        "median_merge_hours": {
            "label": "중앙값 머지 시간",
            "description": "풀 리퀘스트 생성부터 머지까지 걸린 시간의 중앙값입니다.",
        },
        "median_time_to_merge_hours": {
            "label": "중앙값 머지 시간",
            "description": "풀 리퀘스트 생성부터 머지까지 걸린 시간의 중앙값입니다.",
        },
        "time_to_merge_median_hours": {
            "label": "중앙값 머지 시간",
            "description": "풀 리퀘스트 생성부터 머지까지 걸린 시간의 중앙값입니다.",
        },
        "median_first_review_hours": {
            "label": "중앙값 첫 리뷰",
            "description": "리뷰 가능 상태부터 첫 리뷰까지 걸린 시간의 중앙값입니다.",
        },
        "median_time_to_first_review_hours": {
            "label": "중앙값 첫 리뷰 시간",
            "description": "리뷰 가능 상태부터 첫 리뷰까지 걸린 시간의 중앙값입니다.",
        },
        "time_to_first_review_median_hours": {
            "label": "중앙값 첫 리뷰 시간",
            "description": "리뷰 가능 상태부터 첫 리뷰까지 걸린 시간의 중앙값입니다.",
        },
        "median_approval_hours": {
            "label": "중앙값 승인 시간",
            "description": "리뷰 가능 상태부터 최종 승인까지 걸린 시간의 중앙값입니다.",
        },
        "review_sla_24h_pct": {
            "label": "리뷰 SLA",
            "description": "24시간 안에 첫 리뷰를 받은 풀 리퀘스트 비율입니다.",
        },
        "stale_open_pull_requests": {
            "label": "열린 백로그 위험",
            "description": "보고 기준 시점에 72시간 넘게 열려 있는 풀 리퀘스트 수입니다.",
        },
        "average_changed_lines_per_pr": {
            "label": "PR당 평균 라인",
            "description": "풀 리퀘스트당 평균 변경 라인 수입니다.",
        },
        "repository_share": {
            "label": "PR 비중",
            "description": "이 저장소 또는 작성자가 차지하는 조직 PR 비율입니다.",
        },
        "authors": {
            "label": "작성자",
            "description": "선택한 행에 포함된 고유 풀 리퀘스트 작성자 수입니다.",
        },
    },
}
