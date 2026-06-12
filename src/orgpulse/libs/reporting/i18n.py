from __future__ import annotations

import json
from typing import Any

from markupsafe import Markup, escape

from orgpulse.common.config import get_settings
from orgpulse.common.models import ReportLocale


def resolve_report_locale(locale: ReportLocale | str | None = None) -> ReportLocale:
    """Resolve an explicit or environment-backed report locale."""

    if locale is None:
        return ReportLocale(get_settings().locale)
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
        f'title="{escape(description)}" aria-label="{escape(description)}" '
        f'data-tooltip="{escape(description)}">?</span>'
        "</span>"
    )


def period_state_text(
    locale: ReportLocale | str | None,
    row: Any,
) -> str:
    """Return localized display text for a period state row."""

    resolved_locale = resolve_report_locale(locale)
    label = _row_value(row, "state_label") or _row_value(row, "label")
    if resolved_locale is ReportLocale.EN and label:
        return str(label)
    if _row_value(row, "open_week"):
        return report_text(resolved_locale, "common.open_week")
    if _row_value(row, "open_month"):
        return report_text(resolved_locale, "common.open_month")
    if _row_value(row, "is_partial"):
        return report_text(resolved_locale, "common.partial_period")
    label_key = _PERIOD_LABEL_KEYS.get(str(label or "").lower())
    if label_key is not None:
        return report_text(resolved_locale, label_key)
    if _row_value(row, "is_open") or _row_value(row, "status") == "open":
        return report_text(resolved_locale, "common.open_period")
    return report_text(resolved_locale, "common.closed_period")


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


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
        return (
            f"{minutes}분" if resolved_locale is ReportLocale.KO else f"{minutes} min"
        )
    if hours >= 24:
        days = format_number(hours / 24, resolved_locale)
        return f"{days}일" if resolved_locale is ReportLocale.KO else f"{days} d"
    formatted_hours = format_number(hours, resolved_locale)
    return (
        f"{formatted_hours}시간"
        if resolved_locale is ReportLocale.KO
        else f"{formatted_hours} h"
    )


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
        "analysis.same_period_created": "Same-period created",
        "analysis.older_pr_ratio": "Older PR ratio",
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
        "common.author": "Author",
        "common.source_grain": "Source grain",
        "common.distribution_cutoff": "Distribution cutoff",
        "common.percentile": "percentile",
        "common.anchor": "Anchor",
        "common.generated": "Generated",
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
        "common.closed_week": "closed week",
        "common.closed_month": "closed month",
        "common.partial_period": "partial period",
        "common.closed_period": "closed period",
        "common.open_period": "open period",
        "common.observed_through": "observed through",
        "common.to": "to",
        "common.report_sections": "Report sections",
        "common.report_cadence": "Report cadence",
        "common.show_more_rows": "Show more rows",
        "common.show_more_repositories": "Show more repositories",
        "common.show_more_people": "Show more people",
        "common.collapse": "Collapse",
        "common.rows_hidden_until_expanded": "older rows are hidden until expanded.",
        "metric.merged_open": "merged / open",
        "metric.merged": "merged",
        "metric.open": "open",
        "metric.total_review_submissions": "total review submissions",
        "metric.review_submissions_received": "review submissions received",
        "metric.review_submissions": "review submissions",
        "metric.reviews": "reviews",
        "metric.reviews_per_pr": "reviews per PR",
        "metric.prs": "PRs",
        "metric.prs_per_month": "PRs per month",
        "metric.lines_per_month": "lines / month",
        "metric.lines_per_pr": "lines per PR",
        "metric.changed_lines": "changed lines",
        "metric.reviewed_lines": "reviewed lines",
        "metric.commits_across_authored_prs": "commits across authored PRs",
        "metric.median_merge_time": "median merge time",
        "metric.median_approval_time": "median time to final approval on authored PRs",
        "metric.median_first_review_window": "median first review across the window.",
        "metric.within_24h": "within 24h",
        "metric.stale_open_prs": "stale open PRs",
        "metric.approvals": "approvals",
        "metric.changes_requested": "changes requested",
        "metric.comments": "comments",
        "metric.reviewers": "reviewers",
        "metric.average_active_authors_per_month": "avg active authors / month",
        "metric.normalized_changed_lines_per_active_author": "normalized changed lines per active author",
        "metric.reviewers_in_window": "reviewers in window",
        "metric.top3_repository_share": "of PRs came from the top 3 repositories",
        "metric.authored_prs": "authored PRs",
        "metric.reviews_given": "reviews given",
        "metric.ready_to_approval": "from ready/request to final approval",
        "metric.creation_to_merge": "from PR creation to merge",
        "dashboard.trend_surface_copy": "Primary trend surface for throughput, code volume, and approval speed. Dense weekly/monthly tables live in the reference section below.",
        "dashboard.interpretation": "Interpretation",
        "dashboard.interpretation_copy": "Use the chart for movement. Full weekly and monthly tables are intentionally collapsed into reference to keep the main flow shorter.",
        "dashboard.normalized_lens": "Normalized lens",
        "dashboard.normalized_lens_copy": "The team averaged",
        "dashboard.normalized_lens_suffix": "which normalizes to",
        "dashboard.backlog": "Backlog",
        "dashboard.backlog_copy": "open PRs are older than 72 hours at window end.",
        "dashboard.people_copy": "Switch metrics to reorder people by that signal. The selected person remains active across ranking changes.",
        "dashboard.people_ranking_metric": "People ranking metric",
        "dashboard.people_top_note": "Top 10 people shown first. Expand the ranking to inspect the long tail.",
        "dashboard.selected_profile": "Selected profile",
        "dashboard.person_detail": "Person detail",
        "dashboard.author_reviewer_signals": "Author + reviewer signals",
        "dashboard.author_chart": "Author chart",
        "dashboard.selected_person": "Selected person",
        "dashboard.repository_leaderboard": "Repository leaderboard",
        "dashboard.repository_concentration": "Where throughput concentrated",
        "dashboard.repository_copy": "Team people rankings live in the command center above. Repository flow stays here with the top 5 visible by default and long tails loaded in batches of 5.",
        "dashboard.top_repositories_cover": "Top 5 repositories cover",
        "dashboard.top_repositories_suffix": "of org PR throughput.",
        "dashboard.latency_by_size": "Review latency by PR size",
        "dashboard.size_flow": "How bigger changes slowed flow",
        "dashboard.latency_quality": "Latency and quality",
        "dashboard.review_depth": "Review depth",
        "dashboard.prs_first_review_day": "PRs that received a first review within one day of becoming reviewable.",
        "dashboard.reviewed_prs": "of PRs received at least one review.",
        "dashboard.open_prs_older_72h": "Open pull requests older than 72 hours at the end of the reporting window.",
        "dashboard.dense_tables": "Dense tables and methodology, on demand",
        "dashboard.reference_copy": "These sections stay collapsed by default so large org windows do not force the main dashboard into a long scroll-only document.",
        "dashboard.trend_tables": "Trend tables",
        "dashboard.author_ledger": "Author ledger",
        "dashboard.author_ledger_copy": "Dense comparison table. Top 5 already cover",
        "dashboard.authored_pr_flow_suffix": "of authored PR flow in the main explorer.",
        "dashboard.org_pr_flow": "of org PR flow",
        "dashboard.median_lines": "median lines",
        "person.authored_anchor_prefix": "Authored pull request metrics use",
        "person.chart_copy": "Metric and grain tabs use the same local source data as the extract table.",
        "person.cadence_copy": "Both cadences are available in HTML so review and author movement can be compared without rerunning the extract.",
        "person.repository_copy": "Top repositories are ranked by authored PR count, then review activity and code volume.",
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
        "analysis.same_period_created": "같은 기간에 생성됨",
        "analysis.older_pr_ratio": "이전 PR 비율",
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
        "common.author": "작성자",
        "common.source_grain": "소스 단위",
        "common.distribution_cutoff": "분포 절단값",
        "common.percentile": "퍼센타일",
        "common.anchor": "기준",
        "common.generated": "생성 시각",
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
        "common.closed_week": "닫힌 주",
        "common.closed_month": "닫힌 월",
        "common.partial_period": "부분 기간",
        "common.closed_period": "닫힌 기간",
        "common.open_period": "열린 기간",
        "common.observed_through": "관측일",
        "common.to": "부터",
        "common.report_sections": "보고서 섹션",
        "common.report_cadence": "보고서 주기",
        "common.show_more_rows": "행 더 보기",
        "common.show_more_repositories": "저장소 더 보기",
        "common.show_more_people": "사람 더 보기",
        "common.collapse": "접기",
        "common.rows_hidden_until_expanded": "이전 행은 펼칠 때까지 숨겨집니다.",
        "metric.merged_open": "머지됨 / 열림",
        "metric.merged": "머지됨",
        "metric.open": "열림",
        "metric.total_review_submissions": "전체 리뷰 제출",
        "metric.review_submissions_received": "받은 리뷰 제출",
        "metric.review_submissions": "리뷰 제출",
        "metric.reviews": "리뷰",
        "metric.reviews_per_pr": "PR당 리뷰",
        "metric.prs": "PR",
        "metric.prs_per_month": "월별 PR",
        "metric.lines_per_month": "월별 라인",
        "metric.lines_per_pr": "PR당 라인",
        "metric.changed_lines": "변경 라인",
        "metric.reviewed_lines": "리뷰한 라인",
        "metric.commits_across_authored_prs": "작성한 PR의 커밋",
        "metric.median_merge_time": "중앙값 머지 시간",
        "metric.median_approval_time": "작성한 PR의 최종 승인까지 중앙값 시간",
        "metric.median_first_review_window": "기간 내 중앙값 첫 리뷰입니다.",
        "metric.within_24h": "24시간 이내",
        "metric.stale_open_prs": "오래 열린 PR",
        "metric.approvals": "승인",
        "metric.changes_requested": "변경 요청",
        "metric.comments": "코멘트",
        "metric.reviewers": "리뷰어",
        "metric.average_active_authors_per_month": "월평균 활성 작성자",
        "metric.normalized_changed_lines_per_active_author": "활성 작성자당 정규화된 변경 라인",
        "metric.reviewers_in_window": "기간 내 리뷰어",
        "metric.top3_repository_share": "상위 3개 저장소에서 나온 PR 비율",
        "metric.authored_prs": "작성한 PR",
        "metric.reviews_given": "제공한 리뷰",
        "metric.ready_to_approval": "리뷰 요청/준비부터 최종 승인까지",
        "metric.creation_to_merge": "PR 생성부터 머지까지",
        "dashboard.trend_surface_copy": "처리량, 코드 규모, 승인 속도를 보는 기본 추세 화면입니다. 자세한 주간/월간 표는 아래 참고 섹션에 접혀 있습니다.",
        "dashboard.interpretation": "해석",
        "dashboard.interpretation_copy": "차트는 움직임을 보는 데 사용합니다. 전체 주간 및 월간 표는 주요 흐름을 짧게 유지하기 위해 참고 섹션에 접혀 있습니다.",
        "dashboard.normalized_lens": "정규화 관점",
        "dashboard.normalized_lens_copy": "팀은 월평균",
        "dashboard.normalized_lens_suffix": "기준으로 정규화하면",
        "dashboard.backlog": "백로그",
        "dashboard.backlog_copy": "개의 열린 PR이 기간 종료 시점에 72시간을 넘었습니다.",
        "dashboard.people_copy": "지표를 전환해 해당 신호 기준으로 사람 순위를 다시 정렬합니다. 선택한 사람은 순위 변경 후에도 유지됩니다.",
        "dashboard.people_ranking_metric": "사람 순위 지표",
        "dashboard.people_top_note": "상위 10명을 먼저 표시합니다. 순위를 펼치면 나머지도 확인할 수 있습니다.",
        "dashboard.selected_profile": "선택한 프로필",
        "dashboard.person_detail": "개인 상세",
        "dashboard.author_reviewer_signals": "작성자 및 리뷰어 신호",
        "dashboard.author_chart": "작성자 차트",
        "dashboard.selected_person": "선택한 사람",
        "dashboard.repository_leaderboard": "저장소 리더보드",
        "dashboard.repository_concentration": "처리량이 집중된 위치",
        "dashboard.repository_copy": "사람 순위는 위 명령 센터에 두고, 저장소 흐름은 여기에서 상위 5개를 기본 표시하며 긴 목록은 5개씩 더 불러옵니다.",
        "dashboard.top_repositories_cover": "상위 5개 저장소가 조직 PR 처리량의",
        "dashboard.top_repositories_suffix": "를 차지합니다.",
        "dashboard.latency_by_size": "PR 크기별 리뷰 지연",
        "dashboard.size_flow": "큰 변경이 흐름을 늦춘 방식",
        "dashboard.latency_quality": "지연 및 품질",
        "dashboard.review_depth": "리뷰 깊이",
        "dashboard.prs_first_review_day": "리뷰 가능 상태가 된 뒤 하루 안에 첫 리뷰를 받은 PR입니다.",
        "dashboard.reviewed_prs": "의 PR이 하나 이상의 리뷰를 받았습니다.",
        "dashboard.open_prs_older_72h": "보고 기간 종료 시점에 72시간 넘게 열려 있던 풀 리퀘스트입니다.",
        "dashboard.dense_tables": "상세 표와 방법론",
        "dashboard.reference_copy": "큰 조직 기간에서도 주요 대시보드가 길어지지 않도록 이 섹션들은 기본적으로 접어 둡니다.",
        "dashboard.trend_tables": "추세 표",
        "dashboard.author_ledger": "작성자 원장",
        "dashboard.author_ledger_copy": "상세 비교 표입니다. 상위 5명이 주요 탐색기에서 작성 PR 흐름의",
        "dashboard.authored_pr_flow_suffix": "를 이미 차지합니다.",
        "dashboard.org_pr_flow": "조직 PR 흐름 비율",
        "dashboard.median_lines": "중앙값 라인",
        "person.authored_anchor_prefix": "작성한 풀 리퀘스트 지표 기준:",
        "person.chart_copy": "지표 및 단위 탭은 추출 표와 같은 로컬 소스 데이터를 사용합니다.",
        "person.cadence_copy": "HTML에서 두 주기를 모두 제공하므로 추출을 다시 실행하지 않고 리뷰와 작성자 움직임을 비교할 수 있습니다.",
        "person.repository_copy": "상위 저장소는 작성한 PR 수, 리뷰 활동, 코드 규모 순으로 정렬됩니다.",
    },
}


_PERIOD_LABEL_KEYS = {
    "open week": "common.open_week",
    "open month": "common.open_month",
    "open period": "common.open_period",
    "partial period": "common.partial_period",
    "closed week": "common.closed_week",
    "closed month": "common.closed_month",
    "closed window": "common.closed_window",
    "closed period": "common.closed_period",
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
