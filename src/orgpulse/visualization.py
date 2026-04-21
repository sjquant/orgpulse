from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from statistics import fmean, median
from typing import cast

from orgpulse.models import (
    OrganizationMetricCollection,
    PullRequestMetricCollection,
    PullRequestMetricRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    RepositoryMetricCollection,
    RunConfig,
)

_PERIOD_METRIC_DEFINITIONS = (
    {"key": "pull_request_count", "label": "Pull requests", "format": "int"},
    {
        "key": "merged_pull_request_count",
        "label": "Merged pull requests",
        "format": "int",
    },
    {"key": "active_author_count", "label": "Active authors", "format": "int"},
    {
        "key": "merged_pull_requests_per_active_author",
        "label": "Merged PRs / active author",
        "format": "float",
    },
    {
        "key": "time_to_merge_median_hours",
        "label": "Median time to merge (hours)",
        "format": "float",
    },
    {
        "key": "time_to_first_review_median_hours",
        "label": "Median time to first review (hours)",
        "format": "float",
    },
)

_ENTITY_METRIC_DEFINITIONS = (
    {"key": "pull_request_count", "label": "Pull requests", "format": "int"},
    {
        "key": "merged_pull_request_count",
        "label": "Merged pull requests",
        "format": "int",
    },
    {
        "key": "total_changed_lines",
        "label": "Changed lines",
        "format": "int",
    },
    {
        "key": "median_time_to_merge_hours",
        "label": "Median time to merge (hours)",
        "format": "float",
    },
    {
        "key": "median_time_to_first_review_hours",
        "label": "Median time to first review (hours)",
        "format": "float",
    },
)


def build_analysis_report_payload(
    *,
    target_org: str,
    grain: str,
    time_anchor: str,
    initial_view: str,
    default_top_n: int,
    since: date | None,
    until: date | None,
    matched_pull_request_count: int,
    filtered_metrics: tuple[PullRequestMetricRecord, ...],
    raw_snapshot: RawSnapshotWriteResult,
) -> dict[str, object]:
    raw_periods = _load_filtered_raw_periods(raw_snapshot, filtered_metrics)
    period_catalog = _period_catalog(raw_snapshot, filtered_metrics)
    metrics_by_period = _metrics_by_period(filtered_metrics)
    period_reports = []
    for period_descriptor in period_catalog:
        period_key = cast(str, period_descriptor["key"])
        period_reports.append(
            _build_period_report(
                period_descriptor=period_descriptor,
                period_metrics=metrics_by_period.get(period_key, ()),
                raw_period=raw_periods.get(period_key),
            )
        )
    repository_view = _build_entity_view(
        period_catalog=period_catalog,
        filtered_metrics=filtered_metrics,
        identity_builder=lambda metric: (
            metric.repository_full_name,
            metric.repository_full_name,
        ),
    )
    author_view = _build_entity_view(
        period_catalog=period_catalog,
        filtered_metrics=filtered_metrics,
        identity_builder=lambda metric: _author_identity(metric.author_login),
    )
    return {
        "target_org": target_org,
        "grain": grain,
        "time_anchor": time_anchor,
        "initial_view": initial_view,
        "default_top_n": default_top_n,
        "since": None if since is None else since.isoformat(),
        "until": None if until is None else until.isoformat(),
        "matched_pull_request_count": matched_pull_request_count,
        "default_period_key": (
            period_reports[-1]["key"] if period_reports else ""
        ),
        "periods": period_reports,
        "views": {
            "period": {
                "default_metric": "pull_request_count",
                "metrics": list(_PERIOD_METRIC_DEFINITIONS),
                "periods": [
                    {
                        "key": period_report["key"],
                        "label": period_report["label"],
                        "start_date": period_report["start_date"],
                        "end_date": period_report["end_date"],
                        "closed": period_report["closed"],
                        "values": period_report["values"],
                    }
                    for period_report in period_reports
                ],
            },
            "repository": repository_view,
            "author": author_view,
        },
    }


def build_organization_report_payload(
    *,
    config: RunConfig,
    raw_snapshot: RawSnapshotWriteResult,
    pull_request_metrics: PullRequestMetricCollection,
    repository_metrics: RepositoryMetricCollection,
    org_metrics: OrganizationMetricCollection,
) -> dict[str, object]:
    """Build the legacy organization report payload from full metric collections."""
    return build_analysis_report_payload(
        target_org=org_metrics.target_org,
        grain=config.period.value,
        time_anchor=config.time_anchor.value,
        initial_view="period",
        default_top_n=8,
        since=None,
        until=None,
        matched_pull_request_count=sum(
            len(period.pull_request_metrics)
            for period in pull_request_metrics.periods
        ),
        filtered_metrics=tuple(
            metric
            for period in pull_request_metrics.periods
            for metric in period.pull_request_metrics
        ),
        raw_snapshot=raw_snapshot,
    )


def _load_filtered_raw_periods(
    raw_snapshot: RawSnapshotWriteResult,
    filtered_metrics: tuple[PullRequestMetricRecord, ...],
) -> dict[str, dict[str, tuple[dict[str, str], ...]]]:
    metric_keys = {
        period_key: {
            (metric.repository_full_name, str(metric.pull_request_number))
            for metric in period_metrics
        }
        for period_key, period_metrics in _metrics_by_period(filtered_metrics).items()
    }
    return {
        period.key: _load_filtered_raw_period(
            period,
            pull_request_keys=metric_keys.get(period.key, set()),
        )
        for period in raw_snapshot.periods
        if period.key in metric_keys
    }


def _load_filtered_raw_period(
    period: RawSnapshotPeriod,
    *,
    pull_request_keys: set[tuple[str, str]],
) -> dict[str, tuple[dict[str, str], ...]]:
    pull_request_rows = tuple(
        row
        for row in _read_csv_rows(period.pull_requests_path)
        if _raw_pull_request_key(row) in pull_request_keys
    )
    timeline_rows = tuple(
        row
        for row in _read_csv_rows(period.timeline_events_path)
        if _raw_pull_request_key(row) in pull_request_keys
    )
    return {
        "pull_requests": pull_request_rows,
        "timeline_events": timeline_rows,
    }


def _read_csv_rows(path: Path) -> tuple[dict[str, str], ...]:
    if not path.exists():
        return ()
    with path.open(encoding="utf-8", newline="") as handle:
        return tuple(csv.DictReader(handle))


def _raw_pull_request_key(row: dict[str, str]) -> tuple[str, str]:
    return row["repository_full_name"], row["pull_request_number"]


def _period_catalog(
    raw_snapshot: RawSnapshotWriteResult,
    filtered_metrics: tuple[PullRequestMetricRecord, ...],
) -> list[dict[str, object]]:
    included_periods = set(metric.period_key for metric in filtered_metrics)
    return [
        {
            "key": period.key,
            "label": period.key,
            "start_date": period.start_date.isoformat(),
            "end_date": period.end_date.isoformat(),
            "closed": getattr(period, "closed", False),
        }
        for period in raw_snapshot.periods
        if period.key in included_periods
    ]


def _metrics_by_period(
    filtered_metrics: tuple[PullRequestMetricRecord, ...],
) -> dict[str, tuple[PullRequestMetricRecord, ...]]:
    metrics_by_period: dict[str, list[PullRequestMetricRecord]] = defaultdict(list)
    for metric in filtered_metrics:
        metrics_by_period[metric.period_key].append(metric)
    return {
        period_key: tuple(period_metrics)
        for period_key, period_metrics in metrics_by_period.items()
    }


def _build_period_report(
    *,
    period_descriptor: dict[str, object],
    period_metrics: tuple[PullRequestMetricRecord, ...],
    raw_period: dict[str, tuple[dict[str, str], ...]] | None,
) -> dict[str, object]:
    summary = _aggregate_metric_summary(period_metrics)
    return {
        **period_descriptor,
        "summary": summary,
        "values": _period_chart_values(summary),
        "diagnostics": _build_period_diagnostics(
            period_descriptor=period_descriptor,
            period_metrics=period_metrics,
            raw_period=raw_period,
        ),
    }


def _aggregate_metric_summary(
    metrics: Sequence[PullRequestMetricRecord],
) -> dict[str, int | float | None]:
    merged_pull_request_count = sum(1 for metric in metrics if metric.merged)
    active_author_count = len(
        {
            metric.author_login.lower()
            for metric in metrics
            if metric.author_login is not None
        }
    )
    time_to_merge_values = [
        metric.time_to_merge_seconds
        for metric in metrics
        if metric.time_to_merge_seconds is not None
    ]
    time_to_first_review_values = [
        metric.time_to_first_review_seconds
        for metric in metrics
        if metric.time_to_first_review_seconds is not None
    ]
    return {
        "pull_request_count": len(metrics),
        "merged_pull_request_count": merged_pull_request_count,
        "active_author_count": active_author_count,
        "merged_pull_requests_per_active_author": _safe_ratio(
            merged_pull_request_count,
            active_author_count,
        ),
        "time_to_merge_count": len(time_to_merge_values),
        "time_to_merge_average_hours": _hours_from_seconds(
            _average(time_to_merge_values)
        ),
        "time_to_merge_median_hours": _hours_from_seconds(
            _median(time_to_merge_values)
        ),
        "time_to_first_review_count": len(time_to_first_review_values),
        "time_to_first_review_average_hours": _hours_from_seconds(
            _average(time_to_first_review_values)
        ),
        "time_to_first_review_median_hours": _hours_from_seconds(
            _median(time_to_first_review_values)
        ),
        "total_changed_lines": sum(metric.changed_lines for metric in metrics),
    }


def _period_chart_values(
    summary: dict[str, int | float | None],
) -> dict[str, int | float | None]:
    return {
        "pull_request_count": summary["pull_request_count"],
        "merged_pull_request_count": summary["merged_pull_request_count"],
        "active_author_count": summary["active_author_count"],
        "merged_pull_requests_per_active_author": summary[
            "merged_pull_requests_per_active_author"
        ],
        "time_to_merge_median_hours": summary["time_to_merge_median_hours"],
        "time_to_first_review_median_hours": summary[
            "time_to_first_review_median_hours"
        ],
    }


def _build_period_diagnostics(
    *,
    period_descriptor: dict[str, object],
    period_metrics: tuple[PullRequestMetricRecord, ...],
    raw_period: dict[str, tuple[dict[str, str], ...]] | None,
) -> dict[str, object]:
    start_date = date.fromisoformat(cast(str, period_descriptor["start_date"]))
    end_date = date.fromisoformat(cast(str, period_descriptor["end_date"]))
    same_period_created_count = sum(
        1
        for metric in period_metrics
        if start_date <= metric.created_at.date() <= end_date
    )
    older_pull_request_count = sum(
        1 for metric in period_metrics if metric.created_at.date() < start_date
    )
    repositories = Counter(metric.repository_full_name for metric in period_metrics)
    total_count = len(period_metrics)
    top_repositories = [
        {
            "label": repository_name,
            "pull_request_count": count,
            "share": _safe_ratio(count, total_count),
        }
        for repository_name, count in sorted(
            repositories.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )[:5]
    ]
    return {
        "same_period_created_count": same_period_created_count,
        "same_period_created_ratio": _safe_ratio(
            same_period_created_count,
            total_count,
        ),
        "older_pull_request_count": older_pull_request_count,
        "older_pull_request_ratio": _safe_ratio(
            older_pull_request_count,
            total_count,
        ),
        "top_contributing_repositories": top_repositories,
        "top_updated_dates": _top_updated_dates(raw_period),
        "timeline_event_breakdown": _timeline_event_breakdown(raw_period),
    }


def _top_updated_dates(
    raw_period: dict[str, tuple[dict[str, str], ...]] | None,
) -> list[dict[str, object]]:
    if raw_period is None:
        return []
    updated_dates = Counter(
        datetime.fromisoformat(row["updated_at"]).date().isoformat()
        for row in raw_period["pull_requests"]
    )
    total_updates = sum(updated_dates.values())
    return [
        {
            "label": updated_date,
            "count": count,
            "share": _safe_ratio(count, total_updates),
        }
        for updated_date, count in sorted(
            updated_dates.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )[:5]
    ]


def _timeline_event_breakdown(
    raw_period: dict[str, tuple[dict[str, str], ...]] | None,
) -> list[dict[str, object]]:
    if raw_period is None:
        return []
    event_counts = Counter(row["event"] for row in raw_period["timeline_events"])
    pull_request_counts: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in raw_period["timeline_events"]:
        pull_request_counts[row["event"]].add(_raw_pull_request_key(row))
    total_events = sum(event_counts.values())
    return [
        {
            "label": event_name,
            "event_count": count,
            "pull_request_count": len(pull_request_counts[event_name]),
            "share": _safe_ratio(count, total_events),
        }
        for event_name, count in sorted(
            event_counts.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )
    ]


def _build_entity_view(
    *,
    period_catalog: Sequence[dict[str, object]],
    filtered_metrics: tuple[PullRequestMetricRecord, ...],
    identity_builder,
) -> dict[str, object]:
    entities_by_period: dict[str, dict[str, list[PullRequestMetricRecord]]] = defaultdict(
        lambda: defaultdict(list)
    )
    entity_labels: dict[str, str] = {}
    for metric in filtered_metrics:
        entity_key, entity_label = identity_builder(metric)
        entity_labels.setdefault(entity_key, entity_label)
        entities_by_period[metric.period_key][entity_key].append(metric)
    return {
        "default_metric": "pull_request_count",
        "metrics": list(_ENTITY_METRIC_DEFINITIONS),
        "periods": list(period_catalog),
        "entities": [
            _build_entity_report(
                entity_key=entity_key,
                entity_label=entity_labels[entity_key],
                period_catalog=period_catalog,
                entities_by_period=entities_by_period,
            )
            for entity_key in sorted(entity_labels)
        ],
    }


def _build_entity_report(
    *,
    entity_key: str,
    entity_label: str,
    period_catalog: Sequence[dict[str, object]],
    entities_by_period: dict[str, dict[str, list[PullRequestMetricRecord]]],
) -> dict[str, object]:
    period_values = [
        {
            **period_descriptor,
            "values": _entity_period_values(
                entities_by_period.get(cast(str, period_descriptor["key"]), {}).get(
                    entity_key,
                    [],
                )
            ),
        }
        for period_descriptor in period_catalog
    ]
    totals = {
        definition["key"]: sum(
            value
            for value in (
                cast(
                    int | float | None,
                    cast(dict[str, object], period_value["values"]).get(
                        definition["key"]
                    ),
                )
                for period_value in period_values
            )
            if value is not None
        )
        for definition in _ENTITY_METRIC_DEFINITIONS
    }
    return {
        "key": entity_key,
        "label": entity_label,
        "period_values": period_values,
        "totals": totals,
    }


def _entity_period_values(
    metrics: Sequence[PullRequestMetricRecord],
) -> dict[str, int | float | None]:
    merge_values = [
        metric.time_to_merge_seconds
        for metric in metrics
        if metric.time_to_merge_seconds is not None
    ]
    first_review_values = [
        metric.time_to_first_review_seconds
        for metric in metrics
        if metric.time_to_first_review_seconds is not None
    ]
    return {
        "pull_request_count": len(metrics),
        "merged_pull_request_count": sum(1 for metric in metrics if metric.merged),
        "total_changed_lines": sum(metric.changed_lines for metric in metrics),
        "median_time_to_merge_hours": _hours_from_seconds(_median(merge_values)),
        "median_time_to_first_review_hours": _hours_from_seconds(
            _median(first_review_values)
        ),
    }


def _author_identity(author_login: str | None) -> tuple[str, str]:
    if author_login is None or not author_login.strip():
        return "unknown", "Unknown author"
    return author_login.lower(), author_login


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _average(values: Sequence[int]) -> float | None:
    if not values:
        return None
    return float(fmean(values))


def _median(values: Sequence[int]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _hours_from_seconds(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 3600.0


def render_analysis_report_html(
    report_payload: dict[str, object],
) -> str:
    serialized_payload = json.dumps(report_payload, sort_keys=True).replace(
        "</script>",
        "<\\/script>",
    )
    return "\n".join(
        (
            "<!DOCTYPE html>",
            '<html lang="en">',
            "<head>",
            '  <meta charset="utf-8">',
            '  <meta name="viewport" content="width=device-width, initial-scale=1">',
            "  <title>orgpulse analysis report</title>",
            f"  <style>{_report_styles()}</style>",
            "</head>",
            "<body>",
            '  <div class="shell">',
            '    <header class="hero">',
            '      <p class="eyebrow">Diagnostics and visualization</p>',
            '      <h1 id="report-title">orgpulse analysis report</h1>',
            '      <p id="report-subtitle" class="subtitle"></p>',
            "    </header>",
            '    <section class="panel controls-panel">',
            '      <div class="controls-grid">',
            '        <label class="control" for="view-select">',
            "          <span>View</span>",
            '          <select id="view-select" data-control="view"></select>',
            "        </label>",
            '        <label class="control" for="metric-select">',
            "          <span>Metric</span>",
            '          <select id="metric-select" data-control="metric"></select>',
            "        </label>",
            '        <label class="control" for="period-select">',
            "          <span>Period</span>",
            '          <select id="period-select" data-control="period"></select>',
            "        </label>",
            '        <label class="control" for="top-n-select">',
            "          <span>Top series</span>",
            '          <select id="top-n-select" data-control="top-n">',
            '            <option value="5">Top 5</option>',
            '            <option value="8">Top 8</option>',
            '            <option value="12">Top 12</option>',
            "          </select>",
            "        </label>",
            '        <label class="control" for="search-input">',
            "          <span>Series filter</span>",
            '          <input id="search-input" data-control="search" type="search" placeholder="repo or author name">',
            "        </label>",
            '        <label class="control" for="focus-series-select">',
            "          <span>Single-series focus</span>",
            '          <select id="focus-series-select" data-control="focus-series"></select>',
            "        </label>",
            "      </div>",
            "    </section>",
            '    <section class="summary-grid" id="summary-cards"></section>',
            '    <section class="panel chart-panel">',
            '      <div class="panel-header">',
            "        <div>",
            '          <p class="panel-kicker">Trend view</p>',
            '          <h2 id="chart-title">Trend</h2>',
            "        </div>",
            '        <p id="chart-caption" class="caption"></p>',
            "      </div>",
            '      <div id="chart-root" class="chart-root"></div>',
            '      <div id="chart-legend" class="legend"></div>',
            "    </section>",
            '    <div class="two-column-layout">',
            '      <section class="panel">',
            '        <div class="panel-header">',
            "          <div>",
            '            <p class="panel-kicker">Selected period</p>',
            '            <h2 id="diagnostics-title">Spike diagnostics</h2>',
            "          </div>",
            "        </div>",
            '        <div id="diagnostics-root"></div>',
            "      </section>",
            '      <section class="panel">',
            '        <div class="panel-header">',
            "          <div>",
            '            <p class="panel-kicker">Ranking</p>',
            '            <h2 id="table-title">Current ranking</h2>',
            "          </div>",
            "        </div>",
            '        <div id="table-root"></div>',
            "      </section>",
            "    </div>",
            "  </div>",
            f'  <script id="report-data" type="application/json">{serialized_payload}</script>',
            f"  <script>{_report_script()}</script>",
            "</body>",
            "</html>",
        )
    )


def render_organization_report_html(
    report_payload: dict[str, object],
) -> str:
    """Render the legacy organization report HTML wrapper."""
    return render_analysis_report_html(report_payload)


def _report_styles() -> str:
    return """
      :root {
        color-scheme: light;
        --bg: #f4efe6;
        --panel: rgba(255, 252, 247, 0.9);
        --ink: #1f2b2a;
        --muted: #576764;
        --line: rgba(31, 43, 42, 0.12);
        --strong: #0e5c52;
        --strong-soft: rgba(14, 92, 82, 0.12);
        --warn: #8f5f15;
        --shadow: 0 18px 40px rgba(31, 43, 42, 0.08);
      }

      * { box-sizing: border-box; }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: "Avenir Next", "IBM Plex Sans", "Segoe UI Variable", sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(216, 234, 223, 0.9), transparent 38%),
          radial-gradient(circle at top right, rgba(244, 192, 149, 0.28), transparent 24%),
          linear-gradient(180deg, #f8f4eb 0%, var(--bg) 100%);
      }

      .shell {
        max-width: 1320px;
        margin: 0 auto;
        padding: 40px 24px 48px;
      }

      .hero { margin-bottom: 22px; }

      .eyebrow,
      .panel-kicker {
        margin: 0 0 8px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        font-size: 0.72rem;
        color: var(--strong);
        font-weight: 700;
      }

      h1, h2, h3, p { margin-top: 0; }

      h1 {
        margin-bottom: 10px;
        font-size: clamp(2rem, 3vw, 3.1rem);
        line-height: 1.04;
      }

      h2 { margin-bottom: 0; font-size: 1.2rem; }

      .subtitle,
      .caption { color: var(--muted); }

      .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        box-shadow: var(--shadow);
        backdrop-filter: blur(18px);
      }

      .controls-panel,
      .chart-panel,
      .two-column-layout > .panel { padding: 18px; }

      .controls-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 14px;
      }

      .control {
        display: flex;
        flex-direction: column;
        gap: 8px;
        font-size: 0.88rem;
        color: var(--muted);
      }

      .control span { font-weight: 700; color: var(--ink); }

      .control select,
      .control input {
        width: 100%;
        padding: 11px 12px;
        border-radius: 14px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.9);
        color: var(--ink);
        font: inherit;
      }

      .summary-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
        margin: 18px 0;
      }

      .summary-card {
        padding: 18px;
        border-radius: 22px;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.94), rgba(245, 251, 249, 0.94));
        border: 1px solid var(--line);
        box-shadow: var(--shadow);
      }

      .summary-card h3 {
        margin-bottom: 10px;
        font-size: 0.86rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .summary-value {
        font-size: 1.9rem;
        font-weight: 800;
        line-height: 1;
      }

      .summary-meta { margin-top: 8px; font-size: 0.9rem; color: var(--muted); }

      .panel-header {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: flex-end;
        margin-bottom: 14px;
      }

      .chart-root { min-height: 330px; }

      .legend {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 12px;
      }

      .legend-chip {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        border-radius: 999px;
        padding: 8px 12px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.84);
        color: var(--ink);
        font-size: 0.84rem;
      }

      button.legend-chip { cursor: pointer; }

      .legend-swatch {
        width: 10px;
        height: 10px;
        border-radius: 999px;
      }

      .two-column-layout {
        display: grid;
        grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr);
        gap: 18px;
        margin-top: 18px;
      }

      .diagnostic-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
        margin-bottom: 18px;
      }

      .diagnostic-card {
        padding: 14px;
        border-radius: 18px;
        background: var(--strong-soft);
        border: 1px solid rgba(14, 92, 82, 0.16);
      }

      .diagnostic-card.warn {
        background: rgba(143, 95, 21, 0.09);
        border-color: rgba(143, 95, 21, 0.16);
      }

      .diagnostic-card h3 {
        margin-bottom: 8px;
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .diagnostic-value { font-size: 1.5rem; font-weight: 800; }

      .mini-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.92rem;
      }

      .mini-table th,
      .mini-table td {
        padding: 10px 8px;
        border-bottom: 1px solid var(--line);
        text-align: left;
      }

      .mini-table th {
        color: var(--muted);
        font-size: 0.78rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .table-stack { display: grid; gap: 18px; }

      .section-title {
        margin: 0 0 8px;
        font-size: 0.92rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .empty-state {
        padding: 28px;
        border-radius: 18px;
        border: 1px dashed var(--line);
        color: var(--muted);
        text-align: center;
      }

      .axis-label {
        font-size: 0.75rem;
        fill: var(--muted);
        font-family: "JetBrains Mono", "SFMono-Regular", monospace;
      }

      @media (max-width: 920px) {
        .two-column-layout { grid-template-columns: 1fr; }
      }
    """
def _report_script() -> str:
    return """
      const report = JSON.parse(document.getElementById("report-data").textContent);
      const viewCatalog = {
        period: { label: "Period", data: report.views.period },
        repository: { label: "Repository", data: report.views.repository },
        author: { label: "Author", data: report.views.author }
      };
      const state = {
        view: report.initial_view,
        metric: report.views[report.initial_view].default_metric,
        periodKey: report.default_period_key,
        topN: report.default_top_n,
        search: "",
        focusSeriesKey: ""
      };

      const palette = [
        "#0e5c52", "#d96f3d", "#5e6ad2", "#b5517b",
        "#4f7f25", "#8b4cd7", "#2176ae", "#8c5a1f",
        "#2c9988", "#c23a3a", "#4d4da8", "#a8802d"
      ];

      const viewSelect = document.getElementById("view-select");
      const metricSelect = document.getElementById("metric-select");
      const periodSelect = document.getElementById("period-select");
      const topNSelect = document.getElementById("top-n-select");
      const searchInput = document.getElementById("search-input");
      const focusSeriesSelect = document.getElementById("focus-series-select");
      const summaryCards = document.getElementById("summary-cards");
      const chartRoot = document.getElementById("chart-root");
      const chartLegend = document.getElementById("chart-legend");
      const diagnosticsRoot = document.getElementById("diagnostics-root");
      const tableRoot = document.getElementById("table-root");
      const chartTitle = document.getElementById("chart-title");
      const chartCaption = document.getElementById("chart-caption");
      const tableTitle = document.getElementById("table-title");
      const diagnosticsTitle = document.getElementById("diagnostics-title");
      const reportTitle = document.getElementById("report-title");
      const reportSubtitle = document.getElementById("report-subtitle");

      initialize();

      function initialize() {
        reportTitle.textContent = `${report.target_org} diagnostics`;
        reportSubtitle.textContent = `${report.grain} grain anchored on ${report.time_anchor} with ${report.matched_pull_request_count} matched pull requests`;
        populateViewOptions();
        populatePeriodOptions();
        syncMetricOptions();
        topNSelect.value = String(report.default_top_n);
        render();
        viewSelect.addEventListener("change", onViewChange);
        metricSelect.addEventListener("change", onMetricChange);
        periodSelect.addEventListener("change", onPeriodChange);
        topNSelect.addEventListener("change", onTopNChange);
        searchInput.addEventListener("input", onSearchChange);
        focusSeriesSelect.addEventListener("change", onFocusSeriesChange);
      }

      function onViewChange(event) {
        state.view = event.target.value;
        state.focusSeriesKey = "";
        syncMetricOptions();
        render();
      }

      function onMetricChange(event) {
        state.metric = event.target.value;
        state.focusSeriesKey = "";
        render();
      }

      function onPeriodChange(event) {
        state.periodKey = event.target.value;
        render();
      }

      function onTopNChange(event) {
        state.topN = Number(event.target.value);
        state.focusSeriesKey = "";
        render();
      }

      function onSearchChange(event) {
        state.search = event.target.value.trim().toLowerCase();
        state.focusSeriesKey = "";
        render();
      }

      function onFocusSeriesChange(event) {
        state.focusSeriesKey = event.target.value;
        render();
      }

      function populateViewOptions() {
        viewSelect.innerHTML = Object.entries(viewCatalog)
          .map(([key, value]) => `<option value="${key}">${value.label}</option>`)
          .join("");
        viewSelect.value = state.view;
      }

      function populatePeriodOptions() {
        periodSelect.innerHTML = report.periods
          .map(period => `<option value="${period.key}">${period.label}</option>`)
          .join("");
        periodSelect.value = state.periodKey;
      }

      function syncMetricOptions() {
        const viewData = viewCatalog[state.view].data;
        metricSelect.innerHTML = viewData.metrics
          .map(metric => `<option value="${metric.key}">${metric.label}</option>`)
          .join("");
        if (!viewData.metrics.some(metric => metric.key === state.metric)) {
          state.metric = viewData.default_metric;
        }
        metricSelect.value = state.metric;
      }

      function render() {
        const selectedPeriod = report.periods.find(period => period.key === state.periodKey);
        renderSummaryCards(selectedPeriod);
        renderDiagnostics(selectedPeriod);
        if (state.view === "period") {
          renderPeriodChart();
          renderPeriodTable();
        } else {
          renderEntityChart();
          renderEntityTable();
        }
      }

      function renderSummaryCards(selectedPeriod) {
        if (!selectedPeriod) {
          summaryCards.innerHTML = renderEmptyState("No period data available.");
          return;
        }
        const summary = selectedPeriod.summary;
        summaryCards.innerHTML = [
          summaryCard("Period", selectedPeriod.label, `${selectedPeriod.start_date} to ${selectedPeriod.end_date}`),
          summaryCard("Pull requests", formatValue(summary.pull_request_count, "int"), `Merged ${formatValue(summary.merged_pull_request_count, "int")}`),
          summaryCard("Active authors", formatValue(summary.active_author_count, "int"), `Merged PR / author ${formatValue(summary.merged_pull_requests_per_active_author, "float")}`),
          summaryCard("Median merge", formatValue(summary.time_to_merge_median_hours, "float"), "hours"),
          summaryCard("Median first review", formatValue(summary.time_to_first_review_median_hours, "float"), "hours")
        ].join("");
      }

      function summaryCard(title, value, meta) {
        return `
          <article class="summary-card">
            <h3>${title}</h3>
            <div class="summary-value">${value}</div>
            <div class="summary-meta">${meta}</div>
          </article>
        `;
      }

      function renderDiagnostics(selectedPeriod) {
        if (!selectedPeriod) {
          diagnosticsRoot.innerHTML = renderEmptyState("No diagnostics are available.");
          diagnosticsTitle.textContent = "Spike diagnostics";
          return;
        }
        diagnosticsTitle.textContent = `Spike diagnostics for ${selectedPeriod.label}`;
        const diagnostics = selectedPeriod.diagnostics;
        diagnosticsRoot.innerHTML = `
          <div class="diagnostic-grid">
            ${diagnosticCard("Same-period created", formatValue(diagnostics.same_period_created_ratio, "percent"), `${formatValue(diagnostics.same_period_created_count, "int")} pull requests`, "good")}
            ${diagnosticCard("Older PR ratio", formatValue(diagnostics.older_pull_request_ratio, "percent"), `${formatValue(diagnostics.older_pull_request_count, "int")} carried into this period`, "warn")}
          </div>
          <div class="table-stack">
            ${renderMiniTable("Top contributing repositories", diagnostics.top_contributing_repositories, [
              { key: "label", label: "Repository" },
              { key: "pull_request_count", label: "PRs", format: "int" },
              { key: "share", label: "Share", format: "percent" }
            ])}
            ${renderMiniTable("Top updated dates", diagnostics.top_updated_dates, [
              { key: "label", label: "Date" },
              { key: "count", label: "PRs", format: "int" },
              { key: "share", label: "Share", format: "percent" }
            ])}
            ${renderMiniTable("Timeline event breakdown", diagnostics.timeline_event_breakdown, [
              { key: "label", label: "Event" },
              { key: "event_count", label: "Events", format: "int" },
              { key: "pull_request_count", label: "PRs", format: "int" },
              { key: "share", label: "Share", format: "percent" }
            ])}
          </div>
        `;
      }

      function diagnosticCard(title, value, meta, tone) {
        const toneClass = tone === "warn" ? "warn" : "";
        return `
          <article class="diagnostic-card ${toneClass}">
            <h3>${title}</h3>
            <div class="diagnostic-value">${value}</div>
            <p>${meta}</p>
          </article>
        `;
      }

      function renderMiniTable(title, rows, columns) {
        if (!rows.length) {
          return `
            <section>
              <h3 class="section-title">${title}</h3>
              ${renderEmptyState("No rows available.")}
            </section>
          `;
        }
        const headers = columns.map(column => `<th>${column.label}</th>`).join("");
        const body = rows.map(row => `
          <tr>
            ${columns.map(column => `<td>${formatValue(row[column.key], column.format ?? "text")}</td>`).join("")}
          </tr>
        `).join("");
        return `
          <section>
            <h3 class="section-title">${title}</h3>
            <table class="mini-table">
              <thead><tr>${headers}</tr></thead>
              <tbody>${body}</tbody>
            </table>
          </section>
        `;
      }

      function renderPeriodChart() {
        const periodView = viewCatalog.period.data;
        const metric = metricDefinition(periodView.metrics, state.metric);
        chartTitle.textContent = `${metric.label} by period`;
        chartCaption.textContent = "Matched org-wide trend for the selected metric.";
        chartRoot.innerHTML = lineChart([
          {
            label: metric.label,
            color: palette[0],
            points: periodView.periods.map(period => ({
              label: period.label,
              value: period.values[state.metric]
            }))
          }
        ], metric.format);
        chartLegend.innerHTML = `<span class="legend-chip"><span class="legend-swatch" style="background:${palette[0]}"></span>${metric.label}</span>`;
        focusSeriesSelect.innerHTML = '<option value="">Not applicable</option>';
        focusSeriesSelect.disabled = true;
      }

      function renderEntityChart() {
        const viewData = viewCatalog[state.view].data;
        const metric = metricDefinition(viewData.metrics, state.metric);
        const entities = rankedEntities(viewData.entities, state.metric);
        const focusedEntities = state.focusSeriesKey
          ? entities.filter(entity => entity.key === state.focusSeriesKey)
          : entities;
        const chartEntities = focusedEntities.slice(0, state.focusSeriesKey ? focusedEntities.length : state.topN);
        chartTitle.textContent = `${metric.label} by ${viewCatalog[state.view].label.toLowerCase()}`;
        chartCaption.textContent = `Top ${chartEntities.length} series across ${viewData.periods.length} periods.`;
        chartRoot.innerHTML = chartEntities.length
          ? lineChart(chartEntities.map((entity, index) => ({
              key: entity.key,
              label: entity.label,
              color: palette[index % palette.length],
              points: entity.period_values.map(periodValue => ({
                label: periodValue.label,
                value: periodValue.values[state.metric]
              }))
            })), metric.format)
          : renderEmptyState("No matching series for the current filter.");
        chartLegend.innerHTML = chartEntities.map((entity, index) => `
          <button type="button" class="legend-chip" data-entity-key="${entity.key}">
            <span class="legend-swatch" style="background:${palette[index % palette.length]}"></span>
            ${entity.label}
          </button>
        `).join("");
        chartLegend.querySelectorAll("[data-entity-key]").forEach(button => {
          button.addEventListener("click", () => {
            const entityKey = button.getAttribute("data-entity-key");
            state.focusSeriesKey = state.focusSeriesKey === entityKey ? "" : entityKey;
            focusSeriesSelect.value = state.focusSeriesKey;
            render();
          });
        });
        focusSeriesSelect.disabled = false;
        focusSeriesSelect.innerHTML = ['<option value="">All visible series</option>']
          .concat(entities.map(entity => `<option value="${entity.key}">${entity.label}</option>`))
          .join("");
        focusSeriesSelect.value = state.focusSeriesKey;
      }

      function renderPeriodTable() {
        const periodView = viewCatalog.period.data;
        const metric = metricDefinition(periodView.metrics, state.metric);
        tableTitle.textContent = `${metric.label} across periods`;
        tableRoot.innerHTML = renderMiniTable("Periods", periodView.periods.map(period => ({
          label: period.label,
          start_date: period.start_date,
          end_date: period.end_date,
          value: period.values[state.metric]
        })), [
          { key: "label", label: "Period" },
          { key: "start_date", label: "Start" },
          { key: "end_date", label: "End" },
          { key: "value", label: metric.label, format: metric.format }
        ]);
      }

      function renderEntityTable() {
        const viewData = viewCatalog[state.view].data;
        const metric = metricDefinition(viewData.metrics, state.metric);
        const rows = rankedEntities(viewData.entities, state.metric)
          .map(entity => {
            const periodValue = entity.period_values.find(period => period.key === state.periodKey);
            return {
              label: entity.label,
              value: periodValue?.values[state.metric] ?? null,
              total: entity.totals[state.metric]
            };
          })
          .filter(row => row.value !== null)
          .slice(0, state.topN);
        tableTitle.textContent = `${viewCatalog[state.view].label} ranking for ${state.periodKey}`;
        tableRoot.innerHTML = renderMiniTable(`Top ${viewCatalog[state.view].label.toLowerCase()}s`, rows, [
          { key: "label", label: viewCatalog[state.view].label },
          { key: "value", label: `Selected period ${metric.label}`, format: metric.format },
          { key: "total", label: `All-period ${metric.label}`, format: metric.format }
        ]);
      }

      function rankedEntities(entities, metricKey) {
        return entities
          .filter(entity => !state.search || entity.label.toLowerCase().includes(state.search))
          .sort((left, right) => {
            const leftTotal = Number(left.totals[metricKey] ?? 0);
            const rightTotal = Number(right.totals[metricKey] ?? 0);
            if (rightTotal !== leftTotal) {
              return rightTotal - leftTotal;
            }
            return left.label.localeCompare(right.label);
          });
      }

      function metricDefinition(metrics, metricKey) {
        return metrics.find(metric => metric.key === metricKey) ?? metrics[0];
      }

      function lineChart(seriesList, format) {
        const allValues = seriesList.flatMap(series => series.points.map(point => point.value).filter(value => value !== null && value !== undefined));
        if (!allValues.length) {
          return renderEmptyState("No values are available for this metric.");
        }
        const width = 960;
        const height = 320;
        const padding = { top: 18, right: 20, bottom: 40, left: 48 };
        const innerWidth = width - padding.left - padding.right;
        const innerHeight = height - padding.top - padding.bottom;
        const maxValue = Math.max(...allValues.map(value => Number(value)));
        const minValue = Math.min(...allValues.map(value => Number(value)));
        const baseline = minValue < 0 ? minValue : 0;
        const range = Math.max(maxValue - baseline, 1);
        const xFor = index => padding.left + (seriesList[0].points.length === 1 ? innerWidth / 2 : (innerWidth * index) / (seriesList[0].points.length - 1));
        const yFor = value => padding.top + innerHeight - ((Number(value) - baseline) / range) * innerHeight;
        const axisTicks = Array.from({ length: 4 }, (_, index) => baseline + (range * index) / 3);

        const gridLines = axisTicks.map(tick => {
          const y = yFor(tick);
          return `
            <line x1="${padding.left}" x2="${width - padding.right}" y1="${y}" y2="${y}" stroke="rgba(31,43,42,0.08)" />
            <text x="${padding.left - 10}" y="${y + 4}" text-anchor="end" class="axis-label">${formatValue(tick, format)}</text>
          `;
        }).join("");

        const labels = seriesList[0].points.map((point, index) => `
          <text x="${xFor(index)}" y="${height - 10}" text-anchor="middle" class="axis-label">${point.label}</text>
        `).join("");

        const lines = seriesList.map(series => {
          const path = series.points
            .map((point, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(point.value ?? baseline)}`)
            .join(" ");
          const markers = series.points.map((point, index) => `
            <circle cx="${xFor(index)}" cy="${yFor(point.value ?? baseline)}" r="4" fill="${series.color}" />
          `).join("");
          return `
            <path d="${path}" fill="none" stroke="${series.color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />
            ${markers}
          `;
        }).join("");

        return `
          <svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" role="img" aria-label="Line chart">
            ${gridLines}
            ${lines}
            ${labels}
          </svg>
        `;
      }

      function renderEmptyState(message) {
        return `<div class="empty-state">${message}</div>`;
      }

      function formatValue(value, format) {
        if (value === null || value === undefined || Number.isNaN(Number(value))) {
          return "n/a";
        }
        if (format === "int") {
          return Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(value));
        }
        if (format === "percent") {
          return Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 }).format(Number(value));
        }
        if (format === "float") {
          return Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(Number(value));
        }
        return String(value);
      }
    """
