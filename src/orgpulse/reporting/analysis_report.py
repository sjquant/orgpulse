from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from statistics import fmean, median
from typing import cast

from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from orgpulse.distribution import trim_upper_tail
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
    distribution_percentile: int,
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
                distribution_percentile=distribution_percentile,
            )
        )
    repository_view = _build_entity_view(
        period_catalog=period_catalog,
        filtered_metrics=filtered_metrics,
        identity_builder=lambda metric: (
            metric.repository_full_name,
            metric.repository_full_name,
        ),
        distribution_percentile=distribution_percentile,
    )
    author_view = _build_entity_view(
        period_catalog=period_catalog,
        filtered_metrics=filtered_metrics,
        identity_builder=lambda metric: _author_identity(metric.author_login),
        distribution_percentile=distribution_percentile,
    )
    return {
        "target_org": target_org,
        "grain": grain,
        "time_anchor": time_anchor,
        "initial_view": initial_view,
        "default_top_n": default_top_n,
        "since": None if since is None else since.isoformat(),
        "until": None if until is None else until.isoformat(),
        "distribution_percentile": distribution_percentile,
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
        distribution_percentile=100,
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
            "closed": period.closed,
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
    distribution_percentile: int,
) -> dict[str, object]:
    summary = _aggregate_metric_summary(
        period_metrics,
        distribution_percentile=distribution_percentile,
    )
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
    *,
    distribution_percentile: int,
) -> dict[str, int | float | None]:
    merged_pull_request_count = sum(1 for metric in metrics if metric.merged)
    active_author_count = len(
        {
            metric.author_login.lower()
            for metric in metrics
            if metric.author_login is not None
        }
    )
    time_to_merge_values = trim_upper_tail(
        [
            metric.time_to_merge_seconds
            for metric in metrics
            if metric.time_to_merge_seconds is not None
        ],
        percentile=distribution_percentile,
    )
    time_to_first_review_values = trim_upper_tail(
        [
            metric.time_to_first_review_seconds
            for metric in metrics
            if metric.time_to_first_review_seconds is not None
        ],
        percentile=distribution_percentile,
    )
    changed_line_values = trim_upper_tail(
        [metric.changed_lines for metric in metrics],
        percentile=distribution_percentile,
    )
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
        "total_changed_lines": sum(changed_line_values),
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
    distribution_percentile: int,
) -> dict[str, object]:
    entities_by_period: dict[str, dict[str, list[PullRequestMetricRecord]]] = defaultdict(
        lambda: defaultdict(list)
    )
    entity_metrics: dict[str, list[PullRequestMetricRecord]] = defaultdict(list)
    entity_labels: dict[str, str] = {}
    for metric in filtered_metrics:
        entity_key, entity_label = identity_builder(metric)
        entity_labels.setdefault(entity_key, entity_label)
        entity_metrics[entity_key].append(metric)
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
                entity_metrics=tuple(entity_metrics[entity_key]),
                entities_by_period=entities_by_period,
                distribution_percentile=distribution_percentile,
            )
            for entity_key in sorted(entity_labels)
        ],
    }


def _build_entity_report(
    *,
    entity_key: str,
    entity_label: str,
    period_catalog: Sequence[dict[str, object]],
    entity_metrics: tuple[PullRequestMetricRecord, ...],
    entities_by_period: dict[str, dict[str, list[PullRequestMetricRecord]]],
    distribution_percentile: int,
) -> dict[str, object]:
    period_values = [
        {
            **period_descriptor,
            "values": _entity_period_values(
                entities_by_period.get(cast(str, period_descriptor["key"]), {}).get(
                    entity_key,
                    [],
                ),
                distribution_percentile=distribution_percentile,
            ),
        }
        for period_descriptor in period_catalog
    ]
    return {
        "key": entity_key,
        "label": entity_label,
        "period_values": period_values,
        "totals": _entity_totals(
            entity_metrics,
            distribution_percentile=distribution_percentile,
        ),
    }


def _entity_period_values(
    metrics: Sequence[PullRequestMetricRecord],
    *,
    distribution_percentile: int,
) -> dict[str, int | float | None]:
    merge_values = trim_upper_tail(
        [
            metric.time_to_merge_seconds
            for metric in metrics
            if metric.time_to_merge_seconds is not None
        ],
        percentile=distribution_percentile,
    )
    first_review_values = trim_upper_tail(
        [
            metric.time_to_first_review_seconds
            for metric in metrics
            if metric.time_to_first_review_seconds is not None
        ],
        percentile=distribution_percentile,
    )
    changed_line_values = trim_upper_tail(
        [metric.changed_lines for metric in metrics],
        percentile=distribution_percentile,
    )
    return {
        "pull_request_count": len(metrics),
        "merged_pull_request_count": sum(1 for metric in metrics if metric.merged),
        "total_changed_lines": sum(changed_line_values),
        "median_time_to_merge_hours": _hours_from_seconds(_median(merge_values)),
        "median_time_to_first_review_hours": _hours_from_seconds(
            _median(first_review_values)
        ),
    }


def _entity_totals(
    metrics: Sequence[PullRequestMetricRecord],
    *,
    distribution_percentile: int,
) -> dict[str, int | float | None]:
    merge_values = trim_upper_tail(
        [
            metric.time_to_merge_seconds
            for metric in metrics
            if metric.time_to_merge_seconds is not None
        ],
        percentile=distribution_percentile,
    )
    first_review_values = trim_upper_tail(
        [
            metric.time_to_first_review_seconds
            for metric in metrics
            if metric.time_to_first_review_seconds is not None
        ],
        percentile=distribution_percentile,
    )
    changed_line_values = trim_upper_tail(
        [metric.changed_lines for metric in metrics],
        percentile=distribution_percentile,
    )
    return {
        "pull_request_count": len(metrics),
        "merged_pull_request_count": sum(1 for metric in metrics if metric.merged),
        "total_changed_lines": sum(changed_line_values),
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
    return _analysis_report_template().render(
        report_payload_json=serialized_payload,
    )


def render_organization_report_html(
    report_payload: dict[str, object],
) -> str:
    """Render the legacy organization report HTML wrapper."""
    return render_analysis_report_html(report_payload)


@lru_cache(maxsize=1)
def _analysis_report_template() -> Template:
    templates_dir = Path(__file__).resolve().parents[1] / "templates"
    environment = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "html.j2", "xml"]),
    )
    return environment.get_template("analysis_report.html.j2")
