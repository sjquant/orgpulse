from __future__ import annotations

import csv
import json
from datetime import date, datetime
from enum import StrEnum
from io import StringIO
from pathlib import Path
from statistics import fmean, median

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from orgpulse.config import get_settings
from orgpulse.errors import AnalysisInputError
from orgpulse.metrics import PullRequestMetricCollectionBuilder
from orgpulse.models import (
    MetricValueSummary,
    OrgSlug,
    PeriodGrain,
    PullRequestMetricCollection,
    PullRequestMetricRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunConfig,
    RunManifest,
    TimeAnchor,
)
from orgpulse.visualization import (
    build_analysis_report_payload,
    render_analysis_report_html,
)


class AnalysisGrouping(StrEnum):
    PERIOD = "period"
    REPOSITORY = "repository"
    AUTHOR = "author"


class AnalysisExportFormat(StrEnum):
    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"
    HTML = "html"


class AnalysisConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    org: OrgSlug
    output_dir: Path = Field(default_factory=lambda: Path("output"))
    grain: PeriodGrain = PeriodGrain.MONTH
    time_anchor: TimeAnchor = TimeAnchor.CREATED_AT
    grouping: AnalysisGrouping = AnalysisGrouping.PERIOD
    top_n: int | None = Field(default=None, ge=1)
    since: date | None = None
    until: date | None = None
    export_format: AnalysisExportFormat = AnalysisExportFormat.JSON

    @field_validator("output_dir", mode="before")
    @classmethod
    def normalize_output_dir(cls, value: object) -> Path:
        if value is None:
            return Path("output")
        return Path(str(value)).expanduser()

    @field_validator("until")
    @classmethod
    def validate_until(
        cls,
        value: date | None,
        info: ValidationInfo,
    ) -> date | None:
        since = info.data.get("since")
        if since is not None and value is not None and value < since:
            raise ValueError("--until must be on or after --since")
        return value


class AnalysisRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    group_value: str
    period_key: str | None = None
    period_start_date: date | None = None
    period_end_date: date | None = None
    pull_request_count: int
    merged_pull_request_count: int
    active_author_count: int
    merged_pull_requests_per_active_author: float | None
    time_to_merge_count: int
    time_to_merge_average_seconds: float | None
    time_to_merge_median_seconds: float | None
    time_to_first_review_count: int
    time_to_first_review_average_seconds: float | None
    time_to_first_review_median_seconds: float | None
    additions_total: int
    additions_average: float | None
    deletions_total: int
    deletions_average: float | None
    changed_lines_total: int
    changed_lines_average: float | None
    changed_files_total: int
    changed_files_average: float | None
    commits_total: int
    commits_average: float | None


class AnalysisResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    target_org: str
    source_manifest_path: Path
    output_dir: Path
    grain: PeriodGrain
    grouping: AnalysisGrouping
    time_anchor: TimeAnchor
    since: date | None
    until: date | None
    top_n: int | None
    matched_pull_request_count: int
    rows: tuple[AnalysisRow, ...]
    export_format: AnalysisExportFormat
    report_payload: dict[str, object] | None = Field(default=None, exclude=True)


class AnalysisService:
    """Build grouped local analysis views from normalized raw snapshots."""

    def analyze(
        self,
        config: AnalysisConfig,
    ) -> AnalysisResult:
        manifest_path, manifest = self._load_manifest(config)
        raw_snapshot = self._load_raw_snapshot(manifest)
        pull_request_metrics = self._load_pull_request_metrics(
            config,
            manifest,
            raw_snapshot,
        )
        filtered_metrics = self._filter_metrics(config, pull_request_metrics)
        rows = self._build_rows(config, pull_request_metrics, filtered_metrics)
        return AnalysisResult(
            target_org=manifest.target_org,
            source_manifest_path=manifest_path,
            output_dir=config.output_dir,
            grain=config.grain,
            grouping=config.grouping,
            time_anchor=config.time_anchor,
            since=config.since,
            until=config.until,
            top_n=config.top_n,
            matched_pull_request_count=len(filtered_metrics),
            rows=rows,
            export_format=config.export_format,
            report_payload=self._build_report_payload(
                config,
                manifest=manifest,
                raw_snapshot=raw_snapshot,
                filtered_metrics=filtered_metrics,
            ),
        )

    def _load_manifest(
        self,
        config: AnalysisConfig,
    ) -> tuple[Path, RunManifest]:
        manifest_path = (
            config.output_dir
            / "manifest"
            / config.grain.value
            / config.time_anchor.value
            / "manifest.json"
        )
        if not manifest_path.exists():
            raise AnalysisInputError(
                "analysis input is missing: "
                f"{manifest_path}. Run `orgpulse run` for this grain and time anchor first."
            )
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AnalysisInputError(
                f"analysis manifest is unreadable: {manifest_path}"
            ) from exc

        manifest = RunManifest.model_validate(payload)
        if manifest.target_org.lower() != config.org.lower():
            raise AnalysisInputError(
                "analysis manifest org does not match the requested org: "
                f"expected {config.org}, found {manifest.target_org}"
            )
        return manifest_path, manifest

    def _load_raw_snapshot(
        self,
        manifest: RunManifest,
    ) -> RawSnapshotWriteResult:
        return RawSnapshotWriteResult(
            root_dir=manifest.raw_snapshot_root_dir,
            periods=tuple(
                self._build_snapshot_period(manifest.raw_snapshot_root_dir, period)
                for period in (*manifest.locked_periods, *manifest.refreshed_periods)
            ),
        )

    def _load_pull_request_metrics(
        self,
        config: AnalysisConfig,
        manifest: RunManifest,
        raw_snapshot: RawSnapshotWriteResult,
    ) -> PullRequestMetricCollection:
        metric_config = RunConfig.model_validate(
            {
                "org": manifest.target_org,
                "as_of": manifest.last_successful_run.as_of,
                "period": manifest.period_grain,
                "time_anchor": manifest.time_anchor,
                "output_dir": config.output_dir,
            }
        )
        return PullRequestMetricCollectionBuilder().build(metric_config, raw_snapshot)

    def _build_snapshot_period(
        self,
        root_dir: Path,
        period: ReportingPeriod | RawSnapshotPeriod,
    ) -> RawSnapshotPeriod:
        if isinstance(period, RawSnapshotPeriod):
            return period
        period_dir = root_dir / period.key
        return RawSnapshotPeriod(
            key=period.key,
            start_date=period.start_date,
            end_date=period.end_date,
            directory=period_dir,
            pull_requests_path=period_dir / "pull_requests.csv",
            pull_request_count=0,
            reviews_path=period_dir / "pull_request_reviews.csv",
            review_count=0,
            timeline_events_path=period_dir / "pull_request_timeline_events.csv",
            timeline_event_count=0,
        )

    def _build_report_payload(
        self,
        config: AnalysisConfig,
        *,
        manifest: RunManifest,
        raw_snapshot: RawSnapshotWriteResult,
        filtered_metrics: tuple[PullRequestMetricRecord, ...],
    ) -> dict[str, object]:
        return build_analysis_report_payload(
            target_org=manifest.target_org,
            grain=config.grain.value,
            time_anchor=config.time_anchor.value,
            initial_view=config.grouping.value,
            default_top_n=8 if config.top_n is None else config.top_n,
            since=config.since,
            until=config.until,
            matched_pull_request_count=len(filtered_metrics),
            filtered_metrics=filtered_metrics,
            raw_snapshot=raw_snapshot,
        )

    def _filter_metrics(
        self,
        config: AnalysisConfig,
        pull_request_metrics: PullRequestMetricCollection,
    ) -> tuple[PullRequestMetricRecord, ...]:
        filtered_metrics: list[PullRequestMetricRecord] = []
        for period in pull_request_metrics.periods:
            for pull_request_metric in period.pull_request_metrics:
                anchor_at = self._anchor_datetime(config.time_anchor, pull_request_metric)
                if anchor_at is None:
                    continue
                if config.since is not None and anchor_at.date() < config.since:
                    continue
                if config.until is not None and anchor_at.date() > config.until:
                    continue
                filtered_metrics.append(pull_request_metric)
        return tuple(filtered_metrics)

    def _anchor_datetime(
        self,
        time_anchor: TimeAnchor,
        pull_request_metric: PullRequestMetricRecord,
    ) -> datetime | None:
        if time_anchor is TimeAnchor.CREATED_AT:
            return pull_request_metric.created_at
        if time_anchor is TimeAnchor.UPDATED_AT:
            return pull_request_metric.updated_at
        return pull_request_metric.merged_at

    def _build_rows(
        self,
        config: AnalysisConfig,
        pull_request_metrics: PullRequestMetricCollection,
        filtered_metrics: tuple[PullRequestMetricRecord, ...],
    ) -> tuple[AnalysisRow, ...]:
        if config.grouping is AnalysisGrouping.PERIOD:
            return self._period_rows(
                config=config,
                pull_request_metrics=pull_request_metrics,
                filtered_metrics=filtered_metrics,
            )
        if config.grouping is AnalysisGrouping.REPOSITORY:
            return self._grouped_rows(
                config=config,
                filtered_metrics=filtered_metrics,
                key_builder=lambda metric: metric.repository_full_name,
            )
        return self._grouped_rows(
            config=config,
            filtered_metrics=filtered_metrics,
            key_builder=lambda metric: metric.author_login or "unknown",
        )

    def _period_rows(
        self,
        *,
        config: AnalysisConfig,
        pull_request_metrics: PullRequestMetricCollection,
        filtered_metrics: tuple[PullRequestMetricRecord, ...],
    ) -> tuple[AnalysisRow, ...]:
        metrics_by_period_key: dict[str, list[PullRequestMetricRecord]] = {}
        for pull_request_metric in filtered_metrics:
            metrics_by_period_key.setdefault(pull_request_metric.period_key, []).append(
                pull_request_metric
            )

        rows = [
            self._analysis_row(
                group_value=period.key,
                period_key=period.key,
                period_start_date=period.start_date,
                period_end_date=period.end_date,
                pull_request_metrics=tuple(metrics_by_period_key.get(period.key, ())),
            )
            for period in pull_request_metrics.periods
            if period.key in metrics_by_period_key
        ]
        rows.sort(
            key=lambda row: (
                row.period_start_date or date.min,
                row.group_value,
            )
        )
        if config.top_n is None:
            return tuple(rows)
        return tuple(rows[-config.top_n :])

    def _grouped_rows(
        self,
        *,
        config: AnalysisConfig,
        filtered_metrics: tuple[PullRequestMetricRecord, ...],
        key_builder,
    ) -> tuple[AnalysisRow, ...]:
        metrics_by_group: dict[str, list[PullRequestMetricRecord]] = {}
        for pull_request_metric in filtered_metrics:
            metrics_by_group.setdefault(key_builder(pull_request_metric), []).append(
                pull_request_metric
            )

        rows = [
            self._analysis_row(
                group_value=group_value,
                period_key=None,
                period_start_date=None,
                period_end_date=None,
                pull_request_metrics=tuple(group_metrics),
            )
            for group_value, group_metrics in metrics_by_group.items()
        ]
        rows.sort(
            key=lambda row: (
                -row.pull_request_count,
                row.group_value,
            )
        )
        if config.top_n is None:
            return tuple(rows)
        return tuple(rows[: config.top_n])

    def _analysis_row(
        self,
        *,
        group_value: str,
        period_key: str | None,
        period_start_date: date | None,
        period_end_date: date | None,
        pull_request_metrics: tuple[PullRequestMetricRecord, ...],
    ) -> AnalysisRow:
        merged_pull_request_count = len(
            tuple(metric for metric in pull_request_metrics if metric.merged)
        )
        active_author_count = len(
            {
                metric.author_login
                for metric in pull_request_metrics
                if metric.author_login is not None
            }
        )
        time_to_merge = self._summary(
            tuple(
                metric.time_to_merge_seconds
                for metric in pull_request_metrics
                if metric.time_to_merge_seconds is not None
            )
        )
        time_to_first_review = self._summary(
            tuple(
                metric.time_to_first_review_seconds
                for metric in pull_request_metrics
                if metric.time_to_first_review_seconds is not None
            )
        )
        additions = self._summary(tuple(metric.additions for metric in pull_request_metrics))
        deletions = self._summary(tuple(metric.deletions for metric in pull_request_metrics))
        changed_lines = self._summary(
            tuple(metric.changed_lines for metric in pull_request_metrics)
        )
        changed_files = self._summary(
            tuple(metric.changed_files for metric in pull_request_metrics)
        )
        commits = self._summary(tuple(metric.commits for metric in pull_request_metrics))
        return AnalysisRow(
            group_value=group_value,
            period_key=period_key,
            period_start_date=period_start_date,
            period_end_date=period_end_date,
            pull_request_count=len(pull_request_metrics),
            merged_pull_request_count=merged_pull_request_count,
            active_author_count=active_author_count,
            merged_pull_requests_per_active_author=self._per_active_author(
                merged_pull_request_count,
                active_author_count,
            ),
            time_to_merge_count=time_to_merge.count,
            time_to_merge_average_seconds=time_to_merge.average,
            time_to_merge_median_seconds=time_to_merge.median,
            time_to_first_review_count=time_to_first_review.count,
            time_to_first_review_average_seconds=time_to_first_review.average,
            time_to_first_review_median_seconds=time_to_first_review.median,
            additions_total=additions.total,
            additions_average=additions.average,
            deletions_total=deletions.total,
            deletions_average=deletions.average,
            changed_lines_total=changed_lines.total,
            changed_lines_average=changed_lines.average,
            changed_files_total=changed_files.total,
            changed_files_average=changed_files.average,
            commits_total=commits.total,
            commits_average=commits.average,
        )

    def _summary(
        self,
        values: tuple[int, ...],
    ) -> MetricValueSummary:
        if not values:
            return MetricValueSummary(
                count=0,
                total=0,
                average=None,
                median=None,
            )
        return MetricValueSummary(
            count=len(values),
            total=sum(values),
            average=fmean(values),
            median=float(median(values)),
        )

    def _per_active_author(
        self,
        merged_pull_request_count: int,
        active_author_count: int,
    ) -> float | None:
        if active_author_count == 0:
            return None
        return merged_pull_request_count / active_author_count


def build_analysis_config(
    *,
    org: str | None = None,
    output_dir: Path | None = None,
    grain: PeriodGrain | None = None,
    time_anchor: TimeAnchor | None = None,
    grouping: AnalysisGrouping | None = None,
    top_n: int | None = None,
    since: date | str | None = None,
    until: date | str | None = None,
    export_format: AnalysisExportFormat | None = None,
) -> AnalysisConfig:
    settings = get_settings()
    payload: dict[str, object] = {
        "org": settings.org if org is None else org,
        "output_dir": settings.output_dir if output_dir is None else output_dir,
        "grain": settings.period if grain is None else grain,
        "time_anchor": settings.time_anchor if time_anchor is None else time_anchor,
        "grouping": (
            AnalysisGrouping.PERIOD if grouping is None else grouping
        ),
        "export_format": (
            AnalysisExportFormat.JSON
            if export_format is None
            else export_format
        ),
    }
    if top_n is not None:
        payload["top_n"] = top_n
    if since is not None:
        payload["since"] = since
    if until is not None:
        payload["until"] = until
    return AnalysisConfig.model_validate(payload)


def render_analysis_result(
    result: AnalysisResult,
) -> str:
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
