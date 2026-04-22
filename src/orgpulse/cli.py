from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from github import Auth, Github
from pydantic import ValidationError

from orgpulse.analysis import (
    AnalysisExportFormat,
    AnalysisGrouping,
    AnalysisService,
    build_analysis_config,
    render_analysis_result,
)
from orgpulse.config import get_settings
from orgpulse.errors import (
    AnalysisInputError,
    AuthResolutionError,
    GitHubApiError,
    OrgTargetingError,
)
from orgpulse.github_auth import GitHubAuthService, resolve_auth_token
from orgpulse.ingestion import (
    PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME,
    PULL_REQUEST_SNAPSHOT_FILENAME,
    PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME,
    CanonicalRawInventoryStore,
    GitHubIngestionService,
    NormalizedRawSnapshotWriter,
)
from orgpulse.metrics import (
    MetricValidationCollectionBuilder,
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.models import (
    ManifestWriteResult,
    MetricValidationCollection,
    OrganizationMetricCollection,
    OrgSummaryWriteResult,
    PeriodGrain,
    PullRequestCollection,
    PullRequestRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RepositorySummaryCsvWriteResult,
    RunConfig,
    RunManifest,
    RunMode,
    TimeAnchor,
)
from orgpulse.output import (
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)

app = typer.Typer(
    add_completion=False,
    help="Collect GitHub organization metrics and write stable file outputs.",
    no_args_is_help=True,
)


@app.callback()
def callback() -> None:
    """Org-wide GitHub metrics reporting CLI."""


@app.command("run")
def run_command(
    org: Annotated[
        str | None,
        typer.Option(
            "--org", help="GitHub organization to collect. Falls back to ORGPULSE_ORG."
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            help="Anchor date used to resolve the current open reporting period. Falls back to ORGPULSE_AS_OF or today.",
        ),
    ] = None,
    period: Annotated[
        PeriodGrain | None,
        typer.Option(
            "--period",
            help="Reporting grain for snapshots and rollups. Falls back to ORGPULSE_PERIOD.",
        ),
    ] = None,
    mode: Annotated[
        RunMode | None,
        typer.Option(
            "--mode",
            help="Run strategy: full rebuild ignores locks, incremental refreshes pull requests updated during the current open period, and backfill refreshes an explicit closed-period range. Falls back to ORGPULSE_MODE.",
        ),
    ] = None,
    time_anchor: Annotated[
        TimeAnchor | None,
        typer.Option(
            "--time-anchor",
            help="Timestamp used to bucket and filter pull requests. Defaults to created_at and falls back to ORGPULSE_TIME_ANCHOR.",
        ),
    ] = None,
    include_repos: Annotated[
        list[str] | None,
        typer.Option(
            "--repo",
            help="Restrict collection to a repository. May be provided multiple times.",
        ),
    ] = None,
    exclude_repos: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude-repo",
            help="Exclude a repository from collection. May be provided multiple times.",
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            help="Directory where raw snapshots and rollups will be written. Falls back to ORGPULSE_OUTPUT_DIR.",
        ),
    ] = None,
    backfill_start: Annotated[
        str | None,
        typer.Option(
            "--backfill-start",
            help="Inclusive ISO date for backfill mode, for example 2026-01-01.",
        ),
    ] = None,
    backfill_end: Annotated[
        str | None,
        typer.Option(
            "--backfill-end",
            help="Inclusive ISO date for backfill mode, for example 2026-03-31.",
        ),
    ] = None,
) -> None:
    try:
        config = build_run_config(
            org=org,
            as_of=as_of,
            period=period,
            mode=mode,
            time_anchor=time_anchor,
            include_repos=include_repos,
            exclude_repos=exclude_repos,
            output_dir=output_dir,
            backfill_start=backfill_start,
            backfill_end=backfill_end,
        )
    except ValidationError as exc:
        typer.echo(f"orgpulse: invalid configuration\n{exc}", err=True)
        raise typer.Exit(code=2) from exc
    try:
        resolved_token = resolve_auth_token(config)
        github_client = Github(auth=Auth.Token(resolved_token.token))
        github_context = GitHubAuthService(
            github_client, resolved_token.source
        ).validate_access(config)
        ingestion_service = GitHubIngestionService(github_client)
        inventory = ingestion_service.load_repository_inventory(config)
        collection = ingestion_service.fetch_pull_requests(config, inventory)
        (
            raw_snapshot,
            raw_snapshot_skipped_reason,
            manifest,
            manifest_skipped_reason,
        ) = _write_outputs(
            config,
            len(inventory.repositories),
            collection,
        )
        (
            repo_summary,
            repo_summary_skipped_reason,
            org_metrics,
            org_metrics_skipped_reason,
            metric_validation,
            metric_validation_skipped_reason,
        ) = _build_metric_outputs(
            config,
            manifest=manifest,
            raw_snapshot=raw_snapshot,
            raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
        )
        org_summary, org_summary_skipped_reason = _write_org_summary(
            config,
            org_metrics=org_metrics,
            org_metrics_skipped_reason=org_metrics_skipped_reason,
            refreshed_period_keys=()
            if raw_snapshot is None
            else tuple(period.key for period in raw_snapshot.periods),
        )
        if not collection.failures:
            ingestion_service.clear_checkpoint(config)
    except AuthResolutionError as exc:
        typer.echo(f"orgpulse: GitHub authentication failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc
    except GitHubApiError as exc:
        typer.echo(f"orgpulse: GitHub API request failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc
    except OrgTargetingError as exc:
        typer.echo(f"orgpulse: GitHub access validation failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        json.dumps(
            {
                "config": config.model_dump(mode="json"),
                "github": github_context.model_dump(mode="json"),
                "inventory": {
                    "organization_login": inventory.organization_login,
                    "repository_count": len(inventory.repositories),
                },
                "collection": {
                    "window": collection.window.model_dump(mode="json"),
                    "pull_request_count": len(collection.pull_requests),
                    "failure_count": len(collection.failures),
                    "failures": [
                        failure.model_dump(mode="json")
                        for failure in collection.failures
                    ],
                },
                "raw_snapshot": None
                if raw_snapshot is None
                else raw_snapshot.model_dump(mode="json"),
                "raw_snapshot_skipped_reason": raw_snapshot_skipped_reason,
                "manifest": None
                if manifest is None
                else manifest.manifest.model_dump(mode="json"),
                "manifest_path": None
                if manifest is None
                else str(manifest.path),
                "manifest_skipped_reason": manifest_skipped_reason,
                "repo_summary": None
                if repo_summary is None
                else repo_summary.model_dump(mode="json"),
                "repo_summary_skipped_reason": repo_summary_skipped_reason,
                "org_metrics": None
                if org_metrics is None
                else org_metrics.model_dump(mode="json"),
                "org_metrics_skipped_reason": org_metrics_skipped_reason,
                "org_summary": None
                if org_summary is None
                else org_summary.model_dump(mode="json"),
                "org_summary_skipped_reason": org_summary_skipped_reason,
                "metric_validation": None
                if metric_validation is None
                else metric_validation.model_dump(mode="json"),
                "metric_validation_skipped_reason": metric_validation_skipped_reason,
            },
            indent=2,
            sort_keys=True,
        )
    )


@app.command("reaggregate")
def reaggregate_command(
    org: Annotated[
        str | None,
        typer.Option(
            "--org",
            help="GitHub organization to re-aggregate. Falls back to ORGPULSE_ORG.",
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            help="Anchor date used to resolve regenerated reporting periods. Falls back to ORGPULSE_AS_OF or today.",
        ),
    ] = None,
    period: Annotated[
        PeriodGrain | None,
        typer.Option(
            "--period",
            help="Reporting grain for regenerated snapshots and rollups. Falls back to ORGPULSE_PERIOD.",
        ),
    ] = None,
    time_anchor: Annotated[
        TimeAnchor | None,
        typer.Option(
            "--time-anchor",
            help="Timestamp used to bucket regenerated pull requests. Defaults to created_at and falls back to ORGPULSE_TIME_ANCHOR.",
        ),
    ] = None,
    include_repos: Annotated[
        list[str] | None,
        typer.Option(
            "--repo",
            help="Restrict re-aggregation to the stored repository scope. May be provided multiple times.",
        ),
    ] = None,
    exclude_repos: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude-repo",
            help="Exclude repositories from the stored scope. May be provided multiple times.",
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            help="Directory where the canonical raw inventory and regenerated outputs live. Falls back to ORGPULSE_OUTPUT_DIR.",
        ),
    ] = None,
) -> None:
    try:
        config = build_run_config(
            org=org,
            as_of=as_of,
            period=period,
            mode=RunMode.FULL,
            time_anchor=time_anchor,
            include_repos=include_repos,
            exclude_repos=exclude_repos,
            output_dir=output_dir,
        )
    except ValidationError as exc:
        typer.echo(f"orgpulse: invalid configuration\n{exc}", err=True)
        raise typer.Exit(code=2) from exc

    canonical_pull_requests = CanonicalRawInventoryStore().load(config)
    if canonical_pull_requests is None:
        typer.echo(
            "orgpulse: canonical raw inventory is missing or does not match the requested org and repository scope",
            err=True,
        )
        raise typer.Exit(code=1)

    filtered_pull_requests = _reaggregate_pull_requests(
        config,
        canonical_pull_requests=canonical_pull_requests,
    )
    collection = _canonical_inventory_collection(config, filtered_pull_requests)
    repository_count = _canonical_inventory_repository_count(filtered_pull_requests)
    (
        raw_snapshot,
        raw_snapshot_skipped_reason,
        manifest,
        manifest_skipped_reason,
    ) = _write_outputs(
        config,
        repository_count,
        collection,
    )
    (
        repo_summary,
        repo_summary_skipped_reason,
        org_metrics,
        org_metrics_skipped_reason,
        metric_validation,
        metric_validation_skipped_reason,
    ) = _build_metric_outputs(
        config,
        manifest=manifest,
        raw_snapshot=raw_snapshot,
        raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
    )
    org_summary, org_summary_skipped_reason = _write_org_summary(
        config,
        org_metrics=org_metrics,
        org_metrics_skipped_reason=org_metrics_skipped_reason,
        refreshed_period_keys=()
        if raw_snapshot is None
        else tuple(period_record.key for period_record in raw_snapshot.periods),
    )
    typer.echo(
        json.dumps(
            {
                "config": config.model_dump(mode="json"),
                "source": {
                    "kind": "canonical_raw_inventory",
                    "repository_count": repository_count,
                    "pull_request_count": len(filtered_pull_requests),
                },
                "collection": {
                    "window": collection.window.model_dump(mode="json"),
                    "pull_request_count": len(collection.pull_requests),
                    "failure_count": len(collection.failures),
                    "failures": [],
                },
                "raw_snapshot": None
                if raw_snapshot is None
                else raw_snapshot.model_dump(mode="json"),
                "raw_snapshot_skipped_reason": raw_snapshot_skipped_reason,
                "manifest": None
                if manifest is None
                else manifest.manifest.model_dump(mode="json"),
                "manifest_path": None
                if manifest is None
                else str(manifest.path),
                "manifest_skipped_reason": manifest_skipped_reason,
                "repo_summary": None
                if repo_summary is None
                else repo_summary.model_dump(mode="json"),
                "repo_summary_skipped_reason": repo_summary_skipped_reason,
                "org_metrics": None
                if org_metrics is None
                else org_metrics.model_dump(mode="json"),
                "org_metrics_skipped_reason": org_metrics_skipped_reason,
                "org_summary": None
                if org_summary is None
                else org_summary.model_dump(mode="json"),
                "org_summary_skipped_reason": org_summary_skipped_reason,
                "metric_validation": None
                if metric_validation is None
                else metric_validation.model_dump(mode="json"),
                "metric_validation_skipped_reason": metric_validation_skipped_reason,
            },
            indent=2,
            sort_keys=True,
        )
    )


@app.command("analyze")
def analyze_command(
    org: Annotated[
        str | None,
        typer.Option(
            "--org",
            help="GitHub organization whose local outputs should be analyzed. Falls back to ORGPULSE_ORG.",
        ),
    ] = None,
    grain: Annotated[
        PeriodGrain | None,
        typer.Option(
            "--grain",
            help="Snapshot grain to analyze. Falls back to ORGPULSE_PERIOD.",
        ),
    ] = None,
    grouping: Annotated[
        AnalysisGrouping | None,
        typer.Option(
            "--group-by",
            help="Dimension used to group the analysis output.",
        ),
    ] = None,
    top_n: Annotated[
        int | None,
        typer.Option(
            "--top",
            min=1,
            help="Limit the output to the top N grouped rows.",
        ),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Inclusive ISO date lower bound for the selected time anchor.",
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help="Inclusive ISO date upper bound for the selected time anchor.",
        ),
    ] = None,
    time_anchor: Annotated[
        TimeAnchor | None,
        typer.Option(
            "--time-anchor",
            help="Timestamp used to filter the local pull request dataset. Falls back to ORGPULSE_TIME_ANCHOR.",
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            help="Directory containing local orgpulse outputs. Falls back to ORGPULSE_OUTPUT_DIR.",
        ),
    ] = None,
    export_format: Annotated[
        AnalysisExportFormat | None,
        typer.Option(
            "--format",
            help="Analysis export format written to stdout.",
        ),
    ] = None,
) -> None:
    try:
        config = build_analysis_config(
            org=org,
            output_dir=output_dir,
            grain=grain,
            time_anchor=time_anchor,
            grouping=grouping,
            top_n=top_n,
            since=since,
            until=until,
            export_format=export_format,
        )
    except ValidationError as exc:
        typer.echo(f"orgpulse: invalid analysis configuration\n{exc}", err=True)
        raise typer.Exit(code=2) from exc

    try:
        result = AnalysisService().analyze(config)
    except AnalysisInputError as exc:
        typer.echo(f"orgpulse: analysis input failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(render_analysis_result(result))


def _write_outputs(
    config: RunConfig,
    repository_count: int,
    collection: PullRequestCollection,
) -> tuple[
    RawSnapshotWriteResult | None,
    str | None,
    ManifestWriteResult | None,
    str | None,
]:
    raw_snapshot, raw_snapshot_skipped_reason = _write_raw_snapshot(
        config,
        collection,
    )
    manifest, manifest_skipped_reason = _write_manifest(
        config,
        repository_count=repository_count,
        collection=collection,
        raw_snapshot=raw_snapshot,
        raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
    )
    return (
        raw_snapshot,
        raw_snapshot_skipped_reason,
        manifest,
        manifest_skipped_reason,
    )


def _write_raw_snapshot(
    config: RunConfig,
    collection: PullRequestCollection,
) -> tuple[RawSnapshotWriteResult | None, str | None]:
    if collection.failures:
        return None, "repository_collection_failures"
    return NormalizedRawSnapshotWriter().write(config, collection), None


def _write_manifest(
    config: RunConfig,
    *,
    repository_count: int,
    collection: PullRequestCollection,
    raw_snapshot: RawSnapshotWriteResult | None,
    raw_snapshot_skipped_reason: str | None,
) -> tuple[ManifestWriteResult | None, str | None]:
    if raw_snapshot is None:
        return None, raw_snapshot_skipped_reason
    return (
        RunManifestWriter().write(
            config,
            collection,
            raw_snapshot,
            repository_count=repository_count,
        ),
        None,
    )


def _build_metric_outputs(
    config: RunConfig,
    *,
    manifest: ManifestWriteResult | None,
    raw_snapshot: RawSnapshotWriteResult | None,
    raw_snapshot_skipped_reason: str | None,
) -> tuple[
    RepositorySummaryCsvWriteResult | None,
    str | None,
    OrganizationMetricCollection | None,
    str | None,
    MetricValidationCollection | None,
    str | None,
]:
    if raw_snapshot is None or manifest is None:
        return (
            None,
            raw_snapshot_skipped_reason,
            None,
            raw_snapshot_skipped_reason,
            None,
            raw_snapshot_skipped_reason,
        )
    metric_snapshot = _build_metric_snapshot(
        manifest=manifest.manifest,
        refreshed_snapshot=raw_snapshot,
    )
    pull_request_metrics = PullRequestMetricCollectionBuilder().build(
        config,
        metric_snapshot,
    )
    repository_metrics = RepositoryMetricCollectionBuilder().build(
        config,
        pull_request_metrics,
    )
    repo_summary = RepositorySummaryCsvWriter().write(
        config,
        repository_metrics,
        refreshed_period_keys=tuple(period.key for period in raw_snapshot.periods),
    )
    org_metrics = OrganizationMetricCollectionBuilder().build(
        config,
        pull_request_metrics,
    )
    metric_validation = MetricValidationCollectionBuilder().build(
        config,
        raw_snapshot=metric_snapshot,
        pull_request_metrics=pull_request_metrics,
        org_metrics=org_metrics,
    )
    return repo_summary, None, org_metrics, None, metric_validation, None


def _write_org_summary(
    config: RunConfig,
    *,
    org_metrics: OrganizationMetricCollection | None,
    org_metrics_skipped_reason: str | None,
    refreshed_period_keys: tuple[str, ...],
) -> tuple[OrgSummaryWriteResult | None, str | None]:
    if org_metrics is None:
        return None, org_metrics_skipped_reason
    return (
        OrgSummaryWriter().write(
            config,
            org_metrics,
            refreshed_period_keys=refreshed_period_keys,
        ),
        None,
    )


def _build_metric_snapshot(
    *,
    manifest: RunManifest,
    refreshed_snapshot: RawSnapshotWriteResult,
) -> RawSnapshotWriteResult:
    period_index = {
        period.key: period for period in refreshed_snapshot.periods
    }
    for locked_period in manifest.locked_periods:
        period_index.setdefault(
            locked_period.key,
            _build_snapshot_period(manifest.raw_snapshot_root_dir, locked_period),
        )
    return RawSnapshotWriteResult(
        root_dir=manifest.raw_snapshot_root_dir,
        periods=tuple(
            period_index[key]
            for key in sorted(
                period_index.keys(),
                key=lambda period_key: (
                    period_index[period_key].start_date,
                    period_key,
                ),
            )
        ),
    )


def _build_snapshot_period(
    root_dir: Path,
    period: ReportingPeriod,
) -> RawSnapshotPeriod:
    period_dir = root_dir / period.key
    pull_requests_path = period_dir / PULL_REQUEST_SNAPSHOT_FILENAME
    reviews_path = period_dir / PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME
    timeline_events_path = period_dir / PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME
    return RawSnapshotPeriod(
        key=period.key,
        start_date=period.start_date,
        end_date=period.end_date,
        closed=period.closed,
        directory=period_dir,
        pull_requests_path=pull_requests_path,
        pull_request_count=_count_snapshot_rows(pull_requests_path),
        reviews_path=reviews_path,
        review_count=_count_snapshot_rows(reviews_path),
        timeline_events_path=timeline_events_path,
        timeline_event_count=_count_snapshot_rows(timeline_events_path),
    )


def _count_snapshot_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            return max(sum(1 for _ in handle) - 1, 0)
    except OSError:
        return 0


def _reaggregate_pull_requests(
    config: RunConfig,
    *,
    canonical_pull_requests: tuple[PullRequestRecord, ...],
) -> tuple[PullRequestRecord, ...]:
    return tuple(
        pull_request
        for pull_request in canonical_pull_requests
        if (
            anchor_at := config.time_anchor.pull_request_datetime(pull_request)
        )
        is not None
        and anchor_at.date() <= config.collection_window.end_date
    )


def _canonical_inventory_collection(
    config: RunConfig,
    canonical_pull_requests: tuple[PullRequestRecord, ...],
) -> PullRequestCollection:
    return PullRequestCollection(
        window=config.collection_window,
        pull_requests=canonical_pull_requests,
        failures=(),
    )


def _canonical_inventory_repository_count(
    canonical_pull_requests: tuple[PullRequestRecord, ...],
) -> int:
    return len(
        {
            pull_request.repository_full_name
            for pull_request in canonical_pull_requests
        }
    )


def build_run_config(
    *,
    org: str | None = None,
    as_of: date | str | None = None,
    period: PeriodGrain | None = None,
    mode: RunMode | None = None,
    time_anchor: TimeAnchor | None = None,
    include_repos: list[str] | None = None,
    exclude_repos: list[str] | None = None,
    output_dir: Path | None = None,
    backfill_start: str | None = None,
    backfill_end: str | None = None,
) -> RunConfig:
    settings = get_settings()
    payload: dict[str, object] = {
        "org": settings.org if org is None else org,
        "github_token": settings.github_token,
        "as_of": settings.as_of if as_of is None else as_of,
        "period": settings.period if period is None else period,
        "time_anchor": settings.time_anchor if time_anchor is None else time_anchor,
        "mode": settings.mode if mode is None else mode,
        "output_dir": settings.output_dir if output_dir is None else output_dir,
    }
    if include_repos is not None:
        payload["include_repos"] = include_repos
    if exclude_repos is not None:
        payload["exclude_repos"] = exclude_repos
    if backfill_start is not None:
        payload["backfill_start"] = backfill_start
    if backfill_end is not None:
        payload["backfill_end"] = backfill_end
    return RunConfig.model_validate(payload)


def main() -> None:
    app()
