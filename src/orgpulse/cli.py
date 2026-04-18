from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from github import Auth, Github
from pydantic import ValidationError

from orgpulse.config import get_settings
from orgpulse.errors import AuthResolutionError, GitHubApiError, OrgTargetingError
from orgpulse.github_auth import GitHubAuthService, resolve_auth_token
from orgpulse.ingestion import (
    PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME,
    PULL_REQUEST_SNAPSHOT_FILENAME,
    PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME,
    GitHubIngestionService,
    NormalizedRawSnapshotWriter,
)
from orgpulse.metrics import (
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
)
from orgpulse.models import (
    ManifestWriteResult,
    OrganizationMetricCollection,
    PeriodGrain,
    PullRequestCollection,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunConfig,
    RunManifest,
    RunMode,
)
from orgpulse.output import RunManifestWriter

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
            help="Run strategy: full rebuild ignores locks, incremental refreshes the current open period, and backfill refreshes an explicit closed-period range. Falls back to ORGPULSE_MODE.",
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
        org_metrics, org_metrics_skipped_reason = _build_org_metrics(
            config,
            manifest=manifest,
            raw_snapshot=raw_snapshot,
            raw_snapshot_skipped_reason=raw_snapshot_skipped_reason,
        )
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
                "org_metrics": None
                if org_metrics is None
                else org_metrics.model_dump(mode="json"),
                "org_metrics_skipped_reason": org_metrics_skipped_reason,
            },
            indent=2,
            sort_keys=True,
        )
    )


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


def _build_org_metrics(
    config: RunConfig,
    *,
    manifest: ManifestWriteResult | None,
    raw_snapshot: RawSnapshotWriteResult | None,
    raw_snapshot_skipped_reason: str | None,
) -> tuple[OrganizationMetricCollection | None, str | None]:
    if raw_snapshot is None or manifest is None:
        return None, raw_snapshot_skipped_reason
    metric_snapshot = _build_metric_snapshot(
        manifest=manifest.manifest,
        refreshed_snapshot=raw_snapshot,
    )
    pull_request_metrics = PullRequestMetricCollectionBuilder().build(
        config,
        metric_snapshot,
    )
    return (
        OrganizationMetricCollectionBuilder().build(config, pull_request_metrics),
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
    root_dir,
    period: ReportingPeriod,
) -> RawSnapshotPeriod:
    period_dir = root_dir / period.key
    return RawSnapshotPeriod(
        key=period.key,
        start_date=period.start_date,
        end_date=period.end_date,
        directory=period_dir,
        pull_requests_path=period_dir / PULL_REQUEST_SNAPSHOT_FILENAME,
        pull_request_count=0,
        reviews_path=period_dir / PULL_REQUEST_REVIEW_SNAPSHOT_FILENAME,
        review_count=0,
        timeline_events_path=period_dir / PULL_REQUEST_TIMELINE_EVENT_SNAPSHOT_FILENAME,
        timeline_event_count=0,
    )


def build_run_config(
    *,
    org: str | None = None,
    as_of: date | str | None = None,
    period: PeriodGrain | None = None,
    mode: RunMode | None = None,
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
