from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn

from orgpulse.apps.analysis.export import render_analysis_result
from orgpulse.apps.analysis.service import (
    AnalysisExportFormat,
    AnalysisGrouping,
    AnalysisService,
    build_analysis_config,
)
from orgpulse.apps.person_metrics.export import render_person_metrics_result
from orgpulse.apps.person_metrics.service import (
    PersonExportFormat,
    PersonMetricsService,
    build_person_config,
)
from orgpulse.apps.reaggregation.service import execute_reaggregation
from orgpulse.common.config import build_run_config, get_settings
from orgpulse.common.errors import (
    AnalysisInputError,
    AuthResolutionError,
    GitHubApiError,
    OrgTargetingError,
)
from orgpulse.common.models import (
    PeriodGrain,
    ReportLocale,
    RunMode,
    TimeAnchor,
)
from orgpulse.libs.github.ingestion import PullRequestFetchProgress
from orgpulse.libs.snapshots.refresh import execute_source_refresh

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
    """Collect GitHub data and write normalized run outputs."""

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
    progress_reporter = _PullRequestProgressReporter()
    try:
        result = execute_source_refresh(
            config,
            progress_callback=progress_reporter,
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
    finally:
        progress_reporter.stop()

    typer.echo(
        json.dumps(
            result.to_payload(),
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
    """Rebuild stored raw data into alternate grains or anchors locally."""

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

    result = execute_reaggregation(config)
    if result is None:
        typer.echo(
            "orgpulse: canonical raw inventory is missing or does not match the requested org and repository scope",
            err=True,
        )
        raise typer.Exit(code=1)

    typer.echo(
        json.dumps(
            result.to_payload(),
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
            "--period",
            help="Snapshot period grain to analyze. Falls back to ORGPULSE_PERIOD.",
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
    distribution_percentile: Annotated[
        int | None,
        typer.Option(
            "--distribution-percentile",
            help="Upper-tail percentile retained for distribution-based metrics. Use 95, 99, or 100.",
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
            "--source-output-dir",
            help="Directory containing source local orgpulse outputs. Falls back to ORGPULSE_OUTPUT_DIR.",
        ),
    ] = None,
    export_format: Annotated[
        AnalysisExportFormat | None,
        typer.Option(
            "--format",
            help="Analysis export format written to stdout.",
        ),
    ] = None,
    locale: Annotated[
        ReportLocale | None,
        typer.Option(
            "--locale",
            help="HTML report locale. Falls back to ORGPULSE_LOCALE.",
        ),
    ] = None,
) -> None:
    """Analyze stored raw data with explicit grouping and export controls."""

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
            distribution_percentile=distribution_percentile,
            export_format=export_format,
            locale=locale,
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


@app.command("person")
def person_command(
    login: Annotated[
        str | None,
        typer.Argument(
            help="GitHub login whose local person metrics should be extracted.",
            metavar="LOGIN",
        ),
    ] = None,
    login_option: Annotated[
        str | None,
        typer.Option(
            "--login",
            help="GitHub login whose local person metrics should be extracted.",
        ),
    ] = None,
    org: Annotated[
        str | None,
        typer.Option(
            "--org",
            help="GitHub organization whose local outputs should be analyzed. Falls back to ORGPULSE_ORG.",
        ),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Inclusive ISO date lower bound for authored PR anchors and submitted reviews.",
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help="Inclusive ISO date upper bound for authored PR anchors and submitted reviews.",
        ),
    ] = None,
    distribution_percentile: Annotated[
        int | None,
        typer.Option(
            "--distribution-percentile",
            help="Upper-tail percentile retained for latency metrics. Use 95, 99, or 100.",
        ),
    ] = None,
    time_anchor: Annotated[
        TimeAnchor | None,
        typer.Option(
            "--time-anchor",
            "--pr-time-anchor",
            help="Timestamp used to filter authored pull requests. Reviews use submitted_at. Falls back to ORGPULSE_TIME_ANCHOR.",
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "--source-output-dir",
            help="Directory containing source local orgpulse outputs. Falls back to ORGPULSE_OUTPUT_DIR.",
        ),
    ] = None,
    output_file: Annotated[
        Path | None,
        typer.Option(
            "--output-file",
            help="Write the rendered person metrics output to this file instead of stdout.",
        ),
    ] = None,
    include_repos: Annotated[
        list[str] | None,
        typer.Option(
            "--repo",
            help="Restrict person metrics to a repository already present in local outputs. May be provided multiple times.",
        ),
    ] = None,
    exclude_repos: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude-repo",
            help="Exclude a repository from person metrics. May be provided multiple times.",
        ),
    ] = None,
    include_org_trends: Annotated[
        bool,
        typer.Option(
            "--include-org-trends",
            help="Include scoped org-wide weekly and monthly trend rows in person outputs.",
        ),
    ] = False,
    export_format: Annotated[
        PersonExportFormat | None,
        typer.Option(
            "--format",
            help="Person metrics export format written to stdout or --output-file.",
        ),
    ] = None,
    locale: Annotated[
        ReportLocale | None,
        typer.Option(
            "--locale",
            help="HTML report locale. Falls back to ORGPULSE_LOCALE.",
        ),
    ] = None,
) -> None:
    """Extract local performance metrics for one GitHub login."""

    try:
        resolved_login = _resolve_person_login(login, login_option)
        config = build_person_config(
            org=org,
            login=resolved_login,
            output_dir=output_dir,
            time_anchor=time_anchor,
            since=since,
            until=until,
            distribution_percentile=distribution_percentile,
            export_format=export_format,
            locale=locale,
            include_repos=include_repos,
            exclude_repos=exclude_repos,
            include_org_trends=include_org_trends,
        )
    except (ValidationError, ValueError) as exc:
        typer.echo(f"orgpulse: invalid person metrics configuration\n{exc}", err=True)
        raise typer.Exit(code=2) from exc

    try:
        result = PersonMetricsService().extract(config)
    except AnalysisInputError as exc:
        typer.echo(f"orgpulse: person metrics input failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc

    _write_person_output(render_person_metrics_result(result), output_file)


def _resolve_person_login(
    login_arg: str | None,
    login_option: str | None,
) -> str:
    if login_arg is not None and login_option is not None and login_arg != login_option:
        raise ValueError(
            "person login argument and --login must match when both are provided"
        )
    login = login_option if login_option is not None else login_arg
    if login is None:
        raise ValueError("person login is required as an argument or --login")
    return login


def _write_person_output(
    rendered_output: str,
    output_file: Path | None,
) -> None:
    if output_file is None:
        typer.echo(rendered_output)
        return
    output_file = output_file.expanduser()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(rendered_output, encoding="utf-8")


class _PullRequestProgressReporter:
    """Render pull request fetch progress for interactive and captured CLI output."""

    def __init__(self) -> None:
        self._console = Console(stderr=True)
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None

    def __call__(
        self,
        progress: PullRequestFetchProgress,
    ) -> None:
        if not self._console.is_terminal:
            _echo_pull_request_fetch_progress(progress)
            return
        self._render_live_progress(progress)

    def _render_live_progress(
        self,
        progress: PullRequestFetchProgress,
    ) -> None:
        if self._progress is None:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold]orgpulse[/] pull request download"),
                BarColumn(),
                TextColumn("{task.percentage:>5.1f}%"),
                TextColumn("{task.fields[message]}"),
                console=self._console,
                transient=False,
            )
            self._progress.start()
        if self._task_id is None:
            self._task_id = self._progress.add_task(
                "pull request download",
                total=max(progress.total_repositories, 1),
                completed=self._completed_units(progress),
                message=self._progress_message(progress),
            )
        else:
            self._progress.update(
                self._task_id,
                total=max(progress.total_repositories, 1),
                completed=self._completed_units(progress),
                message=self._progress_message(progress),
            )
        if progress.phase == "finish":
            self.stop()

    def _completed_units(
        self,
        progress: PullRequestFetchProgress,
    ) -> int:
        if progress.total_repositories == 0:
            return 1
        return progress.completed_repositories

    def _progress_message(
        self,
        progress: PullRequestFetchProgress,
    ) -> str:
        repository_progress = (
            f"{progress.completed_repositories}/{progress.total_repositories}"
        )
        if progress.phase == "start":
            return f"{repository_progress} repositories complete"
        if progress.phase == "repository_started":
            return (
                f"{repository_progress} fetching "
                f"{progress.repository_full_name or 'repository'}"
            )
        if progress.phase == "repository_completed":
            return (
                f"{repository_progress} done "
                f"{progress.repository_full_name or 'repository'} "
                f"+{progress.fetched_pull_request_count} PRs "
                f"({progress.repository_pull_request_count} total)"
            )
        if progress.phase == "repository_failed":
            return (
                f"{repository_progress} failed "
                f"{progress.repository_full_name or 'repository'}; "
                f"{progress.failure_count} failures"
            )
        if progress.phase == "finish":
            return f"{repository_progress} complete; {progress.failure_count} failures"
        return repository_progress

    def stop(self) -> None:
        if self._progress is None:
            return
        self._progress.stop()
        self._progress = None
        self._task_id = None


def _echo_pull_request_fetch_progress(
    progress: PullRequestFetchProgress,
) -> None:
    if progress.phase == "start":
        typer.echo(
            "orgpulse: pull request download "
            f"{progress.completed_repositories}/{progress.total_repositories} "
            f"({progress.progress_percent:.1f}%) repositories complete",
            err=True,
        )
        return
    if progress.phase == "repository_started":
        typer.echo(
            "orgpulse: pull request download "
            f"{progress.completed_repositories}/{progress.total_repositories} "
            f"({progress.progress_percent:.1f}%) fetching "
            f"{progress.repository_full_name}",
            err=True,
        )
        return
    if progress.phase == "repository_completed":
        typer.echo(
            "orgpulse: pull request download "
            f"{progress.completed_repositories}/{progress.total_repositories} "
            f"({progress.progress_percent:.1f}%) done "
            f"{progress.repository_full_name} "
            f"+{progress.fetched_pull_request_count} PRs "
            f"({progress.repository_pull_request_count} total)",
            err=True,
        )
        return
    if progress.phase == "repository_failed":
        typer.echo(
            "orgpulse: pull request download "
            f"{progress.completed_repositories}/{progress.total_repositories} "
            f"({progress.progress_percent:.1f}%) failed "
            f"{progress.repository_full_name}; "
            f"{progress.failure_count} failures",
            err=True,
        )
        return
    if progress.phase == "finish":
        typer.echo(
            "orgpulse: pull request download "
            f"{progress.completed_repositories}/{progress.total_repositories} "
            f"({progress.progress_percent:.1f}%) complete; "
            f"{progress.failure_count} failures",
            err=True,
        )


@app.command("dashboard")
def dashboard_command(
    since: Annotated[
        str,
        typer.Option(
            "--since",
            help="Inclusive ISO date lower bound for the dashboard window.",
        ),
    ],
    until: Annotated[
        str,
        typer.Option(
            "--until",
            help="Inclusive ISO date upper bound for the dashboard window.",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            help="Directory where the dashboard JSON, CSV, and HTML exports will be written.",
        ),
    ],
    org: Annotated[
        str | None,
        typer.Option(
            "--org",
            help="GitHub organization whose local outputs should be rendered into a dashboard. Falls back to ORGPULSE_ORG.",
        ),
    ] = None,
    source_output_dir: Annotated[
        Path | None,
        typer.Option(
            "--source-output-dir",
            help="Directory containing local orgpulse outputs used as the dashboard source. Falls back to ORGPULSE_OUTPUT_DIR. Currently supports month/created_at sources only.",
        ),
    ] = None,
    base_name: Annotated[
        str | None,
        typer.Option(
            "--base-name",
            help="Base filename for the rendered dashboard artifacts. Defaults to <org>-created-at-since-<since>.",
        ),
    ] = None,
    refresh: Annotated[
        bool,
        typer.Option(
            "--refresh/--no-refresh",
            help="Refresh the current open source period incrementally before rendering.",
        ),
    ] = True,
    distribution_percentile: Annotated[
        int,
        typer.Option(
            "--distribution-percentile",
            help="Upper-tail percentile retained for distribution-based metrics. Use 95, 99, or 100.",
        ),
    ] = 100,
    locale: Annotated[
        ReportLocale | None,
        typer.Option(
            "--locale",
            help="HTML report locale. Falls back to ORGPULSE_LOCALE.",
        ),
    ] = None,
) -> None:
    """Build a dashboard report from stored local outputs."""

    from orgpulse.apps.dashboard.service import generate_dashboard_report

    try:
        resolved_org, resolved_source_output_dir = _resolve_dashboard_source(
            org=org,
            source_output_dir=source_output_dir,
        )
        resolved_since = date.fromisoformat(since)
        resolved_until = date.fromisoformat(until)
        if resolved_since > resolved_until:
            raise ValueError("--since must be on or before --until")
        _validate_dashboard_distribution_percentile(distribution_percentile)
    except (ValidationError, ValueError) as exc:
        typer.echo(f"orgpulse: invalid dashboard configuration\n{exc}", err=True)
        raise typer.Exit(code=2) from exc

    try:
        result = generate_dashboard_report(
            org=resolved_org,
            since=resolved_since,
            until=resolved_until,
            source_output_dir=resolved_source_output_dir,
            output_dir=output_dir,
            base_name=_default_dashboard_base_name(
                org=resolved_org,
                since=resolved_since,
                base_name=base_name,
            ),
            refresh=refresh,
            distribution_percentile=distribution_percentile,
            locale=locale,
        )
    except RuntimeError as exc:
        typer.echo(f"orgpulse: dashboard generation failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )


@app.command("dashboard-render")
def dashboard_render_command(
    input_json: Annotated[
        Path,
        typer.Option(
            "--input-json",
            help="Existing dashboard JSON payload to render.",
        ),
    ],
    output_html: Annotated[
        Path,
        typer.Option(
            "--output-html",
            help="Destination HTML path for the rendered dashboard.",
        ),
    ],
    distribution_percentile: Annotated[
        int,
        typer.Option(
            "--distribution-percentile",
            help="Upper-tail percentile retained for distribution-based metrics. Use 95, 99, or 100.",
        ),
    ] = 100,
    locale: Annotated[
        ReportLocale | None,
        typer.Option(
            "--locale",
            help="HTML report locale. Falls back to ORGPULSE_LOCALE.",
        ),
    ] = None,
) -> None:
    """Render dashboard HTML from a previously generated JSON payload."""

    from orgpulse.libs.reporting.dashboard_html import render_dashboard_artifact

    try:
        _validate_dashboard_distribution_percentile(distribution_percentile)
        result = render_dashboard_artifact(
            input_json=input_json,
            output_html=output_html,
            distribution_percentile=distribution_percentile,
            locale=locale,
        )
    except (OSError, json.JSONDecodeError, RuntimeError, ValueError) as exc:
        typer.echo(f"orgpulse: dashboard render failed\n{exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )


def _default_dashboard_base_name(
    *,
    org: str,
    since: date,
    base_name: str | None,
) -> str:
    if base_name is not None:
        return base_name
    return f"{org}-created-at-since-{since.isoformat()}"


def _resolve_dashboard_source(
    *,
    org: str | None,
    source_output_dir: Path | None,
) -> tuple[str, Path]:
    settings = get_settings()
    resolved_org = settings.org if org is None else org
    if resolved_org is None:
        raise ValueError("dashboard rendering requires --org or ORGPULSE_ORG")
    return (
        resolved_org,
        settings.output_dir if source_output_dir is None else source_output_dir,
    )


def _validate_dashboard_distribution_percentile(value: int) -> None:
    if value not in {95, 99, 100}:
        raise ValueError("distribution percentile must be one of 95, 99, or 100")


def main() -> None:
    """Run the Typer CLI application."""

    app()
