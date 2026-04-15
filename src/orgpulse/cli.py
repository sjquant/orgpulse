from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

from pydantic import ValidationError
import typer

from orgpulse.config import PeriodGrain, RunConfig, RunMode

app = typer.Typer(
    add_completion=False,
    help="Collect GitHub organization metrics and write stable file outputs.",
    no_args_is_help=True,
)


@app.callback()
def callback() -> None:
    """Org-wide GitHub metrics reporting CLI."""


def build_run_config(
    *,
    org: str | None = None,
    period: PeriodGrain | None = None,
    mode: RunMode | None = None,
    include_repos: list[str] | None = None,
    exclude_repos: list[str] | None = None,
    output_dir: Path | None = None,
    backfill_start: str | None = None,
    backfill_end: str | None = None,
) -> RunConfig:
    payload: dict[str, object] = {}
    if org is not None:
        payload["org"] = org
    if period is not None:
        payload["period"] = period
    if mode is not None:
        payload["mode"] = mode
    if include_repos is not None:
        payload["include_repos"] = include_repos
    if exclude_repos is not None:
        payload["exclude_repos"] = exclude_repos
    if output_dir is not None:
        payload["output_dir"] = output_dir
    if backfill_start is not None:
        payload["backfill_start"] = backfill_start
    if backfill_end is not None:
        payload["backfill_end"] = backfill_end
    return RunConfig(**payload)


@app.command("run")
def run_command(
    org: Annotated[
        str | None,
        typer.Option("--org", help="GitHub organization to collect. Falls back to ORGPULSE_ORG."),
    ] = None,
    period: Annotated[
        PeriodGrain | None,
        typer.Option("--period", help="Reporting grain for snapshots and rollups. Falls back to ORGPULSE_PERIOD."),
    ] = None,
    mode: Annotated[
        RunMode | None,
        typer.Option("--mode", help="Run strategy for historical refresh behavior. Falls back to ORGPULSE_MODE."),
    ] = None,
    include_repos: Annotated[
        list[str] | None,
        typer.Option("--repo", help="Restrict collection to a repository. May be provided multiple times."),
    ] = None,
    exclude_repos: Annotated[
        list[str] | None,
        typer.Option("--exclude-repo", help="Exclude a repository from collection. May be provided multiple times."),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", help="Directory where raw snapshots and rollups will be written. Falls back to ORGPULSE_OUTPUT_DIR."),
    ] = None,
    backfill_start: Annotated[
        str | None,
        typer.Option("--backfill-start", help="Inclusive ISO date for backfill mode, for example 2026-01-01."),
    ] = None,
    backfill_end: Annotated[
        str | None,
        typer.Option("--backfill-end", help="Inclusive ISO date for backfill mode, for example 2026-03-31."),
    ] = None,
) -> None:
    try:
        config = build_run_config(
            org=org,
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

    typer.echo(json.dumps(config.model_dump(mode="json"), indent=2, sort_keys=True))


def main() -> None:
    app()
