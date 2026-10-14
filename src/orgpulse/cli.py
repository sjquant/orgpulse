from __future__ import annotations

import webbrowser
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from orgpulse.apps.sync import sync as sync_pull_requests
from orgpulse.apps.web.server import make_server
from orgpulse.common.config import DEFAULT_CONFIG_PATH, load_config
from orgpulse.common.errors import OrgpulseError
from orgpulse.libs.github import graphql_requester, resolve_token

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Pull request metrics for chosen people and groups in a GitHub organization.",
)
console = Console(stderr=True)

ConfigOption = Annotated[
    Path,
    typer.Option(
        "--config",
        "-c",
        envvar="ORGPULSE_CONFIG",
        help="Path to orgpulse.toml.",
    ),
]


@app.command()
def sync(config_path: ConfigOption = DEFAULT_CONFIG_PATH) -> None:
    """Fetch pull requests changed since the last sync into the local database."""

    config = load_config(config_path)
    request = graphql_requester(resolve_token())
    with console.status(f"Syncing {config.org}…"):
        saved = sync_pull_requests(
            config,
            request,
            on_repository=lambda full_name, count: console.print(
                f"  {full_name}: {count} updated"
            ),
        )
    console.print(f"[green]Saved {saved} pull requests to {config.database}[/green]")


@app.command()
def serve(
    config_path: ConfigOption = DEFAULT_CONFIG_PATH,
    host: Annotated[str, typer.Option(help="Interface to bind.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to listen on.")] = 8765,
    open_browser: Annotated[
        bool, typer.Option("--open", help="Open the report in a browser.")
    ] = False,
) -> None:
    """Serve the report UI from the local database."""

    config = load_config(config_path)
    server = make_server(config, host, port)
    url = f"http://{host}:{server.server_port}"
    console.print(f"Serving {config.org} at [bold]{url}[/bold] (Ctrl+C to stop)")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> int:
    try:
        app()
    except OrgpulseError as exc:
        console.print(f"[red]{exc}[/red]")
        return 1
    return 0
