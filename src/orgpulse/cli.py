from __future__ import annotations

import argparse
import sys
from pathlib import Path

from orgpulse.apps.sync import sync
from orgpulse.apps.web.server import make_server
from orgpulse.common.config import DEFAULT_CONFIG_PATH, Config, load_config
from orgpulse.common.errors import OrgpulseError
from orgpulse.libs.github import graphql_requester, resolve_token


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="orgpulse",
        description="Pull request metrics for chosen people and groups in a GitHub organization.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to orgpulse.toml",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("sync", help="fetch merged pull requests into the database")
    serve = commands.add_parser("serve", help="serve the dashboard on 127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        if args.command == "sync":
            saved = sync(
                config,
                graphql_requester(resolve_token()),
                on_repository=lambda name, count: print(f"{name}: {count}"),
            )
            print(f"Saved {saved} pull requests to {config.database}")
        else:
            _serve(config, args.port)
    except OrgpulseError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


def _serve(config: Config, port: int) -> None:
    server = make_server(config, "127.0.0.1", port)
    print(
        f"Serving {config.org} at http://127.0.0.1:{server.server_port} (Ctrl+C to stop)"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
