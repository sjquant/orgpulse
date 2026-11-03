from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from orgpulse.apps.sync import sync
from orgpulse.apps.web.server import export_reports, make_server
from orgpulse.common.config import DEFAULT_CONFIG_PATH, Config, load_config
from orgpulse.common.errors import OrgpulseError
from orgpulse.libs.github import avatar_urls, graphql_requester, resolve_token
from orgpulse.libs.metrics import DISTRIBUTION_METRICS


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
    export = commands.add_parser(
        "export", help="save standalone personal HTML reports or a group ZIP"
    )
    scope = export.add_mutually_exclusive_group()
    scope.add_argument("--person", help="GitHub login; exports one HTML file")
    scope.add_argument(
        "--group",
        help="configured group; exports a ZIP (default: all configured people)",
    )
    export.add_argument(
        "--since",
        type=date.fromisoformat,
        help="inclusive merge-date start (default: configured since)",
    )
    export.add_argument(
        "--until",
        type=date.fromisoformat,
        help="inclusive merge-date end (default: today in configured timezone)",
    )
    export.add_argument("--period", choices=("week", "month"), default="month")
    export.add_argument(
        "--statistic",
        choices=("mean", "median"),
        help="initial statistic for all distribution metrics",
    )
    export.add_argument(
        "--exclude-outliers",
        action="store_true",
        help="initially exclude 1.5×IQR outliers for distribution metrics",
    )
    export.add_argument(
        "--no-avatars",
        action="store_true",
        help="fully offline export, using initials instead of fetching profile photos",
    )
    export.add_argument(
        "--output",
        type=Path,
        required=True,
        help="new .html file for --person, otherwise a new .zip file",
    )
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
        elif args.command == "export":
            _export(config, args)
        else:
            _serve(config, args.port)
    except (OrgpulseError, ValueError, OSError) as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


def _export(config: Config, args: argparse.Namespace) -> None:
    expected_suffix = ".html" if args.person else ".zip"
    if args.output.suffix.lower() != expected_suffix:
        raise ValueError(f"--output must end with {expected_suffix}")
    if args.output.exists():
        raise ValueError(f"Output already exists: {args.output}; choose a new filename")
    query = {
        "from": [(args.since or config.since).isoformat()],
        "until": [(args.until or datetime.now(config.timezone).date()).isoformat()],
        "grain": [args.period],
    }
    if args.person:
        query["person"] = [args.person]
    if args.group:
        query["group"] = [args.group]
    if args.statistic or args.exclude_outliers:
        query["analysis"] = [
            json.dumps(
                {
                    metric: {
                        "statistic": args.statistic
                        or ("mean" if metric == "commits" else "median"),
                        "exclude_outliers": args.exclude_outliers,
                    }
                    for metric in DISTRIBUTION_METRICS
                }
            )
        ]

    def profiles(people: tuple[str, ...]) -> dict[str, str]:
        if args.no_avatars:
            return {}
        try:
            return avatar_urls(
                graphql_requester(resolve_token(), timeout=3, max_attempts=1), people
            )
        except OrgpulseError:
            return {}

    body, _, _ = export_reports(config, query, profiles)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as target:
        target.write(body)
    print(f"Saved {args.output} ({len(body):,} bytes)")


def _serve(config: Config, port: int) -> None:
    server = make_server(
        config,
        "127.0.0.1",
        port,
        sync_runner=lambda progress: sync(
            config,
            graphql_requester(resolve_token()),
            on_repository=progress,
        ),
        avatar_loader=lambda people: avatar_urls(
            graphql_requester(resolve_token(), timeout=3, max_attempts=1), people
        ),
    )
    print(
        f"Serving {config.org} at http://127.0.0.1:{server.server_port} (Ctrl+C to stop)"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
