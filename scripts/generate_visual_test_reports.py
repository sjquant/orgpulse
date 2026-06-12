from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from orgpulse.apps.dashboard.service import generate_dashboard_report
from orgpulse.apps.person_metrics.export import render_person_metrics_result
from orgpulse.apps.person_metrics.service import (
    PersonExportFormat,
    PersonMetricsService,
    build_person_config,
)
from orgpulse.common.models import (
    PeriodGrain,
    PullRequestCollection,
    PullRequestRecord,
    ReportLocale,
    RunConfig,
    RunMode,
    TimeAnchor,
)
from orgpulse.libs.github.ingestion import NormalizedRawSnapshotWriter
from orgpulse.libs.output_store.run_outputs import RunManifestWriter

DEFAULT_DATA_FILE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "visual_testing"
    / "acme_cloud_demo.json"
)
DEFAULT_OUTPUT_ROOT = Path("/tmp/orgpulse-visual-testing/person-metrics-demo")


def main() -> None:
    args = _parse_args()
    demo = _load_demo(args.data_file)
    paths = _render_reports(
        demo=demo,
        output_root=args.output_root,
        base_name=args.base_name,
        locale=ReportLocale(args.locale),
    )
    _print_summary(paths)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate stable orgpulse visual-testing reports.",
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        default=DEFAULT_DATA_FILE,
        help="JSON fixture containing synthetic pull request data.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Directory where source snapshots and reports will be written.",
    )
    parser.add_argument(
        "--base-name",
        default="acme-cloud-org-dashboard",
        help="Base filename for the organization dashboard outputs.",
    )
    parser.add_argument(
        "--locale",
        choices=[locale.value for locale in ReportLocale],
        default=ReportLocale.EN.value,
        help="HTML report locale.",
    )
    return parser.parse_args()


def _load_demo(data_file: Path) -> dict[str, Any]:
    return json.loads(data_file.read_text(encoding="utf-8"))


def _render_reports(
    *,
    demo: dict[str, Any],
    output_root: Path,
    base_name: str,
    locale: ReportLocale,
) -> dict[str, Path]:
    source_dir = output_root / "source"
    report_dir = output_root / "reports"
    _reset_source_dir(source_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    pull_requests = _pull_requests(demo["pull_requests"])
    config = _run_config(demo, source_dir)
    collection = PullRequestCollection(
        window=config.collection_window,
        pull_requests=pull_requests,
        failures=(),
    )
    raw_snapshot = NormalizedRawSnapshotWriter().write(config, collection)
    RunManifestWriter(
        now=lambda: datetime.combine(config.as_of, datetime.min.time(), tzinfo=UTC)
    ).write(
        config,
        collection,
        raw_snapshot,
        repository_count=int(demo["repository_count"]),
    )
    person_path = _write_person_report(
        demo=demo,
        source_dir=source_dir,
        report_dir=report_dir,
        locale=locale,
    )
    dashboard_outputs = generate_dashboard_report(
        org=str(demo["org"]),
        since=date.fromisoformat(str(demo["since"])),
        until=date.fromisoformat(str(demo["until"])),
        source_output_dir=source_dir,
        output_dir=report_dir,
        base_name=base_name,
        refresh=False,
        distribution_percentile=int(demo["distribution_percentile"]),
        locale=locale,
    )
    return {
        "person_html": person_path,
        "org_html": Path(str(dashboard_outputs["html_path"])),
        "org_json": Path(str(dashboard_outputs["json_path"])),
        "org_csv": Path(str(dashboard_outputs["csv_path"])),
        "source_dir": source_dir,
    }


def _reset_source_dir(source_dir: Path) -> None:
    if source_dir.exists():
        shutil.rmtree(source_dir)


def _pull_requests(rows: list[dict[str, Any]]) -> tuple[PullRequestRecord, ...]:
    return tuple(PullRequestRecord.model_validate(row) for row in rows)


def _run_config(
    demo: dict[str, Any],
    source_dir: Path,
) -> RunConfig:
    return RunConfig(
        org=str(demo["org"]),
        as_of=date.fromisoformat(str(demo["as_of"])),
        period=PeriodGrain.MONTH,
        time_anchor=TimeAnchor.CREATED_AT,
        mode=RunMode.FULL,
        output_dir=source_dir,
    )


def _write_person_report(
    *,
    demo: dict[str, Any],
    source_dir: Path,
    report_dir: Path,
    locale: ReportLocale,
) -> Path:
    config = build_person_config(
        org=str(demo["org"]),
        login=str(demo["person_login"]),
        output_dir=source_dir,
        grain=PeriodGrain.MONTH,
        time_anchor=TimeAnchor.CREATED_AT,
        since=date.fromisoformat(str(demo["since"])),
        until=date.fromisoformat(str(demo["until"])),
        distribution_percentile=int(demo["distribution_percentile"]),
        export_format=PersonExportFormat.HTML,
        locale=locale,
        include_org_trends=True,
    )
    result = PersonMetricsService().extract(config)
    person_path = report_dir / f"{config.login}-person-report.html"
    person_path.write_text(render_person_metrics_result(result), encoding="utf-8")
    return person_path


def _print_summary(paths: dict[str, Path]) -> None:
    print(f"source_dir={paths['source_dir']}")
    print(f"person_html={paths['person_html']}")
    print(f"org_html={paths['org_html']}")
    print(f"org_json={paths['org_json']}")
    print(f"org_csv={paths['org_csv']}")


if __name__ == "__main__":
    main()
