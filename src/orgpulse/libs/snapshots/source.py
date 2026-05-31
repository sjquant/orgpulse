from __future__ import annotations

import csv
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Literal

from orgpulse.common.errors import AnalysisInputError
from orgpulse.common.models import (
    PeriodGrain,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunManifest,
    TimeAnchor,
)


class _LocalSnapshotPurpose(StrEnum):
    """Enumerate local snapshot consumers with distinct user-facing failures."""

    ANALYSIS = "analysis"
    PERSON_METRICS = "person metrics"
    DASHBOARD = "dashboard"


@dataclass(frozen=True)
class LocalSnapshotSourceResult:
    """Store a validated manifest and its materialized raw snapshot periods."""

    manifest_path: Path
    manifest: RunManifest
    raw_snapshot: RawSnapshotWriteResult


class LocalSnapshotSource:
    """Load validated local snapshot outputs for read-only reports."""

    def load_analysis_source(
        self,
        *,
        org: str,
        output_dir: Path,
        grain: PeriodGrain,
        time_anchor: TimeAnchor,
    ) -> LocalSnapshotSourceResult:
        return self._load_source(
            org=org,
            output_dir=output_dir,
            grain=grain,
            time_anchor=time_anchor,
            purpose=_LocalSnapshotPurpose.ANALYSIS,
        )

    def load_person_metrics_source(
        self,
        *,
        org: str,
        output_dir: Path,
        grain: PeriodGrain,
        time_anchor: TimeAnchor,
        until: date | None,
    ) -> LocalSnapshotSourceResult:
        source = self._load_source(
            org=org,
            output_dir=output_dir,
            grain=grain,
            time_anchor=time_anchor,
            purpose=_LocalSnapshotPurpose.PERSON_METRICS,
        )
        self._validate_person_source_freshness(source.manifest, until=until)
        return source

    def load_dashboard_source(
        self,
        *,
        org: str,
        output_dir: Path,
        since: date,
        until: date,
    ) -> LocalSnapshotSourceResult:
        source = self._load_source(
            org=org,
            output_dir=output_dir,
            grain=PeriodGrain.MONTH,
            time_anchor=TimeAnchor.CREATED_AT,
            purpose=_LocalSnapshotPurpose.DASHBOARD,
        )
        self._validate_dashboard_source_contract(source.manifest)
        self._validate_dashboard_source_freshness(source.manifest, until=until)
        self._validate_dashboard_source_coverage(
            period_index=self._period_index(source.raw_snapshot),
            since=since,
            until=until,
        )
        return source

    def try_load_dashboard_manifest(
        self,
        *,
        org: str,
        output_dir: Path,
    ) -> RunManifest | None:
        manifest_path = self._manifest_path(
            output_dir,
            grain=PeriodGrain.MONTH,
            time_anchor=TimeAnchor.CREATED_AT,
        )
        if not manifest_path.exists():
            return None
        source = self._load_source(
            org=org,
            output_dir=output_dir,
            grain=PeriodGrain.MONTH,
            time_anchor=TimeAnchor.CREATED_AT,
            purpose=_LocalSnapshotPurpose.DASHBOARD,
        )
        self._validate_dashboard_source_contract(source.manifest)
        return source.manifest

    def _load_source(
        self,
        *,
        org: str,
        output_dir: Path,
        grain: PeriodGrain,
        time_anchor: TimeAnchor,
        purpose: _LocalSnapshotPurpose,
    ) -> LocalSnapshotSourceResult:
        manifest_path = self._manifest_path(
            output_dir,
            grain=grain,
            time_anchor=time_anchor,
        )
        manifest = self._load_manifest(
            org=org,
            output_dir=output_dir,
            manifest_path=manifest_path,
            purpose=purpose,
        )
        return LocalSnapshotSourceResult(
            manifest_path=manifest_path,
            manifest=manifest,
            raw_snapshot=self._materialize_raw_snapshot(manifest),
        )

    def _manifest_path(
        self,
        output_dir: Path,
        *,
        grain: PeriodGrain,
        time_anchor: TimeAnchor,
    ) -> Path:
        return (
            output_dir / "manifest" / grain.value / time_anchor.value / "manifest.json"
        )

    def _load_manifest(
        self,
        *,
        org: str,
        output_dir: Path,
        manifest_path: Path,
        purpose: _LocalSnapshotPurpose,
    ) -> RunManifest:
        if not manifest_path.exists():
            raise self._missing_manifest_error(
                purpose=purpose,
                org=org,
                output_dir=output_dir,
                manifest_path=manifest_path,
            )
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise self._unreadable_manifest_error(
                purpose=purpose,
                manifest_path=manifest_path,
            ) from exc

        manifest = RunManifest.model_validate(payload)
        if manifest.target_org.lower() != org.lower():
            raise self._org_mismatch_error(
                purpose=purpose,
                requested_org=org,
                manifest_org=manifest.target_org,
            )
        return manifest

    def _missing_manifest_error(
        self,
        *,
        purpose: _LocalSnapshotPurpose,
        org: str,
        output_dir: Path,
        manifest_path: Path,
    ) -> Exception:
        if purpose is _LocalSnapshotPurpose.DASHBOARD:
            available_manifest_paths = self._available_manifest_paths(output_dir)
            if available_manifest_paths:
                return RuntimeError(
                    "dashboard currently supports only month/created_at local outputs. "
                    f"Found: {', '.join(str(path.relative_to(output_dir)) for path in available_manifest_paths)}"
                )
            return RuntimeError(
                "local manual dashboard source is missing: "
                f"{manifest_path}. Run `orgpulse run --org {org}` first."
            )
        if purpose is _LocalSnapshotPurpose.PERSON_METRICS:
            return AnalysisInputError(
                "person metrics input is missing: "
                f"{manifest_path}. Run `orgpulse run` for this grain and time anchor first."
            )
        return AnalysisInputError(
            "analysis input is missing: "
            f"{manifest_path}. Run `orgpulse run` for this grain and time anchor first."
        )

    def _available_manifest_paths(self, output_dir: Path) -> list[Path]:
        return sorted(output_dir.glob("manifest/*/*/manifest.json"))

    def _unreadable_manifest_error(
        self,
        *,
        purpose: _LocalSnapshotPurpose,
        manifest_path: Path,
    ) -> Exception:
        if purpose is _LocalSnapshotPurpose.DASHBOARD:
            return RuntimeError(f"local manifest is unreadable: {manifest_path}")
        if purpose is _LocalSnapshotPurpose.PERSON_METRICS:
            return AnalysisInputError(
                f"person metrics manifest is unreadable: {manifest_path}"
            )
        return AnalysisInputError(f"analysis manifest is unreadable: {manifest_path}")

    def _org_mismatch_error(
        self,
        *,
        purpose: _LocalSnapshotPurpose,
        requested_org: str,
        manifest_org: str,
    ) -> Exception:
        if purpose is _LocalSnapshotPurpose.DASHBOARD:
            return RuntimeError(
                "local manifest org does not match the requested org: "
                f"expected {requested_org}, found {manifest_org}"
            )
        if purpose is _LocalSnapshotPurpose.PERSON_METRICS:
            return AnalysisInputError(
                "person metrics manifest org does not match the requested org: "
                f"expected {requested_org}, found {manifest_org}"
            )
        return AnalysisInputError(
            "analysis manifest org does not match the requested org: "
            f"expected {requested_org}, found {manifest_org}"
        )

    def _materialize_raw_snapshot(
        self,
        manifest: RunManifest,
    ) -> RawSnapshotWriteResult:
        period_index = {
            period.key: self._snapshot_period(
                manifest.raw_snapshot_root_dir,
                period,
            )
            for period in (*manifest.locked_periods, *manifest.refreshed_periods)
        }
        return RawSnapshotWriteResult(
            root_dir=manifest.raw_snapshot_root_dir,
            periods=tuple(
                period_index[key]
                for key in sorted(
                    period_index,
                    key=lambda period_key: (
                        period_index[period_key].start_date,
                        period_key,
                    ),
                )
            ),
        )

    def _period_index(
        self,
        raw_snapshot: RawSnapshotWriteResult,
    ) -> dict[str, RawSnapshotPeriod]:
        return {period.key: period for period in raw_snapshot.periods}

    def _snapshot_period(
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
            closed=period.closed,
            directory=period_dir,
            pull_requests_path=period_dir / "pull_requests.csv",
            pull_request_count=0,
            reviews_path=period_dir / "pull_request_reviews.csv",
            review_count=0,
            timeline_events_path=period_dir / "pull_request_timeline_events.csv",
            timeline_event_count=0,
        )

    def _validate_person_source_freshness(
        self,
        manifest: RunManifest,
        *,
        until: date | None,
    ) -> None:
        if until is None or until <= manifest.last_successful_run.as_of:
            return
        raise AnalysisInputError(
            "local person metrics source is stale for the requested window: "
            f"latest local as-of is {manifest.last_successful_run.as_of.isoformat()}, "
            f"but --until is {until.isoformat()}."
        )

    def _validate_dashboard_source_contract(
        self,
        manifest: RunManifest,
    ) -> None:
        if manifest.period_grain is not PeriodGrain.MONTH:
            raise RuntimeError(
                "dashboard currently supports only month-grain local outputs."
            )
        if manifest.time_anchor is not TimeAnchor.CREATED_AT:
            raise RuntimeError(
                "dashboard currently supports only created_at local outputs."
            )

    def _validate_dashboard_source_freshness(
        self,
        manifest: RunManifest,
        *,
        until: date,
    ) -> None:
        if until <= manifest.last_successful_run.as_of:
            return
        raise RuntimeError(
            "local source outputs are stale for the requested window: "
            f"latest local as-of is {manifest.last_successful_run.as_of.isoformat()}, "
            f"but --until is {until.isoformat()}."
        )

    def _validate_dashboard_source_coverage(
        self,
        *,
        period_index: dict[str, RawSnapshotPeriod],
        since: date,
        until: date,
    ) -> None:
        expected_keys = self._period_keys_for_window(
            grain=PeriodGrain.MONTH,
            since=since,
            until=until,
        )
        missing_keys = [key for key in expected_keys if key not in period_index]
        if not missing_keys:
            return
        raise RuntimeError(
            "local source outputs do not cover the requested historical window. "
            f"Missing periods: {', '.join(missing_keys)}. "
            "Run a full rebuild or period backfill before rendering this dashboard."
        )

    def _period_keys_for_window(
        self,
        *,
        grain: PeriodGrain,
        since: date,
        until: date,
    ) -> list[str]:
        keys: list[str] = []
        cursor = grain.start_for(since)
        until_period_start = grain.start_for(until)
        while cursor <= until_period_start:
            keys.append(grain.key_for(cursor))
            cursor = grain.end_for(cursor) + timedelta(days=1)
        return keys


def read_snapshot_csv_rows(
    path: Path,
    *,
    missing: Literal["error", "empty"] = "error",
) -> tuple[dict[str, str], ...]:
    """Read normalized raw snapshot rows from disk.

    Args:
        path: CSV file to read.
        missing: Missing-file policy for the requested CSV.

    Returns:
        Parsed CSV rows in file order.
    """

    if not path.exists():
        if missing == "empty":
            return ()
        raise RuntimeError(f"local source snapshot file is missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return tuple(csv.DictReader(handle))


def pull_request_row_key(row: Mapping[str, str]) -> tuple[str, str]:
    """Build a stable pull request identity tuple from a raw row.

    Args:
        row: Raw snapshot row containing repository and pull request identifiers.

    Returns:
        A repository-and-number tuple suitable for grouping related rows.
    """

    return row["repository_full_name"], row["pull_request_number"]
