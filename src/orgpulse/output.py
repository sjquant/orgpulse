from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from orgpulse.models import (
    LastSuccessfulRun,
    ManifestWatermarks,
    ManifestWriteResult,
    PullRequestCollection,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunConfig,
    RunManifest,
)

MANIFEST_FILENAME = "manifest.json"


class RunManifestWriter:
    """Persist run metadata that catalogs refreshed and locked raw snapshot periods."""

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._now = now or self._current_time

    def write(
        self,
        config: RunConfig,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        *,
        repository_count: int,
    ) -> ManifestWriteResult:
        manifest = self._build_manifest(
            config=config,
            collection=collection,
            raw_snapshot=raw_snapshot,
            repository_count=repository_count,
        )
        path = self._manifest_path(config.output_dir)
        self._write_manifest_file(path, manifest)
        return ManifestWriteResult(path=path, manifest=manifest)

    def _build_manifest(
        self,
        *,
        config: RunConfig,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        repository_count: int,
    ) -> RunManifest:
        locked_periods = self._build_locked_periods(
            config=config,
            raw_snapshot=raw_snapshot,
        )
        return RunManifest(
            target_org=config.org,
            period_grain=config.period,
            include_repos=config.include_repos,
            exclude_repos=config.exclude_repos,
            raw_snapshot_root_dir=raw_snapshot.root_dir,
            refreshed_periods=raw_snapshot.periods,
            locked_periods=locked_periods,
            watermarks=self._build_watermarks(
                collection=collection,
                raw_snapshot=raw_snapshot,
                locked_periods=locked_periods,
            ),
            last_successful_run=self._build_last_successful_run(
                config=config,
                collection=collection,
                repository_count=repository_count,
            ),
        )

    def _build_locked_periods(
        self,
        *,
        config: RunConfig,
        raw_snapshot: RawSnapshotWriteResult,
    ) -> tuple[ReportingPeriod, ...]:
        existing_periods = self._load_existing_periods(
            config=config,
            root_dir=raw_snapshot.root_dir,
        )
        return tuple(period for period in existing_periods if period.closed)

    def _load_existing_periods(
        self,
        *,
        config: RunConfig,
        root_dir: Path,
    ) -> tuple[ReportingPeriod, ...]:
        if not root_dir.exists():
            return ()
        periods: list[ReportingPeriod] = []
        for child in sorted(root_dir.iterdir(), key=lambda path: path.name):
            if not child.is_dir():
                continue
            period = self._build_period_from_directory(config=config, directory=child)
            if period is None:
                continue
            periods.append(period)
        return tuple(periods)

    def _build_period_from_directory(
        self,
        *,
        config: RunConfig,
        directory: Path,
    ) -> ReportingPeriod | None:
        try:
            start_date = config.period.start_for_key(directory.name)
        except ValueError:
            return None
        end_date = config.period.end_for(start_date)
        return ReportingPeriod(
            grain=config.period,
            start_date=start_date,
            end_date=end_date,
            key=directory.name,
            closed=end_date < config.active_period.start_date,
        )

    def _build_watermarks(
        self,
        *,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        locked_periods: tuple[ReportingPeriod, ...],
    ) -> ManifestWatermarks:
        return ManifestWatermarks(
            collection_window_start_date=collection.window.start_date,
            collection_window_end_date=collection.window.end_date,
            latest_refreshed_period_end_date=self._latest_period_end_date(
                raw_snapshot.periods
            ),
            latest_locked_period_end_date=self._latest_period_end_date(locked_periods),
        )

    def _build_last_successful_run(
        self,
        *,
        config: RunConfig,
        collection: PullRequestCollection,
        repository_count: int,
    ) -> LastSuccessfulRun:
        return LastSuccessfulRun(
            completed_at=self._now(),
            as_of=config.as_of,
            mode=config.mode,
            refresh_scope=config.refresh_scope,
            repository_count=repository_count,
            pull_request_count=len(collection.pull_requests),
        )

    def _latest_period_end_date(
        self,
        periods: Sequence[RawSnapshotPeriod | ReportingPeriod],
    ) -> date | None:
        if not periods:
            return None
        return max(period.end_date for period in periods)

    def _manifest_path(self, output_dir: Path) -> Path:
        return output_dir / MANIFEST_FILENAME

    def _write_manifest_file(self, path: Path, manifest: RunManifest) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                manifest.model_dump(mode="json"),
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")

    def _current_time(self) -> datetime:
        return datetime.now(UTC).replace(microsecond=0)
