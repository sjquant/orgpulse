from __future__ import annotations

import csv
import json
from datetime import date, datetime

from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    PULL_REQUEST_REVIEW_FIELDNAMES,
    PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
    NormalizedRawSnapshotWriter,
)
from orgpulse.metrics import (
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.models import (
    CollectionWindow,
    LastSuccessfulRun,
    ManifestWatermarks,
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunConfig,
    RunManifest,
    RunMode,
    RunScope,
)
from orgpulse.output import RepositorySummaryCsvWriter, RunManifestWriter


class TestRunManifestWriter:
    def test_carries_forward_only_locked_periods_for_the_same_run_contract(
        self,
        tmp_path,
    ) -> None:
        """Carry forward locked periods only when the saved manifest matches the current org and repo filter contract."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
            include_repos=("api",),
        )
        raw_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_keys=("2026-04",),
        )
        self._write_complete_period(tmp_path, "2026-03")
        self._write_manifest(
            tmp_path,
            config=config,
            raw_snapshot=raw_snapshot,
            locked_period_keys=("2026-03",),
        )
        writer = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        )

        # When
        carried_manifest = writer.write(
            config,
            self._build_collection(config),
            raw_snapshot,
            repository_count=1,
        ).manifest
        other_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
            include_repos=("web",),
        )
        filtered_manifest = writer.write(
            other_config,
            self._build_collection(other_config),
            raw_snapshot,
            repository_count=1,
        ).manifest

        # Then
        assert [period.key for period in carried_manifest.locked_periods] == ["2026-03"]
        assert filtered_manifest.locked_periods == ()

    def test_treats_equivalent_repo_filters_as_the_same_manifest_contract(
        self,
        tmp_path,
    ) -> None:
        """Treat reordered and owner-qualified repo filters as the same manifest contract."""
        # Given
        previous_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
            include_repos=("api", "acme/web"),
        )
        current_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
            include_repos=("web", "acme/api"),
        )
        raw_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_keys=("2026-04",),
        )
        self._write_complete_period(tmp_path, "2026-03")
        self._write_manifest(
            tmp_path,
            config=previous_config,
            raw_snapshot=raw_snapshot,
            locked_period_keys=("2026-03",),
        )

        # When
        manifest = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        ).write(
            current_config,
            self._build_collection(current_config),
            raw_snapshot,
            repository_count=1,
        ).manifest

        # Then
        assert [period.key for period in manifest.locked_periods] == ["2026-03"]

    def test_treats_org_casing_as_the_same_manifest_contract(
        self,
        tmp_path,
    ) -> None:
        """Treat org names with different casing as the same manifest contract."""
        # Given
        previous_config = self._build_run_config(
            org="Acme",
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        current_config = self._build_run_config(
            org="acme",
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_keys=("2026-04",),
        )
        self._write_complete_period(tmp_path, "2026-03")
        self._write_manifest(
            tmp_path,
            config=previous_config,
            raw_snapshot=raw_snapshot,
            locked_period_keys=("2026-03",),
        )

        # When
        manifest = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        ).write(
            current_config,
            self._build_collection(current_config),
            raw_snapshot,
            repository_count=1,
        ).manifest

        # Then
        assert [period.key for period in manifest.locked_periods] == ["2026-03"]

    def test_promotes_previous_refreshed_periods_once_they_close(
        self,
        tmp_path,
    ) -> None:
        """Promote a previously refreshed open period into locked periods after the next period begins."""
        # Given
        previous_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        current_config = self._build_run_config(
            as_of="2026-05-18",
            output_dir=tmp_path,
        )
        self._write_complete_period(tmp_path, "2026-04")
        raw_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_keys=("2026-05",),
        )
        self._write_manifest(
            tmp_path,
            config=previous_config,
            raw_snapshot=RawSnapshotWriteResult(
                root_dir=tmp_path / "raw" / "month",
                periods=(
                    self._build_raw_snapshot_period(tmp_path, "2026-04"),
                ),
            ),
            locked_period_keys=(),
        )

        # When
        manifest = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-05-18T00:00:00+00:00")
        ).write(
            current_config,
            self._build_collection(current_config),
            raw_snapshot,
            repository_count=1,
        ).manifest

        # Then
        assert [period.key for period in manifest.locked_periods] == ["2026-04"]

    def test_requires_complete_raw_snapshot_files_before_locking_a_period(
        self,
        tmp_path,
    ) -> None:
        """Require all raw snapshot CSVs to exist before a period is carried forward as locked."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_keys=("2026-04",),
        )
        incomplete_period_dir = tmp_path / "raw" / "month" / "2026-03"
        incomplete_period_dir.mkdir(parents=True)
        (incomplete_period_dir / "pull_requests.csv").write_text("", encoding="utf-8")
        self._write_manifest(
            tmp_path,
            config=config,
            raw_snapshot=raw_snapshot,
            locked_period_keys=("2026-03",),
        )

        # When
        manifest = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        ).write(
            config,
            self._build_collection(config),
            raw_snapshot,
            repository_count=1,
        ).manifest

        # Then
        assert manifest.locked_periods == ()

    def test_rejects_truncated_snapshot_files_when_locking_a_period(
        self,
        tmp_path,
    ) -> None:
        """Reject a locked period when any required raw snapshot CSV is truncated below its header row."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_keys=("2026-04",),
        )
        self._write_complete_period(tmp_path, "2026-03")
        truncated_reviews = (
            tmp_path / "raw" / "month" / "2026-03" / "pull_request_reviews.csv"
        )
        truncated_reviews.write_text("", encoding="utf-8")
        self._write_manifest(
            tmp_path,
            config=config,
            raw_snapshot=raw_snapshot,
            locked_period_keys=("2026-03",),
        )

        # When
        manifest = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        ).write(
            config,
            self._build_collection(config),
            raw_snapshot,
            repository_count=1,
        ).manifest

        # Then
        assert manifest.locked_periods == ()

    def test_writes_manifests_under_a_grain_scoped_path(
        self,
        tmp_path,
    ) -> None:
        """Write separate manifest files for different reporting grains in the same output directory."""
        # Given
        monthly_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        weekly_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
            period="week",
        )
        monthly_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_grain="month",
            period_keys=("2026-04",),
        )
        weekly_snapshot = self._build_raw_snapshot(
            tmp_path,
            period_grain="week",
            period_keys=("2026-W16",),
        )
        writer = RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
        )

        # When
        monthly_result = writer.write(
            monthly_config,
            self._build_collection(monthly_config),
            monthly_snapshot,
            repository_count=1,
        )
        weekly_result = writer.write(
            weekly_config,
            self._build_collection(weekly_config),
            weekly_snapshot,
            repository_count=1,
        )

        # Then
        assert monthly_result.path == tmp_path / "manifest" / "month" / "manifest.json"
        assert weekly_result.path == tmp_path / "manifest" / "week" / "manifest.json"
        assert monthly_result.path.exists()
        assert weekly_result.path.exists()

    def _build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for manifest tests."""
        return RunConfig.model_validate({"org": "acme", **overrides})

    def _build_collection(self, config: RunConfig) -> PullRequestCollection:
        """Build the minimal empty collection for manifest writer tests."""
        return PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=config.collection_window.start_date,
                end_date=config.collection_window.end_date,
            ),
            pull_requests=(),
            failures=(),
        )

    def _build_raw_snapshot(
        self,
        tmp_path,
        *,
        period_grain: str = "month",
        period_keys: tuple[str, ...],
    ) -> RawSnapshotWriteResult:
        """Build a raw snapshot result with deterministic period metadata."""
        periods = [
            self._build_raw_snapshot_period(
                tmp_path,
                period_key,
                period_grain=period_grain,
            )
            for period_key in period_keys
        ]
        for period_key in period_keys:
            self._write_complete_period(tmp_path, period_key, period_grain=period_grain)
        return RawSnapshotWriteResult(
            root_dir=tmp_path / "raw" / period_grain,
            periods=tuple(periods),
        )

    def _build_raw_snapshot_period(
        self,
        tmp_path,
        period_key: str,
        *,
        period_grain: str = "month",
    ) -> RawSnapshotPeriod:
        """Build deterministic raw snapshot period metadata for a period key."""
        period_dir = tmp_path / "raw" / period_grain / period_key
        period_dir.mkdir(parents=True, exist_ok=True)
        start_date, end_date = self._period_dates(period_key)
        return RawSnapshotPeriod(
            key=period_key,
            start_date=start_date,
            end_date=end_date,
            directory=period_dir,
            pull_requests_path=period_dir / "pull_requests.csv",
            pull_request_count=0,
            reviews_path=period_dir / "pull_request_reviews.csv",
            review_count=0,
            timeline_events_path=period_dir / "pull_request_timeline_events.csv",
            timeline_event_count=0,
        )

    def _write_complete_period(
        self,
        tmp_path,
        period_key: str,
        *,
        period_grain: str = "month",
    ) -> None:
        """Write the full set of raw snapshot CSV files for a period directory."""
        period_dir = tmp_path / "raw" / period_grain / period_key
        period_dir.mkdir(parents=True, exist_ok=True)
        for filename, header in (
            ("pull_requests.csv", ",".join(PULL_REQUEST_FIELDNAMES)),
            ("pull_request_reviews.csv", ",".join(PULL_REQUEST_REVIEW_FIELDNAMES)),
            (
                "pull_request_timeline_events.csv",
                ",".join(PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES),
            ),
        ):
            (period_dir / filename).write_text(f"{header}\n", encoding="utf-8")

    def _write_manifest(
        self,
        tmp_path,
        *,
        config: RunConfig,
        raw_snapshot: RawSnapshotWriteResult,
        locked_period_keys: tuple[str, ...],
    ) -> None:
        """Write a previous manifest file that can be reused across runs."""
        locked_periods = tuple(
            ReportingPeriod(
                grain=config.period,
                start_date=self._period_dates(period_key)[0],
                end_date=self._period_dates(period_key)[1],
                key=period_key,
                closed=True,
            )
            for period_key in locked_period_keys
        )
        manifest = RunManifest(
            target_org=config.org,
            period_grain=config.period,
            include_repos=config.include_repos,
            exclude_repos=config.exclude_repos,
            raw_snapshot_root_dir=raw_snapshot.root_dir,
            refreshed_periods=raw_snapshot.periods,
            locked_periods=locked_periods,
            watermarks=ManifestWatermarks(
                collection_window_start_date=config.collection_window.start_date,
                collection_window_end_date=config.collection_window.end_date,
                latest_refreshed_period_end_date=None,
                latest_locked_period_end_date=None,
            ),
            last_successful_run=LastSuccessfulRun(
                completed_at=datetime.fromisoformat("2026-04-17T00:00:00+00:00"),
                as_of=config.as_of,
                mode=RunMode.INCREMENTAL,
                refresh_scope=config.refresh_scope,
                repository_count=1,
                pull_request_count=0,
            ),
        )
        manifest_path = tmp_path / "manifest" / config.period.value / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest.model_dump(mode="json")),
            encoding="utf-8",
        )

    def _period_dates(self, period_key: str) -> tuple[date, date]:
        """Build deterministic period boundary dates for month and week test keys."""
        if "-W" in period_key:
            year, week = period_key.split("-W", 1)
            start_date = datetime.fromisocalendar(int(year), int(week), 1).date()
            end_date = datetime.fromisocalendar(int(year), int(week), 7).date()
            return start_date, end_date
        start_date = datetime.fromisoformat(f"{period_key}-01T00:00:00").date()
        end_date = datetime.fromisoformat(f"{period_key}-28T00:00:00").date()
        return start_date, end_date


class TestRepositorySummaryCsvWriter:
    def test_writes_deterministic_repo_summary_rows_per_period(
        self,
        tmp_path,
    ) -> None:
        """Write one deterministic repo-summary CSV row per repository in the reporting period."""
        # Given
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/web",
                    number=31,
                    title="Ship dashboard layout",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-08T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-09T13:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-09T13:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-09T12:00:00"),
                    additions=20,
                    deletions=10,
                    changed_files=4,
                    commits=5,
                    html_url="https://example.test/pr/31",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=601,
                            state="APPROVED",
                            author_login="reviewer-b",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-08T15:00:00"
                            ),
                            commit_id="commit-601",
                        ),
                    ),
                ),
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=21,
                    title="Ship API endpoint",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    additions=10,
                    deletions=4,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/21",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=501,
                            state="APPROVED",
                            author_login="reviewer-a",
                            submitted_at=datetime.fromisoformat(
                                "2026-04-05T12:00:00"
                            ),
                            commit_id="commit-501",
                        ),
                    ),
                ),
            ),
        )
        pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            config,
            raw_snapshot,
        )
        repository_metrics = RepositoryMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )

        # When
        result = RepositorySummaryCsvWriter().write(
            config,
            repository_metrics,
            refreshed_period_keys=tuple(period.key for period in raw_snapshot.periods),
        )

        # Then
        assert result.root_dir == tmp_path / "repo_summary" / "month"
        assert [period.key for period in result.periods] == ["2026-04"]
        assert result.periods[0].repository_count == 2
        rows = self._read_rows(result.periods[0].path)
        assert [row["repository_full_name"] for row in rows] == [
            "acme/api",
            "acme/web",
        ]
        assert rows[0]["period_key"] == "2026-04"
        assert rows[0]["period_closed"] == "false"
        assert rows[0]["pull_request_count"] == "1"
        assert rows[0]["merged_pull_request_count"] == "1"
        assert rows[0]["active_author_count"] == "1"
        assert rows[0]["time_to_merge_total_seconds"] == "97200"
        assert rows[0]["time_to_first_review_total_seconds"] == "10800"
        assert rows[1]["time_to_merge_total_seconds"] == "97200"
        assert rows[1]["time_to_first_review_total_seconds"] == "21600"

    def test_preserves_empty_backfill_periods_as_header_only_csvs(
        self,
        tmp_path,
    ) -> None:
        """Preserve explicit empty backfill periods by writing header-only repo summary CSVs."""
        # Given
        config = self._build_run_config(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(config, pull_requests=())
        pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            config,
            raw_snapshot,
        )
        repository_metrics = RepositoryMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )

        # When
        result = RepositorySummaryCsvWriter().write(
            config,
            repository_metrics,
            refreshed_period_keys=tuple(period.key for period in raw_snapshot.periods),
        )

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert [period.repository_count for period in result.periods] == [0, 0]
        assert all(period.path.exists() for period in result.periods)
        assert all(self._read_rows(period.path) == [] for period in result.periods)

    def test_prunes_stale_period_exports_on_full_rerun(
        self,
        tmp_path,
    ) -> None:
        """Prune stale repo summary period directories when a full rerun no longer emits them."""
        # Given
        stale_period_dir = tmp_path / "repo_summary" / "month" / "2026-03"
        stale_period_dir.mkdir(parents=True)
        (stale_period_dir / "repo_summary.csv").write_text(
            "stale export\n",
            encoding="utf-8",
        )
        config = self._build_run_config(
            as_of="2026-04-18",
            mode=RunMode.FULL,
            output_dir=tmp_path,
        )
        raw_snapshot = self._write_raw_snapshot(
            config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=21,
                    title="Ship API endpoint",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    additions=10,
                    deletions=4,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/21",
                ),
            ),
        )
        pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            config,
            raw_snapshot,
        )
        repository_metrics = RepositoryMetricCollectionBuilder().build(
            config,
            pull_request_metrics,
        )

        # When
        result = RepositorySummaryCsvWriter().write(
            config,
            repository_metrics,
            refreshed_period_keys=tuple(period.key for period in raw_snapshot.periods),
        )

        # Then
        assert stale_period_dir.exists() is False
        assert [period.key for period in result.periods] == ["2026-04"]
        assert result.periods[0].path.exists()

    def test_writes_only_refreshed_periods_and_preserves_locked_exports(
        self,
        tmp_path,
    ) -> None:
        """Write only refreshed periods during incremental runs and leave locked exports untouched."""
        # Given
        previous_config = self._build_run_config(
            as_of="2026-03-18",
            output_dir=tmp_path,
        )
        previous_raw_snapshot = self._write_raw_snapshot(
            previous_config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=11,
                    title="Close March work",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-03-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-03-14T12:00:00"),
                    closed_at=datetime.fromisoformat("2026-03-14T12:00:00"),
                    merged_at=datetime.fromisoformat("2026-03-14T12:00:00"),
                    additions=12,
                    deletions=3,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/11",
                ),
            ),
        )
        previous_pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            previous_config,
            previous_raw_snapshot,
        )
        previous_repository_metrics = RepositoryMetricCollectionBuilder().build(
            previous_config,
            previous_pull_request_metrics,
        )
        RepositorySummaryCsvWriter().write(
            previous_config,
            previous_repository_metrics,
            refreshed_period_keys=tuple(
                period.key for period in previous_raw_snapshot.periods
            ),
        )
        locked_repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "2026-03" / "repo_summary.csv"
        )
        locked_repo_summary_csv = locked_repo_summary_path.read_text(encoding="utf-8")
        current_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        current_raw_snapshot = self._write_raw_snapshot(
            current_config,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/web",
                    number=21,
                    title="Open April work",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-09T10:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-12T11:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-12T11:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-12T11:00:00"),
                    additions=18,
                    deletions=4,
                    changed_files=3,
                    commits=3,
                    html_url="https://example.test/pr/21",
                ),
            ),
        )
        repository_metrics = RepositoryMetricCollectionBuilder().build(
            current_config,
            PullRequestMetricCollectionBuilder().build(
                current_config,
                RawSnapshotWriteResult(
                    root_dir=current_raw_snapshot.root_dir,
                    periods=(
                        previous_raw_snapshot.periods[0],
                        current_raw_snapshot.periods[0],
                    ),
                ),
            ),
        )

        # When
        result = RepositorySummaryCsvWriter().write(
            current_config,
            repository_metrics,
            refreshed_period_keys=tuple(period.key for period in current_raw_snapshot.periods),
        )

        # Then
        assert [period.key for period in result.periods] == ["2026-04"]
        assert locked_repo_summary_path.read_text(encoding="utf-8") == locked_repo_summary_csv
        assert result.periods[0].path.exists()

    def _build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for repo summary export tests."""
        return RunConfig.model_validate({"org": "acme", **overrides})

    def _write_raw_snapshot(
        self,
        config: RunConfig,
        *,
        pull_requests: tuple[PullRequestRecord, ...],
    ) -> RawSnapshotWriteResult:
        """Write raw snapshot fixtures through the production snapshot writer."""
        return NormalizedRawSnapshotWriter().write(
            config,
            PullRequestCollection(
                window=config.collection_window,
                pull_requests=pull_requests,
                failures=(),
            ),
        )

    def _read_rows(self, path) -> list[dict[str, str]]:
        """Read repo summary CSV rows into dictionaries for assertions."""
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
