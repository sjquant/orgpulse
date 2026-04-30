from __future__ import annotations

# ruff: noqa: F403,F405
from ..helpers.output import *


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
                root_dir=tmp_path / "raw" / "month" / "created_at",
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
        incomplete_period_dir = tmp_path / "raw" / "month" / "created_at" / "2026-03"
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
            tmp_path / "raw" / "month" / "created_at" / "2026-03" / "pull_request_reviews.csv"
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
        assert monthly_result.path == tmp_path / "manifest" / "month" / "created_at" / "manifest.json"
        assert weekly_result.path == tmp_path / "manifest" / "week" / "created_at" / "manifest.json"
        assert monthly_result.index_path == tmp_path / "manifest" / "month" / "created_at" / "index.json"
        assert monthly_result.readme_path == tmp_path / "manifest" / "month" / "created_at" / "README.md"
        assert monthly_result.path.exists()
        assert weekly_result.path.exists()
        assert json.loads(monthly_result.index_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "history": {
                "locked_periods": [],
                "refreshed_periods": [
                    {
                        "closed": False,
                        "end_date": "2026-04-28",
                        **_expected_period_state(
                            closed=False,
                            observed_through_date="2026-04-18",
                        ),
                        "key": "2026-04",
                        "start_date": "2026-04-01",
                    }
                ],
            },
            "include_repos": [],
            "latest": {
                "as_of": "2026-04-18",
                "completed_at": "2026-04-18T00:00:00+00:00",
                "manifest_path": "manifest.json",
                "mode": "incremental",
                "refresh_scope": "open_period",
            },
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
            "target_org": "acme",
            "watermarks": {
                "collection_window_end_date": "2026-04-18",
                "collection_window_start_date": "2026-04-01",
                "latest_locked_period_end_date": None,
                "latest_refreshed_period_end_date": "2026-04-28",
            },
        }

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
        time_anchor: str = "created_at",
        period_keys: tuple[str, ...],
    ) -> RawSnapshotWriteResult:
        """Build a raw snapshot result with deterministic period metadata."""
        periods = [
            self._build_raw_snapshot_period(
                tmp_path,
                period_key,
                period_grain=period_grain,
                time_anchor=time_anchor,
            )
            for period_key in period_keys
        ]
        for period_key in period_keys:
            self._write_complete_period(
                tmp_path,
                period_key,
                period_grain=period_grain,
                time_anchor=time_anchor,
            )
        return RawSnapshotWriteResult(
            root_dir=tmp_path / "raw" / period_grain / time_anchor,
            periods=tuple(periods),
        )

    def _build_raw_snapshot_period(
        self,
        tmp_path,
        period_key: str,
        *,
        period_grain: str = "month",
        time_anchor: str = "created_at",
    ) -> RawSnapshotPeriod:
        """Build deterministic raw snapshot period metadata for a period key."""
        period_dir = tmp_path / "raw" / period_grain / time_anchor / period_key
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
        time_anchor: str = "created_at",
    ) -> None:
        """Write the full set of raw snapshot CSV files for a period directory."""
        period_dir = tmp_path / "raw" / period_grain / time_anchor / period_key
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
            time_anchor=config.time_anchor,
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
        manifest_path = (
            tmp_path
            / "manifest"
            / config.period.value
            / config.time_anchor.value
            / "manifest.json"
        )
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

