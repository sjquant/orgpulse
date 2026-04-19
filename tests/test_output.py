from __future__ import annotations

import json
from datetime import date, datetime

from orgpulse.ingestion import (
    PULL_REQUEST_FIELDNAMES,
    PULL_REQUEST_REVIEW_FIELDNAMES,
    PULL_REQUEST_TIMELINE_EVENT_FIELDNAMES,
)
from orgpulse.models import (
    CollectionWindow,
    LastSuccessfulRun,
    ManifestWatermarks,
    MetricValueSummary,
    OrganizationMetricCollection,
    OrganizationMetricPeriod,
    OrganizationMetricRollup,
    PullRequestCollection,
    RawSnapshotPeriod,
    RawSnapshotWriteResult,
    ReportingPeriod,
    RunConfig,
    RunManifest,
    RunMode,
    RunScope,
)
from orgpulse.output import OrgSummaryWriter, RunManifestWriter


class TestOrgSummaryWriter:
    def test_writes_deterministic_markdown_and_json_per_period(
        self,
        tmp_path,
    ) -> None:
        """Write stable Markdown and JSON org summaries under a period-scoped directory layout."""
        # Given
        config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-04-18",
                "output_dir": tmp_path,
            }
        )
        org_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-04",
                    start_date=date.fromisoformat("2026-04-01"),
                    end_date=date.fromisoformat("2026-04-30"),
                    closed=False,
                    summary=self._build_rollup(),
                ),
            ),
        )

        # When
        result = OrgSummaryWriter().write(config, org_metrics)

        # Then
        assert result.root_dir == tmp_path / "org_summary" / "month"
        assert result.contract_path == tmp_path / "org_summary" / "month" / "contract.json"
        assert [period.key for period in result.periods] == ["2026-04"]
        assert json.loads(result.contract_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": [],
            "period_grain": "month",
            "target_org": "acme",
        }
        period_result = result.periods[0]
        assert json.loads(period_result.json_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": [],
            "period": {
                "closed": False,
                "end_date": "2026-04-30",
                "key": "2026-04",
                "start_date": "2026-04-01",
            },
            "period_grain": "month",
            "summary": {
                "active_author_count": 2,
                "additions": {
                    "average": 20.0,
                    "count": 2,
                    "median": 20.0,
                    "total": 40,
                },
                "changed_files": {
                    "average": 3.0,
                    "count": 2,
                    "median": 3.0,
                    "total": 6,
                },
                "changed_lines": {
                    "average": 25.0,
                    "count": 2,
                    "median": 25.0,
                    "total": 50,
                },
                "commits": {
                    "average": 4.0,
                    "count": 2,
                    "median": 4.0,
                    "total": 8,
                },
                "deletions": {
                    "average": 5.0,
                    "count": 2,
                    "median": 5.0,
                    "total": 10,
                },
                "merged_pull_request_count": 2,
                "merged_pull_requests_per_active_author": 1.0,
                "pull_request_count": 3,
                "repository_count": 2,
                "time_to_first_review_seconds": {
                    "average": 150.0,
                    "count": 2,
                    "median": 150.0,
                    "total": 300,
                },
                "time_to_merge_seconds": {
                    "average": 90.0,
                    "count": 2,
                    "median": 90.0,
                    "total": 180,
                },
            },
            "target_org": "acme",
        }
        assert period_result.markdown_path.read_text(encoding="utf-8") == (
            "# Organization Summary: acme 2026-04\n"
            "\n"
            "- Target org: acme\n"
            "- Period grain: month\n"
            "- Period key: 2026-04\n"
            "- Include repos: all\n"
            "- Exclude repos: none\n"
            "- Period start: 2026-04-01\n"
            "- Period end: 2026-04-30\n"
            "- Closed: false\n"
            "\n"
            "## Totals\n"
            "\n"
            "- Repository count: 2\n"
            "- Pull request count: 3\n"
            "- Merged pull request count: 2\n"
            "- Active author count: 2\n"
            "- Merged pull requests per active author: 1.00\n"
            "\n"
            "## Value Summaries\n"
            "\n"
            "| Metric | Count | Total | Average | Median |\n"
            "| --- | ---: | ---: | ---: | ---: |\n"
            "| Time to merge (seconds) | 2 | 180 | 90.00 | 90.00 |\n"
            "| Time to first review (seconds) | 2 | 300 | 150.00 | 150.00 |\n"
            "| Additions | 2 | 40 | 20.00 | 20.00 |\n"
            "| Deletions | 2 | 10 | 5.00 | 5.00 |\n"
            "| Changed lines | 2 | 50 | 25.00 | 25.00 |\n"
            "| Changed files | 2 | 6 | 3.00 | 3.00 |\n"
            "| Commits | 2 | 8 | 4.00 | 4.00 |\n"
        )

    def test_prunes_stale_period_directories_on_full_runs(
        self,
        tmp_path,
    ) -> None:
        """Prune stale org summary period directories only during full-history reruns."""
        # Given
        config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-04-18",
                "mode": RunMode.FULL,
                "output_dir": tmp_path,
            }
        )
        stale_period_dir = tmp_path / "org_summary" / "month" / "2026-03"
        stale_period_dir.mkdir(parents=True)
        (stale_period_dir / "summary.json").write_text(
            json.dumps({"target_org": "stale"}),
            encoding="utf-8",
        )
        org_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-04",
                    start_date=date.fromisoformat("2026-04-01"),
                    end_date=date.fromisoformat("2026-04-30"),
                    closed=False,
                    summary=self._build_rollup(),
                ),
            ),
        )

        # When
        result = OrgSummaryWriter().write(config, org_metrics)

        # Then
        assert stale_period_dir.exists() is False
        assert result.periods[0].json_path.exists()
        assert result.periods[0].markdown_path.exists()

    def test_prunes_existing_period_directories_when_the_export_contract_changes(
        self,
        tmp_path,
    ) -> None:
        """Prune org summary period directories before writing when the saved filter contract changes."""
        # Given
        initial_config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-04-18",
                "output_dir": tmp_path,
            }
        )
        current_config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-04-18",
                "output_dir": tmp_path,
                "include_repos": ("api",),
            }
        )
        initial_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-03",
                    start_date=date.fromisoformat("2026-03-01"),
                    end_date=date.fromisoformat("2026-03-31"),
                    closed=True,
                    summary=self._build_rollup(),
                ),
            ),
        )
        current_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-04",
                    start_date=date.fromisoformat("2026-04-01"),
                    end_date=date.fromisoformat("2026-04-30"),
                    closed=False,
                    summary=self._build_rollup(),
                ),
            ),
        )
        writer = OrgSummaryWriter()
        writer.write(initial_config, initial_metrics)

        # When
        result = writer.write(current_config, current_metrics)

        # Then
        assert (tmp_path / "org_summary" / "month" / "2026-03").exists() is False
        assert result.periods[0].directory.exists()
        assert json.loads(result.contract_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": ["acme/api"],
            "period_grain": "month",
            "target_org": "acme",
        }
        assert json.loads(result.periods[0].json_path.read_text(encoding="utf-8"))[
            "include_repos"
        ] == ["acme/api"]
        assert "Include repos: acme/api" in result.periods[0].markdown_path.read_text(
            encoding="utf-8"
        )

    def _build_rollup(self) -> OrganizationMetricRollup:
        """Build a representative org rollup payload for summary writer tests."""
        return OrganizationMetricRollup(
            repository_count=2,
            pull_request_count=3,
            merged_pull_request_count=2,
            active_author_count=2,
            merged_pull_requests_per_active_author=1.0,
            time_to_merge_seconds=self._build_summary(count=2, total=180),
            time_to_first_review_seconds=self._build_summary(count=2, total=300),
            additions=self._build_summary(count=2, total=40),
            deletions=self._build_summary(count=2, total=10),
            changed_lines=self._build_summary(count=2, total=50),
            changed_files=self._build_summary(count=2, total=6),
            commits=self._build_summary(count=2, total=8),
        )

    def _build_summary(
        self,
        *,
        count: int,
        total: int,
    ) -> MetricValueSummary:
        """Build a deterministic value summary for org summary export tests."""
        average = None if count == 0 else float(total / count)
        return MetricValueSummary(
            count=count,
            total=total,
            average=average,
            median=average,
        )


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
