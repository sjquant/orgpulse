from __future__ import annotations

# ruff: noqa: F403,F405
from ..helpers.output import *


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
        assert result.root_dir == tmp_path / "repo_summary" / "month" / "created_at"
        assert result.contract_path == tmp_path / "repo_summary" / "month" / "created_at" / "contract.json"
        assert result.index_path == tmp_path / "repo_summary" / "month" / "created_at" / "index.json"
        assert result.readme_path == tmp_path / "repo_summary" / "month" / "created_at" / "README.md"
        assert result.latest_path == (
            tmp_path / "repo_summary" / "month" / "created_at" / "latest" / "repo_summary.csv"
        )
        assert [period.key for period in result.periods] == ["2026-04"]
        assert result.periods[0].repository_count == 2
        assert json.loads(result.contract_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": [],
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
            "period_state_fields": [
                "status",
                "label",
                "is_open",
                "is_closed",
                "is_partial",
                "observed_through_date",
            ],
            "target_org": "acme",
        }
        assert json.loads(result.index_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "history": [
                {
                    "closed": False,
                    "end_date": "2026-04-30",
                    **_expected_period_state(
                        closed=False,
                        observed_through_date="2026-04-18",
                    ),
                    "key": "2026-04",
                    "path": "2026-04/repo_summary.csv",
                    "start_date": "2026-04-01",
                }
            ],
            "include_repos": [],
            "latest": {
                "closed": False,
                "end_date": "2026-04-30",
                **_expected_period_state(
                    closed=False,
                    observed_through_date="2026-04-18",
                ),
                "key": "2026-04",
                "path": "latest/repo_summary.csv",
                "source_path": "2026-04/repo_summary.csv",
                "start_date": "2026-04-01",
            },
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
            "target_org": "acme",
        }
        rows = self._read_rows(result.periods[0].path)
        assert [row["repository_full_name"] for row in rows] == [
            "acme/api",
            "acme/web",
        ]
        assert rows[0]["period_key"] == "2026-04"
        assert rows[0]["period_grain"] == "month"
        assert rows[0]["time_anchor"] == "created_at"
        assert rows[0]["time_anchor_scope"] == "pull_request.created_at"
        assert rows[0]["period_status"] == "open"
        assert rows[0]["period_label"] == "open month"
        assert rows[0]["period_open"] == "true"
        assert rows[0]["period_closed"] == "false"
        assert rows[0]["period_partial"] == "true"
        assert rows[0]["period_observed_through_date"] == "2026-04-18"
        assert rows[0]["pull_request_count"] == "1"
        assert rows[0]["merged_pull_request_count"] == "1"
        assert rows[0]["active_author_count"] == "1"
        assert rows[0]["time_to_merge_total_seconds"] == "97200"
        assert rows[0]["time_to_first_review_total_seconds"] == "10800"
        assert rows[1]["time_to_merge_total_seconds"] == "97200"
        assert rows[1]["time_to_first_review_total_seconds"] == "21600"
        assert result.latest_path is not None
        assert result.latest_path.read_text(encoding="utf-8") == result.periods[0].path.read_text(
            encoding="utf-8"
        )

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
        stale_period_dir = tmp_path / "repo_summary" / "month" / "created_at" / "2026-03"
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
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-03" / "repo_summary.csv"
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
        assert json.loads(result.index_path.read_text(encoding="utf-8"))["history"] == [
            {
                "closed": True,
                "end_date": "2026-03-31",
                **_expected_period_state(
                    closed=True,
                    observed_through_date="2026-03-31",
                ),
                "key": "2026-03",
                "path": "2026-03/repo_summary.csv",
                "start_date": "2026-03-01",
            },
            {
                "closed": False,
                "end_date": "2026-04-30",
                **_expected_period_state(
                    closed=False,
                    observed_through_date="2026-04-18",
                ),
                "key": "2026-04",
                "path": "2026-04/repo_summary.csv",
                "start_date": "2026-04-01",
            },
        ]
        assert json.loads(result.index_path.read_text(encoding="utf-8"))["latest"] == {
            "closed": False,
            "end_date": "2026-04-30",
            **_expected_period_state(
                closed=False,
                observed_through_date="2026-04-18",
            ),
            "key": "2026-04",
            "path": "latest/repo_summary.csv",
            "source_path": "2026-04/repo_summary.csv",
            "start_date": "2026-04-01",
        }
        assert result.latest_path is not None
        assert result.latest_path.read_text(encoding="utf-8") == result.periods[0].path.read_text(
            encoding="utf-8"
        )

    def test_ignores_additive_history_metadata_from_existing_index_files(
        self,
        tmp_path,
    ) -> None:
        """Ignore extra fields in saved repo-summary history entries when rebuilding incremental history."""
        # Given
        previous_config = self._build_run_config(
            as_of="2026-03-18",
            output_dir=tmp_path,
        )
        current_config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        writer = RepositorySummaryCsvWriter()
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
        previous_repository_metrics = RepositoryMetricCollectionBuilder().build(
            previous_config,
            PullRequestMetricCollectionBuilder().build(
                previous_config,
                previous_raw_snapshot,
            ),
        )
        previous_result = writer.write(
            previous_config,
            previous_repository_metrics,
            refreshed_period_keys=("2026-03",),
        )
        previous_index_payload = json.loads(
            previous_result.index_path.read_text(encoding="utf-8")
        )
        previous_index_payload["history"][0]["debug_note"] = "legacy"
        previous_result.index_path.write_text(
            json.dumps(previous_index_payload),
            encoding="utf-8",
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
        result = writer.write(
            current_config,
            repository_metrics,
            refreshed_period_keys=("2026-04",),
        )

        # Then
        assert [entry["key"] for entry in json.loads(result.index_path.read_text(encoding="utf-8"))["history"]] == [
            "2026-03",
            "2026-04",
        ]

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

