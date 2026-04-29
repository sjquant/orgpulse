from __future__ import annotations

# ruff: noqa: F403,F405
from .support.output_support import *


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
        result = OrgSummaryWriter().write(
            config,
            org_metrics,
            refreshed_period_keys=("2026-04",),
        )

        # Then
        assert result.root_dir == tmp_path / "org_summary" / "month" / "created_at"
        assert result.contract_path == tmp_path / "org_summary" / "month" / "created_at" / "contract.json"
        assert result.index_path == tmp_path / "org_summary" / "month" / "created_at" / "index.json"
        assert result.readme_path == tmp_path / "org_summary" / "month" / "created_at" / "README.md"
        assert result.latest_directory == tmp_path / "org_summary" / "month" / "created_at" / "latest"
        assert result.latest_directory is not None
        assert result.latest_json_path is not None
        assert result.latest_markdown_path is not None
        assert result.latest_json_path == result.latest_directory / "summary.json"
        assert result.latest_markdown_path == result.latest_directory / "summary.md"
        assert [period.key for period in result.periods] == ["2026-04"]
        assert json.loads(result.contract_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": [],
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
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
                    "json_path": "2026-04/summary.json",
                    "key": "2026-04",
                    "markdown_path": "2026-04/summary.md",
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
                "json_path": "latest/summary.json",
                "key": "2026-04",
                "markdown_path": "latest/summary.md",
                "source_json_path": "2026-04/summary.json",
                "source_markdown_path": "2026-04/summary.md",
                "start_date": "2026-04-01",
            },
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
            "target_org": "acme",
        }
        period_result = result.periods[0]
        assert json.loads(period_result.json_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": [],
            "period": {
                "closed": False,
                "end_date": "2026-04-30",
                **_expected_period_state(
                    closed=False,
                    observed_through_date="2026-04-18",
                ),
                "key": "2026-04",
                "start_date": "2026-04-01",
            },
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
            "summary_labels": {
                "merged_pull_request_count": (
                    "Merged pull request count (pull_request.created_at)"
                ),
                "pull_request_count": (
                    "Pull request count (pull_request.created_at)"
                ),
                "value_summaries": (
                    "Value summaries grouped by pull_request.created_at"
                ),
            },
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
        markdown = period_result.markdown_path.read_text(encoding="utf-8")
        assert "# Organization Summary: acme 2026-04 (pull_request.created_at)" in markdown
        assert (
            "- Summary scope: All counts and summaries in this file are grouped by "
            "pull_request.created_at."
        ) in markdown
        assert "- Period status: open month" in markdown
        assert "- Partial period: true" in markdown
        assert "- Observed through: 2026-04-18" in markdown
        assert "- Pull request count (pull_request.created_at): 3" in markdown
        assert "- Merged pull request count (pull_request.created_at): 2" in markdown
        assert "## Value Summaries (pull_request.created_at)" in markdown
        assert result.latest_json_path is not None
        assert result.latest_markdown_path is not None
        assert result.latest_json_path.read_text(encoding="utf-8") == period_result.json_path.read_text(
            encoding="utf-8"
        )
        assert result.latest_markdown_path.read_text(
            encoding="utf-8"
        ) == period_result.markdown_path.read_text(encoding="utf-8")
        readme = result.readme_path.read_text(encoding="utf-8")
        assert (
            "- Summary scope: All counts and summaries in this file are grouped by "
            "pull_request.created_at."
        ) in readme
        assert "- Latest period status: open month" in readme
        assert "- Latest period partial: true" in readme
        assert "- Latest period observed through: 2026-04-18" in readme
        assert (
            "| 2026-04 | 2026-04-01 | 2026-04-30 | open month | true | "
            "2026-04-18 | 2026-04/summary.json | 2026-04/summary.md |"
        ) in readme

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
        stale_period_dir = tmp_path / "org_summary" / "month" / "created_at" / "2026-03"
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
        result = OrgSummaryWriter().write(
            config,
            org_metrics,
            refreshed_period_keys=("2026-04",),
        )

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
        writer.write(
            initial_config,
            initial_metrics,
            refreshed_period_keys=("2026-03",),
        )

        # When
        result = writer.write(
            current_config,
            current_metrics,
            refreshed_period_keys=("2026-04",),
        )

        # Then
        assert (tmp_path / "org_summary" / "month" / "created_at" / "2026-03").exists() is False
        assert result.periods[0].directory.exists()
        assert json.loads(result.contract_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": ["acme/api"],
            "period_grain": "month",
            "time_anchor": "created_at",
            "time_anchor_context": _expected_time_anchor_context(),
            "target_org": "acme",
        }
        assert json.loads(result.periods[0].json_path.read_text(encoding="utf-8"))[
            "include_repos"
        ] == ["acme/api"]
        assert "Include repos: acme/api" in result.periods[0].markdown_path.read_text(
            encoding="utf-8"
        )

    def test_writes_only_refreshed_periods_and_preserves_locked_exports(
        self,
        tmp_path,
    ) -> None:
        """Write only refreshed org-summary periods during incremental runs and leave locked exports untouched."""
        # Given
        previous_config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-03-18",
                "output_dir": tmp_path,
            }
        )
        current_config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-04-18",
                "output_dir": tmp_path,
            }
        )
        previous_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-03",
                    start_date=date.fromisoformat("2026-03-01"),
                    end_date=date.fromisoformat("2026-03-31"),
                    closed=False,
                    summary=self._build_rollup(),
                ),
            ),
        )
        current_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-03",
                    start_date=date.fromisoformat("2026-03-01"),
                    end_date=date.fromisoformat("2026-03-31"),
                    closed=True,
                    summary=self._build_rollup(),
                ),
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
        writer.write(
            previous_config,
            previous_metrics,
            refreshed_period_keys=("2026-03",),
        )
        locked_summary_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-03" / "summary.json"
        )
        locked_summary_payload = locked_summary_path.read_text(encoding="utf-8")

        # When
        result = writer.write(
            current_config,
            current_metrics,
            refreshed_period_keys=("2026-04",),
        )

        # Then
        assert [period.key for period in result.periods] == ["2026-04"]
        assert locked_summary_path.read_text(encoding="utf-8") == locked_summary_payload
        assert result.periods[0].json_path.exists()
        assert result.periods[0].markdown_path.exists()
        assert json.loads(result.index_path.read_text(encoding="utf-8"))["history"] == [
            {
                "closed": True,
                "end_date": "2026-03-31",
                **_expected_period_state(
                    closed=True,
                    observed_through_date="2026-03-31",
                ),
                "json_path": "2026-03/summary.json",
                "key": "2026-03",
                "markdown_path": "2026-03/summary.md",
                "start_date": "2026-03-01",
            },
            {
                "closed": False,
                "end_date": "2026-04-30",
                **_expected_period_state(
                    closed=False,
                    observed_through_date="2026-04-18",
                ),
                "json_path": "2026-04/summary.json",
                "key": "2026-04",
                "markdown_path": "2026-04/summary.md",
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
            "json_path": "latest/summary.json",
            "key": "2026-04",
            "markdown_path": "latest/summary.md",
            "source_json_path": "2026-04/summary.json",
            "source_markdown_path": "2026-04/summary.md",
            "start_date": "2026-04-01",
        }
        assert result.latest_json_path is not None
        assert result.latest_json_path.read_text(encoding="utf-8") == result.periods[0].json_path.read_text(
            encoding="utf-8"
        )

    def test_ignores_additive_history_metadata_from_existing_index_files(
        self,
        tmp_path,
    ) -> None:
        """Ignore extra fields in saved org-summary history entries when rebuilding incremental history."""
        # Given
        previous_config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-03-18",
                "output_dir": tmp_path,
            }
        )
        current_config = RunConfig.model_validate(
            {
                "org": "acme",
                "as_of": "2026-04-18",
                "output_dir": tmp_path,
            }
        )
        writer = OrgSummaryWriter()
        previous_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-03",
                    start_date=date.fromisoformat("2026-03-01"),
                    end_date=date.fromisoformat("2026-03-31"),
                    closed=False,
                    summary=self._build_rollup(),
                ),
            ),
        )
        current_metrics = OrganizationMetricCollection(
            target_org="acme",
            periods=(
                OrganizationMetricPeriod(
                    key="2026-03",
                    start_date=date.fromisoformat("2026-03-01"),
                    end_date=date.fromisoformat("2026-03-31"),
                    closed=True,
                    summary=self._build_rollup(),
                ),
                OrganizationMetricPeriod(
                    key="2026-04",
                    start_date=date.fromisoformat("2026-04-01"),
                    end_date=date.fromisoformat("2026-04-30"),
                    closed=False,
                    summary=self._build_rollup(),
                ),
            ),
        )
        previous_result = writer.write(
            previous_config,
            previous_metrics,
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

        # When
        result = writer.write(
            current_config,
            current_metrics,
            refreshed_period_keys=("2026-04",),
        )

        # Then
        assert [entry["key"] for entry in json.loads(result.index_path.read_text(encoding="utf-8"))["history"]] == [
            "2026-03",
            "2026-04",
        ]

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


