from __future__ import annotations

import csv
import importlib.util
import json
from datetime import date, datetime
from pathlib import Path

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
    MetricValueSummary,
    OrganizationMetricCollection,
    OrganizationMetricPeriod,
    OrganizationMetricRollup,
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
from orgpulse.output import (
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)

_RENDER_MANUAL_DASHBOARD_SPEC = importlib.util.spec_from_file_location(
    "render_manual_org_dashboard",
    Path(__file__).resolve().parents[1] / "scripts" / "render_manual_org_dashboard.py",
)
assert _RENDER_MANUAL_DASHBOARD_SPEC is not None
assert _RENDER_MANUAL_DASHBOARD_SPEC.loader is not None
_render_manual_dashboard = importlib.util.module_from_spec(
    _RENDER_MANUAL_DASHBOARD_SPEC,
)
_RENDER_MANUAL_DASHBOARD_SPEC.loader.exec_module(_render_manual_dashboard)
prepare_manual_dashboard_payload = (
    _render_manual_dashboard.prepare_manual_dashboard_payload
)


def _expected_time_anchor_context(
    time_anchor: str = "created_at",
) -> dict[str, str]:
    return {
        "field": time_anchor,
        "scope": f"pull_request.{time_anchor}",
        "description": (
            "All counts and summaries in this file are grouped by "
            f"pull_request.{time_anchor}."
        ),
    }


def _expected_period_state(
    *,
    grain: str = "month",
    closed: bool,
    observed_through_date: str,
) -> dict[str, object]:
    status = "closed" if closed else "open"
    return {
        "status": status,
        "label": f"{status} {grain}",
        "is_open": not closed,
        "is_closed": closed,
        "is_partial": not closed,
        "observed_through_date": observed_through_date,
    }


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


class TestManualDashboardPayload:
    def test_uses_shared_percentile_cutoff_across_overview_and_breakdowns(
        self,
    ) -> None:
        """Apply one percentile cutoff across overview, trends, repositories, and author breakdowns."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-24T00:00:00+00:00",
                "since": "2026-04-01",
                "until": "2026-04-30",
                "time_anchor": "created_at",
                "top_repository": "acme/api",
                "top_author": "alice",
                "unique_reviewers": 2,
            },
            "reviewers": [
                {
                    "reviewer_login": "alice",
                    "review_submissions": 4,
                    "pull_requests_reviewed": 2,
                    "approvals": 2,
                    "changes_requested": 0,
                    "comments": 2,
                    "authors_supported": 2,
                },
                {
                    "reviewer_login": "bob",
                    "review_submissions": 1,
                    "pull_requests_reviewed": 1,
                    "approvals": 1,
                    "changes_requested": 0,
                    "comments": 0,
                    "authors_supported": 1,
                },
            ],
            "pull_requests": [
                _manual_pull_request(
                    repository_full_name="acme/api",
                    pull_request_number=1,
                    author_login="alice",
                    created_at="2026-04-01T09:00:00+00:00",
                    merged_at="2026-04-02T09:00:00+00:00",
                    changed_lines=10,
                    additions=8,
                    deletions=2,
                    first_review_hours=1.0,
                    merge_hours=24.0,
                    size_bucket="XS",
                ),
                _manual_pull_request(
                    repository_full_name="acme/api",
                    pull_request_number=2,
                    author_login="bob",
                    created_at="2026-04-03T09:00:00+00:00",
                    merged_at="2026-04-04T09:00:00+00:00",
                    changed_lines=20,
                    additions=16,
                    deletions=4,
                    first_review_hours=2.0,
                    merge_hours=24.0,
                    size_bucket="S",
                ),
                _manual_pull_request(
                    repository_full_name="acme/web",
                    pull_request_number=3,
                    author_login="carol",
                    created_at="2026-04-05T09:00:00+00:00",
                    merged_at="2026-04-06T09:00:00+00:00",
                    changed_lines=1000,
                    additions=1000,
                    deletions=0,
                    first_review_hours=3.0,
                    merge_hours=24.0,
                    size_bucket="XL",
                ),
            ],
        }

        # When
        prepared = prepare_manual_dashboard_payload(
            payload,
            distribution_percentile=95,
        )

        # Then
        assert prepared["overview"]["total_changed_lines"] == 30
        assert sum(row["changed_lines"] for row in prepared["repositories"]) == 30
        assert sum(row["changed_lines"] for row in prepared["monthly_trends"]) == 30
        assert prepared["repositories"] == [
            {
                "repository_full_name": "acme/api",
                "pull_requests": 2,
                "merged_pull_requests": 2,
                "open_pull_requests": 0,
                "authors": 2,
                "changed_lines": 30,
                "review_submissions": 2,
                "average_reviews_per_pr": 1.0,
                "median_first_review_hours": 1.5,
                "median_merge_hours": 24.0,
                "share_of_prs_pct": 66.67,
            },
            {
                "repository_full_name": "acme/web",
                "pull_requests": 1,
                "merged_pull_requests": 1,
                "open_pull_requests": 0,
                "authors": 1,
                "changed_lines": 0,
                "review_submissions": 1,
                "average_reviews_per_pr": 1.0,
                "median_first_review_hours": None,
                "median_merge_hours": 24.0,
                "share_of_prs_pct": 33.33,
            },
        ]
        author_details = json.loads(prepared["author_details_json"])
        assert author_details["carol"]["size_mix"][-1] == {
            "bucket": "XL",
            "pull_requests": 1,
            "changed_lines": 0,
            "median_first_review_hours": None,
            "median_merge_hours": 24.0,
            "average_reviews_per_pr": 1.0,
        }


def _manual_pull_request(
    *,
    repository_full_name: str,
    pull_request_number: int,
    author_login: str,
    created_at: str,
    merged_at: str | None,
    changed_lines: int,
    additions: int,
    deletions: int,
    first_review_hours: float | None,
    merge_hours: float | None,
    size_bucket: str,
) -> dict[str, object]:
    """Build a normalized manual dashboard pull request payload for tests."""
    closed_at = merged_at
    return {
        "repository_full_name": repository_full_name,
        "pull_request_number": pull_request_number,
        "title": f"PR {pull_request_number}",
        "author_login": author_login,
        "state": "closed" if merged_at else "open",
        "created_at": created_at,
        "updated_at": merged_at or created_at,
        "closed_at": closed_at,
        "merged_at": merged_at,
        "html_url": f"https://example.test/pr/{pull_request_number}",
        "additions": additions,
        "deletions": deletions,
        "changed_files": 1,
        "changed_lines": changed_lines,
        "commits": 1,
        "review_count": 1,
        "approval_count": 1,
        "changes_requested_count": 0,
        "comment_review_count": 0,
        "reviewer_count": 1,
        "first_review_hours": first_review_hours,
        "merge_hours": merge_hours,
        "close_hours": merge_hours,
        "review_rounds": 1,
        "review_ready_at": created_at,
        "review_requested_at": created_at,
        "size_bucket": size_bucket,
    }
