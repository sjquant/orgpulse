from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from orgpulse.cli import app, build_run_config
from orgpulse.errors import AuthResolutionError, GitHubApiError
from orgpulse.github_auth import GitHubAuthService
from orgpulse.ingestion import PULL_REQUEST_FIELDNAMES, NormalizedRawSnapshotWriter
from orgpulse.metrics import (
    OrganizationMetricCollectionBuilder,
    PullRequestMetricCollectionBuilder,
    RepositoryMetricCollectionBuilder,
)
from orgpulse.models import (
    AuthSource,
    CollectionWindow,
    GitHubTargetContext,
    LastSuccessfulRun,
    ManifestWatermarks,
    ManifestWriteResult,
    OrgSummaryPeriodWriteResult,
    OrgSummaryWriteResult,
    PeriodGrain,
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RawSnapshotWriteResult,
    RepositoryCollectionFailure,
    RepositoryInventory,
    RepositorySummaryCsvWriteResult,
    ResolvedToken,
    RunManifest,
    RunMode,
    RunScope,
    TimeAnchor,
)
from orgpulse.output import (
    REPOSITORY_SUMMARY_CSV_FIELDNAMES,
    OrgSummaryWriter,
    RepositorySummaryCsvWriter,
    RunManifestWriter,
)


@pytest.fixture(scope="module")
def runner() -> CliRunner:
    """Provide a reusable Typer CLI runner for pytest-based tests."""
    return CliRunner()


@pytest.fixture
def github_auth_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the GitHub auth boundary for CLI tests that focus on config behavior."""
    github_auth_service = create_autospec(
        GitHubAuthService, instance=True, spec_set=True
    )
    github_auth_service.validate_access.side_effect = lambda config: (
        GitHubTargetContext(
            auth_source=AuthSource.GH_TOKEN,
            viewer_login="test-user",
            organization_login=config.org,
        )
    )

    monkeypatch.setattr(
        "orgpulse.cli.resolve_auth_token",
        lambda config: ResolvedToken(source=AuthSource.GH_TOKEN, token="env-token"),
    )
    monkeypatch.setattr("orgpulse.cli.Github", lambda auth: object())
    monkeypatch.setattr("orgpulse.cli.Auth.Token", lambda token: object())
    monkeypatch.setattr(
        "orgpulse.cli.GitHubAuthService",
        lambda github_client, auth_source: github_auth_service,
    )
    monkeypatch.setattr(
        "orgpulse.cli.GitHubIngestionService",
        lambda github_client: FakeCliIngestionService(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.NormalizedRawSnapshotWriter",
        lambda: FakeCliSnapshotWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.RunManifestWriter",
        lambda: FakeCliManifestWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.OrgSummaryWriter",
        lambda: FakeCliOrgSummaryWriter(),
    )
    monkeypatch.setattr(
        "orgpulse.cli.RepositorySummaryCsvWriter",
        lambda: FakeCliRepositorySummaryWriter(),
    )


class FakeCliIngestionService:
    def __init__(
        self,
        *,
        inventory: RepositoryInventory | None = None,
        collection: PullRequestCollection | None = None,
    ) -> None:
        self._inventory = inventory
        self._collection = collection

    def load_repository_inventory(self, config) -> RepositoryInventory:
        return (
            self._inventory
            if self._inventory is not None
            else RepositoryInventory(
                organization_login=config.org,
                repositories=(),
            )
        )

    def fetch_pull_requests(
        self,
        config,
        inventory: RepositoryInventory,
    ) -> PullRequestCollection:
        return (
            self._collection
            if self._collection is not None
            else PullRequestCollection(
                window=config.collection_window,
                pull_requests=(),
                failures=(),
            )
        )


class FakeCliSnapshotWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
    ) -> RawSnapshotWriteResult:
        return RawSnapshotWriteResult(
            root_dir=config.output_dir / "raw" / config.period.value,
            periods=(),
        )


class FakeCliManifestWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        *,
        repository_count: int,
    ) -> ManifestWriteResult:
        return ManifestWriteResult(
            path=config.output_dir / "manifest" / config.period.value / "manifest.json",
            index_path=config.output_dir / "manifest" / config.period.value / "index.json",
            readme_path=config.output_dir / "manifest" / config.period.value / "README.md",
            manifest=RunManifest(
                target_org=config.org,
                period_grain=config.period,
                time_anchor=config.time_anchor,
                include_repos=config.include_repos,
                exclude_repos=config.exclude_repos,
                raw_snapshot_root_dir=raw_snapshot.root_dir,
                refreshed_periods=raw_snapshot.periods,
                locked_periods=(),
                watermarks=ManifestWatermarks(
                    collection_window_start_date=collection.window.start_date,
                    collection_window_end_date=collection.window.end_date,
                    latest_refreshed_period_end_date=None,
                    latest_locked_period_end_date=None,
                ),
                last_successful_run=LastSuccessfulRun(
                    completed_at=datetime.fromisoformat("2026-04-18T00:00:00+00:00"),
                    as_of=config.as_of,
                    mode=config.mode,
                    refresh_scope=config.refresh_scope,
                    repository_count=repository_count,
                    pull_request_count=len(collection.pull_requests),
                ),
            ),
        )


class FakeCliOrgSummaryWriter:
    def write(
        self,
        config,
        org_metrics,
        *,
        refreshed_period_keys,
    ) -> OrgSummaryWriteResult:
        return OrgSummaryWriteResult(
            target_org=org_metrics.target_org,
            root_dir=config.output_dir / "org_summary" / config.period.value,
            contract_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "contract.json",
            index_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "index.json",
            readme_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "README.md",
            latest_directory=config.output_dir
            / "org_summary"
            / config.period.value
            / "latest",
            latest_markdown_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "latest"
            / "summary.md",
            latest_json_path=config.output_dir
            / "org_summary"
            / config.period.value
            / "latest"
            / "summary.json",
            periods=tuple(
                OrgSummaryPeriodWriteResult(
                    key=period.key,
                    start_date=period.start_date,
                    end_date=period.end_date,
                    closed=period.closed,
                    directory=config.output_dir
                    / "org_summary"
                    / config.period.value
                    / period.key,
                    markdown_path=config.output_dir
                    / "org_summary"
                    / config.period.value
                    / period.key
                    / "summary.md",
                    json_path=config.output_dir
                    / "org_summary"
                    / config.period.value
                    / period.key
                    / "summary.json",
                )
                for period in org_metrics.periods
            ),
        )


class FakeCliRepositorySummaryWriter:
    def write(
        self,
        config,
        repository_metrics,
        *,
        refreshed_period_keys,
    ) -> RepositorySummaryCsvWriteResult:
        return RepositorySummaryCsvWriteResult(
            root_dir=config.output_dir / "repo_summary" / config.period.value,
            contract_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "contract.json",
            index_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "index.json",
            readme_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "README.md",
            latest_path=config.output_dir
            / "repo_summary"
            / config.period.value
            / "latest"
            / "repo_summary.csv",
            periods=(),
        )


class UnexpectedSnapshotWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
    ) -> RawSnapshotWriteResult:
        raise AssertionError("snapshot writer should not run")


class UnexpectedManifestWriter:
    def write(
        self,
        config,
        collection: PullRequestCollection,
        raw_snapshot: RawSnapshotWriteResult,
        *,
        repository_count: int,
    ) -> None:
        raise AssertionError("manifest writer should not run")


class UnexpectedOrgSummaryWriter:
    def write(
        self,
        config,
        org_metrics,
        *,
        refreshed_period_keys,
    ) -> None:
        raise AssertionError("org summary writer should not run")


class UnexpectedRepositorySummaryWriter:
    def write(
        self,
        config,
        repository_metrics,
        *,
        refreshed_period_keys,
    ) -> None:
        raise AssertionError("repo summary writer should not run")


class TestRunConfigParsing:
    def test_parses_incremental_run_defaults(self) -> None:
        """Parse the default incremental run configuration from explicit input."""
        # Given

        # When
        config = build_run_config(org="acme")

        # Then
        assert config.org == "acme"
        assert config.period == PeriodGrain.MONTH
        assert config.time_anchor == TimeAnchor.CREATED_AT
        assert config.mode == RunMode.INCREMENTAL
        assert config.refresh_scope == RunScope.OPEN_PERIOD
        assert config.output_dir.as_posix() == "output"
        assert config.include_repos == ()
        assert config.exclude_repos == ()
        assert config.checkpoint_policy.resume_from_checkpoint is True
        assert config.checkpoint_policy.persist_checkpoint is True
        assert config.checkpoint_policy.overwrite_checkpoint is False
        assert config.lock_policy.skip_locked_periods is True
        assert config.lock_policy.refresh_locked_periods is False
        assert config.lock_policy.lock_closed_periods_on_success is True

    def test_loads_settings_from_environment(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Load run configuration defaults from ORGPULSE-prefixed environment variables."""
        # Given
        env = {
            "ORGPULSE_AS_OF": "2026-04-18",
            "ORGPULSE_ORG": "env-acme",
            "ORGPULSE_PERIOD": "week",
            "ORGPULSE_TIME_ANCHOR": "updated_at",
            "ORGPULSE_OUTPUT_DIR": "env-output",
        }

        # When
        result = runner.invoke(app, ["run"], env=env)

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["config"]["as_of"] == "2026-04-18"
        assert payload["config"]["org"] == "env-acme"
        assert payload["config"]["period"] == "week"
        assert payload["config"]["time_anchor"] == "updated_at"
        assert payload["config"]["output_dir"] == "env-output"
        assert payload["config"]["active_period"]["start_date"] == "2026-04-13"
        assert payload["config"]["active_period"]["end_date"] == "2026-04-19"
        assert payload["config"]["refresh_scope"] == "open_period"
        assert payload["github"]["organization_login"] == "env-acme"

    def test_cli_options_override_environment_defaults(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Prefer explicit CLI values over ORGPULSE environment defaults."""
        # Given
        env = {
            "ORGPULSE_ORG": "env-acme",
            "ORGPULSE_PERIOD": "month",
            "ORGPULSE_TIME_ANCHOR": "updated_at",
        }

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "cli-acme",
                "--period",
                "week",
                "--time-anchor",
                "merged_at",
            ],
            env=env,
        )

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["config"]["org"] == "cli-acme"
        assert payload["config"]["period"] == "week"
        assert payload["config"]["time_anchor"] == "merged_at"
        assert payload["github"]["organization_login"] == "cli-acme"

    def test_resolves_month_period_boundaries_from_as_of(self) -> None:
        """Resolve the current monthly reporting period from an explicit as-of date."""
        # Given

        # When
        config = build_run_config(org="acme", as_of="2026-04-18")

        # Then
        assert str(config.as_of) == "2026-04-18"
        assert str(config.active_period.start_date) == "2026-04-01"
        assert str(config.active_period.end_date) == "2026-04-30"
        assert config.active_period.key == "2026-04"

    def test_resolves_week_period_boundaries_from_as_of(self) -> None:
        """Resolve ISO-week reporting boundaries from an explicit as-of date."""
        # Given

        # When
        config = build_run_config(
            org="acme", as_of="2026-04-18", period=PeriodGrain.WEEK
        )

        # Then
        assert str(config.active_period.start_date) == "2026-04-13"
        assert str(config.active_period.end_date) == "2026-04-19"
        assert config.active_period.key == "2026-W16"

    def test_parses_backfill_bounds(self) -> None:
        """Parse backfill mode when both inclusive date bounds are provided."""
        # Given

        # When
        config = build_run_config(
            org="acme",
            as_of="2026-04-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-01-01",
            backfill_end="2026-01-31",
            include_repos=["api"],
            exclude_repos=["legacy"],
        )

        # Then
        assert config.mode == RunMode.BACKFILL
        assert str(config.backfill_start) == "2026-01-01"
        assert str(config.backfill_end) == "2026-01-31"
        assert config.refresh_scope == RunScope.BOUNDED_BACKFILL
        assert config.include_repos == ("api",)
        assert config.exclude_repos == ("legacy",)
        assert config.requested_range is not None
        assert config.requested_range.period_count == 1
        assert config.checkpoint_policy.resume_from_checkpoint is False
        assert config.checkpoint_policy.persist_checkpoint is False
        assert config.checkpoint_policy.overwrite_checkpoint is False
        assert config.lock_policy.skip_locked_periods is False
        assert config.lock_policy.refresh_locked_periods is True

    def test_rejects_repo_filters_for_another_org(self) -> None:
        """Reject fully qualified repo filters that target a different organization."""
        # Given

        # When
        with pytest.raises(
            ValidationError, match="repo filter owner must match target org 'acme'"
        ):
            build_run_config(org="acme", include_repos=["other-org/api"])

        # Then

    def test_deduplicates_equivalent_qualified_and_unqualified_repo_filters(
        self,
    ) -> None:
        """Deduplicate repo filters that target the same repo through different forms."""
        # Given

        # When
        config = build_run_config(
            org="acme",
            include_repos=["platform", "acme/platform"],
            exclude_repos=["legacy", "acme/legacy"],
        )

        # Then
        assert config.include_repos == ("platform",)
        assert config.exclude_repos == ("legacy",)

    def test_rejects_backfill_without_bounds(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject backfill mode when one or both required date bounds are missing."""
        # Given

        # When
        result = runner.invoke(app, ["run", "--org", "acme", "--mode", "backfill"])

        # Then
        assert result.exit_code == 2
        assert (
            "backfill mode requires both --backfill-start and --backfill-end"
            in result.stderr
        )

    def test_rejects_backfill_start_outside_period_boundary(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject backfill ranges that do not start on a period boundary."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--mode",
                "backfill",
                "--backfill-start",
                "2026-01-02",
                "--backfill-end",
                "2026-01-31",
            ],
        )

        # Then
        assert result.exit_code == 2
        assert (
            "backfill start must align to the selected period boundary" in result.stderr
        )

    def test_rejects_backfill_end_in_current_open_period(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject backfill ranges that bleed into the current open period."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--mode",
                "backfill",
                "--backfill-start",
                "2026-03-01",
                "--backfill-end",
                "2026-04-30",
            ],
        )

        # Then
        assert result.exit_code == 2
        assert (
            "backfill range must end before the current open period begins"
            in result.stderr
        )

    def test_rejects_overlapping_repo_filters(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject configurations where the same repo is both included and excluded."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--repo",
                "platform",
                "--exclude-repo",
                "platform",
            ],
        )

        # Then
        assert result.exit_code == 2
        assert (
            "repo filters overlap across include and exclude lists: platform"
            in result.stderr
        )

    def test_rejects_case_insensitive_overlapping_repo_filters(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject repo filters that overlap after case-insensitive normalization."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--repo",
                "Platform",
                "--exclude-repo",
                "platform",
            ],
        )

        # Then
        assert result.exit_code == 2
        assert (
            "repo filters overlap across include and exclude lists: Platform"
            in result.stderr
        )

    def test_rejects_equivalent_qualified_and_unqualified_overlapping_repo_filters(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject repo filters that overlap across bare and org-qualified forms."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--repo",
                "platform",
                "--exclude-repo",
                "acme/platform",
            ],
        )

        # Then
        assert result.exit_code == 2
        assert (
            "repo filters overlap across include and exclude lists: platform"
            in result.stderr
        )

    def test_rejects_backfill_dates_for_non_backfill_mode(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Reject backfill date options when the run mode is not backfill."""
        # Given

        # When
        result = runner.invoke(
            app, ["run", "--org", "acme", "--backfill-start", "2026-01-01"]
        )

        # Then
        assert result.exit_code == 2
        assert (
            "backfill date bounds are only valid when --mode backfill is selected"
            in result.stderr
        )

    def test_serializes_full_mode_policy_in_cli_output(
        self,
        runner: CliRunner,
        github_auth_service: None,
    ) -> None:
        """Serialize full-run checkpoint and lock semantics through the CLI contract."""
        # Given

        # When
        result = runner.invoke(
            app, ["run", "--org", "acme", "--as-of", "2026-04-18", "--mode", "full"]
        )

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["config"]["refresh_scope"] == "full_history"
        assert payload["config"]["checkpoint_policy"]["resume_from_checkpoint"] is False
        assert payload["config"]["checkpoint_policy"]["persist_checkpoint"] is True
        assert payload["config"]["checkpoint_policy"]["overwrite_checkpoint"] is True
        assert payload["config"]["lock_policy"]["skip_locked_periods"] is False
        assert payload["config"]["lock_policy"]["refresh_locked_periods"] is True

    def test_surfaces_github_auth_failures_separately(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Surface GitHub auth failures separately from invalid configuration errors."""
        # Given
        github_auth_service = create_autospec(
            GitHubAuthService, instance=True, spec_set=True
        )
        github_auth_service.validate_access.side_effect = AuthResolutionError(
            "token rejected"
        )
        monkeypatch.setattr(
            "orgpulse.cli.resolve_auth_token",
            lambda config: ResolvedToken(source=AuthSource.GH_TOKEN, token="env-token"),
        )
        monkeypatch.setattr("orgpulse.cli.Github", lambda auth: object())
        monkeypatch.setattr("orgpulse.cli.Auth.Token", lambda token: object())
        monkeypatch.setattr(
            "orgpulse.cli.GitHubAuthService",
            lambda github_client, auth_source: github_auth_service,
        )

        # When
        result = runner.invoke(app, ["run", "--org", "acme"])

        # Then
        assert result.exit_code == 1
        assert "orgpulse: GitHub authentication failed" in result.stderr
        assert "invalid configuration" not in result.stderr

    def test_surfaces_github_api_failures_separately(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Surface non-auth GitHub API failures separately from auth errors."""
        # Given
        github_auth_service = create_autospec(
            GitHubAuthService, instance=True, spec_set=True
        )
        github_auth_service.validate_access.side_effect = GitHubApiError("rate limited")
        monkeypatch.setattr(
            "orgpulse.cli.resolve_auth_token",
            lambda config: ResolvedToken(source=AuthSource.GH_TOKEN, token="env-token"),
        )
        monkeypatch.setattr("orgpulse.cli.Github", lambda auth: object())
        monkeypatch.setattr("orgpulse.cli.Auth.Token", lambda token: object())
        monkeypatch.setattr(
            "orgpulse.cli.GitHubAuthService",
            lambda github_client, auth_source: github_auth_service,
        )

        # When
        result = runner.invoke(app, ["run", "--org", "acme"])

        # Then
        assert result.exit_code == 1
        assert "orgpulse: GitHub API request failed" in result.stderr
        assert "GitHub authentication failed" not in result.stderr


class TestRunCommandRuntime:
    def test_writes_normalized_raw_snapshots_for_complete_collection(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Write normalized raw snapshots from the CLI flow when collection completes without repo failures."""
        # Given
        pull_request = PullRequestRecord(
            repository_full_name="acme/api",
            number=17,
            title="Add snapshot writer",
            state="closed",
            draft=False,
            merged=True,
            author_login="alice",
            created_at=datetime.fromisoformat("2026-04-09T10:00:00"),
            updated_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            closed_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            merged_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            additions=25,
            deletions=6,
            changed_files=4,
            commits=3,
            html_url="https://example.test/pr/17",
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=datetime.fromisoformat("2026-04-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(pull_request,),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["collection"]["pull_request_count"] == 1
        assert payload["collection"]["failure_count"] == 0
        assert payload["raw_snapshot"]["periods"][0]["key"] == "2026-04"
        assert payload["raw_snapshot"]["periods"][0]["pull_request_count"] == 1
        assert payload["raw_snapshot_skipped_reason"] is None
        assert payload["manifest"]["refreshed_periods"][0]["key"] == "2026-04"
        assert payload["manifest"]["locked_periods"] == []
        assert payload["manifest"]["watermarks"]["collection_window_end_date"] == (
            "2026-04-18"
        )
        assert payload["manifest_skipped_reason"] is None
        assert payload["repo_summary"]["periods"][0]["key"] == "2026-04"
        assert payload["repo_summary"]["periods"][0]["repository_count"] == 1
        assert payload["repo_summary_skipped_reason"] is None
        assert payload["org_metrics"]["target_org"] == "acme"
        assert payload["org_metrics"]["periods"][0]["key"] == "2026-04"
        assert (
            payload["org_metrics"]["periods"][0]["summary"]["merged_pull_request_count"]
            == 1
        )
        assert payload["org_metrics_skipped_reason"] is None
        assert payload["org_summary"]["target_org"] == "acme"
        assert payload["org_summary"]["periods"][0]["key"] == "2026-04"
        assert payload["org_summary_skipped_reason"] is None
        assert payload["metric_validation"]["target_org"] == "acme"
        assert payload["metric_validation"]["periods"][0]["key"] == "2026-04"
        assert payload["metric_validation"]["periods"][0]["raw_pull_request_count"] == 1
        assert payload["metric_validation"]["periods"][0]["valid"] is True
        assert payload["metric_validation"]["periods"][0]["issues"] == []
        assert payload["metric_validation_skipped_reason"] is None
        pull_requests_path = (
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
        )
        manifest_path = tmp_path / "manifest" / "month" / "created_at" / "manifest.json"
        manifest_index_path = tmp_path / "manifest" / "month" / "created_at" / "index.json"
        manifest_readme_path = tmp_path / "manifest" / "month" / "created_at" / "README.md"
        repo_summary_contract_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "contract.json"
        )
        repo_summary_index_path = tmp_path / "repo_summary" / "month" / "created_at" / "index.json"
        repo_summary_readme_path = tmp_path / "repo_summary" / "month" / "created_at" / "README.md"
        repo_summary_latest_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "latest" / "repo_summary.csv"
        )
        repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-04" / "repo_summary.csv"
        )
        org_summary_contract_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "contract.json"
        )
        org_summary_index_path = tmp_path / "org_summary" / "month" / "created_at" / "index.json"
        org_summary_readme_path = tmp_path / "org_summary" / "month" / "created_at" / "README.md"
        org_summary_latest_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "latest" / "summary.json"
        )
        org_summary_latest_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "latest" / "summary.md"
        )
        org_summary_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-04" / "summary.json"
        )
        org_summary_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-04" / "summary.md"
        )
        assert pull_requests_path.exists()
        assert manifest_path.exists()
        assert manifest_index_path.exists()
        assert manifest_readme_path.exists()
        assert repo_summary_contract_path.exists()
        assert repo_summary_index_path.exists()
        assert repo_summary_readme_path.exists()
        assert repo_summary_latest_path.exists()
        assert repo_summary_path.exists()
        assert org_summary_contract_path.exists()
        assert org_summary_index_path.exists()
        assert org_summary_readme_path.exists()
        assert org_summary_latest_json_path.exists()
        assert org_summary_latest_markdown_path.exists()
        assert org_summary_json_path.exists()
        assert org_summary_markdown_path.exists()
        assert "acme/api" in pull_requests_path.read_text(encoding="utf-8")
        assert "acme/api" in repo_summary_path.read_text(encoding="utf-8")
        assert (
            repo_summary_latest_path.read_text(encoding="utf-8")
            == repo_summary_path.read_text(encoding="utf-8")
        )
        assert json.loads(manifest_path.read_text(encoding="utf-8"))["target_org"] == (
            "acme"
        )
        assert json.loads(manifest_index_path.read_text(encoding="utf-8"))["latest"] == {
            "as_of": "2026-04-18",
            "completed_at": "2026-04-18T00:00:00+00:00",
            "manifest_path": "manifest.json",
            "mode": "incremental",
            "refresh_scope": "open_period",
        }
        assert json.loads(repo_summary_index_path.read_text(encoding="utf-8"))["latest"] == {
            "closed": False,
            "end_date": "2026-04-30",
            "key": "2026-04",
            "path": "latest/repo_summary.csv",
            "source_path": "2026-04/repo_summary.csv",
            "start_date": "2026-04-01",
        }
        assert json.loads(org_summary_json_path.read_text(encoding="utf-8")) == {
            "exclude_repos": [],
            "include_repos": [],
            "period": {
                "closed": False,
                "end_date": "2026-04-30",
                "key": "2026-04",
                "start_date": "2026-04-01",
            },
            "period_grain": "month",
            "time_anchor": "created_at",
            "summary": {
                "active_author_count": 1,
                "additions": {
                    "average": 25.0,
                    "count": 1,
                    "median": 25.0,
                    "total": 25,
                },
                "changed_files": {
                    "average": 4.0,
                    "count": 1,
                    "median": 4.0,
                    "total": 4,
                },
                "changed_lines": {
                    "average": 31.0,
                    "count": 1,
                    "median": 31.0,
                    "total": 31,
                },
                "commits": {
                    "average": 3.0,
                    "count": 1,
                    "median": 3.0,
                    "total": 3,
                },
                "deletions": {
                    "average": 6.0,
                    "count": 1,
                    "median": 6.0,
                    "total": 6,
                },
                "merged_pull_request_count": 1,
                "merged_pull_requests_per_active_author": 1.0,
                "pull_request_count": 1,
                "repository_count": 1,
                "time_to_first_review_seconds": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "time_to_merge_seconds": {
                    "average": 255600.0,
                    "count": 1,
                    "median": 255600.0,
                    "total": 255600,
                },
            },
            "target_org": "acme",
        }
        assert (
            org_summary_latest_json_path.read_text(encoding="utf-8")
            == org_summary_json_path.read_text(encoding="utf-8")
        )
        assert (
            org_summary_latest_markdown_path.read_text(encoding="utf-8")
            == org_summary_markdown_path.read_text(encoding="utf-8")
        )
        assert json.loads(org_summary_index_path.read_text(encoding="utf-8"))["latest"] == {
            "closed": False,
            "end_date": "2026-04-30",
            "json_path": "latest/summary.json",
            "key": "2026-04",
            "markdown_path": "latest/summary.md",
            "source_json_path": "2026-04/summary.json",
            "source_markdown_path": "2026-04/summary.md",
            "start_date": "2026-04-01",
        }
        assert (
            org_summary_markdown_path.read_text(encoding="utf-8").splitlines()[0]
            == "# Organization Summary: acme 2026-04"
        )

    def test_rewrites_identical_outputs_safely_on_rerun(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Rewrite identical raw snapshot and manifest outputs without drifting the exported PR data."""
        # Given
        pull_request = PullRequestRecord(
            repository_full_name="acme/api",
            number=17,
            title="Add snapshot writer",
            state="closed",
            draft=False,
            merged=True,
            author_login="alice",
            created_at=datetime.fromisoformat("2026-04-09T10:00:00"),
            updated_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            closed_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            merged_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            additions=25,
            deletions=6,
            changed_files=4,
            commits=3,
            html_url="https://example.test/pr/17",
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=datetime.fromisoformat("2026-04-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(pull_request,),
            failures=(),
        )
        completed_at_values = iter(
            (
                datetime.fromisoformat("2026-04-18T00:00:00+00:00"),
                datetime.fromisoformat("2026-04-19T00:00:00+00:00"),
            )
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(now=lambda: next(completed_at_values)),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        first_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )
        pull_requests_path = (
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
        )
        manifest_path = tmp_path / "manifest" / "month" / "created_at" / "manifest.json"
        repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-04" / "repo_summary.csv"
        )
        first_pull_requests_csv = pull_requests_path.read_text(encoding="utf-8")
        first_repo_summary_csv = repo_summary_path.read_text(encoding="utf-8")
        first_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        second_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert first_result.exit_code == 0
        assert second_result.exit_code == 0
        assert pull_requests_path.read_text(encoding="utf-8") == first_pull_requests_csv
        assert repo_summary_path.read_text(encoding="utf-8") == first_repo_summary_csv
        second_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert first_manifest["refreshed_periods"] == second_manifest["refreshed_periods"]
        assert first_manifest["locked_periods"] == second_manifest["locked_periods"]
        assert (
            first_manifest["last_successful_run"]["completed_at"]
            == "2026-04-18T00:00:00Z"
        )
        assert (
            second_manifest["last_successful_run"]["completed_at"]
            == "2026-04-19T00:00:00Z"
        )

    def test_includes_locked_periods_in_org_rollups_after_incremental_refresh(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Include prior locked periods in org rollups when an incremental run refreshes only the active period."""
        # Given
        previous_config = build_run_config(
            org="acme",
            as_of="2026-03-18",
            output_dir=tmp_path,
        )
        previous_pull_request = PullRequestRecord(
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
        )
        previous_collection = PullRequestCollection(
            window=previous_config.collection_window,
            pull_requests=(previous_pull_request,),
            failures=(),
        )
        previous_snapshot = NormalizedRawSnapshotWriter().write(
            previous_config,
            previous_collection,
        )
        RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-03-18T00:00:00+00:00")
        ).write(
            previous_config,
            previous_collection,
            previous_snapshot,
            repository_count=1,
        )
        RepositorySummaryCsvWriter().write(
            previous_config,
            RepositoryMetricCollectionBuilder().build(
                previous_config,
                PullRequestMetricCollectionBuilder().build(
                    previous_config,
                    previous_snapshot,
                ),
            ),
            refreshed_period_keys=tuple(period.key for period in previous_snapshot.periods),
        )
        OrgSummaryWriter().write(
            previous_config,
            OrganizationMetricCollectionBuilder().build(
                previous_config,
                PullRequestMetricCollectionBuilder().build(
                    previous_config,
                    previous_snapshot,
                ),
            ),
            refreshed_period_keys=tuple(period.key for period in previous_snapshot.periods),
        )
        march_repo_summary = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-03" / "repo_summary.csv"
        )
        march_repo_summary_csv = march_repo_summary.read_text(encoding="utf-8")
        march_org_summary = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-03" / "summary.json"
        )
        march_org_summary_json = march_org_summary.read_text(encoding="utf-8")
        current_pull_request = PullRequestRecord(
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
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=datetime.fromisoformat("2026-04-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(current_pull_request,),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert [period["key"] for period in payload["repo_summary"]["periods"]] == ["2026-04"]
        assert [period["repository_count"] for period in payload["repo_summary"]["periods"]] == [1]
        assert [period["key"] for period in payload["org_metrics"]["periods"]] == [
            "2026-03",
            "2026-04",
        ]
        summaries = {
            period["key"]: period["summary"] for period in payload["org_metrics"]["periods"]
        }
        assert summaries["2026-03"]["repository_count"] == 1
        assert summaries["2026-03"]["merged_pull_request_count"] == 1
        assert summaries["2026-04"]["repository_count"] == 1
        assert summaries["2026-04"]["merged_pull_request_count"] == 1
        assert [period["key"] for period in payload["org_summary"]["periods"]] == ["2026-04"]
        assert [period["key"] for period in payload["manifest"]["locked_periods"]] == [
            "2026-03"
        ]
        assert [period["key"] for period in payload["metric_validation"]["periods"]] == [
            "2026-03",
            "2026-04",
        ]
        assert [period["raw_pull_request_count"] for period in payload["metric_validation"]["periods"]] == [
            1,
            1,
        ]
        assert all(period["valid"] for period in payload["metric_validation"]["periods"])
        assert march_org_summary.read_text(encoding="utf-8") == march_org_summary_json
        april_repo_summary = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-04" / "repo_summary.csv"
        )
        assert march_repo_summary.read_text(encoding="utf-8") == march_repo_summary_csv
        assert "acme/web" in april_repo_summary.read_text(encoding="utf-8")

    def test_refreshes_only_open_period_outputs_on_incremental_runs(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Refresh only the active-period outputs on incremental runs while preserving locked history across all exports."""
        # Given
        previous_config = build_run_config(
            org="acme",
            as_of="2026-03-18",
            output_dir=tmp_path,
        )
        previous_pull_request = PullRequestRecord(
            repository_full_name="acme/api",
            number=12,
            title="Lock March history",
            state="closed",
            draft=False,
            merged=True,
            author_login="alice",
            created_at=datetime.fromisoformat("2026-03-10T09:00:00"),
            updated_at=datetime.fromisoformat("2026-03-14T12:00:00"),
            closed_at=datetime.fromisoformat("2026-03-14T12:00:00"),
            merged_at=datetime.fromisoformat("2026-03-14T12:00:00"),
            additions=14,
            deletions=2,
            changed_files=3,
            commits=2,
            html_url="https://example.test/pr/12",
            reviews=(
                PullRequestReviewRecord(
                    review_id=501,
                    state="APPROVED",
                    author_login="reviewer-a",
                    submitted_at=datetime.fromisoformat("2026-03-11T10:00:00"),
                    commit_id="commit-501",
                ),
            ),
            timeline_events=(
                PullRequestTimelineEventRecord(
                    event_id=601,
                    event="review_requested",
                    actor_login="alice",
                    created_at=datetime.fromisoformat("2026-03-10T10:00:00"),
                    requested_reviewer_login="reviewer-a",
                    requested_team_name=None,
                ),
            ),
        )
        previous_collection = PullRequestCollection(
            window=previous_config.collection_window,
            pull_requests=(previous_pull_request,),
            failures=(),
        )
        previous_snapshot = NormalizedRawSnapshotWriter().write(
            previous_config,
            previous_collection,
        )
        RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-03-18T00:00:00+00:00")
        ).write(
            previous_config,
            previous_collection,
            previous_snapshot,
            repository_count=1,
        )
        previous_pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            previous_config,
            previous_snapshot,
        )
        RepositorySummaryCsvWriter().write(
            previous_config,
            RepositoryMetricCollectionBuilder().build(
                previous_config,
                previous_pull_request_metrics,
            ),
            refreshed_period_keys=tuple(period.key for period in previous_snapshot.periods),
        )
        OrgSummaryWriter().write(
            previous_config,
            OrganizationMetricCollectionBuilder().build(
                previous_config,
                previous_pull_request_metrics,
            ),
            refreshed_period_keys=tuple(period.key for period in previous_snapshot.periods),
        )
        locked_period_dir = tmp_path / "raw" / "month" / "created_at" / "2026-03"
        locked_pull_requests_path = locked_period_dir / "pull_requests.csv"
        locked_reviews_path = locked_period_dir / "pull_request_reviews.csv"
        locked_timeline_events_path = (
            locked_period_dir / "pull_request_timeline_events.csv"
        )
        locked_pull_requests_csv = locked_pull_requests_path.read_text(
            encoding="utf-8"
        )
        locked_reviews_csv = locked_reviews_path.read_text(encoding="utf-8")
        locked_timeline_events_csv = locked_timeline_events_path.read_text(
            encoding="utf-8"
        )
        locked_repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-03" / "repo_summary.csv"
        )
        locked_repo_summary_csv = locked_repo_summary_path.read_text(encoding="utf-8")
        locked_org_summary_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-03" / "summary.json"
        )
        locked_org_summary_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-03" / "summary.md"
        )
        locked_org_summary_json = locked_org_summary_json_path.read_text(
            encoding="utf-8"
        )
        locked_org_summary_markdown = locked_org_summary_markdown_path.read_text(
            encoding="utf-8"
        )
        current_pull_request = PullRequestRecord(
            repository_full_name="acme/web",
            number=22,
            title="Refresh April work",
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
            html_url="https://example.test/pr/22",
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=datetime.fromisoformat("2026-04-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(current_pull_request,),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["collection"]["window"] == {
            "scope": "open_period",
            "start_date": "2026-04-01",
            "end_date": "2026-04-18",
        }
        assert [period["key"] for period in payload["raw_snapshot"]["periods"]] == [
            "2026-04"
        ]
        assert [period["key"] for period in payload["manifest"]["refreshed_periods"]] == [
            "2026-04"
        ]
        assert [period["key"] for period in payload["manifest"]["locked_periods"]] == [
            "2026-03"
        ]
        assert [period["key"] for period in payload["repo_summary"]["periods"]] == [
            "2026-04"
        ]
        assert [period["key"] for period in payload["org_summary"]["periods"]] == [
            "2026-04"
        ]
        assert locked_pull_requests_path.read_text(encoding="utf-8") == locked_pull_requests_csv
        assert locked_reviews_path.read_text(encoding="utf-8") == locked_reviews_csv
        assert (
            locked_timeline_events_path.read_text(encoding="utf-8")
            == locked_timeline_events_csv
        )
        assert locked_repo_summary_path.read_text(encoding="utf-8") == locked_repo_summary_csv
        assert (
            locked_org_summary_json_path.read_text(encoding="utf-8")
            == locked_org_summary_json
        )
        assert (
            locked_org_summary_markdown_path.read_text(encoding="utf-8")
            == locked_org_summary_markdown
        )
        refreshed_repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-04" / "repo_summary.csv"
        )
        refreshed_org_summary_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-04" / "summary.json"
        )
        refreshed_org_summary_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-04" / "summary.md"
        )
        repo_summary_index_path = tmp_path / "repo_summary" / "month" / "created_at" / "index.json"
        repo_summary_latest_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "latest" / "repo_summary.csv"
        )
        org_summary_index_path = tmp_path / "org_summary" / "month" / "created_at" / "index.json"
        org_summary_latest_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "latest" / "summary.json"
        )
        org_summary_latest_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "latest" / "summary.md"
        )
        manifest_index_path = tmp_path / "manifest" / "month" / "created_at" / "index.json"
        refreshed_repo_summary_csv = refreshed_repo_summary_path.read_text(
            encoding="utf-8"
        )
        refreshed_org_summary_payload = json.loads(
            refreshed_org_summary_json_path.read_text(encoding="utf-8")
        )
        assert "acme/web" in refreshed_repo_summary_csv
        assert "acme/api" not in refreshed_repo_summary_csv
        assert refreshed_org_summary_payload["period"]["key"] == "2026-04"
        assert refreshed_org_summary_payload["summary"]["repository_count"] == 1
        assert (
            refreshed_org_summary_payload["summary"]["merged_pull_request_count"] == 1
        )
        assert (
            refreshed_org_summary_markdown_path.read_text(encoding="utf-8").splitlines()[0]
            == "# Organization Summary: acme 2026-04"
        )
        assert json.loads(repo_summary_index_path.read_text(encoding="utf-8"))["history"] == [
            {
                "closed": True,
                "end_date": "2026-03-31",
                "key": "2026-03",
                "path": "2026-03/repo_summary.csv",
                "start_date": "2026-03-01",
            },
            {
                "closed": False,
                "end_date": "2026-04-30",
                "key": "2026-04",
                "path": "2026-04/repo_summary.csv",
                "start_date": "2026-04-01",
            },
        ]
        assert (
            repo_summary_latest_path.read_text(encoding="utf-8")
            == refreshed_repo_summary_csv
        )
        assert json.loads(org_summary_index_path.read_text(encoding="utf-8"))["history"] == [
            {
                "closed": True,
                "end_date": "2026-03-31",
                "json_path": "2026-03/summary.json",
                "key": "2026-03",
                "markdown_path": "2026-03/summary.md",
                "start_date": "2026-03-01",
            },
            {
                "closed": False,
                "end_date": "2026-04-30",
                "json_path": "2026-04/summary.json",
                "key": "2026-04",
                "markdown_path": "2026-04/summary.md",
                "start_date": "2026-04-01",
            },
        ]
        assert (
            org_summary_latest_json_path.read_text(encoding="utf-8")
            == refreshed_org_summary_json_path.read_text(encoding="utf-8")
        )
        assert (
            org_summary_latest_markdown_path.read_text(encoding="utf-8")
            == refreshed_org_summary_markdown_path.read_text(encoding="utf-8")
        )
        assert json.loads(manifest_index_path.read_text(encoding="utf-8"))["history"] == {
            "locked_periods": [
                {
                    "closed": True,
                    "end_date": "2026-03-31",
                    "key": "2026-03",
                    "start_date": "2026-03-01",
                }
            ],
            "refreshed_periods": [
                {
                    "closed": False,
                    "end_date": "2026-04-30",
                    "key": "2026-04",
                    "start_date": "2026-04-01",
                }
            ],
        }
        validation_periods = {
            period["key"]: period for period in payload["metric_validation"]["periods"]
        }
        assert validation_periods["2026-03"]["raw_pull_request_count"] == 1
        assert validation_periods["2026-03"]["raw_review_count"] == 1
        assert validation_periods["2026-03"]["raw_timeline_event_count"] == 1
        assert validation_periods["2026-04"]["raw_pull_request_count"] == 1
        assert validation_periods["2026-04"]["raw_review_count"] == 0
        assert validation_periods["2026-04"]["raw_timeline_event_count"] == 0
        assert [period["key"] for period in payload["org_metrics"]["periods"]] == [
            "2026-03",
            "2026-04",
        ]
        assert all(period["valid"] for period in payload["metric_validation"]["periods"])

    def test_recalculates_only_requested_closed_period_outputs_on_backfill_runs(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Recalculate only the requested closed-period outputs on backfill while preserving unrelated locked history."""
        # Given
        previous_config = build_run_config(
            org="acme",
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-02-01",
            backfill_end="2026-04-30",
            output_dir=tmp_path,
        )
        previous_collection = PullRequestCollection(
            window=previous_config.collection_window,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=31,
                    title="Keep February locked",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-02-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-02-12T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-02-12T09:00:00"),
                    merged_at=datetime.fromisoformat("2026-02-12T09:00:00"),
                    additions=9,
                    deletions=2,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/31",
                    reviews=(
                        PullRequestReviewRecord(
                            review_id=701,
                            state="APPROVED",
                            author_login="reviewer-a",
                            submitted_at=datetime.fromisoformat(
                                "2026-02-10T12:00:00"
                            ),
                            commit_id="commit-701",
                        ),
                    ),
                ),
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=32,
                    title="Overwrite stale March work",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-03-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-03-12T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-03-12T09:00:00"),
                    merged_at=datetime.fromisoformat("2026-03-12T09:00:00"),
                    additions=11,
                    deletions=3,
                    changed_files=2,
                    commits=2,
                    html_url="https://example.test/pr/32",
                ),
                PullRequestRecord(
                    repository_full_name="acme/web",
                    number=33,
                    title="Overwrite stale April work",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    merged_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    additions=12,
                    deletions=4,
                    changed_files=3,
                    commits=2,
                    html_url="https://example.test/pr/33",
                ),
            ),
            failures=(),
        )
        previous_snapshot = NormalizedRawSnapshotWriter().write(
            previous_config,
            previous_collection,
        )
        RunManifestWriter(
            now=lambda: datetime.fromisoformat("2026-05-18T00:00:00+00:00")
        ).write(
            previous_config,
            previous_collection,
            previous_snapshot,
            repository_count=2,
        )
        previous_pull_request_metrics = PullRequestMetricCollectionBuilder().build(
            previous_config,
            previous_snapshot,
        )
        RepositorySummaryCsvWriter().write(
            previous_config,
            RepositoryMetricCollectionBuilder().build(
                previous_config,
                previous_pull_request_metrics,
            ),
            refreshed_period_keys=tuple(period.key for period in previous_snapshot.periods),
        )
        OrgSummaryWriter().write(
            previous_config,
            OrganizationMetricCollectionBuilder().build(
                previous_config,
                previous_pull_request_metrics,
            ),
            refreshed_period_keys=tuple(period.key for period in previous_snapshot.periods),
        )
        february_pull_requests_path = (
            tmp_path / "raw" / "month" / "created_at" / "2026-02" / "pull_requests.csv"
        )
        march_pull_requests_path = (
            tmp_path / "raw" / "month" / "created_at" / "2026-03" / "pull_requests.csv"
        )
        april_pull_requests_path = (
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
        )
        february_pull_requests_csv = february_pull_requests_path.read_text(
            encoding="utf-8"
        )
        march_pull_requests_csv = march_pull_requests_path.read_text(encoding="utf-8")
        april_pull_requests_csv = april_pull_requests_path.read_text(encoding="utf-8")
        february_repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-02" / "repo_summary.csv"
        )
        march_repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-03" / "repo_summary.csv"
        )
        april_repo_summary_path = (
            tmp_path / "repo_summary" / "month" / "created_at" / "2026-04" / "repo_summary.csv"
        )
        february_repo_summary_csv = february_repo_summary_path.read_text(encoding="utf-8")
        march_repo_summary_csv = march_repo_summary_path.read_text(encoding="utf-8")
        april_repo_summary_csv = april_repo_summary_path.read_text(encoding="utf-8")
        february_org_summary_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-02" / "summary.json"
        )
        march_org_summary_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-03" / "summary.json"
        )
        april_org_summary_json_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-04" / "summary.json"
        )
        february_org_summary_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-02" / "summary.md"
        )
        march_org_summary_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-03" / "summary.md"
        )
        april_org_summary_markdown_path = (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-04" / "summary.md"
        )
        february_org_summary_json = february_org_summary_json_path.read_text(
            encoding="utf-8"
        )
        march_org_summary_json = march_org_summary_json_path.read_text(encoding="utf-8")
        april_org_summary_json = april_org_summary_json_path.read_text(encoding="utf-8")
        february_org_summary_markdown = february_org_summary_markdown_path.read_text(
            encoding="utf-8"
        )
        march_org_summary_markdown = march_org_summary_markdown_path.read_text(
            encoding="utf-8"
        )
        april_org_summary_markdown = april_org_summary_markdown_path.read_text(
            encoding="utf-8"
        )
        refreshed_april_pull_request = PullRequestRecord(
            repository_full_name="acme/web",
            number=44,
            title="Refresh April work",
            state="closed",
            draft=False,
            merged=True,
            author_login="carol",
            created_at=datetime.fromisoformat("2026-04-18T09:00:00"),
            updated_at=datetime.fromisoformat("2026-04-20T09:00:00"),
            closed_at=datetime.fromisoformat("2026-04-20T09:00:00"),
            merged_at=datetime.fromisoformat("2026-04-20T09:00:00"),
            additions=20,
            deletions=5,
            changed_files=4,
            commits=3,
            html_url="https://example.test/pr/44",
            reviews=(
                PullRequestReviewRecord(
                    review_id=801,
                    state="APPROVED",
                    author_login="reviewer-b",
                    submitted_at=datetime.fromisoformat("2026-04-18T12:00:00"),
                    commit_id="commit-801",
                ),
            ),
            timeline_events=(
                PullRequestTimelineEventRecord(
                    event_id=901,
                    event="review_requested",
                    actor_login="carol",
                    created_at=datetime.fromisoformat("2026-04-18T10:00:00"),
                    requested_reviewer_login="reviewer-b",
                    requested_team_name=None,
                ),
            ),
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.BOUNDED_BACKFILL,
                start_date=datetime.fromisoformat("2026-03-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-30T00:00:00").date(),
            ),
            pull_requests=(refreshed_april_pull_request,),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-05-19T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-05-18",
                "--mode",
                "backfill",
                "--backfill-start",
                "2026-03-01",
                "--backfill-end",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["collection"]["window"] == {
            "scope": "bounded_backfill",
            "start_date": "2026-03-01",
            "end_date": "2026-04-30",
        }
        assert [period["key"] for period in payload["raw_snapshot"]["periods"]] == [
            "2026-03",
            "2026-04",
        ]
        assert [period["pull_request_count"] for period in payload["raw_snapshot"]["periods"]] == [
            0,
            1,
        ]
        assert [period["key"] for period in payload["manifest"]["refreshed_periods"]] == [
            "2026-03",
            "2026-04",
        ]
        assert [period["key"] for period in payload["manifest"]["locked_periods"]] == [
            "2026-02",
            "2026-03",
            "2026-04",
        ]
        assert [period["key"] for period in payload["repo_summary"]["periods"]] == [
            "2026-03",
            "2026-04",
        ]
        assert [period["repository_count"] for period in payload["repo_summary"]["periods"]] == [
            0,
            1,
        ]
        assert [period["key"] for period in payload["org_summary"]["periods"]] == [
            "2026-03",
            "2026-04",
        ]
        assert (
            february_pull_requests_path.read_text(encoding="utf-8")
            == february_pull_requests_csv
        )
        assert (
            february_repo_summary_path.read_text(encoding="utf-8")
            == february_repo_summary_csv
        )
        assert (
            february_org_summary_json_path.read_text(encoding="utf-8")
            == february_org_summary_json
        )
        assert (
            february_org_summary_markdown_path.read_text(encoding="utf-8")
            == february_org_summary_markdown
        )
        assert (
            march_pull_requests_path.read_text(encoding="utf-8")
            == f"{','.join(PULL_REQUEST_FIELDNAMES)}\n"
        )
        assert march_pull_requests_path.read_text(encoding="utf-8") != march_pull_requests_csv
        assert (
            march_repo_summary_path.read_text(encoding="utf-8")
            == f"{','.join(REPOSITORY_SUMMARY_CSV_FIELDNAMES)}\n"
        )
        assert march_repo_summary_path.read_text(encoding="utf-8") != march_repo_summary_csv
        refreshed_march_org_summary = json.loads(
            march_org_summary_json_path.read_text(encoding="utf-8")
        )
        assert march_org_summary_json_path.read_text(encoding="utf-8") != march_org_summary_json
        assert (
            march_org_summary_markdown_path.read_text(encoding="utf-8")
            != march_org_summary_markdown
        )
        assert refreshed_march_org_summary["period"]["key"] == "2026-03"
        assert refreshed_march_org_summary["summary"]["pull_request_count"] == 0
        assert refreshed_march_org_summary["summary"]["repository_count"] == 0
        refreshed_april_csv = april_pull_requests_path.read_text(encoding="utf-8")
        assert refreshed_april_csv != april_pull_requests_csv
        assert "Refresh April work" in refreshed_april_csv
        assert "Overwrite stale April work" not in refreshed_april_csv
        refreshed_april_repo_summary = april_repo_summary_path.read_text(encoding="utf-8")
        assert refreshed_april_repo_summary != april_repo_summary_csv
        assert "acme/web" in refreshed_april_repo_summary
        assert "acme/api" not in refreshed_april_repo_summary
        refreshed_april_org_summary = json.loads(
            april_org_summary_json_path.read_text(encoding="utf-8")
        )
        assert april_org_summary_json_path.read_text(encoding="utf-8") != april_org_summary_json
        assert (
            april_org_summary_markdown_path.read_text(encoding="utf-8")
            != april_org_summary_markdown
        )
        assert refreshed_april_org_summary["period"]["key"] == "2026-04"
        assert refreshed_april_org_summary["summary"]["repository_count"] == 1
        assert (
            refreshed_april_org_summary["summary"]["merged_pull_request_count"] == 1
        )
        assert [period["key"] for period in payload["org_metrics"]["periods"]] == [
            "2026-02",
            "2026-03",
            "2026-04",
        ]
        summaries = {
            period["key"]: period["summary"] for period in payload["org_metrics"]["periods"]
        }
        assert summaries["2026-02"]["merged_pull_request_count"] == 1
        assert summaries["2026-03"]["pull_request_count"] == 0
        assert summaries["2026-04"]["merged_pull_request_count"] == 1
        validation_periods = {
            period["key"]: period for period in payload["metric_validation"]["periods"]
        }
        assert validation_periods["2026-02"]["raw_pull_request_count"] == 1
        assert validation_periods["2026-02"]["raw_review_count"] == 1
        assert validation_periods["2026-03"]["raw_pull_request_count"] == 0
        assert validation_periods["2026-03"]["raw_review_count"] == 0
        assert validation_periods["2026-04"]["raw_pull_request_count"] == 1
        assert validation_periods["2026-04"]["raw_review_count"] == 1
        assert validation_periods["2026-04"]["raw_timeline_event_count"] == 1
        assert all(period["valid"] for period in payload["metric_validation"]["periods"])
        assert json.loads(
            (
                tmp_path / "repo_summary" / "month" / "created_at" / "index.json"
            ).read_text(encoding="utf-8")
        )["history"] == [
            {
                "closed": True,
                "end_date": "2026-02-28",
                "key": "2026-02",
                "path": "2026-02/repo_summary.csv",
                "start_date": "2026-02-01",
            },
            {
                "closed": True,
                "end_date": "2026-03-31",
                "key": "2026-03",
                "path": "2026-03/repo_summary.csv",
                "start_date": "2026-03-01",
            },
            {
                "closed": True,
                "end_date": "2026-04-30",
                "key": "2026-04",
                "path": "2026-04/repo_summary.csv",
                "start_date": "2026-04-01",
            },
        ]
        assert json.loads(
            (
                tmp_path / "org_summary" / "month" / "created_at" / "index.json"
            ).read_text(encoding="utf-8")
        )["latest"] == {
            "closed": True,
            "end_date": "2026-04-30",
            "json_path": "latest/summary.json",
            "key": "2026-04",
            "markdown_path": "latest/summary.md",
            "source_json_path": "2026-04/summary.json",
            "source_markdown_path": "2026-04/summary.md",
            "start_date": "2026-04-01",
        }
        assert json.loads(
            (
                tmp_path / "manifest" / "month" / "created_at" / "index.json"
            ).read_text(encoding="utf-8")
        )["history"] == {
            "locked_periods": [
                {
                    "closed": True,
                    "end_date": "2026-02-28",
                    "key": "2026-02",
                    "start_date": "2026-02-01",
                },
                {
                    "closed": True,
                    "end_date": "2026-03-31",
                    "key": "2026-03",
                    "start_date": "2026-03-01",
                },
                {
                    "closed": True,
                    "end_date": "2026-04-30",
                    "key": "2026-04",
                    "start_date": "2026-04-01",
                },
            ],
            "refreshed_periods": [
                {
                    "closed": True,
                    "end_date": "2026-03-31",
                    "key": "2026-03",
                    "start_date": "2026-03-01",
                },
                {
                    "closed": True,
                    "end_date": "2026-04-30",
                    "key": "2026-04",
                    "start_date": "2026-04-01",
                },
            ],
        }

    def test_writes_empty_org_summary_exports_for_backfill_periods_without_pull_requests(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Write zero-valued org summary files when a backfill period has no pull requests to export."""
        # Given
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.BOUNDED_BACKFILL,
                start_date=datetime.fromisoformat("2026-01-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-01-31T00:00:00").date(),
            ),
            pull_requests=(),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--mode",
                "backfill",
                "--backfill-start",
                "2026-01-01",
                "--backfill-end",
                "2026-01-31",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert [period["key"] for period in payload["raw_snapshot"]["periods"]] == [
            "2026-01"
        ]
        assert payload["raw_snapshot"]["periods"][0]["pull_request_count"] == 0
        assert [period["key"] for period in payload["org_summary"]["periods"]] == [
            "2026-01"
        ]
        summary_payload = json.loads(
            (
                tmp_path / "org_summary" / "month" / "created_at" / "2026-01" / "summary.json"
            ).read_text(encoding="utf-8")
        )
        assert summary_payload == {
            "exclude_repos": [],
            "include_repos": [],
            "period": {
                "closed": True,
                "end_date": "2026-01-31",
                "key": "2026-01",
                "start_date": "2026-01-01",
            },
            "period_grain": "month",
            "time_anchor": "created_at",
            "summary": {
                "active_author_count": 0,
                "additions": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "changed_files": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "changed_lines": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "commits": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "deletions": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "merged_pull_request_count": 0,
                "merged_pull_requests_per_active_author": None,
                "pull_request_count": 0,
                "repository_count": 0,
                "time_to_first_review_seconds": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
                "time_to_merge_seconds": {
                    "average": None,
                    "count": 0,
                    "median": None,
                    "total": 0,
                },
            },
            "target_org": "acme",
        }
        assert "Merged pull requests per active author: n/a" in (
            tmp_path / "org_summary" / "month" / "created_at" / "2026-01" / "summary.md"
        ).read_text(encoding="utf-8")

    def test_reports_metric_validation_failures_without_skipping_outputs(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Report validation issues for inconsistent timing data while still emitting the run outputs."""
        # Given
        pull_request = PullRequestRecord(
            repository_full_name="acme/api",
            number=88,
            title="Carry invalid merge timing",
            state="closed",
            draft=False,
            merged=True,
            author_login="alice",
            created_at=datetime.fromisoformat("2026-04-12T12:00:00"),
            updated_at=datetime.fromisoformat("2026-04-12T12:00:00"),
            closed_at=datetime.fromisoformat("2026-04-12T12:00:00"),
            merged_at=datetime.fromisoformat("2026-04-12T10:00:00"),
            additions=3,
            deletions=1,
            changed_files=1,
            commits=1,
            html_url="https://example.test/pr/88",
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=datetime.fromisoformat("2026-04-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(pull_request,),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["repo_summary"]["periods"][0]["repository_count"] == 1
        assert payload["repo_summary_skipped_reason"] is None
        assert payload["org_metrics"]["periods"][0]["summary"]["merged_pull_request_count"] == 1
        assert payload["metric_validation_skipped_reason"] is None
        assert payload["metric_validation"]["periods"][0]["valid"] is False
        assert [
            issue["code"]
            for issue in payload["metric_validation"]["periods"][0]["issues"]
        ] == ["merged_pr_merge_before_creation"]

    def test_prunes_stale_period_outputs_and_overwrites_manifest_on_full_rerun(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Prune stale full-history raw outputs and overwrite the manifest with only the surviving periods."""
        # Given
        stale_period_dir = tmp_path / "raw" / "month" / "created_at" / "2026-03"
        stale_period_dir.mkdir(parents=True)
        (stale_period_dir / "pull_requests.csv").write_text(
            "stale snapshot\n",
            encoding="utf-8",
        )
        stale_org_summary_dir = tmp_path / "org_summary" / "month" / "created_at" / "2026-03"
        stale_org_summary_dir.mkdir(parents=True)
        (stale_org_summary_dir / "summary.json").write_text(
            json.dumps({"target_org": "stale"}),
            encoding="utf-8",
        )
        stale_manifest_path = tmp_path / "manifest" / "month" / "created_at" / "manifest.json"
        stale_manifest_path.parent.mkdir(parents=True)
        stale_manifest_path.write_text(
            json.dumps({"target_org": "stale"}),
            encoding="utf-8",
        )
        pull_request = PullRequestRecord(
            repository_full_name="acme/api",
            number=30,
            title="Rewrite current period only",
            state="closed",
            draft=False,
            merged=True,
            author_login="alice",
            created_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            updated_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            closed_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            merged_at=datetime.fromisoformat("2026-04-12T09:00:00"),
            additions=12,
            deletions=4,
            changed_files=3,
            commits=2,
            html_url="https://example.test/pr/30",
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(),
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(pull_request,),
            failures=(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(
                inventory=inventory,
                collection=collection,
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: NormalizedRawSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: RunManifestWriter(
                now=lambda: datetime.fromisoformat("2026-04-18T00:00:00+00:00")
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: OrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: RepositorySummaryCsvWriter(),
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--mode",
                "full",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert stale_period_dir.exists() is False
        assert stale_org_summary_dir.exists() is False
        assert [period["key"] for period in payload["manifest"]["refreshed_periods"]] == [
            "2026-04"
        ]
        assert payload["manifest"]["locked_periods"] == []
        assert payload["manifest"]["watermarks"]["latest_locked_period_end_date"] is None
        assert [period["key"] for period in payload["repo_summary"]["periods"]] == [
            "2026-04"
        ]
        assert json.loads(stale_manifest_path.read_text(encoding="utf-8"))[
            "target_org"
        ] == "acme"
        assert json.loads(
            (
                tmp_path / "repo_summary" / "month" / "created_at" / "index.json"
            ).read_text(encoding="utf-8")
        )["history"] == [
            {
                "closed": False,
                "end_date": "2026-04-30",
                "key": "2026-04",
                "path": "2026-04/repo_summary.csv",
                "start_date": "2026-04-01",
            }
        ]
        assert json.loads(
            (
                tmp_path / "org_summary" / "month" / "created_at" / "index.json"
            ).read_text(encoding="utf-8")
        )["history"] == [
            {
                "closed": False,
                "end_date": "2026-04-30",
                "json_path": "2026-04/summary.json",
                "key": "2026-04",
                "markdown_path": "2026-04/summary.md",
                "start_date": "2026-04-01",
            }
        ]

    def test_skips_snapshot_writes_when_repo_collection_has_failures(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Skip raw snapshot persistence when repo-scoped failures would make the output incomplete."""
        # Given
        existing_snapshot = tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
        existing_snapshot.parent.mkdir(parents=True)
        existing_snapshot.write_text("existing snapshot\n", encoding="utf-8")
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.OPEN_PERIOD,
                start_date=datetime.fromisoformat("2026-04-01T00:00:00").date(),
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(),
            failures=(
                RepositoryCollectionFailure(
                    repository_full_name="acme/web",
                    operation="pull_requests",
                    status_code=503,
                    retriable=True,
                    message="Service unavailable",
                ),
            ),
        )
        monkeypatch.setattr(
            "orgpulse.cli.GitHubIngestionService",
            lambda github_client: FakeCliIngestionService(collection=collection),
        )
        monkeypatch.setattr(
            "orgpulse.cli.NormalizedRawSnapshotWriter",
            lambda: UnexpectedSnapshotWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RunManifestWriter",
            lambda: UnexpectedManifestWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.OrgSummaryWriter",
            lambda: UnexpectedOrgSummaryWriter(),
        )
        monkeypatch.setattr(
            "orgpulse.cli.RepositorySummaryCsvWriter",
            lambda: UnexpectedRepositorySummaryWriter(),
        )
        existing_manifest = tmp_path / "manifest" / "month" / "created_at" / "manifest.json"
        existing_manifest.parent.mkdir(parents=True)
        existing_manifest.write_text(
            json.dumps({"target_org": "acme", "status": "previous"}),
            encoding="utf-8",
        )

        # When
        result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["collection"]["failure_count"] == 1
        assert payload["raw_snapshot"] is None
        assert (
            payload["raw_snapshot_skipped_reason"] == "repository_collection_failures"
        )
        assert payload["manifest"] is None
        assert payload["manifest_skipped_reason"] == "repository_collection_failures"
        assert payload["repo_summary"] is None
        assert payload["repo_summary_skipped_reason"] == "repository_collection_failures"
        assert payload["org_metrics"] is None
        assert payload["org_metrics_skipped_reason"] == "repository_collection_failures"
        assert payload["org_summary"] is None
        assert payload["org_summary_skipped_reason"] == "repository_collection_failures"
        assert existing_snapshot.read_text(encoding="utf-8") == "existing snapshot\n"
        assert json.loads(existing_manifest.read_text(encoding="utf-8")) == {
            "target_org": "acme",
            "status": "previous",
        }
