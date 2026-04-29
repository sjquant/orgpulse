from __future__ import annotations

# ruff: noqa: F403,F405
from .helpers.cli import *


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

