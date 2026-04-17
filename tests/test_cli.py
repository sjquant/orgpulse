from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from orgpulse.cli import app, build_run_config
from orgpulse.config import get_settings
from orgpulse.errors import AuthResolutionError, GitHubApiError
from orgpulse.github_auth import GitHubAuthService
from orgpulse.models import AuthSource, GitHubTargetContext, PeriodGrain, RunMode


@pytest.fixture(scope="module")
def runner() -> CliRunner:
    """Provide a reusable Typer CLI runner for pytest-based tests."""
    return CliRunner()


@pytest.fixture
def reset_settings_cache() -> None:
    """Clear cached settings around each test so env overrides remain deterministic."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def stub_github_auth_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the GitHub auth boundary for CLI tests that focus on config behavior."""
    github_auth_service = create_autospec(GitHubAuthService, instance=True, spec_set=True)
    github_auth_service.validate_access.side_effect = (
        lambda config: GitHubTargetContext(
            auth_source=AuthSource.GH_TOKEN,
            viewer_login="test-user",
            organization_login=config.org,
        )
    )

    monkeypatch.setattr("orgpulse.cli.GitHubAuthService", lambda: github_auth_service)


class TestRunConfigParsing:
    def test_parses_incremental_run_defaults(self, reset_settings_cache: None) -> None:
        """Parse the default incremental run configuration from explicit input."""
        # Given

        # When
        config = build_run_config(org="acme")

        # Then
        assert config.org == "acme"
        assert config.period == PeriodGrain.MONTH
        assert config.mode == RunMode.INCREMENTAL
        assert config.output_dir.as_posix() == "output"
        assert config.include_repos == ()
        assert config.exclude_repos == ()

    def test_loads_settings_from_environment(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        stub_github_auth_service: None,
    ) -> None:
        """Load run configuration defaults from ORGPULSE-prefixed environment variables."""
        # Given
        env = {
            "ORGPULSE_ORG": "env-acme",
            "ORGPULSE_PERIOD": "week",
            "ORGPULSE_OUTPUT_DIR": "env-output",
        }

        # When
        result = runner.invoke(app, ["run"], env=env)

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["config"]["org"] == "env-acme"
        assert payload["config"]["period"] == "week"
        assert payload["config"]["output_dir"] == "env-output"
        assert payload["github"]["organization_login"] == "env-acme"

    def test_cli_options_override_environment_defaults(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        stub_github_auth_service: None,
    ) -> None:
        """Prefer explicit CLI values over ORGPULSE environment defaults."""
        # Given
        env = {
            "ORGPULSE_ORG": "env-acme",
            "ORGPULSE_PERIOD": "month",
        }

        # When
        result = runner.invoke(app, ["run", "--org", "cli-acme", "--period", "week"], env=env)

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["config"]["org"] == "cli-acme"
        assert payload["config"]["period"] == "week"
        assert payload["github"]["organization_login"] == "cli-acme"

    def test_parses_backfill_bounds(self, reset_settings_cache: None) -> None:
        """Parse backfill mode when both inclusive date bounds are provided."""
        # Given

        # When
        config = build_run_config(
            org="acme",
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
        assert config.include_repos == ("api",)
        assert config.exclude_repos == ("legacy",)

    def test_rejects_repo_filters_for_another_org(self, reset_settings_cache: None) -> None:
        """Reject fully qualified repo filters that target a different organization."""
        # Given

        # When
        with pytest.raises(ValidationError, match="repo filter owner must match target org 'acme'"):
            build_run_config(org="acme", include_repos=["other-org/api"])

        # Then

    def test_rejects_backfill_without_bounds(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        stub_github_auth_service: None,
    ) -> None:
        """Reject backfill mode when one or both required date bounds are missing."""
        # Given

        # When
        result = runner.invoke(app, ["run", "--org", "acme", "--mode", "backfill"])

        # Then
        assert result.exit_code == 2
        assert "backfill mode requires both --backfill-start and --backfill-end" in result.stderr

    def test_rejects_overlapping_repo_filters(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        stub_github_auth_service: None,
    ) -> None:
        """Reject configurations where the same repo is both included and excluded."""
        # Given

        # When
        result = runner.invoke(
            app,
            ["run", "--org", "acme", "--repo", "platform", "--exclude-repo", "platform"],
        )

        # Then
        assert result.exit_code == 2
        assert "repo filters overlap across include and exclude lists: platform" in result.stderr

    def test_rejects_backfill_dates_for_non_backfill_mode(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        stub_github_auth_service: None,
    ) -> None:
        """Reject backfill date options when the run mode is not backfill."""
        # Given

        # When
        result = runner.invoke(app, ["run", "--org", "acme", "--backfill-start", "2026-01-01"])

        # Then
        assert result.exit_code == 2
        assert "backfill date bounds are only valid when --mode backfill is selected" in result.stderr

    def test_surfaces_github_auth_failures_separately(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Surface GitHub auth failures separately from invalid configuration errors."""
        # Given
        github_auth_service = create_autospec(GitHubAuthService, instance=True, spec_set=True)
        github_auth_service.validate_access.side_effect = AuthResolutionError("token rejected")
        monkeypatch.setattr("orgpulse.cli.GitHubAuthService", lambda: github_auth_service)

        # When
        result = runner.invoke(app, ["run", "--org", "acme"])

        # Then
        assert result.exit_code == 1
        assert "orgpulse: GitHub authentication failed" in result.stderr
        assert "invalid configuration" not in result.stderr

    def test_surfaces_github_api_failures_separately(
        self,
        runner: CliRunner,
        reset_settings_cache: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Surface non-auth GitHub API failures separately from auth errors."""
        # Given
        github_auth_service = create_autospec(GitHubAuthService, instance=True, spec_set=True)
        github_auth_service.validate_access.side_effect = GitHubApiError("rate limited")
        monkeypatch.setattr("orgpulse.cli.GitHubAuthService", lambda: github_auth_service)

        # When
        result = runner.invoke(app, ["run", "--org", "acme"])

        # Then
        assert result.exit_code == 1
        assert "orgpulse: GitHub API request failed" in result.stderr
        assert "GitHub authentication failed" not in result.stderr
