from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from orgpulse.cli import app, build_run_config
from orgpulse.config import PeriodGrain, RunMode


@pytest.fixture(scope="module")
def runner() -> CliRunner:
    """Provide a reusable Typer CLI runner for pytest-based tests."""
    return CliRunner()


class TestRunConfigParsing:
    def test_parses_incremental_run_defaults(self) -> None:
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

    def test_loads_settings_from_environment(self, runner: CliRunner) -> None:
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
        assert payload["org"] == "env-acme"
        assert payload["period"] == "week"
        assert payload["output_dir"] == "env-output"

    def test_parses_backfill_bounds(self) -> None:
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

    def test_rejects_backfill_without_bounds(self, runner: CliRunner) -> None:
        """Reject backfill mode when one or both required date bounds are missing."""
        # Given

        # When
        result = runner.invoke(app, ["run", "--org", "acme", "--mode", "backfill"])

        # Then
        assert result.exit_code == 2
        assert "backfill mode requires both --backfill-start and --backfill-end" in result.stderr

    def test_rejects_overlapping_repo_filters(self, runner: CliRunner) -> None:
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

    def test_rejects_backfill_dates_for_non_backfill_mode(self, runner: CliRunner) -> None:
        """Reject backfill date options when the run mode is not backfill."""
        # Given

        # When
        result = runner.invoke(app, ["run", "--org", "acme", "--backfill-start", "2026-01-01"])

        # Then
        assert result.exit_code == 2
        assert "backfill date bounds are only valid when --mode backfill is selected" in result.stderr
