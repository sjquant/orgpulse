from __future__ import annotations

# ruff: noqa: F403,F405
from .helpers.cli import *


class TestDashboardCommand:
    def test_renders_dashboard_exports_from_local_outputs_without_refresh(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Render dashboard JSON, CSV, and HTML artifacts from local snapshot outputs."""
        # Given
        source_output_dir = tmp_path / "source"
        report_output_dir = tmp_path / "report"
        _write_dashboard_source_period(
            period_dir=source_output_dir / "raw" / "month" / "created_at" / "2026-03",
            pull_request_rows=[
                _dashboard_pull_request_row(
                    period_key="2026-03",
                    repository_full_name="acme/api",
                    pull_request_number=1,
                    author_login="alice",
                    created_at="2026-03-20T09:00:00+00:00",
                    updated_at="2026-03-20T12:00:00+00:00",
                    closed_at="2026-03-20T12:00:00+00:00",
                    merged_at="2026-03-20T12:00:00+00:00",
                    additions=30,
                    deletions=10,
                    changed_files=3,
                    commits=2,
                ),
            ],
            review_rows=[
                _dashboard_review_row(
                    period_key="2026-03",
                    repository_full_name="acme/api",
                    pull_request_number=1,
                    review_id=101,
                    author_login="reviewer-1",
                    submitted_at="2026-03-20T10:00:00+00:00",
                ),
            ],
            timeline_rows=[],
        )
        _write_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-03",),
            locked_period_keys=(),
            as_of="2026-03-31",
        )

        # When
        result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-03-01",
                "--until",
                "2026-03-31",
                "--source-output-dir",
                str(source_output_dir),
                "--output-dir",
                str(report_output_dir),
                "--no-refresh",
                "--distribution-percentile",
                "99",
            ],
        )

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["distribution_percentile"] == 99
        assert payload["pull_requests"] == 1
        assert payload["json_path"].endswith("acme-created-at-since-2026-03-01.json")
        assert Path(payload["json_path"]).exists()
        assert Path(payload["csv_path"]).exists()
        assert Path(payload["html_path"]).exists()
        assert json.loads(Path(payload["json_path"]).read_text(encoding="utf-8"))[
            "overview"
        ]["org"] == "acme"
        assert (
            "Lines / Active Author"
            in Path(payload["html_path"]).read_text(encoding="utf-8")
        )

    def test_ignores_run_mode_environment_when_rendering_dashboard(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Render the dashboard without inheriting unrelated ORGPULSE run-mode defaults."""
        # Given
        source_output_dir = tmp_path / "source"
        report_output_dir = tmp_path / "report"
        _write_dashboard_source_period(
            period_dir=source_output_dir / "raw" / "month" / "created_at" / "2026-03",
            pull_request_rows=[
                _dashboard_pull_request_row(
                    period_key="2026-03",
                    repository_full_name="acme/api",
                    pull_request_number=1,
                    author_login="alice",
                    created_at="2026-03-20T09:00:00+00:00",
                    updated_at="2026-03-20T12:00:00+00:00",
                    closed_at="2026-03-20T12:00:00+00:00",
                    merged_at="2026-03-20T12:00:00+00:00",
                    additions=30,
                    deletions=10,
                    changed_files=3,
                    commits=2,
                ),
            ],
            review_rows=[],
            timeline_rows=[],
        )
        _write_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-03",),
            locked_period_keys=(),
            as_of="2026-03-31",
        )

        # When
        result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-03-01",
                "--until",
                "2026-03-31",
                "--source-output-dir",
                str(source_output_dir),
                "--output-dir",
                str(report_output_dir),
                "--no-refresh",
            ],
            env={"ORGPULSE_MODE": "backfill"},
        )

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["pull_requests"] == 1

    def test_fails_when_local_dashboard_history_has_coverage_gaps(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Fail with a clear error when local dashboard source periods do not cover the requested window."""
        # Given
        source_output_dir = tmp_path / "source"
        _write_dashboard_source_period(
            period_dir=source_output_dir / "raw" / "month" / "created_at" / "2026-04",
            pull_request_rows=[
                _dashboard_pull_request_row(
                    period_key="2026-04",
                    repository_full_name="acme/web",
                    pull_request_number=2,
                    author_login="bob",
                    created_at="2026-04-03T09:00:00+00:00",
                    updated_at="2026-04-03T15:00:00+00:00",
                    closed_at="2026-04-03T15:00:00+00:00",
                    merged_at="2026-04-03T15:00:00+00:00",
                    additions=20,
                    deletions=10,
                    changed_files=2,
                    commits=1,
                ),
            ],
            review_rows=[],
            timeline_rows=[],
        )
        _write_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-04",),
            locked_period_keys=(),
            as_of="2026-04-18",
        )

        # When
        result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-03-01",
                "--until",
                "2026-04-18",
                "--source-output-dir",
                str(source_output_dir),
                "--output-dir",
                str(tmp_path / "report"),
                "--no-refresh",
            ],
        )

        # Then
        assert result.exit_code == 1
        assert "orgpulse: dashboard generation failed" in result.stderr
        assert "Missing periods: 2026-03" in result.stderr

    def test_fails_with_user_facing_error_when_dashboard_generation_raises_runtime_error(
        self,
        runner: CliRunner,
        monkeypatch,
        tmp_path,
    ) -> None:
        """Report dashboard payload validation failures through the normal CLI error flow."""
        # Given
        def fake_generate_dashboard_report(**_: object) -> dict[str, object]:
            raise RuntimeError("dashboard payload validation failed: bad payload")

        monkeypatch.setattr(
            dashboard_module,
            "generate_dashboard_report",
            fake_generate_dashboard_report,
        )

        # When
        result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-03-01",
                "--until",
                "2026-03-31",
                "--source-output-dir",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "report"),
                "--no-refresh",
            ],
        )

        # Then
        assert result.exit_code == 1
        assert "orgpulse: dashboard generation failed" in result.stderr
        assert "dashboard payload validation failed" in result.stderr

    def test_fails_for_unsupported_weekly_dashboard_source_outputs(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Reject weekly dashboard sources with a clear month-grain restriction."""
        # Given
        source_output_dir = tmp_path / "source"
        _write_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-W16",),
            locked_period_keys=(),
            as_of="2026-04-18",
            period_grain=PeriodGrain.WEEK,
        )

        # When
        result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-04-01",
                "--until",
                "2026-04-18",
                "--source-output-dir",
                str(source_output_dir),
                "--output-dir",
                str(tmp_path / "report"),
                "--no-refresh",
            ],
        )

        # Then
        assert result.exit_code == 1
        assert "dashboard currently supports only month/created_at local outputs" in result.stderr


class TestDashboardRenderCommand:
    def test_renders_html_from_existing_dashboard_json(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Render dashboard HTML directly from an existing dashboard JSON payload."""
        # Given
        source_output_dir = tmp_path / "source"
        report_output_dir = tmp_path / "report"
        _write_dashboard_source_period(
            period_dir=source_output_dir / "raw" / "month" / "created_at" / "2026-03",
            pull_request_rows=[
                _dashboard_pull_request_row(
                    period_key="2026-03",
                    repository_full_name="acme/api",
                    pull_request_number=1,
                    author_login="alice",
                    created_at="2026-03-20T09:00:00+00:00",
                    updated_at="2026-03-20T12:00:00+00:00",
                    closed_at="2026-03-20T12:00:00+00:00",
                    merged_at="2026-03-20T12:00:00+00:00",
                    additions=30,
                    deletions=10,
                    changed_files=3,
                    commits=2,
                ),
            ],
            review_rows=[],
            timeline_rows=[],
        )
        _write_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-03",),
            locked_period_keys=(),
            as_of="2026-03-31",
        )
        generate_result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-03-01",
                "--until",
                "2026-03-31",
                "--source-output-dir",
                str(source_output_dir),
                "--output-dir",
                str(report_output_dir),
                "--no-refresh",
            ],
        )
        generated_payload = json.loads(generate_result.stdout)
        rendered_html_path = tmp_path / "rerendered.html"

        # When
        result = runner.invoke(
            app,
            [
                "dashboard-render",
                "--input-json",
                generated_payload["json_path"],
                "--output-html",
                str(rendered_html_path),
                "--distribution-percentile",
                "99",
            ],
        )

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["distribution_percentile"] == 99
        assert Path(payload["output_html"]).exists()
        assert "Lines / Active Author" in rendered_html_path.read_text(encoding="utf-8")
