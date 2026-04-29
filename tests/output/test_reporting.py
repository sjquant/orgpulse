from __future__ import annotations

# ruff: noqa: F403,F405
from ..helpers.output import *


class TestReportingModules:
    def test_exposes_reporting_module_constants(self) -> None:
        """Expose reporting constants from the run outputs module."""
        # Given

        # When

        # Then
        assert MANIFEST_FILENAME == "manifest.json"
        assert ORG_SUMMARY_DIRNAME == "org_summary"
        assert "pull_requests.csv" in REQUIRED_RAW_SNAPSHOT_HEADERS

    def test_exposes_analysis_report_renderers(self) -> None:
        """Expose organization report helpers from the analysis report module."""
        # Given

        # When

        # Then
        assert callable(build_organization_report_payload)
        assert callable(render_organization_report_html)

    def test_renders_dashboard_html_through_public_artifact_function(
        self,
        tmp_path: Path,
    ) -> None:
        """Render HTML through the reporting helper without relying on a module entrypoint."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-23T09:00:00+00:00",
                "since": "2026-04-01",
                "until": "2026-04-30",
                "time_anchor": "created_at",
                "top_repository": "acme/api",
                "top_author": "alice",
                "unique_reviewers": 1,
            },
            "reviewers": [
                {
                    "reviewer_login": "reviewer-1",
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
            ],
        }
        input_json = tmp_path / "payload.json"
        output_html = tmp_path / "payload.html"
        input_json.write_text(json.dumps(payload), encoding="utf-8")

        # When
        result = render_dashboard_artifact(
            input_json=input_json,
            output_html=output_html,
            distribution_percentile=99,
        )

        # Then
        assert result["distribution_percentile"] == 99
        assert output_html.exists()
        assert "Lines / Active Author" in output_html.read_text(encoding="utf-8")

    def test_dashboard_module_fails_with_cli_guidance(self) -> None:
        """Fail loudly with CLI guidance when the dashboard module is executed directly."""
        # Given
        command = [
            sys.executable,
            "-m",
            "orgpulse.dashboard",
        ]

        # When
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[1]),
        )

        # Then
        assert result.returncode != 0
        assert "Use `orgpulse dashboard`." in result.stderr

    def test_dashboard_html_module_fails_with_cli_guidance(self) -> None:
        """Fail loudly with CLI guidance when the HTML reporting module is executed directly."""
        # Given
        command = [
            sys.executable,
            "-m",
            "orgpulse.reporting.dashboard_html",
        ]

        # When
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parents[1]),
        )

        # Then
        assert result.returncode != 0
        assert "Use `orgpulse dashboard-render`." in result.stderr
