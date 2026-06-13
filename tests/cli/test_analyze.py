from __future__ import annotations

# ruff: noqa: F403,F405
from orgpulse.common.config import get_settings

from ..helpers.cli import *


class TestAnalyzeCommand:
    def test_writes_period_analysis_as_json(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Write period-grouped analysis output as JSON from local raw snapshots."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=11,
                    title="March carryover",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-03-28T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-03-30T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-03-31T08:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-03-31T08:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=12,
                    title="April API work",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-06T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-07T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-07T12:00:00"),
                    additions=13,
                    deletions=4,
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=13,
                    title="April web work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-13T11:00:00"),
                    merged=False,
                    merged_at=None,
                    closed_at=None,
                ),
            ),
            failures=(),
        )
        _configure_production_cli_runtime(
            monkeypatch,
            collection=collection,
        )
        run_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--mode",
                "full",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert run_result.exit_code == 0

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "period",
                "--since",
                "2026-04-01",
                "--until",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["grouping"] == "period"
        assert payload["grain"] == "month"
        assert payload["time_anchor"] == "created_at"
        assert "locale" not in payload
        assert payload["matched_pull_request_count"] == 2
        assert len(payload["rows"]) == 1
        row = payload["rows"][0]
        assert row["group_value"] == "2026-04"
        assert row["period_key"] == "2026-04"
        assert row["period_start_date"] == "2026-04-01"
        assert row["period_end_date"] == "2026-04-30"
        assert row["pull_request_count"] == 2
        assert row["merged_pull_request_count"] == 1
        assert row["active_author_count"] == 2
        assert row["merged_pull_requests_per_active_author"] == 0.5
        assert row["additions_total"] == 21
        assert row["additions_average"] == 10.5
        assert row["changed_lines_total"] == 27
        assert row["time_to_first_review_count"] == 0
        assert row["time_to_merge_count"] == 1
        assert row["time_to_merge_average_seconds"] == 183600.0
        get_settings.cache_clear()
        invalid_locale_result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "period",
                "--since",
                "2026-04-01",
                "--until",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
            env={"ORGPULSE_LOCALE": "bad-locale"},
        )
        assert invalid_locale_result.exit_code == 0
        assert "locale" not in json.loads(invalid_locale_result.stdout)
        get_settings.cache_clear()

    def test_writes_repository_analysis_as_csv(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Write repository-grouped analysis output as CSV with top-N trimming."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=21,
                    title="API refresh",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-03-28T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-02T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-03T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-03T12:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=22,
                    title="API cleanup",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-01T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-04T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-05T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-05T12:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=23,
                    title="Web polish",
                    author_login="carol",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-04T08:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                ),
            ),
            failures=(),
        )
        _configure_production_cli_runtime(
            monkeypatch,
            collection=collection,
        )
        run_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--mode",
                "full",
                "--as-of",
                "2026-04-18",
                "--time-anchor",
                "updated_at",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert run_result.exit_code == 0

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "repository",
                "--top",
                "1",
                "--since",
                "2026-04-01",
                "--until",
                "2026-04-30",
                "--time-anchor",
                "updated_at",
                "--output-dir",
                str(tmp_path),
                "--format",
                "csv",
            ],
        )

        # Then
        rows = list(csv.DictReader(StringIO(result.stdout)))
        assert result.exit_code == 0
        assert len(rows) == 1
        assert rows[0]["group_value"] == "acme/api"
        assert rows[0]["pull_request_count"] == "2"
        assert rows[0]["merged_pull_request_count"] == "2"
        assert rows[0]["period_key"] == ""

    def test_rejects_html_analysis_format(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Reject the removed analysis HTML output format."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "period",
                "--output-dir",
                str(tmp_path),
                "--format",
                "html",
            ],
        )

        # Then
        assert result.exit_code == 2
        assert "Invalid value for '--format'" in result.stderr
        assert "html" in result.stderr

    def test_writes_period_analysis_top_n_from_largest_periods(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Write period-grouped top-N output using the largest periods instead of the latest periods."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-02-28T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=51,
                    title="January one",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-01-03T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-04T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-01-05T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-01-05T09:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=52,
                    title="January two",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-01-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-11T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-01-12T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-01-12T09:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=53,
                    title="January three",
                    author_login="carol",
                    created_at=datetime.fromisoformat("2026-01-14T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-15T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-01-16T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-01-16T09:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=54,
                    title="February one",
                    author_login="dana",
                    created_at=datetime.fromisoformat("2026-02-10T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-02-11T09:00:00"),
                    closed_at=datetime.fromisoformat("2026-02-12T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-02-12T09:00:00"),
                ),
            ),
            failures=(),
        )
        _configure_production_cli_runtime(
            monkeypatch,
            collection=collection,
        )
        run_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--mode",
                "full",
                "--as-of",
                "2026-02-28",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert run_result.exit_code == 0

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "period",
                "--top",
                "1",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert len(payload["rows"]) == 1
        assert payload["rows"][0]["group_value"] == "2026-01"
        assert payload["rows"][0]["pull_request_count"] == 3

    def test_trims_distribution_metrics_with_percentile_cutoff(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Trim upper-tail outliers from distribution-based analysis summaries."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-30T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=71,
                    title="Normal API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-01T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-02T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-03T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-03T09:00:00"),
                    additions=8,
                    deletions=2,
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=72,
                    title="Normal API follow-up",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-04T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-05T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-06T09:00:00"),
                    additions=16,
                    deletions=4,
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=73,
                    title="Large migration",
                    author_login="carol",
                    created_at=datetime.fromisoformat("2026-04-07T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-08T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-05-30T09:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-05-30T09:00:00"),
                    additions=1000,
                    deletions=0,
                ),
            ),
            failures=(),
        )
        _configure_production_cli_runtime(
            monkeypatch,
            collection=collection,
        )
        run_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--mode",
                "full",
                "--as-of",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert run_result.exit_code == 0

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "period",
                "--distribution-percentile",
                "95",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["distribution_percentile"] == 95
        assert payload["matched_pull_request_count"] == 3
        row = payload["rows"][0]
        assert row["pull_request_count"] == 3
        assert row["changed_lines_total"] == 30
        assert row["changed_lines_average"] == 15.0
        assert row["time_to_merge_count"] == 2
        assert row["time_to_merge_average_seconds"] == 172800.0

    def test_writes_author_analysis_as_markdown(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Write author-grouped analysis output as Markdown from local snapshots."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-18T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=31,
                    title="Alice API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-04T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-04T12:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=32,
                    title="Alice web work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-06T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-07T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-08T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-08T12:00:00"),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=33,
                    title="Bob web work",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-07T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-08T10:00:00"),
                    merged=False,
                    merged_at=None,
                    closed_at=None,
                ),
            ),
            failures=(),
        )
        _configure_production_cli_runtime(
            monkeypatch,
            collection=collection,
        )
        run_result = runner.invoke(
            app,
            [
                "run",
                "--org",
                "acme",
                "--mode",
                "full",
                "--as-of",
                "2026-04-18",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert run_result.exit_code == 0

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--group-by",
                "author",
                "--top",
                "1",
                "--output-dir",
                str(tmp_path),
                "--format",
                "markdown",
            ],
        )

        # Then
        assert result.exit_code == 0
        assert "# orgpulse analysis: acme" in result.stdout
        assert "- Grouping: author" in result.stdout
        assert "| alice | - | 2 | 2 | 1 |" in result.stdout

    def test_rejects_unsupported_distribution_percentile(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Reject unsupported percentile cutoffs before analysis starts."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--distribution-percentile",
                "90",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 2
        assert "orgpulse: invalid analysis configuration" in result.stderr
        assert "distribution percentile must be one of 95, 99, 100" in result.stderr

    def test_fails_when_local_analysis_inputs_are_missing(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Fail with a clear error when no local analysis manifest exists yet."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "analyze",
                "--org",
                "acme",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
            ],
        )

        # Then
        assert result.exit_code == 1
        assert "orgpulse: analysis input failed" in result.stderr
        assert "Run `orgpulse run`" in result.stderr
