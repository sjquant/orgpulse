from __future__ import annotations

# ruff: noqa: F403,F405
from ..helpers.cli import *


class TestPersonCommand:
    def test_writes_person_metrics_as_json(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Write one person's authored and reviewer metrics as JSON from local snapshots."""
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
                    number=11,
                    title="Alice API work",
                    state="closed",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-04T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-04T12:00:00"),
                    additions=20,
                    deletions=5,
                    commits=3,
                    reviews=(
                        review_factory(
                            review_id=101,
                            author_login="bob",
                            submitted_at=datetime.fromisoformat("2026-04-03T09:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=12,
                    title="Carol web work",
                    state="closed",
                    author_login="carol",
                    created_at=datetime.fromisoformat("2026-03-28T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-05T10:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-06T12:00:00"),
                    reviews=(
                        review_factory(
                            review_id=102,
                            author_login="Alice",
                            state="CHANGES_REQUESTED",
                            submitted_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                        ),
                    ),
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
                "person",
                "--org",
                "acme",
                "--login",
                "ALICE",
                "--grain",
                "month",
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
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["target_org"] == "acme"
        assert payload["login"] == "ALICE"
        assert payload["summary"] == {
            "authored_pull_request_count": 1,
            "changed_lines_total": 25,
            "commits_total": 3,
            "median_first_review_hours": 24.0,
            "median_merge_hours": 51.0,
            "merge_rate_pct": 100.0,
            "merged_pull_request_count": 1,
            "open_pull_request_count": 0,
            "review_coverage_pct": 100.0,
            "reviews_received": 1,
        }
        assert payload["reviewer_summary"] == {
            "approvals": 0,
            "authors_supported": 1,
            "changes_requested": 1,
            "comments": 0,
            "pull_requests_reviewed": 1,
            "repositories_reviewed": 1,
            "review_submissions": 1,
        }
        assert payload["period_rows"] == [
            {
                "approvals_given": 0,
                "authored_pull_request_count": 1,
                "changed_lines_total": 25,
                "changes_requested_given": 1,
                "comments_given": 0,
                "commits_total": 3,
                "merged_pull_request_count": 1,
                "open_pull_request_count": 0,
                "period_end_date": "2026-04-30",
                "period_key": "2026-04",
                "period_start_date": "2026-04-01",
                "pull_requests_reviewed": 1,
                "review_submissions_given": 1,
                "reviews_received": 1,
            }
        ]

    def test_counts_reviews_by_submission_date_for_csv(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Count submitted reviews in the selected period even when the reviewed PR was created earlier."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-30T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=21,
                    title="March work reviewed in April",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-03-25T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-04T10:00:00"),
                    reviews=(
                        review_factory(
                            review_id=201,
                            author_login="alice",
                            submitted_at=datetime.fromisoformat("2026-04-04T09:00:00"),
                        ),
                    ),
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
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--since",
                "2026-04-01",
                "--until",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
                "--format",
                "csv",
            ],
        )

        # Then
        assert result.exit_code == 0
        rows = list(csv.DictReader(StringIO(result.stdout)))
        assert rows == [
            {
                "approvals_given": "1",
                "authored_pull_request_count": "0",
                "changed_lines_total": "0",
                "changes_requested_given": "0",
                "comments_given": "0",
                "commits_total": "0",
                "merged_pull_request_count": "0",
                "open_pull_request_count": "0",
                "period_end_date": "2026-04-30",
                "period_key": "2026-04",
                "period_start_date": "2026-04-01",
                "pull_requests_reviewed": "1",
                "review_submissions_given": "1",
                "reviews_received": "0",
            }
        ]

    def test_renders_only_existing_local_periods_for_requested_window(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Render only periods present in local snapshots when the requested window is wider."""
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
                    title="Alice April work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
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
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--since",
                "2026-02-01",
                "--until",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert [row["period_key"] for row in payload["period_rows"]] == ["2026-04"]

    def test_supports_positional_login_aliases_output_file_and_repo_filters(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Extract person metrics with positional login, aliases, output file, and repo filters."""
        # Given
        output_file = tmp_path / "reports" / "alice.json"
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-04-30T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=61,
                    title="Alice API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
                    additions=12,
                    deletions=3,
                    reviews=(
                        review_factory(
                            review_id=601,
                            author_login="alice",
                            submitted_at=datetime.fromisoformat("2026-04-04T09:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=62,
                    title="Alice web work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-05T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-06T10:00:00"),
                    additions=100,
                    deletions=20,
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
                "person",
                "alice",
                "--org",
                "acme",
                "--period",
                "month",
                "--pr-time-anchor",
                "created_at",
                "--source-output-dir",
                str(tmp_path),
                "--repo",
                "api",
                "--exclude-repo",
                "web",
                "--output-file",
                str(output_file),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        assert result.stdout == ""
        payload = json.loads(output_file.read_text(encoding="utf-8"))
        assert payload["summary"]["authored_pull_request_count"] == 1
        assert payload["summary"]["changed_lines_total"] == 15
        assert payload["reviewer_summary"]["review_submissions"] == 1
        assert payload["repository_rows"] == [
            {
                "authored_pull_request_count": 1,
                "changed_lines_total": 15,
                "commits_total": 1,
                "merged_pull_request_count": 0,
                "open_pull_request_count": 1,
                "pull_requests_reviewed": 1,
                "repository_full_name": "acme/api",
                "review_submissions_given": 1,
                "reviews_received": 1,
            }
        ]

    def test_writes_person_metrics_as_markdown_and_html(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Render one person's metrics as human-readable Markdown and HTML."""
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
                    number=31,
                    title="Alice API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
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
        markdown_result = runner.invoke(
            app,
            [
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "markdown",
            ],
        )
        html_result = runner.invoke(
            app,
            [
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "html",
            ],
        )

        # Then
        assert markdown_result.exit_code == 0
        assert "# orgpulse person metrics: alice" in markdown_result.stdout
        assert "| 2026-04 | 1 | 0 | 1 | 10 | 1 | 0 | 0 | 0 |" in markdown_result.stdout
        assert html_result.exit_code == 0
        assert "<title>orgpulse person metrics: alice</title>" in html_result.stdout
        assert '<div class="shell person-report">' in html_result.stdout
        assert 'data-theme-option="dark"' in html_result.stdout
        assert 'class="table-wrap"' in html_result.stdout
        assert '<script id="person-report-data" type="application/json">' in html_result.stdout
        assert 'data-label="Period">2026-04</td>' in html_result.stdout
        assert 'data-label="Authored PRs">1</td>' in html_result.stdout

    def test_writes_person_html_with_progressive_tables(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Render person HTML with collapsed extra periods and repositories."""
        # Given
        pull_requests = tuple(
            pull_request_factory(
                repository_full_name=f"acme/service-{index:02d}",
                number=100 + index,
                title=f"Alice work {index}",
                author_login="alice",
                created_at=datetime.fromisoformat(
                    f"2026-{(index % 8) + 1:02d}-02T09:00:00"
                ),
                updated_at=datetime.fromisoformat(
                    f"2026-{(index % 8) + 1:02d}-03T10:00:00"
                ),
                additions=10 + index,
                deletions=index,
            )
            for index in range(12)
        )
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-08-31T00:00:00").date(),
            ),
            pull_requests=pull_requests,
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
                "2026-08-31",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert run_result.exit_code == 0

        # When
        result = runner.invoke(
            app,
            [
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--since",
                "2026-01-01",
                "--until",
                "2026-08-31",
                "--output-dir",
                str(tmp_path),
                "--format",
                "html",
            ],
        )

        # Then
        assert result.exit_code == 0
        assert 'id="period-extra" class="hidden"' in result.stdout
        assert "Show 2 more older month rows" in result.stdout
        assert 'id="repository-extra" class="hidden"' in result.stdout
        assert "Show 2 more repositories" in result.stdout
        assert "setupProgressiveToggle" in result.stdout

    def test_writes_zero_result_for_unknown_login(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Write a valid zero-result payload when the requested login has no activity."""
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
                    number=41,
                    title="Bob API work",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
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
                "person",
                "--org",
                "acme",
                "--login",
                "nobody",
                "--grain",
                "month",
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
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["summary"]["authored_pull_request_count"] == 0
        assert payload["reviewer_summary"]["review_submissions"] == 0
        assert payload["period_rows"][0]["period_key"] == "2026-04"
        assert payload["period_rows"][0]["authored_pull_request_count"] == 0

    def test_reports_missing_person_metrics_input(
        self,
        runner: CliRunner,
        tmp_path,
    ) -> None:
        """Report missing local person metrics inputs through the normal CLI error flow."""
        # Given

        # When
        result = runner.invoke(
            app,
            [
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 1
        assert "orgpulse: person metrics input failed" in result.stderr
        assert "person metrics input is missing" in result.stderr

    def test_reports_stale_person_metrics_input(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Report local person metrics inputs that are stale for the requested window."""
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
                    number=51,
                    title="Alice API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
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
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--grain",
                "month",
                "--until",
                "2026-04-30",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 1
        assert "orgpulse: person metrics input failed" in result.stderr
        assert "local person metrics source is stale" in result.stderr
