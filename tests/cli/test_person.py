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
                            state="COMMENTED",
                            submitted_at=datetime.fromisoformat("2026-04-03T09:00:00"),
                        ),
                        review_factory(
                            review_id=104,
                            author_login="bob",
                            state="APPROVED",
                            submitted_at=datetime.fromisoformat("2026-04-03T12:00:00"),
                        ),
                        review_factory(
                            review_id=103,
                            author_login="bob",
                            state="APPROVED",
                            submitted_at=datetime.fromisoformat("2026-04-04T12:00:00"),
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
            "median_approval_hours": 51.0,
            "median_first_review_hours": 24.0,
            "median_merge_hours": 51.0,
            "merge_rate_pct": 100.0,
            "merged_pull_request_count": 1,
            "open_pull_request_count": 0,
            "review_coverage_pct": 100.0,
            "reviews_received": 3,
        }
        assert payload["reviewer_summary"] == {
            "approvals": 0,
            "authors_supported": 1,
            "changes_requested": 1,
            "comments": 0,
            "pull_requests_reviewed": 1,
            "pull_requests_reviewed_per_month": 1.0,
            "repositories_reviewed": 1,
            "reviewed_lines": 10,
            "reviewed_lines_per_month": 10.0,
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
                "is_closed": True,
                "is_open": False,
                "is_partial": False,
                "label": "closed month",
                "median_approval_hours": 51.0,
                "median_first_review_hours": 24.0,
                "median_merge_hours": 51.0,
                "merged_pull_request_count": 1,
                "observed_through_date": "2026-04-30",
                "open_month": False,
                "open_pull_request_count": 0,
                "open_week": False,
                "period_end_date": "2026-04-30",
                "period_key": "2026-04",
                "period_start_date": "2026-04-01",
                "pull_requests_reviewed": 1,
                "reviewed_lines": 10,
                "review_submissions_given": 1,
                "reviews_received": 3,
                "status": "closed",
            }
        ]

    def test_counts_approval_time_only_when_final_external_decision_is_approved(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Leave approval timing empty when the final same-time external decision requests changes."""
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
                    number=13,
                    title="Alice approval reversed",
                    state="closed",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T12:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-03T12:00:00"),
                    merged=False,
                    reviews=(
                        review_factory(
                            review_id=110,
                            author_login="bob",
                            state="APPROVED",
                            submitted_at=datetime.fromisoformat("2026-04-03T09:00:00"),
                        ),
                        review_factory(
                            review_id=111,
                            author_login="alice",
                            state="APPROVED",
                            submitted_at=datetime.fromisoformat("2026-04-03T10:00:00"),
                        ),
                        review_factory(
                            review_id=112,
                            author_login="bob",
                            state="CHANGES_REQUESTED",
                            submitted_at=datetime.fromisoformat("2026-04-03T09:00:00"),
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
        assert payload["summary"]["median_approval_hours"] is None
        assert payload["period_rows"][0]["median_approval_hours"] is None
        assert payload["repository_rows"][0]["median_approval_hours"] is None
        assert payload["summary"]["reviews_received"] == 3

    def test_counts_approval_time_from_later_review_request_boundary(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
        timeline_event_factory,
    ) -> None:
        """Measure approval timing from the later ready or review-request boundary."""
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
                    number=14,
                    title="Alice requested approval",
                    state="closed",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-02T14:00:00"),
                    closed_at=datetime.fromisoformat("2026-04-02T14:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-04-02T14:00:00"),
                    reviews=(
                        review_factory(
                            review_id=140,
                            author_login="bob",
                            state="APPROVED",
                            submitted_at=datetime.fromisoformat("2026-04-02T14:00:00"),
                        ),
                    ),
                    timeline_events=(
                        timeline_event_factory(
                            event_id=140,
                            event="review_requested",
                            created_at=datetime.fromisoformat("2026-04-02T11:00:00"),
                            requested_reviewer_login="bob",
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
        assert payload["summary"]["median_approval_hours"] == 3.0
        assert payload["period_rows"][0]["median_approval_hours"] == 3.0
        assert payload["repository_rows"][0]["median_approval_hours"] == 3.0

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
                "is_closed": "True",
                "is_open": "False",
                "is_partial": "True",
                "label": "open week",
                "median_approval_hours": "",
                "median_first_review_hours": "",
                "median_merge_hours": "",
                "merged_pull_request_count": "0",
                "observed_through_date": "2026-04-05",
                "open_month": "False",
                "open_pull_request_count": "0",
                "open_week": "False",
                "period_grain": "week",
                "period_end_date": "2026-04-05",
                "period_key": "2026-W14",
                "period_start_date": "2026-03-30",
                "pull_requests_reviewed": "1",
                "reviewed_lines": "10",
                "review_submissions_given": "1",
                "reviews_received": "0",
                "status": "closed",
            },
            {
                "approvals_given": "1",
                "authored_pull_request_count": "0",
                "changed_lines_total": "0",
                "changes_requested_given": "0",
                "comments_given": "0",
                "commits_total": "0",
                "is_closed": "True",
                "is_open": "False",
                "is_partial": "False",
                "label": "closed month",
                "median_approval_hours": "",
                "median_first_review_hours": "",
                "median_merge_hours": "",
                "merged_pull_request_count": "0",
                "observed_through_date": "2026-04-30",
                "open_month": "False",
                "open_pull_request_count": "0",
                "open_week": "False",
                "period_grain": "month",
                "period_end_date": "2026-04-30",
                "period_key": "2026-04",
                "period_start_date": "2026-04-01",
                "pull_requests_reviewed": "1",
                "reviewed_lines": "10",
                "review_submissions_given": "1",
                "reviews_received": "0",
                "status": "closed",
            }
        ]

    def test_calculates_person_reviewer_monthly_rates_and_reviewed_lines(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Calculate person reviewer monthly rates and reviewed lines from unique reviewed PRs."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-06-30T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=81,
                    title="April API work",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-12T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-13T10:00:00"),
                    additions=25,
                    deletions=5,
                    reviews=(
                        review_factory(
                            review_id=801,
                            author_login="alice",
                            state="COMMENTED",
                            submitted_at=datetime.fromisoformat("2026-04-13T09:00:00"),
                        ),
                        review_factory(
                            review_id=802,
                            author_login="alice",
                            state="APPROVED",
                            submitted_at=datetime.fromisoformat("2026-04-14T09:00:00"),
                        ),
                        review_factory(
                            review_id=804,
                            author_login="alice",
                            state="COMMENTED",
                            submitted_at=datetime.fromisoformat("2026-05-05T09:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=82,
                    title="May web work",
                    author_login="carol",
                    created_at=datetime.fromisoformat("2026-05-03T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-05-04T10:00:00"),
                    additions=12,
                    deletions=3,
                    reviews=(
                        review_factory(
                            review_id=803,
                            author_login="alice",
                            state="CHANGES_REQUESTED",
                            submitted_at=datetime.fromisoformat("2026-05-04T09:00:00"),
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
                "2026-06-30",
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
                "--since",
                "2026-04-01",
                "--until",
                "2026-06-30",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["reviewer_summary"]["pull_requests_reviewed"] == 2
        assert payload["reviewer_summary"]["reviewed_lines"] == 45
        assert payload["reviewer_summary"]["pull_requests_reviewed_per_month"] == 0.67
        assert payload["reviewer_summary"]["reviewed_lines_per_month"] == 15.0
        assert [
            (row["period_key"], row["pull_requests_reviewed"], row["reviewed_lines"])
            for row in payload["period_rows"]
        ] == [
            ("2026-04", 1, 30),
            ("2026-05", 2, 45),
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
        assert payload["reviewer_summary"]["review_submissions"] == 0
        assert payload["repository_rows"] == [
            {
                "authored_pull_request_count": 1,
                "changed_lines_total": 15,
                "commits_total": 1,
                "median_approval_hours": None,
                "median_first_review_hours": None,
                "median_merge_hours": None,
                "merged_pull_request_count": 0,
                "open_pull_request_count": 1,
                "pull_requests_reviewed": 0,
                "reviewed_lines": 0,
                "repository_full_name": "acme/api",
                "review_submissions_given": 0,
                "reviews_received": 1,
            }
        ]

    def test_rejects_person_grain_option(
        self,
        runner: CliRunner,
    ) -> None:
        """Reject obsolete person grain selection because both cadences are reported."""
        # Given
        arguments = [
            "person",
            "alice",
            "--org",
            "acme",
            "--grain",
            "month",
        ]

        # When
        result = runner.invoke(app, arguments)

        # Then
        assert result.exit_code == 2
        assert "No such option: --grain" in result.stderr

    def test_ignores_global_period_for_person_source_selection(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Keep person extraction cadence-independent when global period is weekly."""
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
                    number=81,
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
        monkeypatch.setenv("ORGPULSE_PERIOD", "week")

        # When
        result = runner.invoke(
            app,
            [
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["grain"] == "month"
        assert [row["period_key"] for row in payload["monthly_period_rows"]] == [
            "2026-04"
        ]
        assert payload["weekly_period_rows"][0]["period_key"] == "2026-W14"

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
                "--output-dir",
                str(tmp_path),
                "--format",
                "html",
            ],
        )

        # Then
        assert markdown_result.exit_code == 0
        assert "# orgpulse person metrics: alice" in markdown_result.stdout
        assert (
            "| 2026-04 | 1 | 0 | 1 | 10 | 1 | 0 | - | 0 | 0 | 0 |"
            in markdown_result.stdout
        )
        assert "| acme/api | 1 | 0 | 1 | 10 | - | 0 | 0 | 0 |" in markdown_result.stdout
        assert "## Org Trends" not in markdown_result.stdout
        assert html_result.exit_code == 0
        assert "<title>orgpulse person metrics: alice</title>" in html_result.stdout
        assert '<div class="shell person-report">' in html_result.stdout
        assert 'data-theme-option="dark"' in html_result.stdout
        assert 'id="org-trend-chart-root"' not in html_result.stdout
        assert 'id="person-trend-chart-root"' in html_result.stdout
        assert 'id="person-trend-chart-readout"' in html_result.stdout
        assert 'class="tab-strip primary-tabs"' in html_result.stdout
        assert 'class="tab-strip secondary-tabs"' in html_result.stdout
        assert (
            'data-person-trend-metric="authored_pull_request_count"'
            in html_result.stdout
        )
        assert (
            'data-person-trend-metric="review_submissions_given"' in html_result.stdout
        )
        assert 'data-person-trend-metric="pull_requests_reviewed"' in html_result.stdout
        assert 'data-person-trend-metric="reviewed_lines"' in html_result.stdout
        assert 'data-person-trend-metric="median_merge_hours"' in html_result.stdout
        assert (
            'data-person-trend-metric="median_first_review_hours"' in html_result.stdout
        )
        assert 'data-person-trend-metric="median_approval_hours"' in html_result.stdout
        assert 'data-person-trend-metric="changed_lines_total"' in html_result.stdout
        assert 'data-person-trend-metric="commits_total"' in html_result.stdout
        assert "Yellow band = open period" in html_result.stdout
        assert (
            '<script id="person-report-data" type="application/json">'
            in html_result.stdout
        )
        report_payload_match = re.search(
            r'<script id="person-report-data" type="application/json">(.*?)</script>',
            html_result.stdout,
        )
        assert report_payload_match is not None
        report_payload = json.loads(report_payload_match.group(1))
        assert report_payload["login"] == "alice"
        assert "org_weekly_trend_rows" not in report_payload
        assert "org_monthly_trend_rows" not in report_payload
        assert report_payload["period_rows"][0]["period_key"] == "2026-04"
        assert report_payload["period_rows"][0]["label"] == "closed month"
        assert report_payload["period_rows"][0]["is_partial"] is False
        assert report_payload["period_rows"][0]["median_merge_hours"] is None
        assert [row["period_key"] for row in report_payload["monthly_period_rows"]] == [
            "2026-04"
        ]
        assert report_payload["weekly_period_rows"][0]["period_key"] == "2026-W14"
        assert report_payload["weekly_period_rows"][-1]["period_key"] == "2026-W18"
        assert 'data-label="Month">2026-04</td>' in html_result.stdout
        assert 'data-label="Week">2026-W14</td>' in html_result.stdout
        assert 'data-label="State">' in html_result.stdout
        assert 'data-label="Authored PRs">1</td>' in html_result.stdout
        assert 'id="person-cadence-tabs"' in html_result.stdout
        assert 'data-person-cadence="weekly"' in html_result.stdout
        assert 'data-person-cadence="monthly"' in html_result.stdout
        assert 'id="person-cadence-weekly"' in html_result.stdout
        assert (
            'id="person-cadence-monthly" role="tabpanel"'
            in html_result.stdout
        )
        assert 'data-person-cadence-panel="monthly" hidden' in html_result.stdout
        assert 'id="person-trend-grain-tabs"' in html_result.stdout
        assert 'data-person-trend-grain="weekly"' in html_result.stdout
        assert 'data-person-trend-grain="monthly"' in html_result.stdout
        assert '<div class="two-col">' in html_result.stdout
        assert "grid-template-columns: minmax(0, 1fr) auto;" in html_result.stdout
        assert "width: fit-content;" in html_result.stdout
        assert ".two-col > .secondary-tabs" in html_result.stdout
        assert "margin-left: auto;" in html_result.stdout
        assert (
            '<nav class="section-nav" aria-label="Report sections">'
            in html_result.stdout
        )
        assert '<a href="#charts">Charts</a>' in html_result.stdout
        assert '<a href="#cadences">Weekly / Monthly</a>' in html_result.stdout
        assert '<a href="#repositories">Repositories</a>' in html_result.stdout
        assert '<a href="#methodology">Methodology</a>' in html_result.stdout
        assert "<h3>Weekly report</h3>" in html_result.stdout
        assert "<h3>Monthly report</h3>" in html_result.stdout
        assert str(tmp_path) not in html_result.stdout

    def test_includes_org_trends_in_person_json_markdown_and_html(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Render scoped org trends in flagged person JSON, Markdown, and HTML exports."""
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
                    number=33,
                    title="Alice API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
                    additions=20,
                    deletions=5,
                    reviews=(
                        review_factory(
                            review_id=3301,
                            author_login="bob",
                            submitted_at=datetime.fromisoformat("2026-04-03T09:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=34,
                    title="Bob web work",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-09T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-10T10:00:00"),
                    additions=30,
                    deletions=10,
                ),
                pull_request_factory(
                    repository_full_name="acme/mobile",
                    number=37,
                    title="Carol mobile rewrite",
                    author_login="carol",
                    created_at=datetime.fromisoformat("2026-04-16T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-17T10:00:00"),
                    additions=900,
                    deletions=100,
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
        json_result = runner.invoke(
            app,
            [
                "person",
                "alice",
                "--org",
                "acme",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
                "--distribution-percentile",
                "95",
                "--include-org-trends",
            ],
        )
        markdown_result = runner.invoke(
            app,
            [
                "person",
                "alice",
                "--org",
                "acme",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "markdown",
                "--distribution-percentile",
                "95",
                "--include-org-trends",
            ],
        )
        html_result = runner.invoke(
            app,
            [
                "person",
                "alice",
                "--org",
                "acme",
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "html",
                "--distribution-percentile",
                "95",
                "--include-org-trends",
            ],
        )

        # Then
        assert json_result.exit_code == 0
        payload = json.loads(json_result.stdout)
        assert payload["include_org_trends"] is True
        assert payload["org_monthly_trend_rows"][0]["period_key"] == "2026-04"
        assert payload["org_monthly_trend_rows"][0]["pull_requests"] == 3
        assert payload["org_monthly_trend_rows"][0]["active_authors"] == 3
        assert payload["org_monthly_trend_rows"][0]["changed_lines"] == 65
        assert (
            payload["org_monthly_trend_rows"][0]["changed_lines_per_active_author"]
            == 21.67
        )
        assert payload["org_monthly_trend_rows"][0]["review_submissions"] == 1
        assert payload["org_weekly_trend_rows"][0]["period_key"] == "2026-W14"
        assert markdown_result.exit_code == 0
        assert "## Org Trends" in markdown_result.stdout
        assert "### Weekly" in markdown_result.stdout
        assert "### Monthly" in markdown_result.stdout
        assert "| 2026-04 | 3 | 0 | 3 | 3 | 65 | 1 | 21.67 | 1 |" in (
            markdown_result.stdout
        )
        assert html_result.exit_code == 0
        assert 'id="org-trend-chart-root"' in html_result.stdout
        assert 'id="org-trend-chart-readout"' in html_result.stdout
        assert 'data-org-trend-metric="pull_requests"' in html_result.stdout
        assert 'data-org-trend-metric="pull_requests_per_active_author"' in (
            html_result.stdout
        )
        report_payload_match = re.search(
            r'<script id="person-report-data" type="application/json">(.*?)</script>',
            html_result.stdout,
        )
        assert report_payload_match is not None
        report_payload = json.loads(report_payload_match.group(1))
        assert report_payload["org_monthly_trend_rows"][0]["pull_requests"] == 3
        assert report_payload["org_monthly_trend_rows"][0]["changed_lines"] == 65
        assert report_payload["org_weekly_trend_rows"][0]["period_key"] == "2026-W14"

    def test_applies_repo_filters_to_person_org_trends(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Apply person repository filters to flagged org trend exports."""
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
                    number=35,
                    title="Alice API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-04-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-03T10:00:00"),
                    additions=20,
                    deletions=5,
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=36,
                    title="Bob web work",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2026-04-09T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-04-10T10:00:00"),
                    additions=30,
                    deletions=10,
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=38,
                    title="Alice future API work",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-05-02T09:00:00"),
                    updated_at=datetime.fromisoformat("2026-05-03T10:00:00"),
                    additions=200,
                    deletions=50,
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
                "--grain",
                "month",
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
                "--since",
                "2026-04-01",
                "--until",
                "2026-04-30",
                "--repo",
                "api",
                "--include-org-trends",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["org_monthly_trend_rows"][0]["pull_requests"] == 1
        assert payload["org_monthly_trend_rows"][0]["active_authors"] == 1
        assert payload["org_monthly_trend_rows"][0]["changed_lines"] == 25
        assert payload["org_monthly_trend_rows"][0]["review_submissions"] == 0
        assert [row["period_key"] for row in payload["org_monthly_trend_rows"]] == [
            "2026-04"
        ]
        assert payload["repository_rows"][0]["repository_full_name"] == "acme/api"

    def test_applies_distribution_percentile_to_person_latency_metrics(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
        review_factory,
    ) -> None:
        """Trim upper-tail latency outliers from person summary, period, and repository metrics."""
        # Given
        collection = PullRequestCollection(
            window=CollectionWindow(
                scope=RunScope.FULL_HISTORY,
                start_date=None,
                end_date=datetime.fromisoformat("2026-01-31T00:00:00").date(),
            ),
            pull_requests=(
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=101,
                    title="Fast Alice work",
                    state="closed",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-01-01T00:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-01T01:00:00"),
                    closed_at=datetime.fromisoformat("2026-01-01T01:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-01-01T01:00:00"),
                    reviews=(
                        review_factory(
                            review_id=1000,
                            author_login="alice",
                            submitted_at=datetime.fromisoformat("2026-01-01T00:30:00"),
                        ),
                        review_factory(
                            review_id=1005,
                            author_login="alice",
                            submitted_at=datetime.fromisoformat("2026-02-01T00:30:00"),
                        ),
                        review_factory(
                            review_id=1001,
                            author_login="bob",
                            submitted_at=datetime.fromisoformat("2026-01-01T01:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=102,
                    title="Normal Alice work",
                    state="closed",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-01-02T00:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-02T02:00:00"),
                    closed_at=datetime.fromisoformat("2026-01-02T02:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-01-02T02:00:00"),
                    reviews=(
                        review_factory(
                            review_id=1002,
                            author_login="bob",
                            submitted_at=datetime.fromisoformat("2026-01-02T02:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=103,
                    title="Outlier Alice work",
                    state="closed",
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-01-03T00:00:00"),
                    updated_at=datetime.fromisoformat("2026-02-13T16:00:00"),
                    closed_at=datetime.fromisoformat("2026-02-13T16:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-02-13T16:00:00"),
                    additions=1000,
                    deletions=0,
                    reviews=(
                        review_factory(
                            review_id=1003,
                            author_login="bob",
                            submitted_at=datetime.fromisoformat("2026-02-13T16:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/api",
                    number=105,
                    title="Draft Alice work",
                    state="open",
                    draft=True,
                    author_login="alice",
                    created_at=datetime.fromisoformat("2026-01-04T00:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-04T01:00:00"),
                    reviews=(
                        review_factory(
                            review_id=1006,
                            author_login="bob",
                            submitted_at=datetime.fromisoformat("2026-01-04T01:00:00"),
                        ),
                    ),
                ),
                pull_request_factory(
                    repository_full_name="acme/web",
                    number=104,
                    title="Older Bob work reviewed in window",
                    state="closed",
                    author_login="bob",
                    created_at=datetime.fromisoformat("2025-12-31T00:00:00"),
                    updated_at=datetime.fromisoformat("2026-01-15T02:00:00"),
                    closed_at=datetime.fromisoformat("2026-01-15T02:00:00"),
                    merged=True,
                    merged_at=datetime.fromisoformat("2026-01-15T02:00:00"),
                    additions=30,
                    deletions=10,
                    reviews=(
                        review_factory(
                            review_id=1004,
                            author_login="alice",
                            submitted_at=datetime.fromisoformat("2026-01-15T02:00:00"),
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
                "person",
                "--org",
                "acme",
                "--login",
                "alice",
                "--since",
                "2026-01-01",
                "--until",
                "2026-01-31",
                "--output-dir",
                str(tmp_path),
                "--distribution-percentile",
                "99",
                "--format",
                "json",
            ],
        )
        dashboard_result = runner.invoke(
            app,
            [
                "dashboard",
                "--org",
                "acme",
                "--since",
                "2026-01-01",
                "--until",
                "2026-01-31",
                "--source-output-dir",
                str(tmp_path),
                "--output-dir",
                str(tmp_path / "dashboard-report"),
                "--no-refresh",
                "--distribution-percentile",
                "99",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["distribution_percentile"] == 99
        assert payload["summary"]["median_first_review_hours"] == 1.5
        assert payload["summary"]["median_merge_hours"] == 1.5
        assert payload["period_rows"][0]["median_first_review_hours"] == 1.5
        assert payload["period_rows"][0]["median_merge_hours"] == 1.5
        assert payload["repository_rows"][0]["median_approval_hours"] == 1.5
        assert payload["repository_rows"][0]["median_first_review_hours"] == 1.5
        assert payload["repository_rows"][0]["median_merge_hours"] == 1.5
        assert dashboard_result.exit_code == 0
        dashboard_payload = json.loads(dashboard_result.stdout)
        dashboard_html = Path(dashboard_payload["html_path"]).read_text(
            encoding="utf-8"
        )
        author_details_match = re.search(
            r'<script id="author-details-data" type="application/json">(.*?)</script>',
            dashboard_html,
            re.S,
        )
        assert author_details_match is not None
        author_details = json.loads(author_details_match.group(1))
        alice_detail = author_details["alice"]
        alice_month = alice_detail["monthly_trends"][0]
        person_month = payload["monthly_period_rows"][0]
        assert (
            alice_detail["summary"]["pull_requests"]
            == (payload["summary"]["authored_pull_request_count"])
        )
        assert (
            alice_detail["summary"]["changed_lines"]
            == (payload["summary"]["changed_lines_total"])
        )
        assert alice_detail["summary"]["commits"] == payload["summary"]["commits_total"]
        assert (
            alice_detail["summary"]["median_first_review_hours"]
            == (payload["summary"]["median_first_review_hours"])
        )
        assert (
            alice_detail["summary"]["median_approval_hours"]
            == (payload["summary"]["median_approval_hours"])
        )
        assert (
            alice_detail["summary"]["median_merge_hours"]
            == (payload["summary"]["median_merge_hours"])
        )
        assert (
            alice_detail["summary"]["review_submissions_given"]
            == (payload["reviewer_summary"]["review_submissions"])
        )
        assert (
            alice_detail["summary"]["pull_requests_reviewed"]
            == (payload["reviewer_summary"]["pull_requests_reviewed"])
        )
        assert (
            alice_detail["summary"]["reviewed_lines"]
            == (payload["reviewer_summary"]["reviewed_lines"])
        )
        assert (
            alice_detail["summary"]["authors_supported"]
            == (payload["reviewer_summary"]["authors_supported"])
        )
        assert alice_month["changed_lines"] == person_month["changed_lines_total"]
        assert (
            alice_month["median_first_review_hours"]
            == (person_month["median_first_review_hours"])
        )
        assert (
            alice_month["median_approval_hours"]
            == (person_month["median_approval_hours"])
        )

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
        assert 'id="weekly-period-extra" class="hidden"' in result.stdout
        assert 'id="weekly-period-toggle" class="ghost-button" aria-expanded="false"' in result.stdout
        assert "Show 5 more weekly rows" in result.stdout
        assert 'id="monthly-period-extra" class="hidden"' in result.stdout
        assert 'id="monthly-period-toggle" class="ghost-button" aria-expanded="false"' in result.stdout
        assert "Show 3 more monthly rows" in result.stdout
        assert 'id="repository-extra" class="hidden"' in result.stdout
        assert "Show 5 more repositories" in result.stdout
        assert (
            'id="repository-toggle" class="ghost-button" aria-expanded="false"'
            in result.stdout
        )

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

    def test_bounds_derived_person_grains_to_local_source_as_of(
        self,
        runner: CliRunner,
        github_auth_service: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
        pull_request_factory,
    ) -> None:
        """Avoid emitting derived weekly rows after the local source as-of date."""
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
                    number=61,
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
                "--output-dir",
                str(tmp_path),
                "--format",
                "json",
            ],
        )

        # Then
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert [row["period_key"] for row in payload["weekly_period_rows"]] == [
            "2026-W14",
            "2026-W15",
            "2026-W16",
        ]
        assert payload["weekly_period_rows"][-1]["period_start_date"] == "2026-04-13"
        assert payload["weekly_period_rows"][-1]["period_end_date"] == "2026-04-19"
        assert payload["weekly_period_rows"][-1]["label"] == "open week"
        assert payload["weekly_period_rows"][-1]["is_open"] is True
        assert payload["weekly_period_rows"][-1]["is_partial"] is True
        assert payload["weekly_period_rows"][-1]["observed_through_date"] == (
            "2026-04-18"
        )
        assert payload["weekly_period_rows"][-1]["open_week"] is True
        assert payload["monthly_period_rows"][-1]["label"] == "open month"
        assert payload["monthly_period_rows"][-1]["open_month"] is True
