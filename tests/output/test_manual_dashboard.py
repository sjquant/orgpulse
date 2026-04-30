from __future__ import annotations

# ruff: noqa: F403,F405
from ..helpers.output import *


class TestManualDashboardPayload:
    def test_marks_open_week_and_open_month_in_dashboard_data_and_html(self) -> None:
        """Render partial-period dashboard trends with explicit open week and month state."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-18T00:00:00+00:00",
                "since": "2026-04-01",
                "until": "2026-04-18",
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
                    created_at="2026-04-17T09:00:00+00:00",
                    merged_at=None,
                    changed_lines=12,
                    additions=9,
                    deletions=3,
                    first_review_hours=2.0,
                    merge_hours=None,
                    size_bucket="XS",
                ),
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)
        html = render_dashboard_html(prepared)

        # Then
        assert prepared.overview["open_week"] is True
        assert prepared.overview["open_week_key"] == "2026-W16"
        assert prepared.overview["open_month"] is True
        assert prepared.overview["open_month_key"] == "2026-04"
        assert prepared.overview["time_anchor_context"] == (
            _expected_time_anchor_context()
        )
        assert prepared.weekly_trends[0]["open_week"] is True
        assert prepared.weekly_trends[0]["label"] == "open week"
        assert prepared.monthly_trends[0]["open_month"] is True
        assert prepared.monthly_trends[0]["label"] == "open month"
        assert "2026-04 open month" in html
        assert "chart-partial-band" in html
        assert "partial-period-row" in html
        assert "period-state-pill open" in html
        assert "Yellow band = open or partial period" in html
        assert 'rx="8"' not in html

    def test_labels_closed_truncated_dashboard_periods_as_partial(self) -> None:
        """Label a closed dashboard period as partial when the window starts inside it."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-30T00:00:00+00:00",
                "since": "2026-04-01",
                "until": "2026-04-18",
                "source_as_of": "2026-04-30",
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
                    pull_request_number=2,
                    author_login="alice",
                    created_at="2026-04-10T09:00:00+00:00",
                    merged_at="2026-04-11T09:00:00+00:00",
                    changed_lines=10,
                    additions=8,
                    deletions=2,
                    first_review_hours=1.0,
                    merge_hours=24.0,
                    size_bucket="XS",
                ),
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)
        html = render_dashboard_html(prepared)

        # Then
        assert prepared.monthly_trends[0]["status"] == "closed"
        assert prepared.monthly_trends[0]["label"] == "partial month"
        assert prepared.monthly_trends[0]["is_partial"] is True
        assert prepared.monthly_trends[0]["open_month"] is False
        assert "partial month" in html
        assert "period-state-pill partial" in html
        assert "Yellow band = open or partial period" in html
        assert 'rx="8"' not in html

    def test_renders_theme_switch_with_tokenized_theme_bootstrap(self) -> None:
        """Render a manual dashboard with a persisted light/dark theme switch and CSS tokens."""
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

        # When
        prepared = prepare_dashboard_payload(payload)
        prepared.review_state_rows = []
        html = render_dashboard_html(prepared)

        # Then
        assert 'orgpulse-manual-dashboard-theme' in html
        assert 'data-theme-option="dark"' in html
        assert 'data-theme-option="light"' in html
        assert 'aria-label="Dark theme"' in html
        assert 'aria-label="Light theme"' in html
        assert 'class="theme-switch-icon"' in html
        assert 'class="theme-switch-indicator"' in html
        assert 'data-active-theme' in html
        assert "transform 180ms cubic-bezier(0.22, 1, 0.36, 1)" in html
        assert "--bg-canvas" in html
        assert "--panel-raised" in html
        assert 'window.matchMedia("(prefers-color-scheme: dark)")' in html

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
        prepared = prepare_dashboard_payload(
            payload,
            distribution_percentile=95,
        )

        # Then
        assert prepared.overview["total_changed_lines"] == 30
        assert sum(row["changed_lines"] for row in prepared.repositories) == 30
        assert sum(row["changed_lines"] for row in prepared.monthly_trends) == 30
        assert prepared.repositories == [
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
        author_details = json.loads(prepared.author_details_json)
        assert author_details["carol"]["size_mix"][-1] == {
            "bucket": "XL",
            "pull_requests": 1,
            "changed_lines": 0,
            "median_first_review_hours": None,
            "median_merge_hours": 24.0,
            "average_reviews_per_pr": 1.0,
        }

    def test_adds_team_normalized_author_metrics_to_overview_and_trends(self) -> None:
        """Expose average active-author normalization in overview cards and trend rows."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-24T00:00:00+00:00",
                "since": "2026-01-01",
                "until": "2026-02-28",
                "time_anchor": "created_at",
                "top_repository": "acme/api",
                "top_author": "alice",
                "unique_reviewers": 2,
            },
            "reviewers": [
                {
                    "reviewer_login": "reviewer-1",
                    "review_submissions": 2,
                    "pull_requests_reviewed": 2,
                    "approvals": 2,
                    "changes_requested": 0,
                    "comments": 0,
                    "authors_supported": 2,
                },
                {
                    "reviewer_login": "reviewer-2",
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
                    created_at="2026-01-03T09:00:00+00:00",
                    merged_at="2026-01-04T09:00:00+00:00",
                    changed_lines=10,
                    additions=8,
                    deletions=2,
                    first_review_hours=1.0,
                    merge_hours=24.0,
                    size_bucket="XS",
                ),
                _manual_pull_request(
                    repository_full_name="acme/web",
                    pull_request_number=2,
                    author_login="bob",
                    created_at="2026-01-10T09:00:00+00:00",
                    merged_at="2026-01-11T09:00:00+00:00",
                    changed_lines=30,
                    additions=24,
                    deletions=6,
                    first_review_hours=2.0,
                    merge_hours=24.0,
                    size_bucket="S",
                ),
                _manual_pull_request(
                    repository_full_name="acme/api",
                    pull_request_number=3,
                    author_login="alice",
                    created_at="2026-02-05T09:00:00+00:00",
                    merged_at="2026-02-06T09:00:00+00:00",
                    changed_lines=20,
                    additions=14,
                    deletions=6,
                    first_review_hours=3.0,
                    merge_hours=24.0,
                    size_bucket="S",
                ),
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)
        html = render_dashboard_html(prepared)

        # Then
        assert prepared.overview["average_active_authors_per_month"] == 1.5
        assert prepared.overview["pull_requests_per_active_author"] == 2.0
        assert prepared.overview["changed_lines_per_active_author"] == 40.0
        assert prepared.overview["review_submissions_per_reviewer"] == 1.5
        assert prepared.monthly_trends == [
            {
                "period_key": "2026-01",
                "period_start_date": "2026-01-01",
                "period_end_date": "2026-01-31",
                "status": "closed",
                "label": "closed month",
                "is_open": False,
                "is_closed": True,
                "is_partial": False,
                "observed_through_date": "2026-01-31",
                "open_week": False,
                "open_month": False,
                "pull_requests": 2,
                "merged_pull_requests": 2,
                "open_pull_requests": 0,
                "active_authors": 2,
                "changed_lines": 40,
                "review_submissions": 2,
                "pull_requests_per_active_author": 1.0,
                "changed_lines_per_active_author": 20.0,
                "average_reviews_per_pr": 1.0,
                "median_first_review_hours": 1.5,
                "median_merge_hours": 24.0,
                "pull_request_delta": None,
                "changed_lines_delta": None,
                "period_start_date": "2026-01-01",
                "period_end_date": "2026-01-31",
                "status": "closed",
                "label": "closed month",
                "is_open": False,
                "is_closed": True,
                "is_partial": False,
                "observed_through_date": "2026-01-31",
                "open_week": False,
                "open_month": False,
            },
            {
                "period_key": "2026-02",
                "period_start_date": "2026-02-01",
                "period_end_date": "2026-02-28",
                "status": "closed",
                "label": "closed month",
                "is_open": False,
                "is_closed": True,
                "is_partial": False,
                "observed_through_date": "2026-02-28",
                "open_week": False,
                "open_month": False,
                "pull_requests": 1,
                "merged_pull_requests": 1,
                "open_pull_requests": 0,
                "active_authors": 1,
                "changed_lines": 20,
                "review_submissions": 1,
                "pull_requests_per_active_author": 1.0,
                "changed_lines_per_active_author": 20.0,
                "average_reviews_per_pr": 1.0,
                "median_first_review_hours": 3.0,
                "median_merge_hours": 24.0,
                "pull_request_delta": -1,
                "changed_lines_delta": -20,
                "period_start_date": "2026-02-01",
                "period_end_date": "2026-02-28",
                "status": "closed",
                "label": "closed month",
                "is_open": False,
                "is_closed": True,
                "is_partial": False,
                "observed_through_date": "2026-02-28",
                "open_week": False,
                "open_month": False,
            },
        ]
        assert "PRs / active author" in html
        assert "Lines / active author" in html
        assert "avg active authors / month" in html
        assert "normalized changed lines per active author" in html
        assert 'data-label="Lines / active author"' in html

    def test_tolerates_additive_and_stale_source_sections_when_preparing_payload(
        self,
    ) -> None:
        """Prepare dashboard payloads from legacy JSON even when additive or stale source sections are present."""
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
                "unique_reviewers": 1,
                "debug_note": "legacy",
            },
            "reviewers": [
                {
                    "reviewer_login": "reviewer-1",
                    "review_submissions": 2,
                    "pull_requests_reviewed": 2,
                    "approvals": 2,
                    "changes_requested": 0,
                    "comments": 0,
                    "authors_supported": 1,
                    "custom_field": "legacy",
                },
            ],
            "authors": [
                {
                    "author_login": "stale-author",
                },
            ],
            "charts": {
                "custom_chart": [{"label": "ignored", "count": 1}],
            },
            "size_buckets": [
                {
                    "bucket": "XS",
                },
            ],
            "review_state_rows": [
                {
                    "state": "APPROVED",
                    "count": 1,
                    "share_pct": 100.0,
                    "custom_field": "legacy",
                }
            ],
            "pull_requests": [
                {
                    **_manual_pull_request(
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
                    "custom_field": "legacy",
                },
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)

        # Then
        assert prepared.overview["pull_requests"] == 1
        assert prepared.authors[0]["author_login"] == "alice"
        assert prepared.reviewers[0]["reviewer_login"] == "reviewer-1"
        assert prepared.review_state_rows == [
            {
                "state": "APPROVED",
                "count": 1,
                "share_pct": 100.0,
            }
        ]

    def test_renders_expansion_controls_below_long_tables_and_lists(self) -> None:
        """Render overflow controls below long lists and tables so the dashboard scales vertically."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-24T00:00:00+00:00",
                "since": "2026-01-01",
                "until": "2026-07-31",
                "time_anchor": "created_at",
                "top_repository": "acme/repo-1",
                "top_author": "author-1",
                "unique_reviewers": 11,
            },
            "reviewers": [
                {
                    "reviewer_login": f"reviewer-{index}",
                    "review_submissions": 20 - index,
                    "pull_requests_reviewed": 15 - index,
                    "approvals": 10 - index,
                    "changes_requested": index % 3,
                    "comments": index,
                    "authors_supported": 10 - index,
                }
                for index in range(11)
            ],
            "pull_requests": [
                _manual_pull_request(
                    repository_full_name=f"acme/repo-{index}",
                    pull_request_number=index,
                    author_login=f"author-{index}",
                    created_at=(
                        datetime.fromisoformat("2026-01-01T09:00:00+00:00")
                        + timedelta(days=(index - 1) * 17)
                    ).isoformat(),
                    merged_at=(
                        datetime.fromisoformat("2026-01-02T09:00:00+00:00")
                        + timedelta(days=(index - 1) * 17)
                    ).isoformat(),
                    changed_lines=10 * index,
                    additions=8 * index,
                    deletions=2 * index,
                    first_review_hours=float(index),
                    merge_hours=24.0 + index,
                    size_bucket=("XS", "S", "M", "L", "XL")[(index - 1) % 5],
                )
                for index in range(1, 14)
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)
        html = render_dashboard_html(prepared)

        # Then
        assert 'class="table-footer"' in html
        assert html.index('id="author-roster-toggle"') > html.index('class="person-list"')
        assert html.index('id="reviewer-toggle"') > html.index('id="reviewer-extra"')
        assert html.index('id="repository-toggle"') > html.index('id="repository-extra"')
        assert html.index('id="weekly-trend-toggle"') > html.index('id="weekly-trend-extra"')
        assert html.index('id="monthly-trend-toggle"') > html.index('id="monthly-trend-extra"')

    def test_reference_trend_tables_render_newest_periods_first(self) -> None:
        """Render recent and older reference trend rows in newest-to-oldest order."""
        # Given
        payload = {
            "overview": {
                "org": "acme",
                "generated_at": "2026-04-24T00:00:00+00:00",
                "since": "2026-01-01",
                "until": "2026-04-30",
                "time_anchor": "created_at",
                "top_repository": "acme/api",
                "top_author": "alice",
                "unique_reviewers": 1,
            },
            "reviewers": [
                {
                    "reviewer_login": "reviewer-1",
                    "review_submissions": 8,
                    "pull_requests_reviewed": 8,
                    "approvals": 8,
                    "changes_requested": 0,
                    "comments": 0,
                    "authors_supported": 1,
                },
            ],
            "pull_requests": [
                _manual_pull_request(
                    repository_full_name="acme/api",
                    pull_request_number=index,
                    author_login="alice",
                    created_at=(
                        datetime.fromisoformat("2026-01-01T09:00:00+00:00")
                        + timedelta(days=(index - 1) * 31)
                    ).isoformat(),
                    merged_at=(
                        datetime.fromisoformat("2026-01-02T09:00:00+00:00")
                        + timedelta(days=(index - 1) * 31)
                    ).isoformat(),
                    changed_lines=10 * index,
                    additions=8 * index,
                    deletions=2 * index,
                    first_review_hours=float(index),
                    merge_hours=24.0,
                    size_bucket="S",
                )
                for index in range(1, 9)
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)

        # Then
        assert [row["period_key"] for row in prepared.monthly_trends_recent] == [
            "2026-08",
            "2026-07",
            "2026-06",
            "2026-05",
            "2026-04",
            "2026-03",
        ]
        assert [row["period_key"] for row in prepared.monthly_trends_older] == [
            "2026-02",
            "2026-01",
        ]

    def test_renders_latency_quality_summary_and_chart_tooltip_wiring(self) -> None:
        """Render latency-quality summary cards and explicit chart tooltip wiring in the dashboard shell."""
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
                    "reviewer_login": "reviewer-1",
                    "review_submissions": 2,
                    "pull_requests_reviewed": 2,
                    "approvals": 2,
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
                    repository_full_name="acme/web",
                    pull_request_number=2,
                    author_login="bob",
                    created_at="2026-04-03T09:00:00+00:00",
                    merged_at="2026-04-05T09:00:00+00:00",
                    changed_lines=40,
                    additions=30,
                    deletions=10,
                    first_review_hours=12.0,
                    merge_hours=48.0,
                    size_bucket="S",
                ),
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)
        html = render_dashboard_html(prepared)

        # Then
        assert "Latency and quality" in html
        assert "within 24h" in html
        assert "chart-tooltip" in html
        assert "data-point-label=" in html
        assert "showChartTooltip" in html
        assert "positionChartTooltip" in html

    def test_right_aligns_profile_composition_value_column(self) -> None:
        """Render right-aligned numeric/value columns inside the profile composition mini tables."""
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
                "unique_reviewers": 1,
            },
            "reviewers": [
                {
                    "reviewer_login": "alice",
                    "review_submissions": 3,
                    "pull_requests_reviewed": 2,
                    "approvals": 2,
                    "changes_requested": 0,
                    "comments": 1,
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
                    author_login="alice",
                    created_at="2026-04-03T09:00:00+00:00",
                    merged_at="2026-04-04T09:00:00+00:00",
                    changed_lines=20,
                    additions=15,
                    deletions=5,
                    first_review_hours=2.0,
                    merge_hours=24.0,
                    size_bucket="S",
                ),
            ],
        }

        # When
        prepared = prepare_dashboard_payload(payload)
        html = render_dashboard_html(prepared)

        # Then
        assert '["Value", "value", true]' in html
        assert 'const numericClass = isNumeric || typeof value === "number" ? ` class="num"` : "";' in html

    def test_sorts_reviewer_leaderboard_by_reviewed_pull_requests_first(
        self,
    ) -> None:
        """Sort the reviewer leaderboard by reviewed PR count before review submission count."""
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
                    "review_submissions": 6,
                    "pull_requests_reviewed": 2,
                    "approvals": 2,
                    "changes_requested": 0,
                    "comments": 4,
                    "authors_supported": 2,
                },
                {
                    "reviewer_login": "bob",
                    "review_submissions": 3,
                    "pull_requests_reviewed": 3,
                    "approvals": 3,
                    "changes_requested": 0,
                    "comments": 0,
                    "authors_supported": 3,
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

        # When
        prepared = prepare_dashboard_payload(payload)

        # Then
        assert [row["reviewer_login"] for row in prepared.reviewers] == [
            "bob",
            "alice",
        ]
