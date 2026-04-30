from __future__ import annotations

# ruff: noqa: F403,F405
from ..helpers.output import *


class TestManualDashboardLocalSource:
    def test_refresh_preserves_manifest_repo_filters(
        self,
        monkeypatch,
        tmp_path,
    ) -> None:
        """Preserve include/exclude repo filters from the source manifest during refresh."""
        # Given
        source_output_dir = tmp_path
        raw_root_dir = source_output_dir / "raw" / "month" / "created_at"
        for period_key in ("2026-03", "2026-04"):
            _write_manual_dashboard_source_period(
                period_dir=raw_root_dir / period_key,
                pull_request_rows=[],
                review_rows=[],
                timeline_rows=[],
            )
        _write_manual_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-04",),
            locked_period_keys=("2026-03",),
            as_of="2026-04-27",
        )
        manifest_path = (
            source_output_dir / "manifest" / "month" / "created_at" / "manifest.json"
        )
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_payload["include_repos"] = ["acme/api"]
        manifest_payload["exclude_repos"] = ["acme/legacy"]
        manifest_path.write_text(
            json.dumps(manifest_payload),
            encoding="utf-8",
        )
        source_manifest = _dashboard_module._load_source_manifest(
            org="acme",
            source_output_dir=source_output_dir,
        )
        captured: dict[str, object] = {}

        def fake_build_run_config(**kwargs: object) -> RunConfig:
            captured.update(kwargs)
            return RunConfig.model_validate({})

        monkeypatch.setattr(
            _dashboard_module,
            "build_run_config",
            fake_build_run_config,
        )

        # When
        with pytest.raises(RuntimeError, match="invalid run configuration"):
            _dashboard_module._refresh_local_source_outputs(
                org="acme",
                as_of=date.fromisoformat("2026-04-27"),
                source_output_dir=source_output_dir,
                source_manifest=source_manifest,
            )

        # Then
        assert captured["include_repos"] == ["acme/api"]
        assert captured["exclude_repos"] == ["acme/legacy"]

    def test_generate_report_refreshes_using_today_instead_of_historical_until(
        self,
        monkeypatch,
        tmp_path,
    ) -> None:
        """Refresh the shared source output using today's open period rather than the report's historical until date."""
        # Given
        captured: dict[str, object] = {}

        def fake_try_load_source_manifest(
            *,
            org: str,
            source_output_dir: Path,
        ) -> None:
            captured["manifest_org"] = org
            captured["manifest_source_output_dir"] = source_output_dir
            return None

        def fake_refresh_local_source_outputs(
            *,
            org: str,
            as_of: date,
            source_output_dir: Path,
            source_manifest: object,
        ) -> None:
            captured["refresh_org"] = org
            captured["refresh_as_of"] = as_of
            captured["refresh_source_output_dir"] = source_output_dir
            captured["refresh_source_manifest"] = source_manifest

        def fake_build_dashboard_payload_from_local_outputs(
            *,
            org: str,
            since: date,
            until: date,
            source_output_dir: Path,
        ) -> dict[str, Any]:
            captured["payload_org"] = org
            captured["payload_since"] = since
            captured["payload_until"] = until
            captured["payload_source_output_dir"] = source_output_dir
            return {"pull_requests": []}

        def fake_write_outputs(
            *,
            output_dir: Path,
            base_name: str,
            payload: dict[str, Any],
            distribution_percentile: int,
        ) -> dict[str, object]:
            captured["output_dir"] = output_dir
            captured["base_name"] = base_name
            captured["payload"] = payload
            captured["distribution_percentile"] = distribution_percentile
            return {"html_path": "report.html"}

        monkeypatch.setattr(
            _dashboard_module,
            "_try_load_source_manifest",
            fake_try_load_source_manifest,
        )
        monkeypatch.setattr(
            _dashboard_module,
            "_refresh_local_source_outputs",
            fake_refresh_local_source_outputs,
        )
        monkeypatch.setattr(
            _dashboard_module,
            "build_dashboard_payload_from_local_outputs",
            fake_build_dashboard_payload_from_local_outputs,
        )
        monkeypatch.setattr(
            _dashboard_module,
            "_write_outputs",
            fake_write_outputs,
        )

        # When
        _dashboard_module.generate_dashboard_report(
            org="acme",
            since=date.fromisoformat("2026-01-01"),
            until=date.fromisoformat("2026-03-31"),
            source_output_dir=tmp_path / "source",
            output_dir=tmp_path / "dashboard",
            base_name="acme-created-at-since-2026-01-01",
            refresh=True,
            distribution_percentile=99,
        )

        # Then
        assert captured["refresh_as_of"] == date.today()
        assert captured["payload_until"] == date.fromisoformat("2026-03-31")
        assert captured["distribution_percentile"] == 99

    def test_builds_dashboard_payload_from_locked_and_refreshed_local_periods(
        self,
        tmp_path,
    ) -> None:
        """Build the manual dashboard payload from local manifest-backed raw snapshots across locked and refreshed periods."""
        # Given
        source_output_dir = tmp_path
        raw_root_dir = source_output_dir / "raw" / "month" / "created_at"
        _write_manual_dashboard_source_period(
            period_dir=raw_root_dir / "2026-03",
            pull_request_rows=[
                _manual_dashboard_pull_request_row(
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
                _manual_dashboard_review_row(
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
        _write_manual_dashboard_source_period(
            period_dir=raw_root_dir / "2026-04",
            pull_request_rows=[
                _manual_dashboard_pull_request_row(
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
            review_rows=[
                _manual_dashboard_review_row(
                    period_key="2026-04",
                    repository_full_name="acme/web",
                    pull_request_number=2,
                    review_id=201,
                    author_login="reviewer-2",
                    submitted_at="2026-04-03T12:00:00+00:00",
                ),
            ],
            timeline_rows=[
                _manual_dashboard_timeline_event_row(
                    period_key="2026-04",
                    repository_full_name="acme/web",
                    pull_request_number=2,
                    event_id=301,
                    event="review_requested",
                    created_at="2026-04-03T10:00:00+00:00",
                    requested_reviewer_login="reviewer-2",
                ),
            ],
        )
        _write_manual_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-04",),
            locked_period_keys=("2026-03",),
            as_of="2026-04-18",
        )

        # When
        payload = build_dashboard_payload_from_local_outputs(
            org="acme",
            since=date.fromisoformat("2026-03-01"),
            until=date.fromisoformat("2026-04-18"),
            source_output_dir=source_output_dir,
        )

        # Then
        assert payload.overview.pull_requests == 2
        assert payload.overview.merged_pull_requests == 2
        assert payload.overview.review_submissions == 2
        assert payload.overview.top_author == "alice"
        assert payload.overview.top_repository == "acme/api"
        assert payload.overview.median_first_review_hours == 1.5
        assert [row.repository_full_name for row in payload.pull_requests] == [
            "acme/api",
            "acme/web",
        ]

    def test_rejects_manual_dashboard_window_when_local_manifest_has_gaps(
        self,
        tmp_path,
    ) -> None:
        """Reject manual dashboard rendering when the requested window extends into an uncovered locked period."""
        # Given
        source_output_dir = tmp_path
        raw_root_dir = source_output_dir / "raw" / "month" / "created_at"
        _write_manual_dashboard_source_period(
            period_dir=raw_root_dir / "2026-04",
            pull_request_rows=[
                _manual_dashboard_pull_request_row(
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
        _write_manual_dashboard_source_manifest(
            source_output_dir=source_output_dir,
            refreshed_period_keys=("2026-04",),
            locked_period_keys=(),
            as_of="2026-04-18",
        )

        # When
        error_message = ""
        try:
            build_dashboard_payload_from_local_outputs(
                org="acme",
                since=date.fromisoformat("2026-03-01"),
                until=date.fromisoformat("2026-04-18"),
                source_output_dir=source_output_dir,
            )
        except RuntimeError as exc:
            error_message = str(exc)

        # Then
        assert "Missing periods: 2026-03" in error_message

