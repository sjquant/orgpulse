import json
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from orgpulse.apps.web.server import make_server
from orgpulse.common.errors import GitHubError
from orgpulse.libs.store import Store
from tests.factories import make_config, merged_pr


@pytest.fixture
def base_url(tmp_path):
    config = make_config(tmp_path, groups={"backend": ("alice", "bob")})
    with Store(config.database) as store:
        store.save(
            "acme/api",
            [
                merged_pr("alice", "2026-06-02T12:00:00", number=1),
                merged_pr("bob", "2026-06-03T12:00:00", number=2),
                merged_pr("bob", "2026-06-04T12:00:00", number=3),
                merged_pr("carol", "2026-06-03T12:00:00", number=4),
            ],
            since=config.since,
            synced_at=datetime(2026, 6, 5, tzinfo=timezone.utc),
        )
    server = make_server(config, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


def _get(url):
    with urllib.request.urlopen(url) as response:
        return response.status, response.read().decode()


def test_report_for_a_group_only_counts_its_members(base_url):
    """Requesting a configured group returns metrics and rows for its members only."""

    # When
    status, body = _get(
        f"{base_url}/api/report?group=backend&grain=week&count=1&until=2026-06-07"
    )

    # Then
    report = json.loads(body)
    assert status == 200
    assert report["scope"] == {"kind": "group", "name": "backend"}
    assert report["summary"]["current"]["merged"] == 3
    assert [row["login"] for row in report["people"]] == ["bob", "alice"]


def test_person_report_compares_with_their_group(base_url):
    """Requesting a person returns their PRs set against the first group listing them."""

    # When
    _, body = _get(f"{base_url}/api/report?person=bob&count=1&until=2026-06-07")

    # Then
    report = json.loads(body)
    assert report["scope"] == {"kind": "person", "name": "bob", "team": "backend"}
    assert report["summary"]["current"]["merged"] == 2
    assert report["summary"]["compare"]["merged"] == 3
    assert [pr["number"] for pr in report["recent"]] == [3, 2]


def test_meta_lists_grouped_people_and_groups(base_url):
    """With groups configured, the selectable people are the group members, not
    every author in the organization."""

    # When
    _, body = _get(f"{base_url}/api/meta")

    # Then
    meta = json.loads(body)
    assert meta["people"] == ["alice", "bob"]
    assert meta["groups"] == ["backend"]


@pytest.mark.parametrize(
    "query",
    [
        "group=frontend",
        "grain=year",
        "count=157",
        "grain=month&count=37",
        "from=2026-06-10&until=2026-06-01",
        "from=2026-06-01",
        "from=2020-01-01&until=2026-06-01",
        "from=bad&until=2026-06-01",
        "until=2999-01-01",
    ],
)
def test_invalid_report_query_is_a_bad_request(base_url, query):
    """An unknown group, unknown grain, or out-of-range count is rejected with
    HTTP 400 and a message."""

    # When / Then
    with pytest.raises(urllib.error.HTTPError) as error:
        _get(f"{base_url}/api/report?{query}")
    assert error.value.code == 400
    assert json.loads(error.value.read())["error"]


@pytest.mark.parametrize("grain,count", [("week", 156), ("month", 36)])
def test_three_year_report_ranges(base_url, grain, count):
    _, body = _get(f"{base_url}/api/report?grain={grain}&count={count}")
    assert len(json.loads(body)["periods"]) == count


def test_custom_range_filters_group_and_person_reports(base_url):
    query = "grain=month&from=2026-06-03&until=2026-06-03"
    _, body = _get(f"{base_url}/api/report?group=backend&{query}")
    report = json.loads(body)
    assert report["summary"]["current"]["merged"] == 1
    assert report["periods"][0]["start"] == "2026-06-03"
    _, body = _get(f"{base_url}/api/report?person=bob&{query}")
    report = json.loads(body)
    assert report["summary"]["current"]["merged"] == 1
    assert report["summary"]["compare"]["merged"] == 1


@contextmanager
def _feature_server(tmp_path, **options):
    server = make_server(make_config(tmp_path), "127.0.0.1", 0, **options)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _post(url, **headers):
    request = urllib.request.Request(
        url,
        data=b"{}",
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.status, json.load(response)


def _wait_for_sync(base):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        _, body = _get(f"{base}/api/sync")
        status = json.loads(body)
        if status["state"] != "running" and status["finished_at"]:
            return status
        time.sleep(0.01)
    pytest.fail("sync did not finish")


def test_background_sync_is_single_flight_and_reports_progress(tmp_path):
    release = threading.Event()
    progressed = threading.Event()

    def runner(progress):
        progress("acme/api", 3)
        progressed.set()
        assert release.wait(5)
        return 3

    with _feature_server(tmp_path, sync_runner=runner) as base:
        try:
            status, _ = _post(f"{base}/api/sync", Origin=base)
            assert status == 202
            assert progressed.wait(2)
            _, body = _get(f"{base}/api/sync")
            progress = json.loads(body)
            assert progress["state"] == "running"
            assert progress["repositories"] == 1
            assert progress["processed_prs"] == 3
            assert _get(f"{base}/api/report")[0] == 200
            with pytest.raises(urllib.error.HTTPError) as error:
                _post(f"{base}/api/sync", Origin=base)
            assert error.value.code == 409
        finally:
            release.set()
        final = _wait_for_sync(base)
        assert final["state"] == "success"
        assert final["processed_prs"] == 3


def test_sync_error_is_visible_and_can_be_retried(tmp_path):
    attempts = []

    def runner(progress):
        attempts.append(1)
        if len(attempts) == 1:
            raise GitHubError("GitHub authentication is required")
        return 0

    with _feature_server(tmp_path, sync_runner=runner) as base:
        _post(f"{base}/api/sync")
        failure = _wait_for_sync(base)
        assert failure["state"] == "error"
        assert failure["error"] == "GitHub authentication is required"
        _post(f"{base}/api/sync")
        assert _wait_for_sync(base)["state"] == "success"


def test_unexpected_sync_error_does_not_expose_internal_details(tmp_path):
    def runner(progress):
        raise RuntimeError("secret-token-must-not-leak")

    with _feature_server(tmp_path, sync_runner=runner) as base:
        _post(f"{base}/api/sync")
        status = _wait_for_sync(base)
        assert status["state"] == "error"
        assert "secret-token" not in json.dumps(status)


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({"Origin": "https://unrelated.example"}, 403),
        ({"Host": "unrelated.example"}, 403),
        ({"Sec-Fetch-Site": "cross-site"}, 403),
        ({"Content-Type": "text/plain"}, 415),
    ],
)
def test_sync_rejects_cross_site_or_non_json_requests(tmp_path, headers, expected):
    calls = []
    with _feature_server(
        tmp_path, sync_runner=lambda progress: calls.append(1) or 0
    ) as base:
        with pytest.raises(urllib.error.HTTPError) as error:
            _post(f"{base}/api/sync", **headers)
        assert error.value.code == expected
        assert not calls


def test_sync_requires_a_configured_runner(base_url):
    with pytest.raises(urllib.error.HTTPError) as error:
        _post(f"{base_url}/api/sync")
    assert error.value.code == 503


def test_profile_urls_are_cached_and_optional(tmp_path):
    calls = []

    def profiles(people):
        calls.append(people)
        return {"alice": "https://avatars.githubusercontent.com/u/1"}

    with _feature_server(tmp_path, avatar_loader=profiles) as base:
        for _ in range(2):
            _, body = _get(f"{base}/api/meta")
            assert json.loads(body)["avatars"]["alice"].startswith("https://avatars.")
        assert len(calls) == 1

    def unavailable(people):
        raise GitHubError("offline")

    with _feature_server(tmp_path, avatar_loader=unavailable) as base:
        assert json.loads(_get(f"{base}/api/meta")[1])["avatars"] == {}
