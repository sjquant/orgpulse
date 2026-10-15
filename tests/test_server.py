import json
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pytest

from orgpulse.apps.web.server import make_server
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


@pytest.mark.parametrize("query", ["group=frontend", "grain=year", "count=53"])
def test_invalid_report_query_is_a_bad_request(base_url, query):
    """An unknown group, unknown grain, or out-of-range count is rejected with
    HTTP 400 and a message."""

    # When / Then
    with pytest.raises(urllib.error.HTTPError) as error:
        _get(f"{base_url}/api/report?{query}")
    assert error.value.code == 400
    assert json.loads(error.value.read())["error"]
