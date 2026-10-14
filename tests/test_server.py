import json
import threading
import urllib.error
import urllib.request
from datetime import date, datetime, timezone

import pytest

from orgpulse.apps.web.server import make_server
from orgpulse.common.config import Config
from orgpulse.libs.store import Store
from tests.factories import merged_pr


@pytest.fixture
def base_url(tmp_path):
    config = Config(
        org="acme",
        database=tmp_path / "orgpulse.db",
        since=date(2026, 1, 1),
        timezone=timezone.utc,
        locale="ko",
        groups={"backend": ("alice", "bob")},
    )
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
    """Requesting a configured group returns metrics for its members only."""

    # When
    status, body = _get(
        f"{base_url}/api/report?group=backend&grain=week&count=1&until=2026-06-07"
    )

    # Then
    report = json.loads(body)
    assert status == 200
    assert report["summary"]["current"]["merged"] == 3
    assert [row["login"] for row in report["people"]] == ["bob", "alice"]


def test_meta_lists_people_by_merged_count_and_groups(base_url):
    """The meta endpoint lists authors with the most merged PRs first, along with
    configured groups and the report locale."""

    # When
    _, body = _get(f"{base_url}/api/meta")

    # Then
    meta = json.loads(body)
    assert meta["people"] == ["bob", "alice", "carol"]
    assert meta["groups"] == {"backend": ["alice", "bob"]}
    assert meta["locale"] == "ko"


def test_invalid_report_query_is_a_bad_request(base_url):
    """An unknown group or malformed grain is rejected with HTTP 400 and a message."""

    # When / Then
    for query in ("group=frontend", "grain=year"):
        with pytest.raises(urllib.error.HTTPError) as error:
            _get(f"{base_url}/api/report?{query}")
        assert error.value.code == 400
        assert json.loads(error.value.read())["error"]
