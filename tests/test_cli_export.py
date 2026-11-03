import json
from datetime import datetime, timezone
from urllib.parse import parse_qs
from zipfile import ZipFile

import pytest

from orgpulse.cli import main
from orgpulse.common.config import load_config
from orgpulse.libs.store import Store
from tests.factories import merged_pr
from tests.test_html_export import parse_snapshot


@pytest.fixture
def export_config(tmp_path):
    path = tmp_path / "orgpulse.toml"
    path.write_text(
        'org = "acme"\nsince = 2026-01-01\ntimezone = "Asia/Seoul"\n[groups]\nbackend = ["alice", "bob"]\n'
    )
    config = load_config(path)
    prs = [
        merged_pr("bob", "2026-10-01T12:00:00", number=1, commits=2),
        merged_pr("bob", "2026-10-31T14:59:59", number=2, commits=4),
        # 15:00 UTC on October 31 is November 1 in Seoul, and must be excluded.
        merged_pr("bob", "2026-10-31T15:00:00", number=3, commits=99),
    ]
    with Store(config.database) as store:
        store.save(
            "acme/api", prs, since=config.since, synced_at=datetime.now(timezone.utc)
        )
    return path


def run_export(monkeypatch, config, *args):
    monkeypatch.setattr(
        "sys.argv",
        ["orgpulse", "--config", str(config), "export", "--no-avatars", *args],
    )
    monkeypatch.setattr(
        "orgpulse.cli.avatar_urls",
        lambda *args: pytest.fail("offline export attempted a profile request"),
    )
    return main()


def test_cli_person_export_includes_october_31_in_configured_timezone(
    export_config, tmp_path, monkeypatch
):
    output = tmp_path / "personal.html"
    assert (
        run_export(
            monkeypatch,
            export_config,
            "--person",
            "bob",
            "--team-statistic",
            "median",
            "--since",
            "2026-10-01",
            "--until",
            "2026-10-31",
            "--output",
            str(output),
        )
        == 0
    )
    snapshot, external = parse_snapshot(output.read_text())
    assert not external
    assert parse_qs(snapshot["query"])["team_statistic"] == ["median"]
    assert snapshot["meta"]["snapshot"]["until"] == "2026-10-31"
    stats = snapshot["reports"]["mean:0"]["summary"]["current"]
    assert stats["merged"] == 2 and stats["commits"] == 3
    assert len(snapshot["reports"]["mean:0"]["recent"]) == 2


def test_cli_group_zip_and_initial_analysis_settings(
    export_config, tmp_path, monkeypatch
):
    output = tmp_path / "group.zip"
    assert (
        run_export(
            monkeypatch,
            export_config,
            "--group",
            "backend",
            "--until",
            "2026-10-31",
            "--statistic",
            "median",
            "--exclude-outliers",
            "--output",
            str(output),
        )
        == 0
    )
    with ZipFile(output) as bundle:
        assert len(bundle.namelist()) == 2
        for filename in bundle.namelist():
            snapshot, _ = parse_snapshot(bundle.read(filename).decode())
            analysis = json.loads(parse_qs(snapshot["query"])["analysis"][0])
            assert analysis["commits"] == {
                "statistic": "median",
                "exclude_outliers": True,
            }
            assert snapshot["meta"]["snapshot"]["from"] == "2026-01-01"


def test_cli_never_overwrites_an_existing_report(export_config, tmp_path, monkeypatch):
    output = tmp_path / "existing.html"
    output.write_text("keep original")
    assert (
        run_export(
            monkeypatch, export_config, "--person", "bob", "--output", str(output)
        )
        == 1
    )
    assert output.read_text() == "keep original"


def test_cli_invalid_range_does_not_create_a_file(export_config, tmp_path, monkeypatch):
    output = tmp_path / "invalid.html"
    assert (
        run_export(
            monkeypatch,
            export_config,
            "--person",
            "bob",
            "--since",
            "2026-11-01",
            "--until",
            "2026-10-31",
            "--output",
            str(output),
        )
        == 1
    )
    assert not output.exists()
