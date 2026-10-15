import pytest

from orgpulse.common.config import load_config
from orgpulse.common.errors import ConfigError


def test_config_file_resolves_paths_repos_and_groups(tmp_path):
    """A config file resolves the database next to itself, normalizes repo names,
    lists each grouped person once, and finds a person's first group by login."""

    # Given
    path = tmp_path / "orgpulse.toml"
    path.write_text(
        """
org = "acme"
since = 2025-01-01
timezone = "Asia/Seoul"
exclude_repos = ["acme/Legacy"]

[groups]
backend = ["alice", "bob"]
platform = ["Bob", "carol"]
"""
    )

    # When
    config = load_config(path)

    # Then
    assert config.database == tmp_path / "orgpulse.db"
    assert not config.includes_repository("acme/legacy")
    assert config.includes_repository("acme/api")
    assert config.members(None) == ("alice", "bob", "carol")
    assert config.members("platform") == ("Bob", "carol")
    assert config.team_of("BOB") == "backend"
    assert config.team_of("dave") is None


def test_config_without_org_is_rejected(tmp_path):
    """A config file missing `org` fails with a message naming the field."""

    # Given
    path = tmp_path / "orgpulse.toml"
    path.write_text('since = "2025-01-01"\n')

    # When / Then
    with pytest.raises(ConfigError, match="`org` is required"):
        load_config(path)
