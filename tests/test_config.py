from datetime import date

import pytest

from orgpulse.common.config import load_config
from orgpulse.common.errors import ConfigError


def test_config_file_resolves_paths_repos_and_ai_dates(tmp_path):
    """A config file resolves the database next to itself, normalizes repo names,
    and lets people override the AI adoption date case-insensitively."""

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

[ai]
adopted_on = 2025-06-01

[ai.people]
Bob = "2025-09-01"
"""
    )

    # When
    config = load_config(path)

    # Then
    assert config.database == tmp_path / "orgpulse.db"
    assert config.groups == {"backend": ("alice", "bob")}
    assert not config.includes_repository("acme/legacy")
    assert config.includes_repository("acme/api")
    assert config.ai is not None
    assert config.ai.adopted_on_for("alice") == date(2025, 6, 1)
    assert config.ai.adopted_on_for("bob") == date(2025, 9, 1)


def test_config_without_org_is_rejected(tmp_path):
    """A config file missing `org` fails with a message naming the field."""

    # Given
    path = tmp_path / "orgpulse.toml"
    path.write_text('since = "2025-01-01"\n')

    # When / Then
    with pytest.raises(ConfigError, match="`org` is required"):
        load_config(path)
