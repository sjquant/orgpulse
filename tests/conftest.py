from __future__ import annotations

import pytest

from orgpulse.config import get_settings


@pytest.fixture(autouse=True)
def reset_settings_cache() -> None:
    """Clear cached settings around each test so env overrides remain deterministic."""
    # Given
    get_settings.cache_clear()

    # When
    yield

    # Then
    get_settings.cache_clear()
