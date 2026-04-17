from __future__ import annotations

from datetime import date
from functools import cache
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from orgpulse.models import OrgSlug, PeriodGrain, RunMode


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ORGPULSE_",
        extra="ignore",
        frozen=True,
    )

    as_of: date = Field(default_factory=date.today)
    org: OrgSlug | None = None
    github_token: SecretStr | None = Field(
        default=None,
        exclude=True,
        repr=False,
        validation_alias=AliasChoices("GH_TOKEN"),
    )
    period: PeriodGrain = PeriodGrain.MONTH
    mode: RunMode = RunMode.INCREMENTAL
    output_dir: Path = Field(default_factory=lambda: Path("output"))


@cache
def get_settings() -> AppSettings:
    """Return cached application settings resolved from the environment."""
    return AppSettings()


__all__ = ["AppSettings", "get_settings"]
