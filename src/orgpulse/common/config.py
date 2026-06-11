from __future__ import annotations

from datetime import date
from functools import cache
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from orgpulse.common.models import (
    OrgSlug,
    PeriodGrain,
    ReportLocale,
    RunConfig,
    RunMode,
    TimeAnchor,
)


class AppSettings(BaseSettings):
    """Represent environment-backed defaults for orgpulse commands."""

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
    time_anchor: TimeAnchor = TimeAnchor.CREATED_AT
    mode: RunMode = RunMode.INCREMENTAL
    output_dir: Path = Field(default_factory=lambda: Path("output"))
    locale: str = ReportLocale.EN.value


@cache
def get_settings() -> AppSettings:
    """Return cached application settings resolved from the environment."""
    return AppSettings()


def build_run_config(
    *,
    org: str | None = None,
    as_of: date | str | None = None,
    period: PeriodGrain | None = None,
    mode: RunMode | None = None,
    time_anchor: TimeAnchor | None = None,
    include_repos: list[str] | None = None,
    exclude_repos: list[str] | None = None,
    output_dir: Path | None = None,
    backfill_start: str | None = None,
    backfill_end: str | None = None,
) -> RunConfig:
    """Build a validated run configuration from CLI inputs and defaults."""

    settings = get_settings()
    payload: dict[str, object] = {
        "org": settings.org if org is None else org,
        "github_token": settings.github_token,
        "as_of": settings.as_of if as_of is None else as_of,
        "period": settings.period if period is None else period,
        "time_anchor": settings.time_anchor if time_anchor is None else time_anchor,
        "mode": settings.mode if mode is None else mode,
        "output_dir": settings.output_dir if output_dir is None else output_dir,
    }
    if include_repos is not None:
        payload["include_repos"] = include_repos
    if exclude_repos is not None:
        payload["exclude_repos"] = exclude_repos
    if backfill_start is not None:
        payload["backfill_start"] = backfill_start
    if backfill_end is not None:
        payload["backfill_end"] = backfill_end
    return RunConfig.model_validate(payload)


__all__ = ["AppSettings", "build_run_config", "get_settings"]
