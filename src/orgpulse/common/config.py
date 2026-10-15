from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from orgpulse.common.errors import ConfigError

DEFAULT_CONFIG_PATH = Path("orgpulse.toml")
DEFAULT_SYNC_DAYS = 365
LOCALES = ("en", "ko")


@dataclass(frozen=True)
class Config:
    """Settings loaded from orgpulse.toml."""

    org: str
    database: Path
    since: date
    timezone: tzinfo
    locale: str
    repos: tuple[str, ...] = ()
    exclude_repos: tuple[str, ...] = ()
    groups: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def people(self) -> tuple[str, ...] | None:
        """Everyone in any group, in config order; None when no groups are set."""

        if not self.groups:
            return None
        seen: dict[str, str] = {}
        for members in self.groups.values():
            for login in members:
                seen.setdefault(login.lower(), login)
        return tuple(seen.values())

    def team_of(self, login: str) -> str | None:
        """The first group listing `login`, matched case-insensitively."""

        return next(
            (
                name
                for name, members in self.groups.items()
                if login.lower() in (member.lower() for member in members)
            ),
            None,
        )

    def includes_repository(self, full_name: str) -> bool:
        name = _repo_name(full_name)
        if self.repos and name not in self.repos:
            return False
        return name not in self.exclude_repos


def load_config(path: Path) -> Config:
    """Read and validate an orgpulse.toml file."""

    try:
        raw = tomllib.loads(path.read_text())
    except FileNotFoundError as exc:
        raise ConfigError(
            f"Config file not found: {path}. See README.md for an example."
        ) from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {path}: {exc}") from exc
    try:
        return _parse_config(raw, base_dir=path.parent)
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"Invalid config in {path}: {exc}") from exc


def _parse_config(raw: dict[str, Any], *, base_dir: Path) -> Config:
    org = raw.get("org")
    if not isinstance(org, str) or not org.strip():
        raise ValueError("`org` is required")
    locale = raw.get("locale", "en")
    if locale not in LOCALES:
        raise ValueError(f"`locale` must be one of {', '.join(LOCALES)}")
    groups = raw.get("groups", {})
    return Config(
        org=org.strip(),
        database=base_dir / raw.get("database", "orgpulse.db"),
        since=_date(raw.get("since", date.today() - timedelta(days=DEFAULT_SYNC_DAYS))),
        timezone=_timezone(raw.get("timezone")),
        locale=locale,
        repos=tuple(_repo_name(repo) for repo in raw.get("repos", [])),
        exclude_repos=tuple(_repo_name(repo) for repo in raw.get("exclude_repos", [])),
        groups={
            name: tuple(str(login) for login in members)
            for name, members in groups.items()
        },
    )


def _date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _timezone(value: object) -> tzinfo:
    if value is None:
        local = datetime.now().astimezone().tzinfo
        assert local is not None
        return local
    try:
        return ZoneInfo(str(value))
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone {value!r}") from exc


def _repo_name(value: str) -> str:
    return value.rsplit("/", 1)[-1].lower()
