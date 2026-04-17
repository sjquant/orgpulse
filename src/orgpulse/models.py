from __future__ import annotations

from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StringConstraints, field_validator, model_validator

ORG_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38}[A-Za-z0-9])?$"
REPO_PATTERN = r"^(?:[A-Za-z0-9_.-]+/)?[A-Za-z0-9_.-]+$"

OrgSlug = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, pattern=ORG_PATTERN)]
RepoSlug = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, pattern=REPO_PATTERN)]


class PeriodGrain(StrEnum):
    WEEK = "week"
    MONTH = "month"


class RunMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


class AuthSource(StrEnum):
    GH_TOKEN = "GH_TOKEN"
    GH_CLI = "gh"


class RunConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    org: OrgSlug
    github_token: SecretStr | None = Field(
        default=None,
        exclude=True,
        repr=False,
    )
    period: PeriodGrain = PeriodGrain.MONTH
    mode: RunMode = RunMode.INCREMENTAL
    output_dir: Path = Field(default_factory=lambda: Path("output"))
    include_repos: tuple[RepoSlug, ...] = ()
    exclude_repos: tuple[RepoSlug, ...] = ()
    backfill_start: date | None = None
    backfill_end: date | None = None

    @field_validator("include_repos", "exclude_repos", mode="before")
    @classmethod
    def normalize_repo_filters(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            items = [value]
        else:
            items = list(value)

        deduped: list[str] = []
        seen: set[str] = set()
        for item in items:
            cleaned = item.strip()
            if cleaned and cleaned not in seen:
                deduped.append(cleaned)
                seen.add(cleaned)
        return tuple(deduped)

    @field_validator("output_dir", mode="before")
    @classmethod
    def normalize_output_dir(cls, value: Any) -> Path:
        if value is None:
            return Path("output")
        return Path(value).expanduser()

    @model_validator(mode="after")
    def validate_cross_field_constraints(self) -> "RunConfig":
        overlapping = set(self.include_repos) & set(self.exclude_repos)
        if overlapping:
            overlap = ", ".join(sorted(overlapping))
            raise ValueError(f"repo filters overlap across include and exclude lists: {overlap}")

        for repo_filter in (*self.include_repos, *self.exclude_repos):
            if "/" not in repo_filter:
                continue
            owner, _ = repo_filter.split("/", 1)
            if owner.lower() != self.org.lower():
                raise ValueError(
                    f"repo filter owner must match target org '{self.org}': {repo_filter}"
                )

        has_backfill_bounds = self.backfill_start is not None or self.backfill_end is not None
        if self.mode is RunMode.BACKFILL:
            if self.backfill_start is None or self.backfill_end is None:
                raise ValueError("backfill mode requires both --backfill-start and --backfill-end")
            if self.backfill_start > self.backfill_end:
                raise ValueError("--backfill-start must be on or before --backfill-end")
        elif has_backfill_bounds:
            raise ValueError("backfill date bounds are only valid when --mode backfill is selected")

        return self


class ResolvedToken(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: AuthSource
    token: str


class GitHubTargetContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    auth_source: AuthSource
    viewer_login: str
    organization_login: str
