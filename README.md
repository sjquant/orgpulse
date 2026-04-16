# orgpulse

GitHub organization metrics snapshots and rollups.

`orgpulse` collects pull request activity across every repository in a GitHub organization,
builds repo-level and org-level summaries, and writes stable file outputs for historical tracking.

## What It Does

- Collects PR, review, and merge data across an organization
- Generates repo-level and org-level metrics
- Writes raw snapshots and summary outputs to files
- Locks past reporting periods so historical results stay stable
- Supports incremental updates and targeted backfills

## Why

Most engineering analytics tools are heavier than needed for an initial rollout.
`orgpulse` is designed for a simpler workflow: run a script, generate files, track history.

## Outputs

- Raw PR CSV snapshots
- Repo summary CSV rollups
- Org summary Markdown and JSON
- Period lock manifest and run metadata

## Planned Metrics

- Merged PR count
- Time to merge
- Time to first review
- PR size
- Commit count
- Active author count
- Merged PR per active author

## Usage

```bash
uv sync
uv run orgpulse run --org your-org --period month
```

## Current CLI Contract

- `orgpulse run` requires a target GitHub organization slug from `--org <org>` or `ORGPULSE_ORG`.
- `RunConfig` is backed by `pydantic-settings`, with `ORGPULSE_*` environment variables available as defaults when a CLI option is omitted.
- GitHub authentication resolves from `GH_TOKEN` first and falls back to `gh auth token` when GitHub CLI auth is already present.
- `orgpulse run` validates that the resolved GitHub credentials can access the target organization before continuing.
- `--period` supports `week` and `month`; default is `month`.
- `--mode` supports `full`, `incremental`, and `backfill`; default is `incremental`.
- `--repo` and `--exclude-repo` may be repeated and cannot overlap.
- `--output-dir` defaults to `output`.
- `--backfill-start` and `--backfill-end` accept ISO dates and are required together when `--mode backfill` is used.
