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
orgpulse run --org your-org --period month
```
