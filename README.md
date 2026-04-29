# orgpulse

GitHub organization metrics snapshots and rollups.

`orgpulse` collects pull request activity across every repository in a GitHub
organization, builds repo-level and org-level summaries, and writes stable file
outputs for historical tracking.

## What It Does

- Collects PR, review, and merge data across an organization
- Generates repo-level and org-level metrics
- Writes normalized raw snapshots plus summary outputs to files
- Locks closed reporting periods so historical results stay stable
- Supports `full`, `incremental`, and `backfill` run modes

## Prerequisites

- Python `3.11+`
- `uv`
- GitHub credentials that can read the target organization

Install the runtime dependencies from the repo root:

```bash
uv sync
```

Install contributor tooling if you also want linting and tests:

```bash
uv sync --group dev
```

## Authentication

`orgpulse` resolves GitHub credentials in this order:

1. `GH_TOKEN`
2. `gh auth token` from an existing GitHub CLI login

Example with an environment token:

```bash
export GH_TOKEN=ghp_your_token_here
uv run orgpulse run --org acme
```

Example with GitHub CLI auth:

```bash
gh auth login
uv run orgpulse run --org acme
```

Before collection starts, `orgpulse` validates that the resolved credentials can
access the target organization.

## CLI Contract

`orgpulse run` accepts these operator-facing options:

- `--org <slug>`: target GitHub organization. Falls back to `ORGPULSE_ORG`.
- `--as-of <YYYY-MM-DD>`: anchor date used to resolve the current open reporting
  period. Falls back to `ORGPULSE_AS_OF` or today.
- `--period <month|week>`: reporting grain. Falls back to `ORGPULSE_PERIOD`.
- `--mode <full|incremental|backfill>`: run strategy. Falls back to
  `ORGPULSE_MODE`.
- `--repo <name-or-org/name>`: include only matching repositories. Repeatable.
- `--exclude-repo <name-or-org/name>`: exclude matching repositories.
  Repeatable.
- `--output-dir <path>`: output root. Falls back to `ORGPULSE_OUTPUT_DIR`.
- `--backfill-start <YYYY-MM-DD>` and `--backfill-end <YYYY-MM-DD>`: required
  together for `--mode backfill`.

Notes:

- `--repo` and `--exclude-repo` cannot overlap.
- If a repo filter is owner-qualified, its owner must match `--org`.
- `orgpulse` writes a JSON run summary to stdout and writes period files under
  `--output-dir`.

## Run Modes

### Incremental

`incremental` is the default and is the normal operator mode.

- Refreshes only the current open period.
- Reuses locked closed periods from the existing manifest when the run contract
  matches the same org, period grain, repo filters, and output root.
- Leaves locked historical raw snapshots and summaries untouched.
- Promotes a previously refreshed open period into locked history after that
  period closes on a later run.

Example:

```bash
uv run orgpulse run \
  --org acme \
  --period month \
  --mode incremental \
  --as-of 2026-04-18 \
  --output-dir output
```

### Full

`full` rebuilds the full discovered history up to `--as-of`.

- Ignores locked-period skipping.
- Rewrites refreshed periods from scratch.
- Prunes stale period directories that no longer belong to the rebuilt history.
- Use it when you want to replace the current snapshot set instead of preserving
  prior locked history.

Example:

```bash
uv run orgpulse run \
  --org acme \
  --period month \
  --mode full \
  --as-of 2026-04-18 \
  --output-dir output
```

### Backfill

`backfill` recalculates an explicit closed-period range without rebuilding the
entire history.

- Requires both `--backfill-start` and `--backfill-end`.
- Both dates must align to the selected period boundary.
- The backfill end date must be before the current open period begins, as
  defined by `--as-of`.
- Rewrites only the requested closed periods and preserves unrelated locked
  history.
- Writes header-only raw CSVs and zero-valued summaries when a requested period
  has no matching pull requests.

Monthly backfill example:

```bash
uv run orgpulse run \
  --org acme \
  --period month \
  --mode backfill \
  --as-of 2026-05-18 \
  --backfill-start 2026-03-01 \
  --backfill-end 2026-04-30 \
  --output-dir output
```

Weekly backfill example:

```bash
uv run orgpulse run \
  --org acme \
  --period week \
  --mode backfill \
  --as-of 2026-05-18 \
  --backfill-start 2026-04-06 \
  --backfill-end 2026-04-19 \
  --output-dir output
```

## Practical Operator Workflow

Use `incremental` for routine scheduled runs. Use `backfill` when you need to
repair or refresh one or more closed periods. Use `full` when you intentionally
want to replace the currently materialized history.

Examples with repo filters:

```bash
uv run orgpulse run \
  --org acme \
  --mode incremental \
  --repo api \
  --repo web \
  --exclude-repo legacy \
  --output-dir output
```

Environment-backed defaults can remove repeated flags:

```bash
export ORGPULSE_ORG=acme
export ORGPULSE_PERIOD=month
export ORGPULSE_MODE=incremental
export ORGPULSE_OUTPUT_DIR=output
uv run orgpulse run --as-of 2026-04-18
```

## Locked-Period Behavior

`orgpulse` treats the current open period as mutable and closed periods as
stable history.

- Incremental runs skip locked periods and refresh only the open period.
- Full and backfill runs refresh locked periods instead of skipping them.
- Closed periods become locked after a successful run.
- Locked periods are carried forward only when the saved manifest still matches
  the same org, period grain, repo filters, and raw snapshot root.
- If the saved manifest contract does not match, `orgpulse` does not reuse those
  historical locks.

This keeps normal runs diff-friendly while still allowing explicit historical
repair when needed.

## Output Layout

For `--output-dir output --period month`, the generated layout is:

```text
output/
  raw/month/
    2026-04/
      pull_requests.csv
      pull_request_reviews.csv
      pull_request_timeline_events.csv
  manifest/month/
    manifest.json
    index.json
    README.md
  repo_summary/month/
    contract.json
    index.json
    README.md
    latest/
      repo_summary.csv
    2026-04/
      repo_summary.csv
  org_summary/month/
    contract.json
    index.json
    README.md
    latest/
      summary.json
      summary.md
    2026-04/
      summary.json
      summary.md
```

## Local Analysis

`orgpulse analyze` reads the local snapshot and manifest outputs and builds
focused analysis views without refetching GitHub data.

- Supports `period`, `repository`, and `author` groupings
- Respects `--since`, `--until`, `--time-anchor`, `--top`, and
  `--distribution-percentile`
- Writes JSON, CSV, Markdown, or interactive HTML to stdout
- Trims upper-tail outliers from distribution-based metrics with
  `--distribution-percentile 95|99|100` where `100` keeps all values
- HTML output includes shared controls, single-series focus mode, and spike
  diagnostics such as same-period-created ratio, older-PR ratio, top
  contributing repositories, top updated dates, and timeline-event breakdowns

`orgpulse dashboard` reads local `month/created_at` outputs and renders the
supported dashboard view as JSON, per-PR CSV, and interactive HTML files.

- Reads local manifest-backed raw snapshots instead of refetching full history
- Currently supports only `month` grain with the `created_at` anchor
- Respects `--since`, `--until`, `--distribution-percentile`, and
  `--refresh/--no-refresh`
- Writes dashboard artifacts under an explicit `--output-dir`
- Reuses the saved local manifest contract from `--source-output-dir`

`orgpulse dashboard-render` re-renders HTML from an existing dashboard JSON
payload without requiring manifest-backed raw snapshots.

Example HTML analysis:

```bash
uv run orgpulse analyze \
  --org acme \
  --grain month \
  --group-by repository \
  --time-anchor updated_at \
  --since 2026-04-01 \
  --until 2026-04-30 \
  --distribution-percentile 95 \
  --format html \
  --output-dir output > analysis.html
```

Example dashboard render:

```bash
uv run orgpulse dashboard \
  --org acme \
  --since 2026-01-01 \
  --until 2026-04-27 \
  --source-output-dir output \
  --output-dir output/acme-review/manual-2026-04-27 \
  --distribution-percentile 99
```

Example render-only refresh:

```bash
uv run orgpulse dashboard-render \
  --input-json output/acme-review/manual-2026-04-27/acme-created-at-since-2026-01-01.json \
  --output-html output/acme-review/manual-2026-04-27/acme-created-at-since-2026-01-01.html \
  --distribution-percentile 99
```

### Raw snapshots

- `raw/<grain>/<period>/pull_requests.csv`: normalized PR rows
- `raw/<grain>/<period>/pull_request_reviews.csv`: normalized review rows
- `raw/<grain>/<period>/pull_request_timeline_events.csv`: timeline events used
  for review timing and state transitions

### Manifest

- `manifest.json`: latest run metadata, including refreshed periods, locked
  periods, and watermarks
- `index.json`: machine-readable index with the latest run metadata plus history
  lists for refreshed and locked periods
- `README.md`: human-readable manifest index

The manifest watermarks track:

- `collection_window_start_date`
- `collection_window_end_date`
- `latest_refreshed_period_end_date`
- `latest_locked_period_end_date`

### Repo summaries

- `<period>/repo_summary.csv`: repo-level rollup for that period
- `latest/repo_summary.csv`: convenience copy of the newest period summary
- `index.json`: machine-readable map of the latest summary and all saved history
- `README.md`: human-readable history table
- `contract.json`: run contract for the saved summary set

### Org summaries

- `<period>/summary.json`: machine-readable org rollup for that period
- `<period>/summary.md`: human-readable org summary for that period
- `latest/summary.json` and `latest/summary.md`: convenience copies of the
  newest period outputs
- `index.json`: machine-readable map of the latest summary pair and all saved
  history
- `README.md`: human-readable history table
- `contract.json`: run contract for the saved summary set

## Latest and Index Files

Use the `latest` files when downstream automation only needs the newest summary
without first resolving a period key.

Use the `index.json` files when automation needs:

- the newest available period key
- the source path behind the `latest` copy
- the full saved history for the current contract
- the manifest's latest run metadata and watermarks

Use the generated `README.md` files when a human operator needs to inspect the
same catalog without opening JSON directly.

## Current Metrics

- Merged PR count
- Time to merge
- Time to first review
- PR size
- Commit count
- Active author count
- Merged PR per active author
