# orgpulse

GitHub organization metrics snapshots, rollups, and local reporting.

`orgpulse` collects pull request activity across a GitHub organization, writes
stable raw snapshots, and turns those snapshots into repo, org, person,
analysis, and dashboard views. It is built for repeatable operator runs: closed
periods stay locked, the current period remains refreshable, and local reports
can be regenerated without refetching GitHub history.

## Highlights

- Collect PR, review, timeline, merge, size, author, reviewer, and repository
  metrics across an organization.
- Bucket outputs by reporting grain (`month` or `week`) and PR time anchor
  (`created_at`, `updated_at`, or `merged_at`).
- Use `incremental`, `full`, and `backfill` run modes for routine refreshes,
  complete rebuilds, and targeted historical repair.
- Keep a canonical raw inventory so alternate grains or anchors can be
  reaggregated locally.
- Render local analysis, person metrics, and dashboard artifacts as JSON, CSV,
  Markdown, or interactive HTML.

## Setup

Requirements:

- Python `3.11+`
- `uv`
- GitHub credentials that can read the target organization

Install runtime dependencies:

```bash
uv sync
```

Install contributor tooling:

```bash
uv sync --group dev
```

`orgpulse` resolves GitHub credentials from `GH_TOKEN`, then from
`gh auth token` if the GitHub CLI is already logged in.

```bash
export GH_TOKEN=ghp_your_token_here
uv run orgpulse run --org acme
```

## Core Workflow

Run the collector for the default monthly `created_at` view:

```bash
uv run orgpulse run \
  --org acme \
  --mode incremental \
  --period month \
  --time-anchor created_at \
  --output-dir output
```

Rebuild a different local view from the stored canonical inventory:

```bash
uv run orgpulse reaggregate \
  --org acme \
  --period week \
  --time-anchor updated_at \
  --output-dir output
```

Analyze stored snapshots without touching GitHub:

```bash
uv run orgpulse analyze \
  --org acme \
  --period month \
  --group-by repository \
  --time-anchor created_at \
  --since 2026-04-01 \
  --until 2026-04-30 \
  --format html \
  --output-dir output > analysis.html
```

Extract one person's metrics:

```bash
uv run orgpulse person alice \
  --org acme \
  --since 2026-01-01 \
  --until 2026-04-30 \
  --source-output-dir output \
  --format markdown
```

Render the supported dashboard from local `month/created_at` outputs:

```bash
uv run orgpulse dashboard \
  --org acme \
  --since 2026-01-01 \
  --until 2026-04-27 \
  --source-output-dir output \
  --output-dir output/acme-review/manual-2026-04-27 \
  --distribution-percentile 99
```

## Commands

`orgpulse run` fetches GitHub data and writes raw snapshots, manifests, repo
summaries, and org summaries.

Useful options:

- `--org <slug>`: target organization. Falls back to `ORGPULSE_ORG`.
- `--as-of <YYYY-MM-DD>`: anchor date for the current open period. Falls back
  to `ORGPULSE_AS_OF` or today.
- `--period <month|week>`: reporting grain. Falls back to `ORGPULSE_PERIOD`.
- `--time-anchor <created_at|updated_at|merged_at>`: timestamp used to bucket
  PRs. Falls back to `ORGPULSE_TIME_ANCHOR`.
- `--mode <incremental|full|backfill>`: run strategy. Falls back to
  `ORGPULSE_MODE`.
- `--repo <name-or-org/name>` and `--exclude-repo <name-or-org/name>`:
  repeatable repository filters.
- `--output-dir <path>`: output root. Falls back to `ORGPULSE_OUTPUT_DIR`.
- `--backfill-start` and `--backfill-end`: required together for backfills.

`orgpulse reaggregate` rebuilds period snapshots and rollups from
`raw_inventory/` without GitHub auth. Use it after a successful `run` when you
need another grain or time anchor for the same org and repository scope.

`orgpulse analyze` reads local manifests and raw snapshots, then groups metrics
by `period`, `repository`, or `author`.

`orgpulse person` reads local outputs for one GitHub login. Authored PR metrics
use `--time-anchor`; review-given metrics use review submission dates.

`orgpulse dashboard` validates local `month/created_at` coverage, optionally
refreshes the open period, and writes dashboard JSON, per-PR CSV, and HTML.

`orgpulse dashboard-render` re-renders HTML from an existing dashboard JSON
payload.

## Run Modes

`incremental` is the normal scheduled mode. It refreshes the current open
period, preserves locked closed periods when the manifest contract still
matches, and promotes closed open-period outputs into history on later runs.

`full` rebuilds discovered history up to `--as-of`, rewrites refreshed periods,
and prunes stale period directories for the selected grain and time anchor.

`backfill` rewrites an explicit closed-period range. Both bounds must align to
the selected period and end before the current open period.

```bash
uv run orgpulse run \
  --org acme \
  --mode backfill \
  --period month \
  --as-of 2026-05-18 \
  --backfill-start 2026-03-01 \
  --backfill-end 2026-04-30 \
  --output-dir output
```

## Output Layout

For `--output-dir output --period month --time-anchor created_at`:

```text
output/
  raw_inventory/
    contract.json
    pull_requests.csv
    pull_request_reviews.csv
    pull_request_timeline_events.csv
  raw/month/created_at/
    2026-04/
      pull_requests.csv
      pull_request_reviews.csv
      pull_request_timeline_events.csv
  manifest/month/created_at/
    manifest.json
    index.json
    README.md
  repo_summary/month/created_at/
    contract.json
    index.json
    README.md
    latest/repo_summary.csv
    2026-04/repo_summary.csv
  org_summary/month/created_at/
    contract.json
    index.json
    README.md
    latest/summary.json
    latest/summary.md
    2026-04/summary.json
    2026-04/summary.md
```

Use `latest/` for simple downstream automation, `index.json` for machine-readable
history, and generated `README.md` files for operator inspection.

Local analysis, person metrics, and dashboard generation all read manifest-backed
snapshots. They fail fast when the org, grain, anchor, freshness, or dashboard
coverage does not match the requested report.

## Metric Surface

Current outputs cover:

- PR throughput by org, repository, author, reviewer, and period
- merged, open, stale-open, and merge-rate metrics
- time to first review, merge, and close
- review coverage, review submissions, unique reviewers, and 24-hour review SLA
- PR size by additions, deletions, changed lines, changed files, and commits
- active author counts, PRs per active author, and changed lines per active
  author
- reviewer workload and unique PRs reviewed
- repository concentration, author concentration, and PR size-bucket diagnostics

## Development

```bash
uv run pytest
uv run ruff check .
uv run ty check
```
