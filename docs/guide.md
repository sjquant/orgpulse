# User guide

Setup options, metric definitions, and report exports.

## Setup

You need Python 3.11+, [uv](https://docs.astral.sh/uv/), and GitHub credentials
that can read your organization: set `GH_TOKEN` or sign in with `gh auth login`.

```bash
uv sync
```

Create `orgpulse.toml`:

```toml
org = "acme"
since = 2025-01-01          # earliest PR creation date to collect
timezone = "Asia/Seoul"    # reporting dates and week/month boundaries
locale = "en"              # "en" or "ko"

[groups]
backend = ["alice", "bob"]
frontend = ["carol", "dave"]
```

Then collect data and open the dashboard:

```bash
uv run orgpulse sync
uv run orgpulse serve       # http://127.0.0.1:8765
```

The first sync collects history; later syncs fetch changes. Move `since` earlier
to backfill older history. If omitted, it defaults to one year ago. Pass
`--config <path>` before a command to use another configuration file.

Optional configuration: `database` chooses the local data file (default:
`orgpulse.db` beside the config), `repos` includes only named repositories, and
`exclude_repos` omits them. Without `timezone`, reporting uses system time.

## Explore the dashboard

Choose a group or person, then click a metric to chart it. Compare up to
156 weeks or 36 months, or choose inclusive start/end dates for a custom range
of up to 1,096 days. Changing the view does not collect more history.

A person is compared with the first configured group listing them. With several
groups, Everyone combines their members; with one group, that group opens by
default. Without groups, people with merged PRs are listed.

Use **Sync now** to refresh data while continuing to read reports. Progress and
errors appear beside the button, and results refresh when it finishes. Keep the
server running until completion. Avoid a separate CLI sync at the same time;
the web button prevents duplicate jobs within that server. Photos load when
available and fall back to initials. Credentials stay on your computer.

### Six metrics

| Metric | What it shows |
|---|---|
| Merged PRs / person | Merged PRs per person per elapsed week or month |
| Time to merge | Time from PR creation to merge; median by default, including Draft time, nights, and weekends |
| PR size | Added + deleted lines per PR; median by default |
| First review | Time from ready-for-review to the first review by someone other than the author; median by default |
| Total changed lines | Added + deleted lines across the selected PRs; neither net growth nor unique lines |
| Commits / PR | Commit count on the original GitHub PR, before squash/rebase merging; mean by default |

In a personal view, changed-line totals are compared with the **team's per-person
average**, including configured members with no PRs in the range. Group views
retain actual totals. PR counts in an unfinished week/month are projected to a
full period at the current pace and drawn dashed. Other unfinished-period values
are observed statistics, not forecasts.

### Adjust the analysis

Open **Analysis options** on time, size, review-wait, or commit charts to choose
mean/median or exclude outliers for that metric. Outlier exclusion starts off.
It uses 1.5×IQR fences with at least four known values, calculated separately for
each displayed scope and chart bucket. The chart shows analyzed, excluded, and
missing counts. PR counts and changed-line totals never change with these options.
Your selections stay in the URL.

Orange markers highlight unusually high values relative to the team; they are
prompts to investigate, not performance scores. PR activity alone does not measure
a person's contribution or the quality of the work.

## Save a report

On a person's page, click **Download HTML report**. On a group page, choose
**Personal reports ZIP** for one HTML file per member.

Reports keep the chosen dates, timezone, data-sync time, and initial analysis
settings. Open them in a browser without orgpulse or Internet access; switching
metrics and analysis options still works. They contain that person's recent PRs
and aggregate team comparisons. Export again for another period or newer data.

You can also export without starting the web server:

```bash
uv run orgpulse export --group backend \
  --since 2025-01-01 --until 2026-10-31 \
  --output reports/backend-through-2026-10-31.zip

uv run orgpulse export --person alice \
  --since 2025-01-01 --until 2026-10-31 \
  --output reports/alice-through-2026-10-31.html
```

Both dates include the entire day in the configured timezone. Omit `--since` to
use the configured history start. `--statistic mean|median` and
`--exclude-outliers` set the initial distribution-metric view. `--no-avatars`
avoids all network requests and uses initials. Export reads stored data without
syncing and refuses to overwrite an existing output file.

## Data coverage

Only merged, human-authored PRs **created on or after the configured `since` date**
are collected. Earlier-created PRs are excluded even if merged during a selected
reporting period. Bot PRs/reviews, unmerged PRs, issues, and full commit histories
are outside this report. Collection covers repositories visible to the credential.

Periods before collected history show no data. Unknown commit counts and PRs
without a qualifying review are excluded from those statistics, not counted as
zero. The next sync fills missing commit counts in an existing database. First
review currently checks the first 30 reviews and first ready-for-review event per
PR. Changing group membership also changes historical comparisons; the report
does not reconstruct past team rosters.

## Development

```bash
uv sync --group dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run ty check
```
