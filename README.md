# orgpulse

Pull request metrics for the people and groups you care about in a GitHub
organization: how many PRs each person merges, how big they are, how long they
take to merge, and how long they wait for a first review.

`orgpulse sync` pulls pull requests into a local SQLite file. `orgpulse serve`
runs a local dashboard where you pick a group or a person, compare the range
with the one before it, and see how each person compares with their team.

## Setup

Requirements: Python 3.11+, [`uv`](https://docs.astral.sh/uv/), and GitHub
credentials that can read the organization (`GH_TOKEN`, or a logged-in `gh` CLI).

```bash
uv sync
```

Create `orgpulse.toml`:

```toml
org = "acme"
since = 2025-01-01          # earliest PR creation date to sync (default: one year ago)
timezone = "Asia/Seoul"     # buckets days/weeks/months (default: system timezone)
locale = "en"               # report language: "en" or "ko"
# database = "orgpulse.db"  # relative to this file
# repos = ["api", "web"]    # only these repositories
# exclude_repos = ["sandbox"]

# The sidebar lists these groups and their members. A person is compared with
# the first group that lists them. Without groups, everyone who merged a PR is listed.
[groups]
backend = ["alice", "bob"]
frontend = ["carol", "dave"]
```

## Usage

```bash
uv run orgpulse sync     # first run fetches history since `since`; later runs fetch only changes
uv run orgpulse serve    # http://127.0.0.1:8765 (--port to change)
```

Pass `--config <path>` before the command to use another config file. Only merged
PRs are stored; bot-authored PRs and bot reviews are ignored. Moving `since` earlier
backfills on the next sync.

The dashboard offers up to 156 weeks or 36 months, plus a custom start/end date
range of up to 1096 days (about three years). Both custom boundary dates are
inclusive in the configured timezone. Group comparisons use the preceding
same-length date range; person comparisons use their team in the selected range.
Dates before the configured sync history show no data. Viewing a longer range
does not fetch older history automatically.

GitHub profile photos are shown when available, with initials as the offline
fallback. Use **Sync now** in the sidebar to run the same incremental collection
as the CLI without blocking the report. Progress and errors appear below the
button; the report refreshes when the job finishes. One web sync can run per
server at a time. Keep the local server running until it finishes, and avoid
running a separate CLI sync concurrently. Authentication stays on the server.

## Metrics

Four metrics, per week or month, never cumulative. Click a metric to chart it.

| Metric | Definition |
|---|---|
| Merged PRs / person | Merged PRs ÷ people in scope ÷ elapsed periods, by merge date |
| Time to merge | Median time from opening a PR to merging it |
| PR size | Median lines added plus deleted per merged PR |
| First review | Median time from ready-for-review to the first review by someone other than the author |

- A group or everyone: each period is drawn against the same period of the previous
  range, and the team table shows every member next to the team value.
- A person: each period is drawn against their team, and their recent PRs are listed.
- PR chart points for partial weeks/months are projected to a full period using
  the current pace and drawn dashed. Actual merged totals remain in the API.
  KPI cards and team tables show range averages per elapsed week or month.
- An orange dot marks a person's median above 1.5× the team's, or a PR above the
  team's 90th percentile. Periods before `since` show no data rather than zeros.

## Development

```bash
uv sync --group dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run ty check
```
