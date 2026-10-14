# orgpulse

Pull request metrics for the people and groups you care about in a GitHub
organization: how many PRs each person ships, how big they are, how many commits
they carry, how long they take to merge and to get a first review, and how all
of that changed after the team adopted AI tooling.

`orgpulse sync` pulls pull requests into a local SQLite file. `orgpulse serve`
runs a local web page where you pick a group or individual people, switch
between weekly and monthly trends, and compare against the previous range.

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

[groups]
backend = ["alice", "bob"]
frontend = ["carol", "dave"]

# Optional: compare each person's window before adoption with the latest window.
[ai]
adopted_on = 2025-06-01
window_days = 90

[ai.people]                 # per-person adoption dates override `adopted_on`
carol = 2025-09-01
```

## Usage

```bash
uv run orgpulse sync             # first run fetches history since `since`; later runs fetch only changes
uv run orgpulse serve --open     # http://127.0.0.1:8765
```

Both commands take `--config <path>` (or `ORGPULSE_CONFIG`). Bot-authored PRs
and bot reviews are ignored. Moving `since` earlier backfills on the next sync.

## Metrics

All metrics are per period (week or month), never cumulative. Distributions are
shown as median or p90.

| Metric | Definition |
|---|---|
| Merged / opened PRs per person | PRs merged (by merge date) or opened (by creation date), divided by people active in the period |
| PR size | Added + deleted lines of merged PRs |
| Commits per PR | Commits on merged PRs |
| Open → merge | PR creation to merge |
| First commit → merge | First authored commit on the PR to merge |
| Wait for first review | Ready-for-review to the first review by someone other than the author |
| Merged without review | Share of merged PRs with no review from someone else |
| AI adoption | Each person's `window_days` before their adoption date vs. the latest `window_days`; people without merged PRs in both windows are excluded |

## Development

```bash
uv sync --group dev
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run ty check
```
