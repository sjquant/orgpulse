# orgpulse

See where pull requests slow down and how your GitHub team's work changes over time.

- Compare PR flow, change volume, and review participation across people and groups.
- Explore up to three years of trends, with averages, medians, and outlier filtering.
- Save HTML reports for people or groups that stay interactive offline.

## Try it with your team

You need Python 3.11+, [uv](https://docs.astral.sh/uv/), and GitHub credentials
that can read your organization (`GH_TOKEN` or `gh auth login`).

```bash
uv sync
```

Create `orgpulse.toml` with your organization and team:

```toml
org = "acme"
since = 2025-01-01
timezone = "Asia/Seoul"

[groups]
backend = ["alice", "bob"]
```

Collect your PRs and open the dashboard:

```bash
uv run orgpulse sync
uv run orgpulse serve
```

Open **http://127.0.0.1:8765**, choose a person or group, and select a metric to
explore the trend. Use **Sync now** to refresh or download a report to share.

Author metrics cover merged PRs; reviewer metrics also include unmerged PRs. PR activity is context for a conversation,
not a performance score.

[User guide: configuration, metrics, and exports](docs/guide.md) ·
[Development](docs/guide.md#development)
