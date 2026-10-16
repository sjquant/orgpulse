# orgpulse

`orgpulse` syncs merged GitHub organization pull requests into a local SQLite database and serves a local web report of per-person and per-group PR metrics.

## Structure

- `src/orgpulse/cli.py`: root CLI adapter (`sync`, `serve`). It may orchestrate apps and libraries.
- `src/orgpulse/apps/`: feature workflows: `sync.py` (GitHub → store) and `web/` (HTTP server plus static UI in `web/static/`). Apps should not import other apps.
- `src/orgpulse/libs/`: reusable libraries: `github.py` (GraphQL client), `store.py` (SQLite), `metrics.py` (report computation). Libraries must not import apps or the CLI.
- `src/orgpulse/common/`: shared configuration, models, and errors. Common code must stay independent of apps, libs, and the CLI.

Keep dependencies flowing downward: `cli -> apps -> libs -> common`.

Do not pin report UI/CSS details in tests; validate rendering manually.
