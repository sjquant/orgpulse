# orgpulse

`orgpulse` collects GitHub organization pull request activity, stores stable local snapshots and rollups, and renders analysis, person, and dashboard reports.

## Structure

- `src/orgpulse/cli.py`: root CLI adapter. It may orchestrate apps and libraries.
- `src/orgpulse/apps/`: feature workflows such as analysis, dashboard, person metrics, and reaggregation. Apps should not import other apps.
- `src/orgpulse/libs/`: reusable infrastructure and domain libraries for GitHub, snapshots, metrics, output storage, and reporting. Libraries must not import apps or the CLI.
- `src/orgpulse/common/`: shared configuration, models, errors, and small utilities. Common code must stay independent of apps, libs, and the CLI.
- `src/orgpulse/templates/`: report templates only.

Keep dependencies flowing downward: `cli -> apps -> libs -> common`.
