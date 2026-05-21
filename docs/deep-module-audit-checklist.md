# Deep Module Audit Checklist

This audit applies the deep module lens from _A Philosophy of Software Design_ to
every source, template, test, and test-support module currently present in the
repository.

Central question used for every row:

> Does this module reduce what callers need to know and hide meaningful
> complexity behind a small interface?

## Scope

- Source Python modules audited: 26
- Source template modules audited: 7
- Test and test-support modules audited: 23
- Total audited units: 56
- Domain docs available: `README.md` and `pyproject.toml`
- Domain docs missing: no `CONTEXT.md`, no ADR directory

## Legend

- `Deep`: interface hides meaningful policy or transformation work.
- `Mostly deep`: useful interface, but some caller knowledge or internal size risk remains.
- `Shallow/support`: intentionally small adapter, package marker, template shell, or helper.
- `Candidate`: refactoring opportunity with meaningful locality or leverage upside.
- `Done`: already improved by the local snapshot source refactor.

## High-Priority Deepening Candidates

1. [src/orgpulse/dashboard.py](../src/orgpulse/dashboard.py) imports private CLI helpers from
   [src/orgpulse/cli.py](../src/orgpulse/cli.py) for dashboard refresh. The dashboard refresh
   interface requires knowledge of the run pipeline's internal call sequence.
   - Checklist pressure: location of knowledge, interface depth, blast radius.
   - Deepening direction: move shared run execution into an application-level run pipeline
     module that both CLI and dashboard refresh can call through one public interface.

2. [src/orgpulse/cli.py](../src/orgpulse/cli.py) owns command parsing, orchestration, error
   mapping, progress rendering, run output assembly, metric output assembly, and JSON response
   shape.
   - Checklist pressure: together/apart, errors and exceptions, interface depth.
   - Deepening direction: keep Typer command functions thin and move user-intent workflows
     into explicit application modules such as run, reaggregate, analyze, person, and dashboard
     command handlers.

3. [src/orgpulse/reporting/dashboard_html.py](../src/orgpulse/reporting/dashboard_html.py) prepares
   dashboard presentation using large `dict[str, Any]` payloads, repeated keyed lookups, and many
   hidden data-shape assumptions.
   - Checklist pressure: location of knowledge, naming and explanation, test code.
   - Deepening direction: introduce typed presentation models or a dashboard presentation builder
     so template data shape, trimming, slicing, and author-detail policies live behind a small
     interface.

4. [src/orgpulse/reporting/run_outputs.py](../src/orgpulse/reporting/run_outputs.py) contains three
   deep writers, but repo summary, org summary, and manifest writing repeat history, index, latest,
   contract, and pruning policies.
   - Checklist pressure: knowledge that changes together, duplication, blast radius.
   - Deepening direction: extract an internal period artifact history policy used by the writers,
     while preserving each writer's public `write(...)` interface.

5. [src/orgpulse/ingestion.py](../src/orgpulse/ingestion.py) is large enough that several deep
   modules live in one file: normalized raw snapshots, canonical raw inventory, GitHub collection,
   checkpointing, retry, REST mapping, and GraphQL mapping.
   - Checklist pressure: locality and "where does this knowledge live?"
   - Deepening direction: split by stable module concepts only when the interface stays deep:
     snapshot writer, canonical inventory store, GitHub collection adapter, and GitHub record
     mapper.

6. [src/orgpulse/models.py](../src/orgpulse/models.py) is a useful source of truth, but it mixes
   run configuration, domain records, metric records, dashboard payloads, report payloads, manifest
   payloads, and writer results in one 1,600-line module.
   - Checklist pressure: knowledge that changes for different reasons kept apart.
   - Deepening direction: split only along existing stable contracts, for example config/run
     models, raw snapshot models, metric models, report payload models, and manifest/output
     models.

7. [src/orgpulse/metrics.py](../src/orgpulse/metrics.py) keeps metric policy behind builder
   interfaces, but repository and organization rollup builders duplicate summary, active-author,
   and per-author calculation policy.
   - Checklist pressure: duplicated policy and next similar change.
   - Deepening direction: share an internal metric rollup helper that owns metric summaries and
     active-author policy without exposing more knobs to callers.

8. Template modules under [src/orgpulse/templates](../src/orgpulse/templates) contain substantial
   JavaScript and CSS behavior. They are presentation modules, but their data contracts are mostly
   implicit.
   - Checklist pressure: interface depth and test surface.
   - Deepening direction: make dashboard/person/report presentation data shape explicit in typed
     Python payloads before it reaches the templates.

## Source Module Checklist

- [x] [src/orgpulse/__init__.py](../src/orgpulse/__init__.py)
  - Status: shallow/support.
  - Interface: package version export.
  - Judgment: appropriate tiny module; no deepening needed.

- [x] [src/orgpulse/__main__.py](../src/orgpulse/__main__.py)
  - Status: shallow/support.
  - Interface: `python -m orgpulse`.
  - Judgment: appropriate CLI entry adapter; no deepening needed.

- [x] [src/orgpulse/analysis.py](../src/orgpulse/analysis.py)
  - Status: mostly deep, improved by `LocalSnapshotSource`.
  - Interface: analysis config, service, result, and config builder.
  - Judgment: callers now avoid local manifest/source policy; remaining concern is that report
    payload construction is coupled to analysis execution.

- [x] [src/orgpulse/cli.py](../src/orgpulse/cli.py)
  - Status: candidate.
  - Interface: Typer commands and `build_run_config`.
  - Judgment: command interface is user-intent oriented, but implementation owns too many workflow
    policies and private helpers are imported by dashboard refresh.

- [x] [src/orgpulse/config.py](../src/orgpulse/config.py)
  - Status: deep.
  - Interface: `AppSettings`, `get_settings`.
  - Judgment: hides environment lookup and typed defaults behind a small interface.

- [x] [src/orgpulse/dashboard.py](../src/orgpulse/dashboard.py)
  - Status: candidate.
  - Interface: generate dashboard artifacts and build dashboard payload from local outputs.
  - Judgment: local snapshot source policy is improved, but refresh still knows the run pipeline
    sequence and imports private CLI helpers.

- [x] [src/orgpulse/distribution.py](../src/orgpulse/distribution.py)
  - Status: deep.
  - Interface: validate percentile, trim values, compute threshold.
  - Judgment: small interface hides percentile policy and avoids duplicated caller math.

- [x] [src/orgpulse/errors.py](../src/orgpulse/errors.py)
  - Status: shallow/support.
  - Interface: domain exception names.
  - Judgment: useful error taxonomy; no additional implementation expected.

- [x] [src/orgpulse/files.py](../src/orgpulse/files.py)
  - Status: deep.
  - Interface: atomic CSV, JSON, text writers.
  - Judgment: hides temp-file and replacement policy behind focused file-write helpers.

- [x] [src/orgpulse/github_auth.py](../src/orgpulse/github_auth.py)
  - Status: deep.
  - Interface: resolve token and validate GitHub access.
  - Judgment: authentication ordering, CLI fallback, and access validation are localized.

- [x] [src/orgpulse/ingestion.py](../src/orgpulse/ingestion.py)
  - Status: candidate.
  - Interface: raw snapshot writer, canonical inventory store, GitHub ingestion adapter.
  - Judgment: classes are individually useful, but the file combines several reasons for change:
    storage layout, checkpointing, REST fetch, GraphQL fetch, retry, mapping, and normalization.

- [x] [src/orgpulse/metrics.py](../src/orgpulse/metrics.py)
  - Status: mostly deep, candidate for internal duplication cleanup.
  - Interface: metric collection builders and validation builder.
  - Judgment: callers get strong leverage from builders; repository and organization rollups repeat
    summary policy that should change together.

- [x] [src/orgpulse/models.py](../src/orgpulse/models.py)
  - Status: mostly deep, candidate for contract partitioning.
  - Interface: project-wide Pydantic contracts and enums.
  - Judgment: strong source of truth, but unrelated payload families change for different reasons
    inside one module.

- [x] [src/orgpulse/person.py](../src/orgpulse/person.py)
  - Status: mostly deep, improved by `LocalSnapshotSource`.
  - Interface: person config, service, result, and config builder.
  - Judgment: person extraction hides useful behavior; derived weekly/monthly period catalog policy
    is still complex but contained.

- [x] [src/orgpulse/person_source.py](../src/orgpulse/person_source.py)
  - Status: deep.
  - Interface: load person facts from a raw snapshot.
  - Judgment: hides CSV joining, sorting, optional parsing, and person fact construction behind one
    source loader.

- [x] [src/orgpulse/raw_snapshot_source.py](../src/orgpulse/raw_snapshot_source.py)
  - Status: done/deep.
  - Interface: load analysis, person metrics, and dashboard local snapshot sources.
  - Judgment: local manifest paths, parsing, org validation, freshness, coverage, and raw period
    materialization now live in the source module.

- [x] [src/orgpulse/reporting/__init__.py](../src/orgpulse/reporting/__init__.py)
  - Status: shallow/support.
  - Interface: reporting package marker.
  - Judgment: no deepening needed.

- [x] [src/orgpulse/reporting/analysis_export.py](../src/orgpulse/reporting/analysis_export.py)
  - Status: deep.
  - Interface: render an analysis result in the requested export format.
  - Judgment: callers do not need JSON/CSV/Markdown/HTML branching details.

- [x] [src/orgpulse/reporting/analysis_report.py](../src/orgpulse/reporting/analysis_report.py)
  - Status: mostly deep.
  - Interface: build and render analysis report payloads.
  - Judgment: hides report aggregation and chart payload construction; size and raw-row filtering
    policy make this a future candidate if report behavior grows.

- [x] [src/orgpulse/reporting/contracts.py](../src/orgpulse/reporting/contracts.py)
  - Status: deep.
  - Interface: shared time-anchor labels and period-state payloads.
  - Judgment: clear locality for reporting vocabulary and period state policy.

- [x] [src/orgpulse/reporting/dashboard_html.py](../src/orgpulse/reporting/dashboard_html.py)
  - Status: candidate.
  - Interface: prepare dashboard payload and render HTML.
  - Judgment: caller-facing interface is good, but implementation relies heavily on untyped dict
    shape and repeated string-key policies.

- [x] [src/orgpulse/reporting/person_export.py](../src/orgpulse/reporting/person_export.py)
  - Status: deep.
  - Interface: render a person metrics result in the requested export format.
  - Judgment: hides export branching and row formatting from callers.

- [x] [src/orgpulse/reporting/person_report.py](../src/orgpulse/reporting/person_report.py)
  - Status: mostly deep.
  - Interface: render person HTML report.
  - Judgment: report presentation policy is localized; template data shape is partly implicit.

- [x] [src/orgpulse/reporting/run_outputs.py](../src/orgpulse/reporting/run_outputs.py)
  - Status: candidate.
  - Interface: repo summary writer, org summary writer, run manifest writer.
  - Judgment: public writer interfaces are deep, but shared history/index/latest/contract policies
    are duplicated across implementations.

- [x] [src/orgpulse/types/__init__.py](../src/orgpulse/types/__init__.py)
  - Status: shallow/support.
  - Interface: type package marker.
  - Judgment: no deepening needed.

- [x] [src/orgpulse/types/github.py](../src/orgpulse/types/github.py)
  - Status: deep.
  - Interface: GitHub protocol contracts.
  - Judgment: keeps tests and ingestion decoupled from concrete PyGithub objects without exposing
    implementation state.

## Template Module Checklist

- [x] [src/orgpulse/templates/analysis_report.html.j2](../src/orgpulse/templates/analysis_report.html.j2)
  - Status: mostly deep presentation module.
  - Judgment: contains cohesive analysis report UI, but JavaScript behavior and payload assumptions
    are implicit in the template.

- [x] [src/orgpulse/templates/org_dashboard.html.j2](../src/orgpulse/templates/org_dashboard.html.j2)
  - Status: shallow/support.
  - Judgment: shell template that composes dashboard partials.

- [x] [src/orgpulse/templates/org_dashboard/_app.js.j2](../src/orgpulse/templates/org_dashboard/_app.js.j2)
  - Status: candidate.
  - Judgment: substantial dashboard client behavior lives in one template partial; consider making
    the dashboard client contract explicit before adding more interactions.

- [x] [src/orgpulse/templates/org_dashboard/_body.html.j2](../src/orgpulse/templates/org_dashboard/_body.html.j2)
  - Status: mostly deep presentation module.
  - Judgment: cohesive dashboard markup, but depends on many prepared payload keys.

- [x] [src/orgpulse/templates/org_dashboard/_styles.css.j2](../src/orgpulse/templates/org_dashboard/_styles.css.j2)
  - Status: mostly deep presentation module.
  - Judgment: cohesive styling module; not an APOSD priority unless theme tokens start changing in
    multiple places.

- [x] [src/orgpulse/templates/org_dashboard/_theme_bootstrap.js.j2](../src/orgpulse/templates/org_dashboard/_theme_bootstrap.js.j2)
  - Status: deep.
  - Judgment: small module hides theme initialization ordering from templates.

- [x] [src/orgpulse/templates/person_report.html.j2](../src/orgpulse/templates/person_report.html.j2)
  - Status: mostly deep presentation module.
  - Judgment: cohesive person report UI; JavaScript chart behavior and payload shape are implicit.

## Test Module Checklist

- [x] [tests/__init__.py](../tests/__init__.py)
  - Status: shallow/support.
  - Judgment: package marker.

- [x] [tests/cli/__init__.py](../tests/cli/__init__.py)
  - Status: shallow/support.
  - Judgment: package marker.

- [x] [tests/cli/test_analyze.py](../tests/cli/test_analyze.py)
  - Status: deep black-box coverage.
  - Judgment: validates CLI behavior through public command output; wildcard helper import is a
    test ergonomics tradeoff.

- [x] [tests/cli/test_dashboard.py](../tests/cli/test_dashboard.py)
  - Status: deep black-box coverage.
  - Judgment: covers dashboard CLI behavior and user-facing failures.

- [x] [tests/cli/test_person.py](../tests/cli/test_person.py)
  - Status: deep black-box coverage.
  - Judgment: covers person CLI behavior, formats, stale input, and derived grains.

- [x] [tests/cli/test_run_command.py](../tests/cli/test_run_command.py)
  - Status: deep black-box coverage.
  - Judgment: covers the run pipeline through CLI behavior; long module mirrors the broad run
    workflow.

- [x] [tests/cli/test_run_config.py](../tests/cli/test_run_config.py)
  - Status: deep config coverage.
  - Judgment: validates public config parsing and cross-field constraints.

- [x] [tests/conftest.py](../tests/conftest.py)
  - Status: mostly deep test support.
  - Judgment: metric harness hides snapshot-writing setup; appropriate support seam.

- [x] [tests/helpers/__init__.py](../tests/helpers/__init__.py)
  - Status: shallow/support.
  - Judgment: package marker.

- [x] [tests/helpers/cli.py](../tests/helpers/cli.py)
  - Status: mostly deep test support.
  - Judgment: hides production CLI monkeypatch setup; wildcard-export pattern increases implicit
    caller knowledge.

- [x] [tests/helpers/dashboard_source.py](../tests/helpers/dashboard_source.py)
  - Status: deep test support.
  - Judgment: centralizes dashboard fixture source layout and expected period state.

- [x] [tests/helpers/mocks.py](../tests/helpers/mocks.py)
  - Status: mostly deep test support.
  - Judgment: fake CLI collaborators are cohesive; unexpected writers are clear guard adapters.

- [x] [tests/helpers/output.py](../tests/helpers/output.py)
  - Status: mostly deep test support.
  - Judgment: centralizes output/report helpers; wildcard export hides exact dependencies.

- [x] [tests/output/__init__.py](../tests/output/__init__.py)
  - Status: shallow/support.
  - Judgment: package marker.

- [x] [tests/output/test_local_source.py](../tests/output/test_local_source.py)
  - Status: deep behavioral coverage.
  - Judgment: validates dashboard local-source behavior through public source/reporting interfaces.

- [x] [tests/output/test_manifest.py](../tests/output/test_manifest.py)
  - Status: deep writer coverage.
  - Judgment: covers manifest carry-forward, locking, contract equivalence, and truncation checks.

- [x] [tests/output/test_manual_dashboard.py](../tests/output/test_manual_dashboard.py)
  - Status: deep dashboard payload coverage, with lint debt.
  - Judgment: strong black-box coverage of dashboard output; project-wide ruff reports duplicate
    expected dict keys in this module.

- [x] [tests/output/test_org_summary.py](../tests/output/test_org_summary.py)
  - Status: deep writer coverage.
  - Judgment: validates deterministic org summary outputs and history behavior.

- [x] [tests/output/test_repo_summary.py](../tests/output/test_repo_summary.py)
  - Status: deep writer coverage.
  - Judgment: validates deterministic repo summary outputs, stale pruning, and locked period
    preservation.

- [x] [tests/output/test_reporting.py](../tests/output/test_reporting.py)
  - Status: mostly deep reporting coverage.
  - Judgment: covers public reporting entry points and module execution guidance.

- [x] [tests/test_github_auth.py](../tests/test_github_auth.py)
  - Status: deep auth coverage.
  - Judgment: validates token precedence, CLI fallback, access validation, and auth error modes.

- [x] [tests/test_ingestion.py](../tests/test_ingestion.py)
  - Status: deep ingestion coverage.
  - Judgment: broad behavioral coverage over repository inventory, REST/GraphQL fetch, retry,
    checkpointing, and mapping.

- [x] [tests/test_metrics.py](../tests/test_metrics.py)
  - Status: deep metrics coverage.
  - Judgment: validates review timing, aggregation, and metric validation behavior from public
    builder interfaces.

## Checklist Summary

- [x] Can the purpose of each listed module be stated in one sentence?
- [x] Is every source Python module listed?
- [x] Is every source template module listed?
- [x] Is every test and test-support module listed?
- [x] Are high-leverage deepening candidates identified separately from support modules?
- [x] Are modules already deep enough marked without forcing cosmetic refactors?
- [x] Are repeated policies and leaked call sequences called out?
- [x] Are test modules judged by public behavior coverage rather than private helper access alone?
- [x] Are known verification debts recorded?

## Verification Notes

Existing verification from the branch still applies:

- `uv run ty check`
- `uv run pytest`
- scoped ruff checks over changed files from the local snapshot source refactor

Known pre-existing debt outside the refactor remains:

- Project-wide `ruff format --check src tests` reports formatting drift in unrelated files.
- Project-wide `ruff check src tests` reports duplicate dict keys in
  [tests/output/test_manual_dashboard.py](../tests/output/test_manual_dashboard.py).
