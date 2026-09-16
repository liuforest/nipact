# NIPACT

NIPACT is a Python package and CLI tool for orchestrating, executing, and auditing scientific workflows. Currently it is built around Snakemake and uses a SQLite registry for tracking steps and artifacts.

Public documentation lives in a separate repository, `nipact-docs`. See: https://liuforest.github.io/nipact-docs/.

Contents:
- a `nipact` Python package under `src/`
- deterministic packaged demo generators used by the tests

Features:
- Workflow inspection commands (`workflow list`, `workflow steps`, `workflow plan`, `workflow graph`)
- Workflow execution via Snakemake (`workflow run`)
- Runtime artifact provenance auditing in a SQLite registry (`trace`)
- Project-specific GUI viewer for browsing workflow runs, artifacts, and provenance (`gui`)
- Fully runnable colors demo with synthetic data and no external dependencies

Work in Progress:
- fMRI and dFC demos


## Installation

The current source-checkout pre-release is `0.0.1a14`. Install release in a clean environment with:

```bash
python -m pip install nipact==0.0.1a14
nipact --version
```

For development from this repo:

```bash
python -m pip install -e . pytest
```

## Local Setup

Install the package and test runner from the repo root:

```bash
python -m pip install -e . pytest
```

Run the Python tests:

```bash
python -m pytest
```

## Specification sets

The current source checkout includes specification sets as an optional finite overlay on ordinary workflows. A project can continue to define and run only steps and workflows; specification declarations are loaded only when a `specifications` command selects a registered key or an explicit file.

For a project that registers a specification as `analysis-curve`, preview and freeze use:

```bash
nipact specifications preview analysis-curve --context analysis
nipact specifications freeze analysis-curve --context analysis
```

Run either one member or all included members, then read the snapshot results:

```bash
nipact specifications run FULL_SNAPSHOT_DIGEST \
  --member member-000001 --context analysis --cores 1
# or
nipact specifications run FULL_SNAPSHOT_DIGEST \
  --all --context analysis --cores 1

nipact specifications results FULL_SNAPSHOT_DIGEST --context analysis
```

`preview` compiles declaration files without reading or mutating the registry. `freeze` persists the immutable denominator and returns its full digest. A run checks each frozen member against current workflow declarations before attempt insertion, then delegates one ordinary workflow invocation per selected member; exact upstream and final artifacts remain reuse-eligible. `results` returns JSON arrays for members, attempts, exact results, and historical source basis. It distinguishes the selecting run from the earlier producing run when an artifact is reused; a current publication path is only an optional view of that historical record.

`freeze`, `run`, and `results` require registry V19. Migration from an exact V18 runtime is explicit: `nipact registry migrate --context analysis`. V1 supports finite static members, reconciles the selected source scope before each ordinary member execution, executes members sequentially, requires every declared result role for completeness, and treats a rerun as a new attempt. It provides JSON records, not statistical interpretation, campaign scheduling, or campaign management.

## Colors Demo via CLI

See more details in the documentation at https://liuforest.github.io/nipact-docs/

The current release supports three packaged demos:

```bash
nipact init \
  --demo colors \
  --project-dir demos/colors/project \
  --runtime-dir demos/colors/runtime

nipact validate --context colors
```

```bash
nipact init \
  --demo fmri \
  --project-dir demos/fmri/project \
  --runtime-dir demos/fmri/runtime

nipact validate --context fmri
```

```bash
nipact init \
  --demo dfc \
  --project-dir demos/dfc/project \
  --runtime-dir demos/dfc/runtime

nipact validate --context dfc
```

NOTES:
`--project-dir` and `--runtime-dir` must be empty and must not contain each other.

`init` creates a generated demo project plus mutable runtime files. The project contains `nipact.yaml`, `sources.yaml`, manifests, step YAML, and workflow YAML.

The runtime contains demo source files under `data/` and `database/registry.db`. It also writes `nipact.contexts.yaml` in the current workspace so later commands can resolve `--context <demo>` to the generated project root. The context index is workspace-local state; this repository ignores the root file so source-checkout testing does not add tutorial state to version control. `validate` is read-only.

Workflow inspection and execution:

```bash
nipact workflow list \
  --context colors

nipact workflow steps \
  --context colors \
  --workflow base

nipact workflow plan \
  --context colors \
  --workflow base \
  --step color_sector_analysis

nipact workflow graph \
  --context colors \
  --workflow base \
  --step color_sector_analysis

nipact workflow run \
  --context colors \
  --workflow base \
  --step color_sector_analysis \
  --dry-run

nipact workflow run \
  --context colors \
  --workflow base \
  --step color_sector_analysis \
  --cores 1

nipact workflow run \
  --context colors \
  --workflow base \
  --step color_local_transform \
  --address color_007 \
  --cores 1

nipact trace \
  --context colors \
  --workflow base \
  --step color_sector_analysis \
  --output sector_counts \
  --address init

nipact gui \
  --context colors \
  --port 8765
```

`workflow run --address ENTITY_ID` targets one entity of an entity-addressed step:

- The address must be a member of the effective workflow's `execution_population`; cohort-addressed steps reject the option. Omitting `--address` keeps the full-population default.
- The selection requests that result for one entity. An already satisfied exact request remains reuse-eligible and is verified without rebuilding; otherwise NIPACT executes the required fresh closure. Descendant steps are not automatically rerun.
- Plan construction stays population-wide; reused-input preparation, fresh execution, publication, and recording are scoped to the target's reachable closure. Computing a fresh cohort-fit ancestor can therefore execute and publish other entities' upstream jobs, and `planned_jobs` counts compiled fresh jobs in the generated Snakefile, not jobs guaranteed to execute.
- A targeted run becomes the latest run for its step/output scope while retaining the complete `execution_population` binding; the published-output table remains a composite of coordinates from multiple runs, not proof of a complete cohort sweep.
- One mutating invocation is supported per runtime root. A second mutating invocation fails fast on the runtime-root lock; concurrent dry runs that share one deterministic dry-run workspace are unsupported.

Successful workflow outputs are stored under the canonical `runtime/outputs/v1/` layout. Their executable-workspace staging files are temporary and are normally removed after the registry transaction commits, so a recorded `staging_path` is historical and may no longer exist. Real-run summaries report `published_outputs` and `published_bytes`; accepted artifact identity and reuse come from the canonical path and registry facts, not continued staging-file presence.

`trace` and `gui` read `runtime/database/registry.db`. The GUI binds to `127.0.0.1` and serves a local browser view for current workflows, manifests, artifacts, workflow topology, and focused artifact lineage.

The gui is for viewing only, not to execute workflows, registry rows, etc.
