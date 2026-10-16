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
- Step outputs that are regular files or complete directory trees
- Fully runnable colors demo with synthetic data and no external dependencies

Work in Progress:
- fMRI and dFC demos


## Installation

The current source-checkout pre-release is `0.0.1a17`. Install release in a clean environment with:

```bash
python -m pip install nipact==0.0.1a17
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

## Project layout

The current source checkout reads `sources.yaml` and direct `.yaml` files in `steps/`, `workflows/` and `manifests/`. File stems name steps, workflows and manifests; nested directories can hold parked files. Keep the context and runtime explicit in `nipact.yaml`:

```yaml
context: analysis
paths:
  runtime: ../runtime
specifications:
  analysis-curve: specifications/analysis-curve.yaml
```

Specification sets and libraries remain explicitly registered with `specifications` and `specification_libraries`. The current source checkout rejects the old `sources`, `steps`, `workflows` and `manifests` configuration keys; this layout change is unreleased.

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

`freeze`, `run`, and `results` require the current registry schema, V20; see [Registry upgrade to V20](#registry-upgrade-to-v20). V1 supports finite static members, reconciles the selected source scope before each ordinary member execution, executes members sequentially, requires every declared result role for completeness, and treats a rerun as a new attempt. It provides JSON records, not statistical interpretation, campaign scheduling, or campaign management.

## Shorter declarations

**Available from `0.0.1a17`:** repeated names, output scopes, and empty variant overrides can be omitted. For example, `steps/segment_image.yaml` can contain:

```yaml
step_contract_version: "1"
pattern_kind: pattern_a
execution_role: transform
address_scope: entity
callable: nipact.examples.directory_outputs_demo.runtime:segment_image_directory
params: {low_threshold: 4.0, high_threshold: 10.0}
inputs:
  raw_image: {artifact: image_source.raw_image, dependency_role: source_input}
outputs:
  volumes: {extension: .json}
  maps: {kind: directory}
```

An omitted `step_name` comes from the step file's stem (`segment_image` here). An omitted `workflow_name` comes from the workflow file's stem. File and directory outputs inherit the step's explicit `address_scope` when their own scope is omitted. Written names and output scopes must still match those authorities.

A variant in `workflows/small-cohort.yaml` can contain only:

```yaml
base_workflow: base
execution_population: small
```

The base workflow and population must be declared as usual. Omitting `step_overrides` supplies an empty local mapping, preserving inherited overrides; explicit `{}` has the same meaning. These defaults apply only to absent fields: explicit `null` remains invalid for `step_name`, `workflow_name`, output `address_scope`, and variant `step_overrides`. Existing `inputs: null` and `params: null` still mean empty mappings. Generated demos keep the explicit declarations.

Malformed declarations remain visible in `nipact validate`, which lists each step, workflow and manifest as `valid`, `invalid` or `unavailable` and exits nonzero if any declaration or registry check fails. Unavailable declarations name the original invalid file. Commands that load declarations warn about unusable files and can select unrelated valid workflows; selecting an unusable workflow fails with its root cause. A runtime without a registry reports `registry  not initialized`.

By default, validation checks registry metadata, retained output contracts, canonical paths, output existence and kind, and regular-file size without reading output contents or walking directory payloads. `nipact validate --outputs` also re-hashes every accepted workflow output in the context, including older outputs with no current published membership and all directory contents. Both modes check that same accepted-output scope; the count on the registry line is the number of current published outputs. A same-size file alteration or a change inside a directory can pass default validation and fail with `--outputs`.

## Directory outputs

A step output can be a regular file or one complete directory tree, and one step can declare both. Directory outputs are new after `0.0.1a15` and require registry V20.

```yaml
outputs:
  volumes:
    extension: .json
    address_scope: entity
  maps:
    kind: directory
    address_scope: entity
```

**Declaration.** `kind` is `file` (the default) or `directory`. A directory output must not declare `extension`. In the current source checkout, an omitted output `address_scope` inherits the step's scope (see [Shorter declarations](#shorter-declarations)); earlier releases require it explicitly. Omitting `kind` and writing `kind: file` are the same declaration, so existing file steps keep their identities, paths, and reuse. Each output's `address_scope`, for files and directories, must equal its step's `address_scope`; NIPACT marks a mismatch invalid in any step file, including a step that no workflow uses.

**Callable contract.** The callable interface is unchanged. For a directory output, `outputs[name]` is a `Path` that does not exist yet; NIPACT creates only its parent, and the callable creates and finishes the tree before returning. For a directory input, `inputs[name]` holds one `Path` per tree, and the callable reads members by relative path, such as `inputs["maps"][0] / "label-high.npy"`. Callables must not modify their inputs.

**Provider validation.** NIPACT accepts any tree of directories and single-link regular files and cannot judge whether a tree is scientifically complete. A callable that requires particular members checks them and raises before returning. If a declared output is missing or has the wrong kind when the callable returns, the job fails and none of its sibling outputs is accepted.

**Content identity.** Every member is content, including hidden files and empty directories; timestamps, permissions, and ownership are not. Symbolic links, hard-linked files, special files, and member names that are not valid UTF-8 are rejected. The digest scheme `nipact-directory-tree-sha256-v1` is fixed by the kind. `file_size`, shown as payload bytes, is the sum of member file sizes. A directory's canonical path under `outputs/v1/` has no suffix.

**Reuse and cost.** A consumer depends on the whole tree. A reused tree consumed by fresh work is verified and copied in one pass, with one copy per tree per workflow invocation. After the whole invocation succeeds, NIPACT removes its copies; cleanup errors produce warnings and may leave copies behind. A failed invocation keeps its copies. Each specification member has its own invocation and cleanup. A rerun that only reuses a selected result verifies its complete sibling bundle, including every tree. `nipact validate --outputs` hashes every accepted output in the context, so it reads all stored trees; default validation checks their roots without walking their contents.

**Limits.** Directories cannot be `sources.yaml` sources. There are no per-member dependencies and no partial-tree reuse, and reused directories are not delivered in place. Staging and `outputs/` must be on the same filesystem, and extremely deep trees (about 1,000 nested levels) are unsupported.

**Inspection.** `trace --json` and the GUI artifact detail show the kind, digest scheme, and payload bytes. `trace --file-path` accepts a tree's root path but not a member path.

**Demo.** `nipact init --demo directories` creates a two-entity project whose `segment_image` step writes a `volumes` file and a `maps` directory and checks the directory's required members. The `base` workflow summarizes the maps; the `low-label` variant changes only the summary, so it reuses both trees through verified copies:

```bash
nipact workflow run \
  --context directories \
  --workflow base \
  --step map_summary \
  --cores 1

nipact workflow run \
  --context directories \
  --workflow low-label \
  --step map_summary \
  --cores 1
```

**Snakemake.** Directory outputs rely on Snakemake `directory()` outputs. The `snakemake>=9,<10` requirement predates this feature. In September 2026 the directory lifecycle tests were verified on Snakemake 9.14.5, 9.14.6, and 9.27.0. Widening the requirement beyond 9.x requires repeating the container/payload scheduler proof.

**Design notes.** These record the adopted decisions:

- Native trees were chosen over archives or dozens of per-map outputs, and NIPACT neither packs nor extracts.
- One tree is one artifact. For results that consumers use independently, declare separate file outputs.
- Output contract V2 is used only when a sibling is a directory, so file-only requests keep byte-identical projections, digests, and paths. Changing a port's kind is a new request, and historical artifacts are validated against their original contracts.
- Snakemake schedules a disposable `directory()` container, and the callable writes `payload/` inside it, so scheduler bookkeeping never enters the artifact.
- Registry V20 adds artifact `kind` and `digest_scheme` and allows a NULL extension for directories.
- Measured I/O: on two sessions of real segmentation maps (1.11 GB of directory payload; local ext4 with 98 GB RAM; Snakemake 9.14.6), verifying while copying cut logical reused-input preparation reads from 3.32 GB to 1.11 GB. Copy bytes retained after success fell from 1.11 GB to 0. Warm preparation took 0.84 s instead of 1.36 s. With the payload evicted from the page cache it took 2.12 s instead of 1.86 s, 14% slower, and this was accepted. These are delivery timings on one host, not processing times.
- Deferred for separate decisions: declared in-place delivery of reused inputs, narrower verification on zero-fresh reruns, sharing verification across specification members, and copy-on-write copies.

## Registry upgrade to V20

Fresh `init` creates a V20 registry. Commands that open a V19 registry stop and name the migrate command; nothing migrates automatically.

For a V19 runtime, stop other mutating NIPACT commands for that runtime, then run:

```bash
nipact registry migrate --context CONTEXT --project-dir PROJECT_DIR
```

The command writes and validates `database/registry.v19-before-v20.db` as a backup, converts the registry in one transaction, preserves every existing registry value and output file, and runs no workflow. It refuses to replace an existing backup path.

For a V18 runtime, first run the same command with `nipact==0.0.1a15`, then with the current version.

`0.0.1a15` and earlier cannot open a V20 registry. To go back, restore the backup over `database/registry.db` while no NIPACT command runs, then use `0.0.1a15`. Work recorded after the migration is not in the backup.

## Colors Demo via CLI

See more details in the documentation at https://liuforest.github.io/nipact-docs/

Four packaged demos are available; `directories` is new after `0.0.1a15`:

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

```bash
nipact init \
  --demo directories \
  --project-dir demos/directories/project \
  --runtime-dir demos/directories/runtime

nipact validate --context directories
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

Successful workflow outputs are stored under the canonical `runtime/outputs/v1/` layout. Their executable-workspace staging outputs are temporary and are normally removed after the registry transaction commits, so a recorded `staging_path` is historical and may no longer exist. Real-run summaries report `published_outputs` and `published_bytes`, to which a directory output contributes its member file bytes; accepted artifact identity and reuse come from the canonical path and registry facts, not continued staging-file presence.

`trace` and `gui` read `runtime/database/registry.db`. The GUI binds to `127.0.0.1` and serves a local browser view for current workflows, manifests, artifacts, workflow topology, and focused artifact lineage.

The gui is for viewing only, not to execute workflows, registry rows, etc.
