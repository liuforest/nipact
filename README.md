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

The current source-checkout pre-release is `0.0.1a18`. Install release in a clean environment with:

```bash
python -m pip install nipact==0.0.1a18
nipact --version
```

For development from this repo, install the package and test runner, then run the tests:

```bash
python -m pip install -e . pytest
python -m pytest
```

## Project layout

A project directory contains `nipact.yaml`, `sources.yaml`, and one `.yaml` file per declaration directly in `steps/`, `workflows/` and `manifests/`. File stems name steps, workflows and manifests; nested directories can hold parked files. `nipact.yaml` names the context and the runtime directory and registers specification sets:

```yaml
context: analysis
paths:
  runtime: ../runtime
specifications:
  analysis-curve: specifications/analysis-curve.yaml
```

Commands find the project by searching the current directory and its parents for the nearest `nipact.contexts.yaml` (a workspace index, such as the one `init --demo` writes) or `nipact.yaml`, so they run from anywhere inside a project without selection flags. `--project-dir` selects a project directly, and `--context` is needed only when a workspace index lists several projects.

To set up runtime storage for an existing project, run `nipact init` without `--demo`, then validate:

```bash
cd /path/to/project
nipact init
nipact validate
```

`init` creates the runtime directories and registry under the runtime path named in `nipact.yaml` and catalogs the valid manifests. It refuses a runtime that already has a registry.

## Declarations and validation

A step file such as `steps/segment_image.yaml` contains:

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

`step_name` and `workflow_name` default to the file stem, and outputs inherit the step's `address_scope`. A variant such as `workflows/small-cohort.yaml` can contain only:

```yaml
base_workflow: base
execution_population: small
```

`nipact validate` lists each step, workflow and manifest as `valid`, `invalid` or `unavailable`, checks the registry, and exits nonzero if any check fails. An unusable declaration does not stop commands from using unrelated valid workflows. By default, validation checks that accepted outputs exist with the recorded kind and, for files, size; `nipact validate --outputs` also re-hashes their contents.

## Specification sets

Specification sets are an optional finite overlay on ordinary workflows; their declarations load only when a `specifications` command selects one. For a project that registers `analysis-curve`:

```bash
nipact specifications preview analysis-curve --context analysis
nipact specifications freeze analysis-curve --context analysis

nipact specifications run FULL_SNAPSHOT_DIGEST \
  --member member-000001 --context analysis --cores 1
# or
nipact specifications run FULL_SNAPSHOT_DIGEST \
  --all --context analysis --cores 1

nipact specifications results FULL_SNAPSHOT_DIGEST --context analysis
```

`preview` compiles the declarations without touching the registry, `freeze` stores the member set and prints its digest, `run` executes each selected member as an ordinary workflow invocation, reusing existing artifacts, and `results` prints JSON records of members, attempts and results. Members run sequentially, and a rerun is a new attempt.

## Directory outputs

A step output can be a regular file or one complete directory tree, and one step can declare both. For a step with `address_scope: entity`:

```yaml
outputs:
  volumes:
    extension: .json
  maps:
    kind: directory
```

`kind` is `file` (the default) or `directory`; a directory output must not declare `extension`. The callable interface is unchanged: for a directory output, `outputs[name]` is a `Path` that does not exist yet, and the callable creates the tree before returning. A directory input holds one `Path` per tree, read by relative path such as `inputs["maps"][0] / "label-high.npy"`. Callables must not modify their inputs, and a callable that requires particular members checks them itself.

One tree is one artifact. Every member is content, including hidden files and empty directories; timestamps, permissions, and ownership are not. Symbolic links, hard-linked files and special files are rejected. A reused tree is verified and copied for fresh consumers. Directories cannot be `sources.yaml` sources, there is no partial-tree reuse, and staging and `outputs/` must be on the same filesystem.

`nipact init --demo directories` creates a two-entity demo. Its `low-label` variant changes only the summary step, so it reuses the trees from `base`:

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

## Demos

Four packaged demos are available: `colors`, `fmri`, `dfc` and `directories`. `init --demo` needs empty `--project-dir` and `--runtime-dir` directories that do not contain each other:

```bash
nipact init \
  --demo colors \
  --project-dir demos/colors/project \
  --runtime-dir demos/colors/runtime

nipact validate --context colors
```

`init --demo` writes the generated project, a runtime with demo source data and the registry, and a `nipact.contexts.yaml` index in the current directory so later commands find the project from there. Pass `--context` when the index lists several demos. This repository ignores the root index file.

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

`workflow run --address ENTITY_ID` runs one entity of an entity-addressed step; the entity must belong to the workflow's `execution_population`, and descendant steps are not rerun. Outputs are stored under `runtime/outputs/v1/`. `trace` and `gui` read `runtime/database/registry.db`; the GUI binds to `127.0.0.1` and is view-only.
