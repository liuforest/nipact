import json
import os
import sqlite3
import subprocess
from pathlib import Path

import pytest
import yaml

import nipact.execution as execution_module
import nipact.registry as registry_module
from nipact.cli import main
from nipact.errors import ValidationError
from nipact.execution import RunOutcome, build_run_plan, execute_run_plan
from nipact.hashing import sha256_file_digest
from nipact.projection import (
    CollectionBinding,
    RequestBundleProjectionV1,
    SourceCoordinate,
    UnresolvedRequestBundleProjection,
    UpstreamRequestedOutputBinding,
    canonical_projection_json,
)
from nipact.trace import build_trace_graph_for_workflow_coordinate


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_cache_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    entities: tuple[str, ...] = ("sub_001",),
) -> tuple[Path, Path, Path]:
    module_dir = tmp_path / "importable"
    module_dir.mkdir()
    log_path = tmp_path / "execution-log.txt"
    (module_dir / "cache_runtime.py").write_text(
        """
import json
from pathlib import Path


def _write_json(path, payload):
    path.write_text(json.dumps(payload, sort_keys=True) + "\\n", encoding="utf-8")


def step_a_file(*, inputs, outputs, params, address):
    seed = inputs["seed"][0].read_text(encoding="utf-8").strip()
    _write_json(outputs["a_out"], {"address": address, "value": seed})


def step_a_alt_file(*, inputs, outputs, params, address):
    seed = inputs["seed"][0].read_text(encoding="utf-8").strip()
    _write_json(outputs["a_out"], {"address": address, "value": seed + "-alt"})


def step_b_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["a_input"][0].read_text(encoding="utf-8"))
    log_path = Path(params["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(f"B {address}\\n")
    version = params.get("version")
    suffix = "-b" if version is None else f"-b-{version}"
    _write_json(
        outputs["b_out"],
        {"address": address, "value": payload["value"] + suffix},
    )


def step_b_alt_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["a_input"][0].read_text(encoding="utf-8"))
    log_path = Path(params["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(f"B_ALT {address}\\n")
    _write_json(
        outputs["b_out"],
        {"address": address, "value": payload["value"] + "-b-alt"},
    )


def step_c_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["b_input"][0].read_text(encoding="utf-8"))
    _write_json(
        outputs["c_out"],
        {"address": address, "value": payload["value"] + "-c"},
    )


def step_d_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["c_input"][0].read_text(encoding="utf-8"))
    _write_json(
        outputs["d_out"],
        {"address": address, "value": payload["value"] + "-d"},
    )


def step_multi_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["b_input"][0].read_text(encoding="utf-8"))
    _write_json(
        outputs["left_out"],
        {"address": address, "side": "left", "value": payload["value"] + "-left"},
    )
    _write_json(
        outputs["right_out"],
        {"address": address, "side": "right", "value": payload["value"] + "-right"},
    )


def step_use_multi_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["left_input"][0].read_text(encoding="utf-8"))
    _write_json(
        outputs["multi_used"],
        {"address": address, "value": payload["value"] + "-used"},
    )


def step_fit_file(*, inputs, outputs, params, address):
    payloads = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in inputs["b_inputs"]
    ]
    log_path = Path(params["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(f"FIT {address} {len(payloads)}\\n")
    _write_json(
        outputs["fit_out"],
        {
            "address": address,
            "count": len(payloads),
            "values": sorted(payload["value"] for payload in payloads),
        },
    )


def step_apply_file(*, inputs, outputs, params, address):
    payload = json.loads(inputs["b_input"][0].read_text(encoding="utf-8"))
    fit = json.loads(inputs["fit_input"][0].read_text(encoding="utf-8"))
    log_path = Path(params["log_path"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(f"APPLY {address}\\n")
    _write_json(
        outputs["apply_out"],
        {
            "address": address,
            "fit_count": fit["count"],
            "value": payload["value"] + "-apply",
        },
    )
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    monkeypatch.setenv(
        "PYTHONPATH",
        str(module_dir)
        if not os.environ.get("PYTHONPATH")
        else f"{module_dir}{os.pathsep}{os.environ['PYTHONPATH']}",
    )

    project_dir = tmp_path / "project"
    runtime_dir = tmp_path / "runtime"
    (project_dir / "manifests").mkdir(parents=True)
    (project_dir / "steps").mkdir()
    (project_dir / "workflows").mkdir()
    (runtime_dir / "data/source").mkdir(parents=True)
    (runtime_dir / "database").mkdir()
    (runtime_dir / "outputs").mkdir()

    _write_yaml(
        project_dir / "nipact.yaml",
        {
            "context": "cache",
            "paths": {
                "runtime": "../runtime",
            },
            "sources": {
                "index": "sources.yaml",
            },
            "manifests": {
                "subjects": "manifests/subjects.yaml",
            },
            "steps": {
                "directory": "steps",
            },
            "workflows": {
                "main": "workflows/main.yaml",
                "derivative": "workflows/derivative.yaml",
            },
        },
    )
    _write_yaml(
        project_dir / "sources.yaml",
        {
            "entities": {
                entity: {"seed": f"data/source/{entity}.txt"}
                for entity in entities
            },
        },
    )
    _write_yaml(
        project_dir / "manifests/subjects.yaml",
        {
            "description": "Phase 23A cache characterization manifest",
            "entities": list(entities),
        },
    )
    _write_yaml(
        project_dir / "steps/a_source.yaml",
        {
            "step_name": "a_source",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": "cache_runtime:step_a_file",
            "source_inputs": ["seed"],
            "manifest_binding": {
                "role": "source_population",
                "manifest": "subjects",
            },
            "outputs": {
                "a_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/b_transform.yaml",
        {
            "step_name": "b_transform",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_b_file",
            "inputs": {
                "a_input": {
                    "artifact": "a_source.a_out",
                    "dependency_role": "source_input",
                },
            },
            "params": {
                "log_path": str(log_path),
            },
            "outputs": {
                "b_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/c_transform.yaml",
        {
            "step_name": "c_transform",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_c_file",
            "inputs": {
                "b_input": {
                    "artifact": "b_transform.b_out",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "c_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/d_transform.yaml",
        {
            "step_name": "d_transform",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_d_file",
            "inputs": {
                "c_input": {
                    "artifact": "c_transform.c_out",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "d_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/fit_transform.yaml",
        {
            "step_name": "fit_transform",
            "pattern_kind": "pattern_a",
            "execution_role": "b_fit",
            "address_scope": "cohort",
            "callable": "cache_runtime:step_fit_file",
            "manifest_binding": {
                "role": "fit_cohort",
                "manifest": "subjects",
            },
            "inputs": {
                "b_inputs": {
                    "artifact": "b_transform.b_out",
                    "dependency_role": "fit_input",
                },
            },
            "params": {
                "log_path": str(log_path),
            },
            "outputs": {
                "fit_out": {
                    "extension": ".json",
                    "address_scope": "cohort",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/apply_transform.yaml",
        {
            "step_name": "apply_transform",
            "pattern_kind": "pattern_b",
            "execution_role": "b_apply",
            "address_scope": "entity",
            "callable": "cache_runtime:step_apply_file",
            "inputs": {
                "b_input": {
                    "artifact": "b_transform.b_out",
                    "dependency_role": "apply_input",
                },
                "fit_input": {
                    "artifact": "fit_transform.fit_out",
                    "dependency_role": "collective_fit",
                },
            },
            "params": {
                "log_path": str(log_path),
            },
            "outputs": {
                "apply_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/multi_transform.yaml",
        {
            "step_name": "multi_transform",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_multi_file",
            "inputs": {
                "b_input": {
                    "artifact": "b_transform.b_out",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "left_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
                "right_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/use_multi.yaml",
        {
            "step_name": "use_multi",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_use_multi_file",
            "inputs": {
                "left_input": {
                    "artifact": "multi_transform.left_out",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "multi_used": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "workflows/main.yaml",
        {
            "workflow_name": "main",
            "steps": [
                {
                    "step_name": "a_source",
                    "output_name": "a_out",
                },
                {
                    "step_name": "b_transform",
                    "output_name": "b_out",
                },
                {
                    "step_name": "c_transform",
                    "output_name": "c_out",
                },
                {
                    "step_name": "d_transform",
                    "output_name": "d_out",
                },
                {
                    "step_name": "fit_transform",
                    "output_name": "fit_out",
                },
                {
                    "step_name": "multi_transform",
                    "output_name": "left_out",
                },
                {
                    "step_name": "use_multi",
                    "output_name": "multi_used",
                },
            ],
        },
    )
    _write_yaml(
        project_dir / "workflows/derivative.yaml",
        {
            "workflow_name": "derivative",
            "steps": [
                {
                    "step_name": "a_source",
                    "output_name": "a_out",
                },
                {
                    "step_name": "b_transform",
                    "output_name": "b_out",
                },
                {
                    "step_name": "c_transform",
                    "output_name": "c_out",
                },
            ],
        },
    )
    seed_values = {
        "sub_001": "alpha",
        "sub_002": "beta",
        "sub_003": "gamma",
    }
    for entity in entities:
        (runtime_dir / f"data/source/{entity}.txt").write_text(
            f"{seed_values.get(entity, entity)}\n",
            encoding="utf-8",
        )

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        registry_module._create_schema(conn)
        conn.execute(
            "INSERT INTO contexts (context, runtime_path) VALUES (?, ?)",
            ("cache", str(runtime_dir)),
        )
    return project_dir, runtime_dir, log_path


def _add_cache_entity(
    project_dir: Path,
    runtime_dir: Path,
    *,
    address: str,
    seed: str,
) -> None:
    sources_path = project_dir / "sources.yaml"
    sources = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
    sources["entities"][address] = {"seed": f"data/source/{address}.txt"}
    _write_yaml(sources_path, sources)

    manifest_path = project_dir / "manifests/subjects.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["entities"].append(address)
    _write_yaml(manifest_path, manifest)

    (runtime_dir / f"data/source/{address}.txt").write_text(
        f"{seed}\n",
        encoding="utf-8",
    )


def _write_workflow_variant(
    project_dir: Path,
    *,
    workflow_name: str,
    base_workflow: str,
    step_overrides: dict[str, object] | None = None,
) -> None:
    config_path = project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["workflows"][workflow_name] = f"workflows/{workflow_name}.yaml"
    _write_yaml(config_path, config)
    _write_yaml(
        project_dir / f"workflows/{workflow_name}.yaml",
        {
            "workflow_name": workflow_name,
            "base_workflow": base_workflow,
            "step_overrides": step_overrides or {},
        },
    )


_STEP_SELECTED_OUTPUT = {
    "a_source": "a_out",
    "b_transform": "b_out",
    "c_transform": "c_out",
    "d_transform": "d_out",
    "fit_transform": "fit_out",
    "apply_transform": "apply_out",
    "multi_transform": "left_out",
    "use_multi": "multi_used",
}


def _write_sibling_workflow(
    project_dir: Path,
    *,
    workflow_name: str,
    step_names: list[str],
) -> None:
    # A base-style workflow with no base_workflow pointer: an independent sibling
    # of `main`/`derivative`, listing a subset of the global step pool.
    config_path = project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["workflows"][workflow_name] = f"workflows/{workflow_name}.yaml"
    _write_yaml(config_path, config)
    _write_yaml(
        project_dir / f"workflows/{workflow_name}.yaml",
        {
            "workflow_name": workflow_name,
            "steps": [
                {"step_name": name, "output_name": _STEP_SELECTED_OUTPUT[name]}
                for name in step_names
            ],
        },
    )


def _workflow_input_job(run_plan: object, *, step_name: str) -> object:
    return next(job for job in run_plan.jobs if job.step_name == step_name)


def _reused_keys(run_plan: object) -> set[tuple[str, str, str]]:
    return {
        (ref.step_name, ref.output_name, ref.address)
        for ref in run_plan.reused_outputs
    }


def _latest_registered_path(
    runtime_dir: Path,
    *,
    step_name: str,
    output_name: str,
    address: str,
) -> str:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            """
            SELECT path
            FROM artifacts
            WHERE context = 'cache'
              AND origin = 'workflow_output'
              AND step_name = ?
              AND output_name = ?
              AND address = ?
            ORDER BY artifact_id DESC
            LIMIT 1
            """,
            (step_name, output_name, address),
        ).fetchone()
    assert row is not None
    return str(row[0])


def _published_output_row(
    runtime_dir: Path,
    *,
    step_name: str,
    output_name: str,
    address: str,
) -> tuple[int, str]:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            """
            SELECT artifact_id, path
            FROM published_outputs
            WHERE context = 'cache'
              AND step_name = ?
              AND output_name = ?
              AND address = ?
            """,
            (step_name, output_name, address),
        ).fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def _latest_workflow_artifact_id(
    runtime_dir: Path,
    *,
    step_name: str,
    output_name: str,
    address: str,
) -> int:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE context = 'cache'
              AND origin = 'workflow_output'
              AND step_name = ?
              AND output_name = ?
              AND address = ?
            ORDER BY artifact_id DESC
            LIMIT 1
            """,
            (step_name, output_name, address),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _workflow_artifact_id(
    runtime_dir: Path,
    *,
    workflow_name: str,
    step_name: str,
    output_name: str,
    address: str,
) -> int:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE context = 'cache'
              AND origin = 'workflow_output'
              AND workflow_name = ?
              AND step_name = ?
              AND output_name = ?
              AND address = ?
            ORDER BY artifact_id DESC
            LIMIT 1
            """,
            (workflow_name, step_name, output_name, address),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _selected_artifact_id(
    runtime_dir: Path,
    *,
    step_name: str,
    output_name: str,
    address: str,
) -> int:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE context = 'cache'
              AND origin = 'workflow_output'
              AND step_name = ?
              AND output_name = ?
              AND address = ?
              AND is_selected_output = 1
            ORDER BY artifact_id DESC
            LIMIT 1
            """,
            (step_name, output_name, address),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _dependency_source_ids(runtime_dir: Path, *, dependent_artifact_id: int) -> list[int]:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        return [
            int(row[0])
            for row in conn.execute(
                """
                SELECT source_artifact_id
                FROM artifact_dependencies
                WHERE dependent_artifact_id = ?
                ORDER BY source_artifact_id
                """,
                (dependent_artifact_id,),
            ).fetchall()
        ]


def _registry_row_counts(runtime_dir: Path) -> dict[str, int]:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        return {
            "workflow_runs": conn.execute(
                "SELECT COUNT(*) FROM workflow_runs"
            ).fetchone()[0],
            "workflow_outputs": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
        }


def _workflow_artifact_count_for_run(
    runtime_dir: Path,
    *,
    run_id: int,
    step_name: str,
    output_name: str,
    address: str,
) -> int:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM artifacts
            WHERE run_id = ?
              AND origin = 'workflow_output'
              AND step_name = ?
              AND output_name = ?
              AND address = ?
            """,
            (run_id, step_name, output_name, address),
        ).fetchone()
    return int(row[0])


def _artifact_run_id(runtime_dir: Path, *, artifact_id: int) -> int:
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        row = conn.execute(
            "SELECT run_id FROM artifacts WHERE artifact_id = ?",
            (artifact_id,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _latest_workflow_payload(
    runtime_dir: Path,
    *,
    step_name: str,
    output_name: str,
    address: str,
) -> dict[str, object]:
    artifact_path = runtime_dir / _latest_registered_path(
        runtime_dir,
        step_name=step_name,
        output_name=output_name,
        address=address,
    )
    return json.loads(artifact_path.read_text(encoding="utf-8"))


def test_projection_planning_propagates_unregistered_source_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
    )
    snapshot_reads = 0
    original_read = execution_module.read_registered_source_snapshots

    def count_snapshot_read(*args: object, **kwargs: object) -> object:
        nonlocal snapshot_reads
        snapshot_reads += 1
        return original_read(*args, **kwargs)

    monkeypatch.setattr(
        execution_module,
        "read_registered_source_snapshots",
        count_snapshot_read,
    )

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )

    expected = UnresolvedRequestBundleProjection(
        (SourceCoordinate("cache", "data/source/sub_001.txt"),)
    )
    assert {job.step_name for job in run_plan.jobs} == {"a_source", "b_transform"}
    assert all(job.projection_state == expected for job in run_plan.jobs)
    assert run_plan.selected_jobs[0].projection_state == expected
    assert run_plan.reused_outputs == ()
    assert snapshot_reads == 1


def test_projection_planning_uses_prospective_upstream_request_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(first_plan, cores=1).published_count == len(
        first_plan.published_outputs
    )

    fresh_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    fresh_b = _workflow_input_job(fresh_b_plan, step_name="b_transform")
    assert isinstance(fresh_b.projection_state, RequestBundleProjectionV1)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    reused_b = next(
        output
        for output in c_plan.reused_outputs
        if output.step_name == "b_transform"
    )
    c_job = _workflow_input_job(c_plan, step_name="c_transform")
    assert reused_b.projection_state == fresh_b.projection_state
    assert isinstance(c_job.projection_state, RequestBundleProjectionV1)
    assert c_job.projection_state.role_labelled_bindings == (
        UpstreamRequestedOutputBinding(
            role="b_input",
            upstream_request_projection=fresh_b.projection_state,
            output_name="b_out",
        ),
    )

    derivative_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="b_transform",
    )
    derivative_b = _workflow_input_job(
        derivative_b_plan,
        step_name="b_transform",
    )
    assert derivative_b.projection_state == fresh_b.projection_state

    _write_workflow_variant(
        project_dir,
        workflow_name="parameter_variant",
        base_workflow="main",
        step_overrides={"b_transform": {"params": {"version": "2"}}},
    )
    parameter_variant_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="parameter_variant",
        step_name="b_transform",
    )
    parameter_variant_b = _workflow_input_job(
        parameter_variant_plan,
        step_name="b_transform",
    )
    assert parameter_variant_b.projection_state != fresh_b.projection_state

    before_edit = canonical_projection_json(c_job.projection_state)
    (runtime_dir / "data/source/sub_001.txt").write_text(
        "edited without source re-registration\n",
        encoding="utf-8",
    )
    after_edit_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    after_edit_c = _workflow_input_job(after_edit_plan, step_name="c_transform")
    assert isinstance(after_edit_c.projection_state, RequestBundleProjectionV1)
    assert canonical_projection_json(after_edit_c.projection_state) == before_edit

    source_step_path = project_dir / "steps/a_source.yaml"
    source_step = yaml.safe_load(source_step_path.read_text(encoding="utf-8"))
    source_step["step_contract_version"] = "2"
    _write_yaml(source_step_path, source_step)
    changed_request_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    changed_b = _workflow_input_job(
        changed_request_plan,
        step_name="b_transform",
    )
    assert isinstance(changed_b.projection_state, RequestBundleProjectionV1)
    assert changed_b.projection_state != fresh_b.projection_state


def test_projection_planning_covers_collection_cohort_and_sibling_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    _write_sibling_workflow(
        project_dir,
        workflow_name="apply_flow",
        step_names=["a_source", "b_transform", "fit_transform", "apply_transform"],
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )

    fit_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="fit_transform",
    )
    fit_job = _workflow_input_job(fit_plan, step_name="fit_transform")
    assert fit_job.address == "subjects"
    assert isinstance(fit_job.projection_state, RequestBundleProjectionV1)
    collection = fit_job.projection_state.role_labelled_bindings[0]
    assert isinstance(collection, CollectionBinding)
    assert collection.collection_semantics == "coordinate_set_v1"
    assert collection.manifest_digest is not None
    assert [member.upstream_request_projection.address for member in collection.members] == [
        "sub_001",
        "sub_002",
    ]

    apply_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="apply_transform",
    )
    apply_jobs = [
        job for job in apply_plan.jobs if job.step_name == "apply_transform"
    ]
    assert len(apply_jobs) == 2
    for apply_job in apply_jobs:
        assert isinstance(apply_job.projection_state, RequestBundleProjectionV1)
        assert {
            binding.role
            for binding in apply_job.projection_state.role_labelled_bindings
            if isinstance(binding, UpstreamRequestedOutputBinding)
        } == {"b_input", "fit_input"}

    multi_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
    )
    multi_job = _workflow_input_job(multi_plan, step_name="multi_transform")
    assert isinstance(multi_job.projection_state, RequestBundleProjectionV1)
    assert [
        sibling.output_name
        for sibling in multi_job.projection_state.output_contract.sibling_outputs
    ] == ["left_out", "right_out"]
    assert multi_job.output_ref("left_out").projection_plan is multi_job.projection_plan
    assert multi_job.output_ref("right_out").projection_state is multi_job.projection_state


def test_dependency_free_workflow_output_remains_reusable_as_an_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    _write_yaml(
        project_dir / "steps/constant_value.yaml",
        {
            "step_name": "constant_value",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_a_file",
            "manifest_binding": {
                "role": "source_population",
                "manifest": "subjects",
            },
            "outputs": {
                "constant_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                }
            },
        },
    )
    _write_yaml(
        project_dir / "steps/use_constant.yaml",
        {
            "step_name": "use_constant",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "cache_runtime:step_b_file",
            "inputs": {
                "constant_input": {
                    "artifact": "constant_value.constant_out",
                    "dependency_role": "source_input",
                }
            },
            "outputs": {
                "used_out": {
                    "extension": ".json",
                    "address_scope": "entity",
                }
            },
        },
    )
    config_path = project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["workflows"]["constant_flow"] = "workflows/constant_flow.yaml"
    _write_yaml(config_path, config)
    _write_yaml(
        project_dir / "workflows/constant_flow.yaml",
        {
            "workflow_name": "constant_flow",
            "steps": [
                {"step_name": "constant_value", "output_name": "constant_out"},
                {"step_name": "use_constant", "output_name": "used_out"},
            ],
        },
    )

    def execute_synthetic(plan: object) -> int:
        for job in plan.jobs:
            for output in job.outputs.values():
                output.staging_path.parent.mkdir(parents=True, exist_ok=True)
                output.staging_path.write_text(
                    json.dumps({"job_id": job.job_id}) + "\n",
                    encoding="utf-8",
                )
        return 0

    constant_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="constant_flow",
        step_name="constant_value",
        address="sub_001",
    )
    monkeypatch.setattr(
        "nipact.execution._run_snakemake",
        lambda *_args, **_kwargs: execute_synthetic(constant_plan),
    )
    assert execute_run_plan(constant_plan, cores=1).all_selected_published

    consumer_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="constant_flow",
        step_name="use_constant",
        address="sub_001",
    )
    assert [ref.step_name for ref in consumer_plan.reused_outputs] == [
        "constant_value"
    ]
    monkeypatch.setattr(
        "nipact.execution._run_snakemake",
        lambda *_args, **_kwargs: execute_synthetic(consumer_plan),
    )
    assert execute_run_plan(consumer_plan, cores=1).all_selected_published


def test_cross_target_run_plan_reuses_upstream_from_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)
    registered_b_path = _latest_registered_path(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    registered_b_digest = sha256_file_digest(runtime_dir / registered_b_path)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    c_job = _workflow_input_job(c_plan, step_name="c_transform")
    assert "b_transform" not in [job.step_name for job in c_plan.jobs]
    assert len(c_plan.reused_outputs) == 1
    assert c_plan.reused_outputs[0].source_artifact_id == _latest_workflow_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )

    assert c_job.inputs["b_input"] == (
        "staging/b_transform/b_out/sub_001.json",
    )
    registered_b_relative_to_c = os.path.relpath(
        runtime_dir / registered_b_path,
        c_plan.run_workspace,
    ).replace(os.sep, "/")
    assert c_job.inputs["b_input"][0] != registered_b_relative_to_c
    assert execute_run_plan(c_plan, cores=1).published_count == len(c_plan.published_outputs)

    hydrated_b = c_plan.run_workspace / "staging/b_transform/b_out/sub_001.json"
    assert hydrated_b.is_file()
    assert sha256_file_digest(hydrated_b) == registered_b_digest
    assert "job__b_transform__b_out__sub_001" not in (
        c_plan.run_workspace / "Snakefile"
    ).read_text(encoding="utf-8")
    assert '"staging/b_transform/b_out/sub_001.json"' in (
        c_plan.run_workspace / "Snakefile"
    ).read_text(encoding="utf-8")
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]

    c_artifact_id = _selected_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    c_run_id = _artifact_run_id(runtime_dir, artifact_id=c_artifact_id)
    # Hydrated files are inputs to the current run, not outputs produced by the
    # current run. If we accidentally register the hydrated B file under C's
    # run_id, trace would imply recomputation even though the B producer rule was
    # omitted from the generated Snakefile.
    assert (
        _workflow_artifact_count_for_run(
            runtime_dir,
            run_id=c_run_id,
            step_name="b_transform",
            output_name="b_out",
            address="sub_001",
        )
        == 0
    )
    assert _dependency_source_ids(runtime_dir, dependent_artifact_id=c_artifact_id) == [
        c_plan.reused_outputs[0].source_artifact_id
    ]


def test_cross_target_dry_run_maps_reused_upstream_without_copying(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]
    registered_b_path = _latest_registered_path(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    counts_before = _registry_row_counts(runtime_dir)
    outputs_before = {
        str(path.relative_to(runtime_dir)): sha256_file_digest(path)
        for path in sorted((runtime_dir / "outputs").rglob("*"))
        if path.is_file()
    }

    def fail_copy(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("dry run must not copy reused artifacts")

    monkeypatch.setattr("nipact.execution.shutil.copy2", fail_copy)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        dry_run=True,
    )
    assert execute_run_plan(c_plan, cores=1).published_count == 0

    # Real Snakemake built the dry-run DAG against the mapped registered
    # source: the reused producer rule is absent, no artifact was staged, and
    # no callable executed.
    assert "b_transform" not in [job.step_name for job in c_plan.jobs]
    assert list((c_plan.run_workspace / "staging").rglob("*")) == []
    snakefile_text = (c_plan.run_workspace / "Snakefile").read_text(encoding="utf-8")
    mapped_b = os.path.relpath(
        runtime_dir / registered_b_path,
        c_plan.run_workspace,
    ).replace(os.sep, "/")
    assert json.dumps(mapped_b) in snakefile_text
    assert "staging/b_transform/b_out/sub_001.json" not in snakefile_text
    # The serialized staging contract is unchanged: only the generated
    # Snakefile maps reused inputs to registered sources.
    run_plan_payload = json.loads(
        (c_plan.run_workspace / "run_plan.json").read_text(encoding="utf-8")
    )
    assert run_plan_payload["reused_outputs"][0]["staging_path"] == (
        "staging/b_transform/b_out/sub_001.json"
    )
    assert run_plan_payload["reused_outputs"][0]["source_path"] == registered_b_path
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]
    assert _registry_row_counts(runtime_dir) == counts_before
    outputs_after = {
        str(path.relative_to(runtime_dir)): sha256_file_digest(path)
        for path in sorted((runtime_dir / "outputs").rglob("*"))
        if path.is_file()
    }
    assert outputs_after == outputs_before


def test_dry_run_cli_reports_reuse_without_hydration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )
    log_before = log_path.read_text(encoding="utf-8")
    counts_before = _registry_row_counts(runtime_dir)
    capsys.readouterr()

    assert (
        main(
            [
                "workflow",
                "run",
                "--project-dir",
                str(project_dir),
                "--context",
                "cache",
                "--workflow",
                "main",
                "--step",
                "c_transform",
                "--dry-run",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    summary = dict(line.split("=", maxsplit=1) for line in lines if "=" in line)
    assert summary["dry_run"] == "true"
    # b is reused, so the forecast is the single fresh c job while the reuse
    # counters report the reference without any hydration.
    assert summary["planned_reachable_fresh_jobs"] == "1"
    assert summary["planned_reused_registered_artifacts"] == "1"
    assert summary["planned_reused_inputs"] == "1"
    assert summary["planned_hydrated_inputs"] == "0"
    assert "existing_staged_outputs" not in summary
    assert "planned_hydration_bytes" not in summary
    assert summary["note"].startswith("Dry run:")
    assert summary["outputs_published"] == "false"
    assert summary["registry"] == "not_updated"
    assert "PASS: workflow run" in lines
    # Real Snakemake built the DAG; nothing executed, staged, or recorded.
    assert log_path.read_text(encoding="utf-8") == log_before
    assert _registry_row_counts(runtime_dir) == counts_before
    dry_workspace = runtime_dir / "runs/cache/main/c_transform/dry-run"
    assert list((dry_workspace / "staging").rglob("*")) == []


def test_real_run_cli_reports_planned_hydration_bytes_fanout_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    _write_sibling_workflow(
        project_dir,
        workflow_name="apply_flow",
        step_names=["a_source", "b_transform", "fit_transform", "apply_transform"],
    )
    fit_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="fit_transform",
    )
    assert execute_run_plan(fit_plan, cores=1).published_count == len(
        fit_plan.published_outputs
    )
    # Expected bytes come from the published files themselves, independent of
    # the plan's file_size bookkeeping the CLI sums.
    expected_bytes = sum(
        (runtime_dir / _latest_registered_path(
            runtime_dir,
            step_name=step_name,
            output_name=output_name,
            address=address,
        )).stat().st_size
        for step_name, output_name, address in (
            ("b_transform", "b_out", "sub_001"),
            ("b_transform", "b_out", "sub_002"),
            ("fit_transform", "fit_out", "subjects"),
        )
    )

    captured_plans = []

    def publish_stub(run_plan: object, **_kwargs: object) -> RunOutcome:
        captured_plans.append(run_plan)
        return RunOutcome(
            published_count=0,
            failed_jobs=(),
            all_selected_published=True,
        )

    monkeypatch.setattr("nipact.execution.execute_run_plan", publish_stub)
    capsys.readouterr()

    assert (
        main(
            [
                "workflow",
                "run",
                "--project-dir",
                str(project_dir),
                "--context",
                "cache",
                "--workflow",
                "apply_flow",
                "--step",
                "apply_transform",
            ]
        )
        == 0
    )

    summary = dict(
        line.split("=", maxsplit=1)
        for line in capsys.readouterr().out.splitlines()
        if "=" in line
    )
    (apply_plan,) = captured_plans
    # The cohort fit fans out to both apply jobs but is one reused ref — one
    # staged copy — so the aggregate counts its bytes once, not per consumer.
    fit_ref = next(
        ref for ref in apply_plan.reused_outputs if ref.step_name == "fit_transform"
    )
    fit_consumers = [
        job
        for job in apply_plan.jobs
        if fit_ref.staging_path_relative
        in [str(path) for paths in job.inputs.values() for path in paths]
    ]
    assert len(fit_consumers) == 2
    assert len(apply_plan.reused_outputs) == 3
    assert summary["dry_run"] == "false"
    assert summary["planned_reachable_fresh_jobs"] == "2"
    assert summary["planned_reused_registered_artifacts"] == "3"
    assert summary["planned_reused_inputs"] == "3"
    # Real execution hydrates everything it reuses: the two counters agree.
    assert summary["planned_hydrated_inputs"] == "3"
    assert summary["planned_hydration_bytes"] == str(expected_bytes)
    assert summary["existing_staged_outputs"] == "0"
    assert "can be hydrated" in summary["note"]


def test_dry_run_maps_fresh_candidate_path_when_registered_path_moves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        dry_run=True,
    )
    assert len(c_plan.reused_outputs) == 1
    planned_source = c_plan.reused_outputs[0].source_path
    old_rel = c_plan.reused_outputs[0].source_path_relative
    artifact_id = c_plan.reused_outputs[0].source_artifact_id

    # Re-register the artifact at a new path under outputs/ while leaving the
    # old file present with a valid size: an implementation that confirmed only
    # the artifact_id and then mapped the stale planned path would still pass
    # every validation check, so only genuine re-resolution finds the move.
    new_rel = "/".join([*old_rel.split("/")[:-1], "moved", old_rel.split("/")[-1]])
    new_path = runtime_dir / new_rel
    new_path.parent.mkdir(parents=True, exist_ok=True)
    new_path.write_bytes(planned_source.read_bytes())
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            "UPDATE artifacts SET path = ?, published_path = ? WHERE artifact_id = ?",
            (new_rel, new_rel, artifact_id),
        )
        conn.execute(
            "UPDATE published_outputs SET path = ? WHERE artifact_id = ?",
            (new_rel, artifact_id),
        )
    assert planned_source.is_file()

    calls: list[bool] = []

    def record_run(run_plan: object, *, cores: int, dry_run: bool) -> int:
        calls.append(dry_run)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", record_run)
    assert execute_run_plan(c_plan, cores=1).published_count == 0
    assert calls == [True]

    snakefile_text = (c_plan.run_workspace / "Snakefile").read_text(encoding="utf-8")
    mapped_new = os.path.relpath(
        runtime_dir / new_rel,
        c_plan.run_workspace,
    ).replace(os.sep, "/")
    mapped_old = os.path.relpath(
        runtime_dir / old_rel,
        c_plan.run_workspace,
    ).replace(os.sep, "/")
    assert json.dumps(mapped_new) in snakefile_text
    assert mapped_old not in snakefile_text


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("delete", "registered reusable artifact file is missing"),
        ("resize", "registered reusable artifact file size mismatch"),
        ("delete_membership", None),
    ],
)
def test_dry_run_revalidates_reused_source_before_snakemake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    message: str | None,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        dry_run=True,
    )
    assert len(c_plan.reused_outputs) == 1
    source = c_plan.reused_outputs[0].source_path

    # The mapping exposes a direct registered path to Snakemake, so the reused
    # source is revalidated between planning and Snakemake invocation exactly
    # like hydration would: a failure aborts before the dry workspace or any
    # Snakemake process exists.
    if change == "delete":
        source.unlink()
    elif change == "resize":
        source.write_text(
            source.read_text(encoding="utf-8") + "tampered\n",
            encoding="utf-8",
        )
    else:
        with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
            conn.execute(
                "DELETE FROM published_outputs WHERE artifact_id = ?",
                (c_plan.reused_outputs[0].source_artifact_id,),
            )
    counts_before = _registry_row_counts(runtime_dir)

    calls: list[bool] = []

    def record_run(run_plan: object, *, cores: int, dry_run: bool) -> int:
        calls.append(dry_run)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", record_run)
    if message is None:
        assert execute_run_plan(c_plan, cores=1).published_count == 0
        assert calls == [True]
        assert c_plan.run_workspace.exists()
    else:
        with pytest.raises(ValidationError, match=message):
            execute_run_plan(c_plan, cores=1)
        assert calls == []
        assert not c_plan.run_workspace.exists()
    assert _registry_row_counts(runtime_dir) == counts_before


def test_real_run_records_reused_dependency_without_current_membership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )
    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    reused_id = c_plan.reused_outputs[0].source_artifact_id
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            "DELETE FROM published_outputs WHERE artifact_id = ?",
            (reused_id,),
        )

    assert execute_run_plan(c_plan, cores=1).all_selected_published
    c_id = _latest_workflow_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute(
            """
            SELECT source_artifact_id
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
              AND source_step_name = 'b_transform'
            """,
            (c_id,),
        ).fetchone() == (reused_id,)


def test_execution_reresolution_rejects_retracted_artifact_before_snakemake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )
    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            "UPDATE artifacts SET is_published = 0 WHERE artifact_id = ?",
            (c_plan.reused_outputs[0].source_artifact_id,),
        )
    calls: list[bool] = []
    monkeypatch.setattr(
        "nipact.execution._run_snakemake",
        lambda *_args, **_kwargs: calls.append(True) or 0,
    )

    with pytest.raises(
        ValidationError,
        match="reusable artifact bundle is no longer valid",
    ):
        execute_run_plan(c_plan, cores=1)
    assert calls == []


def test_real_hydration_reads_freshly_reresolved_registered_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )
    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    reused = c_plan.reused_outputs[0]
    old_path = reused.source_path
    new_rel = (
        "outputs/cache/main/b_transform/b_out/relocated/"
        f"{old_path.name}"
    )
    new_path = runtime_dir / new_rel
    new_path.parent.mkdir(parents=True, exist_ok=True)
    new_path.write_bytes(old_path.read_bytes())
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            """
            UPDATE artifacts
            SET path = ?, published_path = ?
            WHERE artifact_id = ?
            """,
            (new_rel, new_rel, reused.source_artifact_id),
        )

    original_digest = execution_module.sha256_file_digest
    hashed_paths: list[Path] = []

    def recording_digest(path: Path) -> str:
        hashed_paths.append(Path(path))
        return original_digest(path)

    monkeypatch.setattr(execution_module, "sha256_file_digest", recording_digest)
    assert execute_run_plan(c_plan, cores=1).all_selected_published
    assert new_path in hashed_paths
    assert old_path not in hashed_paths


def test_dry_run_accepts_same_size_corruption_real_run_rejects_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    dry_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        dry_run=True,
    )
    assert len(dry_plan.reused_outputs) == 1
    source = dry_plan.reused_outputs[0].source_path
    source.write_text(
        source.read_text(encoding="utf-8").replace("alpha", "omega"),
        encoding="utf-8",
    )

    # The byte-integrity boundary: dry-run revalidation is existence and size
    # without hashing, so same-size corruption passes the forecast; the
    # following real execution still hashes the source at hydration and fails.
    assert execute_run_plan(dry_plan, cores=1).published_count == 0

    real_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    with pytest.raises(
        ValidationError,
        match="reusable artifact digest mismatch during hydration",
    ):
        execute_run_plan(real_plan, cores=1)


def test_dry_run_forecast_not_suppressed_by_completed_real_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    real_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert execute_run_plan(real_plan, cores=1).published_count == len(
        real_plan.published_outputs
    )
    staged_real_c = real_plan.selected_output_refs[0].staging_path
    assert staged_real_c.is_file()

    dry_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        dry_run=True,
    )
    assert dry_plan.run_workspace == real_plan.run_workspace / "dry-run"

    staged_seen: list[bool] = []

    def record_staging(*_args: object, **_kwargs: object) -> int:
        staged_seen.append(dry_plan.selected_output_refs[0].staging_path.exists())
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", record_staging)
    assert execute_run_plan(dry_plan, cores=1).published_count == 0

    # The executable workspace still holds the completed run's staged output,
    # but the isolated dry-run workspace does not, so Snakemake plans the
    # fresh job instead of reporting nothing to do.
    assert staged_seen == [False]
    assert staged_real_c.is_file()


def test_dry_run_leaves_executable_workspace_byte_identical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    real_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert execute_run_plan(real_plan, cores=1).published_count == len(
        real_plan.published_outputs
    )

    def executable_snapshot() -> dict[str, str]:
        # The isolated dry-run workspace nests under the executable one, so
        # the executable snapshot is everything outside the dry-run subtree.
        return {
            str(path.relative_to(real_plan.run_workspace)): sha256_file_digest(path)
            for path in sorted(real_plan.run_workspace.rglob("*"))
            if path.is_file()
            and "dry-run" not in path.relative_to(real_plan.run_workspace).parts
        }

    snapshot_before = executable_snapshot()

    dry_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        dry_run=True,
    )
    assert execute_run_plan(dry_plan, cores=1).published_count == 0

    assert executable_snapshot() == snapshot_before


@pytest.mark.parametrize("change", ["params", "callable", "extension"])
def test_downstream_does_not_reuse_when_direct_upstream_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    # The old B artifact shares address, source bytes, and output name, but the
    # current step declaration changes the concrete artifact contract (params,
    # callable, or extension). The downstream C run must therefore recompute B
    # instead of hydrating the prior B output.
    b_step_path = project_dir / "steps/b_transform.yaml"
    b_step = yaml.safe_load(b_step_path.read_text(encoding="utf-8"))
    if change == "params":
        b_step["params"]["version"] = "v2"
        expected_log = ["B sub_001", "B sub_001"]
        expected_value = "alpha-b-v2-c"
    elif change == "callable":
        b_step["callable"] = "cache_runtime:step_b_alt_file"
        expected_log = ["B sub_001", "B_ALT sub_001"]
        expected_value = "alpha-b-alt-c"
    else:
        b_step["outputs"]["b_out"]["extension"] = ".txt"
        expected_log = ["B sub_001", "B sub_001"]
        expected_value = "alpha-b-c"
    _write_yaml(b_step_path, b_step)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )

    assert [output_ref.step_name for output_ref in c_plan.reused_outputs] == [
        "a_source"
    ]
    assert "b_transform" in [job.step_name for job in c_plan.jobs]
    if change == "extension":
        c_job = _workflow_input_job(c_plan, step_name="c_transform")
        assert c_job.inputs["b_input"] == (
            "staging/b_transform/b_out/sub_001.txt",
        )
    assert execute_run_plan(c_plan, cores=1).published_count == len(c_plan.published_outputs)
    assert log_path.read_text(encoding="utf-8").splitlines() == expected_log
    assert _latest_workflow_payload(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )["value"] == expected_value


def test_expanded_manifest_reuses_unchanged_entity_and_computes_new_entity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)
    first_b_sub_001 = _selected_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )

    # This mirrors the practical "I added subjects" workflow. The manifest has
    # changed, so selected downstream C must run for both entities. But the
    # already-published single-entity B(sub_001) remains valid and should be
    # hydrated, while B(sub_002) has no cache candidate and must compute fresh.
    _add_cache_entity(project_dir, runtime_dir, address="sub_002", seed="beta")

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )

    assert len(c_plan.reused_outputs) == 1
    assert c_plan.reused_outputs[0].source_artifact_id == first_b_sub_001
    assert execute_run_plan(c_plan, cores=1).published_count == len(c_plan.published_outputs)
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "B sub_001",
        "B sub_002",
    ]

    c_sub_001 = _selected_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    c_sub_002 = _selected_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_002",
    )
    b_sub_002 = _latest_workflow_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )
    assert _dependency_source_ids(runtime_dir, dependent_artifact_id=c_sub_001) == [
        first_b_sub_001
    ]
    assert _dependency_source_ids(runtime_dir, dependent_artifact_id=c_sub_002) == [
        b_sub_002
    ]
    assert _latest_workflow_payload(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )["value"] == "alpha-b-c"
    assert _latest_workflow_payload(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_002",
    )["value"] == "beta-b-c"


def test_multi_output_selected_step_publishes_siblings_for_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    multi_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
    )
    assert execute_run_plan(multi_plan, cores=1).published_count == len(multi_plan.published_outputs)
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published = conn.execute(
            """
            SELECT output_name
            FROM published_outputs
            WHERE context = 'cache'
              AND workflow_name = 'main'
              AND step_name = 'multi_transform'
              AND address = 'sub_001'
            ORDER BY output_name
            """
        ).fetchall()
    assert [row[0] for row in published] == ["left_out", "right_out"]

    fresh_multi_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
    )
    assert execute_run_plan(fresh_multi_plan, cores=1).published_count == len(fresh_multi_plan.published_outputs)
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published_after_rerun = conn.execute(
            """
            SELECT output_name
            FROM published_outputs
            WHERE context = 'cache'
              AND workflow_name = 'main'
              AND step_name = 'multi_transform'
              AND address = 'sub_001'
            ORDER BY output_name
            """
        ).fetchall()
    assert [row[0] for row in published_after_rerun] == ["left_out", "right_out"]

    # The workflow target names one output, but the selected job produced both
    # siblings. Publishing both lets the downstream all-or-nothing producer reuse
    # check pass without rerunning the multi-output step.
    use_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="use_multi",
    )

    assert "multi_transform" not in [job.step_name for job in use_plan.jobs]
    assert [
        output_ref.output_name
        for output_ref in use_plan.reused_outputs
        if output_ref.step_name == "multi_transform"
    ] == ["left_out"]
    assert execute_run_plan(use_plan, cores=1).published_count == len(use_plan.published_outputs)
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]


def test_cross_workflow_membership_adopts_complete_transitive_reused_bundles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
    )
    main_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
    )
    assert execute_run_plan(main_plan, cores=1).published_count == len(
        main_plan.published_outputs
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        main_artifacts_before = conn.execute(
            """
            SELECT artifact_id, run_id, workflow_name, step_name, output_name,
                   address, path, content_digest, output_hash,
                   request_bundle_projection_json
            FROM artifacts
            WHERE context = 'cache' AND workflow_name = 'main'
            ORDER BY artifact_id
            """
        ).fetchall()
        main_dependencies_before = conn.execute(
            """
            SELECT ad.*
            FROM artifact_dependencies ad
            JOIN artifacts a ON a.artifact_id = ad.dependent_artifact_id
            WHERE a.context = 'cache' AND a.workflow_name = 'main'
            ORDER BY ad.dependent_artifact_id, ad.binding_name,
                     ad.input_path, ad.source_artifact_id
            """
        ).fetchall()
    _write_sibling_workflow(
        project_dir,
        workflow_name="multi_derivative",
        step_names=[
            "a_source",
            "b_transform",
            "multi_transform",
            "use_multi",
        ],
    )

    derivative_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="multi_derivative",
        step_name="use_multi",
    )
    assert [ref.output_name for ref in derivative_plan.reused_outputs] == ["left_out"]
    outcome = execute_run_plan(derivative_plan, cores=1)
    assert outcome.published_count == len(derivative_plan.published_outputs) == 1

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        memberships = conn.execute(
            """
            SELECT po.step_name, po.output_name, po.artifact_id, a.workflow_name
            FROM published_outputs po
            JOIN artifacts a ON a.artifact_id = po.artifact_id
            WHERE po.context = 'cache'
              AND po.workflow_name = 'multi_derivative'
              AND po.address = 'sub_001'
            ORDER BY po.step_name, po.output_name
            """
        ).fetchall()
        resolution_summary = json.loads(
            conn.execute(
                """
                SELECT resolution_summary_json
                FROM workflow_runs
                WHERE context = 'cache'
                  AND workflow_name = 'multi_derivative'
                  AND selected_step_name = 'use_multi'
                  AND is_current = 1
                """
            ).fetchone()[0]
        )
        assert conn.execute(
            """
            SELECT artifact_id, run_id, workflow_name, step_name, output_name,
                   address, path, content_digest, output_hash,
                   request_bundle_projection_json
            FROM artifacts
            WHERE context = 'cache' AND workflow_name = 'main'
            ORDER BY artifact_id
            """
        ).fetchall() == main_artifacts_before
        assert conn.execute(
            """
            SELECT ad.*
            FROM artifact_dependencies ad
            JOIN artifacts a ON a.artifact_id = ad.dependent_artifact_id
            WHERE a.context = 'cache' AND a.workflow_name = 'main'
            ORDER BY ad.dependent_artifact_id, ad.binding_name,
                     ad.input_path, ad.source_artifact_id
            """
        ).fetchall() == main_dependencies_before
    assert [(row[0], row[1]) for row in memberships] == [
        ("a_source", "a_out"),
        ("b_transform", "b_out"),
        ("multi_transform", "left_out"),
        ("multi_transform", "right_out"),
        ("use_multi", "multi_used"),
    ]
    assert [row[3] for row in memberships] == [
        "main",
        "main",
        "main",
        "main",
        "multi_derivative",
    ]
    sibling_run_ids = {
        _artifact_run_id(runtime_dir, artifact_id=int(row[2]))
        for row in memberships
        if row[0] == "multi_transform"
    }
    assert len(sibling_run_ids) == 1
    assert resolution_summary["selected_outputs"] == [
        {
            "context": "cache",
            "workflow_name": "multi_derivative",
            "step_name": "use_multi",
            "output_name": "multi_used",
            "address": "sub_001",
            "resolution": {
                "artifact_id": next(
                    int(row[2]) for row in memberships if row[0] == "use_multi"
                ),
                "outcome": "generated",
            },
        }
    ]


def test_failed_fresh_branch_does_not_adopt_its_reused_memberships(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    main_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(main_plan, cores=1).all_selected_published

    derivative_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
    )

    def write_one_selected_output(
        _run_plan: object,
        *,
        cores: int,
        dry_run: bool,
    ) -> int:
        job = next(
            job
            for job in derivative_plan.jobs
            if job.step_name == "c_transform" and job.address == "sub_001"
        )
        output = job.outputs["c_out"]
        output.staging_path.parent.mkdir(parents=True, exist_ok=True)
        output.staging_path.write_text('{"value":"retained"}\n', encoding="utf-8")
        return 1

    monkeypatch.setattr("nipact.execution._run_snakemake", write_one_selected_output)
    outcome = execute_run_plan(derivative_plan, cores=1)
    assert outcome.published_count == 1
    assert any(address == "sub_002" for _step, address, _reason in outcome.failed_jobs)

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        derivative_memberships = conn.execute(
            """
            SELECT step_name, output_name, address
            FROM published_outputs
            WHERE context = 'cache' AND workflow_name = 'derivative'
            ORDER BY step_name, output_name, address
            """
        ).fetchall()
        main_memberships = conn.execute(
            """
            SELECT step_name, output_name, address
            FROM published_outputs
            WHERE context = 'cache'
              AND workflow_name = 'main'
              AND address = 'sub_002'
            ORDER BY step_name, output_name
            """
        ).fetchall()
    assert derivative_memberships == [
        ("a_source", "a_out", "sub_001"),
        ("b_transform", "b_out", "sub_001"),
        ("c_transform", "c_out", "sub_001"),
    ]
    assert main_memberships == [
        ("a_source", "a_out", "sub_002"),
        ("b_transform", "b_out", "sub_002"),
    ]


def test_multi_output_dry_run_maps_consumed_reused_sibling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)
    multi_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
    )
    assert execute_run_plan(multi_plan, cores=1).published_count == len(
        multi_plan.published_outputs
    )
    log_before = log_path.read_text(encoding="utf-8")
    counts_before = _registry_row_counts(runtime_dir)

    use_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="use_multi",
        dry_run=True,
    )
    assert "multi_transform" not in [job.step_name for job in use_plan.jobs]
    assert [
        output_ref.output_name
        for output_ref in use_plan.reused_outputs
        if output_ref.step_name == "multi_transform"
    ] == ["left_out"]
    assert execute_run_plan(use_plan, cores=1).published_count == 0

    # Every reachable reused input — including the consumed sibling of the
    # multi-output producer — is mapped to its registered source, and the
    # staging alias appears nowhere in the generated rules.
    snakefile_text = (use_plan.run_workspace / "Snakefile").read_text(encoding="utf-8")
    for output_ref in use_plan.reused_outputs:
        mapped = os.path.relpath(
            runtime_dir / output_ref.source_path_relative,
            use_plan.run_workspace,
        ).replace(os.sep, "/")
        assert json.dumps(mapped) in snakefile_text
        assert output_ref.staging_path_relative not in snakefile_text
    assert list((use_plan.run_workspace / "staging").rglob("*")) == []
    assert log_path.read_text(encoding="utf-8") == log_before
    assert _registry_row_counts(runtime_dir) == counts_before

    invalidated_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="use_multi",
        dry_run=True,
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        right_path = conn.execute(
            """
            SELECT path
            FROM artifacts
            WHERE context = 'cache'
              AND step_name = 'multi_transform'
              AND output_name = 'right_out'
              AND address = 'sub_001'
              AND is_published = 1
            ORDER BY run_id
            LIMIT 1
            """
        ).fetchone()[0]
    (runtime_dir / str(right_path)).unlink()
    calls: list[bool] = []
    monkeypatch.setattr(
        "nipact.execution._run_snakemake",
        lambda *_args, **_kwargs: calls.append(True) or 0,
    )
    with pytest.raises(
        ValidationError,
        match="registered reusable artifact file is missing",
    ):
        execute_run_plan(invalidated_plan, cores=1)
    assert calls == []


def test_targeted_multi_output_run_publishes_both_siblings_for_one_entity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    multi_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )
    assert len(multi_plan.selected_output_refs) == 1
    assert multi_plan.run_workspace == (
        runtime_dir / "runs/cache/main/multi_transform/left_out/addresses/sub_001"
    )
    assert execute_run_plan(multi_plan, cores=1).all_selected_published
    # The workflow target names left_out for one entity, but the selected job
    # writes both siblings; both publish so the downstream all-or-nothing
    # producer reuse check stays satisfiable, and nothing publishes or
    # executes for sub_002.
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published = conn.execute(
            """
            SELECT step_name, output_name, address
            FROM published_outputs
            WHERE context = 'cache'
            ORDER BY step_name, output_name, address
            """
        ).fetchall()
    assert [(str(row[0]), str(row[1]), str(row[2])) for row in published] == [
        ("a_source", "a_out", "sub_001"),
        ("b_transform", "b_out", "sub_001"),
        ("multi_transform", "left_out", "sub_001"),
        ("multi_transform", "right_out", "sub_001"),
    ]
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]


def test_derivative_reuses_compatible_base_ancestor_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    _write_workflow_variant(
        project_dir,
        workflow_name="derivative",
        base_workflow="main",
    )
    main_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(main_b_plan, cores=1).published_count == len(main_b_plan.published_outputs)
    main_b_artifact_id = _selected_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    main_b_run_id = _artifact_run_id(runtime_dir, artifact_id=main_b_artifact_id)
    registered_b_path = _latest_registered_path(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    registered_b_digest = sha256_file_digest(runtime_dir / registered_b_path)

    derivative_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
    )

    assert [output_ref.step_name for output_ref in derivative_c_plan.reused_outputs] == [
        "b_transform"
    ]
    assert derivative_c_plan.reused_outputs[0].source_artifact_id == main_b_artifact_id
    assert derivative_c_plan.reused_outputs[0].source_workflow_name == "main"
    assert "b_transform" not in [job.step_name for job in derivative_c_plan.jobs]
    assert execute_run_plan(derivative_c_plan, cores=1).published_count == len(
        derivative_c_plan.published_outputs
    )

    hydrated_b = derivative_c_plan.run_workspace / "staging/b_transform/b_out/sub_001.json"
    assert hydrated_b.is_file()
    assert sha256_file_digest(hydrated_b) == registered_b_digest
    run_plan_payload = json.loads(
        (derivative_c_plan.run_workspace / "run_plan.json").read_text(encoding="utf-8")
    )
    assert run_plan_payload["base_workflow"] == "main"
    assert run_plan_payload["reused_outputs"] == [
        {
            "step_name": "b_transform",
            "output_name": "b_out",
            "address": "sub_001",
            "staging_path": "staging/b_transform/b_out/sub_001.json",
            "source_path": registered_b_path,
            "source_artifact_id": main_b_artifact_id,
            "source_workflow_name": "main",
            "source_run_id": main_b_run_id,
            "content_digest": registered_b_digest,
            "file_size": (runtime_dir / registered_b_path).stat().st_size,
        }
    ]
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute(
            """
            SELECT base_workflow_name
            FROM workflow_runs
            WHERE context = 'cache'
              AND workflow_name = 'derivative'
              AND selected_step_name = 'c_transform'
              AND selected_output_name = 'c_out'
              AND is_current = 1
            """
        ).fetchone()[0] == "main"
        membership = conn.execute(
            """
            SELECT po.artifact_id, a.workflow_name, a.run_id
            FROM published_outputs po
            JOIN artifacts a ON a.artifact_id = po.artifact_id
            WHERE po.context = 'cache'
              AND po.workflow_name = 'derivative'
              AND po.step_name = 'b_transform'
              AND po.output_name = 'b_out'
              AND po.address = 'sub_001'
            """
        ).fetchone()
        assert membership == (main_b_artifact_id, "main", main_b_run_id)

    derivative_b = registry_module.read_current_published_artifact(
        runtime_dir / "database/registry.db",
        context="cache",
        workflow_name="derivative",
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    assert derivative_b.artifact_id == main_b_artifact_id
    assert derivative_b.workflow_name == "main"

    derivative_c_artifact_id = _selected_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    assert _dependency_source_ids(
        runtime_dir,
        dependent_artifact_id=derivative_c_artifact_id,
    ) == [main_b_artifact_id]
    graph = build_trace_graph_for_workflow_coordinate(
        runtime_dir / "database/registry.db",
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    artifacts_by_id = {artifact["artifact_id"]: artifact for artifact in graph["artifacts"]}
    assert artifacts_by_id[main_b_artifact_id]["workflow_name"] == "main"
    assert [
        dependency["is_reused_input"]
        for dependency in graph["dependencies"]
        if dependency["dependent_artifact_id"] == derivative_c_artifact_id
    ] == [True]


def test_published_artifact_from_another_context_is_not_reused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)
    probe_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    request = probe_plan.reused_outputs[0].reuse_request

    # Runtime roots are separate in normal projects, but the registry resolver
    # still needs an explicit context predicate. Without it, a valid selected
    # output from another context in the same registry could be hydrated into the
    # wrong workflow declaration.
    foreign_context_request = registry_module.ReusableArtifactBundleRequest(
        **{**request.__dict__, "context": "other_context"}
    )

    assert (
        registry_module.resolve_reusable_artifact_bundle(
            runtime_dir / "database/registry.db",
            runtime_root=runtime_dir,
            request=foreign_context_request,
        )
        is None
    )


@pytest.mark.parametrize(
    ("change", "message"),
        [
            ("delete", "registered reusable artifact file is missing"),
            ("mutate", "reusable artifact digest mismatch during hydration"),
            (
                "symlink_escape",
                "registered reusable artifact path must stay inside outputs/",
            ),
        ],
    )
def test_hydration_revalidates_published_file_after_plan_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    message: str,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert len(c_plan.reused_outputs) == 1
    published_b = c_plan.reused_outputs[0].source_path
    counts_before = _registry_row_counts(runtime_dir)

    # The planner chooses a reusable artifact from the registry, but the file is
    # still ordinary filesystem state. A user, cleanup job, or failed sync could
    # remove or mutate that file between planning and execution — or swap in a
    # symlink whose resolved target leaves outputs/ while staying inside the
    # runtime root. Hydration must re-check the registered path and digest at
    # execution time rather than trusting the earlier plan snapshot.
    if change == "delete":
        published_b.unlink()
    elif change == "mutate":
        published_b.write_text(
            published_b.read_text(encoding="utf-8").replace("alpha", "omega"),
            encoding="utf-8",
        )
    else:
        escaped_target = runtime_dir / "data" / published_b.name
        escaped_target.parent.mkdir(parents=True, exist_ok=True)
        published_b.rename(escaped_target)
        published_b.symlink_to(escaped_target)

    with pytest.raises(ValidationError, match=message):
        execute_run_plan(c_plan, cores=1)
    assert _registry_row_counts(runtime_dir) == counts_before


def test_plan_construction_rejects_reuse_candidate_resolving_outside_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    probe_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert len(probe_plan.reused_outputs) == 1
    published_b = probe_plan.reused_outputs[0].source_path
    counts_before = _registry_row_counts(runtime_dir)

    # Corrupt the published path before planning: the registered string still
    # points lexically inside outputs/, but the symlink resolves to a location
    # elsewhere inside the runtime root. Reuse resolution is fail-closed, so
    # plan construction itself must reject the candidate rather than silently
    # planning against a file outside outputs/.
    escaped_target = runtime_dir / "data" / published_b.name
    escaped_target.parent.mkdir(parents=True, exist_ok=True)
    published_b.rename(escaped_target)
    published_b.symlink_to(escaped_target)

    with pytest.raises(
        ValidationError,
        match="registered reusable artifact path must stay inside outputs/",
    ):
        build_run_plan(
            project_dir=project_dir,
            context="cache",
            workflow_name="main",
            step_name="c_transform",
        )
    assert _registry_row_counts(runtime_dir) == counts_before


def test_symlink_resolving_inside_outputs_remains_reusable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(tmp_path, monkeypatch)
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)

    probe_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert len(probe_plan.reused_outputs) == 1
    published_b = probe_plan.reused_outputs[0].source_path

    # The containment check is about where the path resolves, not whether it is
    # a symlink — mirroring the publication-side rule. A symlink whose resolved
    # target stays inside outputs/ remains a valid reuse candidate.
    relocated_target = published_b.parent / "relocated" / published_b.name
    relocated_target.parent.mkdir(parents=True, exist_ok=True)
    published_b.rename(relocated_target)
    published_b.symlink_to(relocated_target)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert len(c_plan.reused_outputs) == 1
    assert execute_run_plan(c_plan, cores=1).published_count == len(
        c_plan.published_outputs
    )


# ---------------------------------------------------------------------------
# Cross-workflow (sibling) reuse — broadening the reuse candidate set from the
# base chain to every workflow in the context (see
# .local/docs/DEVELOPMENT/20260627-PR-artifact-reuse.md). `main` and
# `derivative` are independent base-style workflows (neither is the other's
# base_workflow), so they exercise the sibling boundary directly.
# ---------------------------------------------------------------------------


def test_sibling_workflow_reuses_upstream_across_base_chain_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    main_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(main_b_plan, cores=1).published_count == len(
        main_b_plan.published_outputs
    )
    main_b_artifact_id = _selected_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )

    # Reuse is artifact-direct and independent of workflow membership, so the
    # derivative hydrates main's compatible published b.
    derivative_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
    )
    assert [ref.step_name for ref in derivative_c_plan.reused_outputs] == [
        "b_transform"
    ]
    assert derivative_c_plan.reused_outputs[0].source_artifact_id == main_b_artifact_id
    assert derivative_c_plan.reused_outputs[0].source_workflow_name == "main"
    assert "b_transform" not in [job.step_name for job in derivative_c_plan.jobs]
    assert execute_run_plan(derivative_c_plan, cores=1).published_count == len(
        derivative_c_plan.published_outputs
    )

    # b was hydrated from main, not recomputed: its runtime side effect (the log
    # line) fired only once, during main's run.
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]
    derivative_c_artifact_id = _selected_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    assert _dependency_source_ids(
        runtime_dir,
        dependent_artifact_id=derivative_c_artifact_id,
    ) == [main_b_artifact_id]


@pytest.mark.parametrize("change", ["params", "callable"])
def test_sibling_does_not_reuse_when_computation_diverges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    main_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(main_b_plan, cores=1).published_count == len(
        main_b_plan.published_outputs
    )

    # The global step `b_transform` changes computation after main published.
    # Exact request-projection equality rejects main's prior b, so b is recomputed;
    # only the unchanged a_source prefix is reused.
    b_step_path = project_dir / "steps/b_transform.yaml"
    b_step = yaml.safe_load(b_step_path.read_text(encoding="utf-8"))
    if change == "params":
        b_step["params"]["version"] = "v2"
        expected_log = ["B sub_001", "B sub_001"]
    else:
        b_step["callable"] = "cache_runtime:step_b_alt_file"
        expected_log = ["B sub_001", "B_ALT sub_001"]
    _write_yaml(b_step_path, b_step)

    derivative_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
    )
    assert [ref.step_name for ref in derivative_c_plan.reused_outputs] == ["a_source"]
    assert derivative_c_plan.reused_outputs[0].source_workflow_name == "main"
    assert "b_transform" in [job.step_name for job in derivative_c_plan.jobs]
    assert execute_run_plan(derivative_c_plan, cores=1).published_count == len(
        derivative_c_plan.published_outputs
    )
    assert log_path.read_text(encoding="utf-8").splitlines() == expected_log


def test_sibling_does_not_reuse_when_source_data_differs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Source identity is a registry fact, not a plan-time file read, and there is
    # exactly one source row per (context, path) — so two siblings can never
    # disagree on source bytes at one instant. A genuine source-data divergence is
    # therefore *temporal*: publish b from bytes A, re-import bytes B (running the
    # source step re-hashes and upserts the single source row), then have a sibling
    # try to reuse the now-stale b. b's input is a_source.a_out (a workflow_output,
    # not a source leaf), so the rejection fires one level up in
    # _workflow_dependency_source_matches_registry_source: b's recorded a_out lineage
    # (alpha digest) mismatches the live re-imported a_out (omega digest), so b is
    # recomputed. (a_source itself reuses cleanly — its source-leaf match against the
    # re-imported omega row succeeds.)
    # See §7.1 of the PR doc: editing the file alone is a no-op against the registry.
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    main_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(main_b_plan, cores=1).published_count == len(
        main_b_plan.published_outputs
    )
    main_a_alpha_id = _workflow_artifact_id(
        runtime_dir,
        workflow_name="main",
        step_name="a_source",
        output_name="a_out",
        address="sub_001",
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        alpha_edge_snapshot = conn.execute(
            """
            SELECT source_content_digest, source_file_size, source_extension
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
            """,
            (main_a_alpha_id,),
        ).fetchone()
        alpha_source_digest = conn.execute(
            """
            SELECT content_digest
            FROM artifacts
            WHERE origin = 'source' AND path = 'data/source/sub_001.txt'
            """
        ).fetchone()[0]

    # Re-import: new bytes on the same path, then run the source step so the registry
    # source row is re-hashed to the new digest. main's published b is now stale.
    (runtime_dir / "data/source/sub_001.txt").write_text("omega\n", encoding="utf-8")
    main_a_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="a_source",
    )
    assert execute_run_plan(main_a_plan, cores=1).published_count == len(
        main_a_plan.published_outputs
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute(
            """
            SELECT source_content_digest, source_file_size, source_extension
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
            """,
            (main_a_alpha_id,),
        ).fetchone() == alpha_edge_snapshot
        assert conn.execute(
            """
            SELECT content_digest
            FROM artifacts
            WHERE origin = 'source' AND path = 'data/source/sub_001.txt'
            """
        ).fetchone()[0] != alpha_source_digest
    main_a_omega_id = _workflow_artifact_id(
        runtime_dir,
        workflow_name="main",
        step_name="a_source",
        output_name="a_out",
        address="sub_001",
    )

    derivative_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
    )
    # a_source reuses main's re-imported (omega) artifact; b is rejected because its
    # recorded lineage descends from the old (alpha) source, so it recomputes.
    assert [ref.step_name for ref in derivative_c_plan.reused_outputs] == ["a_source"]
    assert derivative_c_plan.reused_outputs[0].source_workflow_name == "main"
    assert derivative_c_plan.reused_outputs[0].source_artifact_id == main_a_omega_id
    job_steps = {job.step_name for job in derivative_c_plan.jobs}
    assert "b_transform" in job_steps
    assert "a_source" not in job_steps
    assert execute_run_plan(derivative_c_plan, cores=1).published_count == len(
        derivative_c_plan.published_outputs
    )
    assert log_path.read_text(encoding="utf-8").splitlines() == [
        "B sub_001",
        "B sub_001",
    ]
    assert _latest_workflow_payload(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )["value"] == "omega-b-c"


def test_sibling_does_not_reuse_downstream_when_midchain_lineage_differs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The soundness core of broadening the candidate set: a step whose own
    # callable+params are byte-identical across the sibling boundary must still be
    # rejected when a *mid-chain* upstream differs in content. Selecting d makes c
    # a non-selected upstream so its reuse is actually attempted.
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    _write_sibling_workflow(
        project_dir,
        workflow_name="sib_full",
        step_names=["a_source", "b_transform", "c_transform", "d_transform"],
    )
    main_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert execute_run_plan(main_c_plan, cores=1).published_count == len(
        main_c_plan.published_outputs
    )
    main_a_artifact_id = _workflow_artifact_id(
        runtime_dir,
        workflow_name="main",
        step_name="a_source",
        output_name="a_out",
        address="sub_001",
    )

    # b diverges in content (new param), but c's own step declaration is
    # untouched. c therefore matches main's c at the top SELECT yet must be
    # rejected by the recursive _workflow_dependency_matches_input check.
    b_step_path = project_dir / "steps/b_transform.yaml"
    b_step = yaml.safe_load(b_step_path.read_text(encoding="utf-8"))
    b_step["params"]["version"] = "v2"
    _write_yaml(b_step_path, b_step)

    sib_d_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="sib_full",
        step_name="d_transform",
    )
    reused_steps = [ref.step_name for ref in sib_d_plan.reused_outputs]
    assert reused_steps == ["a_source"]
    assert sib_d_plan.reused_outputs[0].source_artifact_id == main_a_artifact_id
    assert sib_d_plan.reused_outputs[0].source_workflow_name == "main"
    assert {"b_transform", "c_transform", "d_transform"} <= {
        job.step_name for job in sib_d_plan.jobs
    }
    assert execute_run_plan(sib_d_plan, cores=1).published_count == len(
        sib_d_plan.published_outputs
    )


def test_base_chain_ancestor_wins_tie_break_over_sibling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Base-chain regression + tie-break: when both an ancestor and a sibling hold
    # a byte-identical artifact, the ancestor wins. This locks the §5 "strictly
    # additive" ordering — base chain first, then sorted siblings.
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    _write_workflow_variant(project_dir, workflow_name="child", base_workflow="main")
    _write_sibling_workflow(
        project_dir,
        workflow_name="sib",
        step_names=["a_source", "b_transform", "c_transform"],
    )

    for workflow_name in ("main", "sib"):
        b_plan = build_run_plan(
            project_dir=project_dir,
            context="cache",
            workflow_name=workflow_name,
            step_name="b_transform",
        )
        assert execute_run_plan(b_plan, cores=1).published_count == len(
            b_plan.published_outputs
        )
    main_b_artifact_id = _workflow_artifact_id(
        runtime_dir,
        workflow_name="main",
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )

    # child's chain is (child, main); sib is a sorted-fallback sibling. main's b
    # matches before sib's, so the ancestor wins even though sib published last.
    child_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="child",
        step_name="c_transform",
    )
    assert [ref.step_name for ref in child_c_plan.reused_outputs] == ["b_transform"]
    assert child_c_plan.reused_outputs[0].source_workflow_name == "main"
    assert child_c_plan.reused_outputs[0].source_artifact_id == main_b_artifact_id
    assert execute_run_plan(child_c_plan, cores=1).published_count == len(
        child_c_plan.published_outputs
    )


def test_sibling_cohort_collection_mixes_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # §6.1: a manifest-bound (cohort) step collects one upstream job per entity,
    # and each entity resolves its reuse source independently. Under whole-context
    # reuse, sub_001's b can come from one workflow and sub_002's from another in
    # the *same* fit run; each entity artifact is content+lineage validated on its
    # own, so the mixed collection is sound. (Scope, per §7.1: cohort fit_out
    # *output* reuse is not exercised here — fit_transform is terminal in this
    # fixture and a selected step never reuses, so fit_out can never be a
    # non-selected upstream. The manifest_digest/edge_cardinality gate is covered
    # at entity granularity by
    # test_expanded_manifest_reuses_unchanged_entity_and_computes_new_entity.)
    project_dir, runtime_dir, log_path = _write_cache_project(tmp_path, monkeypatch)
    _write_sibling_workflow(
        project_dir,
        workflow_name="cohort_sib",
        step_names=["a_source", "b_transform", "fit_transform"],
    )

    # main publishes b for sub_001 only (manifest is {sub_001}).
    main_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(main_b_plan, cores=1).published_count == len(
        main_b_plan.published_outputs
    )
    main_b_001 = _workflow_artifact_id(
        runtime_dir,
        workflow_name="main",
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )

    # Add sub_002 and let the sibling compute b for both entities (b is its
    # selected step, so both are recomputed and published under cohort_sib).
    _add_cache_entity(project_dir, runtime_dir, address="sub_002", seed="beta")
    cohort_sib_b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="cohort_sib",
        step_name="b_transform",
    )
    assert execute_run_plan(cohort_sib_b_plan, cores=1).published_count == len(
        cohort_sib_b_plan.published_outputs
    )
    sib_b_002 = _workflow_artifact_id(
        runtime_dir,
        workflow_name="cohort_sib",
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )

    # main's fit over {sub_001, sub_002} collects a mixed-source cohort:
    # sub_001's oldest valid b is main's, while sub_002's only candidate is the
    # sibling's. fit itself recomputes because no compatible fit bundle exists.
    main_fit_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="fit_transform",
    )
    reused_b_source = {
        ref.address: ref.source_workflow_name
        for ref in main_fit_plan.reused_outputs
        if ref.step_name == "b_transform"
    }
    reused_b_id = {
        ref.address: ref.source_artifact_id
        for ref in main_fit_plan.reused_outputs
        if ref.step_name == "b_transform"
    }
    assert reused_b_source == {"sub_001": "main", "sub_002": "cohort_sib"}
    assert reused_b_id == {"sub_001": main_b_001, "sub_002": sib_b_002}
    assert "fit_transform" in [job.step_name for job in main_fit_plan.jobs]
    assert execute_run_plan(main_fit_plan, cores=1).published_count == len(
        main_fit_plan.published_outputs
    )

    # The recomputed cohort output reflects both entities, drawn from the mixed but
    # content-validated inputs (cohort address is the manifest name, "subjects").
    fit_payload = _latest_workflow_payload(
        runtime_dir,
        step_name="fit_transform",
        output_name="fit_out",
        address="subjects",
    )
    assert fit_payload["count"] == 2
    assert fit_payload["values"] == ["alpha-b", "beta-b"]


def test_targeted_selected_job_executes_despite_registered_equivalent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    full_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(full_plan, cores=1).published_count == len(
        full_plan.published_outputs
    )
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert sorted(lines) == ["B sub_001", "B sub_002"]

    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    # The selected key skips reuse resolution: sub_001's job stays fresh even
    # though an equivalent registered output exists, while sub_002's job is
    # resolved as reusable and dropped from the compiled job set.
    job_keys = [(job.step_name, job.address) for job in targeted_plan.jobs]
    assert ("b_transform", "sub_001") in job_keys
    assert ("b_transform", "sub_002") not in job_keys
    assert execute_run_plan(targeted_plan, cores=1).all_selected_published
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("B sub_001") == 2
    assert lines.count("B sub_002") == 1


def test_targeted_run_reuses_valid_upstream_for_selected_entity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        address="sub_001",
    )
    assert ("b_transform", "b_out", "sub_001") in _reused_keys(c_plan)
    reused_b = next(
        ref for ref in c_plan.reused_outputs if ref.address == "sub_001"
    )
    assert reused_b.source_artifact_id == _latest_workflow_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    assert execute_run_plan(c_plan, cores=1).all_selected_published
    # b was hydrated from the registry, not recomputed.
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("B sub_001") == 1
    assert (c_plan.run_workspace / "staging/b_transform/b_out/sub_001.json").is_file()
    c_payload = _latest_workflow_payload(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    assert c_payload == {"address": "sub_001", "value": "alpha-b-c"}


def test_targeted_run_excludes_reuse_needed_only_by_unreachable_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )

    # sub_002's registered b is consumed only by fresh jobs outside the
    # selected target's reachable closure (its own c, the fit fan-in, multi),
    # so it must not be hydrated by a targeted sub_001 run.
    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        address="sub_001",
    )
    assert _reused_keys(c_plan) == {("b_transform", "b_out", "sub_001")}
    assert execute_run_plan(c_plan, cores=1).all_selected_published
    payload = json.loads(
        (c_plan.run_workspace / "run_plan.json").read_text(encoding="utf-8")
    )
    assert [entry["address"] for entry in payload["reused_outputs"]] == ["sub_001"]
    staging = c_plan.run_workspace / "staging"
    assert (staging / "b_transform/b_out/sub_001.json").is_file()
    assert not (staging / "b_transform/b_out/sub_002.json").exists()
    assert not (staging / "c_transform/c_out/sub_002.json").exists()


def test_targeted_run_unaffected_by_same_size_corruption_outside_closure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )

    # Same-size corruption passes the plan-time existence+size check, so this
    # artifact remains a resolvable reuse candidate; only closure-scoped
    # hydration keeps it from failing the targeted run at digest validation.
    corrupt_path = runtime_dir / _latest_registered_path(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )
    original = corrupt_path.read_bytes()
    corrupt_path.write_bytes(b"#" + original[1:])

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        address="sub_001",
    )
    assert ("b_transform", "b_out", "sub_002") not in _reused_keys(c_plan)
    assert execute_run_plan(c_plan, cores=1).all_selected_published
    c_payload = _latest_workflow_payload(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    assert c_payload == {"address": "sub_001", "value": "alpha-b-c"}


def test_targeted_plan_construction_still_fails_on_unrelated_missing_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    # Accepted plan-time coupling: job construction stays population-wide, so a
    # missing source for unselected sub_002 aborts a targeted sub_001 plan.
    (runtime_dir / "data/source/sub_002.txt").unlink()
    with pytest.raises(ValidationError, match="missing source artifact"):
        build_run_plan(
            project_dir=project_dir,
            context="cache",
            workflow_name="main",
            step_name="b_transform",
            address="sub_001",
        )


def test_targeted_run_does_not_execute_or_republish_sibling_entity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    full_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(full_plan, cores=1).published_count == len(
        full_plan.published_outputs
    )
    sibling_artifact_id = _latest_workflow_artifact_id(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )
    sibling_path = runtime_dir / _latest_registered_path(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )
    sibling_digest = sha256_file_digest(sibling_path)

    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert {
        (spec.step_name, spec.output_name, spec.address)
        for spec in targeted_plan.published_outputs
    } == {("b_transform", "b_out", "sub_001")}
    assert execute_run_plan(targeted_plan, cores=1).all_selected_published
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("B sub_002") == 1
    assert (
        _latest_workflow_artifact_id(
            runtime_dir,
            step_name="b_transform",
            output_name="b_out",
            address="sub_002",
        )
        == sibling_artifact_id
    )
    assert sha256_file_digest(sibling_path) == sibling_digest


def test_targeted_apply_reuses_registered_cohort_fit_without_executing_sibling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    _write_sibling_workflow(
        project_dir,
        workflow_name="apply_flow",
        step_names=["a_source", "b_transform", "fit_transform", "apply_transform"],
    )
    fit_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="fit_transform",
    )
    assert execute_run_plan(fit_plan, cores=1).published_count == len(
        fit_plan.published_outputs
    )
    assert log_path.read_text(encoding="utf-8").count("FIT subjects 2") == 1

    apply_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="apply_transform",
        address="sub_001",
    )
    # The registered cohort fit is reused, so the closure stops at the selected
    # apply job: only its direct inputs hydrate, and sub_002 never executes.
    assert _reused_keys(apply_plan) == {
        ("b_transform", "b_out", "sub_001"),
        ("fit_transform", "fit_out", "subjects"),
    }
    assert execute_run_plan(apply_plan, cores=1).all_selected_published
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("B sub_001") == 1
    assert lines.count("B sub_002") == 1
    assert lines.count("FIT subjects 2") == 1
    assert lines.count("APPLY sub_001") == 1
    assert "APPLY sub_002" not in lines
    apply_payload = _latest_workflow_payload(
        runtime_dir,
        step_name="apply_transform",
        output_name="apply_out",
        address="sub_001",
    )
    assert apply_payload == {
        "address": "sub_001",
        "fit_count": 2,
        "value": "alpha-b-apply",
    }


def test_targeted_apply_dry_run_maps_registered_cohort_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    _write_sibling_workflow(
        project_dir,
        workflow_name="apply_flow",
        step_names=["a_source", "b_transform", "fit_transform", "apply_transform"],
    )
    fit_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="fit_transform",
    )
    assert execute_run_plan(fit_plan, cores=1).published_count == len(
        fit_plan.published_outputs
    )
    log_before = log_path.read_text(encoding="utf-8")
    counts_before = _registry_row_counts(runtime_dir)

    apply_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="apply_transform",
        address="sub_001",
        dry_run=True,
    )
    assert _reused_keys(apply_plan) == {
        ("b_transform", "b_out", "sub_001"),
        ("fit_transform", "fit_out", "subjects"),
    }
    assert execute_run_plan(apply_plan, cores=1).published_count == 0

    snakefile_text = (apply_plan.run_workspace / "Snakefile").read_text(encoding="utf-8")
    mapped_by_key = {
        (output_ref.step_name, output_ref.output_name): os.path.relpath(
            runtime_dir / output_ref.source_path_relative,
            apply_plan.run_workspace,
        ).replace(os.sep, "/")
        for output_ref in apply_plan.reused_outputs
    }
    assert json.dumps(mapped_by_key[("b_transform", "b_out")]) in snakefile_text
    # The cohort fit fans out: one mapping entry keyed by one reused ref, and
    # the substitution rewrites every consuming rule's input line — both apply
    # rules reference the same registered source.
    assert snakefile_text.count(
        json.dumps(mapped_by_key[("fit_transform", "fit_out")])
    ) == 2
    assert list((apply_plan.run_workspace / "staging").rglob("*")) == []
    assert log_path.read_text(encoding="utf-8") == log_before
    assert "APPLY" not in log_path.read_text(encoding="utf-8")
    assert _registry_row_counts(runtime_dir) == counts_before


def test_targeted_apply_executes_fresh_cohort_ancestor_with_population_fan_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    _write_sibling_workflow(
        project_dir,
        workflow_name="apply_flow",
        step_names=["a_source", "b_transform", "fit_transform", "apply_transform"],
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(
        b_plan.published_outputs
    )

    apply_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="apply_transform",
        address="sub_001",
    )
    # No reusable fit exists, so the fresh cohort ancestor joins the reachable
    # closure and its population fan-in pulls sub_002's registered b back into
    # the hydration set (reachability follows dependency records, not address).
    assert _reused_keys(apply_plan) == {
        ("b_transform", "b_out", "sub_001"),
        ("b_transform", "b_out", "sub_002"),
    }
    assert {
        (spec.step_name, spec.output_name, spec.address)
        for spec in apply_plan.published_outputs
    } == {
        ("apply_transform", "apply_out", "sub_001"),
        ("fit_transform", "fit_out", "subjects"),
    }
    assert execute_run_plan(apply_plan, cores=1).all_selected_published
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("B sub_001") == 1
    assert lines.count("B sub_002") == 1
    assert lines.count("FIT subjects 2") == 1
    assert lines.count("APPLY sub_001") == 1
    assert "APPLY sub_002" not in lines
    fit_payload = _latest_workflow_payload(
        runtime_dir,
        step_name="fit_transform",
        output_name="fit_out",
        address="subjects",
    )
    assert fit_payload["count"] == 2
    assert fit_payload["values"] == ["alpha-b", "beta-b"]


def test_targeted_apply_with_empty_registry_executes_fresh_population_fan_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    _write_sibling_workflow(
        project_dir,
        workflow_name="apply_flow",
        step_names=["a_source", "b_transform", "fit_transform", "apply_transform"],
    )

    apply_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="apply_flow",
        step_name="apply_transform",
        address="sub_001",
    )
    # Nothing is registered, so nothing is reusable: the fresh cohort fit joins
    # the reachable closure and pulls sub_002's fresh upstream jobs with it —
    # unlike the hydrated variant above, sub_002's chain must actually execute.
    assert _reused_keys(apply_plan) == set()
    expected_coordinates = {
        ("a_source", "a_out", "sub_001"),
        ("a_source", "a_out", "sub_002"),
        ("b_transform", "b_out", "sub_001"),
        ("b_transform", "b_out", "sub_002"),
        ("fit_transform", "fit_out", "subjects"),
        ("apply_transform", "apply_out", "sub_001"),
    }
    assert {
        (spec.step_name, spec.output_name, spec.address)
        for spec in apply_plan.published_outputs
    } == expected_coordinates

    outcome = execute_run_plan(apply_plan, cores=1)
    assert outcome.all_selected_published
    assert outcome.published_count == len(expected_coordinates)
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("B sub_001") == 1
    assert lines.count("B sub_002") == 1
    assert lines.count("FIT subjects 2") == 1
    assert lines.count("APPLY sub_001") == 1
    assert "APPLY sub_002" not in lines
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published = conn.execute(
            """
            SELECT step_name, output_name, address
            FROM published_outputs
            WHERE context = 'cache'
            """
        ).fetchall()
    assert {
        (str(row[0]), str(row[1]), str(row[2])) for row in published
    } == expected_coordinates
    fit_payload = _latest_workflow_payload(
        runtime_dir,
        step_name="fit_transform",
        output_name="fit_out",
        address="subjects",
    )
    assert fit_payload["count"] == 2
    assert fit_payload["values"] == ["alpha-b", "beta-b"]


def test_real_snakemake_targeted_command_receives_one_selected_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    commands: list[list[str]] = []
    real_run = subprocess.run

    def spy_run(command: list[str], **kwargs: object) -> object:
        commands.append(list(command))
        return real_run(command, **kwargs)

    monkeypatch.setattr("nipact.execution.subprocess.run", spy_run)

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(run_plan, cores=1).all_selected_published

    # The real Snakemake command names exactly one selected target path.
    (command,) = commands
    targets = [arg for arg in command if arg.startswith("staging/")]
    assert targets == ["staging/b_transform/b_out/sub_001.json"]
    # Snakemake resolved that target's reachable closure and nothing else.
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]
    staging = run_plan.run_workspace / "staging"
    assert (staging / "b_transform/b_out/sub_001.json").is_file()
    assert not (staging / "a_source/a_out/sub_002.json").exists()
    assert not (staging / "b_transform/b_out/sub_002.json").exists()
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published = conn.execute(
            "SELECT step_name, output_name, address FROM published_outputs "
            "ORDER BY step_name, output_name, address"
        ).fetchall()
    assert published == [
        ("a_source", "a_out", "sub_001"),
        ("b_transform", "b_out", "sub_001"),
    ]


def test_targeted_rerun_replaces_only_selected_coordinates_and_keeps_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    full_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(full_plan, cores=1).published_count == len(
        full_plan.published_outputs
    )
    selected_before = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    sibling_before = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )

    # A changed step parameter is identity-bearing and changes the output
    # bytes, so the targeted rerun publishes a new sub_001 artifact. (A source
    # data change would not do this: plan-time reuse never hashes content, so
    # the upstream import would be reused with its registered old bytes.)
    step_path = project_dir / "steps/b_transform.yaml"
    step_payload = yaml.safe_load(step_path.read_text(encoding="utf-8"))
    step_payload["params"]["version"] = "v2"
    _write_yaml(step_path, step_payload)
    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(targeted_plan, cores=1).all_selected_published

    selected_after = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    assert selected_after[0] != selected_before[0]
    assert selected_after[1] != selected_before[1]
    # The sibling coordinate outside the reachable closure is untouched.
    assert (
        _published_output_row(
            runtime_dir,
            step_name="b_transform",
            output_name="b_out",
            address="sub_002",
        )
        == sibling_before
    )
    # The former artifact row and file remain as historical provenance.
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        historical = conn.execute(
            "SELECT path FROM artifacts WHERE artifact_id = ?",
            (selected_before[0],),
        ).fetchone()
    assert historical == (selected_before[1],)
    assert (runtime_dir / selected_before[1]).is_file()


def test_targeted_rerun_with_identical_bytes_revalidates_published_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    full_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(full_plan, cores=1).published_count == len(
        full_plan.published_outputs
    )
    output_dir = runtime_dir / "outputs/cache/main/b_transform/b_out"
    files_before = sorted(path.name for path in output_dir.glob("*.json"))
    row_before = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    digest_before = sha256_file_digest(runtime_dir / row_before[1])
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        projection_before = conn.execute(
            "SELECT request_bundle_projection_json FROM artifacts WHERE artifact_id = ?",
            (row_before[0],),
        ).fetchone()[0]

    # Unchanged inputs: the forced-fresh selected job recomputes identical
    # bytes, so publication validates the existing content-addressed file.
    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(targeted_plan, cores=1).all_selected_published

    row_after = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    assert row_after[1] == row_before[1]
    assert row_after[0] != row_before[0]
    assert sorted(path.name for path in output_dir.glob("*.json")) == files_before
    assert sha256_file_digest(runtime_dir / row_after[1]) == digest_before
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 2
        projection_after, current_run_id = conn.execute(
            """
            SELECT a.request_bundle_projection_json, wr.run_id
            FROM artifacts AS a
            JOIN workflow_runs AS wr ON wr.run_id = a.run_id
            WHERE a.artifact_id = ? AND wr.is_current = 1
            """,
            (row_after[0],),
        ).fetchone()
    assert projection_after == projection_before
    assert current_run_id == _artifact_run_id(
        runtime_dir,
        artifact_id=row_after[0],
    )


def test_path_reads_deduplicate_multiple_memberships_for_one_current_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published
    derivative_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
        address="sub_001",
    )
    assert execute_run_plan(derivative_plan, cores=1).all_selected_published

    rerun_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(rerun_plan, cores=1).all_selected_published
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        current_id, shared_path = conn.execute(
            """
            SELECT artifact_id, path
            FROM published_outputs
            WHERE context = 'cache'
              AND workflow_name = 'main'
              AND step_name = 'b_transform'
              AND output_name = 'b_out'
              AND address = 'sub_001'
            """
        ).fetchone()
        assert conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE context = 'cache' AND path = ?",
            (shared_path,),
        ).fetchone() == (2,)

    registry_path = runtime_dir / "database/registry.db"
    with pytest.raises(ValidationError, match="ambiguous registered artifact path"):
        registry_module.read_artifact_by_path(
            registry_path,
            context="cache",
            artifact_path=shared_path,
        )
    with pytest.raises(ValidationError, match="ambiguous registered artifact path"):
        registry_module.resolve_registered_artifact_path(
            registry_path,
            context="cache",
            artifact_path=shared_path,
        )

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        # Point both workflow memberships at the newer byte-identical entity.
        # The older historical entity deliberately retains the same path.
        conn.execute(
            """
            UPDATE published_outputs
            SET artifact_id = ?
            WHERE context = 'cache'
              AND workflow_name = 'derivative'
              AND step_name = 'b_transform'
              AND output_name = 'b_out'
              AND address = 'sub_001'
            """,
            (current_id,),
        )

    assert registry_module.read_artifact_by_path(
        registry_path,
        context="cache",
        artifact_path=shared_path,
    ).artifact_id == current_id
    assert registry_module.resolve_registered_artifact_path(
        registry_path,
        context="cache",
        artifact_path=shared_path,
    ).artifact_id == current_id


def test_membership_write_failure_restores_registry_and_removes_new_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published
    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        counts_before = _registry_row_counts(runtime_dir)
        memberships_before = conn.execute(
            """
            SELECT context, workflow_name, step_name, output_name, address,
                   artifact_id
            FROM published_outputs
            ORDER BY context, workflow_name, step_name, output_name, address
            """
        ).fetchall()
        current_runs_before = conn.execute(
            """
            SELECT run_id
            FROM workflow_runs
            WHERE is_current = 1
            ORDER BY run_id
            """
        ).fetchall()
    output_files_before = {
        path.relative_to(runtime_dir).as_posix()
        for path in (runtime_dir / "outputs").rglob("*")
        if path.is_file()
    }

    step_path = project_dir / "steps/b_transform.yaml"
    step = yaml.safe_load(step_path.read_text(encoding="utf-8"))
    step["params"]["version"] = "v2"
    _write_yaml(step_path, step)
    changed_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )

    def fail_membership_insert(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.IntegrityError("synthetic membership insert failure")

    monkeypatch.setattr(
        registry_module,
        "_insert_memberships",
        fail_membership_insert,
    )
    with pytest.raises(ValidationError, match="synthetic membership insert failure"):
        execute_run_plan(changed_plan, cores=1)

    with sqlite3.connect(registry_path) as conn:
        assert _registry_row_counts(runtime_dir) == counts_before
        assert conn.execute(
            """
            SELECT context, workflow_name, step_name, output_name, address,
                   artifact_id
            FROM published_outputs
            ORDER BY context, workflow_name, step_name, output_name, address
            """
        ).fetchall() == memberships_before
        assert conn.execute(
            """
            SELECT run_id
            FROM workflow_runs
            WHERE is_current = 1
            ORDER BY run_id
            """
        ).fetchall() == current_runs_before
    assert {
        path.relative_to(runtime_dir).as_posix()
        for path in (runtime_dir / "outputs").rglob("*")
        if path.is_file()
    } == output_files_before


def test_bundle_reresolution_records_the_equivalent_artifact_actually_consumed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published

    first_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        address="sub_001",
    )
    older_id = first_c_plan.reused_outputs[0].source_artifact_id

    # Insert an equivalent bundle after planning. Execution must preserve the
    # still-valid planned bundle rather than silently changing retrospective
    # lineage to the newly inserted entity.
    second_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(second_plan, cores=1).all_selected_published

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        artifact_ids = [
            int(row[0])
            for row in conn.execute(
                """
                SELECT artifact_id
                FROM artifacts
                WHERE context = 'cache'
                  AND step_name = 'b_transform'
                  AND output_name = 'b_out'
                  AND address = 'sub_001'
                ORDER BY run_id
                """
            ).fetchall()
        ]
    assert len(artifact_ids) == 2
    assert artifact_ids[0] == older_id
    newer_id = artifact_ids[1]

    assert first_c_plan.reused_outputs[0].source_artifact_id == older_id
    assert execute_run_plan(first_c_plan, cores=1).all_selected_published
    first_c_id = _latest_workflow_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute(
            """
            SELECT source_artifact_id
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
              AND source_step_name = 'b_transform'
            """,
            (first_c_id,),
        ).fetchone() == (older_id,)

    second_c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="derivative",
        step_name="c_transform",
        address="sub_001",
    )
    assert second_c_plan.reused_outputs[0].source_artifact_id == older_id
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            """
            UPDATE artifacts
            SET path = ?, published_path = ?
            WHERE artifact_id = ?
            """,
            (
                "outputs/cache/main/b_transform/b_out/missing.json",
                "outputs/cache/main/b_transform/b_out/missing.json",
                older_id,
            ),
        )

    assert execute_run_plan(second_c_plan, cores=1).all_selected_published
    second_c_id = _latest_workflow_artifact_id(
        runtime_dir,
        step_name="c_transform",
        output_name="c_out",
        address="sub_001",
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute(
            """
            SELECT source_artifact_id
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
              AND source_step_name = 'b_transform'
            """,
            (second_c_id,),
        ).fetchone() == (newer_id,)
        assert conn.execute(
            """
            SELECT artifact_id
            FROM published_outputs
            WHERE context = 'cache'
              AND workflow_name = 'derivative'
              AND step_name = 'b_transform'
              AND output_name = 'b_out'
              AND address = 'sub_001'
            """
        ).fetchone() == (newer_id,)


def test_multi_output_substitution_records_all_siblings_and_transitive_membership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    main_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )
    assert execute_run_plan(main_plan, cores=1).all_selected_published
    _write_sibling_workflow(
        project_dir,
        workflow_name="multi_derivative",
        step_names=[
            "a_source",
            "b_transform",
            "multi_transform",
            "use_multi",
        ],
    )
    derivative_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="multi_derivative",
        step_name="use_multi",
        address="sub_001",
    )
    planned_multi = next(
        ref
        for ref in derivative_plan.reused_validation_outputs
        if ref.step_name == "multi_transform"
    )
    planned_b = next(
        ref
        for ref in derivative_plan.reused_validation_outputs
        if ref.step_name == "b_transform"
    )

    rerun_b = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(rerun_b, cores=1).all_selected_published
    rerun_multi = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )
    assert execute_run_plan(rerun_multi, cores=1).all_selected_published

    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        new_b_id = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE step_name = 'b_transform'
              AND output_name = 'b_out'
              AND address = 'sub_001'
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()[0]
        new_multi = dict(
            conn.execute(
                """
                SELECT output_name, artifact_id
                FROM artifacts
                WHERE step_name = 'multi_transform' AND address = 'sub_001'
                  AND run_id = (
                    SELECT MAX(run_id)
                    FROM artifacts
                    WHERE step_name = 'multi_transform' AND address = 'sub_001'
                  )
                """
            ).fetchall()
        )
        for artifact_id in (
            *planned_multi.source_bundle_artifact_ids,
            *planned_b.source_bundle_artifact_ids,
        ):
            conn.execute(
                """
                UPDATE artifacts
                SET path = ?, published_path = ?
                WHERE artifact_id = ?
                """,
                (
                    f"outputs/cache/main/missing/{artifact_id}.json",
                    f"outputs/cache/main/missing/{artifact_id}.json",
                    artifact_id,
                ),
            )

    assert execute_run_plan(derivative_plan, cores=1).all_selected_published
    use_artifact_id = _latest_workflow_artifact_id(
        runtime_dir,
        step_name="use_multi",
        output_name="multi_used",
        address="sub_001",
    )
    with sqlite3.connect(registry_path) as conn:
        memberships = dict(
            conn.execute(
                """
                SELECT step_name || '.' || output_name, artifact_id
                FROM published_outputs
                WHERE context = 'cache'
                  AND workflow_name = 'multi_derivative'
                  AND address = 'sub_001'
                  AND step_name IN ('b_transform', 'multi_transform')
                """
            ).fetchall()
        )
        dependency_id = conn.execute(
            """
            SELECT source_artifact_id
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
              AND source_step_name = 'multi_transform'
            """,
            (use_artifact_id,),
        ).fetchone()[0]
    assert memberships == {
        "b_transform.b_out": new_b_id,
        "multi_transform.left_out": new_multi["left_out"],
        "multi_transform.right_out": new_multi["right_out"],
    }
    assert dependency_id == new_multi["left_out"]
    assert {
        _artifact_run_id(runtime_dir, artifact_id=int(artifact_id))
        for key, artifact_id in memberships.items()
        if key.startswith("multi_transform.")
    } == {
        _artifact_run_id(
            runtime_dir,
            artifact_id=int(new_multi["left_out"]),
        )
    }


def test_bundle_resolver_reports_recorded_divergence_before_missing_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    for _ in range(2):
        plan = build_run_plan(
            project_dir=project_dir,
            context="cache",
            workflow_name="main",
            step_name="b_transform",
            address="sub_001",
        )
        assert execute_run_plan(plan, cores=1).all_selected_published

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        rows = conn.execute(
            """
            SELECT artifact_id, path
            FROM artifacts
            WHERE context = 'cache'
              AND step_name = 'b_transform'
              AND output_name = 'b_out'
              AND address = 'sub_001'
            ORDER BY run_id
            """
        ).fetchall()
        assert len(rows) == 2
        conn.execute(
            "UPDATE artifacts SET content_digest = ? WHERE artifact_id = ?",
            ("f" * 64, int(rows[1][0])),
        )
    (runtime_dir / str(rows[0][1])).unlink()

    with pytest.raises(
        ValidationError,
        match="divergent reusable artifact bundles",
    ):
        build_run_plan(
            project_dir=project_dir,
            context="cache",
            workflow_name="main",
            step_name="c_transform",
            address="sub_001",
        )


def test_execution_rechecks_divergence_in_transitive_reused_closure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(b_plan, cores=1).all_selected_published
    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        address="sub_001",
    )
    assert [ref.step_name for ref in c_plan.reused_outputs] == ["b_transform"]
    assert {ref.step_name for ref in c_plan.reused_validation_outputs} == {
        "a_source",
        "b_transform",
    }

    # A is not copied into C's workspace because B is the reuse boundary, but
    # its requested bundle remains part of the transitive validation closure.
    second_a_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="a_source",
        address="sub_001",
    )
    assert execute_run_plan(second_a_plan, cores=1).all_selected_published
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        newest_a_id = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE context = 'cache'
              AND step_name = 'a_source'
              AND output_name = 'a_out'
              AND address = 'sub_001'
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()[0]
        conn.execute(
            "UPDATE artifacts SET content_digest = ? WHERE artifact_id = ?",
            ("f" * 64, int(newest_a_id)),
        )

    calls: list[bool] = []
    monkeypatch.setattr(
        "nipact.execution._run_snakemake",
        lambda *_args, **_kwargs: calls.append(True) or 0,
    )
    with pytest.raises(
        ValidationError,
        match="divergent reusable artifact bundles",
    ):
        execute_run_plan(c_plan, cores=1)
    assert calls == []


def test_targeted_rerun_rejects_divergent_deterministic_output_and_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published

    row_before = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    historical_run_id = _artifact_run_id(runtime_dir, artifact_id=row_before[0])
    historical_path = runtime_dir / row_before[1]
    historical_bytes = historical_path.read_bytes()
    output_dir = historical_path.parent
    files_before = sorted(path.name for path in output_dir.glob("*.json"))
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        counts_before = {
            "runs": conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            "artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
            "memberships": conn.execute(
                "SELECT COUNT(*) FROM published_outputs"
            ).fetchone()[0],
        }
        current_run_before = conn.execute(
            "SELECT run_id FROM workflow_runs WHERE is_current = 1"
        ).fetchone()[0]

    rerun_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )

    def write_divergent_output(
        _run_plan: object,
        *,
        cores: int,
        dry_run: bool,
    ) -> int:
        output = next(
            job.outputs["b_out"]
            for job in rerun_plan.jobs
            if job.step_name == "b_transform"
        )
        output.staging_path.parent.mkdir(parents=True, exist_ok=True)
        output.staging_path.write_text(
            json.dumps({"address": "sub_001", "value": "divergent"}) + "\n",
            encoding="utf-8",
        )
        (rerun_plan.run_workspace / "logs/snakemake.log").write_text(
            "synthetic completed run\n",
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", write_divergent_output)
    with pytest.raises(
        ValidationError,
        match=(
            rf"new run \d+ conflicts with historical run {historical_run_id}, "
            rf"artifacts \({row_before[0]},\)"
        ),
    ):
        execute_run_plan(rerun_plan, cores=1)

    assert sorted(path.name for path in output_dir.glob("*.json")) == files_before
    assert historical_path.read_bytes() == historical_bytes
    divergent_staging = next(
        job.outputs["b_out"].staging_path
        for job in rerun_plan.jobs
        if job.step_name == "b_transform"
    )
    assert divergent_staging.is_file()
    assert divergent_staging.read_bytes() != historical_bytes
    assert (rerun_plan.run_workspace / "run_plan.json").is_file()
    assert (rerun_plan.run_workspace / "Snakefile").is_file()
    assert (rerun_plan.run_workspace / "logs/snakemake.log").is_file()
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert {
            "runs": conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            "artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
            "memberships": conn.execute(
                "SELECT COUNT(*) FROM published_outputs"
            ).fetchone()[0],
        } == counts_before
        assert conn.execute(
            "SELECT run_id FROM workflow_runs WHERE is_current = 1"
        ).fetchone()[0] == current_run_before
    assert (
        _published_output_row(
            runtime_dir,
            step_name="b_transform",
            output_name="b_out",
            address="sub_001",
        )
        == row_before
    )


def test_targeted_multi_output_rerun_rejects_one_divergent_sibling_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published

    rows_before = {
        output_name: _published_output_row(
            runtime_dir,
            step_name="multi_transform",
            output_name=output_name,
            address="sub_001",
        )
        for output_name in ("left_out", "right_out")
    }
    files_before = {
        output_name: (runtime_dir / row[1]).read_bytes()
        for output_name, row in rows_before.items()
    }
    directory_entries_before = {
        path.relative_to(runtime_dir).as_posix()
        for output_name in rows_before
        for path in (runtime_dir / rows_before[output_name][1]).parent.glob("*.json")
    }
    counts_before = _registry_row_counts(runtime_dir)

    rerun_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )

    def write_one_divergent_sibling(
        _run_plan: object,
        *,
        cores: int,
        dry_run: bool,
    ) -> int:
        job = next(
            job for job in rerun_plan.jobs if job.step_name == "multi_transform"
        )
        left = job.outputs["left_out"].staging_path
        right = job.outputs["right_out"].staging_path
        left.parent.mkdir(parents=True, exist_ok=True)
        right.parent.mkdir(parents=True, exist_ok=True)
        left.write_bytes(files_before["left_out"])
        right.write_text(
            json.dumps(
                {"address": "sub_001", "side": "right", "value": "divergent"}
            )
            + "\n",
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        "nipact.execution._run_snakemake",
        write_one_divergent_sibling,
    )
    with pytest.raises(
        ValidationError,
        match="deterministic request bundle produced divergent content",
    ):
        execute_run_plan(rerun_plan, cores=1)

    assert _registry_row_counts(runtime_dir) == counts_before
    assert {
        path.relative_to(runtime_dir).as_posix()
        for output_name in rows_before
        for path in (runtime_dir / rows_before[output_name][1]).parent.glob("*.json")
    } == directory_entries_before
    for output_name, row in rows_before.items():
        assert (runtime_dir / row[1]).read_bytes() == files_before[output_name]
        assert (
            _published_output_row(
                runtime_dir,
                step_name="multi_transform",
                output_name=output_name,
                address="sub_001",
            )
            == row
        )


def test_incomplete_historical_bundle_does_not_block_fresh_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            """
            UPDATE artifacts
            SET content_digest = ?, output_hash = ?
            WHERE context = 'cache'
              AND step_name = 'multi_transform'
              AND output_name = 'left_out'
              AND address = 'sub_001'
            """,
            ("f" * 64, "f" * 16),
        )
        conn.execute(
            """
            UPDATE artifacts
            SET is_published = 0
            WHERE context = 'cache'
              AND step_name = 'multi_transform'
              AND output_name = 'right_out'
              AND address = 'sub_001'
            """
        )

    rerun_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
        address="sub_001",
    )
    assert execute_run_plan(rerun_plan, cores=1).all_selected_published
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        rows = conn.execute(
            """
            SELECT output_name, content_digest
            FROM artifacts
            WHERE run_id = (SELECT MAX(run_id) FROM workflow_runs)
              AND step_name = 'multi_transform'
              AND address = 'sub_001'
            ORDER BY output_name
            """
        ).fetchall()
    assert [str(row[0]) for row in rows] == ["left_out", "right_out"]
    assert all(str(row[1]) != "f" * 64 for row in rows)


def test_recorded_divergence_precedes_missing_file_and_reports_first_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001",),
    )
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(first_plan, cores=1).all_selected_published
    second_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(second_plan, cores=1).all_selected_published
    shared_path = runtime_dir / _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )[1]
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        historical = conn.execute(
            """
            SELECT artifact_id, run_id
            FROM artifacts
            WHERE context = 'cache'
              AND step_name = 'b_transform'
              AND output_name = 'b_out'
              AND address = 'sub_001'
            ORDER BY run_id
            """
        ).fetchall()
        assert len(historical) == 2
        for row, digest in zip(historical, ("1" * 64, "2" * 64), strict=True):
            conn.execute(
                """
                UPDATE artifacts
                SET content_digest = ?, output_hash = ?
                WHERE artifact_id = ?
                """,
                (digest, digest[:16], int(row[0])),
            )
    shared_path.unlink()
    rerun_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    with pytest.raises(
        ValidationError,
        match=(
            rf"new run \d+ conflicts with historical run {int(historical[0][1])}, "
            rf"artifacts \({int(historical[0][0])},\)"
        ),
    ):
        execute_run_plan(rerun_plan, cores=1)
    assert not shared_path.exists()


def test_failed_targeted_rerun_preserves_prior_published_coordinate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    plan_one = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(plan_one, cores=1).all_selected_published
    row_before = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    digest_before = sha256_file_digest(runtime_dir / row_before[1])
    staged_output = plan_one.run_workspace / "staging/b_transform/b_out/sub_001.json"
    assert staged_output.is_file()

    plan_two = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert plan_two.run_workspace == plan_one.run_workspace
    stale_staging_seen: list[bool] = []

    def fail_without_writing(
        _run_plan: object,
        *,
        cores: int,
        dry_run: bool,
    ) -> int:
        stale_staging_seen.append(staged_output.exists())
        return 1

    monkeypatch.setattr("nipact.execution._run_snakemake", fail_without_writing)
    with pytest.raises(ValidationError, match="Snakemake failed with exit code 1"):
        execute_run_plan(plan_two, cores=1)

    # Run one's stale staged output was removed before the second execution,
    # so it could not be republished as the failed run's result.
    assert stale_staging_seen == [False]
    assert (
        _published_output_row(
            runtime_dir,
            step_name="b_transform",
            output_name="b_out",
            address="sub_001",
        )
        == row_before
    )
    assert sha256_file_digest(runtime_dir / row_before[1]) == digest_before
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 1


def test_targeted_dry_run_writes_no_outputs_and_no_registry_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
        dry_run=True,
    )
    outcome = execute_run_plan(run_plan, cores=1)

    assert outcome.published_count == 0
    # The targeted dry-run workspace itself may be written (run plan, Snakefile).
    assert run_plan.run_workspace == (
        runtime_dir / "runs/cache/main/b_transform/addresses/sub_001/dry-run"
    )
    assert (run_plan.run_workspace / "run_plan.json").is_file()
    assert not log_path.exists()
    assert list((runtime_dir / "outputs").rglob("*")) == []
    assert _registry_row_counts(runtime_dir) == {
        "workflow_runs": 0,
        "workflow_outputs": 0,
        "dependencies": 0,
    }
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM published_outputs").fetchone()[0] == 0
        )


def test_targeted_dry_run_maps_only_reachable_reused_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    b_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(b_plan, cores=1).published_count == len(b_plan.published_outputs)
    registered_b_path = _latest_registered_path(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_001",
    )
    log_before = log_path.read_text(encoding="utf-8")
    counts_before = _registry_row_counts(runtime_dir)

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
        address="sub_001",
        dry_run=True,
    )
    assert c_plan.run_workspace == (
        runtime_dir / "runs/cache/main/c_transform/addresses/sub_001/dry-run"
    )
    assert _reused_keys(c_plan) == {("b_transform", "b_out", "sub_001")}
    # The retained forecast count matches the Snakemake-verified closure: one
    # fresh c job, while the compiled job set stays population-wide.
    assert c_plan.reachable_job_count == 1
    assert len(c_plan.jobs) > c_plan.reachable_job_count
    assert execute_run_plan(c_plan, cores=1).published_count == 0

    # Only the reachable closure's reused inputs are mapped: sub_002's reused
    # input keeps its (absent) staging path, and the real dry-run DAG still
    # builds because Snakemake never demands the unreachable consumer rule.
    snakefile_text = (c_plan.run_workspace / "Snakefile").read_text(encoding="utf-8")
    mapped_b = os.path.relpath(
        runtime_dir / registered_b_path,
        c_plan.run_workspace,
    ).replace(os.sep, "/")
    assert json.dumps(mapped_b) in snakefile_text
    assert "staging/b_transform/b_out/sub_001.json" not in snakefile_text
    assert '"staging/b_transform/b_out/sub_002.json"' in snakefile_text
    assert list((c_plan.run_workspace / "staging").rglob("*")) == []
    assert log_path.read_text(encoding="utf-8") == log_before
    assert _registry_row_counts(runtime_dir) == counts_before


def test_targeted_run_becomes_current_and_keeps_full_manifest_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _log_path = _write_cache_project(
        tmp_path,
        monkeypatch,
        entities=("sub_001", "sub_002"),
    )
    full_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
    )
    assert execute_run_plan(full_plan, cores=1).published_count == len(
        full_plan.published_outputs
    )

    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="b_transform",
        address="sub_001",
    )
    assert execute_run_plan(targeted_plan, cores=1).all_selected_published

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        runs = conn.execute(
            """
            SELECT run_id, run_workspace, is_current
            FROM workflow_runs
            WHERE context = 'cache'
              AND workflow_name = 'main'
              AND selected_step_name = 'b_transform'
              AND selected_output_name = 'b_out'
            ORDER BY run_id
            """
        ).fetchall()
        binding_rows_by_run = {
            run_id: conn.execute(
                """
                SELECT step_name, role, manifest_name, manifest_digest, entity_count
                FROM run_manifest_bindings
                WHERE run_id = ?
                ORDER BY step_name, role, manifest_name
                """,
                (run_id,),
            ).fetchall()
            for run_id, _workspace, _is_current in runs
        }

    # is_current is scoped without address: the targeted run is the sole
    # current run for the step/output scope even though it selected one entity.
    assert len(runs) == 2
    full_run, targeted_run = runs
    assert full_run[2] == 0
    assert targeted_run[2] == 1
    assert targeted_run[1].endswith("addresses/sub_001")
    # The sibling coordinate still refers to its full-run artifact.
    sibling_row = _published_output_row(
        runtime_dir,
        step_name="b_transform",
        output_name="b_out",
        address="sub_002",
    )
    assert _artifact_run_id(runtime_dir, artifact_id=sibling_row[0]) == full_run[0]
    # The targeted run records the original two-entity source-population
    # binding, not a synthetic one-entity manifest.
    targeted_bindings = binding_rows_by_run[targeted_run[0]]
    assert targeted_bindings == binding_rows_by_run[full_run[0]]
    assert ("a_source", "source_population", "subjects") in {
        (step, role, manifest)
        for step, role, manifest, _digest, _count in targeted_bindings
    }
    assert all(count == 2 for *_rest, count in targeted_bindings)
