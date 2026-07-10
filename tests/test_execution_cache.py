import json
import os
import sqlite3
import subprocess
from pathlib import Path

import pytest
import yaml

import nipact.registry as registry_module
from nipact.errors import ValidationError
from nipact.execution import build_run_plan, execute_run_plan
from nipact.hashing import sha256_file_digest
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


def _compact_json(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _reuse_request_for_job(
    run_plan: object,
    *,
    step_name: str,
) -> registry_module.ReusableArtifactRequest:
    job = _workflow_input_job(run_plan, step_name=step_name)
    output = job._single_output()
    return registry_module.ReusableArtifactRequest(
        context=run_plan.context,
        workflow_name=run_plan.workflow_name,
        step_name=job.step_name,
        output_name=output.output_name,
        address=job.address,
        extension=output.declared_extension,
        callable_ref=job.callable_ref,
        parameters_json=_compact_json(job.params),
        input_records=job.input_records,
    )


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


def test_cross_target_dry_run_hydrates_reused_upstream_without_registry_update(
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
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        counts_before = {
            "workflow_runs": conn.execute(
                "SELECT COUNT(*) FROM workflow_runs"
            ).fetchone()[0],
            "artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
        }

    c_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="c_transform",
    )
    assert execute_run_plan(c_plan, cores=1, dry_run=True).published_count == 0

    assert (c_plan.run_workspace / "staging/b_transform/b_out/sub_001.json").is_file()
    assert not (c_plan.run_workspace / "staging/c_transform/c_out/sub_001.json").exists()
    assert log_path.read_text(encoding="utf-8").splitlines() == ["B sub_001"]
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        counts_after = {
            "workflow_runs": conn.execute(
                "SELECT COUNT(*) FROM workflow_runs"
            ).fetchone()[0],
            "artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
        }
    assert counts_after == counts_before


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
    request = _reuse_request_for_job(b_plan, step_name="b_transform")

    # Runtime roots are separate in normal projects, but the registry resolver
    # still needs an explicit context predicate. Without it, a valid selected
    # output from another context in the same registry could be hydrated into the
    # wrong workflow declaration.
    foreign_context_request = registry_module.ReusableArtifactRequest(
        **{**request.__dict__, "context": "other_context"}
    )

    assert (
        registry_module.resolve_reusable_artifact(
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
    # remove or mutate that file between planning and execution. Hydration must
    # re-check the registered path and digest at execution time rather than
    # trusting the earlier plan snapshot.
    if change == "delete":
        published_b.unlink()
    else:
        published_b.write_text(
            published_b.read_text(encoding="utf-8").replace("alpha", "omega"),
            encoding="utf-8",
        )

    with pytest.raises(ValidationError, match=message):
        execute_run_plan(c_plan, cores=1)
    assert _registry_row_counts(runtime_dir) == counts_before


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

    # `derivative` has no base_workflow, so before this change its reuse set was
    # just itself and it recomputed b. Reuse now spans the whole context, so it
    # hydrates main's published b across the sibling boundary.
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

    # The global step `b_transform` changes computation after main published. The
    # sibling now *considers* main's b (the base-chain gate is gone) but the
    # content match in resolve_reusable_artifact rejects it on the diverging
    # predicate, so b is recomputed; only the unchanged a_source prefix is reused.
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
    # sub_001's b from main (base chain wins), sub_002's b from the sibling (the
    # only holder). fit itself recomputes since no single workflow published it.
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
    )
    outcome = execute_run_plan(run_plan, cores=1, dry_run=True)

    assert outcome.published_count == 0
    # The targeted workspace itself may be written (run plan, Snakefile).
    assert run_plan.run_workspace == (
        runtime_dir / "runs/cache/main/b_transform/addresses/sub_001"
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
