import json
import os
import sqlite3
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


def _workflow_input_job(run_plan: object, *, step_name: str) -> object:
    return next(job for job in run_plan.jobs if job.step_name == step_name)


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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)
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
    assert execute_run_plan(c_plan, cores=1) == len(c_plan.published_outputs)

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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)
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
    assert execute_run_plan(c_plan, cores=1, dry_run=True) == 0

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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)

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
    assert execute_run_plan(c_plan, cores=1) == len(c_plan.published_outputs)
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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)
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
    assert execute_run_plan(c_plan, cores=1) == len(c_plan.published_outputs)
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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)

    multi_plan = build_run_plan(
        project_dir=project_dir,
        context="cache",
        workflow_name="main",
        step_name="multi_transform",
    )
    assert execute_run_plan(multi_plan, cores=1) == len(multi_plan.published_outputs)
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
    assert execute_run_plan(fresh_multi_plan, cores=1) == len(fresh_multi_plan.published_outputs)
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
    assert execute_run_plan(use_plan, cores=1) == len(use_plan.published_outputs)
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
    assert execute_run_plan(main_b_plan, cores=1) == len(main_b_plan.published_outputs)
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
    assert execute_run_plan(derivative_c_plan, cores=1) == len(
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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)
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
    assert execute_run_plan(b_plan, cores=1) == len(b_plan.published_outputs)

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
