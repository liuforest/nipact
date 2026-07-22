"""Projector tests for the observed topology (PR 2, commit 2).

These exercise ``build_observed_topology`` as a pure function over a
``build_trace_graph()`` dict. Most cases use hand-built trace dicts carrying
only the keys the projector reads (legitimate because the projector is pure
over the dict); one real-trace smoke test runs a genuine ``build_trace_graph()``
output through the projector to guard against key-name drift. Per the design
doc, there is deliberately no test asserting the absence of registry calls —
the no-new-SQL property is structural, not observable.
"""

import json
import os
from pathlib import Path

import pytest

from nipact.cli import main
from nipact.execution import build_run_plan, execute_run_plan
from nipact.execution_evidence import CompletionReceipt, write_completion_receipt_atomic
from nipact.gui.models import ObservedTopologyResponse
from nipact.gui.topology import build_observed_topology
from nipact.registry import REGISTRY_DB_PATH, list_artifacts
from nipact.trace import build_trace_graph_for_artifact_id


# --- hand-built trace-dict builders ------------------------------------------


def _artifact(
    artifact_id: int,
    origin: str,
    *,
    workflow_name: str | None = None,
    step_name: str | None = None,
    output_name: str | None = None,
    address: str | None = None,
    display_path: str | None = None,
    run_id: int | None = None,
) -> dict:
    return {
        "artifact_id": artifact_id,
        "origin": origin,
        "run_id": run_id,
        "workflow_name": workflow_name,
        "step_name": step_name,
        "output_name": output_name,
        "address": address,
        "display_path": display_path or f"path/{artifact_id}",
    }


def _dependency(
    source_artifact_id: int,
    dependent_artifact_id: int,
    *,
    binding_name: str,
    dependency_role: str,
) -> dict:
    return {
        "source_artifact_id": source_artifact_id,
        "dependent_artifact_id": dependent_artifact_id,
        "binding_name": binding_name,
        "dependency_role": dependency_role,
    }


def _manifest_binding(
    run_id: int,
    *,
    workflow_name: str = "wf",
    step_name: str = "fit",
    manifest_usage_role: str = "fit_cohort",
    manifest_name: str = "cohort",
    manifest_digest: str = "d1",
    manifest_hash: str = "h1",
    entity_count: int = 100,
) -> dict:
    return {
        "run_id": run_id,
        "workflow_name": workflow_name,
        "step_name": step_name,
        "manifest_usage_role": manifest_usage_role,
        "manifest_name": manifest_name,
        "manifest_value_schema": "entity_set_v1",
        "manifest_digest": manifest_digest,
        "manifest_hash": manifest_hash,
        "entity_count": entity_count,
    }


def _execution_population(
    run_id: int,
    *,
    workflow_name: str = "wf",
    manifest_name: str = "population",
    manifest_digest: str = "d1",
    manifest_hash: str = "h1",
    entity_count: int = 100,
) -> dict:
    return {
        "run_id": run_id,
        "workflow_name": workflow_name,
        "manifest_name": manifest_name,
        "manifest_value_schema": "entity_set_v1",
        "manifest_digest": manifest_digest,
        "manifest_hash": manifest_hash,
        "entity_count": entity_count,
    }


def _trace(
    *,
    artifacts: list[dict],
    dependencies: list[dict],
    selected_artifact_id: int,
    execution_populations: list[dict] | None = None,
    manifest_bindings: list[dict] | None = None,
    warnings: list[dict] | None = None,
    provenance_status: str | None = None,
    context: str = "demo",
) -> dict:
    warnings = warnings or []
    return {
        "schema_version": 2,
        "context": context,
        "selected_artifact_id": selected_artifact_id,
        "provenance_status": provenance_status
        or ("degraded" if warnings else "complete"),
        "artifacts": artifacts,
        "dependencies": dependencies,
        "execution_populations": execution_populations or [],
        "manifest_bindings": manifest_bindings or [],
        "warnings": warnings,
    }


def _repeated_topology(instance_count: int) -> dict:
    """One source feeding one `wf/fit` step producing one `model` slot, ×N."""
    artifacts: list[dict] = []
    dependencies: list[dict] = []
    for index in range(1, instance_count + 1):
        source_id = index
        output_id = 100 + index
        artifacts.append(
            _artifact(
                source_id,
                "source",
                display_path=f"sources/sub-{index:02d}.nii",
            )
        )
        artifacts.append(
            _artifact(
                output_id,
                "workflow_output",
                workflow_name="wf",
                step_name="fit",
                output_name="model",
                address=f"sub-{index:02d}",
            )
        )
        dependencies.append(
            _dependency(
                source_id,
                output_id,
                binding_name="bold",
                dependency_role="fit_input",
            )
        )
    return _trace(
        artifacts=artifacts,
        dependencies=dependencies,
        selected_artifact_id=101,
    )


def _nodes_by_id(topology: dict) -> dict[str, dict]:
    return {node["node_id"]: node for node in topology["nodes"]}


def _node_of_kind(topology: dict, kind: str) -> list[dict]:
    return [node for node in topology["nodes"] if node["kind"] == kind]


def _edges_of_kind(topology: dict, kind: str) -> list[dict]:
    return [edge for edge in topology["edges"] if edge["kind"] == kind]


# --- repeated topology: same coordinates, different metrics -------------------


def test_repeated_topology_same_coordinates_different_metrics():
    small = build_observed_topology(_repeated_topology(1))
    medium = build_observed_topology(_repeated_topology(4))

    # identical topology shape
    assert small["summary"]["node_count"] == medium["summary"]["node_count"] == 3
    assert small["summary"]["edge_count"] == medium["summary"]["edge_count"] == 2

    def coordinates(topology: dict) -> list[tuple]:
        coords = []
        for node in topology["nodes"]:
            coords.append(
                (
                    node["kind"],
                    node.get("workflow_name"),
                    node.get("step_name"),
                    node.get("output_name"),
                    node.get("binding_name"),
                    node.get("dependency_role"),
                )
            )
        return coords

    assert coordinates(small) == coordinates(medium)

    # metrics scale with instance count
    small_slot = _node_of_kind(small, "artifact_slot")[0]
    medium_slot = _node_of_kind(medium, "artifact_slot")[0]
    assert small_slot["registry_artifact_count"] == 1
    assert medium_slot["registry_artifact_count"] == 4
    assert small_slot["distinct_address_count"] == 1
    assert medium_slot["distinct_address_count"] == 4

    assert _node_of_kind(small, "step")[0]["produced_registry_artifact_count"] == 1
    assert _node_of_kind(medium, "step")[0]["produced_registry_artifact_count"] == 4
    assert _node_of_kind(small, "source_input")[0]["registry_artifact_count"] == 1
    assert _node_of_kind(medium, "source_input")[0]["registry_artifact_count"] == 4

    assert _edges_of_kind(small, "consumes")[0]["registry_dependency_count"] == 1
    assert _edges_of_kind(medium, "consumes")[0]["registry_dependency_count"] == 4
    assert small["summary"]["distinct_artifact_count"] == 2
    assert medium["summary"]["distinct_artifact_count"] == 8


def test_same_local_names_in_different_workflows_stay_distinct():
    trace = _trace(
        artifacts=[
            _artifact(
                100,
                "workflow_output",
                workflow_name="first",
                step_name="finish",
                output_name="result",
            ),
            _artifact(
                200,
                "workflow_output",
                workflow_name="second",
                step_name="finish",
                output_name="result",
            ),
        ],
        dependencies=[
            _dependency(
                100,
                200,
                binding_name="upstream",
                dependency_role="analysis_input",
            )
        ],
        selected_artifact_id=200,
    )

    topology = build_observed_topology(trace)

    assert {
        (node["workflow_name"], node["step_name"])
        for node in _node_of_kind(topology, "step")
    } == {("first", "finish"), ("second", "finish")}
    assert {
        (node["workflow_name"], node["step_name"], node["output_name"])
        for node in _node_of_kind(topology, "artifact_slot")
    } == {
        ("first", "finish", "result"),
        ("second", "finish", "result"),
    }
    ObservedTopologyResponse.model_validate(topology)


# --- root identity -----------------------------------------------------------


def test_workflow_output_root_maps_to_its_artifact_slot():
    topology = build_observed_topology(_repeated_topology(1))
    root = _nodes_by_id(topology)[topology["root_node_id"]]
    assert root["kind"] == "artifact_slot"
    assert (root["workflow_name"], root["step_name"], root["output_name"]) == (
        "wf",
        "fit",
        "model",
    )


def test_source_root_produces_explicit_source_root_node():
    trace = _trace(
        artifacts=[
            _artifact(1, "source", display_path="sources/bold/sub-01.nii.gz"),
        ],
        dependencies=[],
        selected_artifact_id=1,
    )
    topology = build_observed_topology(trace)

    assert len(topology["nodes"]) == 1
    root = _nodes_by_id(topology)[topology["root_node_id"]]
    assert root["kind"] == "source_root"
    assert root["display_path"] == "sources/bold/sub-01.nii.gz"
    assert root["registry_artifact_count"] == 1
    assert topology["edges"] == []
    assert topology["summary"]["distinct_artifact_count"] == 1
    assert topology["summary"]["registry_dependency_count"] == 0
    ObservedTopologyResponse.model_validate(topology)


# --- edge distinctness -------------------------------------------------------


def test_distinct_bindings_and_roles_stay_distinct_edges():
    # one workflow_output source slot consumed by one step three ways
    trace = _trace(
        artifacts=[
            _artifact(
                50,
                "workflow_output",
                workflow_name="wf",
                step_name="prep",
                output_name="clean",
                address="sub-01",
            ),
            _artifact(
                100,
                "workflow_output",
                workflow_name="wf",
                step_name="fit",
                output_name="model",
                address="sub-01",
            ),
        ],
        dependencies=[
            _dependency(50, 100, binding_name="bold", dependency_role="fit_input"),
            _dependency(50, 100, binding_name="mask", dependency_role="fit_input"),
            _dependency(50, 100, binding_name="bold", dependency_role="analysis_input"),
        ],
        selected_artifact_id=100,
    )
    topology = build_observed_topology(trace)

    consumes = _edges_of_kind(topology, "consumes")
    assert len(consumes) == 3
    pairs = {(edge["binding_name"], edge["dependency_role"]) for edge in consumes}
    assert pairs == {
        ("bold", "fit_input"),
        ("mask", "fit_input"),
        ("bold", "analysis_input"),
    }
    # all anchor the same slot->step endpoints
    assert {edge["source_node_id"] for edge in consumes} == {consumes[0]["source_node_id"]}
    assert {edge["target_node_id"] for edge in consumes} == {consumes[0]["target_node_id"]}
    ObservedTopologyResponse.model_validate(topology)


# --- degraded missing-source policy ------------------------------------------


def test_missing_source_edge_omitted_but_counted_and_warned():
    trace = _trace(
        artifacts=[
            _artifact(1, "source", display_path="sources/sub-01.nii"),
            _artifact(
                100,
                "workflow_output",
                workflow_name="wf",
                step_name="fit",
                output_name="model",
                address="sub-01",
            ),
        ],
        dependencies=[
            _dependency(1, 100, binding_name="bold", dependency_role="fit_input"),
            # source 999 never entered `artifacts` (degraded)
            _dependency(999, 100, binding_name="mask", dependency_role="fit_input"),
        ],
        warnings=[
            {
                "warning_type": "missing_artifact",
                "message": "artifact 999 not found",
                "artifact_id": 999,
                "input_path": "inputs/mask.nii",
            }
        ],
        selected_artifact_id=100,
    )
    topology = build_observed_topology(trace)

    # only the resolvable dependency renders as a consumption edge
    consumes = _edges_of_kind(topology, "consumes")
    assert len(consumes) == 1
    assert consumes[0]["registry_dependency_count"] == 1

    # every edge endpoint resolves to a real node (no dangling omitted edge)
    node_ids = set(_nodes_by_id(topology))
    for edge in topology["edges"]:
        assert edge["source_node_id"] in node_ids
        assert edge["target_node_id"] in node_ids

    # the omitted row is still counted in the summary total
    assert topology["summary"]["registry_dependency_count"] == 2
    assert topology["provenance_status"] == "degraded"
    assert topology["warnings"] == [
        {"warning_type": "missing_artifact", "occurrence_count": 1}
    ]
    ObservedTopologyResponse.model_validate(topology)


# --- manifest grouping -------------------------------------------------------


def test_execution_populations_are_grouped_separately() -> None:
    trace = _trace(
        artifacts=[
            _artifact(
                100,
                "workflow_output",
                workflow_name="wf",
                step_name="fit",
                output_name="model",
                address="cohort",
            ),
        ],
        dependencies=[],
        selected_artifact_id=100,
        execution_populations=[
            _execution_population(1),
            _execution_population(2),
        ],
    )

    topology = build_observed_topology(trace)

    assert topology["manifest_bindings"] == []
    assert topology["execution_populations"] == [
        {
            "workflow_name": "wf",
            "manifest_name": "population",
            "manifest_value_schema": "entity_set_v1",
            "distinct_run_count": 2,
            "distinct_manifest_digest_count": 1,
            "manifest_digest": "d1",
            "manifest_hash": "h1",
            "entity_count": 100,
        }
    ]


def test_manifest_bindings_grouped_with_disagreement_nulled():
    trace = _trace(
        artifacts=[
            _artifact(
                100,
                "workflow_output",
                workflow_name="wf",
                step_name="fit",
                output_name="model",
                address="sub-01",
            ),
        ],
        dependencies=[],
        selected_artifact_id=100,
        manifest_bindings=[
            _manifest_binding(1, manifest_digest="d1", manifest_hash="h", entity_count=100),
            _manifest_binding(2, manifest_digest="d2", manifest_hash="h", entity_count=100),
        ],
    )
    topology = build_observed_topology(trace)

    assert len(topology["manifest_bindings"]) == 1
    summary = topology["manifest_bindings"][0]
    assert (
        summary["workflow_name"],
        summary["step_name"],
        summary["manifest_usage_role"],
    ) == (
        "wf",
        "fit",
        "fit_cohort",
    )
    assert summary["manifest_value_schema"] == "entity_set_v1"
    assert summary["distinct_run_count"] == 2
    assert summary["distinct_manifest_digest_count"] == 2
    assert summary["manifest_digest"] is None  # disagree -> null
    assert summary["manifest_hash"] == "h"  # agree -> carried
    assert summary["entity_count"] == 100  # agree -> carried
    ObservedTopologyResponse.model_validate(topology)


# --- warning aggregation -----------------------------------------------------


def test_warnings_aggregate_by_type():
    trace = _trace(
        artifacts=[
            _artifact(
                100,
                "workflow_output",
                workflow_name="wf",
                step_name="fit",
                output_name="model",
                address="sub-01",
            ),
        ],
        dependencies=[],
        selected_artifact_id=100,
        warnings=[
            {"warning_type": "missing_artifact", "message": "a", "artifact_id": 1, "input_path": None},
            {"warning_type": "missing_artifact", "message": "b", "artifact_id": 2, "input_path": None},
            {"warning_type": "cross_context_dependency", "message": "c", "artifact_id": 3, "input_path": None},
        ],
    )
    topology = build_observed_topology(trace)

    assert topology["warnings"] == [
        {"warning_type": "cross_context_dependency", "occurrence_count": 1},
        {"warning_type": "missing_artifact", "occurrence_count": 2},
    ]


# --- determinism and validation ----------------------------------------------


def test_projection_is_deterministic_and_validates():
    trace = _repeated_topology(3)
    first = build_observed_topology(trace)
    second = build_observed_topology(trace)
    assert first == second

    node_ids = [node["node_id"] for node in first["nodes"]]
    assert node_ids == [f"n{index}" for index in range(len(node_ids))]
    edge_ids = [edge["edge_id"] for edge in first["edges"]]
    assert edge_ids == [f"e{index}" for index in range(len(edge_ids))]

    ObservedTopologyResponse.model_validate(first)


# --- real-trace smoke test (guards key-name drift) ---------------------------


def _run_main_from(cwd: Path, argv: list[str]) -> int:
    old_cwd = Path.cwd()
    os.chdir(cwd)
    try:
        return main(argv)
    finally:
        os.chdir(old_cwd)


def _write_all_staged_outputs(run_plan: object) -> None:
    selected_keys = {
        (job.step_name, job.output_name, job.address)
        for job in run_plan.selected_fresh_jobs
    }
    for job in run_plan.jobs:
        job.staging_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "job_id": job.job_id,
            "step_name": job.step_name,
            "output_name": job.output_name,
            "address": job.address,
        }
        if (job.step_name, job.output_name, job.address) in selected_keys:
            payload["selected"] = True
        job.staging_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    execution_payload = json.loads(
        (run_plan.run_workspace / "run_plan.json").read_text(encoding="utf-8")
    )
    for job_id, job_payload in execution_payload["jobs"].items():
        write_completion_receipt_atomic(
            run_plan.run_workspace / job_payload["completion_receipt_path"],
            CompletionReceipt(
                invocation_token=execution_payload["invocation_token"],
                job_id=job_id,
                request_bundle_digest=job_payload["request_bundle_digest"],
                outputs=tuple(job_payload["declared_outputs"]),
            ),
        )


def test_real_trace_projects_and_validates(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir = tmp_path / "project"
    runtime_dir = tmp_path / "runtime"
    assert (
        _run_main_from(
            tmp_path,
            [
                "init",
                "--demo",
                "colors",
                "--project-dir",
                "project",
                "--runtime-dir",
                "runtime",
                "--context",
                "colors",
            ],
        )
        == 0
    )
    capsys.readouterr()

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )
    def write_staged_outputs(*_args: object, **_kwargs: object) -> int:
        _write_all_staged_outputs(run_plan)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", write_staged_outputs)
    execute_run_plan(run_plan, cores=1)

    registry_path = runtime_dir / REGISTRY_DB_PATH
    selected = list_artifacts(
        registry_path,
        context="colors",
        origin="workflow_output",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        is_published=True,
    )[0]
    graph = build_trace_graph_for_artifact_id(
        registry_path,
        artifact_id=selected.artifact_id,
    )

    topology = build_observed_topology(graph)
    response = ObservedTopologyResponse.model_validate(topology)

    assert response.perspective == "observed"
    assert response.scope == "ancestor_closure"
    assert response.context == "colors"
    assert response.root_artifact_id == selected.artifact_id
    assert response.root_node_id in {node.node_id for node in response.nodes}
    assert response.summary.distinct_artifact_count == len(graph["artifacts"])
    assert response.summary.registry_dependency_count == len(graph["dependencies"])
    assert response.summary.node_count == len(response.nodes)
    assert response.summary.edge_count == len(response.edges)
    assert len(response.execution_populations) == 1
    # the colors closure has at least one source feeding the modeling step
    assert any(node.kind == "source_input" for node in response.nodes)
