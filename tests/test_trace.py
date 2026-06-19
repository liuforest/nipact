import json
import os
import sqlite3
from pathlib import Path

import pytest

from nipact.cli import main
from nipact.errors import ValidationError
from nipact.execution import build_run_plan, execute_run_plan
from nipact.registry import REGISTRY_DB_PATH, list_artifacts
from nipact.trace import (
    build_trace_graph_for_artifact_id,
    build_trace_graph_for_path,
    build_trace_graph_for_workflow_coordinate,
)


def _run_main_from(cwd: Path, argv: list[str]) -> int:
    old_cwd = Path.cwd()
    os.chdir(cwd)
    try:
        return main(argv)
    finally:
        os.chdir(old_cwd)


def _init_demo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> tuple[Path, Path]:
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
    return project_dir, runtime_dir


def _write_all_staged_outputs(run_plan: object) -> None:
    selected_keys = {
        (job.step_name, job.output_name, job.address)
        for job in run_plan.selected_jobs
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


def _successful_sector_run(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, object]:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )

    def write_staged_outputs(*_args: object, **_kwargs: object) -> None:
        _write_all_staged_outputs(run_plan)

    monkeypatch.setattr("nipact.execution._run_snakemake", write_staged_outputs)
    assert execute_run_plan(run_plan, cores=1) == len(run_plan.published_outputs)
    return project_dir, runtime_dir, run_plan


def _selected_artifact_id(graph: dict[str, object]) -> int:
    return int(graph["selected_artifact_id"])


def test_trace_graph_by_artifact_id_includes_sources_and_manifest_bindings(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
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

    assert graph["schema_version"] == 1
    assert graph["context"] == "colors"
    assert graph["selected_artifact_id"] == selected.artifact_id
    assert graph["provenance_status"] == "complete"
    assert graph["warnings"] == []
    artifacts = graph["artifacts"]
    dependencies = graph["dependencies"]
    source_artifacts = [
        artifact for artifact in artifacts if artifact["origin"] == "source"
    ]
    assert len(source_artifacts) == 1
    assert source_artifacts[0]["path"] == "data/color_source.json"
    assert source_artifacts[0]["workflow_artifact_ref"] is None
    assert len(artifacts) == len(run_plan.jobs) + 1
    assert len(dependencies) == sum(len(job.input_records) for job in run_plan.jobs)
    assert len(graph["manifest_bindings"]) == len(run_plan.manifest_bindings)


def test_trace_graph_payload_shape_is_stable_for_gui_contract(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
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

    assert set(graph) == {
        "schema_version",
        "context",
        "selected_artifact_id",
        "provenance_status",
        "artifacts",
        "dependencies",
        "manifest_bindings",
        "warnings",
    }
    assert set(graph["artifacts"][0]) == {
        "artifact_id",
        "origin",
        "run_id",
        "job_id",
        "artifact_set_id",
        "path",
        "display_path",
        "is_selected",
        "is_selected_output",
        "is_published",
        "published_path",
        "staging_path",
        "workflow_name",
        "step_name",
        "output_name",
        "address",
        "parameter_hash",
        "content_digest",
        "output_hash",
        "file_size",
        "extension",
        "subject_id",
        "session_id",
        "task_name",
        "run_label",
        "datatype",
        "suffix",
        "source_metadata",
        "workflow_artifact_ref",
        "callable_ref",
        "software_ref",
    }
    assert set(graph["dependencies"][0]) == {
        "edge_id",
        "source_artifact_id",
        "dependent_artifact_id",
        "is_reused_input",
        "dependency_role",
        "binding_name",
        "input_path",
        "source_content_digest",
        "source_file_size",
        "source_extension",
        "dependency_set_id",
        "manifest_digest",
        "edge_cardinality",
    }
    assert set(graph["manifest_bindings"][0]) == {
        "run_id",
        "workflow_name",
        "step_name",
        "role",
        "manifest_name",
        "manifest_digest",
        "manifest_hash",
        "entity_count",
    }
    selected_node = next(
        artifact for artifact in graph["artifacts"] if artifact["is_selected"]
    )
    assert selected_node["job_id"] == "job__color_sector_analysis__sector_counts__init"
    assert selected_node["extension"] == ".json"
    assert selected_node["artifact_set_id"] is None
    assert selected_node["callable_ref"] == (
        "nipact.examples.colors_processing_demo.runtime:color_sector_analysis_file"
    )
    assert selected_node["software_ref"] is None


def test_trace_graph_by_registered_path_uses_same_query_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
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

    by_id = build_trace_graph_for_artifact_id(
        registry_path,
        artifact_id=selected.artifact_id,
    )
    by_path = build_trace_graph_for_path(
        registry_path,
        context="colors",
        artifact_path=selected.path,
    )

    assert _selected_artifact_id(by_path) == _selected_artifact_id(by_id)
    assert by_path["artifacts"] == by_id["artifacts"]
    assert by_path["dependencies"] == by_id["dependencies"]


@pytest.mark.parametrize(
    "path_column",
    ["path"],
)
def test_trace_by_artifact_id_rejects_unsafe_registered_paths(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    path_column: str,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
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

    with sqlite3.connect(registry_path) as conn:
        conn.execute(
            f"UPDATE artifacts SET {path_column} = ? WHERE artifact_id = ?",
            ("/tmp/leaked-artifact.json", selected.artifact_id),
        )

    with pytest.raises(ValidationError, match="relative to runtime dir"):
        build_trace_graph_for_artifact_id(
            registry_path,
            artifact_id=selected.artifact_id,
        )


def test_trace_graph_by_workflow_coordinate_accepts_published_intermediate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH

    graph = build_trace_graph_for_workflow_coordinate(
        registry_path,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="init",
    )

    selected_node = next(
        artifact for artifact in graph["artifacts"] if artifact["is_selected"]
    )
    assert selected_node["is_published"] is True
    assert selected_node["path"].startswith("outputs/colors/base/")
    assert selected_node["workflow_artifact_ref"] == (
        "artifact:color_sector_analysis:sector_counts"
    )
    intermediate_graph = build_trace_graph_for_workflow_coordinate(
        registry_path,
        context="colors",
        workflow_name="base",
        step_name="color_features",
        output_name="features",
        address="color_000",
    )
    intermediate_node = next(
        artifact for artifact in intermediate_graph["artifacts"] if artifact["is_selected"]
    )
    assert intermediate_node["is_published"] is True
    assert intermediate_node["is_selected_output"] is False


def test_trace_graph_marks_damaged_dependency_as_degraded(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
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

    with sqlite3.connect(registry_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """
            INSERT INTO artifact_dependencies (
                dependent_artifact_id, source_artifact_id,
                source_content_digest, source_file_size, source_extension,
                input_path, binding_name, dependency_role
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                selected.artifact_id,
                999999,
                "0" * 64,
                0,
                ".json",
                "runs/colors/base/missing.json",
                "missing_input",
                "input",
            ),
        )

    graph = build_trace_graph_for_artifact_id(
        registry_path,
        artifact_id=selected.artifact_id,
    )

    assert graph["provenance_status"] == "degraded"
    assert graph["warnings"] == [
        {
            "warning_type": "missing_artifact",
            "message": "unknown registry artifact id: 999999",
            "artifact_id": 999999,
            "input_path": "runs/colors/base/missing.json",
        }
    ]
    assert any(
        dependency["source_artifact_id"] == 999999
        for dependency in graph["dependencies"]
    )
