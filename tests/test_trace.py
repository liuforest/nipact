import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest

import nipact.registry as registry
from nipact.cli import main
from nipact.errors import ValidationError
from nipact.execution import build_run_plan, execute_run_plan
from nipact.gui.topology import build_observed_topology
from nipact.registry import REGISTRY_DB_PATH, list_artifacts
from nipact.trace import (
    build_trace_graph,
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

    def write_staged_outputs(*_args: object, **_kwargs: object) -> int:
        _write_all_staged_outputs(run_plan)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", write_staged_outputs)
    assert execute_run_plan(run_plan, cores=1).published_count == len(run_plan.published_outputs)
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

    assert graph["schema_version"] == 2
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
    assert source_artifacts[0]["source_scope"] == "global"
    assert source_artifacts[0]["source_name"] == "colors_source"
    assert source_artifacts[0]["source_entity_id"] is None
    assert source_artifacts[0]["workflow_artifact_ref"] is None
    assert len(artifacts) == len(run_plan.jobs) + 1
    assert len(dependencies) == sum(len(job.input_records) for job in run_plan.jobs)
    assert len(graph["execution_populations"]) == 1
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
        "execution_populations",
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
        "source_scope",
        "source_name",
        "source_entity_id",
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
        "source_scope",
        "source_name",
        "source_entity_id",
        "source_occurrence_path",
        "dependency_set_id",
        "manifest_value_schema",
        "manifest_digest",
        "edge_cardinality",
    }
    assert set(graph["manifest_bindings"][0]) == {
        "run_id",
        "workflow_name",
        "step_name",
        "manifest_usage_role",
        "manifest_name",
        "manifest_value_schema",
        "manifest_digest",
        "manifest_hash",
        "entity_count",
    }
    assert set(graph["execution_populations"][0]) == {
        "run_id",
        "workflow_name",
        "manifest_name",
        "manifest_value_schema",
        "manifest_digest",
        "manifest_hash",
        "entity_count",
    }
    selected_node = next(
        artifact for artifact in graph["artifacts"] if artifact["is_selected"]
    )
    assert selected_node["job_id"] == "job__color_sector_analysis__sector_counts__cohort"
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
        address="cohort",
    )

    selected_node = next(
        artifact for artifact in graph["artifacts"] if artifact["is_selected"]
    )
    assert selected_node["is_published"] is True
    assert selected_node["path"].startswith("outputs/v1/colors/")
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


def test_shared_membership_trace_and_gui_keep_generating_workflow_label(
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
    with sqlite3.connect(registry_path) as conn:
        conn.execute(
            """
            INSERT INTO published_outputs (
                context, workflow_name, step_name, output_name, address, path,
                output_digest, output_hash, artifact_id
            )
            SELECT context, 'red-qc-target', step_name, output_name, address,
                   path, output_digest, output_hash, artifact_id
            FROM published_outputs
            WHERE context = 'colors'
              AND workflow_name = 'base'
              AND step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
            """
        )
        membership_count, artifact_count = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT artifact_id)
            FROM published_outputs
            WHERE context = 'colors'
              AND step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
            """
        ).fetchone()

    assert (membership_count, artifact_count) == (2, 1)
    graph = build_trace_graph_for_workflow_coordinate(
        registry_path,
        context="colors",
        workflow_name="red-qc-target",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="cohort",
    )
    selected_nodes = [
        artifact for artifact in graph["artifacts"] if artifact["is_selected"]
    ]
    assert len(selected_nodes) == 1
    assert selected_nodes[0]["workflow_name"] == "base"
    assert sum(
        artifact["artifact_id"] == graph["selected_artifact_id"]
        for artifact in graph["artifacts"]
    ) == 1

    topology = build_observed_topology(graph)
    selected_slots = [
        node
        for node in topology["nodes"]
        if node["kind"] == "artifact_slot"
        and node["step_name"] == "color_sector_analysis"
        and node["output_name"] == "sector_counts"
    ]
    assert len(selected_slots) == 1
    assert selected_slots[0]["workflow_name"] == "base"
    assert selected_slots[0]["registry_artifact_count"] == 1


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


def _selected_sector_counts(registry_path: Path) -> object:
    return list_artifacts(
        registry_path,
        context="colors",
        origin="workflow_output",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        is_published=True,
    )[0]


def _a_feature_artifact(registry_path: Path) -> object:
    return list_artifacts(
        registry_path,
        context="colors",
        origin="workflow_output",
        workflow_name="base",
        step_name="color_features",
        output_name="features",
    )[0]


def test_trace_traversal_shares_one_read_session_regardless_of_closure(
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
    large_root = _selected_sector_counts(registry_path)
    small_root = _a_feature_artifact(registry_path)

    real_connect = registry._connect_readonly_rows
    real_validate = registry._validate_schema_version

    def run_counted(
        selected_artifact: object,
    ) -> tuple[dict[str, object], list[Path], list[bool], list[sqlite3.Connection]]:
        connect_calls: list[Path] = []
        validate_in_transaction: list[bool] = []
        conns: list[sqlite3.Connection] = []

        @contextmanager
        def counting_connect(path: Path):
            connect_calls.append(path)
            with real_connect(path) as conn:
                conns.append(conn)
                yield conn

        def counting_validate(conn: sqlite3.Connection) -> None:
            validate_in_transaction.append(conn.in_transaction)
            real_validate(conn)

        monkeypatch.setattr(registry, "_connect_readonly_rows", counting_connect)
        monkeypatch.setattr(registry, "_validate_schema_version", counting_validate)
        graph = build_trace_graph(
            registry_path,
            selected_artifact=selected_artifact,
            active_context="colors",
        )
        return graph, connect_calls, validate_in_transaction, conns

    large_graph, large_connects, large_validate, large_conns = run_counted(large_root)
    small_graph, small_connects, small_validate, small_conns = run_counted(small_root)

    # One traversal connection and one schema validation, no matter the closure.
    assert large_connects == [registry_path]
    assert small_connects == [registry_path]
    assert large_validate == [True]
    assert small_validate == [True]
    # The two closures genuinely differ, so a constant connection count is meaningful.
    assert len(large_graph["artifacts"]) > len(small_graph["artifacts"])
    # Each traversal session is closed once its graph is built.
    for conn in (*large_conns, *small_conns):
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


def test_trace_selector_wrapper_opens_separate_selector_lookup(
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
    selected = _selected_sector_counts(registry_path)

    connect_calls: list[Path] = []
    real_connect = registry._connect_readonly_rows

    @contextmanager
    def counting_connect(path: Path):
        connect_calls.append(path)
        with real_connect(path) as conn:
            yield conn

    monkeypatch.setattr(registry, "_connect_readonly_rows", counting_connect)
    graph = build_trace_graph_for_artifact_id(
        registry_path,
        artifact_id=selected.artifact_id,
    )

    assert graph["selected_artifact_id"] == selected.artifact_id
    # One connection for the selector root lookup, one for the traversal session.
    assert connect_calls == [registry_path, registry_path]


def test_trace_terminates_on_dependency_cycle(
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
    selected = _selected_sector_counts(registry_path)
    feature = _a_feature_artifact(registry_path)

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
                feature.artifact_id,
                selected.artifact_id,
                "0" * 64,
                0,
                ".json",
                "runs/colors/base/cycle.json",
                "cycle_input",
                "input",
            ),
        )

    graph = build_trace_graph_for_artifact_id(
        registry_path,
        artifact_id=selected.artifact_id,
        context="colors",
    )

    artifact_ids = {artifact["artifact_id"] for artifact in graph["artifacts"]}
    assert selected.artifact_id in artifact_ids
    assert feature.artifact_id in artifact_ids
    assert any(
        dependency["source_artifact_id"] == selected.artifact_id
        and dependency["dependent_artifact_id"] == feature.artifact_id
        for dependency in graph["dependencies"]
    )


def test_trace_raw_decoder_error_escapes_and_closes_session(
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
    selected = _selected_sector_counts(registry_path)
    feature = _a_feature_artifact(registry_path)

    with sqlite3.connect(registry_path) as conn:
        conn.execute(
            "UPDATE artifacts SET file_size = ? WHERE artifact_id = ?",
            ("not-a-number", feature.artifact_id),
        )

    conns: list[sqlite3.Connection] = []
    real_connect = registry._connect_readonly_rows

    @contextmanager
    def counting_connect(path: Path):
        with real_connect(path) as conn:
            conns.append(conn)
            yield conn

    monkeypatch.setattr(registry, "_connect_readonly_rows", counting_connect)
    with pytest.raises(ValueError) as excinfo:
        build_trace_graph(
            registry_path,
            selected_artifact=selected,
            active_context="colors",
        )

    # A raw conversion failure keeps its original type (decoders run outside the
    # sqlite3.Error -> ValidationError translation).
    assert not isinstance(excinfo.value, ValidationError)
    assert "invalid literal for int()" in str(excinfo.value)
    # The traversal session is still closed despite the raw failure.
    assert conns
    for conn in conns:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
