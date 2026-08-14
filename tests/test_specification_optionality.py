from __future__ import annotations

import builtins
import json
import os
from pathlib import Path
import sqlite3

import pytest
import yaml
from fastapi.testclient import TestClient

import nipact.execution as execution_module
from conftest import RegistryV19Fixture
from nipact.cli import main
from nipact.execution import build_run_plan, execute_run_plan
from nipact.gui.app import create_gui_app
from nipact.project_setup import validate_project
from nipact.trace import build_trace_graph_for_artifact_id
from nipact.workflow import (
    compile_workflow_plan,
    load_workflow_project,
    workflow_plan_to_graph,
)


_MANIFEST_SCHEMA = "entity_set_v1"
_MANIFEST_DIGEST = "ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23"


def _plan_summary(plan: object) -> dict[str, object]:
    return {
        "fresh": tuple(
            (ref.step_name, ref.output_name, ref.address)
            for ref in plan.selected_fresh_output_refs
        ),
        "reused": tuple(
            (
                ref.step_name,
                ref.output_name,
                ref.address,
                ref.planned_sibling_artifact_ids,
            )
            for ref in plan.selected_reused_output_refs
        ),
        "request_digests": tuple(
            sorted(
                state.request_bundle_digest
                for job in plan.jobs
                if (state := job.projection_state).request_bundle_digest is not None
            )
        ),
    }


def _ordinary_scientific_state(database: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with sqlite3.connect(database) as connection:
        state = {
            table: tuple(
                sorted(
                    connection.execute(f'SELECT * FROM "{table}"').fetchall(),
                    key=repr,
                )
            )
            for table in (
                "artifact_dependencies",
                "contexts",
                "manifest_declarations",
                "manifest_values",
                "parameters",
                "published_outputs",
                "request_bundle_projections",
            )
        }
        artifacts = connection.execute(
            "SELECT * FROM artifacts ORDER BY artifact_id"
        ).fetchall()
    state["artifact_identities"] = tuple(row[:-1] for row in artifacts)
    return state


def _guard_registered_targets(
    monkeypatch: pytest.MonkeyPatch,
    targets: set[Path],
) -> list[Path]:
    accessed: list[Path] = []
    normalized_targets = {Path(os.path.abspath(path)) for path in targets}

    def checked(value: object) -> None:
        if isinstance(value, int):
            return
        try:
            path = Path(os.path.abspath(os.fspath(value)))
        except TypeError:
            return
        if path in normalized_targets:
            accessed.append(path)
            pytest.fail(f"ordinary command accessed dormant registration: {path}")

    for method_name in (
        "resolve",
        "stat",
        "lstat",
        "exists",
        "is_file",
        "is_dir",
        "open",
        "read_text",
        "read_bytes",
    ):
        original = getattr(Path, method_name)

        def guarded_path_method(
            self: Path,
            *args: object,
            _original: object = original,
            **kwargs: object,
        ) -> object:
            checked(self)
            return _original(self, *args, **kwargs)  # type: ignore[operator]

        monkeypatch.setattr(Path, method_name, guarded_path_method)

    original_open = builtins.open

    def guarded_open(file: object, *args: object, **kwargs: object) -> object:
        checked(file)
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", guarded_open)
    for function_name in ("stat", "lstat"):
        original = getattr(os, function_name)

        def guarded_os_stat(
            path: object,
            *args: object,
            _original: object = original,
            **kwargs: object,
        ) -> object:
            checked(path)
            return _original(path, *args, **kwargs)  # type: ignore[operator]

        monkeypatch.setattr(os, function_name, guarded_os_stat)
    return accessed


def test_dormant_specification_registrations_do_not_affect_ordinary_surfaces(
    registry_v19_fixture: RegistryV19Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = registry_v19_fixture
    loaded_before = load_workflow_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    )
    plan_before = compile_workflow_plan(
        loaded_before,
        workflow_name="base",
        step_name="fixture_analysis",
    )
    graph_before = workflow_plan_to_graph(plan_before)
    validation_before = validate_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    )
    dry_before = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        dry_run=True,
    )
    trace_before = build_trace_graph_for_artifact_id(
        fixture.registry_path,
        artifact_id=fixture.selected_artifact_id,
        context=fixture.context,
    )
    summary_before = TestClient(
        create_gui_app(project_dir=fixture.project_dir, context=fixture.context)
    ).get("/api/summary")
    assert summary_before.status_code == 200

    specifications = fixture.project_dir / "specifications"
    specifications.mkdir()
    malformed = specifications / "malformed.yaml"
    malformed.write_text("not: [valid\n", encoding="utf-8")
    inaccessible = specifications / "inaccessible.yaml"
    inaccessible.write_text("valid: but-dormant\n", encoding="utf-8")
    missing = specifications / "missing.yaml"
    config_path = fixture.project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["specification_libraries"] = {
        "dormant": "specifications/inaccessible.yaml",
    }
    config["specifications"] = {
        "malformed": "specifications/malformed.yaml",
        "missing": "specifications/missing.yaml",
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    accessed = _guard_registered_targets(
        monkeypatch,
        {malformed, inaccessible, missing},
    )

    loaded_after = load_workflow_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    )
    plan_after = compile_workflow_plan(
        loaded_after,
        workflow_name="base",
        step_name="fixture_analysis",
    )
    assert loaded_after == loaded_before
    assert plan_after == plan_before
    assert workflow_plan_to_graph(plan_after) == graph_before
    assert validate_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    ) == validation_before

    assert (
        main(
            [
                "workflow",
                "graph",
                "--project-dir",
                str(fixture.project_dir),
                "--context",
                fixture.context,
                "--workflow",
                "base",
                "--step",
                "fixture_analysis",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == graph_before

    dry_after = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        dry_run=True,
    )
    assert _plan_summary(dry_after) == _plan_summary(dry_before)
    assert execute_run_plan(dry_after, cores=1).all_selected_resolved
    assert build_trace_graph_for_artifact_id(
        fixture.registry_path,
        artifact_id=fixture.selected_artifact_id,
        context=fixture.context,
    ) == trace_before
    summary_after = TestClient(
        create_gui_app(project_dir=fixture.project_dir, context=fixture.context)
    ).get("/api/summary")
    assert summary_after.status_code == 200
    assert summary_after.json() == summary_before.json()

    def reject_snakemake(*_args: object, **_kwargs: object) -> int:
        pytest.fail("reuse-only ordinary execution invoked Snakemake")

    scientific_before = _ordinary_scientific_state(fixture.registry_path)
    monkeypatch.setattr(execution_module, "_run_snakemake", reject_snakemake)
    outcome = execute_run_plan(
        build_run_plan(
            project_dir=fixture.project_dir,
            context=fixture.context,
            workflow_name="base",
            step_name="fixture_analysis",
        ),
        cores=1,
    )
    scientific_after = _ordinary_scientific_state(fixture.registry_path)
    with sqlite3.connect(fixture.registry_path) as connection:
        runs = connection.execute(
            """
            SELECT run_id, context, workflow_name, selected_step_name,
                   selected_output_name, resolution_summary_json, is_current
            FROM workflow_runs
            ORDER BY run_id
            """
        ).fetchall()
        populations = connection.execute(
            """
            SELECT run_id, manifest_name, manifest_value_schema, manifest_digest
            FROM run_execution_population
            ORDER BY run_id
            """
        ).fetchall()
        bindings = connection.execute(
            """
            SELECT run_id, step_name, manifest_usage_role, manifest_name,
                   manifest_value_schema, manifest_digest
            FROM run_manifest_bindings
            ORDER BY run_id
            """
        ).fetchall()
        specification_counts = {
            table: connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            for table in (
                "specification_snapshots",
                "specification_members",
                "specification_snapshot_manifest_values",
                "specification_expected_results",
                "specification_member_attempts",
                "specification_attempt_results",
            )
        }
    assert outcome.all_selected_resolved
    assert outcome.selected_generated_count == 0
    assert outcome.selected_reused_count == 1
    assert scientific_after == scientific_before
    assert [(row[0], row[6]) for row in runs] == [(1, 0), (2, 1)]
    assert runs[1][1:5] == (
        "v18_fixture",
        "base",
        "fixture_analysis",
        "summary",
    )
    assert json.loads(runs[1][5]) == {
        "all_selected_resolved": True,
        "forced": False,
        "schema_version": 1,
        "selected_outputs": [
            {
                "address": "cohort",
                "context": "v18_fixture",
                "output_name": "summary",
                "resolution": {"artifact_id": 5, "outcome": "reused"},
                "step_name": "fixture_analysis",
                "workflow_name": "base",
            }
        ],
    }
    assert populations[1] == (2, "cohort", _MANIFEST_SCHEMA, _MANIFEST_DIGEST)
    assert bindings[1] == (
        2,
        "fixture_analysis",
        "analysis_population",
        "cohort",
        _MANIFEST_SCHEMA,
        _MANIFEST_DIGEST,
    )
    assert set(specification_counts.values()) == {0}
    assert accessed == []
