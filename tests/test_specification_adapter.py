from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import os
from pathlib import Path
import sqlite3

import pytest
import yaml

import nipact.execution as execution_module
import nipact.registry as registry_module
from nipact.errors import ValidationError
from nipact.execution import build_run_plan, execute_run_plan
from nipact.manifest import load_manifest
from nipact.specification_adapter import apply_provisional_member
from nipact.specification_compiler import (
    DecisionCoordinate,
    ExecutionPopulationWrite,
    ManifestBindingWrite,
    ParameterWrite,
    ProvisionalMember,
    ResultWrite,
    TargetWrite,
)
from nipact.workflow import load_workflow_project


def _write_yaml(path: Path, payload: object) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    module_dir = tmp_path / "importable"
    module_dir.mkdir()
    (module_dir / "adapter_runtime.py").write_text(
        """
import json


def import_seed(*, inputs, outputs, params, address):
    value = inputs["seed"][0].read_text(encoding="utf-8").strip()
    outputs["raw"].write_text(
        json.dumps({"address": address, "value": value}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )


def model(*, inputs, outputs, params, address):
    source = json.loads(inputs["raw"][0].read_text(encoding="utf-8"))
    value = source["value"] + params["suffix"]
    outputs["estimate"].write_text(
        json.dumps({"address": address, "value": value}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )
    outputs["diagnostics"].write_text(
        json.dumps({"address": address, "length": len(value)}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )


def alt_model(*, inputs, outputs, params, address):
    source = json.loads(inputs["raw"][0].read_text(encoding="utf-8"))
    value = source["value"] + params["suffix"]
    outputs["alt_estimate"].write_text(
        json.dumps({"address": address, "value": value}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )
    outputs["alt_diagnostics"].write_text(
        json.dumps({"address": address, "length": len(value)}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )


def later(*, inputs, outputs, params, address):
    outputs["later"].write_text("later\\n", encoding="utf-8")
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
            "context": "adapter",
            "paths": {"runtime": "../runtime"},
            "sources": {"index": "sources.yaml"},
            "manifests": {
                "subjects": "manifests/subjects.yaml",
                "selected": "manifests/selected.yaml",
                "empty_fit": "manifests/empty_fit.yaml",
            },
            "steps": {"directory": "steps"},
            "workflows": {
                "base": "workflows/base.yaml",
                "alternative": "workflows/alternative.yaml",
                "direct_a": "workflows/direct_a.yaml",
                "direct_b": "workflows/direct_b.yaml",
            },
        },
    )
    _write_yaml(
        project_dir / "sources.yaml",
        {"entities": {"entity_001": {"seed": "data/source/entity_001.txt"}}},
    )
    for name, entities in (
        ("subjects", ["entity_001"]),
        ("selected", ["entity_001"]),
        ("empty_fit", ["entity_002"]),
    ):
        _write_yaml(
            project_dir / f"manifests/{name}.yaml",
            {"description": name, "entities": entities},
        )

    _write_yaml(
        project_dir / "steps/import_seed.yaml",
        {
            "step_name": "import_seed",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": "adapter_runtime:import_seed",
            "source_inputs": ["seed"],
            "outputs": {"raw": {"extension": ".json", "address_scope": "entity"}},
        },
    )
    _write_yaml(
        project_dir / "steps/model.yaml",
        {
            "step_name": "model",
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "entity",
            "callable": "adapter_runtime:model",
            "inputs": {
                "raw": {
                    "artifact": "import_seed.raw",
                    "dependency_role": "analysis_input",
                }
            },
            "params": {"suffix": "-base", "nested": {"items": [1]}},
            "manifest_binding": {"role": "analysis_population", "manifest": "subjects"},
            "outputs": {
                "estimate": {"extension": ".json", "address_scope": "entity"},
                "diagnostics": {"extension": ".json", "address_scope": "entity"},
            },
        },
    )
    _write_yaml(
        project_dir / "steps/later.yaml",
        {
            "step_name": "later",
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "cohort",
            "callable": "adapter_runtime:later",
            "params": {"unused": 0},
            "outputs": {"later": {"extension": ".txt", "address_scope": "cohort"}},
        },
    )
    _write_yaml(
        project_dir / "steps/alt_model.yaml",
        {
            "step_name": "alt_model",
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "entity",
            "callable": "adapter_runtime:alt_model",
            "inputs": {
                "raw": {
                    "artifact": "import_seed.raw",
                    "dependency_role": "analysis_input",
                }
            },
            "params": {"suffix": "-alt", "nested": {"items": [1]}},
            "manifest_binding": {
                "role": "analysis_population",
                "manifest": "subjects",
            },
            "outputs": {
                "alt_estimate": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
                "alt_diagnostics": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "workflows/base.yaml",
        {
            "workflow_name": "base",
            "execution_population": "subjects",
            "steps": [
                "import_seed",
                {"step_name": "model", "output_name": "estimate"},
                {"step_name": "later", "output_name": "later"},
            ],
        },
    )
    _write_yaml(
        project_dir / "workflows/alternative.yaml",
        {
            "workflow_name": "alternative",
            "execution_population": "subjects",
            "steps": [
                "import_seed",
                {"step_name": "alt_model", "output_name": "alt_estimate"},
            ],
        },
    )
    for name, suffix in (("direct_a", "-a"), ("direct_b", "-b")):
        _write_yaml(
            project_dir / f"workflows/{name}.yaml",
            {
                "workflow_name": name,
                "base_workflow": "base",
                "step_overrides": {
                    "model": {
                        "params": {"suffix": suffix, "nested": {"items": [2]}}
                    }
                },
            },
        )

    (runtime_dir / "data/source/entity_001.txt").write_text(
        "alpha\n", encoding="utf-8"
    )
    manifests = {
        name: load_manifest(project_dir / f"manifests/{name}.yaml")
        for name in ("subjects", "selected", "empty_fit")
    }
    registry_module.initialize_prepared_demo_registry_db(
        runtime_dir / "database/registry.db",
        context="adapter",
        runtime_root=runtime_dir,
        manifests=manifests,
        manifest_paths={name: f"manifests/{name}.yaml" for name in manifests},
    )
    return project_dir, runtime_dir


def _member(
    *,
    key: str = "member-a",
    suffix: str = "-a",
    writes: tuple[object, ...] | None = None,
) -> ProvisionalMember:
    default_writes = (
        ParameterWrite("model", "suffix", suffix),
        ParameterWrite("model", "nested", {"items": [2]}),
        ExecutionPopulationWrite("selected"),
        ManifestBindingWrite("model", "analysis_population", "selected"),
        TargetWrite("model", "estimate"),
        ResultWrite("reported_diagnostics", "model", "diagnostics"),
        ResultWrite("reported_estimate", "model", "estimate"),
    )
    return ProvisionalMember(
        member_key=key,
        decision_coordinates=(DecisionCoordinate("member_choice", suffix),),
        workflow_name="base",
        writes=default_writes if writes is None else writes,  # type: ignore[arg-type]
        disposition="included",
        exclusion_reason=None,
    )


def test_apply_member_uses_copy_on_write_and_owned_effective_declaration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _write_project(tmp_path, monkeypatch)
    loaded = load_workflow_project(project_dir=project_dir, context="adapter")
    member = replace(_member(suffix="-member"), workflow_name="direct_a")
    loaded_before = deepcopy(loaded)
    member_before = deepcopy(member)
    project_bytes = {
        path.relative_to(project_dir): path.read_bytes()
        for path in project_dir.rglob("*.yaml")
    }

    applied = apply_provisional_member(loaded=loaded, member=member)

    assert loaded == loaded_before
    assert member == member_before
    assert applied.member == member and applied.member is not member
    assert applied.loaded_project.source_index is loaded.source_index
    assert applied.loaded_project.manifests is loaded.manifests
    assert applied.loaded_project.manifest_paths is loaded.manifest_paths
    assert applied.loaded_project.steps is not loaded.steps
    assert applied.loaded_project.workflows is not loaded.workflows
    assert applied.loaded_project.steps["import_seed"] is loaded.steps["import_seed"]
    assert applied.loaded_project.steps["model"] is not loaded.steps["model"]
    assert loaded.steps["model"].params["suffix"] == "-base"
    assert loaded.workflows["direct_a"].step_overrides["model"].params["suffix"] == "-a"
    assert applied.loaded_project.workflows["direct_a"].step_overrides["model"].params[
        "suffix"
    ] == "-member"

    declaration = applied.effective_declaration
    assert declaration.target_step_name == "model"
    assert declaration.target_output_name == "estimate"
    assert [step.step_name for step in declaration.steps] == ["import_seed", "model"]
    assert declaration.execution_population is not None
    assert declaration.execution_population.manifest_name == "selected"
    assert declaration.manifest_bindings[0].manifest_name == "selected"
    assert [
        (result.role, result.output_name, result.address_scope)
        for result in declaration.results
    ] == [
        ("reported_diagnostics", "diagnostics", "entity"),
        ("reported_estimate", "estimate", "entity"),
    ]

    effective_params = applied.loaded_project.workflows["direct_a"].step_overrides[
        "model"
    ].params
    effective_params["suffix"] = "mutated"
    effective_params["nested"]["items"].append(3)
    assert next(step for step in declaration.steps if step.step_name == "model").params[
        "suffix"
    ] == "-member"
    assert next(step for step in declaration.steps if step.step_name == "model").params[
        "nested"
    ] == {"items": [2]}
    assert {
        path.relative_to(project_dir): path.read_bytes()
        for path in project_dir.rglob("*.yaml")
    } == project_bytes


@pytest.mark.parametrize(
    ("member", "message"),
    [
        (replace(_member(), workflow_name="missing"), "unknown workflow"),
        (
            _member(
                writes=(
                    ParameterWrite("later", "unused", 1),
                    TargetWrite("model", "estimate"),
                    ResultWrite("estimate", "model", "estimate"),
                )
            ),
            "outside the selected target closure",
        ),
        (
            _member(
                writes=(
                    ParameterWrite("model", "unknown", 1),
                    TargetWrite("model", "estimate"),
                    ResultWrite("estimate", "model", "estimate"),
                )
            ),
            "undeclared top-level parameter",
        ),
        (
            _member(
                writes=(
                    ManifestBindingWrite("model", "wrong_role", "selected"),
                    TargetWrite("model", "estimate"),
                    ResultWrite("estimate", "model", "estimate"),
                )
            ),
            "role does not match",
        ),
        (
            _member(
                writes=(
                    TargetWrite("model", "missing"),
                    ResultWrite("estimate", "model", "estimate"),
                )
            ),
            "unknown output",
        ),
        (
            _member(
                writes=(
                    ExecutionPopulationWrite("empty_fit"),
                    ManifestBindingWrite(
                        "model", "analysis_population", "subjects"
                    ),
                    TargetWrite("model", "estimate"),
                    ResultWrite("estimate", "model", "estimate"),
                )
            ),
            "not a subset",
        ),
        (
            _member(
                writes=(
                    ExecutionPopulationWrite("selected"),
                    TargetWrite("later", "later"),
                    ResultWrite("reported_later", "later", "later"),
                )
            ),
            "unused by the selected target closure",
        ),
        (
            _member(
                writes=(
                    TargetWrite("model", "estimate"),
                    ResultWrite("reported_later", "later", "later"),
                )
            ),
            "siblings on the target step",
        ),
    ],
)
def test_apply_member_rejects_adapter_owned_invalid_cases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    member: ProvisionalMember,
    message: str,
) -> None:
    project_dir, _runtime_dir = _write_project(tmp_path, monkeypatch)
    loaded = load_workflow_project(project_dir=project_dir, context="adapter")

    with pytest.raises(ValidationError, match=message):
        apply_provisional_member(loaded=loaded, member=member)


def test_alternative_topologies_share_only_validated_result_role_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _write_project(tmp_path, monkeypatch)
    loaded = load_workflow_project(project_dir=project_dir, context="adapter")
    base = apply_provisional_member(loaded=loaded, member=_member())
    alternative = apply_provisional_member(
        loaded=loaded,
        member=ProvisionalMember(
            member_key="alternative",
            decision_coordinates=(DecisionCoordinate("factory", "alternative"),),
            workflow_name="alternative",
            writes=(
                ParameterWrite("alt_model", "suffix", "-a"),
                ExecutionPopulationWrite("selected"),
                ManifestBindingWrite(
                    "alt_model", "analysis_population", "selected"
                ),
                TargetWrite("alt_model", "alt_estimate"),
                ResultWrite(
                    "reported_diagnostics", "alt_model", "alt_diagnostics"
                ),
                ResultWrite("reported_estimate", "alt_model", "alt_estimate"),
            ),
            disposition="included",
            exclusion_reason=None,
        ),
    )

    assert base.effective_declaration.workflow_name == "base"
    assert alternative.effective_declaration.workflow_name == "alternative"
    assert [step.step_name for step in base.effective_declaration.steps] != [
        step.step_name for step in alternative.effective_declaration.steps
    ]
    assert [
        (result.role, result.address_scope)
        for result in base.effective_declaration.results
    ] == [
        (result.role, result.address_scope)
        for result in alternative.effective_declaration.results
    ]


def _selected_sibling_ids(plan: object) -> tuple[int, ...]:
    ref = plan.selected_reused_output_refs[0]
    return tuple(artifact_id for _name, artifact_id in ref.planned_sibling_artifact_ids)


def _artifact_and_dependency_rows(registry_path: Path) -> tuple[list[tuple], list[tuple]]:
    with sqlite3.connect(registry_path) as conn:
        artifacts = conn.execute(
            "SELECT * FROM artifacts WHERE origin = 'workflow_output' "
            "ORDER BY artifact_id"
        ).fetchall()
        dependencies = conn.execute(
            "SELECT * FROM artifact_dependencies "
            "ORDER BY dependent_artifact_id, source_artifact_id, input_path, binding_name"
        ).fetchall()
    return artifacts, dependencies


def _published_sibling_ids(
    registry_path: Path,
    *,
    workflow_name: str,
) -> tuple[int, ...]:
    with sqlite3.connect(registry_path) as conn:
        return tuple(
            row[0]
            for row in conn.execute(
                "SELECT artifact_id FROM published_outputs "
                "WHERE context = 'adapter' AND workflow_name = ? "
                "AND step_name = 'model' AND address = 'entity_001' "
                "ORDER BY output_name",
                (workflow_name,),
            )
        )


def test_member_and_direct_workflows_reuse_exact_ordinary_sibling_bundles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_project(tmp_path, monkeypatch)
    loaded = load_workflow_project(project_dir=project_dir, context="adapter")
    applied_a = apply_provisional_member(loaded=loaded, member=_member())
    member_a_plan = execution_module._build_run_plan_from_loaded_project(
        loaded=applied_a.loaded_project,
        workflow_name="base",
        step_name="model",
        address="entity_001",
    )
    direct_a_plan = build_run_plan(
        project_dir=project_dir,
        context="adapter",
        workflow_name="direct_a",
        step_name="model",
        address="entity_001",
    )
    member_a_job = next(job for job in member_a_plan.jobs if job.step_name == "model")
    direct_a_job = next(job for job in direct_a_plan.jobs if job.step_name == "model")
    assert member_a_job.projection_plan == direct_a_job.projection_plan
    assert member_a_job.projection_state == direct_a_job.projection_state

    direct_a_outcome = execute_run_plan(direct_a_plan, cores=1)
    assert direct_a_outcome.selected_generated_count == 1
    registry_path = runtime_dir / "database/registry.db"
    direct_a_scientific_rows = _artifact_and_dependency_rows(registry_path)
    refreshed_member_a = execution_module._build_run_plan_from_loaded_project(
        loaded=applied_a.loaded_project,
        workflow_name="base",
        step_name="model",
        address="entity_001",
    )
    refreshed_direct_a = build_run_plan(
        project_dir=project_dir,
        context="adapter",
        workflow_name="direct_a",
        step_name="model",
        address="entity_001",
    )
    assert len(_selected_sibling_ids(refreshed_member_a)) == 2
    assert _selected_sibling_ids(refreshed_member_a) == _selected_sibling_ids(
        refreshed_direct_a
    )
    assert (
        refreshed_member_a.selected_reused_output_refs[0]
        .reuse_request.resolved_projection
        == refreshed_direct_a.selected_reused_output_refs[0]
        .reuse_request.resolved_projection
    )
    with monkeypatch.context() as reuse_patch:
        reuse_patch.setattr(
            execution_module,
            "_run_snakemake",
            lambda *_args, **_kwargs: pytest.fail("reuse-only member ran Snakemake"),
        )
        reuse_patch.setattr(
            execution_module,
            "_publish_run_outputs",
            lambda *_args, **_kwargs: pytest.fail("reuse-only member published outputs"),
        )
        reuse_patch.setattr(
            execution_module.shutil,
            "copy2",
            lambda *_args, **_kwargs: pytest.fail("reuse-only member copied data"),
        )
        member_a_outcome = execute_run_plan(member_a_plan, cores=1)
    assert member_a_outcome.selected_generated_count == 0
    assert member_a_outcome.selected_reused_count == 1
    assert _artifact_and_dependency_rows(registry_path) == direct_a_scientific_rows
    assert _published_sibling_ids(
        registry_path,
        workflow_name="base",
    ) == _selected_sibling_ids(refreshed_member_a)
    member_a_payload = (member_a_plan.run_workspace / "run_plan.json").read_text(
        encoding="utf-8"
    )
    for specification_fact in (
        "member-a",
        "member_choice",
        "reported_estimate",
        "reported_diagnostics",
    ):
        assert specification_fact not in member_a_payload

    equivalent_a = apply_provisional_member(
        loaded=loaded,
        member=_member(key="member-a-equivalent"),
    )
    assert equivalent_a.effective_declaration == applied_a.effective_declaration
    equivalent_a_plan = execution_module._build_run_plan_from_loaded_project(
        loaded=equivalent_a.loaded_project,
        workflow_name="base",
        step_name="model",
        address="entity_001",
    )
    assert _selected_sibling_ids(equivalent_a_plan) == _selected_sibling_ids(
        refreshed_member_a
    )

    applied_b = apply_provisional_member(
        loaded=loaded,
        member=_member(key="member-b", suffix="-b"),
    )
    member_b_plan = execution_module._build_run_plan_from_loaded_project(
        loaded=applied_b.loaded_project,
        workflow_name="base",
        step_name="model",
        address="entity_001",
    )
    assert member_b_plan.selected_reused_output_refs == ()
    member_b_outcome = execute_run_plan(member_b_plan, cores=1)
    assert member_b_outcome.selected_generated_count == 1
    member_b_scientific_rows = _artifact_and_dependency_rows(registry_path)

    direct_b_plan = build_run_plan(
        project_dir=project_dir,
        context="adapter",
        workflow_name="direct_b",
        step_name="model",
        address="entity_001",
    )
    assert direct_b_plan.selected_fresh_output_refs == ()
    assert _selected_sibling_ids(direct_b_plan) == _selected_sibling_ids(
        execution_module._build_run_plan_from_loaded_project(
            loaded=applied_b.loaded_project,
            workflow_name="base",
            step_name="model",
            address="entity_001",
        )
    )
    with monkeypatch.context() as reuse_patch:
        reuse_patch.setattr(
            execution_module,
            "_run_snakemake",
            lambda *_args, **_kwargs: pytest.fail("reuse-only direct run ran Snakemake"),
        )
        reuse_patch.setattr(
            execution_module,
            "_publish_run_outputs",
            lambda *_args, **_kwargs: pytest.fail("reuse-only direct run published outputs"),
        )
        reuse_patch.setattr(
            execution_module.shutil,
            "copy2",
            lambda *_args, **_kwargs: pytest.fail("reuse-only direct run copied data"),
        )
        direct_b_outcome = execute_run_plan(direct_b_plan, cores=1)
    assert direct_b_outcome.selected_generated_count == 0
    assert direct_b_outcome.selected_reused_count == 1
    assert _artifact_and_dependency_rows(registry_path) == member_b_scientific_rows
    assert _published_sibling_ids(
        registry_path,
        workflow_name="direct_b",
    ) == _selected_sibling_ids(direct_b_plan)

    with sqlite3.connect(registry_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (18,)
        model_artifacts = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output' "
            "AND step_name = 'model'"
        ).fetchone()[0]
    assert model_artifacts == 4
