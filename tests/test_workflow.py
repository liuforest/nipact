from pathlib import Path
from typing import Any

import pytest
import yaml

from nipact.errors import ValidationError
from nipact.project_setup import init_project
from nipact.workflow import (
    StepInput,
    compile_workflow_plan,
    load_workflow_project,
    validate_workflow_graph,
    workflow_plan_to_graph,
)


def _init_demo(tmp_path: Path) -> tuple[Path, Path]:
    project_dir = tmp_path / "project"
    runtime_dir = tmp_path / "runtime"
    init_project(
        demo="colors",
        project_dir=project_dir,
        runtime_dir=runtime_dir,
        context="colors",
    )
    return project_dir, runtime_dir


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _load(project_dir: Path):
    return load_workflow_project(project_dir=project_dir, context="colors")


def _compile(
    project_dir: Path,
    *,
    workflow_name: str = "base",
    step_name: str = "color_sector_analysis",
):
    return compile_workflow_plan(
        _load(project_dir),
        workflow_name=workflow_name,
        step_name=step_name,
    )


def _graph(
    project_dir: Path,
    *,
    workflow_name: str = "base",
    step_name: str = "color_sector_analysis",
) -> dict[str, Any]:
    return workflow_plan_to_graph(
        _compile(project_dir, workflow_name=workflow_name, step_name=step_name)
    )


def test_generated_colors_demo_workflow_files_load(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    loaded = _load(project_dir)

    assert loaded.source_index.path == project_dir / "sources.yaml"
    assert loaded.source_index.global_bindings == {
        "colors_source": "data/color_source.json",
    }
    assert loaded.source_index.entity_bindings == {}
    assert set(loaded.manifests) == {"demo-40", "init"}
    assert set(loaded.steps) == {
        "color_source",
        "color_features",
        "color_local_transform",
        "color_candidate_select",
        "color_cohort_fit",
        "color_cohort_apply",
        "color_sector_label",
        "color_sector_analysis",
    }
    assert loaded.steps["color_source"].source_inputs == ("colors_source",)
    assert loaded.steps["color_source"].step_contract_version == "1"
    assert set(loaded.workflows) == {"base", "red-qc-target"}
    expected_step_order = (
        "color_source",
        "color_features",
        "color_local_transform",
        "color_candidate_select",
        "color_cohort_fit",
        "color_cohort_apply",
        "color_sector_label",
        "color_sector_analysis",
    )

    base = loaded.workflows["base"]
    assert base.base_workflow is None
    assert base.steps == expected_step_order
    assert base.step_outputs == {
        "color_local_transform": "local_color",
        "color_candidate_select": "selected_color",
        "color_cohort_fit": "cohort_fit",
        "color_cohort_apply": "cohort_color",
        "color_sector_analysis": "sector_counts",
    }

    variant = loaded.workflows["red-qc-target"]
    assert variant.base_workflow == "base"
    assert variant.steps == base.steps
    assert variant.step_outputs == base.step_outputs
    assert variant.step_overrides["color_candidate_select"].params == {
        "qc_target_theta": 0.0
    }


def test_step_contract_version_is_loaded_and_compiled(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    step_path = project_dir / "steps/color_source.yaml"
    payload = _read_yaml(step_path)
    payload["step_contract_version"] = "source-v2"
    _write_yaml(step_path, payload)

    loaded = _load(project_dir)
    plan = compile_workflow_plan(
        loaded,
        workflow_name="base",
        step_name="color_local_transform",
    )

    assert loaded.steps["color_source"].step_contract_version == "source-v2"
    assert plan.steps[0].step_contract_version == "source-v2"
    assert plan.steps[1].step_contract_version == "1"


@pytest.mark.parametrize("value", [1, True, ""])
def test_loader_rejects_invalid_step_contract_version(
    tmp_path: Path,
    value: object,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    step_path = project_dir / "steps/color_source.yaml"
    payload = _read_yaml(step_path)
    payload["step_contract_version"] = value
    _write_yaml(step_path, payload)

    with pytest.raises(
        ValidationError,
        match="step color_source step_contract_version must be a non-empty string",
    ):
        _load(project_dir)


def test_loader_rejects_missing_workflow_file(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    (project_dir / "workflows/base.yaml").unlink()

    with pytest.raises(ValidationError, match="missing YAML file"):
        _load(project_dir)


def test_loader_merges_chained_workflow_inheritance(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    config_path = project_dir / "nipact.yaml"
    config = _read_yaml(config_path)
    config["workflows"]["mid-qc-target"] = "workflows/mid-qc-target.yaml"
    config["workflows"]["leaf-qc-target"] = "workflows/leaf-qc-target.yaml"
    _write_yaml(config_path, config)
    _write_yaml(
        project_dir / "workflows/mid-qc-target.yaml",
        {
            "workflow_name": "mid-qc-target",
            "base_workflow": "red-qc-target",
            "step_overrides": {
                "color_candidate_select": {"params": {"mid_param": "yes"}}
            },
        },
    )
    _write_yaml(
        project_dir / "workflows/leaf-qc-target.yaml",
        {
            "workflow_name": "leaf-qc-target",
            "base_workflow": "mid-qc-target",
            "step_overrides": {
                "color_candidate_select": {"params": {"qc_target_theta": 0.5}}
            },
        },
    )

    loaded = _load(project_dir)
    base = loaded.workflows["base"]
    leaf = loaded.workflows["leaf-qc-target"]

    assert leaf.base_workflow == "mid-qc-target"
    assert leaf.steps == base.steps
    assert leaf.step_outputs == base.step_outputs
    assert leaf.step_overrides["color_candidate_select"].params == {
        "mid_param": "yes",
        "qc_target_theta": 0.5,
    }


def test_compile_base_sector_analysis_plan_includes_expected_steps(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    plan = _compile(project_dir)

    assert plan.workflow_name == "base"
    assert plan.selected_step_name == "color_sector_analysis"
    assert plan.selected_output_name == "sector_counts"
    assert [step.step_name for step in plan.steps] == [
        "color_source",
        "color_features",
        "color_local_transform",
        "color_candidate_select",
        "color_cohort_fit",
        "color_cohort_apply",
        "color_sector_label",
        "color_sector_analysis",
    ]
    assert [binding.role for binding in plan.manifest_bindings] == [
        "source_population",
        "fit_cohort",
        "analysis_cohort",
    ]
    assert plan.steps[0].source_inputs == ("colors_source",)
    assert plan.warnings == ()


def test_compile_base_local_transform_plan_trims_downstream_steps(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    plan = _compile(project_dir, step_name="color_local_transform")

    assert [step.step_name for step in plan.steps] == [
        "color_source",
        "color_features",
        "color_local_transform",
    ]
    assert [binding.role for binding in plan.manifest_bindings] == ["source_population"]


def test_compile_variant_applies_step_override(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    plan = _compile(project_dir, workflow_name="red-qc-target")
    params_by_step = {step.step_name: step.params for step in plan.steps}

    assert params_by_step["color_candidate_select"]["qc_target_theta"] == 0.0
    assert "qc_target_radius" in params_by_step["color_candidate_select"]


def test_compile_rejects_unknown_workflow(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    loaded = _load(project_dir)

    with pytest.raises(ValidationError, match="unknown workflow"):
        compile_workflow_plan(
            loaded,
            workflow_name="missing",
            step_name="color_sector_analysis",
        )


def test_compile_rejects_dependency_cycle(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    loaded = _load(project_dir)
    loaded.steps["color_source"].inputs["cycle"] = StepInput(
        name="cycle",
        artifact="color_sector_analysis.sector_counts",
        dependency_role="cycle",
        source_step_name="color_sector_analysis",
        source_output_name="sector_counts",
    )

    with pytest.raises(ValidationError, match="dependency cycle"):
        compile_workflow_plan(
            loaded,
            workflow_name="base",
            step_name="color_sector_analysis",
        )


def test_compile_manifest_binding_facts_match_loaded_manifests(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    loaded = _load(project_dir)

    plan = compile_workflow_plan(
        loaded,
        workflow_name="base",
        step_name="color_sector_analysis",
    )
    bindings = {(binding.step_name, binding.role): binding for binding in plan.manifest_bindings}

    source_binding = bindings[("color_source", "source_population")]
    source_manifest = loaded.manifests[source_binding.manifest_name]
    assert source_binding.manifest_digest == source_manifest.manifest_digest
    assert source_binding.manifest_hash == source_manifest.manifest_hash
    assert source_binding.entity_count == source_manifest.entity_count

    fit_binding = bindings[("color_cohort_fit", "fit_cohort")]
    fit_manifest = loaded.manifests[fit_binding.manifest_name]
    assert fit_binding.manifest_digest == fit_manifest.manifest_digest
    assert fit_binding.manifest_hash == fit_manifest.manifest_hash
    assert fit_binding.entity_count == fit_manifest.entity_count


def test_graph_projection_has_stable_ids_and_terminal_kind(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    graph = _graph(project_dir)

    assert graph["workflow_name"] == "base"
    assert graph["selected_step_name"] == "color_sector_analysis"
    assert graph["selected_output_name"] == "sector_counts"
    assert graph["terminal_step_kind"] == "analysis"
    assert [node["node_id"] for node in graph["nodes"]] == [
        "step:color_source",
        "step:color_features",
        "step:color_local_transform",
        "step:color_candidate_select",
        "step:color_cohort_fit",
        "step:color_cohort_apply",
        "step:color_sector_label",
        "step:color_sector_analysis",
    ]
    assert [artifact["artifact_id"] for artifact in graph["artifacts"]] == [
        "artifact:color_source:source_color",
        "artifact:color_features:features",
        "artifact:color_local_transform:local_color",
        "artifact:color_candidate_select:selected_color",
        "artifact:color_cohort_fit:cohort_fit",
        "artifact:color_cohort_apply:cohort_color",
        "artifact:color_sector_label:sector_label",
        "artifact:color_sector_analysis:sector_counts",
    ]
    assert {artifact["identity_status"] for artifact in graph["artifacts"]} == {"compiled"}
    assert graph["warnings"] == []


def test_graph_projection_matches_expected_dependency_chain(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    graph = _graph(project_dir)
    edges = [
        (
            edge["source_artifact_id"],
            edge["target_node_id"],
            edge["binding_name"],
            edge["dependency_role"],
        )
        for edge in graph["edges"]
    ]

    assert edges == [
        (
            "artifact:color_source:source_color",
            "step:color_features",
            "source_color",
            "source_input",
        ),
        (
            "artifact:color_features:features",
            "step:color_local_transform",
            "features",
            "source_input",
        ),
        (
            "artifact:color_local_transform:local_color",
            "step:color_candidate_select",
            "local_color",
            "source_input",
        ),
        (
            "artifact:color_candidate_select:selected_color",
            "step:color_cohort_fit",
            "selected_color",
            "fit_input",
        ),
        (
            "artifact:color_candidate_select:selected_color",
            "step:color_cohort_apply",
            "selected_color",
            "apply_input",
        ),
        (
            "artifact:color_cohort_fit:cohort_fit",
            "step:color_cohort_apply",
            "cohort_fit",
            "collective_fit",
        ),
        (
            "artifact:color_cohort_apply:cohort_color",
            "step:color_sector_label",
            "cohort_color",
            "source_input",
        ),
        (
            "artifact:color_sector_label:sector_label",
            "step:color_sector_analysis",
            "sector_label",
            "analysis_input",
        ),
    ]


def test_graph_projection_includes_manifest_bindings_from_dependency_path(
    tmp_path: Path,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    graph = _graph(project_dir)
    bindings = {binding["role"]: binding for binding in graph["manifest_bindings"]}

    assert list(bindings) == ["source_population", "fit_cohort", "analysis_cohort"]
    assert bindings["source_population"]["node_id"] == "step:color_source"
    assert bindings["source_population"]["manifest_name"] == "init"
    assert bindings["source_population"]["entity_count"] == 200
    assert bindings["fit_cohort"]["node_id"] == "step:color_cohort_fit"
    assert bindings["fit_cohort"]["manifest_name"] == "demo-40"
    assert bindings["analysis_cohort"]["node_id"] == "step:color_sector_analysis"
    assert bindings["analysis_cohort"]["manifest_name"] == "init"
    for binding in bindings.values():
        assert binding["binding_source"] == "explicit"
        assert len(binding["manifest_digest"]) == 64
        assert binding["manifest_digest"].startswith(binding["manifest_hash"])
        assert len(binding["manifest_hash"]) == 16


def test_graph_projection_for_trimmed_step_omits_downstream_steps(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    graph = _graph(project_dir, step_name="color_local_transform")

    assert graph["terminal_step_kind"] == "pattern_a"
    assert [node["step_name"] for node in graph["nodes"]] == [
        "color_source",
        "color_features",
        "color_local_transform",
    ]
    assert [binding["role"] for binding in graph["manifest_bindings"]] == [
        "source_population"
    ]


def test_graph_validation_rejects_duplicate_ids(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    graph = _graph(project_dir)
    graph["nodes"][1]["node_id"] = graph["nodes"][0]["node_id"]

    with pytest.raises(ValidationError, match="duplicate node id"):
        validate_workflow_graph(graph)


def test_graph_validation_rejects_bad_references(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    graph = _graph(project_dir)
    graph["edges"][0]["source_artifact_id"] = "artifact:missing:output"

    with pytest.raises(ValidationError, match="unknown source artifact"):
        validate_workflow_graph(graph)
