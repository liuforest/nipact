import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from nipact.errors import ValidationError
from nipact.project_setup import init_project
from nipact.workflow import (
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


def _assert_invalid(project_dir: Path, kind: str, name: str, diagnostic: str) -> None:
    loaded = _load(project_dir)
    issue = loaded.declaration_issue(kind, name)
    assert issue is not None and issue.status == "invalid"
    assert issue.path.name in issue.error
    assert re.search(diagnostic, issue.error)
    assert name not in getattr(loaded, {"step": "steps", "workflow": "workflows"}[kind])


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


def test_loader_discovers_direct_files_and_preserves_symlink_names(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    for directory in ("steps", "workflows", "manifests"):
        folder = project_dir / directory
        (folder / ".#draft.yaml").write_text("malformed: [", encoding="utf-8")
        (folder / "notes.txt").write_text("malformed: [", encoding="utf-8")
        (folder / "parked").mkdir()
        (folder / "parked/bad.yaml").write_text("malformed: [", encoding="utf-8")
    for relative, field in (
        ("steps/color_source.yaml", "step_name"),
        ("workflows/base.yaml", "workflow_name"),
    ):
        path = project_dir / relative
        payload = _read_yaml(path)
        del payload[field]
        target = path.parent / "parked/different-name.yaml"
        _write_yaml(target, payload)
        path.unlink()
        path.symlink_to(target)

    loaded = _load(project_dir)
    assert loaded.steps["color_source"].source_path == (
        project_dir / "steps/parked/different-name.yaml"
    )
    assert loaded.workflows["base"].source_path == (
        project_dir / "workflows/parked/different-name.yaml"
    )

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
    assert base.execution_population_name == "init"
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
    assert variant.execution_population_name == "init"
    assert variant.steps == base.steps
    assert variant.step_outputs == base.step_outputs
    assert variant.step_overrides["color_candidate_select"].params == {
        "qc_target_theta": 0.0
    }


@pytest.mark.parametrize(
    "case,diagnostic",
    [
        ("legacy-layout", "unknown field\\(s\\): manifests, sources, steps, workflows"),
        ("typo", "unknown field\\(s\\): specification"),
        ("context", "context must be a non-empty string"),
        ("runtime", "missing paths.runtime"),
        ("sources-missing", "missing YAML file: .*sources.yaml"),
        ("sources-empty", "source index must declare at least one source binding"),
        *[
            (
                f"{directory}-{state}",
                f"missing {directory} " + ("directory" if state == "missing" else "YAML files"),
            )
            for directory in ("steps", "workflows", "manifests")
            for state in ("missing", "empty")
        ],
    ],
)
def test_loader_rejects_invalid_project_layout(
    tmp_path: Path, case: str, diagnostic: str
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    config_path = project_dir / "nipact.yaml"
    config = _read_yaml(config_path)
    if case == "legacy-layout":
        config.update(
            {
                "sources": {"index": "sources.yaml"},
                "steps": {"directory": "steps"},
                "workflows": {
                    "base": "workflows/base.yaml",
                    "red-qc-target": "workflows/red-qc-target.yaml",
                },
                "manifests": {
                    "init": "manifests/init.yaml",
                    "demo-40": "manifests/demo-40.yaml",
                },
            }
        )
    elif case == "typo":
        config["specification"] = {}
    elif case == "context":
        del config["context"]
    elif case == "runtime":
        del config["paths"]["runtime"]
    elif case == "sources-missing":
        (project_dir / "sources.yaml").unlink()
    elif case == "sources-empty":
        _write_yaml(project_dir / "sources.yaml", {})
    else:
        directory, state = case.split("-")
        folder = project_dir / directory
        for path in folder.iterdir():
            path.unlink()
        if state == "missing":
            folder.rmdir()
    _write_yaml(config_path, config)

    with pytest.raises(ValidationError, match=diagnostic):
        _load(project_dir)


@pytest.mark.parametrize(
    "case,diagnostic",
    [
        ("yml-only", "rename to use the .yaml extension"),
        ("uppercase-extension", "rename to use the .yaml extension"),
        ("unsafe-stem", "contains unsupported characters: _draft"),
        (
            "casefold-pair",
            "name collision: .*Color_source.yaml and .*color_source.yaml",
        ),
        ("directory", "must be a regular file"),
        ("broken-link", "must be a regular file"),
        ("escaping-link", "must stay inside project dir"),
        ("escaping-directory", "steps directory must stay inside project dir"),
    ],
)
def test_loader_rejects_invalid_discovered_declaration(
    tmp_path: Path, case: str, diagnostic: str
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    steps = project_dir / "steps"
    if case == "yml-only":
        workflows = project_dir / "workflows"
        for path in workflows.iterdir():
            path.unlink()
        (workflows / "draft.yml").write_text("{}", encoding="utf-8")
    elif case == "uppercase-extension":
        (project_dir / "manifests/draft.YAML").write_text("{}", encoding="utf-8")
    elif case == "unsafe-stem":
        (steps / "_draft.yaml").write_text("{}", encoding="utf-8")
    elif case == "casefold-pair":
        (steps / "Color_source.yaml").write_text(
            (steps / "color_source.yaml").read_text(encoding="utf-8"), encoding="utf-8"
        )
    elif case == "directory":
        (steps / "draft.yaml").mkdir()
    elif case == "broken-link":
        (steps / "draft.yaml").symlink_to(steps / "absent")
    elif case == "escaping-link":
        outside = tmp_path / "outside.yaml"
        outside.write_text("{}", encoding="utf-8")
        (steps / "draft.yaml").symlink_to(outside)
    else:
        outside = tmp_path / "outside-steps"
        steps.rename(outside)
        steps.symlink_to(outside, target_is_directory=True)

    if case == "escaping-directory":
        with pytest.raises(ValidationError, match=diagnostic):
            _load(project_dir)
        return
    loaded = _load(project_dir)
    invalid = [
        record
        for records in loaded.declarations.values()
        for record in records
        if record.status == "invalid"
    ]
    assert invalid and all(re.search(diagnostic, record.error) for record in invalid)
    assert all(record.path.name in record.error for record in invalid)
    if case == "casefold-pair":
        assert {record.name for record in invalid} == {"Color_source", "color_source"}
        assert "color_source" not in loaded.steps
    elif case == "yml-only":
        assert [(record.kind, record.name) for record in invalid] == [
            ("workflow", "draft")
        ]
        assert loaded.workflows == {}


@pytest.mark.parametrize("demo", ["colors", "fmri", "dfc", "directories"])
def test_omitted_declaration_fields_preserve_loaded_project(
    tmp_path: Path, demo: str
) -> None:
    project_dir = tmp_path / "project"
    init_project(
        demo=demo,
        project_dir=project_dir,
        runtime_dir=tmp_path / "runtime",
        context=demo,
    )
    if demo == "colors":
        _write_yaml(
            project_dir / "workflows/inherited-qc-target.yaml",
            {
                "workflow_name": "inherited-qc-target",
                "base_workflow": "red-qc-target",
                "step_overrides": {},
            },
        )

    explicit = load_workflow_project(project_dir=project_dir, context=demo)
    if demo == "colors":
        assert explicit.workflows["inherited-qc-target"].step_overrides[
            "color_candidate_select"
        ].params["qc_target_theta"] == 0.0

    for step_path in (project_dir / "steps").glob("*.yaml"):
        payload = _read_yaml(step_path)
        del payload["step_name"]
        for output in payload["outputs"].values():
            del output["address_scope"]
        _write_yaml(step_path, payload)
    for workflow_path in (project_dir / "workflows").glob("*.yaml"):
        payload = _read_yaml(workflow_path)
        del payload["workflow_name"]
        if payload.get("step_overrides") == {}:
            del payload["step_overrides"]
        _write_yaml(workflow_path, payload)

    shortened = load_workflow_project(project_dir=project_dir, context=demo)
    assert shortened == explicit


@pytest.mark.parametrize(
    "declaration,field,values,diagnostic",
    [
        (
            "steps/color_source.yaml", "step_name", [None, "", False],
            "step color_source.yaml step_name must be a non-empty string",
        ),
        (
            "workflows/base.yaml", "workflow_name", [None, "", False],
            "workflow base workflow_name must be a non-empty string",
        ),
        (
            "workflows/red-qc-target.yaml", "workflow_name", [None, "", False],
            "workflow red-qc-target workflow_name must be a non-empty string",
        ),
        (
            "steps/color_source.yaml", "address_scope", [None, "", False],
            "outputs 'source_color' address_scope must be a non-empty string",
        ),
        (
            "workflows/red-qc-target.yaml", "step_overrides", [None, [], False],
            "workflow red-qc-target step_overrides must be a mapping",
        ),
        (
            "steps/color_source.yaml", "step_name", ["different-source"],
            "must match file name",
        ),
        (
            "workflows/base.yaml", "workflow_name", ["different-base"],
            "must match file name",
        ),
        (
            "workflows/red-qc-target.yaml", "workflow_name", ["different-variant"],
            "must match file name",
        ),
        (
            "steps/color_source.yaml", "address_scope", ["unsupported"],
            "address_scope must be one of",
        ),
    ],
)
def test_loader_rejects_invalid_supplied_defaultable_fields(
    tmp_path: Path,
    declaration: str,
    field: str,
    values: list[object],
    diagnostic: str,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    path = project_dir / declaration
    payload = _read_yaml(path)
    target = payload["outputs"]["source_color"] if field == "address_scope" else payload
    for value in values:
        target[field] = value
        _write_yaml(path, payload)
        kind = "step" if declaration.startswith("steps/") else "workflow"
        _assert_invalid(project_dir, kind, path.stem, diagnostic)


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


def test_loader_rejects_missing_step_contract_version(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    step_path = project_dir / "steps/color_source.yaml"
    payload = _read_yaml(step_path)
    del payload["step_contract_version"]
    _write_yaml(step_path, payload)

    _assert_invalid(
        project_dir,
        "step",
        "color_source",
        "missing required field.*step_contract_version",
    )


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

    _assert_invalid(
        project_dir,
        "step",
        "color_source",
        "step color_source step_contract_version must be a non-empty string",
    )


def test_loader_rejects_missing_base_workflow(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    (project_dir / "workflows/base.yaml").unlink()

    _assert_invalid(
        project_dir, "workflow", "red-qc-target", "unknown base_workflow: base"
    )


def test_loader_merges_chained_workflow_inheritance(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    original = _load(project_dir)
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
    assert {
        name: loaded.workflows[name] for name in original.workflows
    } == original.workflows
    base = loaded.workflows["base"]
    leaf = loaded.workflows["leaf-qc-target"]

    assert leaf.base_workflow == "mid-qc-target"
    assert leaf.execution_population_name == "init"
    assert leaf.steps == base.steps
    assert leaf.step_outputs == base.step_outputs
    assert leaf.step_overrides["color_candidate_select"].params == {
        "mid_param": "yes",
        "qc_target_theta": 0.5,
    }


def test_derived_workflow_can_override_execution_population(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    workflow_path = project_dir / "workflows/red-qc-target.yaml"
    payload = _read_yaml(workflow_path)
    payload["execution_population"] = "demo-40"
    _write_yaml(workflow_path, payload)

    loaded = _load(project_dir)
    plan = compile_workflow_plan(
        loaded,
        workflow_name="red-qc-target",
        step_name="color_local_transform",
    )

    assert loaded.workflows["red-qc-target"].execution_population_name == "demo-40"
    assert plan.execution_population is not None
    assert plan.execution_population.manifest_name == "demo-40"


def test_loader_rejects_unknown_execution_population(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    workflow_path = project_dir / "workflows/base.yaml"
    payload = _read_yaml(workflow_path)
    payload["execution_population"] = "missing"
    _write_yaml(workflow_path, payload)

    _assert_invalid(project_dir, "workflow", "base", "unknown execution_population")


def test_compile_requires_execution_population_for_entity_path(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    workflow_path = project_dir / "workflows/base.yaml"
    payload = _read_yaml(workflow_path)
    del payload["execution_population"]
    _write_yaml(workflow_path, payload)

    with pytest.raises(ValidationError, match="requires an execution_population"):
        _compile(project_dir, step_name="color_local_transform")


def test_population_free_cohort_only_workflow_is_valid(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    _write_yaml(
        project_dir / "steps/cohort_summary.yaml",
        {
            "step_name": "cohort_summary",
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "cohort",
            "callable": (
                "nipact.examples.colors_processing_demo.runtime:"
                "color_sector_analysis_file"
            ),
            "outputs": {
                "summary": {"extension": ".json", "address_scope": "cohort"}
            },
        },
    )
    _write_yaml(
        project_dir / "workflows/cohort-only.yaml",
        {
            "workflow_name": "cohort-only",
            "steps": [{"step_name": "cohort_summary", "output_name": "summary"}],
        },
    )

    plan = _compile(
        project_dir,
        workflow_name="cohort-only",
        step_name="cohort_summary",
    )

    assert plan.execution_population is None
    assert plan.manifest_bindings == ()


def test_loader_rejects_output_scope_mismatch_in_unused_step(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    _write_yaml(
        project_dir / "steps/unused_summary.yaml",
        {
            "step_name": "unused_summary",
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "cohort",
            "callable": (
                "nipact.examples.colors_processing_demo.runtime:"
                "color_sector_analysis_file"
            ),
            "outputs": {
                "counts": {"extension": ".json", "address_scope": "cohort"},
                "table": {"extension": ".json", "address_scope": "entity"},
            },
        },
    )

    _assert_invalid(
        project_dir,
        "step",
        "unused_summary",
        "outputs 'table' address_scope must match step address_scope",
    )


def test_loader_rejects_reserved_source_population_role(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    step_path = project_dir / "steps/color_source.yaml"
    payload = _read_yaml(step_path)
    payload["manifest_binding"] = {
        "role": "source_population",
        "manifest": "init",
    }
    _write_yaml(step_path, payload)

    _assert_invalid(
        project_dir, "step", "color_source", "role 'source_population' is reserved"
    )


def test_loader_requires_scientific_manifest_for_collection_input(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    step_path = project_dir / "steps/color_cohort_fit.yaml"
    payload = _read_yaml(step_path)
    del payload["manifest_binding"]
    _write_yaml(step_path, payload)

    _assert_invalid(
        project_dir,
        "step",
        "color_cohort_fit",
        "requires a scientific manifest_binding",
    )


def test_compile_rejects_scientific_manifest_outside_execution_population(
    tmp_path: Path,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)
    manifest_path = project_dir / "manifests/demo-40.yaml"
    payload = _read_yaml(manifest_path)
    payload["entities"].append("outside_entity")
    _write_yaml(manifest_path, payload)

    with pytest.raises(ValidationError, match="is not a subset"):
        _compile(project_dir, step_name="color_cohort_fit")


def test_compile_variant_applies_step_override(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    plan = _compile(project_dir, workflow_name="red-qc-target")
    params_by_step = {step.step_name: step.params for step in plan.steps}

    assert params_by_step["color_candidate_select"]["qc_target_theta"] == 0.0
    assert "qc_target_radius" in params_by_step["color_candidate_select"]


def test_loader_rejects_input_from_later_workflow_step(tmp_path: Path) -> None:
    # A forward reference is the only way to declare a dependency cycle.
    project_dir, _runtime_dir = _init_demo(tmp_path)
    step_path = project_dir / "steps/color_features.yaml"
    payload = _read_yaml(step_path)
    payload["inputs"]["cycle"] = {
        "artifact": "color_sector_analysis.sector_counts",
        "dependency_role": "source_input",
    }
    _write_yaml(step_path, payload)

    _assert_invalid(
        project_dir,
        "workflow",
        "base",
        (
            "step 'color_features' input 'color_sector_analysis.sector_counts' "
            "does not reference an earlier workflow output"
        ),
    )


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
    manifests = _load(project_dir).manifests

    graph = _graph(project_dir)
    population = graph["execution_population"]
    assert population["manifest_name"] == "init"
    assert population["manifest_value_schema"] == "entity_set_v1"
    assert population["entity_count"] == 200
    bindings = {
        binding["manifest_usage_role"]: binding
        for binding in graph["manifest_bindings"]
    }

    assert list(bindings) == ["fit_cohort", "analysis_cohort"]
    assert bindings["fit_cohort"]["node_id"] == "step:color_cohort_fit"
    assert bindings["fit_cohort"]["manifest_name"] == "demo-40"
    assert bindings["analysis_cohort"]["node_id"] == "step:color_sector_analysis"
    assert bindings["analysis_cohort"]["manifest_name"] == "init"
    for binding in bindings.values():
        assert binding["binding_source"] == "explicit"
    # Each projected manifest carries the facts of the manifest it names.
    for projected in [population, *bindings.values()]:
        manifest = manifests[projected["manifest_name"]]
        assert projected["manifest_value_schema"] == manifest.manifest_value_schema
        assert projected["manifest_digest"] == manifest.manifest_digest
        assert projected["manifest_hash"] == manifest.manifest_hash
        assert projected["entity_count"] == manifest.entity_count


def test_graph_projection_for_trimmed_step_omits_downstream_steps(tmp_path: Path) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path)

    graph = _graph(project_dir, step_name="color_local_transform")

    assert graph["terminal_step_kind"] == "pattern_a"
    assert [node["step_name"] for node in graph["nodes"]] == [
        "color_source",
        "color_features",
        "color_local_transform",
    ]
    assert graph["execution_population"]["manifest_name"] == "init"
    assert graph["manifest_bindings"] == []


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


@pytest.mark.parametrize(
    "output,valid",
    [
        ({"extension": ".json", "address_scope": "entity"}, True),
        ({"kind": "file", "extension": ".json", "address_scope": "entity"}, True),
        ({"kind": "directory", "address_scope": "entity"}, True),
        ({"kind": "directory", "extension": None, "address_scope": "entity"}, False),
        ({"kind": "directory", "extension": ".zip", "address_scope": "entity"}, False),
        ({"kind": "folder", "address_scope": "entity"}, False),
        ({"kind": "directory"}, True),
    ],
)
def test_output_kind_loader_and_compiled_graph(tmp_path, monkeypatch, output, valid):
    from directory_support import directory_project, change_step

    project, runtime = directory_project(tmp_path, monkeypatch)
    change_step(project, "source_text", outputs={"raw_text": output})
    if not valid:
        loaded = load_workflow_project(project_dir=project, context="mini")
        assert loaded.declaration_issue("step", "source_text").status == "invalid"
        return
    loaded = load_workflow_project(project_dir=project, context="mini")
    graph = workflow_plan_to_graph(
        compile_workflow_plan(loaded, workflow_name="main", step_name="source_text")
    )
    validate_workflow_graph(graph)
    artifact = graph["artifacts"][0]
    assert artifact["kind"] == output.get("kind", "file")
    assert artifact["extension"] == output.get("extension")


def test_explicit_file_kind_keeps_public_request_and_frozen_bytes(
    tmp_path, monkeypatch
):
    from directory_support import directory_project, change_step, plan, run
    from nipact.specification_execution import compile_specification_snapshot
    from nipact.specification_loading import ExplicitSpecificationSource

    project, runtime = directory_project(tmp_path, monkeypatch)
    assert run(project, "source_text", "sub_001").all_selected_resolved
    request = (
        plan(project, "source_text", "sub_001")
        .selected_reused_output_refs[0]
        .reuse_request.resolved_projection
    )
    source = project / "file-spec.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "schema": "nipact/specification-set/v1",
                "specification_set": {"key": "files"},
                "libraries": [],
                "fixed": {
                    "workflow": "main",
                    "execution_population": "subjects",
                    "target": {"step": "source_text", "output": "raw_text"},
                    "results": {"text": {"step": "source_text", "output": "raw_text"}},
                },
                "members": [
                    {
                        "key": "one",
                        "decision_coordinates": {"choice": "one"},
                        "disposition": "included",
                    }
                ],
                "expected_counts": {"candidates": 1, "included": 1, "excluded": 0},
            }
        )
    )
    before = compile_specification_snapshot(
        project_dir=project, context="mini", source=ExplicitSpecificationSource(source)
    ).snapshot
    change_step(
        project,
        "source_text",
        outputs={
            "raw_text": {
                "kind": "file",
                "extension": ".json",
                "address_scope": "entity",
            }
        },
    )
    after = compile_specification_snapshot(
        project_dir=project, context="mini", source=ExplicitSpecificationSource(source)
    ).snapshot
    assert before.canonical_bytes == after.canonical_bytes
    current = (
        plan(project, "source_text", "sub_001")
        .selected_reused_output_refs[0]
        .reuse_request.resolved_projection
    )
    assert current.canonical_json == request.canonical_json
    assert current.request_bundle_digest == request.request_bundle_digest


@pytest.mark.parametrize(
    "declaration,case",
    [
        ("workflows/draft.yaml", "yaml"),
        ("workflows/draft.yaml", "date"),
        ("manifests/draft.yaml", "date"),
        ("workflows/draft.yaml", "unicode"),
        ("manifests/draft.yaml", "unicode"),
        ("steps/draft.yaml", "read"),
        ("manifests/draft.yaml", "read"),
        ("workflows/draft.yaml", "fields"),
        ("manifests/draft.yaml", "fields"),
        *[("steps/draft.yaml", field) for field in ("inputs", "outputs", "params")],
        ("workflows/draft.yaml", "step_overrides"),
    ],
)
def test_loader_isolates_read_and_mapping_errors(
    tmp_path, monkeypatch, declaration, case
):
    project_dir, _ = _init_demo(tmp_path)
    original = _load(project_dir)
    path = project_dir / declaration
    if case == "unicode":
        path.write_bytes(b"\xff")
    elif case == "date":
        path.write_text("draft: 2026-13-01\n")
    elif case in {"yaml", "read"}:
        path.write_text("malformed: [\n")
        if case == "read":
            read_text = Path.read_text

            def fail_read(self, *args, **kwargs):
                if self == path:
                    raise OSError("declaration read failed")
                return read_text(self, *args, **kwargs)

            monkeypatch.setattr(Path, "read_text", fail_read)
    elif case == "fields":
        _write_yaml(path, {"description": "draft", 1: "mixed", False: "keys"})
    elif case == "step_overrides":
        _write_yaml(path, {"base_workflow": "base", case: {"color_source": {}, 1: {}}})
    else:
        payload = _read_yaml(project_dir / "steps/color_features.yaml")
        payload["step_name"] = "draft"
        payload[case] = {"mixed": {}, 1: {}}
        _write_yaml(path, payload)

    loaded = _load(project_dir)
    issue = loaded.declaration_issue(declaration.split("/")[0][:-1], "draft")
    assert issue.status == "invalid" and str(path) in issue.error
    expected = (
        "keys must be strings"
        if case not in {"yaml", "date", "unicode", "read"}
        else {
            "yaml": "invalid YAML",
            "date": "month must be in 1..12",
            "unicode": "decode",
            "read": "declaration read failed",
        }[case]
    )
    assert expected in issue.error
    assert loaded.steps == original.steps
    assert loaded.workflows == original.workflows
    assert loaded.manifests == original.manifests
    assert loaded.manifest_paths == original.manifest_paths


def test_loader_rejects_both_same_stem_extensions_without_fallback(tmp_path):
    project_dir, _ = _init_demo(tmp_path)
    base = project_dir / "workflows/base.yaml"
    base.with_suffix(".yml").write_text(base.read_text())
    loaded = _load(project_dir)
    records = loaded.declarations[("workflow", "base")]
    assert {record.path.suffix for record in records} == {".yaml", ".yml"}
    assert all(
        record.status == "invalid" and "collision" in record.error for record in records
    )
    assert "base" not in loaded.workflows
    assert loaded.declaration_issue("workflow", "red-qc-target").status == "unavailable"


@pytest.mark.parametrize(
    "root",
    [
        "step",
        "manifest",
        "workflow",
        "missing-source",
        "missing-step",
        "missing-manifest",
        "cycle",
    ],
)
def test_loader_propagates_root_errors_without_partial_workflows(tmp_path, root):
    project_dir, _ = _init_demo(tmp_path)
    original = _load(project_dir)
    _write_yaml(
        project_dir / "workflows/independent.yaml",
        {
            "execution_population": "init",
            "steps": [
                {"step_name": "color_local_transform", "output_name": "local_color"}
            ],
        },
    )
    # The independent chain uses only the first three valid steps.
    independent = _read_yaml(project_dir / "workflows/independent.yaml")
    independent["steps"] = ["color_source", "color_features", *independent["steps"]]
    _write_yaml(project_dir / "workflows/independent.yaml", independent)
    _write_yaml(
        project_dir / "workflows/leaf.yaml",
        {"base_workflow": "red-qc-target", "execution_population": "init"},
    )
    if root == "cycle":
        _write_yaml(
            project_dir / "workflows/base.yaml", {"base_workflow": "red-qc-target"}
        )
        expected_kind, expected_name = "workflow", "base"
    elif root == "missing-source":
        path = project_dir / "steps/color_cohort_fit.yaml"
        payload = _read_yaml(path)
        next(iter(payload["inputs"].values()))["artifact"] = "absent.result"
        _write_yaml(path, payload)
        expected_kind, expected_name = "step", "color_cohort_fit"
    elif root == "missing-step":
        payload = _read_yaml(project_dir / "workflows/base.yaml")
        payload["steps"].append("absent")
        _write_yaml(project_dir / "workflows/base.yaml", payload)
        expected_kind, expected_name = "workflow", "base"
    elif root == "missing-manifest":
        payload = _read_yaml(project_dir / "steps/color_cohort_fit.yaml")
        payload["manifest_binding"]["manifest"] = "absent"
        _write_yaml(project_dir / "steps/color_cohort_fit.yaml", payload)
        expected_kind, expected_name = "step", "color_cohort_fit"
    else:
        expected_kind, expected_name = {
            "step": ("step", "color_cohort_fit"),
            "manifest": ("manifest", "demo-40"),
            "workflow": ("workflow", "base"),
        }[root]
        (project_dir / f"{expected_kind}s/{expected_name}.yaml").write_text(
            "malformed: [\n"
        )

    loaded = _load(project_dir)
    issue = loaded.declaration_issue(expected_kind, expected_name)
    assert issue.status == "invalid" and issue.path.name in issue.error
    assert loaded.workflows["independent"].steps == original.workflows["base"].steps[:3]
    assert all(
        loaded.steps[name] == original.steps[name]
        for name in loaded.workflows["independent"].steps
    )
    for name in ("base", "red-qc-target", "leaf"):
        status = loaded.declaration_issue("workflow", name)
        expected_status = (
            "invalid"
            if (name == "base" and expected_kind == "workflow")
            or (root == "cycle" and name == "red-qc-target")
            else "unavailable"
        )
        assert status.status == expected_status
        if root != "cycle":
            assert status.error == issue.error
        assert name not in loaded.workflows
    if expected_kind in {"step", "manifest"}:
        for name in (
            "color_cohort_fit",
            "color_cohort_apply",
            "color_sector_label",
            "color_sector_analysis",
        ):
            record = loaded.declarations[("step", name)][0]
            assert record.error == issue.error
    # Even an earlier target cannot select a partially usable workflow.
    with pytest.raises(
        ValidationError,
        match=re.escape(issue.error) if root != "cycle" else "inheritance cycle",
    ):
        compile_workflow_plan(
            loaded, workflow_name="base", step_name="color_local_transform"
        )
