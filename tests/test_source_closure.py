from __future__ import annotations

from pathlib import Path

import pytest

from nipact.errors import ValidationError
from nipact.source_authority import LogicalSourceCoordinate, SourceDeclaration
from nipact.source_closure import selected_source_declarations
from nipact.workflow import (
    LoadedWorkflowProject,
    SourceIndex,
    StepInput,
    StepOutput,
    WorkflowPlan,
    WorkflowPlanExecutionPopulation,
    WorkflowPlanManifestBinding,
    WorkflowPlanStep,
)


def _step(
    name: str,
    *,
    address_scope: str,
    execution_role: str = "transform",
    source_inputs: tuple[str, ...] = (),
    inputs: dict[str, StepInput] | None = None,
    manifest_members: tuple[str, ...] | None = None,
) -> WorkflowPlanStep:
    manifest_binding = None
    if manifest_members is not None:
        manifest_binding = WorkflowPlanManifestBinding(
            step_name=name,
            manifest_usage_role="scientific_cohort",
            manifest_name=f"{name}_cohort",
            manifest_value_schema="entity_set_v1",
            manifest_digest="a" * 64,
            manifest_hash="a" * 16,
            entity_ids=manifest_members,
            entity_count=len(manifest_members),
        )
    return WorkflowPlanStep(
        step_name=name,
        pattern_kind="pattern_a",
        execution_role=execution_role,
        address_scope=address_scope,
        callable_ref="tests.fake:callable",
        step_contract_version="1",
        inputs={} if inputs is None else inputs,
        source_inputs=source_inputs,
        params={},
        outputs={
            "result": StepOutput(
                name="result",
                extension=".json",
                address_scope=address_scope,
            )
        },
        manifest_binding=manifest_binding,
    )


def _input(
    name: str,
    *,
    source_step: str,
    dependency_role: str,
) -> StepInput:
    return StepInput(
        name=name,
        artifact=f"{source_step}.result",
        dependency_role=dependency_role,
        source_step_name=source_step,
        source_output_name="result",
    )


def _plan(
    *steps: WorkflowPlanStep,
    selected_step: str,
    population: tuple[str, ...] = ("subject_01", "subject_02", "subject_03"),
) -> WorkflowPlan:
    return WorkflowPlan(
        workflow_name="base",
        selected_step_name=selected_step,
        selected_output_name="result",
        steps=steps,
        execution_population=WorkflowPlanExecutionPopulation(
            manifest_name="full",
            manifest_value_schema="entity_set_v1",
            manifest_digest="b" * 64,
            manifest_hash="b" * 16,
            entity_ids=population,
            entity_count=len(population),
        ),
        manifest_bindings=tuple(
            step.manifest_binding
            for step in steps
            if step.manifest_binding is not None
        ),
        warnings=(),
    )


def _loaded(
    tmp_path: Path,
    *,
    global_bindings: dict[str, str] | None = None,
    entity_bindings: dict[str, dict[str, str]] | None = None,
) -> LoadedWorkflowProject:
    return LoadedWorkflowProject(
        project_root=tmp_path / "project",
        context="study",
        runtime_root=tmp_path / "runtime",
        source_index=SourceIndex(
            path=tmp_path / "project" / "sources.yaml",
            global_bindings={} if global_bindings is None else global_bindings,
            entity_bindings={} if entity_bindings is None else entity_bindings,
        ),
        manifests={},
        steps={},
        workflows={},
    )


def _entity_sources(*entity_ids: str) -> dict[str, dict[str, str]]:
    return {
        entity_id: {"t1_image": f"data/{entity_id}/t1.nii.gz"}
        for entity_id in entity_ids
    }


def _coordinate(
    *,
    scope: str,
    source_name: str,
    entity_id: str | None,
) -> LogicalSourceCoordinate:
    return LogicalSourceCoordinate(
        context="study",
        scope=scope,
        source_name=source_name,
        entity_id=entity_id,
    )


def test_targeted_entity_selection_reaches_only_that_entity_source(
    tmp_path: Path,
) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image",),
    )
    transform = _step(
        "transform",
        address_scope="entity",
        inputs={
            "image": _input(
                "image",
                source_step="source",
                dependency_role="source_input",
            )
        },
    )

    declarations = selected_source_declarations(
        loaded=_loaded(
            tmp_path,
            entity_bindings=_entity_sources(
                "subject_01",
                "subject_02",
                "subject_03",
            ),
        ),
        plan=_plan(source, transform, selected_step="transform"),
        requested_address="subject_02",
    )

    assert declarations == (
        SourceDeclaration(
            coordinate=_coordinate(
                scope="entity",
                source_name="t1_image",
                entity_id="subject_02",
            ),
            declared_path="data/subject_02/t1.nii.gz",
            declared_extension=".nii.gz",
        ),
    )


def test_unaddressed_entity_selection_reaches_complete_execution_population(
    tmp_path: Path,
) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image",),
    )

    declarations = selected_source_declarations(
        loaded=_loaded(
            tmp_path,
            entity_bindings=_entity_sources(
                "subject_01",
                "subject_02",
                "subject_03",
            ),
        ),
        plan=_plan(source, selected_step="source"),
        requested_address=None,
    )

    assert tuple(
        declaration.coordinate.entity_id for declaration in declarations
    ) == ("subject_01", "subject_02", "subject_03")


@pytest.mark.parametrize("dependency_role", ["fit_input", "analysis_input"])
def test_scientific_fan_in_reaches_bound_members_not_complete_population(
    tmp_path: Path,
    dependency_role: str,
) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image",),
    )
    cohort = _step(
        "cohort",
        address_scope="cohort",
        inputs={
            "members": _input(
                "members",
                source_step="source",
                dependency_role=dependency_role,
            )
        },
        manifest_members=("subject_01", "subject_03"),
    )

    declarations = selected_source_declarations(
        loaded=_loaded(
            tmp_path,
            entity_bindings=_entity_sources(
                "subject_01",
                "subject_02",
                "subject_03",
            ),
        ),
        plan=_plan(source, cohort, selected_step="cohort"),
        requested_address=None,
    )

    assert tuple(
        declaration.coordinate.entity_id for declaration in declarations
    ) == ("subject_01", "subject_03")


def test_entity_apply_reaches_direct_entity_and_collective_fit_members(
    tmp_path: Path,
) -> None:
    population = ("subject_01", "subject_02", "subject_03", "subject_04")
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image",),
    )
    fit = _step(
        "fit",
        address_scope="cohort",
        inputs={
            "training": _input(
                "training",
                source_step="source",
                dependency_role="fit_input",
            )
        },
        manifest_members=("subject_01", "subject_02", "subject_03"),
    )
    apply = _step(
        "apply",
        address_scope="entity",
        inputs={
            "image": _input(
                "image",
                source_step="source",
                dependency_role="apply_input",
            ),
            "model": _input(
                "model",
                source_step="fit",
                dependency_role="collective_fit",
            ),
        },
    )

    declarations = selected_source_declarations(
        loaded=_loaded(
            tmp_path,
            entity_bindings=_entity_sources(*population),
        ),
        plan=_plan(
            source,
            fit,
            apply,
            selected_step="apply",
            population=population,
        ),
        requested_address="subject_04",
    )

    assert tuple(
        declaration.coordinate.entity_id for declaration in declarations
    ) == population


def test_shared_global_source_is_deduplicated_across_entity_jobs(tmp_path: Path) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("atlas",),
    )

    declarations = selected_source_declarations(
        loaded=_loaded(
            tmp_path,
            global_bindings={"atlas": "data/reference/atlas.json"},
        ),
        plan=_plan(source, selected_step="source"),
        requested_address=None,
    )

    assert declarations == (
        SourceDeclaration(
            coordinate=_coordinate(
                scope="global",
                source_name="atlas",
                entity_id=None,
            ),
            declared_path="data/reference/atlas.json",
            declared_extension=".json",
        ),
    )


def test_multiple_source_inputs_keep_names_and_deterministic_order(
    tmp_path: Path,
) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image", "atlas"),
    )

    declarations = selected_source_declarations(
        loaded=_loaded(
            tmp_path,
            global_bindings={"atlas": "data/reference/atlas.json"},
            entity_bindings={
                "subject_01": {"t1_image": "data/subject_01/t1.nii.gz"}
            },
        ),
        plan=_plan(source, selected_step="source"),
        requested_address="subject_01",
    )

    assert tuple(
        (
            declaration.coordinate.scope,
            declaration.coordinate.source_name,
            declaration.coordinate.entity_id,
        )
        for declaration in declarations
    ) == (
        ("global", "atlas", None),
        ("entity", "t1_image", "subject_01"),
    )


def test_missing_source_binding_fails(tmp_path: Path) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image",),
    )

    with pytest.raises(ValidationError, match="missing source binding"):
        selected_source_declarations(
            loaded=_loaded(tmp_path),
            plan=_plan(source, selected_step="source"),
            requested_address="subject_01",
        )


def test_ambiguous_global_and_entity_source_binding_fails(tmp_path: Path) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("atlas",),
    )

    with pytest.raises(ValidationError, match="ambiguous source binding"):
        selected_source_declarations(
            loaded=_loaded(
                tmp_path,
                global_bindings={"atlas": "data/reference/atlas.json"},
                entity_bindings={
                    "subject_01": {"atlas": "data/subject_01/atlas.json"}
                },
            ),
            plan=_plan(source, selected_step="source"),
            requested_address="subject_01",
        )


def test_requested_entity_must_belong_to_execution_population(tmp_path: Path) -> None:
    source = _step(
        "source",
        address_scope="entity",
        execution_role="source_import",
        source_inputs=("t1_image",),
    )

    with pytest.raises(ValidationError, match="not a member of execution_population"):
        selected_source_declarations(
            loaded=_loaded(
                tmp_path,
                entity_bindings=_entity_sources("subject_01"),
            ),
            plan=_plan(source, selected_step="source"),
            requested_address="subject_99",
        )


def test_cohort_selection_rejects_entity_address(tmp_path: Path) -> None:
    cohort_source = _step(
        "cohort_source",
        address_scope="cohort",
        execution_role="source_import",
        source_inputs=("table",),
    )

    with pytest.raises(ValidationError, match="cohort-addressed"):
        selected_source_declarations(
            loaded=_loaded(
                tmp_path,
                global_bindings={"table": "data/cohort/table.csv"},
            ),
            plan=_plan(cohort_source, selected_step="cohort_source"),
            requested_address="subject_01",
        )
