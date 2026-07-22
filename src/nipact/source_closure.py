"""Structural source-closure traversal for selected workflow work."""

from __future__ import annotations

from .errors import ValidationError
from .identity import validate_path_token
from .source_authority import (
    LogicalSourceCoordinate,
    SourceDeclaration,
    declared_source_extension,
)
from .workflow import (
    LoadedWorkflowProject,
    SourceIndex,
    WorkflowPlan,
    WorkflowPlanExecutionPopulation,
    WorkflowPlanStep,
)


def selected_source_declarations(
    *,
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
    requested_address: str | None = None,
) -> tuple[SourceDeclaration, ...]:
    """Return declared sources in the selected semantic dependency closure."""
    if not isinstance(loaded, LoadedWorkflowProject):
        raise ValidationError("loaded project must be a LoadedWorkflowProject")
    if not isinstance(plan, WorkflowPlan):
        raise ValidationError("workflow plan must be a WorkflowPlan")

    steps_by_name: dict[str, WorkflowPlanStep] = {}
    for step in plan.steps:
        if step.step_name in steps_by_name:
            raise ValidationError(
                f"workflow plan contains duplicate step: {step.step_name}"
            )
        steps_by_name[step.step_name] = step
    selected_step = _required_step(steps_by_name, plan.selected_step_name)
    if plan.selected_output_name not in selected_step.outputs:
        raise ValidationError(
            "workflow plan selected output is missing from selected step: "
            f"{plan.selected_step_name}.{plan.selected_output_name}"
        )

    pending = [
        (selected_step.step_name, address)
        for address in _selected_addresses(
            plan,
            selected_step,
            requested_address=requested_address,
        )
    ]
    visited: set[tuple[str, str]] = set()
    declarations_by_coordinate: dict[
        LogicalSourceCoordinate,
        SourceDeclaration,
    ] = {}

    while pending:
        step_name, address = pending.pop()
        job_coordinate = (step_name, address)
        if job_coordinate in visited:
            continue
        visited.add(job_coordinate)
        step = _required_step(steps_by_name, step_name)

        if step.execution_role == "source_import":
            for source_name in step.source_inputs:
                declaration = source_declaration_for_binding(
                    context=loaded.context,
                    source_index=loaded.source_index,
                    source_name=source_name,
                    address=address,
                )
                previous = declarations_by_coordinate.get(declaration.coordinate)
                if previous is not None and previous != declaration:
                    raise ValidationError(
                        "logical source coordinate has conflicting declarations"
                    )
                declarations_by_coordinate[declaration.coordinate] = declaration

        for step_input in step.inputs.values():
            source_step = _required_step(
                steps_by_name,
                step_input.source_step_name,
            )
            if step_input.source_output_name not in source_step.outputs:
                raise ValidationError(
                    f"step {step.step_name!r} input {step_input.name!r} references "
                    "an unknown upstream output"
                )
            pending.extend(
                (source_step.step_name, source_address)
                for source_address in _source_addresses(
                    consumer_step=step,
                    source_step=source_step,
                    dependency_role=step_input.dependency_role,
                    consumer_address=address,
                )
            )

    return tuple(
        sorted(
            declarations_by_coordinate.values(),
            key=_source_declaration_sort_key,
        )
    )


def selected_job_coordinates(
    *,
    plan: WorkflowPlan,
    requested_address: str | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return the selected semantic job closure in deterministic order."""
    if not isinstance(plan, WorkflowPlan):
        raise ValidationError("workflow plan must be a WorkflowPlan")
    steps_by_name = {step.step_name: step for step in plan.steps}
    if len(steps_by_name) != len(plan.steps):
        raise ValidationError("workflow plan contains duplicate steps")
    selected_step = _required_step(steps_by_name, plan.selected_step_name)
    pending = [
        (selected_step.step_name, address)
        for address in _selected_addresses(
            plan,
            selected_step,
            requested_address=requested_address,
        )
    ]
    visited: set[tuple[str, str]] = set()
    while pending:
        step_name, address = pending.pop()
        coordinate = (step_name, address)
        if coordinate in visited:
            continue
        visited.add(coordinate)
        step = _required_step(steps_by_name, step_name)
        for step_input in step.inputs.values():
            source_step = _required_step(steps_by_name, step_input.source_step_name)
            pending.extend(
                (source_step.step_name, source_address)
                for source_address in _source_addresses(
                    consumer_step=step,
                    source_step=source_step,
                    dependency_role=step_input.dependency_role,
                    consumer_address=address,
                )
            )
    step_order = {step.step_name: index for index, step in enumerate(plan.steps)}
    return tuple(sorted(visited, key=lambda item: (step_order[item[0]], item[1])))


def _selected_addresses(
    plan: WorkflowPlan,
    selected_step: WorkflowPlanStep,
    *,
    requested_address: str | None,
) -> tuple[str, ...]:
    if selected_step.address_scope == "cohort":
        if requested_address is not None:
            raise ValidationError(
                f"selected step {selected_step.step_name!r} is cohort-addressed "
                "and cannot be targeted by entity address"
            )
        return ("cohort",)
    if selected_step.address_scope != "entity":
        raise ValidationError(
            f"unsupported selected address_scope: {selected_step.address_scope!r}"
        )
    population = _required_execution_population(plan)
    if requested_address is None:
        return population.entity_ids
    address = validate_path_token(requested_address, label="address")
    if address not in population.entity_ids:
        raise ValidationError(
            f"address {address!r} is not a member of execution_population "
            f"{population.manifest_name!r}"
        )
    return (address,)


def _source_addresses(
    *,
    consumer_step: WorkflowPlanStep,
    source_step: WorkflowPlanStep,
    dependency_role: str,
    consumer_address: str,
) -> tuple[str, ...]:
    if dependency_role in {"source_input", "apply_input"}:
        return (consumer_address,)
    if dependency_role == "collective_fit":
        if source_step.address_scope != "cohort":
            raise ValidationError("collective_fit input must reference a cohort step")
        return ("cohort",)
    if dependency_role in {"fit_input", "analysis_input"}:
        if source_step.address_scope != "entity":
            raise ValidationError(
                f"{dependency_role} input must reference an entity step"
            )
        binding = consumer_step.manifest_binding
        if binding is None:
            raise ValidationError(
                f"step {consumer_step.step_name!r} {dependency_role} input requires "
                "a scientific manifest binding"
            )
        return binding.entity_ids
    raise ValidationError(
        f"unsupported dependency role in source closure: {dependency_role}"
    )


def source_declaration_for_binding(
    *,
    context: str,
    source_index: SourceIndex,
    source_name: str,
    address: str,
) -> SourceDeclaration:
    global_path = source_index.global_bindings.get(source_name)
    entity_path = source_index.entity_bindings.get(address, {}).get(source_name)
    if global_path is not None and entity_path is not None:
        raise ValidationError(
            f"ambiguous source binding {source_name!r} for address {address!r}"
        )
    if entity_path is not None:
        coordinate = LogicalSourceCoordinate(
            context=context,
            scope="entity",
            source_name=source_name,
            entity_id=address,
        )
        declared_path = entity_path
    elif global_path is not None:
        coordinate = LogicalSourceCoordinate(
            context=context,
            scope="global",
            source_name=source_name,
            entity_id=None,
        )
        declared_path = global_path
    else:
        raise ValidationError(
            f"missing source binding {source_name!r} for address {address!r}"
        )
    return SourceDeclaration(
        coordinate=coordinate,
        declared_path=declared_path,
        declared_extension=declared_source_extension(declared_path),
    )


def _required_step(
    steps_by_name: dict[str, WorkflowPlanStep],
    step_name: str,
) -> WorkflowPlanStep:
    try:
        return steps_by_name[step_name]
    except KeyError as exc:
        raise ValidationError(
            f"workflow plan is missing required step: {step_name}"
        ) from exc


def _required_execution_population(
    plan: WorkflowPlan,
) -> WorkflowPlanExecutionPopulation:
    if plan.execution_population is None:
        raise ValidationError("entity workflow steps require an execution_population")
    return plan.execution_population


def _source_declaration_sort_key(
    declaration: SourceDeclaration,
) -> tuple[str, int, str, str]:
    coordinate = declaration.coordinate
    scope_order = 0 if coordinate.scope == "global" else 1
    return (
        coordinate.context,
        scope_order,
        coordinate.source_name,
        coordinate.entity_id or "",
    )
