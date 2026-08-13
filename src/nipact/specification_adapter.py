"""Apply provisional specification members to ordinary workflow declarations."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace

from .errors import ValidationError
from .specification_compiler import (
    ExecutionPopulationWrite,
    ManifestBindingWrite,
    ParameterWrite,
    ProvisionalMember,
    ResultWrite,
    SpecificationWrite,
    TargetWrite,
)
from .workflow import (
    LoadedWorkflowProject,
    StepDefinition,
    WorkflowPlanExecutionPopulation,
    WorkflowPlanManifestBinding,
    WorkflowPlanStep,
    WorkflowStepOverride,
    compile_workflow_plan,
)


@dataclass(frozen=True)
class EffectiveResultRole:
    role: str
    step_name: str
    output_name: str
    address_scope: str


@dataclass(frozen=True)
class ProvisionalEffectiveDeclaration:
    workflow_name: str
    target_step_name: str
    target_output_name: str
    steps: tuple[WorkflowPlanStep, ...]
    execution_population: WorkflowPlanExecutionPopulation | None
    manifest_bindings: tuple[WorkflowPlanManifestBinding, ...]
    results: tuple[EffectiveResultRole, ...]


@dataclass(frozen=True)
class AppliedProvisionalMember:
    member: ProvisionalMember
    loaded_project: LoadedWorkflowProject
    effective_declaration: ProvisionalEffectiveDeclaration


def apply_provisional_member(
    *,
    loaded: LoadedWorkflowProject,
    member: ProvisionalMember,
) -> AppliedProvisionalMember:
    """Apply one compiled member without mutating ordinary declarations."""
    if type(loaded) is not LoadedWorkflowProject:
        raise ValidationError("loaded must be a LoadedWorkflowProject")
    if type(member) is not ProvisionalMember:
        raise ValidationError("member must be a ProvisionalMember")

    try:
        selected_workflow = loaded.workflows[member.workflow_name]
    except KeyError as exc:
        raise ValidationError(
            f"specification member references unknown workflow: {member.workflow_name}"
        ) from exc

    writes = tuple(member.writes)
    _validate_write_destinations(writes)
    target = _single_target(writes)
    result_writes = tuple(write for write in writes if isinstance(write, ResultWrite))
    if not result_writes:
        raise ValidationError("specification member requires at least one result role")

    workflow_steps = set(selected_workflow.steps)
    for write in writes:
        if isinstance(write, ParameterWrite):
            step = _selected_step(
                loaded,
                workflow_steps=workflow_steps,
                step_name=write.step_name,
                label="parameter write",
            )
            if write.parameter_name not in step.params:
                raise ValidationError(
                    "parameter write references undeclared top-level parameter: "
                    f"{write.step_name}.{write.parameter_name}"
                )
        elif isinstance(write, ExecutionPopulationWrite):
            _require_manifest(loaded, write.manifest_name, label="execution population")
        elif isinstance(write, ManifestBindingWrite):
            step = _selected_step(
                loaded,
                workflow_steps=workflow_steps,
                step_name=write.step_name,
                label="manifest-binding write",
            )
            if step.manifest_binding is None:
                raise ValidationError(
                    f"manifest-binding write step has no binding: {write.step_name}"
                )
            if step.manifest_binding.role != write.role:
                raise ValidationError(
                    "manifest-binding write role does not match the declared role: "
                    f"{write.step_name}.{write.role}"
                )
            _require_manifest(loaded, write.manifest_name, label="manifest binding")
        elif isinstance(write, TargetWrite):
            step = _selected_step(
                loaded,
                workflow_steps=workflow_steps,
                step_name=write.step_name,
                label="target",
            )
            if write.output_name not in step.outputs:
                raise ValidationError(
                    "target references unknown output: "
                    f"{write.step_name}.{write.output_name}"
                )
        elif isinstance(write, ResultWrite):
            step = _selected_step(
                loaded,
                workflow_steps=workflow_steps,
                step_name=write.step_name,
                label="result",
            )
            if write.output_name not in step.outputs:
                raise ValidationError(
                    "result references unknown output: "
                    f"{write.step_name}.{write.output_name}"
                )
        else:
            raise ValidationError(
                f"unsupported specification write: {type(write).__name__}"
            )

    steps = dict(loaded.steps)
    workflows = dict(loaded.workflows)
    step_overrides = dict(selected_workflow.step_overrides)
    step_outputs = dict(selected_workflow.step_outputs)
    execution_population_name = selected_workflow.execution_population_name

    for write in writes:
        if isinstance(write, ParameterWrite):
            existing = step_overrides.get(
                write.step_name,
                WorkflowStepOverride(step_name=write.step_name, params={}),
            )
            params = dict(existing.params)
            params[write.parameter_name] = deepcopy(write.value)
            step_overrides[write.step_name] = replace(existing, params=params)
        elif isinstance(write, ExecutionPopulationWrite):
            execution_population_name = write.manifest_name
        elif isinstance(write, ManifestBindingWrite):
            step = steps[write.step_name]
            binding = step.manifest_binding
            if binding is None:
                raise AssertionError("validated manifest-binding write lost its binding")
            steps[write.step_name] = replace(
                step,
                manifest_binding=replace(binding, manifest_name=write.manifest_name),
            )
        elif isinstance(write, TargetWrite):
            step_outputs[write.step_name] = write.output_name

    workflows[member.workflow_name] = replace(
        selected_workflow,
        execution_population_name=execution_population_name,
        step_outputs=step_outputs,
        step_overrides=step_overrides,
    )
    effective_project = replace(loaded, steps=steps, workflows=workflows)
    plan = compile_workflow_plan(
        effective_project,
        workflow_name=member.workflow_name,
        step_name=target.step_name,
    )
    if plan.selected_output_name != target.output_name:
        raise ValidationError("effective workflow target does not match the member target")

    closure_steps = {step.step_name for step in plan.steps}
    for write in writes:
        if isinstance(write, (ParameterWrite, ManifestBindingWrite)):
            if write.step_name not in closure_steps:
                raise ValidationError(
                    "specification write is outside the selected target closure: "
                    f"{write.step_name}"
                )
    if any(isinstance(write, ExecutionPopulationWrite) for write in writes):
        if not any(
            step.address_scope == "entity"
            or step.manifest_binding is not None
            or any(
                step_input.dependency_role in {"fit_input", "analysis_input"}
                for step_input in step.inputs.values()
            )
            for step in plan.steps
        ):
            raise ValidationError(
                "execution-population write is unused by the selected target closure"
            )

    results = _validated_results(
        plan.steps,
        target=target,
        writes=result_writes,
    )
    declaration = ProvisionalEffectiveDeclaration(
        workflow_name=plan.workflow_name,
        target_step_name=plan.selected_step_name,
        target_output_name=plan.selected_output_name,
        steps=tuple(_owned_plan_step(step) for step in plan.steps),
        execution_population=plan.execution_population,
        manifest_bindings=tuple(plan.manifest_bindings),
        results=results,
    )
    return AppliedProvisionalMember(
        member=deepcopy(member),
        loaded_project=effective_project,
        effective_declaration=declaration,
    )


def _selected_step(
    loaded: LoadedWorkflowProject,
    *,
    workflow_steps: set[str],
    step_name: str,
    label: str,
) -> StepDefinition:
    if step_name not in workflow_steps:
        raise ValidationError(f"{label} references step outside the workflow: {step_name}")
    try:
        return loaded.steps[step_name]
    except KeyError as exc:
        raise ValidationError(f"{label} references unknown step: {step_name}") from exc


def _require_manifest(
    loaded: LoadedWorkflowProject,
    manifest_name: str,
    *,
    label: str,
) -> None:
    if manifest_name not in loaded.manifests or manifest_name not in loaded.manifest_paths:
        raise ValidationError(f"{label} references unknown manifest: {manifest_name}")


def _single_target(writes: tuple[SpecificationWrite, ...]) -> TargetWrite:
    targets = tuple(write for write in writes if isinstance(write, TargetWrite))
    if len(targets) != 1:
        raise ValidationError("specification member requires exactly one target")
    return targets[0]


def _validate_write_destinations(writes: tuple[SpecificationWrite, ...]) -> None:
    destinations: set[tuple[str, ...]] = set()
    for write in writes:
        if isinstance(write, ParameterWrite):
            destination = ("parameter", write.step_name, write.parameter_name)
        elif isinstance(write, ExecutionPopulationWrite):
            destination = ("execution_population",)
        elif isinstance(write, ManifestBindingWrite):
            destination = ("manifest_binding", write.step_name, write.role)
        elif isinstance(write, TargetWrite):
            destination = ("target",)
        elif isinstance(write, ResultWrite):
            destination = ("result", write.role)
        else:
            raise ValidationError(
                f"unsupported specification write: {type(write).__name__}"
            )
        if destination in destinations:
            raise ValidationError(
                f"specification member writes destination more than once: {destination}"
            )
        destinations.add(destination)


def _validated_results(
    plan_steps: tuple[WorkflowPlanStep, ...],
    *,
    target: TargetWrite,
    writes: tuple[ResultWrite, ...],
) -> tuple[EffectiveResultRole, ...]:
    target_step = next(
        (step for step in plan_steps if step.step_name == target.step_name),
        None,
    )
    if target_step is None:
        raise ValidationError("effective target step is absent from its dependency closure")
    target_output = target_step.outputs[target.output_name]
    seen_roles: set[str] = set()
    seen_ports: set[tuple[str, str]] = set()
    results: list[EffectiveResultRole] = []
    target_count = 0
    for write in sorted(writes, key=lambda item: item.role):
        if write.role in seen_roles:
            raise ValidationError(f"duplicate result role: {write.role}")
        seen_roles.add(write.role)
        port = (write.step_name, write.output_name)
        if port in seen_ports:
            raise ValidationError("one result port cannot satisfy multiple roles")
        seen_ports.add(port)
        if write.step_name != target.step_name:
            raise ValidationError("result ports must be siblings on the target step")
        try:
            output = target_step.outputs[write.output_name]
        except KeyError as exc:
            raise ValidationError(
                "result references unknown target output: "
                f"{write.step_name}.{write.output_name}"
            ) from exc
        if output.address_scope != target_output.address_scope:
            raise ValidationError(
                "result output address scope does not match the target output"
            )
        if port == (target.step_name, target.output_name):
            target_count += 1
        results.append(
            EffectiveResultRole(
                role=write.role,
                step_name=write.step_name,
                output_name=write.output_name,
                address_scope=output.address_scope,
            )
        )
    if target_count != 1:
        raise ValidationError("result roles must include the target output exactly once")
    return tuple(results)


def _owned_plan_step(step: WorkflowPlanStep) -> WorkflowPlanStep:
    return replace(
        step,
        inputs=dict(step.inputs),
        params=deepcopy(step.params),
        outputs=dict(step.outputs),
    )
