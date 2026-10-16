"""Read-only workflow declaration loading for NIPACT projects."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from importlib import import_module
from pathlib import Path, PurePosixPath
import sys
from typing import Any

import yaml

from .errors import ValidationError
from .hashing import SHORT_HASH_LENGTH, is_valid_digest
from .identity import validate_path_token
from .manifest import MANIFEST_VALUE_SCHEMA, Manifest, load_manifest
from .specification_config import parse_specification_registrations

STEP_FIELDS = frozenset(
    {
        "step_name",
        "pattern_kind",
        "execution_role",
        "address_scope",
        "callable",
        "step_contract_version",
        "inputs",
        "source_inputs",
        "params",
        "manifest_binding",
        "outputs",
    }
)
REQUIRED_STEP_FIELDS = frozenset(
    {
        "pattern_kind",
        "execution_role",
        "address_scope",
        "callable",
        "step_contract_version",
        "outputs",
    }
)
INPUT_FIELDS = frozenset({"artifact", "dependency_role"})
OUTPUT_FIELDS = frozenset({"extension", "address_scope", "kind"})
MANIFEST_BINDING_FIELDS = frozenset({"role", "manifest"})
BASE_WORKFLOW_FIELDS = frozenset(
    {"workflow_name", "execution_population", "steps"}
)
REQUIRED_BASE_WORKFLOW_FIELDS = frozenset({"steps"})
VARIANT_WORKFLOW_FIELDS = frozenset(
    {"workflow_name", "base_workflow", "execution_population", "step_overrides"}
)
REQUIRED_VARIANT_WORKFLOW_FIELDS = frozenset({"base_workflow"})
WORKFLOW_STEP_FIELDS = frozenset({"step_name", "output_name"})
REQUIRED_WORKFLOW_STEP_FIELDS = frozenset({"step_name"})
OVERRIDE_FIELDS = frozenset({"params"})
PATTERN_KINDS = frozenset({"pattern_a", "pattern_b", "analysis"})
EXECUTION_ROLES = frozenset(
    {
        "source_import",
        "transform",
        "pattern_b_barrier",
        "b_fit",
        "b_apply",
        "b_export",
        "analysis",
    }
)
ADDRESS_SCOPES = frozenset({"entity", "cohort"})
DEPENDENCY_ROLES = frozenset(
    {"source_input", "fit_input", "apply_input", "collective_fit", "analysis_input"}
)
IDENTITY_STATUSES = frozenset(
    {"compiled", "runtime_owned", "not_applicable", "static_docs_artifact"}
)
TERMINAL_STEP_KINDS = frozenset({"pattern_a", "pattern_b_barrier", "analysis"})
MANIFEST_BINDING_SOURCES = frozenset({"explicit", "defaulted", "requested"})
SOURCE_INDEX_FIELDS = frozenset({"global", "entities"})
SOURCE_PATH_PREFIX = "data/"
PATH_GLOB_CHARS = frozenset("*?[]{}")


@dataclass(frozen=True)
class StepInput:
    name: str
    artifact: str
    dependency_role: str
    source_step_name: str
    source_output_name: str


@dataclass(frozen=True)
class StepOutput:
    name: str
    extension: str | None
    address_scope: str
    kind: str = "file"


@dataclass(frozen=True)
class ManifestBinding:
    role: str
    manifest_name: str


@dataclass(frozen=True)
class StepDefinition:
    name: str
    pattern_kind: str
    execution_role: str
    address_scope: str
    callable_ref: str
    step_contract_version: str
    inputs: dict[str, StepInput]
    source_inputs: tuple[str, ...]
    params: dict[str, Any]
    outputs: dict[str, StepOutput]
    manifest_binding: ManifestBinding | None
    source_path: Path


@dataclass(frozen=True)
class WorkflowStepOverride:
    step_name: str
    params: dict[str, Any]


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    base_workflow: str | None
    execution_population_name: str | None
    steps: tuple[str, ...]
    step_outputs: dict[str, str]
    step_overrides: dict[str, WorkflowStepOverride]
    source_path: Path


@dataclass(frozen=True)
class SourceIndex:
    path: Path
    global_bindings: dict[str, str]
    entity_bindings: dict[str, dict[str, str]]


@dataclass(frozen=True)
class DeclarationRecord:
    kind: str
    name: str
    path: Path
    status: str = "valid"
    error: str | None = None


@dataclass(frozen=True)
class LoadedWorkflowProject:
    project_root: Path
    context: str
    runtime_root: Path
    source_index: SourceIndex
    manifests: dict[str, Manifest]
    manifest_paths: dict[str, Path]
    steps: dict[str, StepDefinition]
    workflows: dict[str, WorkflowDefinition]
    declarations: dict[tuple[str, str], tuple[DeclarationRecord, ...]]

    def declaration_issue(self, kind: str, name: str) -> DeclarationRecord | None:
        return next(
            (
                record
                for record in self.declarations.get((kind, name), ())
                if record.status != "valid"
            ),
            None,
        )

    def require_usable(self, kind: str, name: str) -> None:
        issue = self.declaration_issue(kind, name)
        if issue is not None:
            raise ValidationError(f"{kind} {name!r} is {issue.status}: {issue.error}")

    def workflow(self, name: str) -> WorkflowDefinition:
        self.require_usable("workflow", name)
        try:
            return self.workflows[name]
        except KeyError as exc:
            raise ValidationError(f"unknown workflow: {name}") from exc


def warn_unusable_declarations(loaded: LoadedWorkflowProject) -> None:
    issues = [
        record
        for records in loaded.declarations.values()
        for record in records
        if record.status != "valid"
    ]
    if issues:
        details = "; ".join(
            f"{record.path.relative_to(loaded.project_root)} ({record.status})"
            for record in issues
        )
        print(
            f"WARNING: unusable declarations: {details}; run nipact validate",
            file=sys.stderr,
        )


@dataclass(frozen=True)
class WorkflowPlanManifestBinding:
    step_name: str
    manifest_usage_role: str
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str
    manifest_hash: str
    entity_ids: tuple[str, ...]
    entity_count: int


@dataclass(frozen=True)
class WorkflowPlanExecutionPopulation:
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str
    manifest_hash: str
    entity_ids: tuple[str, ...]
    entity_count: int


@dataclass(frozen=True)
class WorkflowPlanStep:
    step_name: str
    pattern_kind: str
    execution_role: str
    address_scope: str
    callable_ref: str
    step_contract_version: str
    inputs: dict[str, StepInput]
    source_inputs: tuple[str, ...]
    params: dict[str, Any]
    outputs: dict[str, StepOutput]
    manifest_binding: WorkflowPlanManifestBinding | None


@dataclass(frozen=True)
class WorkflowPlan:
    workflow_name: str
    selected_step_name: str
    selected_output_name: str
    steps: tuple[WorkflowPlanStep, ...]
    execution_population: WorkflowPlanExecutionPopulation | None
    manifest_bindings: tuple[WorkflowPlanManifestBinding, ...]
    warnings: tuple[str, ...]


def load_workflow_project(*, project_dir: Path, context: str) -> LoadedWorkflowProject:
    """Load and validate project-local workflow declarations."""
    context = validate_path_token(context, label="context")
    project_root = project_dir.expanduser().resolve()
    if not project_root.is_dir():
        raise ValidationError(f"project dir does not exist: {project_dir}")

    config = _load_yaml_mapping(project_root / "nipact.yaml", label="nipact.yaml")
    _check_fields(
        config,
        allowed=frozenset({"context", "paths", "specifications", "specification_libraries"}),
        required=frozenset(),
        label="nipact.yaml",
    )
    parse_specification_registrations(config)
    if _required_string(config, "context", "nipact.yaml context") != context:
        raise ValidationError(f"context mismatch in nipact.yaml: expected {context!r}")
    runtime_root = _runtime_root(project_root, config)
    source_index = _load_source_index(
        _resolve_project_path(project_root, "sources.yaml", label="sources.yaml"),
        runtime_root=runtime_root,
    )

    declarations: dict[tuple[str, str], tuple[DeclarationRecord, ...]] = {}
    candidates = {
        kind: _discover_project_files(project_root, directory, kind=kind)
        for kind, directory in (
            ("manifest", "manifests"),
            ("step", "steps"),
            ("workflow", "workflows"),
        )
    }
    for records in candidates.values():
        for record in records:
            key = (record.kind, record.name)
            declarations[key] = (*declarations.get(key, ()), record)

    def load_kind(kind: str, parser: Callable[[Path, str], Any]) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for record in candidates[kind]:
            if record.status != "valid":
                continue
            try:
                values[record.name] = parser(record.path.resolve(), record.name)
            except ValidationError as exc:
                _set_declaration_issue(
                    declarations, kind, record.name, "invalid", str(exc)
                )
        return values

    manifest_names = {record.name for record in candidates["manifest"]}
    step_names = {record.name for record in candidates["step"]}
    manifests = load_kind(
        "manifest", lambda path, name: _load_manifest(path, label=f"manifest {name!r}")
    )
    manifest_paths = {
        record.name: record.path.resolve()
        for record in candidates["manifest"]
        if record.name in manifests
    }
    steps = load_kind(
        "step",
        lambda path, name: _load_step(path, name=name, manifest_names=manifest_names),
    )
    for name, step in tuple(steps.items()):
        missing = next(
            (
                item.source_step_name
                for item in step.inputs.values()
                if item.source_step_name not in step_names
            ),
            None,
        )
        if missing is not None:
            _set_declaration_issue(
                declarations,
                "step",
                name,
                "invalid",
                f"step {name!r} references unknown source step: {missing}",
            )
            del steps[name]
    while True:
        unavailable = []
        for name, step in steps.items():
            dependencies = [
                ("step", item.source_step_name) for item in step.inputs.values()
            ]
            if step.manifest_binding is not None:
                dependencies.append(("manifest", step.manifest_binding.manifest_name))
            for key in dependencies:
                issue = next(
                    (
                        record
                        for record in declarations[key]
                        if record.status != "valid"
                    ),
                    None,
                )
                if issue is not None:
                    _set_declaration_issue(
                        declarations, "step", name, "unavailable", issue.error
                    )
                    unavailable.append(name)
                    break
        if not unavailable:
            break
        for name in unavailable:
            del steps[name]

    raw_workflows = load_kind(
        "workflow", lambda path, name: _load_workflow(path, name=name)
    )
    workflows = _resolve_workflows(
        raw_workflows, steps=steps, declarations=declarations
    )

    return LoadedWorkflowProject(
        project_root=project_root,
        context=context,
        runtime_root=runtime_root,
        source_index=source_index,
        manifests=manifests,
        manifest_paths=manifest_paths,
        steps=steps,
        workflows=workflows,
        declarations=declarations,
    )


def compile_workflow_plan(
    loaded: LoadedWorkflowProject,
    *,
    workflow_name: str,
    step_name: str,
) -> WorkflowPlan:
    """Compile one runnable workflow step into a read-only dependency plan."""
    workflow_name = validate_path_token(workflow_name, label="workflow_name")
    selected_step_name = validate_path_token(step_name, label="step_name")
    workflow = loaded.workflow(workflow_name)
    try:
        selected_output_name = workflow.step_outputs[selected_step_name]
    except KeyError as exc:
        raise ValidationError(
            f"workflow {workflow_name!r} step is not runnable: {selected_step_name}"
        ) from exc

    selected_step = _workflow_step(loaded, workflow, selected_step_name)
    if selected_output_name not in selected_step.outputs:
        raise ValidationError(
            f"workflow {workflow.name!r} step {selected_step_name!r} references unknown output: "
            f"{selected_step_name}.{selected_output_name}"
        )

    required_steps = set(
        _collect_dependency_step_names(
            loaded,
            workflow,
            root_step_name=selected_step_name,
        )
    )
    execution_population = _compile_execution_population(loaded, workflow)
    required_definitions = tuple(
        _workflow_step(loaded, workflow, required_step_name)
        for required_step_name in workflow.steps
        if required_step_name in required_steps
    )
    if _requires_execution_population(required_definitions) and execution_population is None:
        raise ValidationError(
            f"workflow {workflow.name!r} requires an execution_population for "
            "the selected dependency path"
        )
    plan_steps: list[WorkflowPlanStep] = []
    manifest_bindings: list[WorkflowPlanManifestBinding] = []

    for step_name in workflow.steps:
        if step_name not in required_steps:
            continue
        step = _workflow_step(loaded, workflow, step_name)
        manifest_binding = _compile_manifest_binding(loaded, step)
        if manifest_binding is not None:
            if execution_population is None:
                raise ValidationError(
                    f"step {step.name!r} scientific manifest requires an "
                    "execution_population"
                )
            missing_members = sorted(
                set(manifest_binding.entity_ids)
                - set(execution_population.entity_ids)
            )
            if missing_members:
                preview = ", ".join(missing_members[:5])
                raise ValidationError(
                    f"step {step.name!r} scientific manifest is not a subset of "
                    f"execution_population {execution_population.manifest_name!r}: "
                    f"{preview}"
                )
            manifest_bindings.append(manifest_binding)
        params = dict(step.params)
        if step_name in workflow.step_overrides:
            params.update(workflow.step_overrides[step_name].params)
        plan_steps.append(
            WorkflowPlanStep(
                step_name=step.name,
                pattern_kind=step.pattern_kind,
                execution_role=step.execution_role,
                address_scope=step.address_scope,
                callable_ref=step.callable_ref,
                step_contract_version=step.step_contract_version,
                inputs=dict(step.inputs),
                source_inputs=step.source_inputs,
                params=params,
                outputs=dict(step.outputs),
                manifest_binding=manifest_binding,
            )
        )

    return WorkflowPlan(
        workflow_name=workflow.name,
        selected_step_name=selected_step_name,
        selected_output_name=selected_output_name,
        steps=tuple(plan_steps),
        execution_population=execution_population,
        manifest_bindings=tuple(manifest_bindings),
        warnings=(),
    )


def workflow_plan_to_graph(plan: WorkflowPlan) -> dict[str, Any]:
    """Project one compiled workflow plan into JSON-ready graph data."""
    steps_by_name = {step.step_name: step for step in plan.steps}
    try:
        selected_step = steps_by_name[plan.selected_step_name]
    except KeyError as exc:
        raise ValidationError(
            "workflow plan selected step is missing from dependency path: "
            f"{plan.selected_step_name}"
        ) from exc
    if plan.selected_output_name not in selected_step.outputs:
        raise ValidationError(
            "workflow plan selected output is missing from selected step: "
            f"{plan.selected_step_name}.{plan.selected_output_name}"
        )

    nodes: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    artifact_ids: dict[tuple[str, str], str] = {}
    step_order = {step.step_name: index for index, step in enumerate(plan.steps)}

    for step in plan.steps:
        nodes.append(
            {
                "node_id": _graph_node_id(step.step_name),
                "label": step.step_name,
                "step_name": step.step_name,
                "pattern_kind": step.pattern_kind,
                "execution_role": step.execution_role,
                "address_scope": step.address_scope,
                "group_id": None,
                "contract_digest": None,
                "address_template": None,
            }
        )
        for output_name, output in step.outputs.items():
            artifact_id = _graph_artifact_id(step.step_name, output_name)
            artifact_ids[(step.step_name, output_name)] = artifact_id
            artifacts.append(
                {
                    "artifact_id": artifact_id,
                    "producer_node_id": _graph_node_id(step.step_name),
                    "step_name": step.step_name,
                    "output_name": output_name,
                    "extension": output.extension,
                    "kind": output.kind,
                    "address_scope": output.address_scope,
                    "hash_version": None,
                    "param_hash": None,
                    "full_param_digest": None,
                    "lineage_digest": None,
                    "identity_status": "compiled",
                }
            )

    edges: list[dict[str, Any]] = []
    for step in plan.steps:
        step_inputs = sorted(
            step.inputs.items(),
            key=lambda item: (
                step_order.get(item[1].source_step_name, len(step_order)),
                item[0],
            ),
        )
        for input_name, step_input in step_inputs:
            source_key = (step_input.source_step_name, step_input.source_output_name)
            source_artifact_id = artifact_ids.get(source_key)
            if source_artifact_id is None:
                raise ValidationError(
                    f"workflow graph input {step.step_name}.{input_name} references "
                    f"artifact outside the compiled plan: {step_input.artifact}"
                )
            target_node_id = _graph_node_id(step.step_name)
            edge_id = _graph_edge_id(
                source_artifact_id,
                target_node_id,
                input_name,
                step_input.dependency_role,
            )
            edges.append(
                {
                    "edge_id": edge_id,
                    "source_artifact_id": source_artifact_id,
                    "target_node_id": target_node_id,
                    "binding_name": input_name,
                    "dependency_role": step_input.dependency_role,
                }
            )

    manifest_bindings = [
        {
            "node_id": _graph_node_id(binding.step_name),
            "group_id": None,
            "manifest_usage_role": binding.manifest_usage_role,
            "manifest_name": binding.manifest_name,
            "manifest_value_schema": binding.manifest_value_schema,
            "manifest_digest": binding.manifest_digest,
            "manifest_hash": binding.manifest_hash,
            "entity_count": binding.entity_count,
            "binding_source": "explicit",
        }
        for binding in plan.manifest_bindings
    ]
    execution_population = None
    if plan.execution_population is not None:
        execution_population = {
            "manifest_name": plan.execution_population.manifest_name,
            "manifest_value_schema": (
                plan.execution_population.manifest_value_schema
            ),
            "manifest_digest": plan.execution_population.manifest_digest,
            "manifest_hash": plan.execution_population.manifest_hash,
            "entity_count": plan.execution_population.entity_count,
        }

    graph = {
        "workflow_name": plan.workflow_name,
        "selected_step_name": plan.selected_step_name,
        "selected_output_name": plan.selected_output_name,
        "terminal_step_kind": _terminal_step_kind(selected_step),
        "nodes": nodes,
        "artifacts": artifacts,
        "edges": edges,
        "execution_population": execution_population,
        "manifest_bindings": manifest_bindings,
        "warnings": list(plan.warnings),
    }
    validate_workflow_graph(graph)
    return graph


def validate_workflow_graph(graph: Mapping[str, Any]) -> None:
    """Validate the graph consistency checks owned by the Python projection."""
    validate_path_token(
        _graph_string(graph, "workflow_name", label="workflow graph"),
        label="workflow_name",
    )
    validate_path_token(
        _graph_string(graph, "selected_step_name", label="workflow graph"),
        label="selected_step_name",
    )
    _graph_allowed_string(
        graph,
        "terminal_step_kind",
        allowed=TERMINAL_STEP_KINDS,
        label="workflow graph",
    )
    nodes = _graph_list(graph, "nodes")
    artifacts = _graph_list(graph, "artifacts")
    edges = _graph_list(graph, "edges")
    execution_population = graph.get("execution_population")
    if execution_population is not None:
        if not isinstance(execution_population, Mapping):
            raise ValidationError(
                "workflow graph execution_population must be a mapping or null"
            )
        _validate_graph_manifest_value(
            execution_population,
            label="execution population",
        )
    manifest_bindings = _graph_list(graph, "manifest_bindings")
    _graph_list_of_strings(graph, "warnings")

    node_ids = _unique_graph_ids(nodes, field="node_id", label="node")
    artifact_ids = _unique_graph_ids(artifacts, field="artifact_id", label="artifact")
    _unique_graph_ids(edges, field="edge_id", label="edge")

    step_names = set()
    group_ids = set()
    artifact_keys = set()
    for node in nodes:
        _graph_string(node, "label", label="node")
        step_name = validate_path_token(
            _graph_string(node, "step_name", label="node"),
            label="node step_name",
        )
        step_names.add(step_name)
        _graph_allowed_string(node, "pattern_kind", allowed=PATTERN_KINDS, label="node")
        _graph_allowed_string(
            node,
            "execution_role",
            allowed=EXECUTION_ROLES,
            label="node",
        )
        _graph_allowed_string(node, "address_scope", allowed=ADDRESS_SCOPES, label="node")
        _graph_nullable_digest(node, "contract_digest", label="node")
        _graph_nullable_string(node, "address_template", label="node")
        group_id = node.get("group_id")
        if group_id is not None:
            if not isinstance(group_id, str):
                raise ValidationError(
                    "workflow graph node group_id must be a string or null"
                )
            group_ids.add(group_id)
    for artifact in artifacts:
        producer_node_id = _graph_string(
            artifact,
            "producer_node_id",
            label="artifact",
        )
        if producer_node_id not in node_ids:
            raise ValidationError(
                f"workflow graph artifact references unknown producer node: {producer_node_id}"
            )
        step_name = validate_path_token(
            _graph_string(artifact, "step_name", label="artifact"),
            label="artifact step_name",
        )
        output_name = validate_path_token(
            _graph_string(artifact, "output_name", label="artifact"),
            label="artifact output_name",
        )
        kind = _graph_allowed_string(
            artifact, "kind", allowed={"file", "directory"}, label="artifact"
        )
        extension = artifact.get("extension")
        if kind == "directory":
            if extension is not None:
                raise ValidationError("workflow graph directory extension must be null")
        elif (
            not isinstance(extension, str)
            or not extension.startswith(".")
            or "/" in extension
            or "\\" in extension
        ):
            raise ValidationError("workflow graph artifact extension must be a file extension")
        _graph_allowed_string(
            artifact,
            "address_scope",
            allowed=ADDRESS_SCOPES,
            label="artifact",
        )
        identity_status = _graph_allowed_string(
            artifact,
            "identity_status",
            allowed=IDENTITY_STATUSES,
            label="artifact",
        )
        _graph_nullable_positive_int(artifact, "hash_version", label="artifact")
        _graph_nullable_hash(artifact, "param_hash", label="artifact")
        _graph_nullable_digest(artifact, "full_param_digest", label="artifact")
        _graph_nullable_digest(artifact, "lineage_digest", label="artifact")
        if identity_status == "static_docs_artifact":
            for field in (
                "hash_version",
                "param_hash",
                "full_param_digest",
                "lineage_digest",
            ):
                if artifact.get(field) is not None:
                    raise ValidationError(
                        f"workflow graph artifact {field} must be null for static docs artifacts"
                    )
        artifact_keys.add((step_name, output_name))

    for edge in edges:
        source_artifact_id = _graph_string(
            edge,
            "source_artifact_id",
            label="edge",
        )
        if source_artifact_id not in artifact_ids:
            raise ValidationError(
                f"workflow graph edge references unknown source artifact: {source_artifact_id}"
            )
        target_node_id = _graph_string(edge, "target_node_id", label="edge")
        if target_node_id not in node_ids:
            raise ValidationError(
                f"workflow graph edge references unknown target node: {target_node_id}"
            )
        validate_path_token(
            _graph_string(edge, "binding_name", label="edge"),
            label="edge binding_name",
        )
        _graph_allowed_string(
            edge,
            "dependency_role",
            allowed=DEPENDENCY_ROLES,
            label="edge",
        )

    selected_step_name = validate_path_token(
        _graph_string(graph, "selected_step_name", label="workflow graph"),
        label="selected_step_name",
    )
    selected_output_name = validate_path_token(
        _graph_string(graph, "selected_output_name", label="workflow graph"),
        label="selected_output_name",
    )
    if selected_step_name not in step_names:
        raise ValidationError(
            "workflow graph selected_step_name does not match a node: "
            f"{selected_step_name}"
        )
    if (selected_step_name, selected_output_name) not in artifact_keys:
        raise ValidationError(
            "workflow graph selected output does not match an artifact: "
            f"{selected_step_name}.{selected_output_name}"
        )

    binding_keys = set()
    for binding in manifest_bindings:
        node_id = binding.get("node_id")
        group_id = binding.get("group_id")
        if node_id is None and group_id is None:
            raise ValidationError("workflow graph manifest binding must reference a node or group")
        if node_id is not None:
            if not isinstance(node_id, str):
                raise ValidationError("workflow graph manifest binding node_id must be a string")
            if node_id not in node_ids:
                raise ValidationError(
                    f"workflow graph manifest binding references unknown node: {node_id}"
                )
        if group_id is not None:
            if not isinstance(group_id, str):
                raise ValidationError("workflow graph manifest binding group_id must be a string")
            if group_id not in group_ids:
                raise ValidationError(
                    f"workflow graph manifest binding references unknown group: {group_id}"
                )
        manifest_usage_role = validate_path_token(
            binding.get("manifest_usage_role"),
            label="manifest binding usage role",
        )
        manifest_name = validate_path_token(
            _graph_string(binding, "manifest_name", label="manifest binding"),
            label="manifest_name",
        )
        _validate_graph_manifest_value(binding, label="manifest binding")
        _graph_allowed_string(
            binding,
            "binding_source",
            allowed=MANIFEST_BINDING_SOURCES,
            label="manifest binding",
        )
        binding_key = (node_id, group_id, manifest_usage_role, manifest_name)
        if binding_key in binding_keys:
            raise ValidationError("workflow graph duplicate manifest binding")
        binding_keys.add(binding_key)


def _validate_graph_manifest_value(
    payload: Mapping[str, Any],
    *,
    label: str,
) -> None:
    value_schema = _graph_string(payload, "manifest_value_schema", label=label)
    if value_schema != MANIFEST_VALUE_SCHEMA:
        raise ValidationError(
            f"workflow graph {label} manifest_value_schema must be "
            f"{MANIFEST_VALUE_SCHEMA!r}"
        )
    manifest_digest = _graph_digest(payload, "manifest_digest", label=label)
    manifest_hash = _graph_hash(payload, "manifest_hash", label=label)
    if not manifest_digest.startswith(manifest_hash):
        raise ValidationError(
            f"workflow graph {label} manifest_hash must prefix manifest_digest"
        )
    _graph_positive_int(payload, "entity_count", label=label)


def _graph_node_id(step_name: str) -> str:
    return f"step:{step_name}"


def _graph_artifact_id(step_name: str, output_name: str) -> str:
    return f"artifact:{step_name}:{output_name}"


def _graph_edge_id(
    source_artifact_id: str,
    target_node_id: str,
    binding_name: str,
    dependency_role: str,
) -> str:
    return f"edge:{source_artifact_id}->{target_node_id}:{binding_name}:{dependency_role}"


def _terminal_step_kind(step: WorkflowPlanStep) -> str:
    if step.pattern_kind == "analysis":
        return "analysis"
    if step.pattern_kind == "pattern_b":
        return "pattern_b_barrier"
    if step.pattern_kind == "pattern_a":
        return "pattern_a"
    raise ValidationError(f"unsupported terminal step pattern_kind: {step.pattern_kind}")


def _graph_list(graph: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = graph.get(key)
    if not isinstance(value, list):
        raise ValidationError(f"workflow graph {key} must be a list")
    items: list[Mapping[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise ValidationError(f"workflow graph {key}[{index}] must be a mapping")
        items.append(item)
    return items


def _graph_string(payload: Mapping[str, Any], key: str, *, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"workflow graph {label} {key} must be a non-empty string")
    return value


def _graph_allowed_string(
    payload: Mapping[str, Any],
    key: str,
    *,
    allowed: frozenset[str],
    label: str,
) -> str:
    value = _graph_string(payload, key, label=label)
    if value not in allowed:
        raise ValidationError(
            f"workflow graph {label} {key} must be one of: {', '.join(sorted(allowed))}"
        )
    return value


def _graph_list_of_strings(graph: Mapping[str, Any], key: str) -> list[str]:
    value = graph.get(key)
    if not isinstance(value, list):
        raise ValidationError(f"workflow graph {key} must be a list")
    strings: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise ValidationError(f"workflow graph {key}[{index}] must be a string")
        strings.append(item)
    return strings


def _graph_nullable_string(payload: Mapping[str, Any], key: str, *, label: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(f"workflow graph {label} {key} must be a string or null")
    return value


def _graph_digest(payload: Mapping[str, Any], key: str, *, label: str) -> str:
    value = _graph_string(payload, key, label=label)
    if not is_valid_digest(value):
        raise ValidationError(
            f"workflow graph {label} {key} must be a lowercase 64-character digest"
        )
    return value


def _graph_nullable_digest(payload: Mapping[str, Any], key: str, *, label: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not is_valid_digest(value):
        raise ValidationError(
            f"workflow graph {label} {key} must be a lowercase 64-character digest or null"
        )
    return value


def _is_valid_short_hash(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == SHORT_HASH_LENGTH
        and all(char in "0123456789abcdef" for char in value)
    )


def _graph_hash(payload: Mapping[str, Any], key: str, *, label: str) -> str:
    value = _graph_string(payload, key, label=label)
    if not _is_valid_short_hash(value):
        raise ValidationError(
            f"workflow graph {label} {key} must be a lowercase {SHORT_HASH_LENGTH}-character hash"
        )
    return value


def _graph_nullable_hash(payload: Mapping[str, Any], key: str, *, label: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not _is_valid_short_hash(value):
        raise ValidationError(
            f"workflow graph {label} {key} must be a lowercase "
            f"{SHORT_HASH_LENGTH}-character hash or null"
        )
    return value


def _graph_positive_int(payload: Mapping[str, Any], key: str, *, label: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValidationError(f"workflow graph {label} {key} must be a positive integer")
    return value


def _graph_nullable_positive_int(
    payload: Mapping[str, Any],
    key: str,
    *,
    label: str,
) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValidationError(
            f"workflow graph {label} {key} must be a positive integer or null"
        )
    return value


def _unique_graph_ids(
    items: list[Mapping[str, Any]],
    *,
    field: str,
    label: str,
) -> set[str]:
    ids: set[str] = set()
    for item in items:
        value = _graph_string(item, field, label=label)
        if value in ids:
            raise ValidationError(f"workflow graph duplicate {label} id: {value}")
        ids.add(value)
    return ids


def _runtime_root(project_root: Path, config: Mapping[str, Any]) -> Path:
    paths = config.get("paths")
    if not isinstance(paths, Mapping):
        raise ValidationError("nipact.yaml missing paths.runtime")
    raw_path = paths.get("runtime")
    if not isinstance(raw_path, str) or not raw_path:
        raise ValidationError("nipact.yaml missing paths.runtime")
    runtime_path = Path(raw_path).expanduser()
    if runtime_path.is_absolute():
        return runtime_path.resolve()
    return (project_root / runtime_path).resolve()


def _load_source_index(path: Path, *, runtime_root: Path) -> SourceIndex:
    payload = _load_yaml_mapping(path, label=str(path))
    _check_fields(
        payload,
        allowed=SOURCE_INDEX_FIELDS,
        required=frozenset(),
        label="source index",
    )
    global_bindings = _parse_source_binding_mapping(
        payload.get("global"),
        runtime_root=runtime_root,
        label="source index global",
    )
    entity_bindings = _parse_entity_source_bindings(
        payload.get("entities"),
        runtime_root=runtime_root,
        label="source index entities",
    )
    binding_count = len(global_bindings) + sum(
        len(bindings) for bindings in entity_bindings.values()
    )
    if binding_count == 0:
        raise ValidationError("source index must declare at least one source binding")
    return SourceIndex(
        path=path,
        global_bindings=global_bindings,
        entity_bindings=entity_bindings,
    )


def _parse_source_binding_mapping(
    payload: Any,
    *,
    runtime_root: Path,
    label: str,
) -> dict[str, str]:
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    _check_string_keys(payload, label=label)
    bindings: dict[str, str] = {}
    for raw_name, raw_path in sorted(payload.items()):
        name = validate_path_token(raw_name, label="source binding")
        bindings[name] = _parse_source_runtime_path(
            raw_path,
            runtime_root=runtime_root,
            label=f"{label} {name!r}",
        )
    return bindings


def _parse_entity_source_bindings(
    payload: Any,
    *,
    runtime_root: Path,
    label: str,
) -> dict[str, dict[str, str]]:
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    _check_string_keys(payload, label=label)
    entities: dict[str, dict[str, str]] = {}
    for raw_address, raw_bindings in sorted(payload.items()):
        address = validate_path_token(raw_address, label="source entity address")
        bindings = _parse_source_binding_mapping(
            raw_bindings,
            runtime_root=runtime_root,
            label=f"{label} {address!r}",
        )
        if not bindings:
            raise ValidationError(f"{label} {address!r} must declare at least one binding")
        entities[address] = bindings
    return entities


def _parse_source_runtime_path(payload: Any, *, runtime_root: Path, label: str) -> str:
    if not isinstance(payload, str) or not payload:
        raise ValidationError(f"{label} source path must be a non-empty string")
    if "\\" in payload:
        raise ValidationError(f"{label} source path must use POSIX runtime-relative path")
    if any(char in payload for char in PATH_GLOB_CHARS):
        raise ValidationError(f"{label} source path cannot contain glob patterns")
    source_path = PurePosixPath(payload)
    if source_path.is_absolute():
        raise ValidationError(f"{label} source path must be runtime-relative")
    if any(part == ".." for part in source_path.parts):
        raise ValidationError(f"{label} source path cannot contain path traversal tokens")
    if not payload.startswith(SOURCE_PATH_PREFIX):
        raise ValidationError(f"{label} source path must start with data/")
    if len(source_path.parts) <= 1:
        raise ValidationError(f"{label} source path must include a file under data/")
    if source_path.as_posix() != payload:
        raise ValidationError(f"{label} source path must be a normalized POSIX path")
    data_root = (runtime_root / "data").resolve()
    resolved = (runtime_root / Path(payload)).resolve()
    if not _path_contains_or_same(data_root, resolved):
        raise ValidationError(f"{label} source path must resolve under runtime_root/data")
    return payload


def _set_declaration_issue(
    declarations: dict[tuple[str, str], tuple[DeclarationRecord, ...]],
    kind: str,
    name: str,
    status: str,
    error: str | None,
) -> None:
    key = (kind, name)
    declarations[key] = tuple(
        replace(
            record,
            status=status,
            error=(f"{record.path}: {error}" if status == "invalid" else error),
        )
        for record in declarations[key]
    )


def _discover_project_files(
    project_root: Path, directory: str, *, kind: str
) -> list[DeclarationRecord]:
    path = _resolve_project_path(project_root, directory, label=f"{directory} directory")
    if not path.is_dir():
        raise ValidationError(f"missing {directory} directory: {path}")
    entries = [
        entry
        for entry in sorted(path.iterdir())
        if not entry.name.startswith(".") and entry.suffix in {".yaml", ".yml", ".YAML"}
    ]
    if not entries:
        raise ValidationError(f"missing {directory} YAML files in {path}")
    groups: dict[str, list[Path]] = {}
    for entry in entries:
        groups.setdefault(entry.stem.casefold(), []).append(entry)
    records: list[DeclarationRecord] = []
    for entry in entries:
        error = None
        try:
            collisions = groups[entry.stem.casefold()]
            if len(collisions) > 1:
                raise ValidationError(
                    "case-insensitive declaration name collision: "
                    + " and ".join(str(item) for item in collisions)
                )
            if entry.suffix != ".yaml":
                raise ValidationError("rename to use the .yaml extension")
            validate_path_token(entry.stem, label=f"{directory} name")
            resolved = entry.resolve()
            if not _path_contains_or_same(project_root, resolved):
                raise ValidationError("declaration must stay inside project dir")
            if not resolved.is_file():
                raise ValidationError("declaration must be a regular file")
        except (OSError, RuntimeError) as exc:
            error = f"{entry}: cannot resolve declaration: {exc}"
        except ValidationError as exc:
            error = f"{entry}: {exc}"
        records.append(
            DeclarationRecord(
                kind, entry.stem, entry, "invalid" if error else "valid", error
            )
        )
    return records


def _resolve_project_path(project_root: Path, raw_path: Any, *, label: str) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise ValidationError(f"{label} must be a non-empty string")
    relative_path = Path(raw_path).expanduser()
    if relative_path.is_absolute():
        raise ValidationError(f"{label} must be relative to project dir")
    resolved = (project_root / relative_path).resolve()
    if not _path_contains_or_same(project_root, resolved):
        raise ValidationError(f"{label} must stay inside project dir")
    return resolved


def _path_contains_or_same(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _load_manifest(path: Path, *, label: str) -> Manifest:
    try:
        return load_manifest(path)
    except ValidationError as exc:
        raise ValidationError(f"{label}: {exc}") from exc


def _load_step(path: Path, *, name: str, manifest_names: set[str]) -> StepDefinition:
    payload = _load_yaml_mapping(path, label=str(path))
    _check_fields(
        payload,
        allowed=STEP_FIELDS,
        required=REQUIRED_STEP_FIELDS,
        label=f"step {path.name}",
    )
    payload.setdefault("step_name", name)
    step_name = validate_path_token(
        _required_string(payload, "step_name", f"step {path.name} step_name"),
        label="step_name",
    )
    if step_name != name:
        raise ValidationError(
            f"step_name {step_name!r} must match file name {name + '.yaml'!r}"
        )
    callable_ref = _required_string(payload, "callable", f"step {step_name} callable")
    _validate_callable(callable_ref, label=f"step {step_name} callable")
    step_contract_version = _required_string(
        payload,
        "step_contract_version",
        f"step {step_name} step_contract_version",
    )
    pattern_kind = _allowed_value(
        _required_string(payload, "pattern_kind", f"step {step_name} pattern_kind"),
        allowed=PATTERN_KINDS,
        label=f"step {step_name} pattern_kind",
    )
    execution_role = _allowed_value(
        _required_string(payload, "execution_role", f"step {step_name} execution_role"),
        allowed=EXECUTION_ROLES,
        label=f"step {step_name} execution_role",
    )
    address_scope = _allowed_value(
        _required_string(payload, "address_scope", f"step {step_name} address_scope"),
        allowed=ADDRESS_SCOPES,
        label=f"step {step_name} address_scope",
    )

    inputs = _parse_inputs(payload.get("inputs"), label=f"step {step_name} inputs")
    source_inputs = _parse_source_inputs(
        payload.get("source_inputs"),
        execution_role=execution_role,
        has_workflow_inputs=bool(inputs),
        label=f"step {step_name} source_inputs",
    )
    outputs = _parse_outputs(
        payload["outputs"],
        address_scope=address_scope,
        label=f"step {step_name} outputs",
    )
    manifest_binding = _parse_manifest_binding(
        payload.get("manifest_binding"),
        manifest_names=manifest_names,
        label=f"step {step_name} manifest_binding",
    )
    collection_roles = {
        step_input.dependency_role
        for step_input in inputs.values()
        if step_input.dependency_role in {"fit_input", "analysis_input"}
    }
    if collection_roles and manifest_binding is None:
        raise ValidationError(
            f"step {step_name!r} with {', '.join(sorted(collection_roles))} "
            "requires a scientific manifest_binding"
        )
    params = _parse_params(payload.get("params"), label=f"step {step_name} params")

    return StepDefinition(
        name=step_name,
        pattern_kind=pattern_kind,
        execution_role=execution_role,
        address_scope=address_scope,
        callable_ref=callable_ref,
        step_contract_version=step_contract_version,
        inputs=inputs,
        source_inputs=source_inputs,
        params=params,
        outputs=outputs,
        manifest_binding=manifest_binding,
        source_path=path,
    )


def _parse_inputs(payload: Any, *, label: str) -> dict[str, StepInput]:
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")

    _check_string_keys(payload, label=label)
    inputs: dict[str, StepInput] = {}
    for raw_name, raw_input in sorted(payload.items()):
        name = validate_path_token(raw_name, label="input name")
        if not isinstance(raw_input, Mapping):
            raise ValidationError(f"{label} {name!r} must be a mapping")
        _check_fields(
            raw_input,
            allowed=INPUT_FIELDS,
            required=INPUT_FIELDS,
            label=f"{label} {name!r}",
        )
        artifact = _required_string(raw_input, "artifact", f"{label} {name!r} artifact")
        source_step_name, source_output_name = _parse_artifact_ref(
            artifact,
            label=f"{label} {name!r} artifact",
        )
        inputs[name] = StepInput(
            name=name,
            artifact=artifact,
            dependency_role=_allowed_value(
                _required_string(
                    raw_input,
                    "dependency_role",
                    f"{label} {name!r} dependency_role",
                ),
                allowed=DEPENDENCY_ROLES,
                label=f"{label} {name!r} dependency_role",
            ),
            source_step_name=source_step_name,
            source_output_name=source_output_name,
        )
    return inputs


def _parse_source_inputs(
    payload: Any,
    *,
    execution_role: str,
    has_workflow_inputs: bool,
    label: str,
) -> tuple[str, ...]:
    if payload is None:
        if execution_role == "source_import":
            raise ValidationError(f"{label} is required for source_import steps")
        return ()
    if execution_role != "source_import":
        raise ValidationError(f"{label} is only allowed on source_import steps")
    if has_workflow_inputs:
        raise ValidationError(
            "source_import steps cannot declare both inputs and source_inputs"
        )
    if not isinstance(payload, list):
        raise ValidationError(f"{label} must be a list")
    if not payload:
        raise ValidationError(f"{label} cannot be empty")

    source_inputs: list[str] = []
    for index, raw_name in enumerate(payload):
        source_inputs.append(
            validate_path_token(raw_name, label=f"{label}[{index}]")
        )
    duplicates = sorted(
        {name for name in source_inputs if source_inputs.count(name) > 1}
    )
    if duplicates:
        raise ValidationError(
            f"{label} contains duplicate binding(s): {', '.join(duplicates)}"
        )
    return tuple(source_inputs)


def _parse_outputs(
    payload: Any, *, address_scope: str, label: str
) -> dict[str, StepOutput]:
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    if not payload:
        raise ValidationError(f"{label} cannot be empty")

    _check_string_keys(payload, label=label)
    outputs: dict[str, StepOutput] = {}
    for raw_name, raw_output in sorted(payload.items()):
        name = validate_path_token(raw_name, label="output name")
        if not isinstance(raw_output, Mapping):
            raise ValidationError(f"{label} {name!r} must be a mapping")
        _check_fields(
            raw_output,
            allowed=OUTPUT_FIELDS,
            required=frozenset(),
            label=f"{label} {name!r}",
        )
        kind = raw_output.get("kind", "file")
        if kind not in ("file", "directory"):
            raise ValidationError(f"{label} {name!r} kind must be file or directory")
        extension = None
        if kind == "directory":
            if "extension" in raw_output:
                raise ValidationError(f"{label} {name!r} directory forbids extension")
        else:
            extension = _required_string(
                raw_output, "extension", f"{label} {name!r} extension"
            )
            if (
                not extension.startswith(".")
                or "/" in extension
                or "\\" in extension
                or extension in {".", ".."}
            ):
                raise ValidationError(
                    f"{label} {name!r} extension must be a file extension"
                )
        output_scope = address_scope
        if "address_scope" in raw_output:
            output_scope = _allowed_value(
                _required_string(
                    raw_output, "address_scope", f"{label} {name!r} address_scope"
                ),
                allowed=ADDRESS_SCOPES,
                label=f"{label} {name!r} address_scope",
            )
        if output_scope != address_scope:
            raise ValidationError(
                f"{label} {name!r} address_scope must match step address_scope"
            )
        outputs[name] = StepOutput(
            name=name,
            extension=extension,
            kind=kind,
            address_scope=output_scope,
        )
    return outputs


def _parse_manifest_binding(
    payload: Any,
    *,
    manifest_names: set[str],
    label: str,
) -> ManifestBinding | None:
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    _check_fields(
        payload,
        allowed=MANIFEST_BINDING_FIELDS,
        required=MANIFEST_BINDING_FIELDS,
        label=label,
    )
    role = validate_path_token(
        _required_string(payload, "role", f"{label} role"),
        label="manifest binding role",
    )
    if role == "source_population":
        raise ValidationError(
            f"{label} role 'source_population' is reserved; declare the "
            "workflow execution_population instead"
        )
    manifest_name = validate_path_token(
        _required_string(payload, "manifest", f"{label} manifest"),
        label="manifest name",
    )
    if manifest_name not in manifest_names:
        raise ValidationError(f"{label} references unknown manifest: {manifest_name}")
    return ManifestBinding(role=role, manifest_name=manifest_name)


def _parse_params(payload: Any, *, label: str) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    _check_string_keys(payload, label=label)
    params: dict[str, Any] = {}
    for raw_name, value in sorted(payload.items()):
        name = validate_path_token(raw_name, label="param name")
        params[name] = value
    return params


def _load_workflow(path: Path, *, name: str) -> WorkflowDefinition:
    payload = _load_yaml_mapping(path, label=str(path))
    payload.setdefault("workflow_name", name)
    workflow_name = validate_path_token(
        _required_string(payload, "workflow_name", f"workflow {name} workflow_name"),
        label="workflow_name",
    )
    if workflow_name != name:
        raise ValidationError(
            f"workflow_name {workflow_name!r} must match file name {name + '.yaml'!r}"
        )

    base_workflow = payload.get("base_workflow")
    if base_workflow is None:
        _check_fields(
            payload,
            allowed=BASE_WORKFLOW_FIELDS,
            required=REQUIRED_BASE_WORKFLOW_FIELDS,
            label=f"workflow {workflow_name}",
        )
        steps = _parse_workflow_steps(payload["steps"], label=f"workflow {workflow_name} steps")
        step_outputs = _parse_workflow_step_outputs(
            payload["steps"],
            label=f"workflow {workflow_name} steps",
        )
        return WorkflowDefinition(
            name=workflow_name,
            base_workflow=None,
            execution_population_name=_parse_execution_population_name(
                payload,
                label=f"workflow {workflow_name}",
            ),
            steps=steps,
            step_outputs=step_outputs,
            step_overrides={},
            source_path=path,
        )

    _check_fields(
        payload,
        allowed=VARIANT_WORKFLOW_FIELDS,
        required=REQUIRED_VARIANT_WORKFLOW_FIELDS,
        label=f"workflow {workflow_name}",
    )
    return WorkflowDefinition(
        name=workflow_name,
        base_workflow=validate_path_token(base_workflow, label="base_workflow"),
        execution_population_name=_parse_execution_population_name(
            payload,
            label=f"workflow {workflow_name}",
        ),
        steps=(),
        step_outputs={},
        step_overrides=_parse_overrides(
            payload.get("step_overrides", {}),
            label=f"workflow {workflow_name} step_overrides",
        ),
        source_path=path,
    )


def _parse_execution_population_name(
    payload: Mapping[str, Any],
    *,
    label: str,
) -> str | None:
    if "execution_population" not in payload:
        return None
    return validate_path_token(
        _required_string(
            payload,
            "execution_population",
            f"{label} execution_population",
        ),
        label="execution_population",
    )


def _parse_workflow_steps(payload: Any, *, label: str) -> tuple[str, ...]:
    if not isinstance(payload, list) or not payload:
        raise ValidationError(f"{label} must be a non-empty list")
    steps = tuple(_parse_workflow_step_name(item, label=f"{label}[{index}]") for index, item in enumerate(payload))
    duplicates = sorted({step for step in steps if steps.count(step) > 1})
    if duplicates:
        raise ValidationError(f"{label} contains duplicate step(s): {', '.join(duplicates)}")
    return steps


def _parse_workflow_step_name(item: Any, *, label: str) -> str:
    if isinstance(item, str):
        return validate_path_token(item, label="workflow step")
    if not isinstance(item, Mapping):
        raise ValidationError(f"{label} must be a step name or mapping")
    _check_fields(
        item,
        allowed=WORKFLOW_STEP_FIELDS,
        required=REQUIRED_WORKFLOW_STEP_FIELDS,
        label=label,
    )
    return validate_path_token(
        _required_string(item, "step_name", f"{label} step_name"),
        label="workflow step",
    )


def _parse_workflow_step_outputs(payload: Any, *, label: str) -> dict[str, str]:
    if not isinstance(payload, list) or not payload:
        raise ValidationError(f"{label} must be a non-empty list")
    step_outputs: dict[str, str] = {}
    for index, item in enumerate(payload):
        if isinstance(item, str):
            continue
        if not isinstance(item, Mapping):
            raise ValidationError(f"{label}[{index}] must be a step name or mapping")
        _check_fields(
            item,
            allowed=WORKFLOW_STEP_FIELDS,
            required=REQUIRED_WORKFLOW_STEP_FIELDS,
            label=f"{label}[{index}]",
        )
        if "output_name" not in item:
            continue
        step_name = validate_path_token(
            _required_string(item, "step_name", f"{label}[{index}] step_name"),
            label="workflow step",
        )
        step_outputs[step_name] = validate_path_token(
            _required_string(item, "output_name", f"{label}[{index}] output_name"),
            label="workflow step output_name",
        )
    return step_outputs


def _parse_overrides(payload: Any, *, label: str) -> dict[str, WorkflowStepOverride]:
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    _check_string_keys(payload, label=label)
    overrides: dict[str, WorkflowStepOverride] = {}
    for raw_step_name, raw_override in sorted(payload.items()):
        step_name = validate_path_token(raw_step_name, label="override step_name")
        if not isinstance(raw_override, Mapping):
            raise ValidationError(f"{label} {step_name!r} must be a mapping")
        _check_fields(
            raw_override,
            allowed=OVERRIDE_FIELDS,
            required=OVERRIDE_FIELDS,
            label=f"{label} {step_name!r}",
        )
        overrides[step_name] = WorkflowStepOverride(
            step_name=step_name,
            params=_parse_params(raw_override["params"], label=f"{label} {step_name!r} params"),
        )
    return overrides


def _resolve_workflows(
    raw_workflows: Mapping[str, WorkflowDefinition],
    *,
    steps: Mapping[str, StepDefinition],
    declarations: dict[tuple[str, str], tuple[DeclarationRecord, ...]],
) -> dict[str, WorkflowDefinition]:
    resolved: dict[str, WorkflowDefinition] = {}
    resolving: list[str] = []

    def resolve(name: str) -> WorkflowDefinition | None:
        if name in resolved:
            return resolved[name]
        if name in resolving:
            for member in resolving[resolving.index(name) :]:
                _set_declaration_issue(
                    declarations,
                    "workflow",
                    member,
                    "invalid",
                    f"workflow inheritance cycle includes {name!r}",
                )
            return None
        if declarations[("workflow", name)][0].status != "valid":
            return None
        resolving.append(name)
        try:
            workflow = resolve_valid(raw_workflows[name])
            if workflow is not None:
                resolved[name] = workflow
            return workflow
        except ValidationError as exc:
            _set_declaration_issue(declarations, "workflow", name, "invalid", str(exc))
            return None
        finally:
            resolving.pop()

    def dependency(workflow: WorkflowDefinition, kind: str, name: str) -> bool:
        issue = next(
            (
                record
                for record in declarations[(kind, name)]
                if record.status != "valid"
            ),
            None,
        )
        if issue is not None:
            # Cycle members already own an invalid error; descendants inherit its root.
            if declarations[("workflow", workflow.name)][0].status == "valid":
                _set_declaration_issue(
                    declarations, "workflow", workflow.name, "unavailable", issue.error
                )
            return False
        return True

    def resolve_valid(workflow: WorkflowDefinition) -> WorkflowDefinition | None:
        manifest_name = workflow.execution_population_name
        if (
            manifest_name is not None
            and ("manifest", manifest_name) not in declarations
        ):
            raise ValidationError(
                f"workflow {workflow.name!r} references unknown execution_population: {manifest_name}"
            )
        for step_name in workflow.steps:
            if ("step", step_name) not in declarations:
                raise ValidationError(
                    f"workflow {workflow.name!r} references unknown step: {step_name}"
                )
        if workflow.base_workflow is not None:
            if ("workflow", workflow.base_workflow) not in declarations:
                raise ValidationError(
                    f"workflow {workflow.name!r} references unknown base_workflow: "
                    f"{workflow.base_workflow}"
                )
            base = resolve(workflow.base_workflow)
            if not dependency(workflow, "workflow", workflow.base_workflow):
                return None
            step_overrides = dict(base.step_overrides)
            for step_name, override in workflow.step_overrides.items():
                if step_name not in base.steps:
                    raise ValidationError(
                        f"workflow {workflow.name!r} override references unknown step: "
                        f"{step_name}"
                    )
                inherited_override = step_overrides.get(
                    step_name,
                    WorkflowStepOverride(step_name, {}),
                )
                merged_params = dict(inherited_override.params)
                merged_params.update(override.params)
                step_overrides[step_name] = WorkflowStepOverride(
                    step_name=step_name,
                    params=merged_params,
                )
            workflow = WorkflowDefinition(
                name=workflow.name,
                base_workflow=workflow.base_workflow,
                execution_population_name=(
                    workflow.execution_population_name
                    if workflow.execution_population_name is not None
                    else base.execution_population_name
                ),
                steps=base.steps,
                step_outputs=dict(base.step_outputs),
                step_overrides=step_overrides,
                source_path=workflow.source_path,
            )

        dependencies = [("step", step_name) for step_name in workflow.steps]
        if workflow.execution_population_name is not None:
            dependencies.append(("manifest", workflow.execution_population_name))
        for kind, name in dependencies:
            if not dependency(workflow, kind, name):
                return None
        _validate_workflow_references(workflow, steps=steps)
        return workflow

    for name in sorted(raw_workflows):
        resolve(name)
    return resolved


def _validate_workflow_references(
    workflow: WorkflowDefinition,
    *,
    steps: Mapping[str, StepDefinition],
) -> None:
    seen_outputs: set[tuple[str, str]] = set()
    workflow_steps = set(workflow.steps)

    for step_name in workflow.steps:
        if step_name not in steps:
            raise ValidationError(
                f"workflow {workflow.name!r} references unknown step: {step_name}"
            )
        step = steps[step_name]
        for step_input in step.inputs.values():
            source = (step_input.source_step_name, step_input.source_output_name)
            if source not in seen_outputs:
                raise ValidationError(
                    f"workflow {workflow.name!r} step {step_name!r} input "
                    f"{step_input.artifact!r} does not reference an earlier workflow output"
                )
        seen_outputs.update((step_name, output_name) for output_name in step.outputs)

    for step_name, output_name in workflow.step_outputs.items():
        if step_name not in workflow_steps:
            raise ValidationError(
                f"workflow {workflow.name!r} runnable step references unknown step: "
                f"{step_name}"
            )
        if output_name not in steps[step_name].outputs:
            raise ValidationError(
                f"workflow {workflow.name!r} runnable step references unknown output: "
                f"{step_name}.{output_name}"
            )


def _collect_dependency_step_names(
    loaded: LoadedWorkflowProject,
    workflow: WorkflowDefinition,
    *,
    root_step_name: str,
) -> tuple[str, ...]:
    workflow_steps = set(workflow.steps)
    visited: set[str] = set()
    visiting: set[str] = set()

    def visit(step_name: str) -> None:
        if step_name in visiting:
            raise ValidationError(
                f"workflow {workflow.name!r} dependency cycle includes step: {step_name}"
            )
        if step_name in visited:
            return

        step = _workflow_step(loaded, workflow, step_name)
        visiting.add(step_name)
        for step_input in step.inputs.values():
            if step_input.source_step_name not in workflow_steps:
                raise ValidationError(
                    f"workflow {workflow.name!r} step {step_name!r} input "
                    f"{step_input.artifact!r} references step outside the workflow"
                )
            source_step = _workflow_step(loaded, workflow, step_input.source_step_name)
            if step_input.source_output_name not in source_step.outputs:
                raise ValidationError(
                    f"workflow {workflow.name!r} step {step_name!r} input "
                    f"{step_input.artifact!r} references unknown output"
                )
            visit(step_input.source_step_name)
        visiting.remove(step_name)
        visited.add(step_name)

    visit(root_step_name)
    return tuple(step_name for step_name in workflow.steps if step_name in visited)


def _workflow_step(
    loaded: LoadedWorkflowProject,
    workflow: WorkflowDefinition,
    step_name: str,
) -> StepDefinition:
    if step_name not in workflow.steps:
        raise ValidationError(
            f"workflow {workflow.name!r} references step outside the workflow: {step_name}"
        )
    try:
        return loaded.steps[step_name]
    except KeyError as exc:
        raise ValidationError(
            f"workflow {workflow.name!r} references unknown step: {step_name}"
        ) from exc


def _compile_manifest_binding(
    loaded: LoadedWorkflowProject,
    step: StepDefinition,
) -> WorkflowPlanManifestBinding | None:
    binding = step.manifest_binding
    if binding is None:
        return None
    try:
        manifest = loaded.manifests[binding.manifest_name]
    except KeyError as exc:
        raise ValidationError(
            f"step {step.name!r} references unknown manifest: {binding.manifest_name}"
        ) from exc
    return WorkflowPlanManifestBinding(
        step_name=step.name,
        manifest_usage_role=binding.role,
        manifest_name=binding.manifest_name,
        manifest_value_schema=manifest.manifest_value_schema,
        manifest_digest=manifest.manifest_digest,
        manifest_hash=manifest.manifest_hash,
        entity_ids=manifest.entity_ids,
        entity_count=manifest.entity_count,
    )


def _compile_execution_population(
    loaded: LoadedWorkflowProject,
    workflow: WorkflowDefinition,
) -> WorkflowPlanExecutionPopulation | None:
    manifest_name = workflow.execution_population_name
    if manifest_name is None:
        return None
    try:
        manifest = loaded.manifests[manifest_name]
    except KeyError as exc:
        raise ValidationError(
            f"workflow {workflow.name!r} references unknown "
            f"execution_population: {manifest_name}"
        ) from exc
    return WorkflowPlanExecutionPopulation(
        manifest_name=manifest_name,
        manifest_value_schema=manifest.manifest_value_schema,
        manifest_digest=manifest.manifest_digest,
        manifest_hash=manifest.manifest_hash,
        entity_ids=manifest.entity_ids,
        entity_count=manifest.entity_count,
    )


def _requires_execution_population(
    steps: tuple[StepDefinition, ...],
) -> bool:
    return any(
        step.address_scope == "entity"
        or any(
            step_input.dependency_role in {"fit_input", "analysis_input"}
            for step_input in step.inputs.values()
        )
        for step in steps
    )


def _parse_artifact_ref(value: str, *, label: str) -> tuple[str, str]:
    if value.count(".") != 1:
        raise ValidationError(f"{label} must use step.output syntax")
    step_name, output_name = value.split(".", maxsplit=1)
    return (
        validate_path_token(step_name, label="artifact step"),
        validate_path_token(output_name, label="artifact output"),
    )


def _validate_callable(value: str, *, label: str) -> None:
    if value.count(":") != 1:
        raise ValidationError(f"{label} must use module:function syntax")
    module_name, function_name = value.split(":", maxsplit=1)
    if not module_name or not function_name:
        raise ValidationError(f"{label} must use module:function syntax")
    try:
        module = import_module(module_name)
    except Exception as exc:
        raise ValidationError(f"{label} module could not be imported: {module_name}") from exc
    if not hasattr(module, function_name):
        raise ValidationError(f"{label} function is missing: {function_name}")
    if not callable(getattr(module, function_name)):
        raise ValidationError(f"{label} does not resolve to a callable")


def _check_string_keys(payload: Mapping[object, Any], *, label: str) -> None:
    if any(not isinstance(key, str) for key in payload):
        raise ValidationError(f"{label} mapping keys must be strings")


def _check_fields(
    payload: Mapping[str, Any],
    *,
    allowed: frozenset[str],
    required: frozenset[str],
    label: str,
) -> None:
    _check_string_keys(payload, label=label)
    keys = set(payload)
    unknown = sorted(keys - allowed)
    if unknown:
        raise ValidationError(f"{label} contains unknown field(s): {', '.join(unknown)}")
    missing = sorted(required - keys)
    if missing:
        raise ValidationError(f"{label} is missing required field(s): {', '.join(missing)}")


def _required_string(payload: Mapping[str, Any], key: str, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{label} must be a non-empty string")
    return value


def _allowed_value(value: str, *, allowed: frozenset[str], label: str) -> str:
    if value not in allowed:
        raise ValidationError(f"{label} must be one of: {', '.join(sorted(allowed))}")
    return value


def _load_yaml_mapping(path: Path, *, label: str) -> dict[str, Any]:
    try:
        if not path.is_file():
            raise ValidationError(f"missing YAML file: {path}")
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"invalid YAML file {path}: {exc}") from exc
    try:
        payload = yaml.safe_load(text)
    except (yaml.YAMLError, ValueError) as exc:
        raise ValidationError(f"invalid YAML file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"{label} must contain a mapping")
    return payload
