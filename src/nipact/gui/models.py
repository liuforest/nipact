"""DTOs for the local NIPACT GUI API."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt


class Dto(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ApiError(Dto):
    code: str
    message: str
    details: dict[str, Any] | None = None


class SummaryResponse(Dto):
    context: str
    workflow_count: int
    runnable_step_count: int
    manifest_count: int
    artifact_count: int
    source_artifact_count: int
    workflow_output_count: int
    workflow_run_count: int


class RunnableWorkflowStep(Dto):
    step_name: str
    output_name: str


class WorkflowSummary(Dto):
    workflow_name: str
    step_count: int
    runnable_step_count: int
    runnable_steps: list[RunnableWorkflowStep]


class WorkflowsResponse(Dto):
    context: str
    workflows: list[WorkflowSummary]


class ManifestSummary(Dto):
    context: str
    name: str
    path: str
    entity_count: int
    first_entity_id: str
    last_entity_id: str
    manifest_digest: str
    manifest_hash: str
    source_artifact_path: str | None


class ManifestDetail(ManifestSummary):
    manifest_body: str


class ManifestsResponse(Dto):
    context: str
    manifests: list[ManifestSummary]


class Artifact(Dto):
    artifact_id: int
    origin: str
    run_id: int | None
    job_id: str | None
    artifact_set_id: str | None
    path: str
    display_path: str
    is_selected_output: bool
    is_published: bool
    published_path: str | None
    staging_path: str | None
    workflow_name: str | None
    step_name: str | None
    output_name: str | None
    address: str | None
    parameter_hash: str | None
    parameter_digest: str | None
    content_digest: str
    output_hash: str | None
    file_size: int
    extension: str
    subject_id: str | None
    session_id: str | None
    task_name: str | None
    run_label: str | None
    datatype: str | None
    suffix: str | None
    source_metadata: dict[str, Any] | None
    workflow_artifact_ref: str | None
    callable_ref: str | None
    software_ref: str | None
    created_at: str
    lineage_url: str


class ArtifactsResponse(Dto):
    context: str
    artifacts: list[Artifact]


class TraceArtifact(Dto):
    artifact_id: int
    origin: str
    run_id: int | None
    job_id: str | None
    artifact_set_id: str | None
    path: str
    display_path: str
    is_selected: bool
    is_selected_output: bool
    is_published: bool
    published_path: str | None
    staging_path: str | None
    workflow_name: str | None
    step_name: str | None
    output_name: str | None
    address: str | None
    parameter_hash: str | None
    content_digest: str
    output_hash: str | None
    file_size: int
    extension: str
    subject_id: str | None
    session_id: str | None
    task_name: str | None
    run_label: str | None
    datatype: str | None
    suffix: str | None
    source_metadata: dict[str, Any] | None
    workflow_artifact_ref: str | None
    callable_ref: str | None
    software_ref: str | None


class TraceDependency(Dto):
    edge_id: str
    source_artifact_id: int
    dependent_artifact_id: int
    is_reused_input: bool
    dependency_role: str
    binding_name: str
    input_path: str
    source_content_digest: str
    source_file_size: int
    source_extension: str
    dependency_set_id: str | None
    manifest_digest: str | None
    edge_cardinality: int | None


class TraceManifestBinding(Dto):
    run_id: int
    workflow_name: str
    step_name: str
    role: str
    manifest_name: str
    manifest_digest: str
    manifest_hash: str
    entity_count: int


class TraceWarning(Dto):
    warning_type: str
    message: str
    artifact_id: int | None
    input_path: str | None


class TraceGraphResponse(Dto):
    schema_version: int
    context: str
    selected_artifact_id: int
    provenance_status: str
    artifacts: list[TraceArtifact]
    dependencies: list[TraceDependency]
    manifest_bindings: list[TraceManifestBinding]
    warnings: list[TraceWarning]


# --- Observed topology (PR 2) -------------------------------------------------
#
# The observed topology is the aggregated projection of a backward provenance
# closure: workflow steps, artifact-output slots, and external source-input
# slots each appear once, with explicitly named counts of the concrete registry
# rows they represent. It is projected in Python from a build_trace_graph()
# result (see gui/service.py, PR 2); these models are its validated contract.
# Graph-local rendering IDs (node_id/edge_id like "n0"/"e0") are display keys
# only — the structured coordinate fields carry drill-down identity.

OBSERVED_TOPOLOGY_SCHEMA_VERSION = 1


class TopologyStepNode(Dto):
    kind: Literal["step"]
    node_id: str
    workflow_name: str
    step_name: str
    # distinct artifact rows produced by the step across all its outputs
    produced_registry_artifact_count: NonNegativeInt


class TopologyArtifactSlotNode(Dto):
    kind: Literal["artifact_slot"]
    node_id: str
    workflow_name: str
    step_name: str
    output_name: str
    # distinct artifact rows in the slot; distinct non-null addresses among them
    registry_artifact_count: NonNegativeInt
    distinct_address_count: NonNegativeInt


class TopologySourceInputNode(Dto):
    kind: Literal["source_input"]
    node_id: str
    # coordinates of the *consuming* step, read from the dependent artifact
    workflow_name: str
    step_name: str
    binding_name: str
    dependency_role: str
    # distinct source artifact IDs in this consumer-derived source-input coordinate
    registry_artifact_count: NonNegativeInt


class TopologySourceRootNode(Dto):
    kind: Literal["source_root"]
    node_id: str
    # a selected source artifact with no consuming edge has no consumer-derived
    # coordinate, so it is identified by its own display path
    display_path: str
    registry_artifact_count: NonNegativeInt


TopologyNode = Annotated[
    Union[
        TopologyStepNode,
        TopologyArtifactSlotNode,
        TopologySourceInputNode,
        TopologySourceRootNode,
    ],
    Field(discriminator="kind"),
]


class TopologyConsumesEdge(Dto):
    kind: Literal["consumes"]
    edge_id: str
    source_node_id: str
    target_node_id: str
    # coordinates of the consuming step and the specific input binding/role, so
    # two semantically different inputs from the same source do not collapse
    workflow_name: str
    step_name: str
    binding_name: str
    dependency_role: str
    # physical artifact_dependencies rows represented by the aggregated edge
    registry_dependency_count: NonNegativeInt


class TopologyProducesEdge(Dto):
    kind: Literal["produces"]
    edge_id: str
    source_node_id: str
    target_node_id: str


TopologyEdge = Annotated[
    Union[TopologyConsumesEdge, TopologyProducesEdge],
    Field(discriminator="kind"),
]


class TopologyManifestBindingSummary(Dto):
    # one row per (workflow_name, step_name, role, manifest_name) coordinate
    workflow_name: str
    step_name: str
    role: str
    manifest_name: str
    distinct_run_count: NonNegativeInt
    distinct_manifest_digest_count: NonNegativeInt
    # carried through only when the grouped rows agree; otherwise null
    manifest_digest: str | None
    manifest_hash: str | None
    entity_count: NonNegativeInt | None


class TopologyWarningSummary(Dto):
    warning_type: str
    occurrence_count: NonNegativeInt


class TopologySummary(Dto):
    # total distinct trace artifacts; total physical trace dependency rows
    # (including degraded rows whose consumption edge is not rendered); topology
    # node and edge counts
    distinct_artifact_count: NonNegativeInt
    registry_dependency_count: NonNegativeInt
    node_count: NonNegativeInt
    edge_count: NonNegativeInt


class ObservedTopologyResponse(Dto):
    schema_version: int
    perspective: Literal["observed"]
    scope: Literal["ancestor_closure"]
    context: str
    root_artifact_id: int
    root_node_id: str
    provenance_status: Literal["complete", "degraded"]
    summary: TopologySummary
    nodes: list[TopologyNode]
    edges: list[TopologyEdge]
    manifest_bindings: list[TopologyManifestBindingSummary]
    warnings: list[TopologyWarningSummary]
