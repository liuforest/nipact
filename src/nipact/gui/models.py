"""DTOs for the local NIPACT GUI API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


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
