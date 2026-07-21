"""Read-only service layer for the local GUI API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nipact.errors import ValidationError
from nipact.registry import (
    ArtifactGroupCount,
    RegistryArtifact,
    RegistryManifest,
    list_artifact_group_counts,
    list_artifacts,
    list_manifests,
    read_artifact_by_id_for_context,
    read_manifest,
    read_registry_summary,
    resolve_registered_artifact_path,
)
from nipact.trace import build_trace_graph

from . import models
from .project import GuiProject
from .topology import build_observed_topology


@dataclass(frozen=True)
class GuiApiError(Exception):
    status_code: int
    code: str
    message: str
    details: dict[str, Any] | None = None


class GuiService:
    def __init__(self, project: GuiProject) -> None:
        self.project = project

    def summary(self) -> models.SummaryResponse:
        registry_counts = read_registry_summary(
            self.project.registry_path,
            context=self.project.context,
        )
        return models.SummaryResponse(
            context=self.project.context,
            workflow_count=len(self.project.loaded_workflow_project.workflows),
            runnable_step_count=sum(
                len(workflow.step_outputs)
                for workflow in self.project.loaded_workflow_project.workflows.values()
            ),
            **registry_counts,
        )

    def workflows(self) -> models.WorkflowsResponse:
        workflows = []
        for workflow in sorted(
            self.project.loaded_workflow_project.workflows.values(),
            key=lambda workflow: workflow.name,
        ):
            workflows.append(
                models.WorkflowSummary(
                    workflow_name=workflow.name,
                    step_count=len(workflow.steps),
                    runnable_step_count=len(workflow.step_outputs),
                    runnable_steps=[
                        models.RunnableWorkflowStep(
                            step_name=step_name,
                            output_name=output_name,
                        )
                        for step_name, output_name in sorted(
                            workflow.step_outputs.items()
                        )
                    ],
                )
            )
        return models.WorkflowsResponse(
            context=self.project.context,
            workflows=workflows,
        )

    def manifests(self) -> models.ManifestsResponse:
        return models.ManifestsResponse(
            context=self.project.context,
            manifests=[
                _manifest_summary(manifest)
                for manifest in list_manifests(
                    self.project.registry_path,
                    context=self.project.context,
                )
            ],
        )

    def manifest(self, manifest_name: str) -> models.ManifestDetail:
        try:
            manifest = read_manifest(
                self.project.registry_path,
                context=self.project.context,
                manifest_name=manifest_name,
            )
        except ValidationError as exc:
            if str(exc).startswith("unknown manifest:"):
                raise GuiApiError(404, "manifest_not_found", str(exc)) from exc
            raise
        return _manifest_detail(manifest)

    def artifacts(
        self,
        *,
        origin: str | None = None,
        workflow_name: str | None = None,
        step_name: str | None = None,
        output_name: str | None = None,
        address: str | None = None,
        is_selected_output: bool | None = None,
        is_published: bool | None = None,
    ) -> models.ArtifactsResponse:
        artifacts = list_artifacts(
            self.project.registry_path,
            context=self.project.context,
            origin=origin,
            workflow_name=workflow_name,
            step_name=step_name,
            output_name=output_name,
            address=address,
            is_selected_output=is_selected_output,
            is_published=is_published,
        )
        return models.ArtifactsResponse(
            context=self.project.context,
            artifacts=[_artifact_summary(artifact) for artifact in artifacts],
        )

    def artifact_groups(
        self,
        *,
        origin: str | None = None,
        workflow_name: str | None = None,
        step_name: str | None = None,
        output_name: str | None = None,
        address: str | None = None,
        is_selected_output: bool | None = None,
        is_published: bool | None = None,
    ) -> models.ArtifactGroupsResponse:
        groups = list_artifact_group_counts(
            self.project.registry_path,
            context=self.project.context,
            origin=origin,
            workflow_name=workflow_name,
            step_name=step_name,
            output_name=output_name,
            address=address,
            is_selected_output=is_selected_output,
            is_published=is_published,
        )
        return models.ArtifactGroupsResponse(
            context=self.project.context,
            groups=[_artifact_group_count(group) for group in groups],
        )

    def artifact(self, artifact_id: int) -> models.ArtifactDetail:
        return _artifact_detail(self._artifact_for_context(artifact_id))

    def resolve_artifact_path(self, artifact_path: str) -> models.ArtifactDetail:
        try:
            artifact = resolve_registered_artifact_path(
                self.project.registry_path,
                context=self.project.context,
                artifact_path=artifact_path,
            )
        except ValidationError as exc:
            message = str(exc)
            if message.startswith("unknown registered artifact path:"):
                raise GuiApiError(404, "artifact_not_found", message) from exc
            raise
        return _artifact_detail(artifact)

    def lineage(self, artifact_id: int) -> models.TraceGraphResponse:
        artifact = self._artifact_for_context(artifact_id)
        graph = build_trace_graph(
            self.project.registry_path,
            selected_artifact=artifact,
            active_context=self.project.context,
        )
        return models.TraceGraphResponse.model_validate(graph)

    def topology(self, artifact_id: int) -> models.ObservedTopologyResponse:
        artifact = self._artifact_for_context(artifact_id)
        graph = build_trace_graph(
            self.project.registry_path,
            selected_artifact=artifact,
            active_context=self.project.context,
        )
        return models.ObservedTopologyResponse.model_validate(
            build_observed_topology(graph)
        )

    def _artifact_for_context(self, artifact_id: int) -> RegistryArtifact:
        try:
            return read_artifact_by_id_for_context(
                self.project.registry_path,
                context=self.project.context,
                artifact_id=artifact_id,
            )
        except ValidationError as exc:
            message = str(exc)
            if message.startswith("unknown registry artifact id:"):
                raise GuiApiError(404, "artifact_not_found", message) from exc
            raise


def _manifest_summary(manifest: RegistryManifest) -> models.ManifestSummary:
    return models.ManifestSummary(
        context=manifest.context,
        name=manifest.name,
        path=manifest.path,
        entity_count=manifest.entity_count,
        first_entity_id=manifest.first_entity_id,
        last_entity_id=manifest.last_entity_id,
        manifest_value_schema=manifest.manifest_value_schema,
        manifest_digest=manifest.manifest_digest,
        manifest_hash=manifest.manifest_hash,
    )


def _manifest_detail(manifest: RegistryManifest) -> models.ManifestDetail:
    return models.ManifestDetail(
        **_manifest_summary(manifest).model_dump(),
        canonical_body=manifest.canonical_body,
    )


def _artifact_summary(artifact: RegistryArtifact) -> models.Artifact:
    return models.Artifact(
        artifact_id=artifact.artifact_id,
        origin=artifact.origin,
        run_id=artifact.run_id,
        job_id=artifact.job_id,
        artifact_set_id=artifact.artifact_set_id,
        path=artifact.path,
        display_path=artifact.published_path or artifact.path,
        is_selected_output=artifact.is_selected_output,
        is_published=artifact.is_published,
        published_path=artifact.published_path,
        staging_path=artifact.staging_path,
        workflow_name=artifact.workflow_name,
        step_name=artifact.step_name,
        output_name=artifact.output_name,
        address=artifact.address,
        parameter_hash=artifact.parameter_hash,
        parameter_digest=artifact.parameter_digest,
        content_digest=artifact.content_digest,
        output_hash=artifact.output_hash,
        file_size=artifact.file_size,
        extension=artifact.extension,
        subject_id=artifact.subject_id,
        session_id=artifact.session_id,
        task_name=artifact.task_name,
        run_label=artifact.run_label,
        datatype=artifact.datatype,
        suffix=artifact.suffix,
        workflow_artifact_ref=_workflow_artifact_ref(artifact),
        callable_ref=artifact.callable_ref,
        software_ref=artifact.software_ref,
        created_at=artifact.created_at,
    )


def _artifact_detail(artifact: RegistryArtifact) -> models.ArtifactDetail:
    return models.ArtifactDetail(
        **_artifact_summary(artifact).model_dump(),
        source_metadata=artifact.source_metadata,
    )


def _artifact_group_count(group: ArtifactGroupCount) -> models.ArtifactGroupCount:
    return models.ArtifactGroupCount(
        origin=group.origin,
        workflow_name=group.workflow_name,
        step_name=group.step_name,
        output_name=group.output_name,
        artifact_count=group.artifact_count,
    )


def _workflow_artifact_ref(artifact: RegistryArtifact) -> str | None:
    if artifact.origin != "workflow_output":
        return None
    if artifact.step_name is None or artifact.output_name is None:
        return None
    return f"artifact:{artifact.step_name}:{artifact.output_name}"
