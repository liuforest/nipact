"""Read-only provenance trace builders."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .errors import ValidationError
from .registry import (
    RegistryArtifact,
    RegistryDependency,
    RegistryManifestBinding,
    list_run_manifest_bindings,
    list_upstream_dependencies,
    read_artifact_by_id,
    read_artifact_by_id_for_context,
    read_artifact_by_path,
    read_current_published_artifact,
)


TRACE_SCHEMA_VERSION = 1


def build_trace_graph_for_artifact_id(
    registry_path: Path,
    *,
    artifact_id: int,
    context: str | None = None,
) -> dict[str, Any]:
    """Build a backward provenance graph from one registry artifact id."""
    if context is None:
        selected_artifact = read_artifact_by_id(registry_path, artifact_id)
    else:
        selected_artifact = read_artifact_by_id_for_context(
            registry_path,
            context=context,
            artifact_id=artifact_id,
        )
    return build_trace_graph(
        registry_path,
        selected_artifact=selected_artifact,
        active_context=context,
    )


def build_trace_graph_for_path(
    registry_path: Path,
    *,
    context: str,
    artifact_path: str,
) -> dict[str, Any]:
    """Build a backward provenance graph from one registered runtime path."""
    return build_trace_graph(
        registry_path,
        selected_artifact=read_artifact_by_path(
            registry_path,
            context=context,
            artifact_path=artifact_path,
        ),
        active_context=context,
    )


def build_trace_graph_for_workflow_coordinate(
    registry_path: Path,
    *,
    context: str,
    workflow_name: str,
    step_name: str,
    output_name: str,
    address: str,
) -> dict[str, Any]:
    """Build a backward provenance graph from one current published workflow output."""
    return build_trace_graph(
        registry_path,
        selected_artifact=read_current_published_artifact(
            registry_path,
            context=context,
            workflow_name=workflow_name,
            step_name=step_name,
            output_name=output_name,
            address=address,
        ),
        active_context=context,
    )


def build_trace_graph(
    registry_path: Path,
    *,
    selected_artifact: RegistryArtifact,
    active_context: str | None = None,
) -> dict[str, Any]:
    """Build a read-only backward provenance graph from one selected artifact."""
    if active_context is not None and selected_artifact.context != active_context:
        raise ValidationError("selected artifact is outside the active context")
    artifacts: dict[int, dict[str, Any]] = {}
    dependencies: dict[tuple[int, int, str, str], RegistryDependency] = {}
    warnings: list[dict[str, Any]] = []
    queued_artifact_ids = {selected_artifact.artifact_id}
    pending_artifact_ids = [selected_artifact.artifact_id]
    pending_dependency_context: dict[int, RegistryDependency] = {}
    loaded_artifacts: dict[int, RegistryArtifact] = {
        selected_artifact.artifact_id: selected_artifact
    }

    while pending_artifact_ids:
        artifact_id = pending_artifact_ids.pop()
        if artifact_id in artifacts:
            continue
        artifact = loaded_artifacts.get(artifact_id)
        if artifact is None:
            artifact = _read_trace_artifact(
                registry_path,
                artifact_id=artifact_id,
                selected_artifact=selected_artifact,
                pending_dependency=pending_dependency_context.get(artifact_id),
                active_context=active_context,
                warnings=warnings,
            )
        if artifact is None:
            continue

        artifacts[artifact.artifact_id] = _artifact_payload(
            artifact,
            is_selected=artifact.artifact_id == selected_artifact.artifact_id,
        )
        for dependency in _read_upstream_dependencies(
            registry_path,
            artifact=artifact,
            warnings=warnings,
        ):
            missing_source = False
            if active_context is not None:
                source_artifact = _read_trace_artifact(
                    registry_path,
                    artifact_id=dependency.source_artifact_id,
                    selected_artifact=selected_artifact,
                    pending_dependency=dependency,
                    active_context=active_context,
                    warnings=warnings,
                )
                if source_artifact is None:
                    if _last_warning_is_cross_context(warnings, dependency):
                        continue
                    missing_source = True
                else:
                    loaded_artifacts[source_artifact.artifact_id] = source_artifact
            dependency_key = _dependency_key(dependency)
            dependencies[dependency_key] = dependency
            if (
                not missing_source
                and dependency.source_artifact_id not in queued_artifact_ids
            ):
                queued_artifact_ids.add(dependency.source_artifact_id)
                pending_dependency_context[dependency.source_artifact_id] = dependency
                pending_artifact_ids.append(dependency.source_artifact_id)

    manifest_bindings = _manifest_binding_payloads(
        registry_path,
        context=selected_artifact.context,
        artifacts=artifacts.values(),
        warnings=warnings,
    )
    return {
        "schema_version": TRACE_SCHEMA_VERSION,
        "context": selected_artifact.context,
        "selected_artifact_id": selected_artifact.artifact_id,
        "provenance_status": "degraded" if warnings else "complete",
        "artifacts": sorted(artifacts.values(), key=lambda row: row["artifact_id"]),
        "dependencies": sorted(
            (
                _dependency_payload(
                    dependency,
                    artifacts_by_id=artifacts,
                )
                for dependency in dependencies.values()
            ),
            key=lambda row: row["edge_id"],
        ),
        "manifest_bindings": manifest_bindings,
        "warnings": warnings,
    }


def _read_trace_artifact(
    registry_path: Path,
    *,
    artifact_id: int,
    selected_artifact: RegistryArtifact,
    pending_dependency: RegistryDependency | None,
    active_context: str | None,
    warnings: list[dict[str, Any]],
) -> RegistryArtifact | None:
    if artifact_id == selected_artifact.artifact_id:
        return selected_artifact
    try:
        artifact = read_artifact_by_id(registry_path, artifact_id)
    except ValidationError as exc:
        warnings.append(
            {
                "warning_type": "missing_artifact",
                "message": str(exc),
                "artifact_id": artifact_id,
                "input_path": (
                    None if pending_dependency is None else pending_dependency.input_path
                ),
            }
        )
        return None
    if active_context is not None and artifact.context != active_context:
        warnings.append(
            {
                "warning_type": "cross_context_dependency",
                "message": "dependency source artifact is outside the active context",
                "artifact_id": artifact_id,
                "input_path": (
                    None if pending_dependency is None else pending_dependency.input_path
                ),
            }
        )
        return None
    return artifact


def _last_warning_is_cross_context(
    warnings: list[dict[str, Any]],
    dependency: RegistryDependency,
) -> bool:
    if not warnings:
        return False
    warning = warnings[-1]
    return (
        warning.get("warning_type") == "cross_context_dependency"
        and warning.get("artifact_id") == dependency.source_artifact_id
        and warning.get("input_path") == dependency.input_path
    )


def _read_upstream_dependencies(
    registry_path: Path,
    *,
    artifact: RegistryArtifact,
    warnings: list[dict[str, Any]],
) -> list[RegistryDependency]:
    try:
        return list_upstream_dependencies(
            registry_path,
            artifact_id=artifact.artifact_id,
        )
    except ValidationError as exc:
        warnings.append(
            {
                "warning_type": "malformed_dependencies",
                "message": str(exc),
                "artifact_id": artifact.artifact_id,
                "input_path": None,
            }
        )
        return []


def _manifest_binding_payloads(
    registry_path: Path,
    *,
    context: str,
    artifacts: Iterable[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    run_ids = sorted(
        {
            artifact["run_id"]
            for artifact in artifacts
            if artifact["run_id"] is not None
        }
    )
    bindings: dict[tuple[int, str, str, str], dict[str, Any]] = {}
    for run_id in run_ids:
        try:
            run_bindings = list_run_manifest_bindings(
                registry_path,
                run_id=run_id,
                context=context,
            )
        except ValidationError as exc:
            warnings.append(
                {
                    "warning_type": "missing_manifest_bindings",
                    "message": str(exc),
                    "artifact_id": None,
                    "input_path": None,
                }
            )
            continue
        for binding in run_bindings:
            bindings[_manifest_binding_key(binding)] = _manifest_binding_payload(
                binding
            )
    return sorted(bindings.values(), key=_manifest_binding_sort_key)


def _artifact_payload(
    artifact: RegistryArtifact,
    *,
    is_selected: bool,
) -> dict[str, Any]:
    return {
        "artifact_id": artifact.artifact_id,
        "origin": artifact.origin,
        "run_id": artifact.run_id,
        "job_id": artifact.job_id,
        "artifact_set_id": artifact.artifact_set_id,
        "path": artifact.path,
        "display_path": artifact.published_path or artifact.path,
        "is_selected": is_selected,
        "is_selected_output": artifact.is_selected_output,
        "is_published": artifact.is_published,
        "published_path": artifact.published_path,
        "staging_path": artifact.staging_path,
        "workflow_name": artifact.workflow_name,
        "step_name": artifact.step_name,
        "output_name": artifact.output_name,
        "address": artifact.address,
        "parameter_hash": artifact.parameter_hash,
        "content_digest": artifact.content_digest,
        "output_hash": artifact.output_hash,
        "file_size": artifact.file_size,
        "extension": artifact.extension,
        "subject_id": artifact.subject_id,
        "session_id": artifact.session_id,
        "task_name": artifact.task_name,
        "run_label": artifact.run_label,
        "datatype": artifact.datatype,
        "suffix": artifact.suffix,
        "source_metadata": artifact.source_metadata,
        "workflow_artifact_ref": _workflow_artifact_ref(artifact),
        "callable_ref": artifact.callable_ref,
        "software_ref": artifact.software_ref,
    }


def _dependency_payload(
    dependency: RegistryDependency,
    *,
    artifacts_by_id: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "edge_id": _edge_id(dependency),
        "source_artifact_id": dependency.source_artifact_id,
        "dependent_artifact_id": dependency.dependent_artifact_id,
        "is_reused_input": _is_reused_input(
            dependency,
            artifacts_by_id=artifacts_by_id,
        ),
        "dependency_role": dependency.dependency_role,
        "binding_name": dependency.binding_name,
        "input_path": dependency.input_path,
        "source_content_digest": dependency.source_content_digest,
        "source_file_size": dependency.source_file_size,
        "source_extension": dependency.source_extension,
        "dependency_set_id": dependency.dependency_set_id,
        "manifest_digest": dependency.manifest_digest,
        "edge_cardinality": dependency.edge_cardinality,
    }


def _is_reused_input(
    dependency: RegistryDependency,
    *,
    artifacts_by_id: dict[int, dict[str, Any]],
) -> bool:
    source = artifacts_by_id.get(dependency.source_artifact_id)
    dependent = artifacts_by_id.get(dependency.dependent_artifact_id)
    if source is None or dependent is None:
        return False
    source_run_id = source["run_id"]
    dependent_run_id = dependent["run_id"]
    return (
        source["origin"] == "workflow_output"
        and dependent["origin"] == "workflow_output"
        and source_run_id is not None
        and dependent_run_id is not None
        and source_run_id != dependent_run_id
    )


def _manifest_binding_payload(
    binding: RegistryManifestBinding,
) -> dict[str, Any]:
    return {
        "run_id": binding.run_id,
        "workflow_name": binding.workflow_name,
        "step_name": binding.step_name,
        "role": binding.role,
        "manifest_name": binding.manifest_name,
        "manifest_digest": binding.manifest_digest,
        "manifest_hash": binding.manifest_hash,
        "entity_count": binding.entity_count,
    }


def _workflow_artifact_ref(artifact: RegistryArtifact) -> str | None:
    if artifact.origin != "workflow_output":
        return None
    if artifact.step_name is None or artifact.output_name is None:
        return None
    return f"artifact:{artifact.step_name}:{artifact.output_name}"


def _dependency_key(
    dependency: RegistryDependency,
) -> tuple[int, int, str, str]:
    return (
        dependency.dependent_artifact_id,
        dependency.source_artifact_id,
        dependency.input_path,
        dependency.binding_name,
    )


def _edge_id(dependency: RegistryDependency) -> str:
    return (
        f"{dependency.source_artifact_id}->{dependency.dependent_artifact_id}:"
        f"{dependency.binding_name}:{dependency.input_path}"
    )


def _manifest_binding_key(
    binding: RegistryManifestBinding,
) -> tuple[int, str, str, str]:
    return (
        binding.run_id,
        binding.step_name,
        binding.role,
        binding.manifest_name,
    )


def _manifest_binding_sort_key(row: dict[str, Any]) -> tuple[int, str, str, str]:
    return (
        row["run_id"],
        row["step_name"],
        row["role"],
        row["manifest_name"],
    )
