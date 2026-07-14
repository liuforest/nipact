"""Focused helpers for runtime/database/registry.db."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Iterator
from urllib.parse import quote

from .artifacts import parse_output_filename
from .errors import ValidationError
from .hashing import is_valid_digest, sha256_digest, sha256_file_digest, short_hash
from .identity import validate_hash_alias, validate_path_token
from .manifest import Manifest

REGISTRY_DB_PATH = "database/registry.db"
REGISTRY_SCHEMA_VERSION = 14
PARAMETER_HASH_VERSION = 1


@dataclass(frozen=True)
class PublishedOutputRow:
    context: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    path: str
    output_digest: str
    output_hash: str


@dataclass(frozen=True)
class ArtifactInputRow:
    binding_name: str
    input_path: str
    dependency_role: str
    origin: str
    source_step_name: str | None = None
    source_output_name: str | None = None
    source_address: str | None = None
    source_callable_ref: str | None = None
    source_parameters_json: str | None = None
    source_extension: str | None = None
    source_execution_role: str | None = None
    source_is_reused: bool | None = None
    source_artifact_path: str | None = None
    manifest_digest: str | None = None
    edge_cardinality: int | None = None
    registry_source_artifact_id: int | None = None
    source_input_records: tuple["ArtifactInputRow", ...] = ()


@dataclass(frozen=True)
class WorkflowOutputArtifactRow:
    step_name: str
    output_name: str
    address: str
    job_id: str
    path: str
    staging_path: str
    published_path: str | None
    content_digest: str
    output_hash: str
    file_size: int
    extension: str
    parameters_json: str
    callable_ref: str
    is_selected_output: bool
    is_published: bool
    input_records: tuple[ArtifactInputRow, ...]


@dataclass(frozen=True)
class RunManifestBindingRow:
    step_name: str
    role: str
    manifest_name: str
    manifest_digest: str
    manifest_hash: str
    entity_count: int


@dataclass(frozen=True)
class RegistryArtifact:
    artifact_id: int
    origin: str
    run_id: int | None
    context: str
    workflow_name: str | None
    step_name: str | None
    output_name: str | None
    address: str | None
    job_id: str | None
    artifact_set_id: str | None
    parameter_id: int | None
    parameter_hash_version: int | None
    parameter_hash: str | None
    parameter_digest: str | None
    parameters_json: str | None
    path: str
    is_selected_output: bool
    is_published: bool
    published_path: str | None
    staging_path: str | None
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
    callable_ref: str | None
    software_ref: str | None
    created_at: str


@dataclass(frozen=True)
class RegistryDependency:
    dependent_artifact_id: int
    source_artifact_id: int
    source_content_digest: str
    source_file_size: int
    source_extension: str
    input_path: str
    binding_name: str
    dependency_role: str
    source_step_name: str | None
    source_output_name: str | None
    source_address: str | None
    dependency_set_id: str | None
    manifest_digest: str | None
    edge_cardinality: int | None


@dataclass(frozen=True)
class ReusableArtifactRequest:
    context: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    extension: str
    callable_ref: str
    parameters_json: str
    input_records: tuple[ArtifactInputRow, ...]
    allowed_workflow_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReusableArtifactCandidate:
    artifact_id: int
    run_id: int
    path: str
    content_digest: str
    file_size: int
    extension: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    parameter_digest: str
    parameters_json: str
    callable_ref: str
    dependencies: tuple[RegistryDependency, ...]


@dataclass(frozen=True)
class RegistryManifestBinding:
    run_id: int
    context: str
    workflow_name: str
    step_name: str
    role: str
    manifest_name: str
    manifest_digest: str
    manifest_hash: str
    entity_count: int


@dataclass(frozen=True)
class RegistryManifest:
    context: str
    name: str
    path: str
    entity_count: int
    first_entity_id: str
    last_entity_id: str
    manifest_digest: str
    manifest_hash: str
    source_artifact_path: str | None
    manifest_body: str


@dataclass(frozen=True)
class ArtifactGroupCount:
    origin: str
    workflow_name: str | None
    step_name: str | None
    output_name: str | None
    artifact_count: int


_ARTIFACT_SELECT_COLUMNS = """
    a.artifact_id,
    a.origin,
    a.run_id,
    a.context,
    a.workflow_name,
    a.step_name,
    a.output_name,
    a.address,
    a.job_id,
    a.artifact_set_id,
    a.parameter_id,
    p.hash_version AS parameter_hash_version,
    p.parameter_hash,
    p.parameter_digest,
    p.parameters_json,
    a.path,
    a.is_selected_output,
    a.is_published,
    a.published_path,
    a.staging_path,
    a.content_digest,
    a.output_hash,
    a.file_size,
    a.extension,
    a.subject_id,
    a.session_id,
    a.task_name,
    a.run_label,
    a.datatype,
    a.suffix,
    a.source_metadata_json,
    a.callable_ref,
    a.software_ref,
    a.created_at
"""


def initialize_registry_db(
    path: Path,
    *,
    context: str,
    runtime_root: Path,
    source_artifact_path: str,
    source_entity_count: int,
    source_digest: str,
    source_hash: str,
    manifests: dict[str, Manifest],
    manifest_paths: dict[str, str],
) -> None:
    """Create the initial registry database for a new runtime root."""
    with _connect(path) as conn:
        _create_schema(conn)
        conn.execute(
            """
            INSERT INTO contexts (context, runtime_path)
            VALUES (?, ?)
            ON CONFLICT(context) DO UPDATE SET
                runtime_path = excluded.runtime_path
            """,
            (context, str(runtime_root)),
        )
        conn.execute(
            """
            INSERT INTO source_artifacts (
                context, path, entity_count, source_digest, source_hash
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(context, path) DO UPDATE SET
                entity_count = excluded.entity_count,
                source_digest = excluded.source_digest,
                source_hash = excluded.source_hash
            """,
            (
                context,
                source_artifact_path,
                source_entity_count,
                source_digest,
                source_hash,
            ),
        )
        _upsert_source_artifact(
            conn,
            context=context,
            runtime_root=runtime_root,
            source_artifact_path=source_artifact_path,
            source_entity_count=source_entity_count,
            source_digest=source_digest,
            source_hash=source_hash,
        )
        for name, manifest in manifests.items():
            conn.execute(
                """
                INSERT INTO manifests (
                    context, name, path, entity_count, first_entity_id,
                    last_entity_id, manifest_digest, manifest_hash,
                    source_artifact_path, manifest_body
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(context, name) DO UPDATE SET
                    path = excluded.path,
                    entity_count = excluded.entity_count,
                    first_entity_id = excluded.first_entity_id,
                    last_entity_id = excluded.last_entity_id,
                    manifest_digest = excluded.manifest_digest,
                    manifest_hash = excluded.manifest_hash,
                    source_artifact_path = excluded.source_artifact_path,
                    manifest_body = excluded.manifest_body
                """,
                (
                    context,
                    name,
                    manifest_paths[name],
                    manifest.entity_count,
                    manifest.first_entity_id,
                    manifest.last_entity_id,
                    manifest.manifest_digest,
                    manifest.manifest_hash,
                    source_artifact_path,
                    manifest.manifest_body,
                ),
            )


def initialize_prepared_demo_registry_db(
    path: Path,
    *,
    context: str,
    runtime_root: Path,
    manifests: dict[str, Manifest],
    manifest_paths: dict[str, str],
) -> None:
    """Create the small prepared-project registry surface for synthetic demos."""
    with _connect(path) as conn:
        _create_schema(conn)
        conn.execute(
            """
            INSERT INTO contexts (context, runtime_path)
            VALUES (?, ?)
            ON CONFLICT(context) DO UPDATE SET
                runtime_path = excluded.runtime_path
            """,
            (context, str(runtime_root)),
        )
        for name, manifest in manifests.items():
            conn.execute(
                """
                INSERT INTO manifests (
                    context, name, path, entity_count, first_entity_id,
                    last_entity_id, manifest_digest, manifest_hash,
                    source_artifact_path, manifest_body
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                ON CONFLICT(context, name) DO UPDATE SET
                    path = excluded.path,
                    entity_count = excluded.entity_count,
                    first_entity_id = excluded.first_entity_id,
                    last_entity_id = excluded.last_entity_id,
                    manifest_digest = excluded.manifest_digest,
                    manifest_hash = excluded.manifest_hash,
                    source_artifact_path = excluded.source_artifact_path,
                    manifest_body = excluded.manifest_body
                """,
                (
                    context,
                    name,
                    manifest_paths[name],
                    manifest.entity_count,
                    manifest.first_entity_id,
                    manifest.last_entity_id,
                    manifest.manifest_digest,
                    manifest.manifest_hash,
                    manifest.manifest_body,
                ),
            )


def validate_registry_db(
    path: Path,
    *,
    project_root: Path,
    runtime_root: Path,
    manifest_paths: dict[str, Path],
    context: str,
    source_artifact_path: str,
    source_entity_count: int,
    source_digest: str,
    source_hash: str,
    manifests: dict[str, Manifest],
    loaded_workflow_project: Any,
) -> dict[str, int]:
    """Validate registry rows against current project declarations and artifacts."""
    try:
        with _connect_readonly(path) as conn:
            _validate_schema_version(conn)
            context_row = conn.execute(
                "SELECT runtime_path FROM contexts WHERE context = ?",
                (context,),
            ).fetchone()
            if context_row is None:
                raise ValidationError("registry.db missing context row")
            if context_row != (str(runtime_root),):
                raise ValidationError("registry.db context row is out of date")
            source_row = conn.execute(
                """
                SELECT entity_count, source_digest, source_hash
                FROM source_artifacts
                WHERE context = ? AND path = ?
                """,
                (context, source_artifact_path),
            ).fetchone()
            if source_row != (source_entity_count, source_digest, source_hash):
                raise ValidationError("registry.db source artifact row is out of date")
            _validate_source_artifact_row(
                conn,
                context=context,
                runtime_root=runtime_root,
                source_artifact_path=source_artifact_path,
                source_entity_count=source_entity_count,
                source_digest=source_digest,
                source_hash=source_hash,
            )
            rows = conn.execute(
                """
                SELECT name, path, entity_count, first_entity_id, last_entity_id,
                       manifest_digest, manifest_hash, source_artifact_path,
                       manifest_body
                FROM manifests
                WHERE context = ?
                ORDER BY name
                """,
                (context,),
            ).fetchall()
            published_rows = conn.execute(
                """
                SELECT workflow_name, step_name, output_name, address, path,
                       output_digest, output_hash
                FROM published_outputs
                WHERE context = ?
                ORDER BY workflow_name, step_name, output_name, address
                """,
                (context,),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc

    expected_rows = [
        (
            name,
            manifest_paths[name].relative_to(project_root).as_posix(),
            manifest.entity_count,
            manifest.first_entity_id,
            manifest.last_entity_id,
            manifest.manifest_digest,
            manifest.manifest_hash,
            source_artifact_path,
            manifest.manifest_body,
        )
        for name, manifest in sorted(manifests.items())
    ]
    if rows != expected_rows:
        raise ValidationError("registry.db manifest rows are out of date")
    _validate_published_output_rows(
        published_rows,
        context=context,
        runtime_root=runtime_root,
        loaded_workflow_project=loaded_workflow_project,
    )
    return {"manifests": len(rows), "published_outputs": len(published_rows)}


def validate_prepared_registry_db(
    path: Path,
    *,
    context: str,
    runtime_root: Path,
) -> dict[str, int]:
    """Validate the small registry surface needed by generic prepared projects."""
    try:
        with _connect_readonly(path) as conn:
            _validate_schema_version(conn)
            context_row = conn.execute(
                "SELECT runtime_path FROM contexts WHERE context = ?",
                (context,),
            ).fetchone()
            if context_row is None:
                raise ValidationError("registry.db missing context row")
            if context_row != (str(runtime_root),):
                raise ValidationError("registry.db context row is out of date")
            published_outputs = conn.execute(
                """
                SELECT COUNT(*)
                FROM published_outputs
                WHERE context = ?
                """,
                (context,),
            ).fetchone()[0]
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return {"published_outputs": int(published_outputs)}


def record_workflow_run(
    path: Path,
    *,
    runtime_root: Path,
    context: str,
    workflow_name: str,
    selected_step_name: str,
    selected_output_name: str,
    run_workspace: str,
    run_plan_path: str,
    run_plan_digest: str,
    artifacts: Iterable[WorkflowOutputArtifactRow],
    manifest_bindings: Iterable[RunManifestBindingRow],
    published_outputs: Iterable[PublishedOutputRow],
    allowed_reused_workflow_names: Iterable[str] | None = None,
    base_workflow_name: str | None = None,
) -> int:
    """Record one run, superseding current rows while retaining history."""
    artifact_rows = tuple(artifacts)
    manifest_binding_rows = tuple(manifest_bindings)
    published_output_rows = tuple(published_outputs)
    allowed_reused_workflows = tuple(allowed_reused_workflow_names or (workflow_name,))
    now = _utc_now()
    try:
        with _connect(path) as conn:
            _validate_schema_version(conn)
            _delete_current_run_scope(
                conn,
                context=context,
                workflow_name=workflow_name,
                selected_step_name=selected_step_name,
                selected_output_name=selected_output_name,
            )
            run_id = _insert_workflow_run(
                conn,
                context=context,
                workflow_name=workflow_name,
                base_workflow_name=base_workflow_name,
                selected_step_name=selected_step_name,
                selected_output_name=selected_output_name,
                run_workspace=run_workspace,
                run_plan_path=run_plan_path,
                run_plan_digest=run_plan_digest,
                created_at=now,
            )
            parameter_ids = {
                (row.step_name, row.parameters_json): _upsert_parameter(
                    conn,
                    step_name=row.step_name,
                    parameters_json=row.parameters_json,
                    created_at=now,
                )
                for row in artifact_rows
            }
            artifact_ids = _insert_workflow_output_artifacts(
                conn,
                context=context,
                workflow_name=workflow_name,
                run_id=run_id,
                artifact_rows=artifact_rows,
                parameter_ids=parameter_ids,
                created_at=now,
            )
            _upsert_source_artifacts_for_inputs(
                conn,
                context=context,
                runtime_root=runtime_root,
                artifact_rows=artifact_rows,
                created_at=now,
            )
            _insert_artifact_dependencies(
                conn,
                runtime_root=runtime_root,
                context=context,
                workflow_name=workflow_name,
                allowed_reused_workflow_names=allowed_reused_workflows,
                artifact_rows=artifact_rows,
                artifact_ids=artifact_ids,
            )
            _insert_run_manifest_bindings(
                conn,
                context=context,
                workflow_name=workflow_name,
                run_id=run_id,
                rows=manifest_binding_rows,
            )
            _delete_published_output_coordinates(conn, rows=published_output_rows)
            _insert_published_outputs(
                conn,
                rows=published_output_rows,
                artifact_ids=artifact_ids,
            )
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return len(published_output_rows)


def read_published_outputs(
    runtime_root: Path,
    *,
    context: str,
    workflow_name: str,
    step_name: str,
    output_name: str,
) -> list[dict[str, str]]:
    """Read published-output rows for one workflow step output."""
    registry_path = runtime_root / REGISTRY_DB_PATH
    try:
        with _connect_readonly(registry_path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                """
                SELECT address, path, output_digest, output_hash
                FROM published_outputs
                WHERE context = ?
                  AND workflow_name = ?
                  AND step_name = ?
                  AND output_name = ?
                ORDER BY address
                """,
                (context, workflow_name, step_name, output_name),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [
        {
            "address": address,
            "path": path,
            "output_digest": output_digest,
            "output_hash": output_hash,
        }
        for address, path, output_digest, output_hash in rows
    ]


def _read_artifact_by_id_conn(
    conn: sqlite3.Connection,
    artifact_id: int,
) -> RegistryArtifact:
    """Read one artifact by id on an already-open read connection."""
    try:
        row = conn.execute(
            f"""
            SELECT {_ARTIFACT_SELECT_COLUMNS}
            FROM artifacts a
            LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
            WHERE a.artifact_id = ?
            """,
            (artifact_id,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if row is None:
        raise ValidationError(f"unknown registry artifact id: {artifact_id}")
    return _registry_artifact_from_row(row)


def read_artifact_by_id(path: Path, artifact_id: int) -> RegistryArtifact:
    """Read one registered artifact by database id."""
    _validate_positive_id(artifact_id, label="artifact id")
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            return _read_artifact_by_id_conn(conn, artifact_id)
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc


def read_artifact_by_id_for_context(
    path: Path,
    *,
    context: str,
    artifact_id: int,
) -> RegistryArtifact:
    """Read one artifact by id without exposing other contexts."""
    _validate_positive_id(artifact_id, label="artifact id")
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            row = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM artifacts a
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.artifact_id = ? AND a.context = ?
                """,
                (artifact_id, context),
            ).fetchone()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if row is None:
        raise ValidationError(f"unknown registry artifact id: {artifact_id}")
    return _registry_artifact_from_row(row)


def read_artifact_by_path(
    path: Path,
    *,
    context: str,
    artifact_path: str,
) -> RegistryArtifact:
    """Read one registered artifact by runtime-relative registry path."""
    artifact_path = _validate_registry_lookup_path(artifact_path)
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM artifacts a
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.context = ? AND a.path = ?
                """,
                (context, artifact_path),
            ).fetchall()
            current_published_rows = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM artifacts a
                JOIN published_outputs po ON po.artifact_id = a.artifact_id
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.context = ?
                  AND a.path = ?
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND po.context = a.context
                """,
                (context, artifact_path),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if not rows:
        raise ValidationError(f"unknown registered artifact path: {artifact_path}")
    if len(rows) > 1 and len(current_published_rows) == 1:
        return _registry_artifact_from_row(current_published_rows[0])
    if len(rows) > 1:
        raise ValidationError(f"ambiguous registered artifact path: {artifact_path}")
    return _registry_artifact_from_row(rows[0])


def resolve_registered_artifact_path(
    path: Path,
    *,
    context: str,
    artifact_path: str,
) -> RegistryArtifact:
    """Resolve a registered artifact path from path, published_path, or staging_path."""
    artifact_path = _validate_registry_lookup_path(artifact_path)
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM artifacts a
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.context = ?
                  AND (
                    a.path = ?
                    OR a.published_path = ?
                    OR a.staging_path = ?
                  )
                ORDER BY a.artifact_id
                """,
                (context, artifact_path, artifact_path, artifact_path),
            ).fetchall()
            current_published_rows = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM artifacts a
                JOIN published_outputs po ON po.artifact_id = a.artifact_id
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.context = ?
                  AND (
                    a.path = ?
                    OR a.published_path = ?
                    OR a.staging_path = ?
                  )
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND po.context = a.context
                """,
                (context, artifact_path, artifact_path, artifact_path),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if not rows:
        raise ValidationError(f"unknown registered artifact path: {artifact_path}")
    if len(rows) > 1 and len(current_published_rows) == 1:
        return _registry_artifact_from_row(current_published_rows[0])
    if len(rows) > 1:
        raise ValidationError(f"ambiguous registered artifact path: {artifact_path}")
    return _registry_artifact_from_row(rows[0])


def _artifact_filter_where(
    *,
    context: str | None,
    origin: str | None,
    workflow_name: str | None,
    step_name: str | None,
    output_name: str | None,
    address: str | None,
    is_selected_output: bool | None,
    is_published: bool | None,
) -> tuple[str, list[object]]:
    """Build the shared exact-match WHERE clause for artifact queries.

    ``list_artifacts`` and ``list_artifact_group_counts`` must filter identically
    so a group's count describes exactly the rows the list would return under the
    same filters; sharing this builder makes that impossible to drift.
    """
    if origin is not None and origin not in {"source", "workflow_output"}:
        raise ValidationError("artifact origin must be source or workflow_output")
    filters: list[tuple[str, object]] = []
    for column, value in (
        ("a.context", context),
        ("a.origin", origin),
        ("a.workflow_name", workflow_name),
        ("a.step_name", step_name),
        ("a.output_name", output_name),
        ("a.address", address),
    ):
        if value is not None:
            filters.append((column, value))
    if is_selected_output is not None:
        filters.append(("a.is_selected_output", int(is_selected_output)))
    if is_published is not None:
        filters.append(("a.is_published", int(is_published)))

    if not filters:
        return "", []
    where_sql = "WHERE " + " AND ".join(f"{column} = ?" for column, _ in filters)
    values = [value for _, value in filters]
    return where_sql, values


def list_artifacts(
    path: Path,
    *,
    context: str | None = None,
    origin: str | None = None,
    workflow_name: str | None = None,
    step_name: str | None = None,
    output_name: str | None = None,
    address: str | None = None,
    is_selected_output: bool | None = None,
    is_published: bool | None = None,
) -> list[RegistryArtifact]:
    """List registered artifacts with simple exact-match filters."""
    where_sql, values = _artifact_filter_where(
        context=context,
        origin=origin,
        workflow_name=workflow_name,
        step_name=step_name,
        output_name=output_name,
        address=address,
        is_selected_output=is_selected_output,
        is_published=is_published,
    )

    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM artifacts a
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                {where_sql}
                ORDER BY
                    a.context, a.origin, a.workflow_name, a.step_name,
                    a.output_name, a.address, a.artifact_id
                """,
                tuple(values),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [_registry_artifact_from_row(row) for row in rows]


def list_artifact_group_counts(
    path: Path,
    *,
    context: str | None = None,
    origin: str | None = None,
    workflow_name: str | None = None,
    step_name: str | None = None,
    output_name: str | None = None,
    address: str | None = None,
    is_selected_output: bool | None = None,
    is_published: bool | None = None,
) -> list[ArtifactGroupCount]:
    """Count registered artifacts grouped by their coordinate.

    Honors the same filters as :func:`list_artifacts`, so each group's count is
    exactly the number of rows that ``list_artifacts`` would return for that
    coordinate. Source rows keep their null workflow/step/output coordinates.
    """
    where_sql, values = _artifact_filter_where(
        context=context,
        origin=origin,
        workflow_name=workflow_name,
        step_name=step_name,
        output_name=output_name,
        address=address,
        is_selected_output=is_selected_output,
        is_published=is_published,
    )

    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                f"""
                SELECT
                    a.origin,
                    a.workflow_name,
                    a.step_name,
                    a.output_name,
                    COUNT(*) AS artifact_count
                FROM artifacts a
                {where_sql}
                GROUP BY a.origin, a.workflow_name, a.step_name, a.output_name
                ORDER BY a.origin, a.workflow_name, a.step_name, a.output_name
                """,
                tuple(values),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [
        ArtifactGroupCount(
            origin=row["origin"],
            workflow_name=row["workflow_name"],
            step_name=row["step_name"],
            output_name=row["output_name"],
            artifact_count=row["artifact_count"],
        )
        for row in rows
    ]


def resolve_reusable_artifact(
    path: Path,
    *,
    runtime_root: Path,
    request: ReusableArtifactRequest,
) -> ReusableArtifactCandidate | None:
    """Return one validated reusable workflow artifact, or None if none exists."""
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM published_outputs po
                JOIN artifacts a ON po.artifact_id = a.artifact_id
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE po.context = ?
                  AND a.context = po.context
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND po.workflow_name = ?
                  AND po.step_name = ?
                  AND po.output_name = ?
                  AND po.address = ?
                  AND a.workflow_name = po.workflow_name
                  AND a.step_name = po.step_name
                  AND a.output_name = po.output_name
                  AND a.address = po.address
                  AND a.published_path = po.path
                  AND a.content_digest = po.output_digest
                  AND a.extension = ?
                  AND a.callable_ref = ?
                  AND p.parameters_json = ?
                ORDER BY a.artifact_id
                """,
                (
                    request.context,
                    request.workflow_name,
                    request.step_name,
                    request.output_name,
                    request.address,
                    request.extension,
                    request.callable_ref,
                    request.parameters_json,
                ),
            ).fetchall()
            valid: list[ReusableArtifactCandidate] = []
            for row in rows:
                artifact = _registry_artifact_from_row(row)
                dependencies = _dependencies_for_artifact(conn, artifact.artifact_id)
                if not _dependencies_match_request(
                    conn,
                    dependencies=dependencies,
                    input_records=request.input_records,
                    allowed_workflow_names=_request_allowed_workflow_names(request),
                ):
                    continue
                _validate_reusable_artifact_file(
                    runtime_root=runtime_root,
                    artifact=artifact,
                )
                if (
                    artifact.parameter_digest is None
                    or artifact.parameters_json is None
                    or artifact.callable_ref is None
                    or artifact.workflow_name is None
                    or artifact.step_name is None
                    or artifact.output_name is None
                    or artifact.address is None
                    or artifact.run_id is None
                ):
                    raise ValidationError("registry reusable artifact row is incomplete")
                valid.append(
                    ReusableArtifactCandidate(
                        artifact_id=artifact.artifact_id,
                        run_id=artifact.run_id,
                        path=artifact.path,
                        content_digest=artifact.content_digest,
                        file_size=artifact.file_size,
                        extension=artifact.extension,
                        workflow_name=artifact.workflow_name,
                        step_name=artifact.step_name,
                        output_name=artifact.output_name,
                        address=artifact.address,
                        parameter_digest=artifact.parameter_digest,
                        parameters_json=artifact.parameters_json,
                        callable_ref=artifact.callable_ref,
                        dependencies=tuple(dependencies),
                    )
                )
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if not valid:
        return None
    if len(valid) > 1:
        raise ValidationError("ambiguous reusable artifact candidates")
    return valid[0]


def _request_allowed_workflow_names(
    request: ReusableArtifactRequest,
) -> tuple[str, ...]:
    return request.allowed_workflow_names or (request.workflow_name,)


def _dependencies_for_artifact(
    conn: sqlite3.Connection,
    artifact_id: int,
) -> list[RegistryDependency]:
    rows = conn.execute(
        """
        SELECT dependent_artifact_id, source_artifact_id,
               source_content_digest, source_file_size, source_extension, input_path,
               binding_name, dependency_role, source_step_name,
               source_output_name, source_address, dependency_set_id,
               manifest_digest, edge_cardinality
        FROM artifact_dependencies
        WHERE dependent_artifact_id = ?
        ORDER BY binding_name, input_path, source_artifact_id
        """,
        (artifact_id,),
    ).fetchall()
    return [_registry_dependency_from_row(row) for row in rows]


def _dependencies_match_request(
    conn: sqlite3.Connection,
    *,
    dependencies: list[RegistryDependency],
    input_records: tuple[ArtifactInputRow, ...],
    allowed_workflow_names: tuple[str, ...],
) -> bool:
    if len(dependencies) != len(input_records):
        return False
    unused = list(dependencies)
    for input_record in input_records:
        match_index = _find_matching_dependency_index(
            conn,
            dependencies=unused,
            input_record=input_record,
            allowed_workflow_names=allowed_workflow_names,
        )
        if match_index is None:
            return False
        unused.pop(match_index)
    return not unused


def _find_matching_dependency_index(
    conn: sqlite3.Connection,
    *,
    dependencies: list[RegistryDependency],
    input_record: ArtifactInputRow,
    allowed_workflow_names: tuple[str, ...],
) -> int | None:
    for index, dependency in enumerate(dependencies):
        if not _dependency_common_fields_match(dependency, input_record):
            continue
        source = _dependency_source_artifact(conn, dependency.source_artifact_id)
        if input_record.origin == "source":
            if not _source_dependency_matches_current_input(
                dependency=dependency,
                source=source,
                input_record=input_record,
            ):
                continue
            return index
        if input_record.origin == "workflow_output":
            if not _workflow_dependency_matches_input(
                conn,
                dependency=dependency,
                source=source,
                input_record=input_record,
                allowed_workflow_names=allowed_workflow_names,
            ):
                continue
            return index
        raise ValidationError(f"unsupported dependency artifact origin: {input_record.origin}")
    return None


def _dependency_common_fields_match(
    dependency: RegistryDependency,
    input_record: ArtifactInputRow,
) -> bool:
    return (
        dependency.binding_name == input_record.binding_name
        and dependency.dependency_role == input_record.dependency_role
        and dependency.manifest_digest == input_record.manifest_digest
        and dependency.edge_cardinality == input_record.edge_cardinality
    )


def _source_dependency_matches_current_input(
    *,
    dependency: RegistryDependency,
    source: RegistryArtifact,
    input_record: ArtifactInputRow,
) -> bool:
    if source.origin != "source":
        return False
    if input_record.source_artifact_path is None:
        raise ValidationError("source dependency is missing source artifact path")
    if source.path != input_record.source_artifact_path:
        return False
    return (
        dependency.source_content_digest == source.content_digest
        and dependency.source_file_size == source.file_size
        and dependency.source_extension == source.extension
    )


def _workflow_dependency_matches_input(
    conn: sqlite3.Connection,
    *,
    dependency: RegistryDependency,
    source: RegistryArtifact,
    input_record: ArtifactInputRow,
    allowed_workflow_names: tuple[str, ...],
) -> bool:
    if source.origin != "workflow_output":
        return False
    if (
        input_record.source_callable_ref is not None
        and source.callable_ref != input_record.source_callable_ref
    ):
        return False
    if (
        input_record.source_parameters_json is not None
        and source.parameters_json != input_record.source_parameters_json
    ):
        return False
    if (
        input_record.source_extension is not None
        and source.extension != input_record.source_extension
    ):
        return False
    if not _workflow_dependency_source_matches_registry_source(
        conn,
        dependency=dependency,
        source=source,
        input_record=input_record,
        allowed_workflow_names=allowed_workflow_names,
    ):
        return False
    _validate_workflow_dependency_snapshot(dependency=dependency, source=source)
    if input_record.source_is_reused is False:
        if input_record.source_execution_role != "source_import":
            return False
        if not input_record.source_input_records:
            return False
        if not _dependencies_match_request(
            conn,
            dependencies=_dependencies_for_artifact(conn, source.artifact_id),
            input_records=input_record.source_input_records,
            allowed_workflow_names=allowed_workflow_names,
        ):
            return False
    elif not _workflow_artifact_dependencies_match_registry(
        conn,
        artifact_id=source.artifact_id,
        visited=set(),
    ):
        return False
    return (
        dependency.source_step_name == input_record.source_step_name
        and dependency.source_output_name == input_record.source_output_name
        and dependency.source_address == input_record.source_address
        and source.step_name == input_record.source_step_name
        and source.output_name == input_record.source_output_name
        and source.address == input_record.source_address
    )


def _workflow_dependency_source_matches_registry_source(
    conn: sqlite3.Connection,
    *,
    dependency: RegistryDependency,
    source: RegistryArtifact,
    input_record: ArtifactInputRow,
    allowed_workflow_names: tuple[str, ...],
) -> bool:
    if input_record.registry_source_artifact_id is None:
        return True
    if dependency.source_artifact_id == input_record.registry_source_artifact_id:
        return True
    requested = _dependency_source_artifact(
        conn,
        input_record.registry_source_artifact_id,
    )
    if requested.origin != "workflow_output":
        return False
    if source.workflow_name != requested.workflow_name and not (
        source.workflow_name in allowed_workflow_names
        and requested.workflow_name in allowed_workflow_names
    ):
        return False
    if (
        source.context != requested.context
        or source.step_name != requested.step_name
        or source.output_name != requested.output_name
        or source.address != requested.address
        or source.callable_ref != requested.callable_ref
        or source.parameters_json != requested.parameters_json
        or source.content_digest != requested.content_digest
        or source.file_size != requested.file_size
        or source.extension != requested.extension
    ):
        return False
    return True


def _workflow_artifact_dependencies_match_registry(
    conn: sqlite3.Connection,
    *,
    artifact_id: int,
    visited: set[int],
) -> bool:
    if artifact_id in visited:
        raise ValidationError("registry dependency graph contains a cycle")
    visited.add(artifact_id)
    dependencies = _dependencies_for_artifact(conn, artifact_id)
    if not dependencies:
        visited.remove(artifact_id)
        return False
    for dependency in dependencies:
        source = _dependency_source_artifact(conn, dependency.source_artifact_id)
        if source.origin == "source":
            if (
                dependency.source_content_digest != source.content_digest
                or dependency.source_file_size != source.file_size
                or dependency.source_extension != source.extension
            ):
                visited.remove(artifact_id)
                return False
            continue
        if source.origin == "workflow_output":
            _validate_workflow_dependency_snapshot(
                dependency=dependency,
                source=source,
            )
            if not _workflow_artifact_dependencies_match_registry(
                conn,
                artifact_id=source.artifact_id,
                visited=visited,
            ):
                visited.remove(artifact_id)
                return False
            continue
        visited.remove(artifact_id)
        return False
    visited.remove(artifact_id)
    return True


def _validate_workflow_dependency_snapshot(
    *,
    dependency: RegistryDependency,
    source: RegistryArtifact,
) -> None:
    if (
        dependency.source_content_digest != source.content_digest
        or dependency.source_file_size != source.file_size
        or dependency.source_extension != source.extension
    ):
        raise ValidationError("registry dependency source snapshot is stale")


def _dependency_source_artifact(
    conn: sqlite3.Connection,
    artifact_id: int,
) -> RegistryArtifact:
    row = conn.execute(
        f"""
        SELECT {_ARTIFACT_SELECT_COLUMNS}
        FROM artifacts a
        LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
        WHERE a.artifact_id = ?
        """,
        (artifact_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("registry dependency source artifact is missing")
    return _registry_artifact_from_row(row)


def _validate_reusable_artifact_file(
    *,
    runtime_root: Path,
    artifact: RegistryArtifact,
) -> None:
    artifact_path = _runtime_relative_file_path(runtime_root, artifact.path)
    if not artifact_path.is_file():
        raise ValidationError("registered reusable artifact file is missing")
    if artifact_path.stat().st_size != artifact.file_size:
        raise ValidationError("registered reusable artifact file size mismatch")


def _runtime_relative_file_path(runtime_root: Path, artifact_path: str) -> Path:
    relative_path = Path(_validate_registry_lookup_path(artifact_path)).expanduser()
    resolved_root = runtime_root.resolve()
    resolved_path = (runtime_root / relative_path).resolve()
    if not _path_contains_or_same(resolved_root, resolved_path):
        raise ValidationError("registered artifact path must stay inside runtime dir")
    return resolved_path


def read_current_published_artifact(
    path: Path,
    *,
    context: str,
    workflow_name: str,
    step_name: str,
    output_name: str,
    address: str,
) -> RegistryArtifact:
    """Read the current published artifact for a workflow coordinate."""
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            row = conn.execute(
                f"""
                SELECT {_ARTIFACT_SELECT_COLUMNS}
                FROM published_outputs po
                JOIN artifacts a ON po.artifact_id = a.artifact_id
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE po.context = ?
                  AND po.workflow_name = ?
                  AND po.step_name = ?
                  AND po.output_name = ?
                  AND po.address = ?
                  AND a.context = po.context
                  AND a.workflow_name = po.workflow_name
                  AND a.step_name = po.step_name
                  AND a.output_name = po.output_name
                  AND a.address = po.address
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND a.published_path = po.path
                  AND a.content_digest = po.output_digest
                """,
                (context, workflow_name, step_name, output_name, address),
            ).fetchone()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if row is None:
        raise ValidationError("unknown current published artifact")
    return _registry_artifact_from_row(row)


def _list_upstream_dependencies_conn(
    conn: sqlite3.Connection,
    *,
    artifact_id: int,
) -> list[RegistryDependency]:
    """List one-hop dependency edges into an artifact on an open read connection."""
    try:
        _require_artifact_id(conn, artifact_id)
        rows = conn.execute(
            """
            SELECT dependent_artifact_id, source_artifact_id,
                   source_content_digest, source_file_size, source_extension, input_path,
                   binding_name, dependency_role, source_step_name,
                   source_output_name, source_address, dependency_set_id,
                   manifest_digest, edge_cardinality
            FROM artifact_dependencies
            WHERE dependent_artifact_id = ?
            ORDER BY binding_name, input_path, source_artifact_id
            """,
            (artifact_id,),
        ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [_registry_dependency_from_row(row) for row in rows]


def list_upstream_dependencies(
    path: Path,
    *,
    artifact_id: int,
) -> list[RegistryDependency]:
    """List one-hop dependency edges into an artifact."""
    _validate_positive_id(artifact_id, label="artifact id")
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            return _list_upstream_dependencies_conn(conn, artifact_id=artifact_id)
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc


def _list_run_manifest_bindings_conn(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    context: str | None = None,
) -> list[RegistryManifestBinding]:
    """List manifest bindings for a run on an already-open read connection."""
    where_sql = "WHERE run_id = ?"
    values: tuple[object, ...] = (run_id,)
    if context is not None:
        where_sql += " AND context = ?"
        values = (run_id, context)
    try:
        _require_run_id(conn, run_id)
        rows = conn.execute(
            """
            SELECT run_id, context, workflow_name, step_name, role,
                   manifest_name, manifest_digest, manifest_hash, entity_count
            FROM run_manifest_bindings
            {where_sql}
            ORDER BY step_name, role, manifest_name
            """.format(where_sql=where_sql),
            values,
        ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [_registry_manifest_binding_from_row(row) for row in rows]


def list_run_manifest_bindings(
    path: Path,
    *,
    run_id: int,
    context: str | None = None,
) -> list[RegistryManifestBinding]:
    """List manifest bindings recorded for a workflow run."""
    _validate_positive_id(run_id, label="run id")
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            return _list_run_manifest_bindings_conn(
                conn,
                run_id=run_id,
                context=context,
            )
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc


class _RegistryReadSession:
    """One read-only snapshot session for a single provenance trace traversal.

    Wraps one open read connection so a traversal can perform many artifact,
    upstream-dependency, and manifest-binding reads without reopening a
    connection or revalidating the schema per hop. Kept private and limited to
    the three trace reads; it does not expose the underlying connection.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def read_artifact_by_id(self, artifact_id: int) -> RegistryArtifact:
        _validate_positive_id(artifact_id, label="artifact id")
        return _read_artifact_by_id_conn(self._conn, artifact_id)

    def list_upstream_dependencies(
        self,
        *,
        artifact_id: int,
    ) -> list[RegistryDependency]:
        _validate_positive_id(artifact_id, label="artifact id")
        return _list_upstream_dependencies_conn(self._conn, artifact_id=artifact_id)

    def list_run_manifest_bindings(
        self,
        *,
        run_id: int,
        context: str | None = None,
    ) -> list[RegistryManifestBinding]:
        _validate_positive_id(run_id, label="run id")
        return _list_run_manifest_bindings_conn(
            self._conn,
            run_id=run_id,
            context=context,
        )


@contextmanager
def _open_registry_read_session(path: Path) -> Iterator[_RegistryReadSession]:
    """Open one read-only snapshot session for a provenance trace traversal.

    Opens a single read-only connection, starts one explicit read transaction so
    the traversal observes a consistent snapshot, and validates the schema once
    inside that transaction. The connection and transaction are released through
    context-manager cleanup on both success and failure.
    """
    with _connect_readonly_rows(path) as conn:
        try:
            conn.execute("BEGIN")
            _validate_schema_version(conn)
        except sqlite3.Error as exc:
            raise ValidationError(f"registry.db is malformed: {exc}") from exc
        yield _RegistryReadSession(conn)


def list_manifests(path: Path, *, context: str) -> list[RegistryManifest]:
    """List manifests recorded for one context."""
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            rows = conn.execute(
                """
                SELECT context, name, path, entity_count, first_entity_id,
                       last_entity_id, manifest_digest, manifest_hash,
                       source_artifact_path, manifest_body
                FROM manifests
                WHERE context = ?
                ORDER BY name
                """,
                (context,),
            ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [_registry_manifest_from_row(row) for row in rows]


def read_manifest(
    path: Path,
    *,
    context: str,
    manifest_name: str,
) -> RegistryManifest:
    """Read one manifest row for one context."""
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            row = conn.execute(
                """
                SELECT context, name, path, entity_count, first_entity_id,
                       last_entity_id, manifest_digest, manifest_hash,
                       source_artifact_path, manifest_body
                FROM manifests
                WHERE context = ? AND name = ?
                """,
                (context, manifest_name),
            ).fetchone()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if row is None:
        raise ValidationError(f"unknown manifest: {manifest_name}")
    return _registry_manifest_from_row(row)


def read_registry_summary(path: Path, *, context: str) -> dict[str, int]:
    """Read small registry counts for a GUI summary."""
    try:
        with _connect_readonly(path) as conn:
            _validate_schema_version(conn)
            context_row = conn.execute(
                "SELECT 1 FROM contexts WHERE context = ?",
                (context,),
            ).fetchone()
            if context_row is None:
                raise ValidationError(f"unknown context: {context}")
            manifest_count = _count_rows(conn, "manifests", context=context)
            artifact_count = _count_rows(conn, "artifacts", context=context)
            source_artifact_count = _count_rows(
                conn,
                "artifacts",
                context=context,
                origin="source",
            )
            workflow_output_count = _count_rows(
                conn,
                "artifacts",
                context=context,
                origin="workflow_output",
            )
            workflow_run_count = _count_rows(conn, "workflow_runs", context=context)
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return {
        "manifest_count": manifest_count,
        "artifact_count": artifact_count,
        "source_artifact_count": source_artifact_count,
        "workflow_output_count": workflow_output_count,
        "workflow_run_count": workflow_run_count,
    }


def read_context_runtime_path(path: Path, *, context: str) -> str:
    """Read the registered runtime path for one context."""
    try:
        with _connect_readonly(path) as conn:
            _validate_schema_version(conn)
            row = conn.execute(
                "SELECT runtime_path FROM contexts WHERE context = ?",
                (context,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if row is None:
        raise ValidationError(f"unknown context: {context}")
    return str(row[0])


def _delete_current_run_scope(
    conn: sqlite3.Connection,
    *,
    context: str,
    workflow_name: str,
    selected_step_name: str,
    selected_output_name: str,
) -> None:
    conn.execute(
        """
        UPDATE workflow_runs
        SET is_current = 0
        WHERE context = ?
          AND workflow_name = ?
          AND selected_step_name = ?
          AND selected_output_name = ?
          AND is_current = 1
        """,
        (context, workflow_name, selected_step_name, selected_output_name),
    )


def _delete_published_output_coordinates(
    conn: sqlite3.Connection,
    *,
    rows: tuple[PublishedOutputRow, ...],
) -> None:
    conn.executemany(
        """
        DELETE FROM published_outputs
        WHERE context = ?
          AND workflow_name = ?
          AND step_name = ?
          AND output_name = ?
          AND address = ?
        """,
        (
            (
                row.context,
                row.workflow_name,
                row.step_name,
                row.output_name,
                row.address,
            )
            for row in rows
        ),
    )


def _insert_workflow_run(
    conn: sqlite3.Connection,
    *,
    context: str,
    workflow_name: str,
    base_workflow_name: str | None,
    selected_step_name: str,
    selected_output_name: str,
    run_workspace: str,
    run_plan_path: str,
    run_plan_digest: str,
    created_at: str,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO workflow_runs (
            context, workflow_name, selected_step_name, selected_output_name,
            run_workspace, run_plan_path, run_plan_digest, base_workflow_name,
            is_current, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            context,
            workflow_name,
            selected_step_name,
            selected_output_name,
            run_workspace,
            run_plan_path,
            run_plan_digest,
            base_workflow_name,
            created_at,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_parameter(
    conn: sqlite3.Connection,
    *,
    step_name: str,
    parameters_json: str,
    created_at: str,
) -> int:
    parameter_digest = sha256_digest(parameters_json.encode("utf-8"))
    parameter_hash = short_hash(parameter_digest)
    conn.execute(
        """
        INSERT INTO parameters (
            hash_version, parameter_hash, parameter_digest, step_name,
            parameters_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(hash_version, step_name, parameter_hash)
        DO UPDATE SET
            parameter_digest = excluded.parameter_digest,
            parameters_json = excluded.parameters_json
        """,
        (
            PARAMETER_HASH_VERSION,
            parameter_hash,
            parameter_digest,
            step_name,
            parameters_json,
            created_at,
        ),
    )
    row = conn.execute(
        """
        SELECT parameter_id
        FROM parameters
        WHERE hash_version = ? AND step_name = ? AND parameter_hash = ?
        """,
        (PARAMETER_HASH_VERSION, step_name, parameter_hash),
    ).fetchone()
    if row is None:
        raise ValidationError("registry.db failed to store parameter row")
    return int(row[0])


def _insert_workflow_output_artifacts(
    conn: sqlite3.Connection,
    *,
    context: str,
    workflow_name: str,
    run_id: int,
    artifact_rows: tuple[WorkflowOutputArtifactRow, ...],
    parameter_ids: dict[tuple[str, str], int],
    created_at: str,
) -> dict[tuple[str, str, str], int]:
    artifact_ids: dict[tuple[str, str, str], int] = {}
    for row in artifact_rows:
        parameter_id = parameter_ids[(row.step_name, row.parameters_json)]
        cursor = conn.execute(
            """
            INSERT INTO artifacts (
                origin, run_id, context, workflow_name, step_name, output_name,
                address, job_id, parameter_id, path, is_selected_output,
                is_published, published_path, staging_path, content_digest,
                output_hash, file_size, extension, callable_ref, created_at
            )
            VALUES (
                'workflow_output', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?
            )
            """,
            (
                run_id,
                context,
                workflow_name,
                row.step_name,
                row.output_name,
                row.address,
                row.job_id,
                parameter_id,
                row.path,
                int(row.is_selected_output),
                int(row.is_published),
                row.published_path,
                row.staging_path,
                row.content_digest,
                row.output_hash,
                row.file_size,
                row.extension,
                row.callable_ref,
                created_at,
            ),
        )
        artifact_ids[(row.step_name, row.output_name, row.address)] = int(
            cursor.lastrowid
        )
    return artifact_ids


def _upsert_source_artifacts_for_inputs(
    conn: sqlite3.Connection,
    *,
    context: str,
    runtime_root: Path,
    artifact_rows: tuple[WorkflowOutputArtifactRow, ...],
    created_at: str,
) -> None:
    source_paths: set[str] = set()
    for row in artifact_rows:
        for input_record in row.input_records:
            if input_record.origin != "source":
                continue
            if input_record.source_artifact_path is None:
                raise ValidationError("source dependency is missing source artifact path")
            source_paths.add(input_record.source_artifact_path)

    for source_artifact_path in sorted(source_paths):
        _upsert_generic_source_artifact(
            conn,
            context=context,
            runtime_root=runtime_root,
            source_artifact_path=source_artifact_path,
            created_at=created_at,
        )


def _upsert_generic_source_artifact(
    conn: sqlite3.Connection,
    *,
    context: str,
    runtime_root: Path,
    source_artifact_path: str,
    created_at: str,
) -> None:
    source_path = _source_file_path(runtime_root, source_artifact_path)
    content_digest = sha256_file_digest(source_path)
    conn.execute(
        """
        INSERT INTO artifacts (
            origin, context, path, content_digest, output_hash, file_size,
            extension, created_at
        )
        VALUES ('source', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(context, path) WHERE origin = 'source'
        DO UPDATE SET
            content_digest = excluded.content_digest,
            output_hash = excluded.output_hash,
            file_size = excluded.file_size,
            extension = excluded.extension,
            created_at = excluded.created_at
        """,
        (
            context,
            source_artifact_path,
            content_digest,
            short_hash(content_digest),
            source_path.stat().st_size,
            _path_extension(source_artifact_path),
            created_at,
        ),
    )


def _insert_artifact_dependencies(
    conn: sqlite3.Connection,
    *,
    runtime_root: Path,
    context: str,
    workflow_name: str,
    allowed_reused_workflow_names: tuple[str, ...],
    artifact_rows: tuple[WorkflowOutputArtifactRow, ...],
    artifact_ids: dict[tuple[str, str, str], int],
) -> None:
    for row in artifact_rows:
        dependent_artifact_id = artifact_ids[(row.step_name, row.output_name, row.address)]
        for input_record in row.input_records:
            source_artifact_id = _dependency_source_artifact_id(
                conn,
                runtime_root=runtime_root,
                context=context,
                workflow_name=workflow_name,
                allowed_reused_workflow_names=allowed_reused_workflow_names,
                input_record=input_record,
                artifact_ids=artifact_ids,
            )
            source_content_digest, source_file_size, source_extension = (
                _source_artifact_snapshot(conn, source_artifact_id)
            )
            conn.execute(
                """
                INSERT INTO artifact_dependencies (
                    dependent_artifact_id, source_artifact_id,
                    source_content_digest, source_file_size, source_extension,
                    input_path, binding_name, dependency_role, source_step_name,
                    source_output_name, source_address, manifest_digest,
                    edge_cardinality
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dependent_artifact_id,
                    source_artifact_id,
                    source_content_digest,
                    source_file_size,
                    source_extension,
                    input_record.input_path,
                    input_record.binding_name,
                    input_record.dependency_role,
                    input_record.source_step_name,
                    input_record.source_output_name,
                    input_record.source_address,
                    input_record.manifest_digest,
                    input_record.edge_cardinality,
                ),
            )


def _dependency_source_artifact_id(
    conn: sqlite3.Connection,
    *,
    runtime_root: Path,
    context: str,
    workflow_name: str,
    allowed_reused_workflow_names: tuple[str, ...],
    input_record: ArtifactInputRow,
    artifact_ids: dict[tuple[str, str, str], int],
) -> int:
    if input_record.origin == "source":
        if input_record.source_artifact_path is None:
            raise ValidationError("source dependency is missing source artifact path")
        row = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE context = ? AND origin = 'source' AND path = ?
            """,
            (context, input_record.source_artifact_path),
        ).fetchone()
        if row is None:
            raise ValidationError("registry.db missing source artifact dependency")
        return int(row[0])
    if input_record.origin == "workflow_output":
        if (
            input_record.source_step_name is None
            or input_record.source_output_name is None
            or input_record.source_address is None
        ):
            raise ValidationError("workflow dependency is missing source coordinates")
        if input_record.registry_source_artifact_id is not None:
            _validate_reused_dependency_source(
                conn,
                runtime_root=runtime_root,
                context=context,
                workflow_name=workflow_name,
                allowed_reused_workflow_names=allowed_reused_workflow_names,
                input_record=input_record,
            )
            return input_record.registry_source_artifact_id
        try:
            return artifact_ids[
                (
                    input_record.source_step_name,
                    input_record.source_output_name,
                    input_record.source_address,
                )
            ]
        except KeyError as exc:
            raise ValidationError("workflow dependency source artifact was not registered") from exc
    raise ValidationError(f"unsupported dependency artifact origin: {input_record.origin}")


def _validate_reused_dependency_source(
    conn: sqlite3.Connection,
    *,
    runtime_root: Path,
    context: str,
    workflow_name: str,
    allowed_reused_workflow_names: tuple[str, ...],
    input_record: ArtifactInputRow,
) -> None:
    if input_record.registry_source_artifact_id is None:
        raise ValidationError("reused workflow dependency source artifact is missing")
    if workflow_name not in allowed_reused_workflow_names:
        raise ValidationError("current workflow is outside allowed reuse ancestry")
    if (
        input_record.source_callable_ref is None
        or input_record.source_parameters_json is None
        or input_record.source_extension is None
    ):
        raise ValidationError("reused workflow dependency source metadata is incomplete")
    row = conn.execute(
        """
        SELECT a.context, a.origin, a.workflow_name, a.step_name, a.output_name,
               a.address, a.callable_ref, p.parameters_json, a.extension,
               a.is_published, a.path, a.published_path, a.content_digest,
               a.file_size, po.path, po.output_digest, po.context,
               po.workflow_name, po.step_name, po.output_name, po.address
        FROM artifacts a
        LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
        LEFT JOIN published_outputs po ON po.artifact_id = a.artifact_id
        WHERE a.artifact_id = ?
        """,
        (input_record.registry_source_artifact_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("reused workflow dependency source artifact is unknown")
    if row[0] != context:
        raise ValidationError("reused workflow dependency source context mismatch")
    if row[1] != "workflow_output":
        raise ValidationError("reused workflow dependency source is not a workflow output")
    if row[2] not in allowed_reused_workflow_names:
        raise ValidationError("reused workflow dependency source is outside allowed ancestry")
    if (
        row[3] != input_record.source_step_name
        or row[4] != input_record.source_output_name
        or row[5] != input_record.source_address
    ):
        raise ValidationError("reused workflow dependency source coordinates mismatch")
    if (
        row[6] != input_record.source_callable_ref
        or row[7] != input_record.source_parameters_json
        or row[8] != input_record.source_extension
    ):
        raise ValidationError("reused workflow dependency source identity mismatch")
    if not row[9] or row[14] is None:
        raise ValidationError("reused workflow dependency source is not current published output")
    if (
        row[16] != row[0]
        or row[17] != row[2]
        or row[18] != row[3]
        or row[19] != row[4]
        or row[20] != row[5]
    ):
        raise ValidationError("reused workflow dependency source publication mismatch")
    if row[10] != row[14] or row[11] != row[14] or row[12] != row[15]:
        raise ValidationError("reused workflow dependency source publication mismatch")
    source_path = runtime_root / str(row[10])
    if not source_path.is_file():
        raise ValidationError("reused workflow dependency source file is missing")
    if source_path.stat().st_size != int(row[13]):
        raise ValidationError("reused workflow dependency source file size mismatch")


def _source_artifact_snapshot(
    conn: sqlite3.Connection,
    artifact_id: int,
) -> tuple[str, int, str]:
    row = conn.execute(
        """
        SELECT content_digest, file_size, extension
        FROM artifacts
        WHERE artifact_id = ?
        """,
        (artifact_id,),
    ).fetchone()
    if row is None:
        raise ValidationError("registry.db missing dependency source artifact")
    return str(row[0]), int(row[1]), str(row[2])


def _insert_run_manifest_bindings(
    conn: sqlite3.Connection,
    *,
    context: str,
    workflow_name: str,
    run_id: int,
    rows: tuple[RunManifestBindingRow, ...],
) -> None:
    conn.executemany(
        """
        INSERT INTO run_manifest_bindings (
            run_id, context, workflow_name, step_name, role, manifest_name,
            manifest_digest, manifest_hash, entity_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                run_id,
                context,
                workflow_name,
                row.step_name,
                row.role,
                row.manifest_name,
                row.manifest_digest,
                row.manifest_hash,
                row.entity_count,
            )
            for row in rows
        ),
    )


def _insert_published_outputs(
    conn: sqlite3.Connection,
    *,
    rows: tuple[PublishedOutputRow, ...],
    artifact_ids: dict[tuple[str, str, str], int],
) -> None:
    conn.executemany(
        """
        INSERT INTO published_outputs (
            context, workflow_name, step_name, output_name, address, path,
            output_digest, output_hash, artifact_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                row.context,
                row.workflow_name,
                row.step_name,
                row.output_name,
                row.address,
                row.path,
                row.output_digest,
                row.output_hash,
                artifact_ids[(row.step_name, row.output_name, row.address)],
            )
            for row in rows
        ),
    )


def _validate_published_output_rows(
    rows: list[tuple[Any, ...]],
    *,
    context: str,
    runtime_root: Path,
    loaded_workflow_project: Any,
) -> None:
    workflow_steps = {
        workflow_name: set(workflow.steps)
        for workflow_name, workflow in loaded_workflow_project.workflows.items()
    }
    for row in rows:
        (
            workflow_name,
            step_name,
            output_name,
            address,
            output_artifact_path,
            output_digest,
            output_hash,
        ) = row
        if not all(
            isinstance(value, str) and value
            for value in (workflow_name, step_name, output_name, address)
        ):
            raise ValidationError("registry.db published output row has invalid identity")
        try:
            address = validate_path_token(address, label="published output address")
        except ValidationError as exc:
            raise ValidationError("registry.db published output address is invalid") from exc
        if workflow_name not in loaded_workflow_project.workflows:
            raise ValidationError("registry.db published output references unknown workflow")
        if step_name not in workflow_steps[workflow_name]:
            raise ValidationError("registry.db published output references unknown workflow step")
        step = loaded_workflow_project.steps.get(step_name)
        if step is None or output_name not in step.outputs:
            raise ValidationError("registry.db published output references unknown step output")
        if not is_valid_digest(output_digest):
            raise ValidationError("registry.db published output digest is invalid")
        try:
            output_hash = validate_hash_alias(output_hash)
        except ValidationError as exc:
            raise ValidationError("registry.db published output hash is invalid") from exc
        if output_hash != short_hash(output_digest):
            raise ValidationError("registry.db published output hash does not match digest")
        expected_directory = f"outputs/{context}/{workflow_name}/{step_name}/{output_name}"
        output_path = _resolve_published_output_path(
            runtime_root,
            output_artifact_path,
            expected_directory=expected_directory,
        )
        if sha256_file_digest(output_path) != output_digest:
            raise ValidationError("published output artifact digest mismatch")
        declared_extension = step.outputs[output_name].extension
        parsed_address, filename_hash = parse_output_filename(
            output_path.name,
            declared_extension=declared_extension,
        )
        if parsed_address != address:
            raise ValidationError("published output artifact filename address mismatch")
        if filename_hash != output_hash:
            raise ValidationError("published output artifact filename hash mismatch")


def _resolve_published_output_path(
    runtime_root: Path,
    raw_path: Any,
    *,
    expected_directory: str,
) -> Path:
    if not isinstance(raw_path, str):
        raise ValidationError("published output artifact path must be a string")
    relative_path = Path(raw_path).expanduser()
    if relative_path.is_absolute():
        raise ValidationError("published output artifact path must be relative to runtime dir")
    if not raw_path.startswith("outputs/"):
        raise ValidationError("published output artifact path must be under outputs/")
    if not raw_path.startswith(f"{expected_directory}/"):
        raise ValidationError("published output artifact path does not match registry identity")
    resolved = (runtime_root / relative_path).resolve()
    if not _path_contains_or_same(runtime_root, resolved):
        raise ValidationError("published output artifact path must stay inside runtime dir")
    outputs_root = (runtime_root / "outputs").resolve()
    if not _path_contains_or_same(outputs_root, resolved):
        raise ValidationError("published output artifact path must stay inside outputs/")
    if not resolved.is_file():
        raise ValidationError(f"missing published output artifact: {raw_path}")
    return resolved


def _create_schema(conn: sqlite3.Connection) -> None:
    version = _schema_version(conn)
    if version not in (0, REGISTRY_SCHEMA_VERSION):
        raise ValidationError(
            "registry.db schema version is incompatible: "
            f"expected {REGISTRY_SCHEMA_VERSION}, found {version}"
        )
    if version == 0 and _has_user_tables(conn):
        raise ValidationError(
            "registry.db schema version is incompatible: "
            f"expected empty or {REGISTRY_SCHEMA_VERSION}, found 0"
        )
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS contexts (
            context TEXT PRIMARY KEY,
            runtime_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_artifacts (
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            path TEXT NOT NULL,
            entity_count INTEGER NOT NULL CHECK(entity_count >= 0),
            source_digest TEXT NOT NULL,
            source_hash TEXT NOT NULL,
            PRIMARY KEY (context, path)
        );

        CREATE TABLE IF NOT EXISTS manifests (
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            entity_count INTEGER NOT NULL CHECK(entity_count >= 0),
            first_entity_id TEXT NOT NULL,
            last_entity_id TEXT NOT NULL,
            manifest_digest TEXT NOT NULL,
            manifest_hash TEXT NOT NULL,
            source_artifact_path TEXT,
            manifest_body TEXT NOT NULL,
            PRIMARY KEY (context, name),
            FOREIGN KEY (context, source_artifact_path)
                REFERENCES source_artifacts(context, path)
        );

        CREATE TABLE IF NOT EXISTS workflow_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT NOT NULL,
            selected_step_name TEXT NOT NULL,
            selected_output_name TEXT NOT NULL,
            run_workspace TEXT NOT NULL,
            run_plan_path TEXT NOT NULL,
            run_plan_digest TEXT NOT NULL,
            base_workflow_name TEXT,
            is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0, 1)),
            created_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS workflow_runs_current_scope_uq
            ON workflow_runs (
                context, workflow_name, selected_step_name, selected_output_name
            )
            WHERE is_current = 1;

        CREATE TABLE IF NOT EXISTS parameters (
            parameter_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_version INTEGER NOT NULL,
            parameter_hash TEXT NOT NULL,
            parameter_digest TEXT NOT NULL,
            step_name TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (hash_version, step_name, parameter_hash)
        );

        CREATE TABLE IF NOT EXISTS artifacts (
            artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT NOT NULL CHECK(origin IN ('source', 'workflow_output')),
            run_id INTEGER REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT,
            step_name TEXT,
            output_name TEXT,
            address TEXT,
            job_id TEXT,
            artifact_set_id TEXT,
            parameter_id INTEGER REFERENCES parameters(parameter_id),
            path TEXT NOT NULL,
            is_selected_output INTEGER NOT NULL DEFAULT 0
                CHECK(is_selected_output IN (0, 1)),
            is_published INTEGER NOT NULL DEFAULT 0 CHECK(is_published IN (0, 1)),
            published_path TEXT,
            staging_path TEXT,
            content_digest TEXT NOT NULL,
            output_hash TEXT,
            file_size INTEGER NOT NULL CHECK(file_size >= 0),
            extension TEXT NOT NULL,
            subject_id TEXT,
            session_id TEXT,
            task_name TEXT,
            run_label TEXT,
            datatype TEXT,
            suffix TEXT,
            source_metadata_json TEXT,
            callable_ref TEXT,
            software_ref TEXT,
            created_at TEXT NOT NULL,
            CHECK (
                origin != 'source'
                OR (
                    run_id IS NULL
                    AND workflow_name IS NULL
                    AND step_name IS NULL
                    AND output_name IS NULL
                    AND address IS NULL
                    AND parameter_id IS NULL
                    AND is_selected_output = 0
                    AND is_published = 0
                    AND published_path IS NULL
                    AND staging_path IS NULL
                )
            )
        );

        CREATE UNIQUE INDEX IF NOT EXISTS artifacts_source_path_uq
            ON artifacts(context, path)
            WHERE origin = 'source';
        CREATE UNIQUE INDEX IF NOT EXISTS artifacts_workflow_output_uq
            ON artifacts(run_id, step_name, output_name, address)
            WHERE origin = 'workflow_output';
        CREATE INDEX IF NOT EXISTS artifacts_path_idx
            ON artifacts(context, path);
        CREATE INDEX IF NOT EXISTS artifacts_selected_lookup_idx
            ON artifacts(context, workflow_name, step_name, output_name, address)
            WHERE is_selected_output = 1;

        CREATE TABLE IF NOT EXISTS artifact_dependencies (
            dependent_artifact_id INTEGER NOT NULL
                REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
            source_artifact_id INTEGER NOT NULL
                REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
            source_content_digest TEXT NOT NULL,
            source_file_size INTEGER NOT NULL CHECK(source_file_size >= 0),
            source_extension TEXT NOT NULL,
            input_path TEXT NOT NULL,
            binding_name TEXT NOT NULL,
            dependency_role TEXT NOT NULL,
            source_step_name TEXT,
            source_output_name TEXT,
            source_address TEXT,
            dependency_set_id TEXT,
            manifest_digest TEXT,
            edge_cardinality INTEGER CHECK(
                edge_cardinality IS NULL OR edge_cardinality >= 0
            ),
            PRIMARY KEY (
                dependent_artifact_id, source_artifact_id, input_path, binding_name
            )
        );

        CREATE TABLE IF NOT EXISTS run_manifest_bindings (
            run_id INTEGER NOT NULL
                REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT NOT NULL,
            step_name TEXT NOT NULL,
            role TEXT NOT NULL,
            manifest_name TEXT NOT NULL,
            manifest_digest TEXT NOT NULL,
            manifest_hash TEXT NOT NULL,
            entity_count INTEGER NOT NULL CHECK(entity_count >= 0),
            PRIMARY KEY (run_id, step_name, role, manifest_name)
        );

        CREATE TABLE IF NOT EXISTS published_outputs (
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            workflow_name TEXT NOT NULL,
            step_name TEXT NOT NULL,
            output_name TEXT NOT NULL,
            address TEXT NOT NULL,
            path TEXT NOT NULL,
            output_digest TEXT NOT NULL,
            output_hash TEXT NOT NULL,
            artifact_id INTEGER UNIQUE
                REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
            PRIMARY KEY (context, workflow_name, step_name, output_name, address)
        );

        PRAGMA user_version = {REGISTRY_SCHEMA_VERSION};
        """
    )


def _upsert_source_artifact(
    conn: sqlite3.Connection,
    *,
    context: str,
    runtime_root: Path,
    source_artifact_path: str,
    source_entity_count: int,
    source_digest: str,
    source_hash: str,
) -> None:
    source_path = _source_file_path(runtime_root, source_artifact_path)
    content_digest = sha256_file_digest(source_path)
    metadata = _compact_json(
        {
            "entity_count": source_entity_count,
            "source_digest": source_digest,
            "source_hash": source_hash,
        }
    )
    conn.execute(
        """
        INSERT INTO artifacts (
            origin, context, path, content_digest, output_hash, file_size,
            extension, source_metadata_json, created_at
        )
        VALUES ('source', ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(context, path) WHERE origin = 'source'
        DO UPDATE SET
            content_digest = excluded.content_digest,
            output_hash = excluded.output_hash,
            file_size = excluded.file_size,
            extension = excluded.extension,
            source_metadata_json = excluded.source_metadata_json,
            created_at = excluded.created_at
        """,
        (
            context,
            source_artifact_path,
            content_digest,
            short_hash(content_digest),
            source_path.stat().st_size,
            _path_extension(source_artifact_path),
            metadata,
            _utc_now(),
        ),
    )


def _validate_source_artifact_row(
    conn: sqlite3.Connection,
    *,
    context: str,
    runtime_root: Path,
    source_artifact_path: str,
    source_entity_count: int,
    source_digest: str,
    source_hash: str,
) -> None:
    source_path = _source_file_path(runtime_root, source_artifact_path)
    content_digest = sha256_file_digest(source_path)
    expected_metadata = _compact_json(
        {
            "entity_count": source_entity_count,
            "source_digest": source_digest,
            "source_hash": source_hash,
        }
    )
    row = conn.execute(
        """
        SELECT origin, path, content_digest, output_hash, file_size, extension,
               source_metadata_json
        FROM artifacts
        WHERE context = ? AND origin = 'source' AND path = ?
        """,
        (context, source_artifact_path),
    ).fetchone()
    expected_row = (
        "source",
        source_artifact_path,
        content_digest,
        short_hash(content_digest),
        source_path.stat().st_size,
        _path_extension(source_artifact_path),
        expected_metadata,
    )
    if row != expected_row:
        raise ValidationError("registry.db source artifact row is out of date")


def _source_file_path(runtime_root: Path, source_artifact_path: str) -> Path:
    relative_path = Path(source_artifact_path).expanduser()
    if relative_path.is_absolute():
        raise ValidationError("source artifact path must be relative to runtime dir")
    if ".." in relative_path.parts:
        raise ValidationError("source artifact path must stay inside data/")
    if not source_artifact_path.startswith("data/"):
        raise ValidationError("source artifact path must be under data/")
    source_path = (runtime_root / relative_path).resolve()
    data_root = (runtime_root / "data").resolve()
    if not _path_contains_or_same(data_root, source_path):
        raise ValidationError("source artifact path must stay inside data/")
    if not source_path.is_file():
        raise ValidationError(f"missing source artifact: {source_artifact_path}")
    return source_path


def _path_extension(path: str) -> str:
    if path.endswith(".nii.gz"):
        return ".nii.gz"
    return Path(path).suffix


def _compact_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _registry_artifact_from_row(row: sqlite3.Row) -> RegistryArtifact:
    origin = str(row["origin"])
    path = _validate_registry_artifact_path(str(row["path"]), origin=origin)
    is_selected_output = bool(row["is_selected_output"])
    is_published = bool(row["is_published"])
    published_path = _optional_registry_path(
        row["published_path"],
        label="published artifact path",
    )
    staging_path = _optional_registry_path(
        row["staging_path"],
        label="staging artifact path",
    )
    _validate_registry_artifact_identity(
        origin=origin,
        run_id=row["run_id"],
        workflow_name=row["workflow_name"],
        step_name=row["step_name"],
        output_name=row["output_name"],
        address=row["address"],
        job_id=row["job_id"],
        parameter_id=row["parameter_id"],
        path=path,
        is_selected_output=is_selected_output,
        is_published=is_published,
        published_path=published_path,
        staging_path=staging_path,
        output_hash=row["output_hash"],
        callable_ref=row["callable_ref"],
    )
    return RegistryArtifact(
        artifact_id=int(row["artifact_id"]),
        origin=origin,
        run_id=_optional_int(row["run_id"]),
        context=str(row["context"]),
        workflow_name=row["workflow_name"],
        step_name=row["step_name"],
        output_name=row["output_name"],
        address=row["address"],
        job_id=row["job_id"],
        artifact_set_id=row["artifact_set_id"],
        parameter_id=_optional_int(row["parameter_id"]),
        parameter_hash_version=_optional_int(row["parameter_hash_version"]),
        parameter_hash=row["parameter_hash"],
        parameter_digest=row["parameter_digest"],
        parameters_json=row["parameters_json"],
        path=path,
        is_selected_output=is_selected_output,
        is_published=is_published,
        published_path=published_path,
        staging_path=staging_path,
        content_digest=str(row["content_digest"]),
        output_hash=row["output_hash"],
        file_size=int(row["file_size"]),
        extension=str(row["extension"]),
        subject_id=row["subject_id"],
        session_id=row["session_id"],
        task_name=row["task_name"],
        run_label=row["run_label"],
        datatype=row["datatype"],
        suffix=row["suffix"],
        source_metadata=_optional_json_object(
            row["source_metadata_json"],
            label="artifact source metadata",
        ),
        callable_ref=row["callable_ref"],
        software_ref=row["software_ref"],
        created_at=str(row["created_at"]),
    )


def _optional_registry_path(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be stored as a string")
    return _validate_registry_lookup_path(value)


def _validate_registry_artifact_path(path: str, *, origin: str) -> str:
    path = _validate_registry_lookup_path(path)
    if origin == "source":
        if not path.startswith("data/"):
            raise ValidationError("source artifact path must be under data/")
    elif origin == "workflow_output":
        if not path.startswith(("runs/", "outputs/")):
            raise ValidationError(
                "workflow output artifact path must be under runs/ or outputs/"
            )
    else:
        raise ValidationError("artifact origin must be source or workflow_output")
    return path


def _validate_registry_artifact_identity(
    *,
    origin: str,
    run_id: Any,
    workflow_name: Any,
    step_name: Any,
    output_name: Any,
    address: Any,
    job_id: Any,
    parameter_id: Any,
    path: str,
    is_selected_output: bool,
    is_published: bool,
    published_path: str | None,
    staging_path: str | None,
    output_hash: Any,
    callable_ref: Any,
) -> None:
    if origin == "source":
        if any(
            value is not None
            for value in (
                run_id,
                workflow_name,
                step_name,
                output_name,
                address,
                parameter_id,
                published_path,
                staging_path,
            )
        ):
            raise ValidationError("source artifact row has workflow fields")
        if is_selected_output or is_published:
            raise ValidationError("source artifact row has workflow publication flags")
        return

    required_values = {
        "run_id": run_id,
        "workflow_name": workflow_name,
        "step_name": step_name,
        "output_name": output_name,
        "address": address,
        "job_id": job_id,
        "parameter_id": parameter_id,
        "staging_path": staging_path,
        "output_hash": output_hash,
        "callable_ref": callable_ref,
    }
    missing = [name for name, value in required_values.items() if value in (None, "")]
    if missing:
        raise ValidationError(
            "workflow output artifact row is missing "
            + ", ".join(sorted(missing))
        )
    if not str(staging_path).startswith("runs/"):
        raise ValidationError("workflow output staging path must be under runs/")
    if is_published:
        if published_path is None:
            raise ValidationError("published workflow output is missing published path")
        if not published_path.startswith("outputs/"):
            raise ValidationError("published workflow output path must be under outputs/")
        if path != published_path:
            raise ValidationError(
                "published workflow output artifact path must match published path"
            )
    elif published_path is not None:
        raise ValidationError("unpublished workflow output has published path")


def _registry_dependency_from_row(row: sqlite3.Row) -> RegistryDependency:
    return RegistryDependency(
        dependent_artifact_id=int(row["dependent_artifact_id"]),
        source_artifact_id=int(row["source_artifact_id"]),
        source_content_digest=str(row["source_content_digest"]),
        source_file_size=int(row["source_file_size"]),
        source_extension=str(row["source_extension"]),
        input_path=str(row["input_path"]),
        binding_name=str(row["binding_name"]),
        dependency_role=str(row["dependency_role"]),
        source_step_name=row["source_step_name"],
        source_output_name=row["source_output_name"],
        source_address=row["source_address"],
        dependency_set_id=row["dependency_set_id"],
        manifest_digest=row["manifest_digest"],
        edge_cardinality=_optional_int(row["edge_cardinality"]),
    )


def _registry_manifest_binding_from_row(
    row: sqlite3.Row,
) -> RegistryManifestBinding:
    return RegistryManifestBinding(
        run_id=int(row["run_id"]),
        context=str(row["context"]),
        workflow_name=str(row["workflow_name"]),
        step_name=str(row["step_name"]),
        role=str(row["role"]),
        manifest_name=str(row["manifest_name"]),
        manifest_digest=str(row["manifest_digest"]),
        manifest_hash=str(row["manifest_hash"]),
        entity_count=int(row["entity_count"]),
    )


def _registry_manifest_from_row(row: sqlite3.Row) -> RegistryManifest:
    return RegistryManifest(
        context=str(row["context"]),
        name=str(row["name"]),
        path=str(row["path"]),
        entity_count=int(row["entity_count"]),
        first_entity_id=str(row["first_entity_id"]),
        last_entity_id=str(row["last_entity_id"]),
        manifest_digest=str(row["manifest_digest"]),
        manifest_hash=str(row["manifest_hash"]),
        source_artifact_path=row["source_artifact_path"],
        manifest_body=str(row["manifest_body"]),
    )


def _count_rows(
    conn: sqlite3.Connection,
    table: str,
    *,
    context: str,
    origin: str | None = None,
) -> int:
    if table not in {"manifests", "artifacts", "workflow_runs"}:
        raise ValidationError("unsupported registry summary table")
    where_sql = "context = ?"
    values: list[object] = [context]
    if origin is not None:
        where_sql += " AND origin = ?"
        values.append(origin)
    row = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {where_sql}",
        tuple(values),
    ).fetchone()
    return int(row[0])


def _require_artifact_id(conn: sqlite3.Connection, artifact_id: int) -> None:
    row = conn.execute(
        "SELECT 1 FROM artifacts WHERE artifact_id = ?",
        (artifact_id,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"unknown registry artifact id: {artifact_id}")


def _require_run_id(conn: sqlite3.Connection, run_id: int) -> None:
    row = conn.execute(
        "SELECT 1 FROM workflow_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"unknown workflow run id: {run_id}")


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_json_object(value: Any, *, label: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be stored as JSON text")
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"{label} is malformed JSON") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"{label} must be a JSON object")
    return payload


def _validate_positive_id(value: int, *, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationError(f"{label} must be a positive integer")


def _validate_registry_lookup_path(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValidationError("artifact path must be a non-empty string")
    if "\\" in path:
        raise ValidationError("artifact path must use forward slashes")
    relative_path = Path(path).expanduser()
    if relative_path.is_absolute():
        raise ValidationError("artifact path must be relative to runtime dir")
    if ".." in relative_path.parts:
        raise ValidationError("artifact path must stay inside runtime dir")
    if not path.startswith(("data/", "runs/", "outputs/")):
        raise ValidationError(
            "artifact path must be under data/, runs/, or outputs/"
        )
    return path


@contextmanager
def _connect(path: Path) -> Iterator[sqlite3.Connection]:
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        raise ValidationError(f"could not open database {path}: {exc}") from exc
    _set_foreign_keys(conn)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


@contextmanager
def _connect_readonly_rows(path: Path) -> Iterator[sqlite3.Connection]:
    with _connect_readonly(path) as conn:
        conn.row_factory = sqlite3.Row
        yield conn


@contextmanager
def _connect_readonly(path: Path) -> Iterator[sqlite3.Connection]:
    if not path.is_file():
        raise ValidationError(f"missing database: {path}")
    try:
        uri_path = quote(path.resolve().as_posix(), safe="/")
        conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise ValidationError(f"could not open database {path}: {exc}") from exc
    _set_foreign_keys(conn)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def _set_foreign_keys(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")


def _validate_schema_version(conn: sqlite3.Connection) -> None:
    version = _schema_version(conn)
    if version != REGISTRY_SCHEMA_VERSION:
        raise ValidationError(
            "registry.db schema version is incompatible: "
            f"expected {REGISTRY_SCHEMA_VERSION}, found {version}"
        )


def _schema_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _has_user_tables(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def _path_contains_or_same(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True
