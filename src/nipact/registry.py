"""Focused helpers for runtime/database/registry.db."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable, Iterable, Iterator, Literal
from urllib.parse import quote

from .artifacts import (
    CANONICAL_OUTPUT_ROOT,
    STORAGE_LAYOUT_VERSION,
    canonical_output_path,
)
from .errors import ValidationError
from .hashing import is_valid_digest, sha256_digest, sha256_file_digest, short_hash
from .identity import validate_hash_alias, validate_path_token
from .manifest import Manifest, ManifestValue
from .projection import (
    RegisteredSourceSnapshot,
    RequestBundleProjectionPlanV3,
    ResolvedRequestBundleProjectionV3,
    RequestedOutputCoordinate,
    ValidatedStoredRequestBundleProjectionV3,
    resolve_request_bundle_projection_plan,
    validate_stored_request_bundle_projection_v3,
)
from .source_authority import (
    LogicalSourceCoordinate,
    ObservedSourceAuthority,
    SourceDeclaration,
    SourceOccurrenceGuard,
    registered_source_authority_from_facts,
)
from .specification_canonical import (
    CanonicalEffectiveDeclaration,
    CanonicalSpecificationMember,
    CanonicalSpecificationSnapshot,
    ExpectedResultDescriptor,
    decode_specification_row,
    decode_specification_snapshot,
)
from .specification_compiler import DecisionCoordinate

REGISTRY_DB_PATH = "database/registry.db"
REGISTRY_SCHEMA_VERSION = 19
REGISTRY_MIGRATION_SOURCE_VERSION = 18
REGISTRY_V18_BACKUP_FILENAME = "registry.v18-before-v19.db"
REGISTRY_V18_SCHEMA_SIGNATURE_SHA256 = (
    "a9f4e68f21602814326580dab4f5efa3d0bf053feb9d5e7eb50436516a9c0069"
)
PARAMETER_HASH_VERSION = 1


@dataclass(frozen=True)
class RegistryMigrationResult:
    context: str
    registry_path: Path
    status: str
    from_schema: int | None = None
    to_schema: int | None = None
    backup_path: Path | None = None


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
    source_scope: str | None = None
    source_name: str | None = None
    source_entity_id: str | None = None
    source_content_digest: str | None = None
    source_file_size: int | None = None
    manifest_value_schema: str | None = None
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
class RetainedJobProjectionRecipe:
    step_name: str
    address: str
    output_names: tuple[str, ...]
    projection_plan: RequestBundleProjectionPlanV3


@dataclass(frozen=True)
class ReusedProjectionSeed:
    requested_output: RequestedOutputCoordinate
    actual_artifact_id: int
    request_bundle_digest: str


@dataclass(frozen=True)
class SelectedOutputResolutionIntent:
    context: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    outcome: str | None
    existing_artifact_id: int | None = None


@dataclass(frozen=True)
class MembershipIntent:
    row: PublishedOutputRow
    existing_artifact_id: int | None = None


SpecificationFailureStage = Literal[
    "planning",
    "execution",
    "acceptance",
]
SpecificationAttemptOutcome = Literal["failed", "partial", "complete"]


@dataclass(frozen=True)
class SpecificationAttemptRef:
    attempt_id: int
    context: str
    snapshot_digest: str
    member_key: str


@dataclass(frozen=True)
class SpecificationFailureDiagnostic:
    stage: SpecificationFailureStage
    summary: str


@dataclass(frozen=True)
class SpecificationMemberProjection:
    member_key: str
    row_digest: str
    disposition: Literal["included", "excluded"]
    exclusion_reason: str | None
    workflow_selector: str
    decision_coordinates: tuple[DecisionCoordinate, ...]
    effective_declaration: CanonicalEffectiveDeclaration
    expected_results: tuple[ExpectedResultDescriptor, ...]


@dataclass(frozen=True)
class SpecificationAttemptProjection:
    attempt_id: int
    member_key: str
    started_at: str
    finished_at: str | None
    outcome: SpecificationAttemptOutcome | None
    selecting_run_id: int | None
    failure: SpecificationFailureDiagnostic | None


@dataclass(frozen=True)
class SpecificationResultProjection:
    attempt_id: int
    member_key: str
    role: str
    step_name: str
    output_name: str
    address: str
    artifact_id: int | None
    producing_run_id: int | None
    producing_workflow_name: str | None
    artifact_path: str | None
    content_digest: str | None
    output_hash: str | None
    file_size: int | None
    extension: str | None
    request_bundle_digest: str | None
    current_publication_path: str | None


@dataclass(frozen=True)
class SpecificationResultSourceBasisProjection:
    attempt_id: int
    member_key: str
    role: str
    address: str
    result_artifact_id: int
    source_scope: Literal["global", "entity"]
    source_name: str
    source_entity_id: str | None
    source_occurrence_path: str
    source_content_digest: str
    source_file_size: int
    source_extension: str


@dataclass(frozen=True)
class SpecificationSnapshotProjections:
    snapshot_digest: str
    context: str
    members: tuple[SpecificationMemberProjection, ...]
    attempts: tuple[SpecificationAttemptProjection, ...]
    results: tuple[SpecificationResultProjection, ...]
    result_source_basis: tuple[SpecificationResultSourceBasisProjection, ...]


@dataclass(frozen=True)
class SpecificationAcceptanceIntent:
    attempt: SpecificationAttemptRef
    member: CanonicalSpecificationMember
    failure: SpecificationFailureDiagnostic | None = None


@dataclass(frozen=True)
class EnvironmentObservationV1:
    nipact_version: str
    python_version: str
    platform: str
    snakemake_version: str
    profile_version: int = 1


@dataclass(frozen=True)
class RunExecutionPopulationRow:
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str


@dataclass(frozen=True)
class RunManifestBindingRow:
    step_name: str
    manifest_usage_role: str
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str


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
    request_bundle_digest: str | None = None
    source_scope: str | None = None
    source_name: str | None = None
    source_entity_id: str | None = None


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
    manifest_value_schema: str | None
    manifest_digest: str | None
    edge_cardinality: int | None
    source_scope: str | None = None
    source_name: str | None = None
    source_entity_id: str | None = None
    source_occurrence_path: str | None = None


@dataclass(frozen=True)
class RegisteredSourceAuthority:
    artifact_id: int
    authority: ObservedSourceAuthority


@dataclass(frozen=True)
class ReusableArtifactBundleRequest:
    context: str
    step_name: str
    address: str
    resolved_projection: ResolvedRequestBundleProjectionV3
    sibling_outputs: tuple[tuple[str, str], ...]
    input_records: tuple[ArtifactInputRow, ...]


@dataclass(frozen=True)
class ReusableArtifactCandidate:
    artifact_id: int
    run_id: int
    path: str
    published_path: str | None
    content_digest: str
    output_hash: str
    file_size: int
    extension: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    request_bundle_digest: str
    dependencies: tuple[RegistryDependency, ...]


@dataclass(frozen=True)
class ReusableArtifactBundleCandidate:
    run_id: int
    outputs: tuple[ReusableArtifactCandidate, ...]

    def output(self, output_name: str) -> ReusableArtifactCandidate:
        for candidate in self.outputs:
            if candidate.output_name == output_name:
                return candidate
        raise ValidationError(
            f"reusable artifact bundle is missing output {output_name!r}"
        )


@dataclass(frozen=True)
class RegistryManifestBinding:
    run_id: int
    context: str
    workflow_name: str
    step_name: str
    manifest_usage_role: str
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str
    manifest_hash: str
    entity_count: int


@dataclass(frozen=True)
class RegistryExecutionPopulation:
    run_id: int
    context: str
    workflow_name: str
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str
    manifest_hash: str
    entity_count: int


@dataclass(frozen=True)
class RegistryManifest:
    context: str
    name: str
    path: str
    manifest_value_schema: str
    entity_count: int
    first_entity_id: str
    last_entity_id: str
    manifest_digest: str
    manifest_hash: str
    canonical_body: str


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
    a.request_bundle_digest,
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
    a.created_at,
    a.source_scope,
    a.source_name,
    a.source_entity_id
"""


def _initialize_storage_layout(runtime_root: Path) -> None:
    runtime_root = runtime_root.resolve()
    outputs_root = runtime_root / "outputs"
    if outputs_root.is_symlink():
        raise ValidationError("runtime outputs path must be a real directory")
    outputs_root.mkdir(parents=True, exist_ok=True)
    if not outputs_root.is_dir():
        raise ValidationError("runtime outputs path must be a directory")
    layout_root = runtime_root / CANONICAL_OUTPUT_ROOT
    if layout_root.is_symlink():
        raise ValidationError("canonical output root must be a real directory")
    layout_root.mkdir(parents=True, exist_ok=True)
    _validate_storage_layout(runtime_root)


def _validate_storage_layout(runtime_root: Path) -> None:
    runtime_root = runtime_root.resolve()
    outputs_root = runtime_root / "outputs"
    layout_root = runtime_root / CANONICAL_OUTPUT_ROOT
    if outputs_root.is_symlink() or not outputs_root.is_dir():
        raise ValidationError("runtime outputs path must be a real directory")
    if not layout_root.is_dir():
        raise ValidationError("canonical output root must be a real directory")
    if layout_root.is_symlink():
        raise ValidationError("canonical output root must be a real directory")
    if not _path_contains_or_same(runtime_root, layout_root.resolve()):
        raise ValidationError("canonical output root must stay inside runtime dir")


def initialize_registry_db(
    path: Path,
    *,
    context: str,
    runtime_root: Path,
    manifests: dict[str, Manifest],
    manifest_paths: dict[str, str],
) -> None:
    """Create the initial registry database for a new runtime root."""
    _initialize_storage_layout(runtime_root)
    with _connect(path) as conn:
        _create_schema(conn)
        conn.execute(
            """
            INSERT INTO contexts (context, runtime_path, storage_layout_version)
            VALUES (?, ?, ?)
            ON CONFLICT(context) DO UPDATE SET
                runtime_path = excluded.runtime_path,
                storage_layout_version = excluded.storage_layout_version
            """,
            (context, str(runtime_root), STORAGE_LAYOUT_VERSION),
        )
        _insert_manifest_declarations(
            conn,
            context=context,
            manifests=manifests,
            manifest_paths=manifest_paths,
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
    _initialize_storage_layout(runtime_root)
    with _connect(path) as conn:
        _create_schema(conn)
        conn.execute(
            """
            INSERT INTO contexts (context, runtime_path, storage_layout_version)
            VALUES (?, ?, ?)
            ON CONFLICT(context) DO UPDATE SET
                runtime_path = excluded.runtime_path,
                storage_layout_version = excluded.storage_layout_version
            """,
            (context, str(runtime_root), STORAGE_LAYOUT_VERSION),
        )
        _insert_manifest_declarations(
            conn,
            context=context,
            manifests=manifests,
            manifest_paths=manifest_paths,
        )


def insert_or_verify_specification_snapshot(
    path: Path,
    *,
    runtime_root: Path,
    snapshot: CanonicalSpecificationSnapshot,
    _fault_hook: Callable[[str], None] | None = None,
) -> bool:
    """Atomically insert one canonical snapshot, or verify an exact replay."""
    if type(snapshot) is not CanonicalSpecificationSnapshot:
        raise ValidationError("snapshot must be a CanonicalSpecificationSnapshot")
    decoded = decode_specification_snapshot(snapshot.canonical_bytes)
    if decoded != snapshot:
        raise ValidationError(
            "specification snapshot does not match its canonical bytes"
        )

    registry_path, resolved_runtime_root = _validate_snapshot_registry_binding(
        path,
        runtime_root=runtime_root,
    )
    with _connect_readwrite_existing(registry_path) as conn:
        _validate_schema_version(conn)
        _validate_exact_registry_structure(
            conn,
            expected_version=REGISTRY_SCHEMA_VERSION,
        )
        _require_snapshot_context_binding(
            conn,
            context=snapshot.context,
            runtime_root=resolved_runtime_root,
        )

        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT 1
                FROM specification_snapshots
                WHERE snapshot_digest = ?
                """,
                (snapshot.snapshot_digest,),
            ).fetchone()
            if existing is not None:
                stored = _read_specification_snapshot_conn(
                    conn,
                    context=snapshot.context,
                    snapshot_digest=snapshot.snapshot_digest,
                )
                if (
                    stored != snapshot
                    or stored.canonical_bytes != snapshot.canonical_bytes
                ):
                    raise ValidationError(
                        "stored specification snapshot does not match exact replay"
                    )
                conn.rollback()
                return False

            for value in snapshot.manifest_values:
                _insert_or_verify_snapshot_manifest_value(conn, value=value)
            _invoke_specification_snapshot_fault(_fault_hook, "after_manifest_values")

            conn.execute(
                """
                INSERT INTO specification_snapshots (
                    snapshot_digest, context, canonical_bytes
                )
                VALUES (?, ?, ?)
                """,
                (
                    snapshot.snapshot_digest,
                    snapshot.context,
                    snapshot.canonical_bytes,
                ),
            )
            _invoke_specification_snapshot_fault(_fault_hook, "after_snapshot")

            conn.executemany(
                """
                INSERT INTO specification_members (
                    snapshot_digest, member_key, row_digest,
                    disposition, exclusion_reason
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    (
                        snapshot.snapshot_digest,
                        member.member_key,
                        member.row.row_digest,
                        member.disposition,
                        member.exclusion_reason,
                    )
                    for member in snapshot.members
                ),
            )
            _invoke_specification_snapshot_fault(_fault_hook, "after_members")

            conn.executemany(
                """
                INSERT INTO specification_snapshot_manifest_values (
                    snapshot_digest, value_schema, manifest_digest
                )
                VALUES (?, ?, ?)
                """,
                (
                    (
                        snapshot.snapshot_digest,
                        value.value_schema,
                        value.manifest_digest,
                    )
                    for value in snapshot.manifest_values
                ),
            )
            _invoke_specification_snapshot_fault(
                _fault_hook,
                "after_manifest_associations",
            )

            conn.executemany(
                """
                INSERT INTO specification_expected_results (
                    snapshot_digest, member_key, role,
                    step_name, output_name, address
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        snapshot.snapshot_digest,
                        member.member_key,
                        result.role,
                        result.step_name,
                        result.output_name,
                        result.address,
                    )
                    for member in snapshot.members
                    for result in member.expected_results
                ),
            )
            _invoke_specification_snapshot_fault(
                _fault_hook,
                "after_expected_results",
            )

            stored = _read_specification_snapshot_conn(
                conn,
                context=snapshot.context,
                snapshot_digest=snapshot.snapshot_digest,
            )
            if stored != snapshot or stored.canonical_bytes != snapshot.canonical_bytes:
                raise ValidationError(
                    "persisted specification snapshot failed exact verification"
                )
            _invoke_specification_snapshot_fault(_fault_hook, "after_verification")
            conn.commit()
            return True
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise


def read_specification_snapshot(
    path: Path,
    *,
    context: str,
    snapshot_digest: str,
) -> CanonicalSpecificationSnapshot:
    """Read one immutable snapshot after verifying all normalized projections."""
    context = validate_path_token(context, label="context")
    if not is_valid_digest(snapshot_digest):
        raise ValidationError(
            "snapshot_digest must be a lowercase 64-character hexadecimal string"
        )
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            _validate_exact_registry_structure(
                conn,
                expected_version=REGISTRY_SCHEMA_VERSION,
            )
            return _read_specification_snapshot_conn(
                conn,
                context=context,
                snapshot_digest=snapshot_digest,
            )
    except sqlite3.Error as exc:
        raise ValidationError(
            f"could not read specification snapshot: {exc}"
        ) from exc


def read_specification_snapshot_projections(
    path: Path,
    *,
    context: str,
    snapshot_digest: str,
) -> SpecificationSnapshotProjections:
    """Read normalized scientific projections for one exact frozen snapshot."""
    context = validate_path_token(context, label="context")
    snapshot_digest = _validate_specification_snapshot_digest(snapshot_digest)
    try:
        with _connect_readonly_rows(path) as conn:
            conn.execute("BEGIN")
            try:
                _validate_schema_version(conn)
                _validate_exact_registry_structure(
                    conn,
                    expected_version=REGISTRY_SCHEMA_VERSION,
                )
                snapshot = _read_specification_snapshot_conn(
                    conn,
                    context=context,
                    snapshot_digest=snapshot_digest,
                )
                projections = _read_specification_snapshot_projections_conn(
                    conn,
                    snapshot=snapshot,
                )
            finally:
                if conn.in_transaction:
                    conn.rollback()
            return projections
    except sqlite3.Error as exc:
        raise ValidationError(
            f"could not read specification snapshot projections: {exc}"
        ) from exc


def append_specification_member_attempt(
    path: Path,
    *,
    runtime_root: Path,
    context: str,
    snapshot_digest: str,
    member: CanonicalSpecificationMember,
    _fault_hook: Callable[[str], None] | None = None,
) -> SpecificationAttemptRef:
    """Append one unresolved attempt for an included specification member."""
    context = validate_path_token(context, label="context")
    snapshot_digest = _validate_specification_snapshot_digest(snapshot_digest)
    registry_path, resolved_runtime_root = _validate_snapshot_registry_binding(
        path,
        runtime_root=runtime_root,
    )
    with _connect_readwrite_existing(registry_path) as conn:
        _validate_schema_version(conn)
        _validate_exact_registry_structure(
            conn,
            expected_version=REGISTRY_SCHEMA_VERSION,
        )
        try:
            conn.execute("BEGIN IMMEDIATE")
            _require_snapshot_context_binding(
                conn,
                context=context,
                runtime_root=resolved_runtime_root,
            )
            _validate_targeted_specification_member(
                conn,
                context=context,
                snapshot_digest=snapshot_digest,
                member=member,
            )
            if member.disposition != "included":
                raise ValidationError(
                    "excluded specification member cannot have an attempt"
                )
            cursor = conn.execute(
                """
                INSERT INTO specification_member_attempts (
                    snapshot_digest, member_key, started_at
                )
                VALUES (?, ?, ?)
                """,
                (snapshot_digest, member.member_key, _utc_now()),
            )
            attempt = SpecificationAttemptRef(
                attempt_id=int(cursor.lastrowid),
                context=context,
                snapshot_digest=snapshot_digest,
                member_key=member.member_key,
            )
            _invoke_specification_fault(_fault_hook, "after_attempt_insert")
            conn.commit()
            return attempt
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise


def fail_specification_member_attempt(
    path: Path,
    *,
    runtime_root: Path,
    attempt: SpecificationAttemptRef,
    member: CanonicalSpecificationMember,
    diagnostic: SpecificationFailureDiagnostic,
    _fault_hook: Callable[[str], None] | None = None,
) -> bool:
    """Conditionally fill one unresolved attempt with an early failure."""
    _validate_specification_attempt_ref(attempt)
    stage, summary = _validate_specification_failure_diagnostic(diagnostic)
    registry_path, resolved_runtime_root = _validate_snapshot_registry_binding(
        path,
        runtime_root=runtime_root,
    )
    with _connect_readwrite_existing(registry_path) as conn:
        _validate_schema_version(conn)
        _validate_exact_registry_structure(
            conn,
            expected_version=REGISTRY_SCHEMA_VERSION,
        )
        try:
            conn.execute("BEGIN IMMEDIATE")
            _require_snapshot_context_binding(
                conn,
                context=attempt.context,
                runtime_root=resolved_runtime_root,
            )
            _validate_targeted_specification_member(
                conn,
                context=attempt.context,
                snapshot_digest=attempt.snapshot_digest,
                member=member,
            )
            if member.disposition != "included":
                raise ValidationError(
                    "excluded specification member cannot have an attempt"
                )
            row = _read_specification_attempt(
                conn,
                attempt=attempt,
                member=member,
            )
            if row["outcome"] is not None:
                conn.rollback()
                return False
            _require_unresolved_specification_attempt(conn, row=row)
            cursor = conn.execute(
                """
                UPDATE specification_member_attempts
                SET finished_at = ?, outcome = 'failed',
                    failure_stage = ?, failure_summary = ?
                WHERE attempt_id = ?
                  AND snapshot_digest = ?
                  AND member_key = ?
                  AND outcome IS NULL
                  AND finished_at IS NULL
                  AND selecting_run_id IS NULL
                  AND failure_stage IS NULL
                  AND failure_summary IS NULL
                """,
                (
                    _utc_now(),
                    stage,
                    summary,
                    attempt.attempt_id,
                    attempt.snapshot_digest,
                    attempt.member_key,
                ),
            )
            if cursor.rowcount != 1:
                raise ValidationError(
                    "specification attempt could not be marked failed"
                )
            _invoke_specification_fault(
                _fault_hook,
                "after_attempt_failure_update",
            )
            conn.commit()
            return True
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise


def read_specification_attempt_outcome(
    path: Path,
    *,
    runtime_root: Path,
    attempt: SpecificationAttemptRef,
    member: CanonicalSpecificationMember,
) -> SpecificationAttemptOutcome | None:
    """Read one attempt outcome after verifying its terminal cardinality."""
    _validate_specification_attempt_ref(attempt)
    registry_path, resolved_runtime_root = _validate_snapshot_registry_binding(
        path,
        runtime_root=runtime_root,
    )
    try:
        with _connect_readonly_rows(registry_path) as conn:
            _validate_schema_version(conn)
            _validate_exact_registry_structure(
                conn,
                expected_version=REGISTRY_SCHEMA_VERSION,
            )
            _require_snapshot_context_binding(
                conn,
                context=attempt.context,
                runtime_root=resolved_runtime_root,
            )
            _validate_targeted_specification_member(
                conn,
                context=attempt.context,
                snapshot_digest=attempt.snapshot_digest,
                member=member,
            )
            if member.disposition != "included":
                raise ValidationError(
                    "excluded specification member cannot have an attempt"
                )
            row = _read_specification_attempt(
                conn,
                attempt=attempt,
                member=member,
            )
            result_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM specification_attempt_results
                    WHERE attempt_id = ?
                    """,
                    (attempt.attempt_id,),
                ).fetchone()[0]
            )
    except sqlite3.Error as exc:
        raise ValidationError(
            f"could not read specification attempt outcome: {exc}"
        ) from exc

    outcome = row["outcome"]
    expected_count = len(member.expected_results)
    if outcome is None:
        valid_count = result_count == 0
    elif outcome == "failed":
        valid_count = result_count == 0
    elif outcome == "partial":
        valid_count = 0 < result_count < expected_count
    else:
        valid_count = result_count == expected_count
    if not valid_count:
        raise ValidationError(
            "stored specification attempt result count is inconsistent"
        )
    return outcome


def _insert_manifest_declarations(
    conn: sqlite3.Connection,
    *,
    context: str,
    manifests: dict[str, Manifest],
    manifest_paths: dict[str, str],
) -> None:
    for name, manifest in manifests.items():
        conn.execute(
            """
            INSERT OR IGNORE INTO manifest_values (
                value_schema, manifest_digest, canonical_body, entity_count
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                manifest.manifest_value_schema,
                manifest.manifest_digest,
                manifest.canonical_body,
                manifest.entity_count,
            ),
        )
        stored_value = conn.execute(
            """
            SELECT canonical_body, entity_count
            FROM manifest_values
            WHERE value_schema = ? AND manifest_digest = ?
            """,
            (manifest.manifest_value_schema, manifest.manifest_digest),
        ).fetchone()
        if stored_value is None or tuple(stored_value) != (
            manifest.canonical_body,
            manifest.entity_count,
        ):
            raise ValidationError(
                "registry manifest value does not match its schema-qualified key"
            )
        conn.execute(
            """
            INSERT INTO manifest_declarations (
                context, manifest_name, declared_path,
                last_validated_manifest_value_schema,
                last_validated_manifest_digest
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(context, manifest_name) DO UPDATE SET
                declared_path = excluded.declared_path,
                last_validated_manifest_value_schema =
                    excluded.last_validated_manifest_value_schema,
                last_validated_manifest_digest =
                    excluded.last_validated_manifest_digest
            """,
            (
                context,
                name,
                manifest_paths[name],
                manifest.manifest_value_schema,
                manifest.manifest_digest,
            ),
        )


def validate_registry_db(
    path: Path,
    *,
    project_root: Path,
    runtime_root: Path,
    manifest_paths: dict[str, Path],
    context: str,
    manifests: dict[str, Manifest],
    loaded_workflow_project: Any,
) -> dict[str, int]:
    """Validate registry rows against current project declarations and artifacts."""
    _validate_storage_layout(runtime_root)
    try:
        with _connect_readonly(path) as conn:
            _validate_schema_version(conn)
            context_row = conn.execute(
                """
                SELECT runtime_path, storage_layout_version
                FROM contexts
                WHERE context = ?
                """,
                (context,),
            ).fetchone()
            if context_row is None:
                raise ValidationError("registry.db missing context row")
            if context_row != (str(runtime_root), STORAGE_LAYOUT_VERSION):
                raise ValidationError("registry.db context row is out of date")
            rows = conn.execute(
                """
                SELECT d.manifest_name, d.declared_path, v.value_schema,
                       v.manifest_digest, v.canonical_body, v.entity_count
                FROM manifest_declarations d
                JOIN manifest_values v
                  ON v.value_schema = d.last_validated_manifest_value_schema
                 AND v.manifest_digest = d.last_validated_manifest_digest
                WHERE d.context = ?
                ORDER BY d.manifest_name
                """,
                (context,),
            ).fetchall()
            published_rows = conn.execute(
                """
                SELECT po.workflow_name, po.step_name, po.output_name, po.address,
                       po.path, po.output_digest, po.output_hash, po.artifact_id,
                       a.context, a.workflow_name, a.step_name, a.output_name,
                       a.address, a.origin, a.is_published, a.published_path,
                       a.content_digest, a.output_hash, a.request_bundle_digest,
                       a.extension
                FROM published_outputs po
                JOIN artifacts a ON a.artifact_id = po.artifact_id
                WHERE po.context = ?
                ORDER BY po.workflow_name, po.step_name, po.output_name, po.address
                """,
                (context,),
            ).fetchall()
            accepted_artifact_rows = conn.execute(
                """
                SELECT artifact_id, context, workflow_name, step_name, output_name,
                       address, origin, is_published, path, published_path,
                       content_digest, output_hash, file_size, extension,
                       request_bundle_digest
                FROM artifacts
                WHERE context = ?
                  AND origin = 'workflow_output'
                  AND is_published = 1
                ORDER BY artifact_id
                """,
                (context,),
            ).fetchall()
            _validate_request_bundle_projection_graph(conn, context=context)
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc

    expected_rows = [
        (
            name,
            manifest_paths[name].relative_to(project_root).as_posix(),
            manifest.manifest_value_schema,
            manifest.manifest_digest,
            manifest.canonical_body,
            manifest.entity_count,
        )
        for name, manifest in sorted(manifests.items())
    ]
    if rows != expected_rows:
        raise ValidationError("registry.db manifest rows are out of date")
    verified_occurrences: set[tuple[Path, str]] = set()
    _validate_published_output_rows(
        published_rows,
        context=context,
        runtime_root=runtime_root,
        loaded_workflow_project=loaded_workflow_project,
    )
    _validate_accepted_workflow_output_rows(
        accepted_artifact_rows,
        context=context,
        runtime_root=runtime_root,
        loaded_workflow_project=loaded_workflow_project,
        verified_occurrences=verified_occurrences,
    )
    return {"manifests": len(rows), "published_outputs": len(published_rows)}


def validate_prepared_registry_db(
    path: Path,
    *,
    context: str,
    runtime_root: Path,
    loaded_workflow_project: Any,
) -> dict[str, int]:
    """Validate the small registry surface needed by generic prepared projects."""
    _validate_storage_layout(runtime_root)
    try:
        with _connect_readonly(path) as conn:
            _validate_schema_version(conn)
            context_row = conn.execute(
                """
                SELECT runtime_path, storage_layout_version
                FROM contexts
                WHERE context = ?
                """,
                (context,),
            ).fetchone()
            if context_row is None:
                raise ValidationError("registry.db missing context row")
            if context_row != (str(runtime_root), STORAGE_LAYOUT_VERSION):
                raise ValidationError("registry.db context row is out of date")
            published_outputs = conn.execute(
                """
                SELECT COUNT(*)
                FROM published_outputs
                WHERE context = ?
                """,
                (context,),
            ).fetchone()[0]
            published_rows = conn.execute(
                """
                SELECT po.workflow_name, po.step_name, po.output_name, po.address,
                       po.path, po.output_digest, po.output_hash, po.artifact_id,
                       a.context, a.workflow_name, a.step_name, a.output_name,
                       a.address, a.origin, a.is_published, a.published_path,
                       a.content_digest, a.output_hash, a.request_bundle_digest,
                       a.extension
                FROM published_outputs po
                JOIN artifacts a ON a.artifact_id = po.artifact_id
                WHERE po.context = ?
                ORDER BY po.workflow_name, po.step_name, po.output_name, po.address
                """,
                (context,),
            ).fetchall()
            accepted_artifact_rows = conn.execute(
                """
                SELECT artifact_id, context, workflow_name, step_name, output_name,
                       address, origin, is_published, path, published_path,
                       content_digest, output_hash, file_size, extension,
                       request_bundle_digest
                FROM artifacts
                WHERE context = ?
                  AND origin = 'workflow_output'
                  AND is_published = 1
                ORDER BY artifact_id
                """,
                (context,),
            ).fetchall()
            _validate_request_bundle_projection_graph(conn, context=context)
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    verified_occurrences: set[tuple[Path, str]] = set()
    _validate_published_output_rows(
        published_rows,
        context=context,
        runtime_root=runtime_root,
        loaded_workflow_project=loaded_workflow_project,
    )
    _validate_accepted_workflow_output_rows(
        accepted_artifact_rows,
        context=context,
        runtime_root=runtime_root,
        loaded_workflow_project=loaded_workflow_project,
        verified_occurrences=verified_occurrences,
    )
    return {"published_outputs": int(published_outputs)}


def _validate_request_bundle_projection_graph(
    conn: sqlite3.Connection,
    *,
    context: str,
) -> None:
    rows = conn.execute(
        """
        SELECT request_bundle_digest, projection_json
        FROM request_bundle_projections
        ORDER BY request_bundle_digest
        """
    ).fetchall()
    graph: dict[str, tuple[str, ...]] = {}
    for row in rows:
        digest = str(row[0])
        validated = validate_stored_request_bundle_projection_v3(
            request_bundle_digest=digest,
            projection_json=str(row[1]),
        )
        graph[digest] = validated.direct_upstream_request_bundle_digests
    for digest, upstream_digests in graph.items():
        for upstream_digest in upstream_digests:
            if upstream_digest not in graph:
                raise ValidationError(
                    "registry request projection references a missing upstream "
                    f"projection: {digest} -> {upstream_digest}"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(digest: str) -> None:
        if digest in visited:
            return
        if digest in visiting:
            raise ValidationError("registry request projection graph contains a cycle")
        visiting.add(digest)
        for upstream_digest in graph[digest]:
            visit(upstream_digest)
        visiting.remove(digest)
        visited.add(digest)

    for digest in graph:
        visit(digest)

    artifact_rows = conn.execute(
        """
        SELECT origin, request_bundle_digest
        FROM artifacts
        WHERE context = ?
        ORDER BY artifact_id
        """,
        (context,),
    ).fetchall()
    for origin, digest in artifact_rows:
        if origin == "source":
            if digest is not None:
                raise ValidationError(
                    "registry source artifact references a request projection"
                )
            continue
        if origin != "workflow_output" or not isinstance(digest, str):
            raise ValidationError(
                "registry workflow artifact is missing its request projection"
            )
        if digest not in graph:
            raise ValidationError(
                "registry workflow artifact references a missing request projection"
            )


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
    projection_recipes: Iterable[RetainedJobProjectionRecipe],
    reused_projection_seeds: Iterable[ReusedProjectionSeed],
    selected_resolution_intents: Iterable[SelectedOutputResolutionIntent],
    environment_observation: EnvironmentObservationV1,
    execution_population: RunExecutionPopulationRow | None = None,
    manifest_bindings: Iterable[RunManifestBindingRow],
    membership_intents: Iterable[MembershipIntent],
    base_workflow_name: str | None = None,
    specification_acceptance: SpecificationAcceptanceIntent | None = None,
    _fault_hook: Callable[[str], None] | None = None,
) -> int:
    """Record one run, superseding current rows while retaining history."""
    artifact_rows = tuple(artifacts)
    projection_recipe_rows = tuple(projection_recipes)
    reused_projection_seed_rows = tuple(reused_projection_seeds)
    selected_resolution_rows = tuple(selected_resolution_intents)
    manifest_binding_rows = tuple(manifest_bindings)
    membership_rows = tuple(membership_intents)
    if any(
        row.context != context or row.workflow_name != workflow_name
        for row in selected_resolution_rows
    ):
        raise ValidationError("selected-output resolution belongs to another run scope")
    if any(
        row.step_name != selected_step_name
        or row.output_name != selected_output_name
        for row in selected_resolution_rows
    ):
        raise ValidationError(
            "selected-output resolution does not match selected output"
        )
    if any(
        intent.row.context != context or intent.row.workflow_name != workflow_name
        for intent in membership_rows
    ):
        raise ValidationError("membership intent belongs to another run scope")
    membership_coordinates = [
        (
            intent.row.context,
            intent.row.workflow_name,
            intent.row.step_name,
            intent.row.output_name,
            intent.row.address,
        )
        for intent in membership_rows
    ]
    if len(membership_coordinates) != len(set(membership_coordinates)):
        raise ValidationError("membership intent coordinate is duplicated")
    _validate_selected_resolution_memberships(
        selected_resolution_rows,
        membership_rows,
    )
    now = _utc_now()
    specification_failure: tuple[str, str] | None = None
    if specification_acceptance is None:
        connection = _connect(path)
        recording_runtime_root = runtime_root
    else:
        registry_path, recording_runtime_root = _validate_snapshot_registry_binding(
            path,
            runtime_root=runtime_root,
        )
        connection = _connect_readwrite_existing(registry_path)
    try:
        with connection as conn:
            try:
                _validate_schema_version(conn)
                if specification_acceptance is not None:
                    _validate_exact_registry_structure(
                        conn,
                        expected_version=REGISTRY_SCHEMA_VERSION,
                    )
                    conn.execute("BEGIN IMMEDIATE")
                    _require_snapshot_context_binding(
                        conn,
                        context=context,
                        runtime_root=recording_runtime_root,
                    )
                    specification_failure = (
                        _validate_specification_acceptance_state(
                            conn,
                            context=context,
                            workflow_name=workflow_name,
                            selected_step_name=selected_step_name,
                            selected_output_name=selected_output_name,
                            intent=specification_acceptance,
                        )
                    )
                # A specification member is one point in a frozen member space,
                # not a new preferred realization of the declared workflow. It
                # records lineage without disturbing ordinary membership.
                if specification_acceptance is None:
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
                    resolution_summary_json=_resolution_summary_json(
                        selected_resolution_rows,
                        artifact_ids=None,
                    ),
                    environment_observation_json=_environment_observation_json(
                        environment_observation
                    ),
                    created_at=now,
                    is_current=specification_acceptance is None,
                )
                if specification_acceptance is not None:
                    _invoke_specification_fault(_fault_hook, "after_workflow_run")
                finalized_projections = _finalize_retained_job_projections(
                    conn,
                    context=context,
                    artifact_rows=artifact_rows,
                    projection_recipes=projection_recipe_rows,
                    reused_projection_seeds=reused_projection_seed_rows,
                )
                reused_projection_identities = _reused_projection_identities(
                    context=context,
                    seeds=reused_projection_seed_rows,
                )
                _validate_no_divergent_fresh_bundles(
                    conn,
                    context=context,
                    run_id=run_id,
                    artifact_rows=artifact_rows,
                    finalized_projections=finalized_projections,
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
                    finalized_projections=finalized_projections,
                    created_at=now,
                )
                _insert_artifact_dependencies(
                    conn,
                    runtime_root=recording_runtime_root,
                    context=context,
                    reused_projection_identities=reused_projection_identities,
                    artifact_rows=artifact_rows,
                    artifact_ids=artifact_ids,
                )
                _insert_run_manifest_bindings(
                    conn,
                    run_id=run_id,
                    rows=manifest_binding_rows,
                )
                if execution_population is not None:
                    _insert_run_execution_population(
                        conn,
                        run_id=run_id,
                        row=execution_population,
                    )
                if specification_acceptance is None:
                    _delete_published_output_coordinates(
                        conn,
                        rows=tuple(intent.row for intent in membership_rows),
                    )
                    accepted_memberships = _insert_memberships(
                        conn,
                        intents=membership_rows,
                        artifact_ids=artifact_ids,
                    )
                else:
                    accepted_memberships = _resolved_membership_artifacts(
                        conn,
                        intents=membership_rows,
                        artifact_ids=artifact_ids,
                    )
                conn.execute(
                    "UPDATE workflow_runs SET resolution_summary_json = ? WHERE run_id = ?",
                    (
                        _resolution_summary_json(
                            selected_resolution_rows,
                            artifact_ids=artifact_ids,
                            conn=conn,
                        ),
                        run_id,
                    ),
                )
                if specification_acceptance is not None:
                    _invoke_specification_fault(
                        _fault_hook,
                        "after_ordinary_acceptance",
                    )
                    resolved_count = _insert_specification_attempt_results(
                        conn,
                        context=context,
                        workflow_name=workflow_name,
                        intent=specification_acceptance,
                        accepted_memberships=accepted_memberships,
                    )
                    _invoke_specification_fault(
                        _fault_hook,
                        "after_specification_results",
                    )
                    _terminalize_specification_attempt(
                        conn,
                        intent=specification_acceptance,
                        run_id=run_id,
                        finished_at=now,
                        resolved_count=resolved_count,
                        failure=specification_failure,
                    )
                    _invoke_specification_fault(
                        _fault_hook,
                        "after_specification_terminalization",
                    )
                    conn.commit()
            except Exception:
                if specification_acceptance is not None and conn.in_transaction:
                    conn.rollback()
                raise
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return len(artifact_rows)


def _validate_selected_resolution_memberships(
    resolutions: tuple[SelectedOutputResolutionIntent, ...],
    memberships: tuple[MembershipIntent, ...],
) -> None:
    """Require this invocation's selected resolutions to match its memberships."""
    memberships_by_coordinate = {
        (
            intent.row.context,
            intent.row.workflow_name,
            intent.row.step_name,
            intent.row.output_name,
            intent.row.address,
        ): intent
        for intent in memberships
    }
    seen: set[tuple[str, str, str, str, str]] = set()
    for resolution in resolutions:
        coordinate = (
            resolution.context,
            resolution.workflow_name,
            resolution.step_name,
            resolution.output_name,
            resolution.address,
        )
        if coordinate in seen:
            raise ValidationError("selected-output resolution coordinate is duplicated")
        seen.add(coordinate)
        membership = memberships_by_coordinate.get(coordinate)
        if resolution.outcome == "generated":
            if resolution.existing_artifact_id is not None:
                raise ValidationError(
                    "generated selected-output resolution cannot name an existing artifact"
                )
            if membership is None or membership.existing_artifact_id is not None:
                raise ValidationError(
                    "generated selected-output resolution requires a fresh membership"
                )
        elif resolution.outcome == "reused":
            if resolution.existing_artifact_id is None:
                raise ValidationError(
                    "reused selected-output resolution is missing its artifact"
                )
            if (
                membership is None
                or membership.existing_artifact_id != resolution.existing_artifact_id
            ):
                raise ValidationError(
                    "reused selected-output resolution requires the same existing membership"
                )
        elif resolution.outcome is None:
            if membership is not None:
                raise ValidationError(
                    "unresolved selected output cannot have a membership intent"
                )
        else:
            raise ValidationError("selected-output resolution outcome is invalid")


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


def read_registered_source_authorities(
    path: Path,
    *,
    coordinates: Iterable[LogicalSourceCoordinate] | None = None,
) -> dict[LogicalSourceCoordinate, RegisteredSourceAuthority]:
    """Read current logical-source authority without touching source files."""
    selected = None if coordinates is None else frozenset(coordinates)
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            return _read_registered_source_authorities_conn(
                conn,
                coordinates=selected,
            )
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc


def read_registered_source_snapshots(
    path: Path,
    *,
    context: str,
) -> dict[LogicalSourceCoordinate, RegisteredSourceSnapshot]:
    """Read current source identity snapshots for one context."""
    authorities = read_registered_source_authorities(path)
    return {
        coordinate: RegisteredSourceSnapshot(
            content_digest=record.authority.content_digest,
            file_size=record.authority.file_size,
            declared_extension=record.authority.declaration.declared_extension,
        )
        for coordinate, record in authorities.items()
        if coordinate.context == context
    }


def _read_registered_source_snapshots_conn(
    conn: sqlite3.Connection,
    *,
    context: str,
) -> dict[LogicalSourceCoordinate, RegisteredSourceSnapshot]:
    rows = conn.execute(
        """
        SELECT source_scope, source_name, source_entity_id,
               content_digest, file_size, extension
        FROM artifacts
        WHERE origin = 'source' AND context = ?
        ORDER BY source_scope, source_name, source_entity_id
        """,
        (context,),
    ).fetchall()
    snapshots: dict[LogicalSourceCoordinate, RegisteredSourceSnapshot] = {}
    for scope, source_name, entity_id, digest, size, extension in rows:
        coordinate = LogicalSourceCoordinate(
            context=context,
            scope=str(scope),
            source_name=str(source_name),
            entity_id=entity_id,
        )
        if coordinate in snapshots:
            raise ValidationError("registry source artifact coordinate is duplicated")
        snapshots[coordinate] = RegisteredSourceSnapshot(
            content_digest=str(digest),
            file_size=int(size),
            declared_extension=str(extension),
        )
    return snapshots


def reconcile_manifest_and_source_authorities(
    path: Path,
    *,
    context: str,
    manifests: dict[str, Manifest],
    manifest_paths: dict[str, str],
    observations: Iterable[ObservedSourceAuthority],
) -> dict[LogicalSourceCoordinate, RegisteredSourceAuthority]:
    """Atomically reconcile frozen relevant manifests and prepared sources."""
    context = validate_path_token(context, label="context")
    if set(manifests) != set(manifest_paths):
        raise ValidationError("reconciled manifest names and paths do not match")
    if any(not isinstance(manifest, Manifest) for manifest in manifests.values()):
        raise ValidationError("reconciled manifest is malformed")
    observed = tuple(observations)
    if any(
        not isinstance(observation, ObservedSourceAuthority)
        for observation in observed
    ):
        raise ValidationError("source authority observation is malformed")
    by_coordinate = {
        observation.declaration.coordinate: observation
        for observation in observed
    }
    if len(by_coordinate) != len(observed):
        raise ValidationError("source authority observation coordinate is duplicated")
    if any(coordinate.context != context for coordinate in by_coordinate):
        raise ValidationError("source authority observation context does not match")
    now = _utc_now()
    try:
        with _connect(path) as conn:
            conn.row_factory = sqlite3.Row
            _validate_schema_version(conn)
            if conn.execute(
                "SELECT 1 FROM contexts WHERE context = ?",
                (context,),
            ).fetchone() is None:
                raise ValidationError(f"registry.db missing context: {context}")
            _insert_manifest_declarations(
                conn,
                context=context,
                manifests=manifests,
                manifest_paths=manifest_paths,
            )
            _reconcile_source_authorities_conn(
                conn,
                observations=by_coordinate,
                now=now,
            )
            return _read_registered_source_authorities_conn(
                conn,
                coordinates=frozenset(by_coordinate),
            )
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc


def _reconcile_source_authorities_conn(
    conn: sqlite3.Connection,
    *,
    observations: dict[LogicalSourceCoordinate, ObservedSourceAuthority],
    now: str,
) -> None:
    for coordinate, observation in sorted(
        observations.items(),
        key=lambda item: _logical_source_coordinate_sort_key(item[0]),
    ):
        existing = _select_source_authority_row(conn, coordinate)
        if (
            existing is not None
            and str(existing["path"]) != observation.declaration.declared_path
        ):
            raise ValidationError(
                "source relocation is unsupported for an already registered coordinate"
            )
        values = (
            observation.declaration.declared_path,
            observation.content_digest,
            short_hash(observation.content_digest),
            observation.file_size,
            observation.declaration.declared_extension,
            observation.guard.st_dev,
            observation.guard.st_ino,
            observation.guard.st_size,
            observation.guard.st_mtime_ns,
            observation.guard.st_ctime_ns,
            now,
        )
        if existing is None:
            conn.execute(
                """
                INSERT INTO artifacts (
                    origin, context, path, content_digest, output_hash,
                    file_size, extension, source_scope, source_name,
                    source_entity_id, source_st_dev, source_st_ino,
                    source_st_size, source_st_mtime_ns, source_st_ctime_ns,
                    created_at
                )
                VALUES (
                    'source', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    coordinate.context,
                    *values[:5],
                    coordinate.scope,
                    coordinate.source_name,
                    coordinate.entity_id,
                    *values[5:],
                ),
            )
        else:
            conn.execute(
                """
                UPDATE artifacts
                SET path = ?, content_digest = ?, output_hash = ?,
                    file_size = ?, extension = ?, source_st_dev = ?,
                    source_st_ino = ?, source_st_size = ?,
                    source_st_mtime_ns = ?, source_st_ctime_ns = ?,
                    created_at = ?
                WHERE artifact_id = ?
                """,
                (*values, int(existing["artifact_id"])),
            )


def _read_registered_source_authorities_conn(
    conn: sqlite3.Connection,
    *,
    coordinates: frozenset[LogicalSourceCoordinate] | None,
) -> dict[LogicalSourceCoordinate, RegisteredSourceAuthority]:
    rows = conn.execute(
        """
        SELECT artifact_id, context, path, content_digest, file_size, extension,
               source_scope, source_name, source_entity_id, source_st_dev,
               source_st_ino, source_st_size, source_st_mtime_ns,
               source_st_ctime_ns
        FROM artifacts
        WHERE origin = 'source'
        ORDER BY context, source_scope, source_name, source_entity_id
        """,
    ).fetchall()
    authorities: dict[LogicalSourceCoordinate, RegisteredSourceAuthority] = {}
    for row in rows:
        coordinate = LogicalSourceCoordinate(
            context=str(row["context"]),
            scope=str(row["source_scope"]),
            source_name=str(row["source_name"]),
            entity_id=row["source_entity_id"],
        )
        if coordinates is not None and coordinate not in coordinates:
            continue
        source_path = _validate_registry_artifact_path(
            str(row["path"]),
            origin="source",
        )
        if coordinate in authorities:
            raise ValidationError("registry source artifact coordinate is duplicated")
        authority = registered_source_authority_from_facts(
            coordinate=coordinate,
            declared_path=source_path,
            declared_extension=str(row["extension"]),
            content_digest=str(row["content_digest"]),
            file_size=int(row["file_size"]),
            guard=SourceOccurrenceGuard(
                st_dev=int(row["source_st_dev"]),
                st_ino=int(row["source_st_ino"]),
                st_size=int(row["source_st_size"]),
                st_mtime_ns=int(row["source_st_mtime_ns"]),
                st_ctime_ns=int(row["source_st_ctime_ns"]),
            ),
        )
        authorities[coordinate] = RegisteredSourceAuthority(
            artifact_id=int(row["artifact_id"]),
            authority=authority,
        )
    if coordinates is not None and not set(authorities).issubset(coordinates):
        raise ValidationError("registry returned an unrequested source coordinate")
    return authorities


def _select_source_authority_row(
    conn: sqlite3.Connection,
    coordinate: LogicalSourceCoordinate,
) -> sqlite3.Row | None:
    if coordinate.scope == "global":
        return conn.execute(
            """
            SELECT artifact_id, path
            FROM artifacts
            WHERE origin = 'source' AND context = ? AND source_scope = 'global'
              AND source_name = ?
            """,
            (coordinate.context, coordinate.source_name),
        ).fetchone()
    return conn.execute(
        """
        SELECT artifact_id, path
        FROM artifacts
        WHERE origin = 'source' AND context = ? AND source_scope = 'entity'
          AND source_name = ? AND source_entity_id = ?
        """,
        (coordinate.context, coordinate.source_name, coordinate.entity_id),
    ).fetchone()


def _logical_source_coordinate_sort_key(
    coordinate: LogicalSourceCoordinate,
) -> tuple[str, str, str, str]:
    return (
        coordinate.context,
        coordinate.scope,
        coordinate.source_name,
        coordinate.entity_id or "",
    )


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
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.context = ?
                  AND a.path = ?
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND EXISTS (
                    SELECT 1
                    FROM published_outputs po
                    WHERE po.artifact_id = a.artifact_id
                      AND po.context = a.context
                  )
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
                LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
                WHERE a.context = ?
                  AND (
                    a.path = ?
                    OR a.published_path = ?
                    OR a.staging_path = ?
                  )
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND EXISTS (
                    SELECT 1
                    FROM published_outputs po
                    WHERE po.artifact_id = a.artifact_id
                      AND po.context = a.context
                  )
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


def resolve_reusable_artifact_bundle(
    path: Path,
    *,
    runtime_root: Path,
    request: ReusableArtifactBundleRequest,
    preferred_artifact_ids: tuple[int, ...] | None = None,
) -> ReusableArtifactBundleCandidate | None:
    """Resolve one coherent reusable bundle independently of workflow membership."""
    validate_stored_request_bundle_projection_v3(
        request_bundle_digest=request.resolved_projection.request_bundle_digest,
        projection_json=request.resolved_projection.canonical_json,
    )
    declared_outputs = dict(request.sibling_outputs)
    if len(declared_outputs) != len(request.sibling_outputs):
        raise ValidationError("reusable bundle request has duplicate sibling outputs")
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            stored_projection = _validated_stored_request_bundle_projection(
                conn,
                request.resolved_projection.request_bundle_digest,
                missing_ok=True,
            )
            if stored_projection is None:
                return None
            if (
                stored_projection.resolved_projection.canonical_json
                != request.resolved_projection.canonical_json
            ):
                raise ValidationError(
                    "request projection digest has conflicting canonical payload"
                )
            rows = conn.execute(
                """
                SELECT artifact_id, run_id, path, published_path,
                       content_digest, output_hash, file_size,
                       extension, workflow_name, step_name, output_name, address,
                       request_bundle_digest
                FROM artifacts
                WHERE context = ?
                  AND origin = 'workflow_output'
                  AND is_published = 1
                  AND step_name = ?
                  AND address = ?
                  AND request_bundle_digest = ?
                ORDER BY run_id, artifact_id
                """,
                (
                    request.context,
                    request.step_name,
                    request.address,
                    request.resolved_projection.request_bundle_digest,
                ),
            ).fetchall()
            candidates_by_run: dict[int, dict[str, ReusableArtifactCandidate]] = {}
            for row in rows:
                if (
                    row["run_id"] is None
                    or row["workflow_name"] is None
                    or row["step_name"] is None
                    or row["output_name"] is None
                    or row["address"] is None
                    or row["output_hash"] is None
                    or row["request_bundle_digest"] is None
                ):
                    raise ValidationError("registry reusable artifact row is incomplete")
                run_id = int(row["run_id"])
                output_name = str(row["output_name"])
                run_outputs = candidates_by_run.setdefault(run_id, {})
                if output_name in run_outputs:
                    raise ValidationError(
                        "registry reusable artifact bundle has duplicate sibling output: "
                        f"run_id={run_id}, output={output_name!r}"
                    )
                artifact_id = int(row["artifact_id"])
                run_outputs[output_name] = ReusableArtifactCandidate(
                    artifact_id=artifact_id,
                    run_id=run_id,
                    path=str(row["path"]),
                    published_path=row["published_path"],
                    content_digest=str(row["content_digest"]),
                    output_hash=str(row["output_hash"]),
                    file_size=int(row["file_size"]),
                    extension=str(row["extension"]),
                    workflow_name=str(row["workflow_name"]),
                    step_name=str(row["step_name"]),
                    output_name=output_name,
                    address=str(row["address"]),
                    request_bundle_digest=str(row["request_bundle_digest"]),
                    dependencies=tuple(_dependencies_for_artifact(conn, artifact_id)),
                )

            complete = [
                ReusableArtifactBundleCandidate(
                    run_id=run_id,
                    outputs=tuple(run_outputs[name] for name in sorted(run_outputs)),
                )
                for run_id, run_outputs in candidates_by_run.items()
                if set(run_outputs) == set(declared_outputs)
            ]
            if not complete:
                return None

            signatures = {
                tuple(
                    (candidate.output_name, candidate.content_digest)
                    for candidate in bundle.outputs
                )
                for bundle in complete
            }
            if len(signatures) > 1:
                conflicts = "; ".join(
                    "run_id="
                    f"{bundle.run_id}, artifact_ids="
                    f"{tuple(candidate.artifact_id for candidate in bundle.outputs)}"
                    for bundle in complete
                )
                raise ValidationError(
                    "divergent reusable artifact bundles for one deterministic "
                    f"request: {conflicts}"
                )

            lineage_compatible = [
                bundle
                for bundle in complete
                if all(
                    _dependencies_match_request(
                        conn,
                        context=request.context,
                        dependencies=list(candidate.dependencies),
                        input_records=request.input_records,
                    )
                    for candidate in bundle.outputs
                )
            ]
            if not lineage_compatible:
                return None

            valid: list[ReusableArtifactBundleCandidate] = []
            invalid_reasons: list[str] = []
            for bundle in lineage_compatible:
                reasons = [
                    reason
                    for candidate in bundle.outputs
                    if (
                        reason := _reusable_artifact_occurrence_error(
                            runtime_root=runtime_root,
                            context=request.context,
                            candidate=candidate,
                            declared_extension=declared_outputs[candidate.output_name],
                        )
                    )
                    is not None
                ]
                if reasons:
                    invalid_reasons.extend(reasons)
                else:
                    valid.append(bundle)
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if not valid:
        raise ValidationError(invalid_reasons[0])
    if preferred_artifact_ids is not None:
        preferred = set(preferred_artifact_ids)
        for bundle in valid:
            if {candidate.artifact_id for candidate in bundle.outputs} == preferred:
                return bundle
    return min(valid, key=lambda bundle: bundle.run_id)


def _dependencies_for_artifact(
    conn: sqlite3.Connection,
    artifact_id: int,
) -> list[RegistryDependency]:
    cursor = conn.execute(
        """
        SELECT dependent_artifact_id, source_artifact_id,
               source_content_digest, source_file_size, source_extension, input_path,
               binding_name, dependency_role, source_step_name,
               source_output_name, source_address, source_scope, source_name,
               source_entity_id, source_occurrence_path, dependency_set_id,
               manifest_value_schema, manifest_digest, edge_cardinality
        FROM artifact_dependencies
        WHERE dependent_artifact_id = ?
        ORDER BY binding_name, input_path, source_artifact_id
        """,
        (artifact_id,),
    )
    rows = cursor.fetchall()
    if not rows or isinstance(rows[0], sqlite3.Row):
        return [_registry_dependency_from_row(row) for row in rows]
    names = [str(description[0]) for description in cursor.description]
    return [
        _registry_dependency_from_row(dict(zip(names, row)))  # type: ignore[arg-type]
        for row in rows
    ]


def _dependencies_match_request(
    conn: sqlite3.Connection,
    *,
    context: str,
    dependencies: list[RegistryDependency],
    input_records: tuple[ArtifactInputRow, ...],
) -> bool:
    if len(dependencies) != len(input_records):
        return False
    unused = list(dependencies)
    for input_record in input_records:
        match_index = _find_matching_dependency_index(
            conn,
            context=context,
            dependencies=unused,
            input_record=input_record,
        )
        if match_index is None:
            return False
        unused.pop(match_index)
    return not unused


def _find_matching_dependency_index(
    conn: sqlite3.Connection,
    *,
    context: str,
    dependencies: list[RegistryDependency],
    input_record: ArtifactInputRow,
) -> int | None:
    for index, dependency in enumerate(dependencies):
        if not _dependency_common_fields_match(dependency, input_record):
            continue
        source = _dependency_source_artifact(conn, dependency.source_artifact_id)
        if input_record.origin == "source":
            if not _source_dependency_matches_current_input(
                context=context,
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
        and dependency.manifest_value_schema == input_record.manifest_value_schema
        and dependency.manifest_digest == input_record.manifest_digest
        and dependency.edge_cardinality == input_record.edge_cardinality
    )


def _source_dependency_matches_current_input(
    *,
    context: str,
    dependency: RegistryDependency,
    source: RegistryArtifact,
    input_record: ArtifactInputRow,
) -> bool:
    if source.origin != "source" or source.context != context:
        return False
    if (
        input_record.source_artifact_path is None
        or input_record.source_scope is None
        or input_record.source_name is None
        or input_record.source_extension is None
        or input_record.source_content_digest is None
        or input_record.source_file_size is None
    ):
        raise ValidationError("source dependency is missing prepared source authority")
    if (
        source.path != input_record.source_artifact_path
        or source.source_scope != input_record.source_scope
        or source.source_name != input_record.source_name
        or source.source_entity_id != input_record.source_entity_id
        or dependency.source_scope != input_record.source_scope
        or dependency.source_name != input_record.source_name
        or dependency.source_entity_id != input_record.source_entity_id
        or dependency.source_occurrence_path != input_record.source_artifact_path
    ):
        return False
    return (
        dependency.source_content_digest
        == source.content_digest
        == input_record.source_content_digest
        and dependency.source_file_size
        == source.file_size
        == input_record.source_file_size
        and dependency.source_extension
        == source.extension
        == input_record.source_extension
    )


def _workflow_dependency_matches_input(
    conn: sqlite3.Connection,
    *,
    dependency: RegistryDependency,
    source: RegistryArtifact,
    input_record: ArtifactInputRow,
) -> bool:
    if source.origin != "workflow_output":
        return False
    if input_record.registry_source_artifact_id is None:
        return False
    requested = _dependency_source_artifact(
        conn,
        input_record.registry_source_artifact_id,
    )
    if not _workflow_artifacts_are_equivalent(source, requested):
        return False
    _validate_workflow_dependency_snapshot(dependency=dependency, source=source)
    if not _workflow_artifact_dependencies_match_registry(
        conn,
        artifact_id=source.artifact_id,
        expected_context=source.context,
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


def _workflow_artifacts_are_equivalent(
    source: RegistryArtifact,
    requested: RegistryArtifact,
) -> bool:
    return (
        source.origin == "workflow_output"
        and requested.origin == "workflow_output"
        and source.context == requested.context
        and source.step_name == requested.step_name
        and source.output_name == requested.output_name
        and source.address == requested.address
        and source.request_bundle_digest == requested.request_bundle_digest
        and source.content_digest == requested.content_digest
        and source.file_size == requested.file_size
        and source.extension == requested.extension
    )


def _workflow_artifact_dependencies_match_registry(
    conn: sqlite3.Connection,
    *,
    artifact_id: int,
    expected_context: str,
    visited: set[int],
) -> bool:
    if artifact_id in visited:
        raise ValidationError("registry dependency graph contains a cycle")
    visited.add(artifact_id)
    dependencies = _dependencies_for_artifact(conn, artifact_id)
    if not dependencies:
        visited.remove(artifact_id)
        artifact = _dependency_source_artifact(conn, artifact_id)
        return _artifact_projection_declares_no_bindings(conn, artifact)
    for dependency in dependencies:
        source = _dependency_source_artifact(conn, dependency.source_artifact_id)
        if source.context != expected_context:
            visited.remove(artifact_id)
            return False
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
                expected_context=expected_context,
                visited=visited,
            ):
                visited.remove(artifact_id)
                return False
            continue
        visited.remove(artifact_id)
        return False
    visited.remove(artifact_id)
    return True


def _artifact_projection_declares_no_bindings(
    conn: sqlite3.Connection,
    artifact: RegistryArtifact,
) -> bool:
    request_bundle_digest = artifact.request_bundle_digest
    if request_bundle_digest is None:
        return False
    projection = _validated_stored_request_bundle_projection(
        conn,
        request_bundle_digest,
    )
    assert projection is not None
    payload = json.loads(projection.resolved_projection.canonical_json)
    return not payload["role_labelled_bindings"]


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
    cursor = conn.execute(
        f"""
        SELECT {_ARTIFACT_SELECT_COLUMNS}
        FROM artifacts a
        LEFT JOIN parameters p ON a.parameter_id = p.parameter_id
        WHERE a.artifact_id = ?
        """,
        (artifact_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValidationError("registry dependency source artifact is missing")
    if isinstance(row, sqlite3.Row):
        return _registry_artifact_from_row(row)
    names = [str(description[0]) for description in cursor.description]
    return _registry_artifact_from_row(dict(zip(names, row)))  # type: ignore[arg-type]


def _reusable_artifact_occurrence_error(
    *,
    runtime_root: Path,
    context: str,
    candidate: ReusableArtifactCandidate,
    declared_extension: str,
) -> str | None:
    if candidate.published_path != candidate.path:
        raise ValidationError(
            "registered reusable artifact publication path is inconsistent"
        )
    if candidate.extension != declared_extension:
        raise ValidationError(
            "registered reusable artifact extension does not match its output contract"
        )
    if not candidate.path.endswith(declared_extension):
        raise ValidationError(
            "registered reusable artifact path does not match its declared extension"
        )
    if not is_valid_digest(candidate.content_digest):
        raise ValidationError("registered reusable artifact digest is invalid")
    if candidate.output_hash != short_hash(candidate.content_digest):
        raise ValidationError("registered reusable artifact hash is invalid")
    expected_path = canonical_output_path(
        context=context,
        step_name=candidate.step_name,
        address=candidate.address,
        request_bundle_digest=candidate.request_bundle_digest,
        output_name=candidate.output_name,
        output_hash=candidate.output_hash,
        declared_extension=declared_extension,
    )
    if candidate.path != expected_path:
        raise ValidationError(
            "registered reusable artifact path does not match its request identity"
        )
    artifact_path = _runtime_relative_file_path(runtime_root, candidate.path)
    outputs_root = (runtime_root / CANONICAL_OUTPUT_ROOT).resolve()
    if not _path_contains_or_same(outputs_root, artifact_path):
        raise ValidationError(
            "registered reusable artifact path must stay inside outputs/v1/"
        )
    if not artifact_path.is_file():
        return "registered reusable artifact file is missing"
    if artifact_path.stat().st_size != candidate.file_size:
        return "registered reusable artifact file size mismatch"
    return None


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
                  AND a.step_name = po.step_name
                  AND a.output_name = po.output_name
                  AND a.address = po.address
                  AND a.origin = 'workflow_output'
                  AND a.is_published = 1
                  AND a.published_path = po.path
                  AND a.content_digest = po.output_digest
                  AND a.output_hash = po.output_hash
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
                   source_output_name, source_address, source_scope, source_name,
                   source_entity_id, source_occurrence_path, dependency_set_id,
                   manifest_value_schema, manifest_digest, edge_cardinality
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
    where_sql = "WHERE b.run_id = ?"
    values: tuple[object, ...] = (run_id,)
    if context is not None:
        where_sql += " AND r.context = ?"
        values = (run_id, context)
    try:
        _require_run_id(conn, run_id)
        rows = conn.execute(
            """
            SELECT b.run_id, r.context, r.workflow_name, b.step_name,
                   b.manifest_usage_role, b.manifest_name,
                   b.manifest_value_schema, b.manifest_digest, v.entity_count,
                   v.canonical_body
            FROM run_manifest_bindings b
            JOIN workflow_runs r ON r.run_id = b.run_id
            JOIN manifest_values v
              ON v.value_schema = b.manifest_value_schema
             AND v.manifest_digest = b.manifest_digest
            {where_sql}
            ORDER BY b.step_name, b.manifest_usage_role, b.manifest_name
            """.format(where_sql=where_sql),
            values,
        ).fetchall()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    return [_registry_manifest_binding_from_row(row) for row in rows]


def _read_run_execution_population_conn(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    context: str | None = None,
) -> RegistryExecutionPopulation | None:
    where_sql = "WHERE p.run_id = ?"
    values: tuple[object, ...] = (run_id,)
    if context is not None:
        where_sql += " AND r.context = ?"
        values = (run_id, context)
    _require_run_id(conn, run_id)
    row = conn.execute(
        """
        SELECT p.run_id, r.context, r.workflow_name, p.manifest_name,
               p.manifest_value_schema, p.manifest_digest, v.entity_count,
               v.canonical_body
        FROM run_execution_population p
        JOIN workflow_runs r ON r.run_id = p.run_id
        JOIN manifest_values v
          ON v.value_schema = p.manifest_value_schema
         AND v.manifest_digest = p.manifest_digest
        {where_sql}
        """.format(where_sql=where_sql),
        values,
    ).fetchone()
    if row is None:
        return None
    return _registry_execution_population_from_row(row)


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


def read_run_execution_population(
    path: Path,
    *,
    run_id: int,
    context: str | None = None,
) -> RegistryExecutionPopulation | None:
    """Read the workflow-level execution population recorded for a run."""
    _validate_positive_id(run_id, label="run id")
    try:
        with _connect_readonly_rows(path) as conn:
            _validate_schema_version(conn)
            return _read_run_execution_population_conn(
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

    def read_run_execution_population(
        self,
        *,
        run_id: int,
        context: str | None = None,
    ) -> RegistryExecutionPopulation | None:
        _validate_positive_id(run_id, label="run id")
        return _read_run_execution_population_conn(
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
                SELECT d.context, d.manifest_name AS name,
                       d.declared_path AS path,
                       v.value_schema AS manifest_value_schema,
                       v.entity_count, v.manifest_digest, v.canonical_body
                FROM manifest_declarations d
                JOIN manifest_values v
                  ON v.value_schema = d.last_validated_manifest_value_schema
                 AND v.manifest_digest = d.last_validated_manifest_digest
                WHERE d.context = ?
                ORDER BY d.manifest_name
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
                SELECT d.context, d.manifest_name AS name,
                       d.declared_path AS path,
                       v.value_schema AS manifest_value_schema,
                       v.entity_count, v.manifest_digest, v.canonical_body
                FROM manifest_declarations d
                JOIN manifest_values v
                  ON v.value_schema = d.last_validated_manifest_value_schema
                 AND v.manifest_digest = d.last_validated_manifest_digest
                WHERE d.context = ? AND d.manifest_name = ?
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
            manifest_count = _count_rows(
                conn,
                "manifest_declarations",
                context=context,
            )
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
                """
                SELECT runtime_path, storage_layout_version
                FROM contexts
                WHERE context = ?
                """,
                (context,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise ValidationError(f"registry.db is malformed: {exc}") from exc
    if row is None:
        raise ValidationError(f"unknown context: {context}")
    if row[1] != STORAGE_LAYOUT_VERSION:
        raise ValidationError("registry.db storage layout version is incompatible")
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
    resolution_summary_json: str,
    environment_observation_json: str,
    created_at: str,
    is_current: bool,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO workflow_runs (
            context, workflow_name, selected_step_name, selected_output_name,
            run_workspace, run_plan_path, run_plan_digest, base_workflow_name,
            resolution_summary_json, environment_observation_json,
            is_current, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            resolution_summary_json,
            environment_observation_json,
            1 if is_current else 0,
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
    finalized_projections: dict[
        tuple[str, str], ResolvedRequestBundleProjectionV3
    ],
    created_at: str,
) -> dict[tuple[str, str, str], int]:
    artifact_ids: dict[tuple[str, str, str], int] = {}
    for row in artifact_rows:
        parameter_id = parameter_ids[(row.step_name, row.parameters_json)]
        try:
            projection = finalized_projections[(row.step_name, row.address)]
        except KeyError as exc:
            raise ValidationError(
                "workflow output artifact is missing its finalized projection"
            ) from exc
        cursor = conn.execute(
            """
            INSERT INTO artifacts (
                origin, run_id, context, workflow_name, step_name, output_name,
                address, job_id, parameter_id, path, is_selected_output,
                is_published, published_path, staging_path, content_digest,
                output_hash, file_size, extension, callable_ref,
                request_bundle_digest, created_at
            )
            VALUES (
                'workflow_output', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?
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
                projection.request_bundle_digest,
                created_at,
            ),
        )
        artifact_ids[(row.step_name, row.output_name, row.address)] = int(
            cursor.lastrowid
        )
    return artifact_ids


def _validate_no_divergent_fresh_bundles(
    conn: sqlite3.Connection,
    *,
    context: str,
    run_id: int,
    artifact_rows: tuple[WorkflowOutputArtifactRow, ...],
    finalized_projections: dict[
        tuple[str, str], ResolvedRequestBundleProjectionV3
    ],
) -> None:
    fresh_rows_by_job: dict[
        tuple[str, str], dict[str, WorkflowOutputArtifactRow]
    ] = {}
    for row in artifact_rows:
        job_key = (row.step_name, row.address)
        fresh_outputs = fresh_rows_by_job.setdefault(job_key, {})
        if row.output_name in fresh_outputs:
            raise ValidationError("fresh deterministic bundle repeats a sibling output")
        fresh_outputs[row.output_name] = row

    for (step_name, address), fresh_outputs in sorted(fresh_rows_by_job.items()):
        try:
            projection = finalized_projections[(step_name, address)]
        except KeyError as exc:
            raise ValidationError(
                "fresh deterministic bundle is missing its finalized projection"
            ) from exc
        fresh_output_names = set(fresh_outputs)
        fresh_signature = tuple(
            (output_name, fresh_outputs[output_name].content_digest)
            for output_name in sorted(fresh_outputs)
        )
        historical_rows = conn.execute(
            """
            SELECT artifact_id, run_id, output_name, content_digest
            FROM artifacts
            WHERE origin = 'workflow_output'
              AND is_published = 1
              AND context = ?
              AND step_name = ?
              AND address = ?
              AND request_bundle_digest = ?
            ORDER BY run_id, output_name, artifact_id
            """,
            (
                context,
                step_name,
                address,
                projection.request_bundle_digest,
            ),
        ).fetchall()
        historical_by_run: dict[int, dict[str, tuple[int, str]]] = {}
        for historical_row in historical_rows:
            historical_run_id = historical_row[1]
            output_name = historical_row[2]
            if historical_run_id is None or not isinstance(output_name, str):
                raise ValidationError("registry deterministic bundle row is incomplete")
            outputs = historical_by_run.setdefault(int(historical_run_id), {})
            if output_name in outputs:
                raise ValidationError(
                    "registry deterministic bundle repeats a sibling output"
                )
            outputs[output_name] = (int(historical_row[0]), str(historical_row[3]))

        for historical_run_id, historical_outputs in sorted(
            historical_by_run.items()
        ):
            if set(historical_outputs) != fresh_output_names:
                continue
            historical_signature = tuple(
                (output_name, historical_outputs[output_name][1])
                for output_name in sorted(historical_outputs)
            )
            if historical_signature == fresh_signature:
                continue
            historical_artifact_ids = tuple(
                historical_outputs[output_name][0]
                for output_name in sorted(historical_outputs)
            )
            raise ValidationError(
                "deterministic request bundle produced divergent content: "
                f"new run {run_id} conflicts with historical run "
                f"{historical_run_id}, artifacts {historical_artifact_ids}"
            )


def _validated_stored_request_bundle_projection(
    conn: sqlite3.Connection,
    request_bundle_digest: str,
    *,
    missing_ok: bool = False,
    cache: dict[str, ValidatedStoredRequestBundleProjectionV3] | None = None,
) -> ValidatedStoredRequestBundleProjectionV3 | None:
    if not is_valid_digest(request_bundle_digest):
        raise ValidationError("registry request projection digest is invalid")
    if cache is not None and request_bundle_digest in cache:
        return cache[request_bundle_digest]
    row = conn.execute(
        """
        SELECT projection_json
        FROM request_bundle_projections
        WHERE request_bundle_digest = ?
        """,
        (request_bundle_digest,),
    ).fetchone()
    if row is None:
        if missing_ok:
            return None
        raise ValidationError("registry request projection is missing")
    validated = validate_stored_request_bundle_projection_v3(
        request_bundle_digest=request_bundle_digest,
        projection_json=str(row[0]),
    )
    if cache is not None:
        cache[request_bundle_digest] = validated
    return validated


def _insert_or_validate_request_bundle_projection(
    conn: sqlite3.Connection,
    projection: ResolvedRequestBundleProjectionV3,
    *,
    cache: dict[str, ValidatedStoredRequestBundleProjectionV3],
) -> None:
    validated = validate_stored_request_bundle_projection_v3(
        request_bundle_digest=projection.request_bundle_digest,
        projection_json=projection.canonical_json,
    )
    for upstream_digest in validated.direct_upstream_request_bundle_digests:
        _validated_stored_request_bundle_projection(
            conn,
            upstream_digest,
            cache=cache,
        )
    existing = _validated_stored_request_bundle_projection(
        conn,
        projection.request_bundle_digest,
        missing_ok=True,
        cache=cache,
    )
    if existing is not None:
        if existing.resolved_projection.canonical_json != projection.canonical_json:
            raise ValidationError(
                "registry request projection digest has conflicting canonical payload"
            )
        return
    conn.execute(
        """
        INSERT INTO request_bundle_projections (
            request_bundle_digest, projection_json
        )
        VALUES (?, ?)
        """,
        (projection.request_bundle_digest, projection.canonical_json),
    )
    cache[projection.request_bundle_digest] = validated


def _authenticate_reused_projection_seed(
    conn: sqlite3.Connection,
    *,
    context: str,
    seed: ReusedProjectionSeed,
    projection_cache: dict[str, ValidatedStoredRequestBundleProjectionV3],
) -> ResolvedRequestBundleProjectionV3:
    if type(seed.actual_artifact_id) is not int or seed.actual_artifact_id <= 0:
        raise ValidationError("reused projection seed artifact id is invalid")
    if not is_valid_digest(seed.request_bundle_digest):
        raise ValidationError("reused projection seed digest is invalid")
    artifact = _dependency_source_artifact(conn, seed.actual_artifact_id)
    coordinate = seed.requested_output
    if (
        artifact.context != context
        or artifact.origin != "workflow_output"
        or not artifact.is_published
        or artifact.step_name != coordinate.step_name
        or artifact.output_name != coordinate.output_name
        or artifact.address != coordinate.address
        or artifact.request_bundle_digest != seed.request_bundle_digest
    ):
        raise ValidationError("reused projection seed does not match its artifact")
    validated = _validated_stored_request_bundle_projection(
        conn,
        seed.request_bundle_digest,
        cache=projection_cache,
    )
    assert validated is not None
    for upstream_digest in validated.direct_upstream_request_bundle_digests:
        _validated_stored_request_bundle_projection(
            conn,
            upstream_digest,
            cache=projection_cache,
        )
    return validated.resolved_projection


def _finalize_retained_job_projections(
    conn: sqlite3.Connection,
    *,
    context: str,
    artifact_rows: tuple[WorkflowOutputArtifactRow, ...],
    projection_recipes: tuple[RetainedJobProjectionRecipe, ...],
    reused_projection_seeds: tuple[ReusedProjectionSeed, ...],
) -> dict[tuple[str, str], ResolvedRequestBundleProjectionV3]:
    source_snapshots = _read_registered_source_snapshots_conn(conn, context=context)
    upstream_states: dict[
        RequestedOutputCoordinate,
        ResolvedRequestBundleProjectionV3,
    ] = {}
    projection_cache: dict[str, ValidatedStoredRequestBundleProjectionV3] = {}
    for seed in reused_projection_seeds:
        if seed.requested_output.namespace != context:
            raise ValidationError("reused projection seed belongs to another context")
        if seed.requested_output in upstream_states:
            raise ValidationError("reused projection seed coordinate is duplicated")
        upstream_states[seed.requested_output] = _authenticate_reused_projection_seed(
            conn,
            context=context,
            seed=seed,
            projection_cache=projection_cache,
        )

    artifact_outputs_by_job: dict[tuple[str, str], set[str]] = {}
    for row in artifact_rows:
        artifact_outputs_by_job.setdefault((row.step_name, row.address), set()).add(
            row.output_name
        )

    finalized: dict[tuple[str, str], ResolvedRequestBundleProjectionV3] = {}
    for recipe in projection_recipes:
        job_key = (recipe.step_name, recipe.address)
        if job_key in finalized:
            raise ValidationError("retained projection recipe is duplicated")
        if len(recipe.output_names) != len(set(recipe.output_names)):
            raise ValidationError("retained projection recipe repeats a sibling output")
        declared_outputs = {
            sibling.output_name
            for sibling in recipe.projection_plan.output_contract.sibling_outputs
        }
        if (
            recipe.projection_plan.namespace != context
            or recipe.projection_plan.step_contract.step_contract_id
            != recipe.step_name
            or recipe.projection_plan.address != recipe.address
            or declared_outputs != set(recipe.output_names)
        ):
            raise ValidationError("retained projection recipe identity is inconsistent")
        actual_outputs = artifact_outputs_by_job.get(job_key)
        if actual_outputs is None or actual_outputs != set(recipe.output_names):
            raise ValidationError(
                "retained projection recipe does not match complete sibling artifacts"
            )
        projection_state = resolve_request_bundle_projection_plan(
            recipe.projection_plan,
            source_snapshots=source_snapshots,
            upstream_states=upstream_states,
        )
        if not isinstance(
            projection_state,
            ResolvedRequestBundleProjectionV3,
        ):
            raise ValidationError("retained projection remained unresolved after source upsert")
        _insert_or_validate_request_bundle_projection(
            conn,
            projection_state,
            cache=projection_cache,
        )
        finalized[job_key] = projection_state
        for output_name in recipe.output_names:
            coordinate = RequestedOutputCoordinate(
                namespace=context,
                step_name=recipe.step_name,
                output_name=output_name,
                address=recipe.address,
            )
            if coordinate in upstream_states:
                raise ValidationError("retained requested-output coordinate is duplicated")
            upstream_states[coordinate] = projection_state

    if set(finalized) != set(artifact_outputs_by_job):
        raise ValidationError("retained artifact is missing its projection recipe")
    return finalized


def _reused_projection_identities(
    *,
    context: str,
    seeds: tuple[ReusedProjectionSeed, ...],
) -> dict[RequestedOutputCoordinate, str]:
    identities: dict[RequestedOutputCoordinate, str] = {}
    for seed in seeds:
        if seed.requested_output.namespace != context:
            raise ValidationError("reused projection seed belongs to another context")
        if seed.requested_output in identities:
            raise ValidationError("reused projection seed coordinate is duplicated")
        if not is_valid_digest(seed.request_bundle_digest):
            raise ValidationError("reused projection seed digest is invalid")
        identities[seed.requested_output] = seed.request_bundle_digest
    return identities


def _insert_artifact_dependencies(
    conn: sqlite3.Connection,
    *,
    runtime_root: Path,
    context: str,
    reused_projection_identities: dict[
        RequestedOutputCoordinate,
        str,
    ],
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
                reused_projection_identities=reused_projection_identities,
                input_record=input_record,
                artifact_ids=artifact_ids,
            )
            if input_record.origin == "source":
                if (
                    input_record.source_content_digest is None
                    or input_record.source_file_size is None
                    or input_record.source_extension is None
                ):
                    raise ValidationError(
                        "source dependency is missing its prepared snapshot"
                    )
                source_content_digest = input_record.source_content_digest
                source_file_size = input_record.source_file_size
                source_extension = input_record.source_extension
            else:
                source_content_digest, source_file_size, source_extension = (
                    _source_artifact_snapshot(conn, source_artifact_id)
                )
            conn.execute(
                """
                INSERT INTO artifact_dependencies (
                    dependent_artifact_id, source_artifact_id,
                    source_content_digest, source_file_size, source_extension,
                    input_path, binding_name, dependency_role, source_step_name,
                    source_output_name, source_address, source_scope, source_name,
                    source_entity_id, source_occurrence_path,
                    manifest_value_schema, manifest_digest, edge_cardinality
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    input_record.source_scope,
                    input_record.source_name,
                    input_record.source_entity_id,
                    input_record.source_artifact_path,
                    input_record.manifest_value_schema,
                    input_record.manifest_digest,
                    input_record.edge_cardinality,
                ),
            )


def _dependency_source_artifact_id(
    conn: sqlite3.Connection,
    *,
    runtime_root: Path,
    context: str,
    reused_projection_identities: dict[
        RequestedOutputCoordinate,
        str,
    ],
    input_record: ArtifactInputRow,
    artifact_ids: dict[tuple[str, str, str], int],
) -> int:
    if input_record.origin == "source":
        if (
            input_record.source_scope is None
            or input_record.source_name is None
            or input_record.source_artifact_path is None
        ):
            raise ValidationError("source dependency is missing its logical coordinate")
        if input_record.source_scope == "global":
            if input_record.source_entity_id is not None:
                raise ValidationError("global source dependency has an entity id")
            row = conn.execute(
                """
                SELECT artifact_id, path, content_digest, file_size, extension
                FROM artifacts
                WHERE context = ? AND origin = 'source'
                  AND source_scope = 'global' AND source_name = ?
                """,
                (context, input_record.source_name),
            ).fetchone()
        elif input_record.source_scope == "entity":
            if input_record.source_entity_id is None:
                raise ValidationError("entity source dependency is missing entity id")
            row = conn.execute(
                """
                SELECT artifact_id, path, content_digest, file_size, extension
                FROM artifacts
                WHERE context = ? AND origin = 'source'
                  AND source_scope = 'entity' AND source_name = ?
                  AND source_entity_id = ?
                """,
                (context, input_record.source_name, input_record.source_entity_id),
            ).fetchone()
        else:
            raise ValidationError("source dependency scope is invalid")
        if row is None:
            raise ValidationError("registry.db missing source artifact dependency")
        expected = (
            input_record.source_artifact_path,
            input_record.source_content_digest,
            input_record.source_file_size,
            input_record.source_extension,
        )
        if tuple(row[1:]) != expected:
            raise ValidationError(
                "source dependency snapshot does not match current authority"
            )
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
                reused_projection_identities=reused_projection_identities,
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
    reused_projection_identities: dict[
        RequestedOutputCoordinate,
        str,
    ],
    input_record: ArtifactInputRow,
) -> None:
    if input_record.registry_source_artifact_id is None:
        raise ValidationError("reused workflow dependency source artifact is missing")
    if input_record.source_extension is None:
        raise ValidationError("reused workflow dependency source metadata is incomplete")
    if (
        input_record.source_step_name is None
        or input_record.source_output_name is None
        or input_record.source_address is None
    ):
        raise ValidationError("reused workflow dependency source coordinates are incomplete")
    coordinate = RequestedOutputCoordinate(
        namespace=context,
        step_name=input_record.source_step_name,
        output_name=input_record.source_output_name,
        address=input_record.source_address,
    )
    try:
        expected_request_bundle_digest = reused_projection_identities[coordinate]
    except KeyError as exc:
        raise ValidationError(
            "reused workflow dependency source projection is missing"
        ) from exc
    artifact = _dependency_source_artifact(
        conn,
        input_record.registry_source_artifact_id,
    )
    _validated_stored_request_bundle_projection(
        conn,
        expected_request_bundle_digest,
    )
    if artifact.context != context:
        raise ValidationError("reused workflow dependency source context mismatch")
    if artifact.origin != "workflow_output":
        raise ValidationError("reused workflow dependency source is not a workflow output")
    if (
        artifact.step_name != input_record.source_step_name
        or artifact.output_name != input_record.source_output_name
        or artifact.address != input_record.source_address
    ):
        raise ValidationError("reused workflow dependency source coordinates mismatch")
    if (
        artifact.request_bundle_digest != expected_request_bundle_digest
        or artifact.extension != input_record.source_extension
    ):
        raise ValidationError("reused workflow dependency source identity mismatch")
    if not artifact.is_published or artifact.published_path != artifact.path:
        raise ValidationError("reused workflow dependency source publication mismatch")
    if not artifact.path.endswith(artifact.extension):
        raise ValidationError("reused workflow dependency source path extension mismatch")
    if not is_valid_digest(artifact.content_digest):
        raise ValidationError("reused workflow dependency source digest is invalid")
    source_path = _runtime_relative_file_path(runtime_root, artifact.path)
    outputs_root = (runtime_root / "outputs").resolve()
    if not _path_contains_or_same(outputs_root, source_path):
        raise ValidationError(
            "reused workflow dependency source path must stay inside outputs/"
        )
    if not source_path.is_file():
        raise ValidationError("reused workflow dependency source file is missing")
    if source_path.stat().st_size != artifact.file_size:
        raise ValidationError("reused workflow dependency source file size mismatch")
    if not _workflow_artifact_dependencies_match_registry(
        conn,
        artifact_id=artifact.artifact_id,
        expected_context=context,
        visited=set(),
    ):
        raise ValidationError("reused workflow dependency source lineage is stale")


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
    run_id: int,
    rows: tuple[RunManifestBindingRow, ...],
) -> None:
    conn.executemany(
        """
        INSERT INTO run_manifest_bindings (
            run_id, step_name, manifest_usage_role, manifest_name,
            manifest_value_schema, manifest_digest
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            (
                run_id,
                row.step_name,
                row.manifest_usage_role,
                row.manifest_name,
                row.manifest_value_schema,
                row.manifest_digest,
            )
            for row in rows
        ),
    )


def _insert_run_execution_population(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    row: RunExecutionPopulationRow,
) -> None:
    conn.execute(
        """
        INSERT INTO run_execution_population (
            run_id, manifest_name, manifest_value_schema, manifest_digest
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            run_id,
            row.manifest_name,
            row.manifest_value_schema,
            row.manifest_digest,
        ),
    )


def _resolved_membership_artifacts(
    conn: sqlite3.Connection,
    *,
    intents: tuple[MembershipIntent, ...],
    artifact_ids: dict[tuple[str, str, str], int],
) -> dict[tuple[str, str, str, str, str], int]:
    """Resolve each membership coordinate to its artifact without publishing."""
    resolved: dict[tuple[str, str, str, str, str], int] = {}
    for intent in intents:
        row = intent.row
        if intent.existing_artifact_id is None:
            try:
                artifact_id = artifact_ids[(row.step_name, row.output_name, row.address)]
            except KeyError as exc:
                raise ValidationError(
                    "fresh membership intent has no recorded artifact"
                ) from exc
        else:
            _validate_positive_id(
                intent.existing_artifact_id,
                label="membership artifact id",
            )
            artifact_id = _validated_existing_artifact_id(
                conn,
                artifact_id=intent.existing_artifact_id,
                context=row.context,
                step_name=row.step_name,
                output_name=row.output_name,
                address=row.address,
                path=row.path,
                content_digest=row.output_digest,
                output_hash=row.output_hash,
            )
        resolved[
            (
                row.context,
                row.workflow_name,
                row.step_name,
                row.output_name,
                row.address,
            )
        ] = artifact_id
    return resolved


def _insert_memberships(
    conn: sqlite3.Connection,
    *,
    intents: tuple[MembershipIntent, ...],
    artifact_ids: dict[tuple[str, str, str], int],
) -> dict[tuple[str, str, str, str, str], int]:
    accepted = _resolved_membership_artifacts(
        conn,
        intents=intents,
        artifact_ids=artifact_ids,
    )
    for intent in intents:
        row = intent.row
        conn.execute(
            """
            INSERT INTO published_outputs (
                context, workflow_name, step_name, output_name, address, path,
                output_digest, output_hash, artifact_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.context,
                row.workflow_name,
                row.step_name,
                row.output_name,
                row.address,
                row.path,
                row.output_digest,
                row.output_hash,
                accepted[
                    (
                        row.context,
                        row.workflow_name,
                        row.step_name,
                        row.output_name,
                        row.address,
                    )
                ],
            ),
        )
    return accepted


def _resolution_summary_json(
    intents: tuple[SelectedOutputResolutionIntent, ...],
    *,
    artifact_ids: dict[tuple[str, str, str], int] | None,
    conn: sqlite3.Connection | None = None,
) -> str:
    selected_outputs: list[dict[str, Any]] = []
    seen_coordinates: set[tuple[str, str, str, str, str]] = set()
    for intent in sorted(
        intents,
        key=lambda value: (
            value.context,
            value.workflow_name,
            value.step_name,
            value.output_name,
            value.address,
        ),
    ):
        coordinate = (
            intent.context,
            intent.workflow_name,
            intent.step_name,
            intent.output_name,
            intent.address,
        )
        if coordinate in seen_coordinates:
            raise ValidationError("selected-output resolution coordinate is duplicated")
        seen_coordinates.add(coordinate)
        if intent.outcome not in {None, "generated", "reused"}:
            raise ValidationError("selected-output resolution outcome is invalid")
        resolution: dict[str, Any] | None = None
        if artifact_ids is not None and intent.outcome is not None:
            if intent.outcome == "generated":
                if intent.existing_artifact_id is not None:
                    raise ValidationError(
                        "generated selected-output resolution cannot name an existing artifact"
                    )
                try:
                    artifact_id = artifact_ids[
                        (intent.step_name, intent.output_name, intent.address)
                    ]
                except KeyError as exc:
                    raise ValidationError(
                        "generated selected-output resolution has no recorded artifact"
                    ) from exc
            else:
                if intent.existing_artifact_id is None:
                    raise ValidationError(
                        "reused selected-output resolution is missing its artifact"
                    )
                _validate_positive_id(
                    intent.existing_artifact_id,
                    label="selected-output artifact id",
                )
                if conn is None:
                    raise ValidationError(
                        "reused selected-output resolution requires a registry transaction"
                    )
                artifact_id = _validated_existing_artifact_id(
                    conn,
                    artifact_id=intent.existing_artifact_id,
                    context=intent.context,
                    step_name=intent.step_name,
                    output_name=intent.output_name,
                    address=intent.address,
                )
            resolution = {"artifact_id": artifact_id, "outcome": intent.outcome}
        selected_outputs.append(
            {
                "context": intent.context,
                "workflow_name": intent.workflow_name,
                "step_name": intent.step_name,
                "output_name": intent.output_name,
                "address": intent.address,
                "resolution": resolution,
            }
        )
    return _compact_json(
        {
            "schema_version": 1,
            "forced": False,
            "all_selected_resolved": bool(selected_outputs)
            and all(item["resolution"] is not None for item in selected_outputs),
            "selected_outputs": selected_outputs,
        }
    )


def _validated_existing_artifact_id(
    conn: sqlite3.Connection,
    *,
    artifact_id: int,
    context: str,
    step_name: str,
    output_name: str,
    address: str,
    path: str | None = None,
    content_digest: str | None = None,
    output_hash: str | None = None,
) -> int:
    row = conn.execute(
        """
        SELECT path, content_digest, output_hash
        FROM artifacts
        WHERE artifact_id = ?
          AND origin = 'workflow_output'
          AND context = ?
          AND step_name = ?
          AND output_name = ?
          AND address = ?
          AND is_published = 1
        """,
        (artifact_id, context, step_name, output_name, address),
    ).fetchone()
    if row is None:
        raise ValidationError("existing artifact intent does not name a published artifact")
    actual_digest = str(row[1])
    actual_output_hash = str(row[2])
    if not is_valid_digest(actual_digest):
        raise ValidationError("existing membership artifact digest is invalid")
    try:
        actual_output_hash = validate_hash_alias(actual_output_hash)
    except ValidationError as exc:
        raise ValidationError("existing membership artifact hash is invalid") from exc
    if actual_output_hash != short_hash(actual_digest):
        raise ValidationError("existing membership artifact hash does not match digest")
    expected = (path, content_digest, output_hash)
    actual = (str(row[0]), actual_digest, actual_output_hash)
    if any(value is not None for value in expected) and actual != expected:
        raise ValidationError("existing membership intent does not match its artifact")
    return artifact_id


def _environment_observation_json(observation: EnvironmentObservationV1) -> str:
    if not isinstance(observation, EnvironmentObservationV1):
        raise ValidationError("environment observation must use the V1 contract")
    if observation.profile_version != 1:
        raise ValidationError("environment observation profile version must be 1")
    values = (
        observation.nipact_version,
        observation.python_version,
        observation.platform,
        observation.snakemake_version,
    )
    if any(type(value) is not str or not value for value in values):
        raise ValidationError("environment observation values must be non-empty strings")
    return _compact_json(
        {
            "profile_version": observation.profile_version,
            "nipact_version": observation.nipact_version,
            "python_version": observation.python_version,
            "platform": observation.platform,
            "snakemake_version": observation.snakemake_version,
        }
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
            membership_workflow_name,
            step_name,
            output_name,
            address,
            output_artifact_path,
            output_digest,
            output_hash,
            artifact_id,
            artifact_context,
            generating_workflow_name,
            artifact_step_name,
            artifact_output_name,
            artifact_address,
            artifact_origin,
            artifact_is_published,
            artifact_published_path,
            artifact_content_digest,
            artifact_output_hash,
            request_bundle_digest,
            artifact_extension,
        ) = row
        if not all(
            isinstance(value, str) and value
            for value in (
                membership_workflow_name,
                step_name,
                output_name,
                address,
                generating_workflow_name,
            )
        ):
            raise ValidationError("registry.db published output row has invalid identity")
        try:
            address = validate_path_token(address, label="published output address")
        except ValidationError as exc:
            raise ValidationError("registry.db published output address is invalid") from exc
        if membership_workflow_name not in loaded_workflow_project.workflows:
            raise ValidationError("registry.db published output references unknown workflow")
        if step_name not in workflow_steps[membership_workflow_name]:
            raise ValidationError("registry.db published output references unknown workflow step")
        step = loaded_workflow_project.steps.get(step_name)
        if step is None or output_name not in step.outputs:
            raise ValidationError("registry.db published output references unknown step output")
        declared_extension = step.outputs[output_name].extension
        if artifact_extension != declared_extension:
            raise ValidationError("registry.db published output extension is invalid")
        if not is_valid_digest(request_bundle_digest):
            raise ValidationError("registry.db published output request digest is invalid")
        if not is_valid_digest(output_digest):
            raise ValidationError("registry.db published output digest is invalid")
        try:
            output_hash = validate_hash_alias(output_hash)
        except ValidationError as exc:
            raise ValidationError("registry.db published output hash is invalid") from exc
        if output_hash != short_hash(output_digest):
            raise ValidationError("registry.db published output hash does not match digest")
        expected_path = canonical_output_path(
            context=context,
            step_name=step_name,
            address=address,
            request_bundle_digest=request_bundle_digest,
            output_name=output_name,
            output_hash=output_hash,
            declared_extension=declared_extension,
        )
        _resolve_published_output_path(
            runtime_root,
            output_artifact_path,
            expected_path=expected_path,
        )
        if (
            artifact_context != context
            or artifact_step_name != step_name
            or artifact_output_name != output_name
            or artifact_address != address
            or artifact_origin != "workflow_output"
            or artifact_is_published != 1
            or artifact_published_path != output_artifact_path
            or artifact_content_digest != output_digest
            or artifact_output_hash != output_hash
        ):
            raise ValidationError(
                "registry.db published output does not match its artifact"
            )
        if type(artifact_id) is not int or artifact_id <= 0:
            raise ValidationError("registry.db published output artifact id is invalid")


def _validate_accepted_workflow_output_rows(
    rows: list[tuple[Any, ...]],
    *,
    context: str,
    runtime_root: Path,
    loaded_workflow_project: Any,
    verified_occurrences: set[tuple[Path, str]],
) -> None:
    for row in rows:
        _validate_accepted_workflow_output_row(
            row,
            context=context,
            runtime_root=runtime_root,
            loaded_workflow_project=loaded_workflow_project,
            verified_occurrences=verified_occurrences,
        )


def _validate_accepted_workflow_output_row(
    row: tuple[Any, ...],
    *,
    context: str,
    runtime_root: Path,
    loaded_workflow_project: Any,
    verified_occurrences: set[tuple[Path, str]],
) -> None:
    (
        artifact_id,
        artifact_context,
        generating_workflow_name,
        step_name,
        output_name,
        address,
        origin,
        is_published,
        artifact_path,
        published_path,
        content_digest,
        output_hash,
        file_size,
        extension,
        request_bundle_digest,
    ) = row
    if type(artifact_id) is not int or artifact_id <= 0:
        raise ValidationError("registry.db accepted artifact id is invalid")
    if not all(
        isinstance(value, str) and value
        for value in (generating_workflow_name, step_name, output_name, address)
    ):
        raise ValidationError("registry.db accepted artifact has invalid identity")
    try:
        address = validate_path_token(address, label="accepted artifact address")
    except ValidationError as exc:
        raise ValidationError("registry.db accepted artifact address is invalid") from exc
    step = loaded_workflow_project.steps.get(step_name)
    if step is None or output_name not in step.outputs:
        raise ValidationError("registry.db accepted artifact references unknown step output")
    if (
        artifact_context != context
        or origin != "workflow_output"
        or is_published != 1
        or artifact_path != published_path
    ):
        raise ValidationError("registry.db accepted artifact row is inconsistent")
    if not is_valid_digest(content_digest):
        raise ValidationError("registry.db accepted artifact digest is invalid")
    try:
        output_hash = validate_hash_alias(output_hash)
    except ValidationError as exc:
        raise ValidationError("registry.db accepted artifact hash is invalid") from exc
    if output_hash != short_hash(content_digest):
        raise ValidationError("registry.db accepted artifact hash does not match digest")
    if type(file_size) is not int or file_size < 0:
        raise ValidationError("registry.db accepted artifact file size is invalid")
    if not is_valid_digest(request_bundle_digest):
        raise ValidationError("registry.db accepted artifact request digest is invalid")
    declared_extension = step.outputs[output_name].extension
    if extension != declared_extension:
        raise ValidationError("registry.db accepted artifact extension is invalid")
    expected_path = canonical_output_path(
        context=context,
        step_name=step_name,
        address=address,
        request_bundle_digest=request_bundle_digest,
        output_name=output_name,
        output_hash=output_hash,
        declared_extension=declared_extension,
    )
    resolved_path = _resolve_published_output_path(
        runtime_root,
        published_path,
        expected_path=expected_path,
    )
    if resolved_path.stat().st_size != file_size:
        raise ValidationError("published output artifact file size mismatch")
    occurrence = (resolved_path, content_digest)
    if occurrence in verified_occurrences:
        return
    if sha256_file_digest(resolved_path) != content_digest:
        raise ValidationError("published output artifact digest mismatch")
    verified_occurrences.add(occurrence)


def _resolve_published_output_path(
    runtime_root: Path,
    raw_path: Any,
    *,
    expected_path: str,
) -> Path:
    if not isinstance(raw_path, str):
        raise ValidationError("published output artifact path must be a string")
    relative_path = Path(raw_path).expanduser()
    if relative_path.is_absolute():
        raise ValidationError("published output artifact path must be relative to runtime dir")
    if relative_path.parts[: len(CANONICAL_OUTPUT_ROOT.parts)] != (
        CANONICAL_OUTPUT_ROOT.parts
    ):
        raise ValidationError("published output artifact path must be under outputs/v1/")
    if raw_path != expected_path:
        raise ValidationError("published output artifact path does not match registry identity")
    resolved = (runtime_root / relative_path).resolve()
    if not _path_contains_or_same(runtime_root, resolved):
        raise ValidationError("published output artifact path must stay inside runtime dir")
    outputs_root = (runtime_root / CANONICAL_OUTPUT_ROOT).resolve()
    if not _path_contains_or_same(outputs_root, resolved):
        raise ValidationError("published output artifact path must stay inside outputs/v1/")
    if not resolved.is_file():
        raise ValidationError(f"missing published output artifact: {raw_path}")
    return resolved


_V18_CORE_SCHEMA_SQL = f"""
        CREATE TABLE IF NOT EXISTS contexts (
            context TEXT PRIMARY KEY,
            runtime_path TEXT NOT NULL,
            storage_layout_version INTEGER NOT NULL DEFAULT 1 CHECK(
                storage_layout_version = {STORAGE_LAYOUT_VERSION}
            )
        );

        CREATE TABLE IF NOT EXISTS manifest_values (
            value_schema TEXT NOT NULL,
            manifest_digest TEXT NOT NULL CHECK(
                length(manifest_digest) = 64
                AND manifest_digest NOT GLOB '*[^0-9a-f]*'
            ),
            canonical_body TEXT NOT NULL CHECK(length(canonical_body) > 0),
            entity_count INTEGER NOT NULL CHECK(entity_count > 0),
            PRIMARY KEY (value_schema, manifest_digest)
        );

        CREATE TABLE IF NOT EXISTS manifest_declarations (
            context TEXT NOT NULL REFERENCES contexts(context) ON DELETE CASCADE,
            manifest_name TEXT NOT NULL,
            declared_path TEXT NOT NULL,
            last_validated_manifest_value_schema TEXT NOT NULL,
            last_validated_manifest_digest TEXT NOT NULL,
            PRIMARY KEY (context, manifest_name),
            FOREIGN KEY (
                last_validated_manifest_value_schema,
                last_validated_manifest_digest
            ) REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT
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
            resolution_summary_json TEXT NOT NULL,
            environment_observation_json TEXT NOT NULL,
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

        CREATE TABLE IF NOT EXISTS request_bundle_projections (
            request_bundle_digest TEXT PRIMARY KEY
                CHECK(
                    length(request_bundle_digest) = 64
                    AND request_bundle_digest NOT GLOB '*[^0-9a-f]*'
                ),
            projection_json TEXT NOT NULL
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
            source_scope TEXT CHECK(
                source_scope IS NULL OR source_scope IN ('global', 'entity')
            ),
            source_name TEXT,
            source_entity_id TEXT,
            source_st_dev INTEGER CHECK(source_st_dev IS NULL OR source_st_dev >= 0),
            source_st_ino INTEGER CHECK(source_st_ino IS NULL OR source_st_ino >= 0),
            source_st_size INTEGER CHECK(source_st_size IS NULL OR source_st_size >= 0),
            source_st_mtime_ns INTEGER CHECK(
                source_st_mtime_ns IS NULL OR source_st_mtime_ns >= 0
            ),
            source_st_ctime_ns INTEGER CHECK(
                source_st_ctime_ns IS NULL OR source_st_ctime_ns >= 0
            ),
            request_bundle_digest TEXT
                REFERENCES request_bundle_projections(request_bundle_digest)
                ON DELETE RESTRICT,
            created_at TEXT NOT NULL,
            CHECK (
                (
                    origin = 'workflow_output'
                    AND request_bundle_digest IS NOT NULL
                )
                OR (
                    origin = 'source'
                    AND request_bundle_digest IS NULL
                )
            ),
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
                    AND source_scope IS NOT NULL
                    AND source_name IS NOT NULL
                    AND (
                        (source_scope = 'global' AND source_entity_id IS NULL)
                        OR
                        (source_scope = 'entity' AND source_entity_id IS NOT NULL)
                    )
                    AND source_st_dev IS NOT NULL
                    AND source_st_ino IS NOT NULL
                    AND source_st_size IS NOT NULL
                    AND source_st_mtime_ns IS NOT NULL
                    AND source_st_ctime_ns IS NOT NULL
                )
            ),
            CHECK (
                origin = 'source'
                OR (
                    source_scope IS NULL
                    AND source_name IS NULL
                    AND source_entity_id IS NULL
                    AND source_st_dev IS NULL
                    AND source_st_ino IS NULL
                    AND source_st_size IS NULL
                    AND source_st_mtime_ns IS NULL
                    AND source_st_ctime_ns IS NULL
                )
            )
        );

        CREATE UNIQUE INDEX IF NOT EXISTS artifacts_source_global_coordinate_uq
            ON artifacts(context, source_name)
            WHERE origin = 'source' AND source_scope = 'global';
        CREATE UNIQUE INDEX IF NOT EXISTS artifacts_source_entity_coordinate_uq
            ON artifacts(context, source_name, source_entity_id)
            WHERE origin = 'source' AND source_scope = 'entity';
        CREATE UNIQUE INDEX IF NOT EXISTS artifacts_workflow_output_uq
            ON artifacts(run_id, step_name, output_name, address)
            WHERE origin = 'workflow_output';
        CREATE INDEX IF NOT EXISTS artifacts_path_idx
            ON artifacts(context, path);
        CREATE INDEX IF NOT EXISTS artifacts_selected_lookup_idx
            ON artifacts(context, workflow_name, step_name, output_name, address)
            WHERE is_selected_output = 1;
        CREATE INDEX IF NOT EXISTS artifacts_reuse_lookup_idx
            ON artifacts(
                context, step_name, address, request_bundle_digest, run_id
            )
            WHERE origin = 'workflow_output' AND is_published = 1;

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
            source_scope TEXT CHECK(
                source_scope IS NULL OR source_scope IN ('global', 'entity')
            ),
            source_name TEXT,
            source_entity_id TEXT,
            source_occurrence_path TEXT,
            dependency_set_id TEXT,
            manifest_value_schema TEXT,
            manifest_digest TEXT,
            edge_cardinality INTEGER CHECK(
                edge_cardinality IS NULL OR edge_cardinality >= 0
            ),
            CHECK (
                (manifest_value_schema IS NULL) = (manifest_digest IS NULL)
            ),
            CHECK (
                (
                    source_scope IS NULL
                    AND source_name IS NULL
                    AND source_entity_id IS NULL
                    AND source_occurrence_path IS NULL
                )
                OR
                (
                    source_scope = 'global'
                    AND source_name IS NOT NULL
                    AND source_entity_id IS NULL
                    AND source_occurrence_path IS NOT NULL
                )
                OR
                (
                    source_scope = 'entity'
                    AND source_name IS NOT NULL
                    AND source_entity_id IS NOT NULL
                    AND source_occurrence_path IS NOT NULL
                )
            ),
            FOREIGN KEY (manifest_value_schema, manifest_digest)
                REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT,
            PRIMARY KEY (
                dependent_artifact_id, source_artifact_id, input_path, binding_name
            )
        );

        CREATE TABLE IF NOT EXISTS run_execution_population (
            run_id INTEGER PRIMARY KEY
                REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            manifest_name TEXT NOT NULL,
            manifest_value_schema TEXT NOT NULL,
            manifest_digest TEXT NOT NULL,
            FOREIGN KEY (manifest_value_schema, manifest_digest)
                REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS run_manifest_bindings (
            run_id INTEGER NOT NULL
                REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
            step_name TEXT NOT NULL,
            manifest_usage_role TEXT NOT NULL,
            manifest_name TEXT NOT NULL,
            manifest_value_schema TEXT NOT NULL,
            manifest_digest TEXT NOT NULL,
            PRIMARY KEY (run_id, step_name, manifest_usage_role),
            FOREIGN KEY (manifest_value_schema, manifest_digest)
                REFERENCES manifest_values(value_schema, manifest_digest)
                ON DELETE RESTRICT
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
            artifact_id INTEGER NOT NULL
                REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
            PRIMARY KEY (context, workflow_name, step_name, output_name, address)
        );

        CREATE INDEX IF NOT EXISTS published_outputs_artifact_id_idx
            ON published_outputs(artifact_id);
"""


_V19_ADDITIVE_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE specification_snapshots (
        snapshot_digest TEXT PRIMARY KEY CHECK(
            length(snapshot_digest) = 64
            AND snapshot_digest NOT GLOB '*[^0-9a-f]*'
        ),
        context TEXT NOT NULL
            REFERENCES contexts(context) ON DELETE RESTRICT,
        canonical_bytes BLOB NOT NULL CHECK(
            typeof(canonical_bytes) = 'blob'
            AND length(canonical_bytes) > 0
        )
    )
    """,
    """
    CREATE TABLE specification_members (
        snapshot_digest TEXT NOT NULL
            REFERENCES specification_snapshots(snapshot_digest) ON DELETE RESTRICT,
        member_key TEXT NOT NULL CHECK(length(trim(member_key)) > 0),
        row_digest TEXT NOT NULL CHECK(
            length(row_digest) = 64
            AND row_digest NOT GLOB '*[^0-9a-f]*'
        ),
        disposition TEXT NOT NULL CHECK(
            disposition IN ('included', 'excluded')
        ),
        exclusion_reason TEXT,
        PRIMARY KEY (snapshot_digest, member_key),
        UNIQUE (snapshot_digest, row_digest),
        CHECK (
            (disposition = 'included' AND exclusion_reason IS NULL)
            OR
            (
                disposition = 'excluded'
                AND exclusion_reason IS NOT NULL
                AND length(trim(exclusion_reason)) > 0
            )
        )
    )
    """,
    """
    CREATE TABLE specification_snapshot_manifest_values (
        snapshot_digest TEXT NOT NULL
            REFERENCES specification_snapshots(snapshot_digest) ON DELETE RESTRICT,
        value_schema TEXT NOT NULL CHECK(length(trim(value_schema)) > 0),
        manifest_digest TEXT NOT NULL,
        PRIMARY KEY (snapshot_digest, value_schema, manifest_digest),
        FOREIGN KEY (value_schema, manifest_digest)
            REFERENCES manifest_values(value_schema, manifest_digest)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE INDEX specification_snapshot_manifest_values_value_idx
        ON specification_snapshot_manifest_values (
            value_schema, manifest_digest, snapshot_digest
        )
    """,
    """
    CREATE TABLE specification_expected_results (
        snapshot_digest TEXT NOT NULL,
        member_key TEXT NOT NULL,
        role TEXT NOT NULL CHECK(length(trim(role)) > 0),
        step_name TEXT NOT NULL CHECK(length(trim(step_name)) > 0),
        output_name TEXT NOT NULL CHECK(length(trim(output_name)) > 0),
        address TEXT NOT NULL CHECK(length(trim(address)) > 0),
        PRIMARY KEY (snapshot_digest, member_key, role, address),
        UNIQUE (
            snapshot_digest, member_key, step_name, output_name, address
        ),
        FOREIGN KEY (snapshot_digest, member_key)
            REFERENCES specification_members(snapshot_digest, member_key)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE specification_member_attempts (
        attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_digest TEXT NOT NULL,
        member_key TEXT NOT NULL,
        started_at TEXT NOT NULL CHECK(length(trim(started_at)) > 0),
        finished_at TEXT CHECK(
            finished_at IS NULL OR length(trim(finished_at)) > 0
        ),
        outcome TEXT CHECK(
            outcome IS NULL OR outcome IN ('failed', 'partial', 'complete')
        ),
        selecting_run_id INTEGER UNIQUE
            REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
        failure_stage TEXT CHECK(
            failure_stage IS NULL
            OR (
                length(trim(failure_stage)) > 0
                AND length(failure_stage) <= 64
            )
        ),
        failure_summary TEXT CHECK(
            failure_summary IS NULL
            OR (
                length(trim(failure_summary)) > 0
                AND length(failure_summary) <= 4096
            )
        ),
        UNIQUE (attempt_id, snapshot_digest, member_key),
        FOREIGN KEY (snapshot_digest, member_key)
            REFERENCES specification_members(snapshot_digest, member_key)
            ON DELETE RESTRICT,
        CHECK (
            (
                outcome IS NULL
                AND finished_at IS NULL
                AND selecting_run_id IS NULL
                AND failure_stage IS NULL
                AND failure_summary IS NULL
            )
            OR
            (
                outcome IN ('partial', 'complete')
                AND finished_at IS NOT NULL
                AND selecting_run_id IS NOT NULL
                AND failure_stage IS NULL
                AND failure_summary IS NULL
            )
            OR
            (
                outcome = 'failed'
                AND finished_at IS NOT NULL
                AND failure_stage IS NOT NULL
                AND failure_summary IS NOT NULL
            )
        )
    )
    """,
    """
    CREATE INDEX specification_member_attempts_member_idx
        ON specification_member_attempts (
            snapshot_digest, member_key, attempt_id
        )
    """,
    """
    CREATE TABLE specification_attempt_results (
        attempt_id INTEGER NOT NULL,
        snapshot_digest TEXT NOT NULL,
        member_key TEXT NOT NULL,
        role TEXT NOT NULL,
        address TEXT NOT NULL,
        artifact_id INTEGER NOT NULL
            REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
        PRIMARY KEY (attempt_id, role, address),
        FOREIGN KEY (attempt_id, snapshot_digest, member_key)
            REFERENCES specification_member_attempts(
                attempt_id, snapshot_digest, member_key
            ) ON DELETE RESTRICT,
        FOREIGN KEY (snapshot_digest, member_key, role, address)
            REFERENCES specification_expected_results(
                snapshot_digest, member_key, role, address
            ) ON DELETE RESTRICT
    )
    """,
    """
    CREATE INDEX specification_attempt_results_artifact_idx
        ON specification_attempt_results (artifact_id, attempt_id)
    """,
)


def _create_schema(conn: sqlite3.Connection) -> None:
    version = _schema_version(conn)
    if version == REGISTRY_SCHEMA_VERSION:
        _validate_exact_registry_structure(
            conn,
            expected_version=REGISTRY_SCHEMA_VERSION,
        )
        return
    if version != 0:
        _validate_schema_version(conn)
    if _has_user_tables(conn):
        raise ValidationError(
            "registry.db schema version is incompatible: "
            f"expected empty or {REGISTRY_SCHEMA_VERSION}, found 0"
        )

    additive_sql = "\n".join(
        f"{statement.strip()};" for statement in _V19_ADDITIVE_SCHEMA_STATEMENTS
    )
    creation_sql = (
        "BEGIN IMMEDIATE;\n"
        f"{_V18_CORE_SCHEMA_SQL}\n"
        f"{additive_sql}\n"
        f"PRAGMA user_version = {REGISTRY_SCHEMA_VERSION};\n"
        "COMMIT;"
    )
    try:
        conn.executescript(creation_sql)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    _validate_exact_registry_structure(
        conn,
        expected_version=REGISTRY_SCHEMA_VERSION,
    )


def registry_schema_signature(conn: sqlite3.Connection) -> dict[str, object]:
    """Return the normalized physical registry schema used by migration checks."""
    objects = _query_dict_rows(
        conn,
        """
        SELECT type, name, tbl_name
        FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
        ORDER BY type, name
        """,
    )
    tables: dict[str, object] = {}
    table_names = sorted(
        str(item["name"]) for item in objects if item["type"] == "table"
    )
    for table_name in table_names:
        quoted_table = _quote_sql_identifier(table_name)
        tables[table_name] = {
            "columns": _query_dict_rows(
                conn,
                f"PRAGMA table_xinfo({quoted_table})",
            ),
            "foreign_keys": _foreign_key_signatures(
                conn,
                table_name=table_name,
            ),
            "indexes": _index_signatures(conn, table_name=table_name),
        }
    sql = {
        str(row["name"]): _normalize_schema_sql(str(row["sql"]))
        for row in _query_dict_rows(
            conn,
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
              AND type IN ('table', 'index')
              AND sql IS NOT NULL
            ORDER BY name
            """,
        )
    }
    return {
        "objects": objects,
        "sql": sql,
        "tables": tables,
        "user_version": _schema_version(conn),
    }


def registry_schema_signature_digest(signature: dict[str, object]) -> str:
    """Return the canonical digest for a normalized registry schema signature."""
    canonical_bytes = json.dumps(
        signature,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return sha256_digest(canonical_bytes)


def migrate_registry_db(
    path: Path,
    *,
    context: str,
    runtime_root: Path,
    fault_hook: Callable[[str], None] | None = None,
) -> RegistryMigrationResult:
    """Explicitly migrate one exact schema-18 registry to schema 19."""
    context = validate_path_token(context, label="context")
    runtime_root = runtime_root.expanduser().resolve()
    if not runtime_root.is_dir():
        raise ValidationError("runtime_root must be an existing directory")
    registry_relative_path = Path(REGISTRY_DB_PATH)
    database_dir = runtime_root / registry_relative_path.parent
    if database_dir.is_symlink() or not database_dir.is_dir():
        raise ValidationError(
            "registry migration database directory must be a real directory"
        )
    resolved_database_dir = database_dir.resolve()
    if not _path_contains_or_same(runtime_root, resolved_database_dir):
        raise ValidationError(
            "registry migration database directory must stay inside runtime_root"
        )
    requested_path = path.expanduser()
    if not requested_path.is_absolute():
        requested_path = Path.cwd() / requested_path
    registry_path = requested_path.parent.resolve() / requested_path.name
    expected_path = resolved_database_dir / registry_relative_path.name
    if registry_path != expected_path:
        raise ValidationError("registry migration registry path is incompatible")
    if registry_path.is_symlink() or not registry_path.is_file():
        raise ValidationError("registry migration registry path must be a real file")

    version = _read_registry_schema_version(registry_path)
    if version == REGISTRY_SCHEMA_VERSION:
        _validate_migration_registry_file(
            registry_path,
            expected_version=REGISTRY_SCHEMA_VERSION,
            context=context,
            runtime_root=runtime_root,
        )
        return RegistryMigrationResult(
            context=context,
            registry_path=registry_path,
            status="already-current",
        )
    _require_supported_migration_version(version)

    from .runtime_lock import acquire_mutating_runtime_lock

    with acquire_mutating_runtime_lock(runtime_root):
        version = _read_registry_schema_version(registry_path)
        if version == REGISTRY_SCHEMA_VERSION:
            _validate_migration_registry_file(
                registry_path,
                expected_version=REGISTRY_SCHEMA_VERSION,
                context=context,
                runtime_root=runtime_root,
            )
            return RegistryMigrationResult(
                context=context,
                registry_path=registry_path,
                status="already-current",
            )
        _require_supported_migration_version(version)
        return _migrate_registry_v18_locked(
            registry_path,
            context=context,
            runtime_root=runtime_root,
            fault_hook=fault_hook,
        )


def _migrate_registry_v18_locked(
    registry_path: Path,
    *,
    context: str,
    runtime_root: Path,
    fault_hook: Callable[[str], None] | None,
) -> RegistryMigrationResult:
    backup_path = registry_path.with_name(REGISTRY_V18_BACKUP_FILENAME)
    live_conn = _open_migration_connection(registry_path)
    backup_validated = False
    try:
        _validate_migration_preflight(
            live_conn,
            expected_version=REGISTRY_MIGRATION_SOURCE_VERSION,
            context=context,
            runtime_root=runtime_root,
        )
        if backup_path.exists() or backup_path.is_symlink():
            raise ValidationError(
                f"registry migration backup already exists: {backup_path}"
            )
        try:
            _create_registry_migration_backup(live_conn, backup_path)
        except Exception as exc:
            if backup_path.exists() or backup_path.is_symlink():
                _remove_created_backup(backup_path)
            raise ValidationError(
                f"registry migration backup creation failed: {exc}"
            ) from exc
        try:
            _validate_migration_registry_file(
                backup_path,
                expected_version=REGISTRY_MIGRATION_SOURCE_VERSION,
                context=context,
                runtime_root=runtime_root,
            )
            backup_validated = True
        except Exception as exc:
            if backup_path.exists() or backup_path.is_symlink():
                _remove_created_backup(backup_path)
            raise ValidationError(
                f"registry migration backup validation failed: {exc}"
            ) from exc

        _invoke_migration_fault(fault_hook, "after_backup")
        live_conn.execute("BEGIN IMMEDIATE")
        for position, statement in enumerate(
            _V19_ADDITIVE_SCHEMA_STATEMENTS,
            start=1,
        ):
            live_conn.execute(statement)
            _invoke_migration_fault(fault_hook, f"after_ddl_{position:02d}")
        live_conn.execute(f"PRAGMA user_version = {REGISTRY_SCHEMA_VERSION}")
        _invoke_migration_fault(fault_hook, "after_user_version")
        _validate_migration_preflight(
            live_conn,
            expected_version=REGISTRY_SCHEMA_VERSION,
            context=context,
            runtime_root=runtime_root,
        )
        _invoke_migration_fault(fault_hook, "before_commit")
        live_conn.commit()
        _invoke_migration_fault(fault_hook, "after_commit")
    except Exception as exc:
        if live_conn.in_transaction:
            live_conn.rollback()
        live_conn.close()
        if backup_validated and _is_exact_migration_registry(
            registry_path,
            expected_version=REGISTRY_SCHEMA_VERSION,
            context=context,
            runtime_root=runtime_root,
        ):
            raise ValidationError(
                "registry migration committed but completion reporting failed; "
                "inspect the live registry and use the validated backup for "
                f"manual recovery: {backup_path}"
            ) from exc
        if backup_validated:
            _validate_migration_registry_file(
                registry_path,
                expected_version=REGISTRY_MIGRATION_SOURCE_VERSION,
                context=context,
                runtime_root=runtime_root,
            )
            _require_equal_migration_contents(registry_path, backup_path)
            raise ValidationError(
                "registry migration failed before commit; live registry remains "
                f"schema 18: validated backup={backup_path}"
            ) from exc
        raise
    else:
        live_conn.close()

    try:
        _validate_migration_registry_file(
            registry_path,
            expected_version=REGISTRY_SCHEMA_VERSION,
            context=context,
            runtime_root=runtime_root,
        )
    except Exception as exc:
        raise ValidationError(
            "registry migration committed but completion reporting failed; "
            "inspect the live registry and use the validated backup for manual "
            f"recovery: {backup_path}"
        ) from exc
    return RegistryMigrationResult(
        context=context,
        registry_path=registry_path,
        status="migrated",
        from_schema=REGISTRY_MIGRATION_SOURCE_VERSION,
        to_schema=REGISTRY_SCHEMA_VERSION,
        backup_path=backup_path,
    )


def _read_registry_schema_version(path: Path) -> int:
    try:
        with _connect_readonly(path) as conn:
            return _schema_version(conn)
    except sqlite3.Error as exc:
        raise ValidationError(
            f"could not inspect registry schema version: {exc}"
        ) from exc


def _require_supported_migration_version(version: int) -> None:
    if version != REGISTRY_MIGRATION_SOURCE_VERSION:
        raise ValidationError(
            "registry migration supports only schema 18 to 19; "
            f"found {version}"
        )


def _open_migration_connection(path: Path) -> sqlite3.Connection:
    try:
        uri_path = quote(path.resolve().as_posix(), safe="/")
        conn = sqlite3.connect(f"file:{uri_path}?mode=rw", uri=True)
    except sqlite3.Error as exc:
        raise ValidationError(f"could not open database {path}: {exc}") from exc
    _set_foreign_keys(conn)
    return conn


def _create_registry_migration_backup(
    source_conn: sqlite3.Connection,
    backup_path: Path,
) -> None:
    try:
        backup_conn = sqlite3.connect(backup_path)
    except sqlite3.Error as exc:
        raise ValidationError(
            f"could not create registry migration backup: {exc}"
        ) from exc
    try:
        source_conn.backup(backup_conn)
    except sqlite3.Error as exc:
        raise ValidationError(
            f"could not create registry migration backup: {exc}"
        ) from exc
    finally:
        backup_conn.close()


def _remove_created_backup(path: Path) -> None:
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
    except OSError as exc:
        raise ValidationError(
            f"could not remove invalid registry migration backup: {path}"
        ) from exc


def _validate_migration_registry_file(
    path: Path,
    *,
    expected_version: int,
    context: str,
    runtime_root: Path,
) -> None:
    try:
        with _connect_readonly(path) as conn:
            _validate_migration_preflight(
                conn,
                expected_version=expected_version,
                context=context,
                runtime_root=runtime_root,
            )
    except sqlite3.Error as exc:
        raise ValidationError(f"registry migration validation failed: {exc}") from exc


def _validate_migration_preflight(
    conn: sqlite3.Connection,
    *,
    expected_version: int,
    context: str,
    runtime_root: Path,
) -> None:
    version = _schema_version(conn)
    if version != expected_version:
        if expected_version == REGISTRY_MIGRATION_SOURCE_VERSION:
            _require_supported_migration_version(version)
        raise ValidationError(
            "registry migration requires the exact V19 schema: "
            f"found schema {version}"
        )
    _validate_exact_registry_structure(conn, expected_version=expected_version)

    integrity_rows = [tuple(row) for row in conn.execute("PRAGMA integrity_check")]
    if integrity_rows != [("ok",)]:
        raise ValidationError(
            "registry migration preflight failed integrity_check: "
            f"{integrity_rows!r}"
        )
    foreign_key_rows = [
        tuple(row) for row in conn.execute("PRAGMA foreign_key_check")
    ]
    if foreign_key_rows:
        raise ValidationError(
            "registry migration preflight found foreign-key violations: "
            f"{foreign_key_rows!r}"
        )
    row = conn.execute(
        """
        SELECT runtime_path, storage_layout_version
        FROM contexts
        WHERE context = ?
        """,
        (context,),
    ).fetchone()
    if (
        row is None
        or Path(str(row[0])).expanduser().resolve() != runtime_root
        or row[1] != STORAGE_LAYOUT_VERSION
    ):
        raise ValidationError("registry migration context binding is incompatible")


def _validate_exact_registry_structure(
    conn: sqlite3.Connection,
    *,
    expected_version: int,
) -> None:
    expected = _expected_registry_schema_signature(expected_version)
    try:
        actual = registry_schema_signature(conn)
    except sqlite3.Error as exc:
        message = _exact_schema_requirement_message(expected_version)
        raise ValidationError(f"{message}: {exc}") from exc
    if actual != expected:
        raise ValidationError(_exact_schema_requirement_message(expected_version))


def _expected_registry_schema_signature(version: int) -> dict[str, object]:
    if version not in {
        REGISTRY_MIGRATION_SOURCE_VERSION,
        REGISTRY_SCHEMA_VERSION,
    }:
        raise ValueError(f"unsupported registry schema signature version: {version}")
    with sqlite3.connect(":memory:") as conn:
        conn.executescript(_V18_CORE_SCHEMA_SQL)
        if version == REGISTRY_SCHEMA_VERSION:
            for statement in _V19_ADDITIVE_SCHEMA_STATEMENTS:
                conn.execute(statement)
        conn.execute(f"PRAGMA user_version = {version}")
        signature = registry_schema_signature(conn)
    if version == REGISTRY_MIGRATION_SOURCE_VERSION:
        digest = registry_schema_signature_digest(signature)
        if digest != REGISTRY_V18_SCHEMA_SIGNATURE_SHA256:
            raise ValidationError(
                "registry migration requires the exact V18 schema: retained "
                "production schema does not match its frozen signature"
            )
    return signature


def _exact_schema_requirement_message(version: int) -> str:
    if version == REGISTRY_MIGRATION_SOURCE_VERSION:
        return "registry migration requires the exact V18 schema"
    return "registry migration requires the exact V19 schema"


def _is_exact_migration_registry(
    path: Path,
    *,
    expected_version: int,
    context: str,
    runtime_root: Path,
) -> bool:
    try:
        _validate_migration_registry_file(
            path,
            expected_version=expected_version,
            context=context,
            runtime_root=runtime_root,
        )
    except Exception:
        return False
    return True


def _require_equal_migration_contents(live_path: Path, backup_path: Path) -> None:
    if _migration_content_snapshot(live_path) != _migration_content_snapshot(
        backup_path
    ):
        raise ValidationError(
            "registry migration rollback did not preserve application rows"
        )


def _migration_content_snapshot(
    path: Path,
) -> tuple[
    dict[str, tuple[tuple[object, ...], ...]],
    tuple[tuple[object, ...], ...],
]:
    with _connect_readonly(path) as conn:
        table_names = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        )
        rows = {
            table_name: tuple(
                sorted(
                    (
                        tuple(row)
                        for row in conn.execute(
                            f"SELECT * FROM {_quote_sql_identifier(table_name)}"
                        )
                    ),
                    key=repr,
                )
            )
            for table_name in table_names
        }
        sequences = tuple(
            conn.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        )
    return rows, sequences


def _invoke_migration_fault(
    fault_hook: Callable[[str], None] | None,
    checkpoint: str,
) -> None:
    if fault_hook is not None:
        fault_hook(checkpoint)


def _normalize_schema_sql(value: str) -> str:
    return " ".join(value.split())


def _quote_sql_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _query_dict_rows(
    conn: sqlite3.Connection,
    statement: str,
) -> list[dict[str, object]]:
    cursor = conn.execute(statement)
    names = tuple(str(item[0]) for item in cursor.description or ())
    return [dict(zip(names, tuple(row), strict=True)) for row in cursor]


def _index_signatures(
    conn: sqlite3.Connection,
    *,
    table_name: str,
) -> list[dict[str, object]]:
    indexes: list[dict[str, object]] = []
    quoted_table = _quote_sql_identifier(table_name)
    for row in _query_dict_rows(conn, f"PRAGMA index_list({quoted_table})"):
        index_name = str(row["name"])
        quoted_index = _quote_sql_identifier(index_name)
        indexes.append(
            {
                "name": index_name,
                "unique": row["unique"],
                "origin": row["origin"],
                "partial": row["partial"],
                "xinfo": sorted(
                    _query_dict_rows(
                        conn,
                        f"PRAGMA index_xinfo({quoted_index})",
                    ),
                    key=lambda item: int(item["seqno"]),
                ),
            }
        )
    return sorted(indexes, key=lambda item: str(item["name"]))


def _foreign_key_signatures(
    conn: sqlite3.Connection,
    *,
    table_name: str,
) -> list[dict[str, object]]:
    quoted_table = _quote_sql_identifier(table_name)
    grouped: dict[int, list[dict[str, object]]] = {}
    for row in _query_dict_rows(conn, f"PRAGMA foreign_key_list({quoted_table})"):
        grouped.setdefault(int(row["id"]), []).append(row)

    foreign_keys: list[dict[str, object]] = []
    for rows in grouped.values():
        ordered = sorted(rows, key=lambda item: int(item["seq"]))
        first = ordered[0]
        foreign_keys.append(
            {
                "table": first["table"],
                "on_update": first["on_update"],
                "on_delete": first["on_delete"],
                "match": first["match"],
                "columns": [
                    {
                        "sequence": row["seq"],
                        "from": row["from"],
                        "to": row["to"],
                    }
                    for row in ordered
                ],
            }
        )
    return sorted(
        foreign_keys,
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )


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
        request_bundle_digest=row["request_bundle_digest"],
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
        source_scope=row["source_scope"],
        source_name=row["source_name"],
        source_entity_id=row["source_entity_id"],
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
        source_scope=row["source_scope"],
        source_name=row["source_name"],
        source_entity_id=row["source_entity_id"],
        source_occurrence_path=row["source_occurrence_path"],
        dependency_set_id=row["dependency_set_id"],
        manifest_value_schema=row["manifest_value_schema"],
        manifest_digest=row["manifest_digest"],
        edge_cardinality=_optional_int(row["edge_cardinality"]),
    )


def _registry_manifest_binding_from_row(
    row: sqlite3.Row,
) -> RegistryManifestBinding:
    value = _manifest_value_from_registry_row(row)
    return RegistryManifestBinding(
        run_id=int(row["run_id"]),
        context=str(row["context"]),
        workflow_name=str(row["workflow_name"]),
        step_name=str(row["step_name"]),
        manifest_usage_role=str(row["manifest_usage_role"]),
        manifest_name=str(row["manifest_name"]),
        manifest_value_schema=value.value_schema,
        manifest_digest=value.manifest_digest,
        manifest_hash=value.manifest_hash,
        entity_count=value.entity_count,
    )


def _registry_execution_population_from_row(
    row: sqlite3.Row,
) -> RegistryExecutionPopulation:
    value = _manifest_value_from_registry_row(row)
    return RegistryExecutionPopulation(
        run_id=int(row["run_id"]),
        context=str(row["context"]),
        workflow_name=str(row["workflow_name"]),
        manifest_name=str(row["manifest_name"]),
        manifest_value_schema=value.value_schema,
        manifest_digest=value.manifest_digest,
        manifest_hash=value.manifest_hash,
        entity_count=value.entity_count,
    )


def _registry_manifest_from_row(row: sqlite3.Row) -> RegistryManifest:
    value = _manifest_value_from_registry_row(row)
    return RegistryManifest(
        context=str(row["context"]),
        name=str(row["name"]),
        path=str(row["path"]),
        manifest_value_schema=value.value_schema,
        entity_count=value.entity_count,
        first_entity_id=value.entity_ids[0],
        last_entity_id=value.entity_ids[-1],
        manifest_digest=value.manifest_digest,
        manifest_hash=value.manifest_hash,
        canonical_body=value.canonical_body,
    )


def _manifest_value_from_registry_row(row: sqlite3.Row) -> ManifestValue:
    value = ManifestValue(
        value_schema=str(row["manifest_value_schema"]),
        manifest_digest=str(row["manifest_digest"]),
        canonical_body=str(row["canonical_body"]),
    )
    entity_count = int(row["entity_count"])
    if entity_count != value.entity_count:
        raise ValidationError("registry manifest value entity count is inconsistent")
    return value


def _validate_snapshot_registry_binding(
    path: Path,
    *,
    runtime_root: Path,
) -> tuple[Path, Path]:
    if not isinstance(path, Path):
        raise ValidationError("registry path must be a Path")
    if not isinstance(runtime_root, Path):
        raise ValidationError("runtime_root must be a Path")

    requested_runtime_root = runtime_root.expanduser()
    if requested_runtime_root.is_symlink():
        raise ValidationError("specification runtime root must be a real directory")
    try:
        resolved_runtime_root = requested_runtime_root.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise ValidationError(
            "specification runtime root must be an existing directory"
        ) from exc
    if not resolved_runtime_root.is_dir():
        raise ValidationError(
            "specification runtime root must be an existing directory"
        )

    registry_relative_path = Path(REGISTRY_DB_PATH)
    database_dir = resolved_runtime_root / registry_relative_path.parent
    if database_dir.is_symlink() or not database_dir.is_dir():
        raise ValidationError(
            "specification registry database path must be a real directory"
        )
    try:
        resolved_database_dir = database_dir.resolve(strict=True)
    except OSError as exc:
        raise ValidationError(
            "specification registry database path must be a real directory"
        ) from exc
    if not _path_contains_or_same(resolved_runtime_root, resolved_database_dir):
        raise ValidationError(
            "specification registry database path must stay inside runtime_root"
        )

    expected_path = resolved_database_dir / registry_relative_path.name
    if expected_path.is_symlink() or not expected_path.is_file():
        raise ValidationError("specification registry path must be a real file")

    requested_path = path.expanduser()
    if requested_path.is_symlink():
        raise ValidationError("specification registry path must be a real file")
    if not requested_path.is_absolute():
        requested_path = Path.cwd() / requested_path
    try:
        resolved_path = requested_path.parent.resolve(strict=True) / requested_path.name
    except (FileNotFoundError, OSError) as exc:
        raise ValidationError(
            "specification registry path must be a real file"
        ) from exc
    if resolved_path != expected_path:
        raise ValidationError("specification registry path is incompatible")
    if not resolved_path.is_file():
        raise ValidationError("specification registry path must be a real file")
    return resolved_path, resolved_runtime_root


def _require_snapshot_context_binding(
    conn: sqlite3.Connection,
    *,
    context: str,
    runtime_root: Path,
) -> None:
    row = conn.execute(
        """
        SELECT runtime_path, storage_layout_version
        FROM contexts
        WHERE context = ?
        """,
        (context,),
    ).fetchone()
    if row is None:
        raise ValidationError("registry.db missing specification context row")
    try:
        stored_runtime_root = Path(str(row[0])).expanduser().resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise ValidationError(
            "registry.db specification context binding is incompatible"
        ) from exc
    if (
        stored_runtime_root != runtime_root
        or row[1] != STORAGE_LAYOUT_VERSION
    ):
        raise ValidationError(
            "registry.db specification context binding is incompatible"
        )


def _insert_or_verify_snapshot_manifest_value(
    conn: sqlite3.Connection,
    *,
    value: ManifestValue,
) -> None:
    row = conn.execute(
        """
        SELECT canonical_body, entity_count
        FROM manifest_values
        WHERE value_schema = ? AND manifest_digest = ?
        """,
        (value.value_schema, value.manifest_digest),
    ).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO manifest_values (
                value_schema, manifest_digest, canonical_body, entity_count
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                value.value_schema,
                value.manifest_digest,
                value.canonical_body,
                value.entity_count,
            ),
        )
        return
    if tuple(row) != (value.canonical_body, value.entity_count):
        raise ValidationError(
            "registry manifest value does not match specification snapshot"
        )


def _read_specification_snapshot_conn(
    conn: sqlite3.Connection,
    *,
    context: str,
    snapshot_digest: str,
) -> CanonicalSpecificationSnapshot:
    parent = conn.execute(
        """
        SELECT snapshot_digest, context, canonical_bytes
        FROM specification_snapshots
        WHERE snapshot_digest = ?
        """,
        (snapshot_digest,),
    ).fetchone()
    if parent is None:
        raise ValidationError(f"unknown specification snapshot: {snapshot_digest}")
    if parent[1] != context:
        raise ValidationError("specification snapshot context does not match")
    canonical_bytes = parent[2]
    if type(canonical_bytes) is not bytes:
        raise ValidationError("specification snapshot canonical bytes must be a BLOB")
    snapshot = decode_specification_snapshot(canonical_bytes)
    if (
        snapshot.snapshot_digest != parent[0]
        or snapshot.snapshot_digest != snapshot_digest
        or snapshot.context != parent[1]
    ):
        raise ValidationError(
            "stored specification snapshot identity does not match canonical bytes"
        )

    stored_members = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT member_key, row_digest, disposition, exclusion_reason
            FROM specification_members
            WHERE snapshot_digest = ?
            ORDER BY member_key
            """,
            (snapshot_digest,),
        )
    )
    expected_members = tuple(
        (
            member.member_key,
            member.row.row_digest,
            member.disposition,
            member.exclusion_reason,
        )
        for member in snapshot.members
    )
    if stored_members != expected_members:
        raise ValidationError("stored specification member projection is inconsistent")

    stored_manifest_rows = tuple(
        conn.execute(
            """
            SELECT association.value_schema, association.manifest_digest,
                   value.canonical_body, value.entity_count
            FROM specification_snapshot_manifest_values AS association
            LEFT JOIN manifest_values AS value
              ON value.value_schema = association.value_schema
             AND value.manifest_digest = association.manifest_digest
            WHERE association.snapshot_digest = ?
            ORDER BY association.value_schema, association.manifest_digest
            """,
            (snapshot_digest,),
        )
    )
    stored_manifest_identities = tuple(
        (row[0], row[1]) for row in stored_manifest_rows
    )
    expected_manifest_identities = tuple(
        (value.value_schema, value.manifest_digest)
        for value in snapshot.manifest_values
    )
    if stored_manifest_identities != expected_manifest_identities:
        raise ValidationError(
            "stored specification manifest association is inconsistent"
        )
    for expected_value, stored_row in zip(
        snapshot.manifest_values,
        stored_manifest_rows,
        strict=True,
    ):
        canonical_body, entity_count = stored_row[2], stored_row[3]
        if canonical_body is None or entity_count is None:
            raise ValidationError("stored specification manifest value is missing")
        if type(canonical_body) is not str or type(entity_count) is not int:
            raise ValidationError("stored specification manifest value is malformed")
        value = ManifestValue(
            value_schema=expected_value.value_schema,
            manifest_digest=expected_value.manifest_digest,
            canonical_body=canonical_body,
        )
        if value.entity_count != entity_count or value != expected_value:
            raise ValidationError("stored specification manifest value is inconsistent")

    stored_results = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT member_key, role, step_name, output_name, address
            FROM specification_expected_results
            WHERE snapshot_digest = ?
            ORDER BY member_key, role, address, step_name, output_name
            """,
            (snapshot_digest,),
        )
    )
    expected_results = tuple(
        sorted(
            (
                (
                    member.member_key,
                    result.role,
                    result.step_name,
                    result.output_name,
                    result.address,
                )
                for member in snapshot.members
                for result in member.expected_results
            ),
            key=lambda row: (row[0], row[1], row[4], row[2], row[3]),
        )
    )
    if stored_results != expected_results:
        raise ValidationError(
            "stored specification expected-result projection is inconsistent"
        )
    return snapshot


def _read_specification_snapshot_projections_conn(
    conn: sqlite3.Connection,
    *,
    snapshot: CanonicalSpecificationSnapshot,
) -> SpecificationSnapshotProjections:
    members = tuple(
        SpecificationMemberProjection(
            member_key=member.member_key,
            row_digest=member.row.row_digest,
            disposition=member.disposition,  # type: ignore[arg-type]
            exclusion_reason=member.exclusion_reason,
            workflow_selector=member.row.workflow_selector,
            decision_coordinates=member.row.decision_coordinates,
            effective_declaration=member.row.effective_declaration,
            expected_results=member.expected_results,
        )
        for member in snapshot.members
    )
    members_by_key = {member.member_key: member for member in snapshot.members}
    if len(members_by_key) != len(snapshot.members):
        raise ValidationError("stored specification members are duplicated")
    for member in snapshot.members:
        if not member.row.effective_declaration.steps or not member.expected_results:
            raise ValidationError("stored specification member is incomplete")

    attempts, attempt_members = _read_specification_attempt_projections(
        conn,
        snapshot=snapshot,
        members_by_key=members_by_key,
    )
    result_facts = _read_specification_result_facts(
        conn,
        snapshot=snapshot,
        attempt_members=attempt_members,
    )
    current_publications = _read_specification_current_publications(
        conn,
        snapshot=snapshot,
        attempt_members=attempt_members,
        result_facts=result_facts,
    )

    results: list[SpecificationResultProjection] = []
    for attempt in attempts:
        member = attempt_members[attempt.attempt_id]
        resolved_count = 0
        for descriptor in member.expected_results:
            key = (attempt.attempt_id, descriptor.role, descriptor.address)
            facts = result_facts.get(key)
            if facts is None:
                results.append(
                    SpecificationResultProjection(
                        attempt_id=attempt.attempt_id,
                        member_key=attempt.member_key,
                        role=descriptor.role,
                        step_name=descriptor.step_name,
                        output_name=descriptor.output_name,
                        address=descriptor.address,
                        artifact_id=None,
                        producing_run_id=None,
                        producing_workflow_name=None,
                        artifact_path=None,
                        content_digest=None,
                        output_hash=None,
                        file_size=None,
                        extension=None,
                        request_bundle_digest=None,
                        current_publication_path=None,
                    )
                )
                continue
            resolved_count += 1
            results.append(
                SpecificationResultProjection(
                    attempt_id=facts.attempt_id,
                    member_key=facts.member_key,
                    role=facts.role,
                    step_name=facts.step_name,
                    output_name=facts.output_name,
                    address=facts.address,
                    artifact_id=facts.artifact_id,
                    producing_run_id=facts.producing_run_id,
                    producing_workflow_name=facts.producing_workflow_name,
                    artifact_path=facts.artifact_path,
                    content_digest=facts.content_digest,
                    output_hash=facts.output_hash,
                    file_size=facts.file_size,
                    extension=facts.extension,
                    request_bundle_digest=facts.request_bundle_digest,
                    current_publication_path=current_publications.get(key),
                )
            )
        _validate_specification_projection_cardinality(
            outcome=attempt.outcome,
            resolved_count=resolved_count,
            expected_count=len(member.expected_results),
        )

    result_tuple = tuple(results)
    return SpecificationSnapshotProjections(
        snapshot_digest=snapshot.snapshot_digest,
        context=snapshot.context,
        members=members,
        attempts=attempts,
        results=result_tuple,
        result_source_basis=_read_specification_result_source_basis(
            conn,
            snapshot=snapshot,
            results=result_tuple,
        ),
    )


def _read_specification_attempt_projections(
    conn: sqlite3.Connection,
    *,
    snapshot: CanonicalSpecificationSnapshot,
    members_by_key: dict[str, CanonicalSpecificationMember],
) -> tuple[
    tuple[SpecificationAttemptProjection, ...],
    dict[int, CanonicalSpecificationMember],
]:
    rows = conn.execute(
        """
        SELECT attempt.attempt_id, attempt.snapshot_digest, attempt.member_key,
               attempt.started_at, attempt.finished_at, attempt.outcome,
               attempt.selecting_run_id, attempt.failure_stage,
               attempt.failure_summary,
               selecting.run_id AS joined_selecting_run_id,
               selecting.context AS selecting_context,
               selecting.workflow_name AS selecting_workflow_name,
               selecting.selected_step_name AS selecting_step_name,
               selecting.selected_output_name AS selecting_output_name
        FROM specification_member_attempts AS attempt
        LEFT JOIN workflow_runs AS selecting
          ON selecting.run_id = attempt.selecting_run_id
        WHERE attempt.snapshot_digest = ?
        ORDER BY attempt.member_key, attempt.attempt_id
        """,
        (snapshot.snapshot_digest,),
    ).fetchall()
    projections: list[SpecificationAttemptProjection] = []
    attempt_members: dict[int, CanonicalSpecificationMember] = {}
    for row in rows:
        attempt_id = row["attempt_id"]
        _validate_positive_id(attempt_id, label="specification attempt id")
        member_key = _require_specification_projection_text(
            row["member_key"],
            label="specification attempt member key",
        )
        member = members_by_key.get(member_key)
        if (
            member is None
            or member.disposition != "included"
            or row["snapshot_digest"] != snapshot.snapshot_digest
        ):
            raise ValidationError("stored specification attempt is inconsistent")
        if attempt_id in attempt_members:
            raise ValidationError("stored specification attempt is duplicated")
        _validate_stored_specification_attempt(row)

        selecting_run_id = row["selecting_run_id"]
        if selecting_run_id is None:
            if any(
                row[name] is not None
                for name in (
                    "joined_selecting_run_id",
                    "selecting_context",
                    "selecting_workflow_name",
                    "selecting_step_name",
                    "selecting_output_name",
                )
            ):
                raise ValidationError("stored specification selecting run is malformed")
        else:
            _validate_positive_id(
                selecting_run_id,
                label="specification selecting run id",
            )
            target = member.row.effective_declaration
            if (
                row["joined_selecting_run_id"] != selecting_run_id
                or row["selecting_context"] != snapshot.context
                or row["selecting_workflow_name"] != member.row.workflow_selector
                or row["selecting_step_name"] != target.target_step_name
                or row["selecting_output_name"] != target.target_output_name
            ):
                raise ValidationError(
                    "stored specification selecting run is inconsistent"
                )

        failure = None
        if row["outcome"] == "failed":
            failure = SpecificationFailureDiagnostic(
                stage=row["failure_stage"],
                summary=row["failure_summary"],
            )
        projection = SpecificationAttemptProjection(
            attempt_id=attempt_id,
            member_key=member_key,
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            outcome=row["outcome"],
            selecting_run_id=selecting_run_id,
            failure=failure,
        )
        projections.append(projection)
        attempt_members[attempt_id] = member
    return tuple(projections), attempt_members


def _read_specification_result_facts(
    conn: sqlite3.Connection,
    *,
    snapshot: CanonicalSpecificationSnapshot,
    attempt_members: dict[int, CanonicalSpecificationMember],
) -> dict[tuple[int, str, str], SpecificationResultProjection]:
    rows = conn.execute(
        """
        SELECT result.attempt_id, result.snapshot_digest, result.member_key,
               result.role, result.address,
               attempt.attempt_id AS joined_attempt_id,
               attempt.snapshot_digest AS joined_attempt_snapshot_digest,
               attempt.member_key AS joined_attempt_member_key,
               expected.member_key AS expected_member_key,
               expected.role AS expected_role,
               expected.step_name AS expected_step_name,
               expected.output_name AS expected_output_name,
               expected.address AS expected_address,
               result.artifact_id AS selected_artifact_id,
               artifact.artifact_id AS joined_artifact_id,
               artifact.origin AS artifact_origin,
               artifact.run_id AS artifact_run_id,
               artifact.context AS artifact_context,
               artifact.workflow_name AS artifact_workflow_name,
               artifact.step_name AS artifact_step_name,
               artifact.output_name AS artifact_output_name,
               artifact.address AS artifact_address,
               artifact.path AS artifact_path,
               artifact.is_published AS artifact_is_published,
               artifact.published_path AS artifact_published_path,
               artifact.content_digest AS artifact_content_digest,
               artifact.output_hash AS artifact_output_hash,
               artifact.file_size AS artifact_file_size,
               artifact.extension AS artifact_extension,
               artifact.request_bundle_digest AS artifact_request_bundle_digest,
               producing.run_id AS joined_producing_run_id,
               producing.context AS producing_context,
               producing.workflow_name AS producing_workflow_name
        FROM specification_attempt_results AS result
        LEFT JOIN specification_member_attempts AS attempt
          ON attempt.attempt_id = result.attempt_id
         AND attempt.snapshot_digest = result.snapshot_digest
         AND attempt.member_key = result.member_key
        LEFT JOIN specification_expected_results AS expected
          ON expected.snapshot_digest = result.snapshot_digest
         AND expected.member_key = result.member_key
         AND expected.role = result.role
         AND expected.address = result.address
        LEFT JOIN artifacts AS artifact
          ON artifact.artifact_id = result.artifact_id
        LEFT JOIN workflow_runs AS producing
          ON producing.run_id = artifact.run_id
        WHERE result.snapshot_digest = ?
        ORDER BY result.member_key, result.attempt_id,
                 result.role, result.address
        """,
        (snapshot.snapshot_digest,),
    ).fetchall()
    facts: dict[tuple[int, str, str], SpecificationResultProjection] = {}
    descriptors_by_member = {
        member.member_key: {
            (descriptor.role, descriptor.address): descriptor
            for descriptor in member.expected_results
        }
        for member in attempt_members.values()
    }
    for row in rows:
        attempt_id = row["attempt_id"]
        member = attempt_members.get(attempt_id)
        if (
            member is None
            or row["member_key"] != member.member_key
            or row["joined_attempt_id"] != attempt_id
            or row["joined_attempt_snapshot_digest"] != snapshot.snapshot_digest
            or row["joined_attempt_member_key"] != member.member_key
        ):
            raise ValidationError("stored specification result attempt is inconsistent")
        role = row["role"]
        address = row["address"]
        descriptor = descriptors_by_member[member.member_key].get((role, address))
        if descriptor is None or (
            row["snapshot_digest"] != snapshot.snapshot_digest
            or row["expected_member_key"] != member.member_key
            or row["expected_role"] != descriptor.role
            or row["expected_step_name"] != descriptor.step_name
            or row["expected_output_name"] != descriptor.output_name
            or row["expected_address"] != descriptor.address
        ):
            raise ValidationError("stored specification result descriptor is inconsistent")

        artifact_id = row["selected_artifact_id"]
        producing_run_id = row["artifact_run_id"]
        _validate_positive_id(artifact_id, label="specification result artifact id")
        _validate_positive_id(
            producing_run_id,
            label="specification result producing run id",
        )
        if (
            row["joined_artifact_id"] != artifact_id
            or row["artifact_origin"] != "workflow_output"
            or row["artifact_context"] != snapshot.context
            or row["artifact_step_name"] != descriptor.step_name
            or row["artifact_output_name"] != descriptor.output_name
            or row["artifact_address"] != descriptor.address
            or row["artifact_is_published"] != 1
            or row["artifact_path"] != row["artifact_published_path"]
            or row["joined_producing_run_id"] != producing_run_id
            or row["producing_context"] != snapshot.context
            or row["artifact_workflow_name"] != row["producing_workflow_name"]
        ):
            raise ValidationError("stored specification result artifact is inconsistent")

        artifact_path = _require_specification_projection_text(
            row["artifact_path"],
            label="specification result artifact path",
        )
        producing_workflow_name = _require_specification_projection_text(
            row["producing_workflow_name"],
            label="specification result producing workflow",
        )
        content_digest = row["artifact_content_digest"]
        request_bundle_digest = row["artifact_request_bundle_digest"]
        if not is_valid_digest(content_digest) or not is_valid_digest(
            request_bundle_digest
        ):
            raise ValidationError("stored specification result digest is invalid")
        try:
            output_hash = validate_hash_alias(row["artifact_output_hash"])
        except ValidationError as exc:
            raise ValidationError(
                "stored specification result hash is invalid"
            ) from exc
        if output_hash != short_hash(content_digest):
            raise ValidationError("stored specification result hash is inconsistent")
        file_size = row["artifact_file_size"]
        if type(file_size) is not int or file_size < 0:
            raise ValidationError("stored specification result file size is invalid")
        extension = _require_specification_projection_text(
            row["artifact_extension"],
            label="specification result extension",
        )
        key = (attempt_id, descriptor.role, descriptor.address)
        if key in facts:
            raise ValidationError("stored specification result membership is duplicated")
        facts[key] = SpecificationResultProjection(
            attempt_id=attempt_id,
            member_key=member.member_key,
            role=descriptor.role,
            step_name=descriptor.step_name,
            output_name=descriptor.output_name,
            address=descriptor.address,
            artifact_id=artifact_id,
            producing_run_id=producing_run_id,
            producing_workflow_name=producing_workflow_name,
            artifact_path=artifact_path,
            content_digest=content_digest,
            output_hash=output_hash,
            file_size=file_size,
            extension=extension,
            request_bundle_digest=request_bundle_digest,
            current_publication_path=None,
        )
    return facts


def _read_specification_current_publications(
    conn: sqlite3.Connection,
    *,
    snapshot: CanonicalSpecificationSnapshot,
    attempt_members: dict[int, CanonicalSpecificationMember],
    result_facts: dict[tuple[int, str, str], SpecificationResultProjection],
) -> dict[tuple[int, str, str], str]:
    rows = conn.execute(
        """
        SELECT result.attempt_id, result.role, result.address,
               result.artifact_id AS selected_artifact_id,
               publication.context AS publication_context,
               publication.workflow_name AS publication_workflow_name,
               publication.step_name AS publication_step_name,
               publication.output_name AS publication_output_name,
               publication.address AS publication_address,
               publication.path AS publication_path,
               publication.output_digest AS publication_digest,
               publication.output_hash AS publication_hash,
               publication.artifact_id AS publication_artifact_id
        FROM specification_member_attempts AS attempt
        JOIN specification_attempt_results AS result
          ON result.attempt_id = attempt.attempt_id
         AND result.snapshot_digest = attempt.snapshot_digest
         AND result.member_key = attempt.member_key
        JOIN specification_expected_results AS expected
          ON expected.snapshot_digest = result.snapshot_digest
         AND expected.member_key = result.member_key
         AND expected.role = result.role
         AND expected.address = result.address
        JOIN workflow_runs AS selecting
          ON selecting.run_id = attempt.selecting_run_id
        LEFT JOIN published_outputs AS publication
          ON publication.context = selecting.context
         AND publication.workflow_name = selecting.workflow_name
         AND publication.step_name = expected.step_name
         AND publication.output_name = expected.output_name
         AND publication.address = expected.address
         AND publication.artifact_id = result.artifact_id
        WHERE attempt.snapshot_digest = ?
        ORDER BY result.attempt_id, result.role, result.address
        """,
        (snapshot.snapshot_digest,),
    ).fetchall()
    current: dict[tuple[int, str, str], str] = {}
    for row in rows:
        key = (row["attempt_id"], row["role"], row["address"])
        facts = result_facts.get(key)
        member = attempt_members.get(row["attempt_id"])
        if facts is None or member is None:
            raise ValidationError("stored specification publication seed is inconsistent")
        if row["publication_artifact_id"] is None:
            continue
        if (
            row["publication_context"] != snapshot.context
            or row["publication_workflow_name"] != member.row.workflow_selector
            or row["publication_step_name"] != facts.step_name
            or row["publication_output_name"] != facts.output_name
            or row["publication_address"] != facts.address
            or row["selected_artifact_id"] != facts.artifact_id
            or row["publication_artifact_id"] != facts.artifact_id
            or row["publication_path"] != facts.artifact_path
            or row["publication_digest"] != facts.content_digest
            or row["publication_hash"] != facts.output_hash
        ):
            raise ValidationError("stored current specification publication is inconsistent")
        if key in current:
            raise ValidationError("stored current specification publication is duplicated")
        current[key] = _require_specification_projection_text(
            row["publication_path"],
            label="specification publication path",
        )
    return current


def _read_specification_result_source_basis(
    conn: sqlite3.Connection,
    *,
    snapshot: CanonicalSpecificationSnapshot,
    results: tuple[SpecificationResultProjection, ...],
) -> tuple[SpecificationResultSourceBasisProjection, ...]:
    rows = conn.execute(
        """
        WITH RECURSIVE reachable(artifact_id) AS (
            SELECT DISTINCT result.artifact_id
            FROM specification_member_attempts AS attempt
            JOIN specification_attempt_results AS result
              ON result.attempt_id = attempt.attempt_id
             AND result.snapshot_digest = attempt.snapshot_digest
             AND result.member_key = attempt.member_key
            WHERE attempt.snapshot_digest = ?
            UNION
            SELECT dependency.source_artifact_id
            FROM reachable
            JOIN artifact_dependencies AS dependency
              ON dependency.dependent_artifact_id = reachable.artifact_id
        )
        SELECT dependency.dependent_artifact_id,
               dependency.source_artifact_id,
               dependency.source_content_digest,
               dependency.source_file_size,
               dependency.source_extension,
               dependency.source_step_name,
               dependency.source_output_name,
               dependency.source_address,
               dependency.source_scope,
               dependency.source_name,
               dependency.source_entity_id,
               dependency.source_occurrence_path,
               dependent.artifact_id AS joined_dependent_artifact_id,
               dependent.origin AS dependent_origin,
               dependent.context AS dependent_context,
               source.artifact_id AS joined_source_artifact_id,
               source.origin AS source_origin,
               source.context AS source_context,
               source.step_name AS source_artifact_step_name,
               source.output_name AS source_artifact_output_name,
               source.address AS source_artifact_address,
               source.content_digest AS source_artifact_content_digest,
               source.file_size AS source_artifact_file_size,
               source.extension AS source_artifact_extension,
               source.source_scope AS source_artifact_scope,
               source.source_name AS source_artifact_name,
               source.source_entity_id AS source_artifact_entity_id
        FROM reachable
        JOIN artifact_dependencies AS dependency
          ON dependency.dependent_artifact_id = reachable.artifact_id
        LEFT JOIN artifacts AS dependent
          ON dependent.artifact_id = dependency.dependent_artifact_id
        LEFT JOIN artifacts AS source
          ON source.artifact_id = dependency.source_artifact_id
        ORDER BY dependency.dependent_artifact_id,
                 dependency.source_artifact_id,
                 dependency.input_path,
                 dependency.binding_name
        """,
        (snapshot.snapshot_digest,),
    ).fetchall()

    adjacency: dict[int, list[tuple[str, sqlite3.Row]]] = {}
    for row in rows:
        dependent_id = row["dependent_artifact_id"]
        source_id = row["source_artifact_id"]
        _validate_positive_id(dependent_id, label="dependent artifact id")
        _validate_positive_id(source_id, label="source artifact id")
        if (
            row["joined_dependent_artifact_id"] != dependent_id
            or row["dependent_origin"] != "workflow_output"
            or row["dependent_context"] != snapshot.context
            or row["joined_source_artifact_id"] != source_id
            or row["source_context"] != snapshot.context
        ):
            raise ValidationError(
                "stored specification dependency edge is inconsistent"
            )

        digest = row["source_content_digest"]
        file_size = row["source_file_size"]
        extension = row["source_extension"]
        if (
            not is_valid_digest(digest)
            or type(file_size) is not int
            or file_size < 0
            or type(extension) is not str
            or not extension.strip()
        ):
            raise ValidationError(
                "stored specification dependency snapshot is malformed"
            )

        workflow_coordinate = (
            row["source_step_name"],
            row["source_output_name"],
            row["source_address"],
        )
        source_coordinate = (
            row["source_scope"],
            row["source_name"],
            row["source_entity_id"],
            row["source_occurrence_path"],
        )
        if all(value is None for value in source_coordinate):
            if not all(
                type(value) is str and bool(value.strip())
                for value in workflow_coordinate
            ):
                raise ValidationError(
                    "stored workflow dependency coordinate is malformed"
                )
            if (
                row["source_origin"] != "workflow_output"
                or row["source_artifact_step_name"] != workflow_coordinate[0]
                or row["source_artifact_output_name"] != workflow_coordinate[1]
                or row["source_artifact_address"] != workflow_coordinate[2]
                or row["source_artifact_content_digest"] != digest
                or row["source_artifact_file_size"] != file_size
                or row["source_artifact_extension"] != extension
            ):
                raise ValidationError(
                    "stored workflow dependency snapshot is inconsistent"
                )
            edge_kind = "workflow"
        elif all(value is None for value in workflow_coordinate):
            scope, name, entity_id, occurrence_path = source_coordinate
            if (
                scope not in {"global", "entity"}
                or type(name) is not str
                or not name.strip()
                or type(occurrence_path) is not str
                or not occurrence_path.strip()
                or (scope == "global" and entity_id is not None)
                or (
                    scope == "entity"
                    and (type(entity_id) is not str or not entity_id.strip())
                )
                or row["source_origin"] != "source"
                or row["source_artifact_scope"] != scope
                or row["source_artifact_name"] != name
                or row["source_artifact_entity_id"] != entity_id
            ):
                raise ValidationError(
                    "stored direct-source dependency snapshot is inconsistent"
                )
            edge_kind = "source"
        else:
            raise ValidationError("stored specification dependency kind is malformed")
        adjacency.setdefault(dependent_id, []).append((edge_kind, row))

    basis_rows: list[SpecificationResultSourceBasisProjection] = []
    basis_by_artifact: dict[
        int,
        set[tuple[str, str, str | None, str, str, int, str]],
    ] = {}
    for result in results:
        if result.artifact_id is None:
            continue
        identities = basis_by_artifact.get(result.artifact_id)
        if identities is None:
            identities = _collect_specification_source_basis(
                root_artifact_id=result.artifact_id,
                adjacency=adjacency,
            )
            basis_by_artifact[result.artifact_id] = identities
        for identity in sorted(
            identities,
            key=lambda value: (
                value[0],
                value[1],
                value[2] or "",
                value[3],
                value[4],
                value[5],
                value[6],
            ),
        ):
            (
                source_scope,
                source_name,
                source_entity_id,
                source_occurrence_path,
                source_content_digest,
                source_file_size,
                source_extension,
            ) = identity
            basis_rows.append(
                SpecificationResultSourceBasisProjection(
                    attempt_id=result.attempt_id,
                    member_key=result.member_key,
                    role=result.role,
                    address=result.address,
                    result_artifact_id=result.artifact_id,
                    source_scope=source_scope,  # type: ignore[arg-type]
                    source_name=source_name,
                    source_entity_id=source_entity_id,
                    source_occurrence_path=source_occurrence_path,
                    source_content_digest=source_content_digest,
                    source_file_size=source_file_size,
                    source_extension=source_extension,
                )
            )
    return tuple(basis_rows)


def _collect_specification_source_basis(
    *,
    root_artifact_id: int,
    adjacency: dict[int, list[tuple[str, sqlite3.Row]]],
) -> set[tuple[str, str, str | None, str, str, int, str]]:
    identities: set[tuple[str, str, str | None, str, str, int, str]] = set()
    visiting: set[int] = set()
    finished: set[int] = set()
    stack: list[tuple[int, bool]] = [(root_artifact_id, False)]
    while stack:
        artifact_id, leaving = stack.pop()
        if leaving:
            visiting.remove(artifact_id)
            finished.add(artifact_id)
            continue
        if artifact_id in finished:
            continue
        if artifact_id in visiting:
            raise ValidationError("stored specification dependency graph has a cycle")
        visiting.add(artifact_id)
        stack.append((artifact_id, True))
        for edge_kind, row in reversed(adjacency.get(artifact_id, [])):
            if edge_kind == "source":
                identities.add(
                    (
                        row["source_scope"],
                        row["source_name"],
                        row["source_entity_id"],
                        row["source_occurrence_path"],
                        row["source_content_digest"],
                        row["source_file_size"],
                        row["source_extension"],
                    )
                )
                continue
            source_id = row["source_artifact_id"]
            if source_id in visiting:
                raise ValidationError(
                    "stored specification dependency graph has a cycle"
                )
            if source_id not in finished:
                stack.append((source_id, False))
    return identities


def _validate_specification_projection_cardinality(
    *,
    outcome: SpecificationAttemptOutcome | None,
    resolved_count: int,
    expected_count: int,
) -> None:
    if outcome is None or outcome == "failed":
        valid = resolved_count == 0
    elif outcome == "partial":
        valid = 0 < resolved_count < expected_count
    else:
        valid = resolved_count == expected_count
    if not valid:
        raise ValidationError(
            "stored specification attempt result count is inconsistent"
        )


def _require_specification_projection_text(value: object, *, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValidationError(f"stored {label} is malformed")
    return value


def _invoke_specification_snapshot_fault(
    fault_hook: Callable[[str], None] | None,
    checkpoint: str,
) -> None:
    if fault_hook is not None:
        fault_hook(checkpoint)


def _validate_specification_snapshot_digest(value: object) -> str:
    if not is_valid_digest(value):
        raise ValidationError(
            "snapshot_digest must be a lowercase 64-character hexadecimal string"
        )
    return value


def _validate_canonical_specification_member(
    member: CanonicalSpecificationMember,
) -> None:
    if type(member) is not CanonicalSpecificationMember:
        raise ValidationError("member must be a CanonicalSpecificationMember")
    validate_path_token(member.member_key, label="specification member key")
    decoded_row = decode_specification_row(member.row.canonical_bytes)
    if (
        decoded_row != member.row
        or decoded_row.canonical_bytes != member.row.canonical_bytes
    ):
        raise ValidationError(
            "specification member does not match its canonical row bytes"
        )


def _validate_targeted_specification_member(
    conn: sqlite3.Connection,
    *,
    context: str,
    snapshot_digest: str,
    member: CanonicalSpecificationMember,
) -> None:
    snapshot_digest = _validate_specification_snapshot_digest(snapshot_digest)
    _validate_canonical_specification_member(member)
    parent = conn.execute(
        """
        SELECT context
        FROM specification_snapshots
        WHERE snapshot_digest = ?
        """,
        (snapshot_digest,),
    ).fetchone()
    if parent is None:
        raise ValidationError(f"unknown specification snapshot: {snapshot_digest}")
    if parent["context"] != context:
        raise ValidationError("specification snapshot context does not match")

    stored_member = conn.execute(
        """
        SELECT row_digest, disposition, exclusion_reason
        FROM specification_members
        WHERE snapshot_digest = ? AND member_key = ?
        """,
        (snapshot_digest, member.member_key),
    ).fetchone()
    expected_member = (
        member.row.row_digest,
        member.disposition,
        member.exclusion_reason,
    )
    if stored_member is None:
        raise ValidationError(
            f"unknown specification member: {member.member_key}"
        )
    if tuple(stored_member) != expected_member:
        raise ValidationError(
            "stored specification member projection is inconsistent"
        )

    stored_results = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT role, step_name, output_name, address
            FROM specification_expected_results
            WHERE snapshot_digest = ? AND member_key = ?
            ORDER BY role, address, step_name, output_name
            """,
            (snapshot_digest, member.member_key),
        )
    )
    expected_results = tuple(
        (
            result.role,
            result.step_name,
            result.output_name,
            result.address,
        )
        for result in member.expected_results
    )
    if stored_results != expected_results:
        raise ValidationError(
            "stored specification expected-result projection is inconsistent"
        )


def _validate_specification_attempt_ref(attempt: SpecificationAttemptRef) -> None:
    if type(attempt) is not SpecificationAttemptRef:
        raise ValidationError("attempt must be a SpecificationAttemptRef")
    _validate_positive_id(attempt.attempt_id, label="specification attempt id")
    validate_path_token(attempt.context, label="specification attempt context")
    _validate_specification_snapshot_digest(attempt.snapshot_digest)
    validate_path_token(
        attempt.member_key,
        label="specification attempt member key",
    )


def _validate_specification_failure_diagnostic(
    diagnostic: SpecificationFailureDiagnostic,
) -> tuple[str, str]:
    if type(diagnostic) is not SpecificationFailureDiagnostic:
        raise ValidationError(
            "diagnostic must be a SpecificationFailureDiagnostic"
        )
    if type(diagnostic.stage) is not str or diagnostic.stage not in {
        "planning",
        "execution",
        "acceptance",
    }:
        raise ValidationError("specification failure stage is invalid")
    if type(diagnostic.summary) is not str:
        raise ValidationError("specification failure summary must be a string")
    summary = diagnostic.summary.strip()
    if not summary:
        raise ValidationError("specification failure summary cannot be blank")
    if len(summary) > 4096:
        raise ValidationError(
            "specification failure summary cannot exceed 4096 characters"
        )
    return diagnostic.stage, summary


def _read_specification_attempt(
    conn: sqlite3.Connection,
    *,
    attempt: SpecificationAttemptRef,
    member: CanonicalSpecificationMember,
) -> sqlite3.Row:
    if attempt.member_key != member.member_key:
        raise ValidationError("specification attempt member does not match")
    row = conn.execute(
        """
        SELECT attempt_id, snapshot_digest, member_key, started_at,
               finished_at, outcome, selecting_run_id,
               failure_stage, failure_summary
        FROM specification_member_attempts
        WHERE attempt_id = ?
        """,
        (attempt.attempt_id,),
    ).fetchone()
    if row is None:
        raise ValidationError(
            f"unknown specification attempt: {attempt.attempt_id}"
        )
    if (row["snapshot_digest"], row["member_key"]) != (
        attempt.snapshot_digest,
        attempt.member_key,
    ):
        raise ValidationError("specification attempt reference does not match")
    _validate_stored_specification_attempt(row)
    return row


def _validate_stored_specification_attempt(row: sqlite3.Row) -> None:
    started_at = row["started_at"]
    if type(started_at) is not str or not started_at.strip():
        raise ValidationError("stored specification attempt is malformed")
    outcome = row["outcome"]
    finished_at = row["finished_at"]
    selecting_run_id = row["selecting_run_id"]
    failure_stage = row["failure_stage"]
    failure_summary = row["failure_summary"]
    if outcome is None:
        if any(
            value is not None
            for value in (
                finished_at,
                selecting_run_id,
                failure_stage,
                failure_summary,
            )
        ):
            raise ValidationError("stored specification attempt is malformed")
        return
    if type(finished_at) is not str or not finished_at.strip():
        raise ValidationError("stored specification attempt is malformed")
    if outcome in {"partial", "complete"}:
        _validate_positive_id(
            selecting_run_id,
            label="specification selecting run id",
        )
        if failure_stage is not None or failure_summary is not None:
            raise ValidationError("stored specification attempt is malformed")
        return
    if outcome != "failed":
        raise ValidationError("stored specification attempt is malformed")
    if selecting_run_id is not None:
        _validate_positive_id(
            selecting_run_id,
            label="specification selecting run id",
        )
    _validate_specification_failure_diagnostic(
        SpecificationFailureDiagnostic(
            stage=failure_stage,
            summary=failure_summary,
        )
    )


def _require_unresolved_specification_attempt(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
) -> None:
    if row["outcome"] is not None:
        raise ValidationError("specification attempt is already terminal")
    result_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM specification_attempt_results
            WHERE attempt_id = ?
            """,
            (row["attempt_id"],),
        ).fetchone()[0]
    )
    if result_count != 0:
        raise ValidationError(
            "unresolved specification attempt already has result memberships"
        )


def _validate_specification_acceptance_state(
    conn: sqlite3.Connection,
    *,
    context: str,
    workflow_name: str,
    selected_step_name: str,
    selected_output_name: str,
    intent: SpecificationAcceptanceIntent,
) -> tuple[str, str] | None:
    if type(intent) is not SpecificationAcceptanceIntent:
        raise ValidationError(
            "specification_acceptance must be a SpecificationAcceptanceIntent"
        )
    context = validate_path_token(context, label="context")
    workflow_name = validate_path_token(workflow_name, label="workflow name")
    selected_step_name = validate_path_token(
        selected_step_name,
        label="selected step name",
    )
    selected_output_name = validate_path_token(
        selected_output_name,
        label="selected output name",
    )
    attempt = intent.attempt
    member = intent.member
    _validate_specification_attempt_ref(attempt)
    if attempt.context != context:
        raise ValidationError("specification attempt context does not match")
    _validate_targeted_specification_member(
        conn,
        context=context,
        snapshot_digest=attempt.snapshot_digest,
        member=member,
    )
    if member.disposition != "included":
        raise ValidationError(
            "excluded specification member cannot be accepted"
        )
    row = _read_specification_attempt(
        conn,
        attempt=attempt,
        member=member,
    )
    _require_unresolved_specification_attempt(conn, row=row)

    effective = member.row.effective_declaration
    if workflow_name != member.row.workflow_selector:
        raise ValidationError("specification member workflow does not match")
    if (selected_step_name, selected_output_name) != (
        effective.target_step_name,
        effective.target_output_name,
    ):
        raise ValidationError("specification member target does not match")
    if not member.expected_results or any(
        result.step_name != effective.target_step_name
        for result in member.expected_results
    ):
        raise ValidationError(
            "specification member results must be siblings on its target step"
        )
    if intent.failure is None:
        return None
    return _validate_specification_failure_diagnostic(intent.failure)


def _insert_specification_attempt_results(
    conn: sqlite3.Connection,
    *,
    context: str,
    workflow_name: str,
    intent: SpecificationAcceptanceIntent,
    accepted_memberships: dict[tuple[str, str, str, str, str], int],
) -> int:
    resolved_count = 0
    for descriptor in intent.member.expected_results:
        artifact_id = accepted_memberships.get(
            (
                context,
                workflow_name,
                descriptor.step_name,
                descriptor.output_name,
                descriptor.address,
            )
        )
        if artifact_id is None:
            continue
        conn.execute(
            """
            INSERT INTO specification_attempt_results (
                attempt_id, snapshot_digest, member_key,
                role, address, artifact_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                intent.attempt.attempt_id,
                intent.attempt.snapshot_digest,
                intent.attempt.member_key,
                descriptor.role,
                descriptor.address,
                artifact_id,
            ),
        )
        resolved_count += 1
    return resolved_count


def _terminalize_specification_attempt(
    conn: sqlite3.Connection,
    *,
    intent: SpecificationAcceptanceIntent,
    run_id: int,
    finished_at: str,
    resolved_count: int,
    failure: tuple[str, str] | None,
) -> None:
    expected_count = len(intent.member.expected_results)
    if expected_count <= 0 or not 0 <= resolved_count <= expected_count:
        raise ValidationError("specification result count is invalid")
    if resolved_count == 0:
        outcome = "failed"
    elif resolved_count < expected_count:
        outcome = "partial"
    else:
        outcome = "complete"

    if outcome == "failed":
        if failure is None or failure[0] != "execution":
            raise ValidationError(
                "zero-result specification acceptance requires an execution failure"
            )
        failure_stage, failure_summary = failure
    else:
        if failure is not None:
            raise ValidationError(
                "accepted specification results cannot include a failure diagnostic"
            )
        failure_stage = None
        failure_summary = None

    _validate_positive_id(run_id, label="specification selecting run id")
    cursor = conn.execute(
        """
        UPDATE specification_member_attempts
        SET finished_at = ?, outcome = ?, selecting_run_id = ?,
            failure_stage = ?, failure_summary = ?
        WHERE attempt_id = ?
          AND snapshot_digest = ?
          AND member_key = ?
          AND outcome IS NULL
          AND finished_at IS NULL
          AND selecting_run_id IS NULL
          AND failure_stage IS NULL
          AND failure_summary IS NULL
        """,
        (
            finished_at,
            outcome,
            run_id,
            failure_stage,
            failure_summary,
            intent.attempt.attempt_id,
            intent.attempt.snapshot_digest,
            intent.attempt.member_key,
        ),
    )
    if cursor.rowcount != 1:
        raise ValidationError("specification attempt could not be terminalized")


def _invoke_specification_fault(
    fault_hook: Callable[[str], None] | None,
    checkpoint: str,
) -> None:
    if fault_hook is not None:
        fault_hook(checkpoint)


def _count_rows(
    conn: sqlite3.Connection,
    table: str,
    *,
    context: str,
    origin: str | None = None,
) -> int:
    if table not in {"manifest_declarations", "artifacts", "workflow_runs"}:
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
def _connect_readwrite_existing(path: Path) -> Iterator[sqlite3.Connection]:
    conn: sqlite3.Connection | None = None
    try:
        uri_path = quote(path.as_posix(), safe="/")
        conn = sqlite3.connect(f"file:{uri_path}?mode=rw", uri=True)
        _set_foreign_keys(conn)
    except sqlite3.Error as exc:
        if conn is not None:
            conn.close()
        raise ValidationError(f"could not open database {path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    except sqlite3.Error as exc:
        if conn.in_transaction:
            conn.rollback()
        raise ValidationError(
            f"could not persist specification state: {exc}"
        ) from exc
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
    if version == REGISTRY_MIGRATION_SOURCE_VERSION:
        raise ValidationError(
            "registry.db schema version 18 requires explicit migration; run "
            "'nipact registry migrate --context CONTEXT --project-dir PROJECT_DIR'"
        )
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
