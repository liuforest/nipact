"""Internal orchestration for immutable specification snapshots and member runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from .errors import ValidationError
from .execution import (
    RunOutcome,
    RunStatusCallback,
    _build_run_plan_from_loaded_project,
    _execute_run_plan_already_locked,
    _SpecificationExecutionStageError,
)
from .hashing import is_valid_digest
from .identity import validate_path_token
from .project_context import ResolvedProjectContext, resolve_project_context
from .registry import (
    SpecificationAcceptanceIntent,
    SpecificationAttemptOutcome,
    SpecificationAttemptRef,
    SpecificationFailureDiagnostic,
    SpecificationFailureStage,
    append_specification_member_attempt,
    fail_specification_member_attempt,
    insert_or_verify_specification_snapshot,
    read_specification_attempt_outcome,
    read_specification_snapshot,
)
from .runtime_lock import acquire_mutating_runtime_lock
from .specification_canonical import (
    CanonicalEffectiveDeclaration,
    CanonicalSpecificationMember,
    CanonicalSpecificationSnapshot,
    canonicalize_specification_snapshot,
    replay_specification_member,
)
from .specification_compiler import compile_specification
from .specification_loading import SpecificationSource, load_specification_project
from .workflow import LoadedWorkflowProject, load_workflow_project


SpecificationTerminalOutcome = SpecificationAttemptOutcome

_ZERO_RESULT_FAILURE = SpecificationFailureDiagnostic(
    stage="execution",
    summary="ordinary execution accepted no required specification results",
)


@dataclass(frozen=True)
class CompiledSpecificationSnapshot:
    loaded_project: LoadedWorkflowProject
    set_key: str
    equal_effective_member_groups: tuple[tuple[str, ...], ...]
    snapshot: CanonicalSpecificationSnapshot


@dataclass(frozen=True)
class SpecificationFreezeResult:
    snapshot: CanonicalSpecificationSnapshot
    inserted: bool


@dataclass(frozen=True)
class SpecificationMemberRunResult:
    attempt: SpecificationAttemptRef
    outcome: SpecificationTerminalOutcome
    ordinary_outcome: RunOutcome


@dataclass(frozen=True)
class SpecificationRunResult:
    snapshot_digest: str
    members: tuple[SpecificationMemberRunResult, ...]


def compile_specification_snapshot(
    *,
    project_dir: Path,
    context: str,
    source: SpecificationSource,
) -> CompiledSpecificationSnapshot:
    """Compile one selected specification into a declaration-only snapshot."""
    loaded = load_specification_project(
        project_dir=project_dir,
        context=context,
        source=source,
    )
    compilation = compile_specification(
        loaded.specification.mapping,
        {name: document.mapping for name, document in loaded.libraries.items()},
    )
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded.workflow_project,
        compilation=compilation,
    )
    return CompiledSpecificationSnapshot(
        loaded_project=loaded.workflow_project,
        set_key=compilation.set_key,
        equal_effective_member_groups=_equal_effective_member_groups(snapshot),
        snapshot=snapshot,
    )


def freeze_specification_snapshot(
    *,
    project_dir: Path,
    context: str,
    source: SpecificationSource,
) -> SpecificationFreezeResult:
    """Compile, canonicalize, and atomically freeze one selected specification."""
    compiled = compile_specification_snapshot(
        project_dir=project_dir,
        context=context,
        source=source,
    )
    resolved = resolve_project_context(project_dir=project_dir, context=context)
    _require_loaded_boundary(loaded=compiled.loaded_project, resolved=resolved)
    with acquire_mutating_runtime_lock(resolved.runtime_root):
        inserted = insert_or_verify_specification_snapshot(
            resolved.registry_path,
            runtime_root=resolved.runtime_root,
            snapshot=compiled.snapshot,
        )
    return SpecificationFreezeResult(snapshot=compiled.snapshot, inserted=inserted)


def _equal_effective_member_groups(
    snapshot: CanonicalSpecificationSnapshot,
) -> tuple[tuple[str, ...], ...]:
    grouped: list[tuple[CanonicalEffectiveDeclaration, list[str]]] = []
    for member in snapshot.members:
        declaration = member.row.effective_declaration
        for existing, member_keys in grouped:
            if declaration == existing:
                member_keys.append(member.member_key)
                break
        else:
            grouped.append((declaration, [member.member_key]))
    groups = [
        tuple(sorted(member_keys))
        for _declaration, member_keys in grouped
        if len(member_keys) > 1
    ]
    return tuple(sorted(groups))


def run_specification_member(
    *,
    project_dir: Path,
    context: str,
    snapshot_digest: str,
    member_key: str,
    cores: int = 1,
    status_callback: RunStatusCallback | None = None,
) -> SpecificationMemberRunResult:
    """Run one included frozen member through the ordinary execution path."""
    context, snapshot_digest = _validate_run_request(
        context=context,
        snapshot_digest=snapshot_digest,
        cores=cores,
    )
    member_key = validate_path_token(member_key, label="specification member key")
    result = _run_specification_members(
        project_dir=project_dir,
        context=context,
        snapshot_digest=snapshot_digest,
        member_key=member_key,
        cores=cores,
        status_callback=status_callback,
    )
    if len(result.members) != 1:  # pragma: no cover - internal invariant
        raise ValidationError("one-member specification run returned no member")
    return result.members[0]


def run_all_specification_members(
    *,
    project_dir: Path,
    context: str,
    snapshot_digest: str,
    cores: int = 1,
    status_callback: RunStatusCallback | None = None,
) -> SpecificationRunResult:
    """Run included frozen members sequentially in canonical snapshot order."""
    context, snapshot_digest = _validate_run_request(
        context=context,
        snapshot_digest=snapshot_digest,
        cores=cores,
    )
    return _run_specification_members(
        project_dir=project_dir,
        context=context,
        snapshot_digest=snapshot_digest,
        member_key=None,
        cores=cores,
        status_callback=status_callback,
    )


def _validate_run_request(
    *,
    context: str,
    snapshot_digest: str,
    cores: int,
) -> tuple[str, str]:
    context = validate_path_token(context, label="context")
    if not is_valid_digest(snapshot_digest):
        raise ValidationError(
            "snapshot_digest must be a lowercase 64-character hexadecimal string"
        )
    if type(cores) is not int or cores <= 0:
        raise ValidationError("cores must be a positive integer")
    return context, snapshot_digest


def _run_specification_members(
    *,
    project_dir: Path,
    context: str,
    snapshot_digest: str,
    member_key: str | None,
    cores: int,
    status_callback: RunStatusCallback | None,
) -> SpecificationRunResult:
    resolved = resolve_project_context(project_dir=project_dir, context=context)
    with acquire_mutating_runtime_lock(resolved.runtime_root):
        snapshot = read_specification_snapshot(
            resolved.registry_path,
            context=resolved.context,
            snapshot_digest=snapshot_digest,
        )
        members = _select_members(snapshot=snapshot, member_key=member_key)
        if not members:
            return SpecificationRunResult(
                snapshot_digest=snapshot.snapshot_digest,
                members=(),
            )

        loaded = load_workflow_project(
            project_dir=resolved.project_root,
            context=resolved.context,
        )
        _require_loaded_boundary(loaded=loaded, resolved=resolved)

        results: list[SpecificationMemberRunResult] = []
        for member in members:
            result = _run_replayed_member(
                resolved=resolved,
                loaded=loaded,
                snapshot=snapshot,
                member=member,
                cores=cores,
                status_callback=status_callback,
            )
            results.append(result)
            if result.outcome != "complete":
                break
        return SpecificationRunResult(
            snapshot_digest=snapshot.snapshot_digest,
            members=tuple(results),
        )


def _select_members(
    *,
    snapshot: CanonicalSpecificationSnapshot,
    member_key: str | None,
) -> tuple[CanonicalSpecificationMember, ...]:
    if member_key is None:
        return tuple(
            member
            for member in snapshot.members
            if member.disposition == "included"
        )
    member = next(
        (member for member in snapshot.members if member.member_key == member_key),
        None,
    )
    if member is None:
        raise ValidationError(f"unknown specification member: {member_key}")
    if member.disposition != "included":
        raise ValidationError(
            f"excluded specification member cannot be run: {member_key}"
        )
    return (member,)


def _require_loaded_boundary(
    *,
    loaded: LoadedWorkflowProject,
    resolved: ResolvedProjectContext,
) -> None:
    if (
        loaded.project_root != resolved.project_root
        or loaded.runtime_root != resolved.runtime_root
        or loaded.context != resolved.context
    ):
        raise ValidationError(
            "loaded workflow project does not match the resolved project boundary"
        )


def _run_replayed_member(
    *,
    resolved: ResolvedProjectContext,
    loaded: LoadedWorkflowProject,
    snapshot: CanonicalSpecificationSnapshot,
    member: CanonicalSpecificationMember,
    cores: int,
    status_callback: RunStatusCallback | None,
) -> SpecificationMemberRunResult:
    applied = replay_specification_member(
        loaded=loaded,
        snapshot=snapshot,
        member_key=member.member_key,
    )
    attempt = append_specification_member_attempt(
        resolved.registry_path,
        runtime_root=resolved.runtime_root,
        context=resolved.context,
        snapshot_digest=snapshot.snapshot_digest,
        member=member,
    )
    acceptance = SpecificationAcceptanceIntent(
        attempt=attempt,
        member=member,
        failure=_ZERO_RESULT_FAILURE,
    )

    try:
        structural = _build_run_plan_from_loaded_project(
            loaded=applied.loaded_project,
            workflow_name=member.row.workflow_selector,
            step_name=member.row.effective_declaration.target_step_name,
            address=None,
            dry_run=False,
        )
        if (
            structural.selected_output_name
            != member.row.effective_declaration.target_output_name
        ):
            raise ValidationError(
                "replayed specification target output does not match its run plan"
            )
    except Exception as exc:
        _record_failure_and_raise(
            resolved=resolved,
            attempt=attempt,
            member=member,
            stage="planning",
            original=exc,
        )

    execution_failure: tuple[SpecificationFailureStage, Exception] | None = None
    try:
        ordinary_outcome = _execute_run_plan_already_locked(
            structural,
            cores=cores,
            status_callback=status_callback,
            specification_acceptance=acceptance,
        )
    except _SpecificationExecutionStageError as exc:
        execution_failure = (exc.stage, exc.original)
    except Exception as exc:
        execution_failure = ("execution", exc)
    if execution_failure is not None:
        stage, original = execution_failure
        _record_failure_and_raise(
            resolved=resolved,
            attempt=attempt,
            member=member,
            stage=stage,
            original=original,
        )

    try:
        outcome = _read_normal_terminal_outcome(
            resolved=resolved,
            attempt=attempt,
            member=member,
            ordinary_outcome=ordinary_outcome,
        )
    except Exception as exc:
        _record_failure_and_raise(
            resolved=resolved,
            attempt=attempt,
            member=member,
            stage="acceptance",
            original=exc,
        )
    return SpecificationMemberRunResult(
        attempt=attempt,
        outcome=outcome,
        ordinary_outcome=ordinary_outcome,
    )


def _read_normal_terminal_outcome(
    *,
    resolved: ResolvedProjectContext,
    attempt: SpecificationAttemptRef,
    member: CanonicalSpecificationMember,
    ordinary_outcome: RunOutcome,
) -> SpecificationTerminalOutcome:
    outcome = read_specification_attempt_outcome(
        resolved.registry_path,
        runtime_root=resolved.runtime_root,
        attempt=attempt,
        member=member,
    )
    if outcome is not None:
        return outcome
    if not _is_zero_survivor_outcome(ordinary_outcome):
        raise ValidationError(
            "ordinary execution returned without resolving the specification attempt"
        )
    fail_specification_member_attempt(
        resolved.registry_path,
        runtime_root=resolved.runtime_root,
        attempt=attempt,
        member=member,
        diagnostic=_ZERO_RESULT_FAILURE,
    )
    outcome = read_specification_attempt_outcome(
        resolved.registry_path,
        runtime_root=resolved.runtime_root,
        attempt=attempt,
        member=member,
    )
    if outcome != "failed":
        raise ValidationError(
            "zero-survivor specification attempt did not resolve as failed"
        )
    return outcome


def _is_zero_survivor_outcome(outcome: RunOutcome) -> bool:
    return (
        outcome.published_count == 0
        and outcome.selected_generated_count == 0
        and outcome.selected_reused_count == 0
        and outcome.all_selected_resolved is False
    )


def _record_failure_and_raise(
    *,
    resolved: ResolvedProjectContext,
    attempt: SpecificationAttemptRef,
    member: CanonicalSpecificationMember,
    stage: SpecificationFailureStage,
    original: Exception,
) -> NoReturn:
    diagnostic = SpecificationFailureDiagnostic(
        stage=stage,
        summary=_failure_summary(original),
    )
    try:
        fail_specification_member_attempt(
            resolved.registry_path,
            runtime_root=resolved.runtime_root,
            attempt=attempt,
            member=member,
            diagnostic=diagnostic,
        )
    except Exception as persistence_error:
        raise persistence_error from original
    original_traceback = original.__traceback__
    if original.__cause__ is not None:
        raise original.with_traceback(original_traceback) from original.__cause__
    if original.__context__ is not None and not original.__suppress_context__:
        raise original.with_traceback(original_traceback) from original.__context__
    raise original.with_traceback(original_traceback) from None


def _failure_summary(exception: Exception) -> str:
    name = type(exception).__name__
    message = str(exception).strip()
    summary = f"{name}: {message}" if message else f"{name}: no message"
    if len(summary) > 4096:
        return f"{name}: failure message exceeded the stored diagnostic limit"
    return summary
