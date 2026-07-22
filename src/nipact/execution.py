"""Internal execution planning helpers for NIPACT workflow runs."""

from __future__ import annotations

import errno
import hashlib
import json
from importlib import metadata
import os
import platform
import shlex
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, BinaryIO, Callable

from ._version import __version__
from .artifacts import (
    CANONICAL_OUTPUT_ROOT,
    canonical_output_directory,
    canonical_output_path,
)
from .errors import ValidationError
from .execution_evidence import (
    RUN_PLAN_SCHEMA_VERSION,
    CompletionReceipt,
    ExecutionEvidenceError,
    completion_receipt_relative_path,
    generate_invocation_token,
    read_completion_receipt,
    write_json_atomic,
)
from .hashing import sha256_file_digest, short_hash
from .identity import validate_path_token
from .manifest import Manifest
from .projection import (
    IDENTITY_CONTRACT_VERSION,
    OUTPUT_CONTRACT_VERSION,
    RUNNER_CONTRACT_VERSION,
    CollectionBindingPlan,
    OutputContract,
    RegisteredSourceSnapshot,
    RequestBundleProjectionPlanV3,
    RequestBundleProjectionState,
    ResolvedRequestBundleProjectionV3,
    RequestedOutputCoordinate,
    SiblingOutput,
    SourceBindingPlan,
    StepContract,
    UpstreamRequestedOutputBindingPlan,
    resolve_request_bundle_projection_plan,
)
from .registry import (
    ArtifactInputRow,
    EnvironmentObservationV1,
    MembershipIntent,
    PublishedOutputRow,
    REGISTRY_DB_PATH,
    RetainedJobProjectionRecipe,
    ReusableArtifactBundleCandidate,
    ReusableArtifactBundleRequest,
    ReusableArtifactCandidate,
    ReusedProjectionSeed,
    RunExecutionPopulationRow,
    RunManifestBindingRow,
    SelectedOutputResolutionIntent,
    WorkflowOutputArtifactRow,
    RegisteredSourceAuthority,
    reconcile_manifest_and_source_authorities,
    record_workflow_run,
    read_context_runtime_path,
    read_registered_source_authorities,
    resolve_reusable_artifact_bundle,
)
from .runtime_lock import acquire_mutating_runtime_lock
from .source_authority import (
    LogicalSourceCoordinate,
    ObservedSourceAuthority,
    SourceDeclaration,
    observe_source_authority,
    read_source_occurrence_guard,
)
from .source_closure import (
    selected_job_coordinates,
    selected_source_declarations,
    source_declaration_for_binding,
)
from .workflow import (
    LoadedWorkflowProject,
    WorkflowPlan,
    WorkflowPlanExecutionPopulation,
    WorkflowPlanManifestBinding,
    WorkflowPlanStep,
    compile_workflow_plan,
    load_workflow_project,
)

RunStatusCallback = Callable[[str], None]


@dataclass(frozen=True)
class PublishedOutputSpec:
    job_id: str
    context: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    declared_extension: str
    request_bundle_digest: str | None


@dataclass(frozen=True)
class _PreparedOutput:
    row: PublishedOutputRow
    staging_path: Path
    file_size: int
    staged_device: int
    staged_inode: int
    staged_link_count: int


@dataclass(frozen=True)
class _MaterializedOutputResult:
    row: PublishedOutputRow
    staging_path: Path
    file_size: int


@dataclass(frozen=True)
class RunJobOutput:
    output_name: str
    declared_extension: str
    staging_path: Path
    staging_path_relative: str


@dataclass(frozen=True)
class RunJob:
    job_id: str
    step_name: str
    address: str
    execution_role: str
    callable_ref: str
    outputs: dict[str, RunJobOutput]
    inputs: dict[str, tuple[str, ...]]
    input_records: tuple[ArtifactInputRow, ...]
    params: dict[str, object]
    projection_plan: RequestBundleProjectionPlanV3
    projection_state: RequestBundleProjectionState

    @property
    def output_name(self) -> str:
        return self._single_output().output_name

    @property
    def declared_extension(self) -> str:
        return self._single_output().declared_extension

    @property
    def staging_path(self) -> Path:
        return self._single_output().staging_path

    @property
    def staging_path_relative(self) -> str:
        return self._single_output().staging_path_relative

    def inputs_as_relative_paths(self) -> tuple[str, ...]:
        paths: list[str] = []
        for input_paths in self.inputs.values():
            paths.extend(input_paths)
        return tuple(paths)

    def output_ref(self, output_name: str) -> "RunJobOutputRef":
        try:
            output = self.outputs[output_name]
        except KeyError as exc:
            raise ValidationError(
                f"run job {self.job_id!r} is missing output {output_name!r}"
            ) from exc
        return RunJobOutputRef(job=self, output=output)

    def _single_output(self) -> RunJobOutput:
        if len(self.outputs) != 1:
            raise ValidationError(
                f"run job has multiple outputs; use an explicit output name: {self.job_id}"
            )
        return next(iter(self.outputs.values()))


@dataclass(frozen=True)
class RunJobOutputRef:
    job: RunJob
    output: RunJobOutput

    @property
    def job_id(self) -> str:
        return self.job.job_id

    @property
    def step_name(self) -> str:
        return self.job.step_name

    @property
    def output_name(self) -> str:
        return self.output.output_name

    @property
    def address(self) -> str:
        return self.job.address

    @property
    def callable_ref(self) -> str:
        return self.job.callable_ref

    @property
    def execution_role(self) -> str:
        return self.job.execution_role

    @property
    def declared_extension(self) -> str:
        return self.output.declared_extension

    @property
    def staging_path(self) -> Path:
        return self.output.staging_path

    @property
    def staging_path_relative(self) -> str:
        return self.output.staging_path_relative

    @property
    def input_records(self) -> tuple[ArtifactInputRow, ...]:
        return self.job.input_records

    @property
    def params(self) -> dict[str, object]:
        return self.job.params

    @property
    def projection_plan(self) -> RequestBundleProjectionPlanV3:
        return self.job.projection_plan

    @property
    def projection_state(self) -> RequestBundleProjectionState:
        return self.job.projection_state


@dataclass(frozen=True)
class ReusedRunJobOutputRef:
    step_name: str
    output_name: str
    address: str
    execution_role: str
    callable_ref: str
    parameters_json: str
    declared_extension: str
    staging_path: Path
    staging_path_relative: str
    source_path: Path
    source_path_relative: str
    source_artifact_id: int
    source_workflow_name: str
    source_run_id: int
    source_bundle_artifact_ids: tuple[int, ...]
    content_digest: str
    file_size: int
    reuse_request: ReusableArtifactBundleRequest
    projection_plan: RequestBundleProjectionPlanV3
    projection_state: RequestBundleProjectionState
    candidate: ReusableArtifactCandidate
    bundle: ReusableArtifactBundleCandidate


@dataclass(frozen=True)
class _PreparedReusedInput:
    output_ref: ReusedRunJobOutputRef
    candidate: ReusableArtifactCandidate
    bound_occurrence_path: Path
    supplied_path: Path


@dataclass(frozen=True)
class _PreparedReusedInputs:
    candidates: dict[int, ReusableArtifactCandidate]
    inputs: tuple[_PreparedReusedInput, ...]


@dataclass(frozen=True)
class SelectedReusedBundleRef:
    """Prospective reused resolution for one user-selected output coordinate."""

    step_name: str
    output_name: str
    address: str
    reuse_request: ReusableArtifactBundleRequest
    planned_sibling_artifact_ids: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if (
            self.step_name != self.reuse_request.step_name
            or self.address != self.reuse_request.address
        ):
            raise ValidationError(
                "selected reused coordinate does not match the request bundle"
            )
        if self.planned_sibling_artifact_ids != tuple(
            sorted(self.planned_sibling_artifact_ids)
        ):
            raise ValidationError(
                "selected reused sibling artifacts must be ordered by output name"
            )
        output_names = tuple(
            name for name, _artifact_id in self.planned_sibling_artifact_ids
        )
        if output_names != tuple(sorted(dict(self.reuse_request.sibling_outputs))):
            raise ValidationError(
                "selected reused sibling artifacts do not match the request bundle"
            )
        artifact_ids = tuple(
            artifact_id
            for _output_name, artifact_id in self.planned_sibling_artifact_ids
        )
        if (
            self.output_name not in output_names
            or any(artifact_id <= 0 for artifact_id in artifact_ids)
            or len(artifact_ids) != len(set(artifact_ids))
        ):
            raise ValidationError("selected reused bundle reference is invalid")


@dataclass(frozen=True)
class ExecutableRunPlan:
    project_root: Path
    runtime_root: Path
    context: str
    workflow_name: str
    base_workflow_name: str | None
    selected_step_name: str
    selected_output_name: str
    requested_address: str | None
    dry_run: bool
    run_workspace: Path
    execution_population: WorkflowPlanExecutionPopulation | None
    manifest_bindings: tuple[WorkflowPlanManifestBinding, ...]
    published_outputs: tuple[PublishedOutputSpec, ...]
    jobs: tuple[RunJob, ...]
    selected_fresh_output_refs: tuple[RunJobOutputRef, ...]
    selected_reused_output_refs: tuple[SelectedReusedBundleRef, ...]
    reused_outputs: tuple[ReusedRunJobOutputRef, ...]
    reused_validation_outputs: tuple[ReusedRunJobOutputRef, ...]
    # Reporting statistic: fresh jobs in the selected semantic closure.
    reachable_job_count: int

    @property
    def selected_fresh_jobs(self) -> tuple[RunJob, ...]:
        return tuple(output_ref.job for output_ref in self.selected_fresh_output_refs)


@dataclass(frozen=True)
class StructuralRunPlan:
    """Frozen declarations plus a non-authoritative metadata forecast."""

    loaded_project: LoadedWorkflowProject
    workflow_plan: WorkflowPlan
    source_declarations: tuple[SourceDeclaration, ...]
    forecast: ExecutableRunPlan

    def __getattr__(self, name: str) -> Any:
        # Preserve the existing read-only planning/reporting surface while
        # making it impossible for real execution to confuse the forecast
        # with the under-lock executable plan.
        return getattr(self.forecast, name)


@dataclass(frozen=True)
class RunOutcome:
    """Result of a best-effort job-atomic run."""

    published_count: int
    selected_generated_count: int
    selected_reused_count: int
    failed_jobs: tuple[tuple[str, str, str], ...]  # (step, address, coarse reason)
    all_selected_resolved: bool
    published_bytes: int = 0
    cleanup_warnings: tuple[str, ...] = ()


def build_run_plan(
    *,
    project_dir: Path,
    context: str,
    workflow_name: str,
    step_name: str,
    address: str | None = None,
    dry_run: bool = False,
) -> StructuralRunPlan:
    """Build frozen structural declarations and a metadata-only forecast."""
    loaded = load_workflow_project(project_dir=project_dir, context=context)
    registered_runtime_path = read_context_runtime_path(
        loaded.runtime_root / REGISTRY_DB_PATH,
        context=loaded.context,
    )
    if registered_runtime_path != str(loaded.runtime_root):
        raise ValidationError("registry.db context runtime path is out of date")
    plan = compile_workflow_plan(
        loaded,
        workflow_name=workflow_name,
        step_name=step_name,
    )
    declarations = selected_source_declarations(
        loaded=loaded,
        plan=plan,
        requested_address=address,
    )
    forecast_authorities = read_registered_source_authorities(
        loaded.runtime_root / REGISTRY_DB_PATH,
        coordinates=(declaration.coordinate for declaration in declarations),
    )
    forecast_authorities = {
        declaration.coordinate: registered
        for declaration in declarations
        if (registered := forecast_authorities.get(declaration.coordinate))
        is not None
        and registered.authority.declaration.declared_path
        == declaration.declared_path
        and registered.authority.guard
        == read_source_occurrence_guard(
            runtime_root=loaded.runtime_root,
            declaration=declaration,
        )
    }
    forecast = _build_executable_run_plan(
        loaded=loaded,
        plan=plan,
        address=address,
        dry_run=dry_run,
        source_authorities=forecast_authorities,
    )
    return StructuralRunPlan(
        loaded_project=loaded,
        workflow_plan=plan,
        source_declarations=declarations,
        forecast=forecast,
    )


def _build_executable_run_plan(
    *,
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
    address: str | None,
    dry_run: bool,
    source_authorities: dict[
        LogicalSourceCoordinate,
        RegisteredSourceAuthority,
    ],
) -> ExecutableRunPlan:
    """Finalize one exact job/reuse graph from prepared source authority."""
    selected_step = _plan_step(plan, plan.selected_step_name)
    _validated_step_outputs(selected_step)
    selected_output = selected_step.outputs[plan.selected_output_name]
    run_workspace = (
        loaded.runtime_root
        / "runs"
        / loaded.context
        / plan.workflow_name
        / plan.selected_step_name
    )
    if len(selected_step.outputs) > 1:
        run_workspace = run_workspace / selected_output.name
    addresses = _selected_addresses(plan, selected_step, requested_address=address)
    if address is not None:
        # Targeted runs get an address-partitioned workspace so runs for
        # different addresses cannot overwrite each other's plan, staging,
        # or logs. The full-population path is unchanged. The address has
        # already been validated as a safe path token above.
        run_workspace = run_workspace / "addresses" / address
    if dry_run:
        # Dry runs plan and stage in an isolated workspace so a previous real
        # run's staged outputs cannot suppress the forecast and dry-run
        # metadata cannot overwrite the executable run plan and logs. The
        # dry-run component is final, after all selected-output and address
        # partitioning.
        run_workspace = run_workspace / "dry-run"
    jobs, reused_outputs_by_artifact = _build_jobs(
        loaded=loaded,
        plan=plan,
        run_workspace=run_workspace,
        requested_address=address,
        source_authorities=source_authorities,
    )
    output_refs_by_key = _output_refs_by_key(jobs)
    selected_fresh_output_refs: list[RunJobOutputRef] = []
    selected_reused_output_refs: list[SelectedReusedBundleRef] = []
    for selected_address in addresses:
        key = (selected_step.step_name, selected_output.name, selected_address)
        fresh_ref = output_refs_by_key.get(key)
        reused_ref = reused_outputs_by_artifact.get(key)
        if fresh_ref is not None:
            selected_fresh_output_refs.append(fresh_ref)
        elif reused_ref is not None:
            sibling_refs = {
                output_name: reused_outputs_by_artifact.get(
                    (selected_step.step_name, output_name, selected_address)
                )
                for output_name in dict(reused_ref.reuse_request.sibling_outputs)
            }
            if any(ref is None for ref in sibling_refs.values()):
                raise ValidationError(
                    "selected reused bundle is missing a planned sibling"
                )
            selected_reused_output_refs.append(
                SelectedReusedBundleRef(
                    step_name=selected_step.step_name,
                    output_name=selected_output.name,
                    address=selected_address,
                    reuse_request=reused_ref.reuse_request,
                    planned_sibling_artifact_ids=tuple(
                        sorted(
                            (output_name, ref.source_artifact_id)
                            for output_name, ref in sibling_refs.items()
                            if ref is not None
                        )
                    ),
                )
            )
        else:
            raise ValidationError("run plan is missing selected output")
    selected_fresh_output_refs_tuple = tuple(selected_fresh_output_refs)
    selected_reused_output_refs_tuple = tuple(selected_reused_output_refs)
    _validate_selected_output_partition(
        selected_step_name=selected_step.step_name,
        selected_output_name=selected_output.name,
        selected_addresses=addresses,
        selected_fresh_output_refs=selected_fresh_output_refs_tuple,
        selected_reused_output_refs=selected_reused_output_refs_tuple,
    )
    # Hydration is scoped to the selected targets' reachable closure: a reused
    # registry artifact is retained only when a reachable fresh job consumes it.
    # Reachability follows dependency records rather than address, so a fresh
    # cohort ancestor can pull sibling-entity reuse back into scope.
    reachable_job_ids = _reachable_job_ids_for_outputs(
        jobs=jobs,
        selected_output_refs=selected_fresh_output_refs_tuple,
    )
    reused_outputs = _used_reused_outputs(
        jobs=tuple(job for job in jobs if job.job_id in reachable_job_ids),
        reused_outputs_by_artifact=reused_outputs_by_artifact,
    )
    reused_validation_outputs = _reused_validation_outputs(
        jobs=tuple(job for job in jobs if job.job_id in reachable_job_ids),
        reused_outputs_by_artifact=reused_outputs_by_artifact,
        selected_reused_output_refs=selected_reused_output_refs_tuple,
    )
    published_outputs = _published_output_specs(
        loaded=loaded,
        plan=plan,
        jobs=jobs,
        reachable_job_ids=reachable_job_ids,
    )
    return ExecutableRunPlan(
        project_root=loaded.project_root,
        runtime_root=loaded.runtime_root,
        context=loaded.context,
        workflow_name=plan.workflow_name,
        base_workflow_name=loaded.workflows[plan.workflow_name].base_workflow,
        selected_step_name=selected_step.step_name,
        selected_output_name=selected_output.name,
        requested_address=address,
        dry_run=dry_run,
        run_workspace=run_workspace,
        execution_population=plan.execution_population,
        manifest_bindings=plan.manifest_bindings,
        published_outputs=published_outputs,
        jobs=jobs,
        selected_fresh_output_refs=selected_fresh_output_refs_tuple,
        selected_reused_output_refs=selected_reused_output_refs_tuple,
        reused_outputs=reused_outputs,
        reused_validation_outputs=reused_validation_outputs,
        reachable_job_count=len(reachable_job_ids),
    )


def _validate_selected_output_partition(
    *,
    selected_step_name: str,
    selected_output_name: str,
    selected_addresses: tuple[str, ...],
    selected_fresh_output_refs: tuple[RunJobOutputRef, ...],
    selected_reused_output_refs: tuple[SelectedReusedBundleRef, ...],
) -> None:
    expected = {
        (selected_step_name, selected_output_name, address)
        for address in selected_addresses
    }
    fresh = [
        (output_ref.step_name, output_ref.output_name, output_ref.address)
        for output_ref in selected_fresh_output_refs
    ]
    reused = [
        (output_ref.step_name, output_ref.output_name, output_ref.address)
        for output_ref in selected_reused_output_refs
    ]
    if len(fresh) != len(set(fresh)) or len(reused) != len(set(reused)):
        raise ValidationError("selected output coordinate is duplicated")
    fresh_set = set(fresh)
    reused_set = set(reused)
    if fresh_set & reused_set:
        raise ValidationError("selected output is both fresh and reused")
    if fresh_set | reused_set != expected:
        raise ValidationError("selected outputs do not cover the requested selection")


def execute_run_plan(
    run_plan: StructuralRunPlan,
    *,
    cores: int = 1,
    status_callback: RunStatusCallback | None = None,
) -> RunOutcome:
    """Finalize source authority once, then execute one frozen exact plan."""
    if not isinstance(run_plan, StructuralRunPlan):
        raise ValidationError("run plan must be a StructuralRunPlan")
    if run_plan.forecast.dry_run:
        return _execute_executable_run_plan(
            run_plan.forecast,
            cores=cores,
            status_callback=status_callback,
        )
    with acquire_mutating_runtime_lock(run_plan.forecast.runtime_root):
        registry_path = run_plan.forecast.runtime_root / REGISTRY_DB_PATH
        coordinates = tuple(
            declaration.coordinate for declaration in run_plan.source_declarations
        )
        registered = read_registered_source_authorities(
            registry_path,
            coordinates=coordinates,
        )
        observations = tuple(
            observe_source_authority(
                runtime_root=run_plan.forecast.runtime_root,
                declaration=declaration,
                registered=(
                    registered[declaration.coordinate].authority
                    if declaration.coordinate in registered
                    else None
                ),
            )
            for declaration in run_plan.source_declarations
        )
        relevant_manifests, relevant_manifest_paths = (
            _relevant_manifest_authority_inputs(
                loaded=run_plan.loaded_project,
                plan=run_plan.workflow_plan,
            )
        )
        source_authorities = reconcile_manifest_and_source_authorities(
            registry_path,
            context=run_plan.loaded_project.context,
            manifests=relevant_manifests,
            manifest_paths=relevant_manifest_paths,
            observations=observations,
        )
        for status in ("new", "changed", "unchanged"):
            count = sum(observation.status == status for observation in observations)
            _emit_status(status_callback, f"sources_{status}:{count}")
        executable = _build_executable_run_plan(
            loaded=run_plan.loaded_project,
            plan=run_plan.workflow_plan,
            address=run_plan.forecast.requested_address,
            dry_run=False,
            source_authorities=source_authorities,
        )
        return _execute_executable_run_plan(
            executable,
            cores=cores,
            status_callback=status_callback,
        )


def _relevant_manifest_authority_inputs(
    *,
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
) -> tuple[dict[str, Manifest], dict[str, str]]:
    references = tuple(
        reference
        for reference in (plan.execution_population, *plan.manifest_bindings)
        if reference is not None
    )
    manifests: dict[str, Manifest] = {}
    manifest_paths: dict[str, str] = {}
    for reference in references:
        name = reference.manifest_name
        if name not in loaded.manifests or name not in loaded.manifest_paths:
            raise ValidationError(f"workflow plan manifest is not configured: {name}")
        manifest = loaded.manifests[name]
        if (
            manifest.manifest_value_schema != reference.manifest_value_schema
            or manifest.manifest_digest != reference.manifest_digest
            or manifest.entity_ids != reference.entity_ids
        ):
            raise ValidationError(f"workflow plan manifest value is inconsistent: {name}")
        try:
            declared_path = loaded.manifest_paths[name].relative_to(
                loaded.project_root
            )
        except ValueError as exc:
            raise ValidationError(
                f"manifest declaration is outside the project root: {name}"
            ) from exc
        manifests[name] = manifest
        manifest_paths[name] = declared_path.as_posix()
    return manifests, manifest_paths


def _execute_executable_run_plan(
    run_plan: ExecutableRunPlan,
    *,
    cores: int,
    status_callback: RunStatusCallback | None,
) -> RunOutcome:
    """Execute one finalized plan and record dependency-consistent survivors.

    Snakemake runs with ``--keep-going``. Current completion receipts, complete
    sibling staging, preparation checks, dependency pruning, materialization, and
    registry acceptance determine which independent jobs become reusable. A real
    fresh-only run with no survivors and a nonzero Snakemake result is a hard
    error, as is a nonzero dry-run result.
    """
    if cores <= 0:
        raise ValidationError("cores must be a positive integer")
    _emit_status(status_callback, "building_workspace")
    has_fresh_selection = bool(run_plan.selected_fresh_output_refs)
    has_selected_reuse = bool(run_plan.selected_reused_output_refs)
    _preflight_publication_layout(run_plan)
    invocation_token = None if run_plan.dry_run else generate_invocation_token()
    _prepare_run_workspace(run_plan)
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate] = {}
    if run_plan.dry_run:
        # Dry runs may refresh the active reused closure as a metadata-only
        # forecast. A fresh plan needs the forecast input paths in its
        # Snakefile; a reuse-only plan writes no executor files at all.
        if not has_fresh_selection:
            _write_reuse_only_workspace(run_plan, invocation_token=None)
        if has_selected_reuse:
            _emit_status(status_callback, "validating_selected_reuse")
        reused_input_paths = _dry_run_forecast_input_paths(run_plan)
        if has_fresh_selection:
            _write_run_workspace(
                run_plan,
                invocation_token=None,
                supplied_input_paths=reused_input_paths,
            )
    else:
        if has_selected_reuse:
            _emit_status(status_callback, "validating_selected_reuse")
        prepared_reused_inputs = _prepare_reused_inputs(run_plan)
        actual_reused_artifacts = prepared_reused_inputs.candidates
        if has_fresh_selection:
            _write_run_workspace(
                run_plan,
                invocation_token=invocation_token,
                prepared_reused_inputs=prepared_reused_inputs,
            )
            _remove_expected_staged_outputs(run_plan)
            _remove_expected_completion_receipts(run_plan)
        else:
            _write_reuse_only_workspace(
                run_plan,
                invocation_token=invocation_token,
            )
    returncode = 0
    if has_fresh_selection:
        _emit_status(status_callback, "starting_snakemake")
        returncode = _run_snakemake(run_plan, cores=cores, dry_run=run_plan.dry_run)
        _emit_status(status_callback, "snakemake_complete")
    if run_plan.dry_run:
        if returncode != 0:
            log_path = run_plan.run_workspace / "logs" / "snakemake.log"
            raise ValidationError(
                f"Snakemake failed with exit code {returncode}; see {log_path}"
            )
        return RunOutcome(
            published_count=0,
            selected_generated_count=0,
            selected_reused_count=0,
            failed_jobs=(),
            all_selected_resolved=True,
        )
    if has_fresh_selection:
        _emit_status(status_callback, "publishing_outputs")
        if invocation_token is None:
            raise ValidationError("real execution is missing its invocation token")
        published_results, publish_failures = _publish_run_outputs(
            run_plan,
            invocation_token=invocation_token,
        )
    else:
        published_results = ()
        publish_failures = ()
    failed_jobs = tuple(sorted(publish_failures))
    published_rows = tuple(result.row for result in published_results)
    published_bytes = sum(result.file_size for result in published_results)
    if not published_rows and not has_selected_reuse:
        if returncode != 0:
            log_path = run_plan.run_workspace / "logs" / "snakemake.log"
            raise ValidationError(
                f"Snakemake failed with exit code {returncode}; see {log_path}"
            )
        return RunOutcome(
            published_count=0,
            selected_generated_count=0,
            selected_reused_count=0,
            failed_jobs=failed_jobs,
            all_selected_resolved=False,
            published_bytes=0,
        )
    artifact_rows = _workflow_output_artifact_rows(
        run_plan,
        published_results=published_results,
        actual_reused_artifacts=actual_reused_artifacts,
        prepared_reused_inputs=prepared_reused_inputs,
    )
    projection_recipes = _retained_projection_recipes(
        run_plan,
        published_rows=published_rows,
    )
    reused_projection_seeds = _reused_projection_seeds(
        run_plan,
        artifact_rows=artifact_rows,
        actual_reused_artifacts=actual_reused_artifacts,
    )
    selected_resolution_intents = _selected_resolution_intents(
        run_plan,
        published_rows=published_rows,
        actual_reused_artifacts=actual_reused_artifacts,
    )
    membership_intents = tuple(
        MembershipIntent(row=row) for row in published_rows
    ) + _reused_membership_intents(
        run_plan,
        published_rows=published_rows,
        actual_reused_artifacts=actual_reused_artifacts,
    )
    published_count = record_workflow_run(
        run_plan.runtime_root / REGISTRY_DB_PATH,
        runtime_root=run_plan.runtime_root,
        context=run_plan.context,
        workflow_name=run_plan.workflow_name,
        base_workflow_name=run_plan.base_workflow_name,
        selected_step_name=run_plan.selected_step_name,
        selected_output_name=run_plan.selected_output_name,
        run_workspace=_runtime_relative_path(run_plan.runtime_root, run_plan.run_workspace),
        run_plan_path=_runtime_relative_path(
            run_plan.runtime_root,
            run_plan.run_workspace / "run_plan.json",
        ),
        run_plan_digest=sha256_file_digest(run_plan.run_workspace / "run_plan.json"),
        artifacts=artifact_rows,
        projection_recipes=projection_recipes,
        reused_projection_seeds=reused_projection_seeds,
        selected_resolution_intents=selected_resolution_intents,
        environment_observation=_environment_observation(),
        execution_population=_run_execution_population_row(run_plan),
        manifest_bindings=_run_manifest_binding_rows(run_plan),
        membership_intents=membership_intents,
    )
    _emit_status(status_callback, "registry_updated")
    cleanup_warnings = _finalize_published_output_staging(published_results)
    selected_generated_count = sum(
        intent.outcome == "generated" for intent in selected_resolution_intents
    )
    selected_reused_count = sum(
        intent.outcome == "reused" for intent in selected_resolution_intents
    )
    return RunOutcome(
        published_count=published_count,
        selected_generated_count=selected_generated_count,
        selected_reused_count=selected_reused_count,
        failed_jobs=failed_jobs,
        all_selected_resolved=all(
            intent.outcome is not None for intent in selected_resolution_intents
        ),
        published_bytes=published_bytes,
        cleanup_warnings=cleanup_warnings,
    )


def _emit_status(callback: RunStatusCallback | None, event: str) -> None:
    if callback is not None:
        callback(event)


def _publish_run_outputs(
    run_plan: ExecutableRunPlan,
    *,
    invocation_token: str,
) -> tuple[tuple[_MaterializedOutputResult, ...], tuple[tuple[str, str, str], ...]]:
    """Prepare, prune, and materialize fresh outputs best-effort per job."""
    prepared, preparation_failures = _prepare_run_outputs(
        run_plan,
        invocation_token=invocation_token,
    )
    prepared, prune_failures = _prune_orphan_prepared_jobs(run_plan, prepared)
    job_order = _dependency_ordered_prepared_jobs(run_plan, prepared)
    materialized, materialization_failures = _materialize_prepared_jobs(
        run_plan,
        prepared,
        job_order=job_order,
    )
    return materialized, (
        preparation_failures + prune_failures + materialization_failures
    )


def _prepare_run_outputs(
    run_plan: ExecutableRunPlan,
    *,
    invocation_token: str,
) -> tuple[tuple[_PreparedOutput, ...], tuple[tuple[str, str, str], ...]]:
    """Validate and hash complete fresh jobs without mutating final storage."""
    publishable_outputs = _output_refs_by_key(run_plan.jobs)
    specs_by_job: dict[tuple[str, str], list[PublishedOutputSpec]] = {}
    for spec in run_plan.published_outputs:
        specs_by_job.setdefault((spec.step_name, spec.address), []).append(spec)
    results: list[_PreparedOutput] = []
    failed: list[tuple[str, str, str]] = []
    for (step_name, address), specs in specs_by_job.items():
        rows, reason = _prepare_one_job(
            run_plan,
            specs,
            publishable_outputs,
            invocation_token=invocation_token,
        )
        if reason is None:
            results.extend(rows)
        else:
            failed.append((step_name, address, reason))
    return tuple(results), tuple(failed)


def _prepare_one_job(
    run_plan: ExecutableRunPlan,
    specs: list[PublishedOutputSpec],
    publishable_outputs: dict[tuple[str, str, str], RunJobOutputRef],
    *,
    invocation_token: str,
) -> tuple[list[_PreparedOutput], str | None]:
    """Prepare one complete sibling bundle without mutating final storage.

    ``reason`` is ``None`` when the whole job prepared, else a coarse receipt,
    staging, or digest failure category.
    """
    if not specs:
        raise ValidationError("publishable job has no declared outputs")
    job_ids = {spec.job_id for spec in specs}
    request_digests = {spec.request_bundle_digest for spec in specs}
    output_names = tuple(sorted(spec.output_name for spec in specs))
    if (
        len(job_ids) != 1
        or len(request_digests) != 1
        or len(output_names) != len(set(output_names))
    ):
        raise ValidationError("publishable siblings disagree on their job contract")
    request_bundle_digest = next(iter(request_digests))
    if request_bundle_digest is None:
        raise ValidationError("publishable output request identity is unresolved")
    expected_receipt = CompletionReceipt(
        invocation_token=invocation_token,
        job_id=next(iter(job_ids)),
        request_bundle_digest=request_bundle_digest,
        outputs=output_names,
    )
    receipt_path = run_plan.run_workspace / completion_receipt_relative_path(
        expected_receipt.job_id
    )
    if not receipt_path.is_file():
        return [], "missing completion receipt"
    try:
        receipt = read_completion_receipt(receipt_path)
    except ExecutionEvidenceError:
        return [], "invalid completion receipt"
    if receipt != expected_receipt:
        return [], "invalid completion receipt"

    prepared: list[_PreparedOutput] = []
    for spec in specs:
        key = (spec.step_name, spec.output_name, spec.address)
        try:
            output_ref = publishable_outputs[key]
        except KeyError as exc:
            raise ValidationError("run plan is missing a publishable output") from exc
        try:
            before = os.lstat(output_ref.staging_path)
        except FileNotFoundError:
            return [], "missing staged output"
        except OSError:
            return [], "unreadable staged output"
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            return [], "invalid staged output"
        try:
            output_digest = sha256_file_digest(output_ref.staging_path)
            after = os.lstat(output_ref.staging_path)
        except FileNotFoundError:
            return [], "staged output changed"
        except OSError:
            return [], "unreadable staged output"
        if (
            not stat.S_ISREG(after.st_mode)
            or after.st_nlink != 1
            or (after.st_dev, after.st_ino, after.st_size)
            != (before.st_dev, before.st_ino, before.st_size)
        ):
            return [], "staged output changed"
        output_hash = short_hash(output_digest)
        if spec.request_bundle_digest is None:
            raise ValidationError("publishable output request identity is unresolved")
        final_path_relative = canonical_output_path(
            context=spec.context,
            step_name=spec.step_name,
            address=spec.address,
            request_bundle_digest=spec.request_bundle_digest,
            output_name=spec.output_name,
            output_hash=output_hash,
            declared_extension=spec.declared_extension,
        )
        _contained_canonical_output_path(
            run_plan.runtime_root,
            final_path_relative,
        )
        prepared.append(
            _PreparedOutput(
                row=PublishedOutputRow(
                    context=spec.context,
                    workflow_name=spec.workflow_name,
                    step_name=spec.step_name,
                    output_name=spec.output_name,
                    address=spec.address,
                    path=final_path_relative,
                    output_digest=output_digest,
                    output_hash=output_hash,
                ),
                staging_path=output_ref.staging_path,
                file_size=after.st_size,
                staged_device=after.st_dev,
                staged_inode=after.st_ino,
                staged_link_count=after.st_nlink,
            )
        )
    return prepared, None

def _prune_orphan_prepared_jobs(
    run_plan: ExecutableRunPlan,
    prepared_results: tuple[_PreparedOutput, ...],
) -> tuple[tuple[_PreparedOutput, ...], tuple[tuple[str, str, str], ...]]:
    """Drop prepared jobs whose fresh workflow-output parents did not prepare."""
    jobs_by_address = {(job.step_name, job.address): job for job in run_plan.jobs}
    prepared_keys = {
        (result.row.step_name, result.row.output_name, result.row.address)
        for result in prepared_results
    }
    dropped: set[tuple[str, str]] = set()
    changed = True
    while changed:
        changed = False
        for result in prepared_results:
            job_address = (result.row.step_name, result.row.address)
            if job_address in dropped:
                continue
            job = jobs_by_address.get(job_address)
            if job is None or not _has_unprepared_fresh_parent(job, prepared_keys):
                continue
            dropped.add(job_address)
            prepared_keys -= {
                key for key in prepared_keys if (key[0], key[2]) == job_address
            }
            changed = True
    survivors = tuple(
        result
        for result in prepared_results
        if (result.row.step_name, result.row.address) not in dropped
    )
    dropped_failures = tuple(
        (step_name, address, "upstream not published")
        for step_name, address in dropped
    )
    return survivors, dropped_failures


def _dependency_ordered_prepared_jobs(
    run_plan: ExecutableRunPlan,
    prepared_results: tuple[_PreparedOutput, ...],
) -> tuple[tuple[str, str], ...]:
    """Return a stable parent-before-descendant order for prepared fresh jobs."""
    prepared_jobs = {
        (result.row.step_name, result.row.address) for result in prepared_results
    }
    jobs_by_key: dict[tuple[str, str], RunJob] = {}
    plan_order: dict[tuple[str, str], int] = {}
    for index, job in enumerate(run_plan.jobs):
        key = (job.step_name, job.address)
        if key in jobs_by_key:
            raise ValidationError(f"run plan contains a duplicate job coordinate: {key}")
        jobs_by_key[key] = job
        plan_order[key] = index
    if prepared_jobs - jobs_by_key.keys():
        raise ValidationError("prepared output has no matching run job")
    parents = {
        key: _fresh_parent_job_keys(jobs_by_key[key]) & prepared_jobs
        for key in prepared_jobs
    }
    ordered: list[tuple[str, str]] = []
    remaining = set(prepared_jobs)
    while remaining:
        ready = sorted(
            (key for key in remaining if not (parents[key] & remaining)),
            key=plan_order.__getitem__,
        )
        if not ready:
            raise ValidationError("prepared fresh-job dependency graph contains a cycle")
        ordered.extend(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def _fresh_parent_job_keys(job: RunJob) -> set[tuple[str, str]]:
    return {
        (record.source_step_name, record.source_address)
        for record in job.input_records
        if record.origin == "workflow_output"
        and record.registry_source_artifact_id is None
        and record.source_step_name is not None
        and record.source_address is not None
    }


def _materialize_prepared_jobs(
    run_plan: ExecutableRunPlan,
    prepared_results: tuple[_PreparedOutput, ...],
    *,
    job_order: tuple[tuple[str, str], ...],
) -> tuple[
    tuple[_MaterializedOutputResult, ...],
    tuple[tuple[str, str, str], ...],
]:
    """Materialize prepared jobs in dependency order, preserving survivors."""
    prepared_by_job: dict[tuple[str, str], list[_PreparedOutput]] = {}
    for result in prepared_results:
        prepared_by_job.setdefault(
            (result.row.step_name, result.row.address), []
        ).append(result)
    jobs_by_key = {(job.step_name, job.address): job for job in run_plan.jobs}
    failed_jobs: set[tuple[str, str]] = set()
    results: list[_MaterializedOutputResult] = []
    failures: list[tuple[str, str, str]] = []
    for job_key in job_order:
        job = jobs_by_key[job_key]
        if _fresh_parent_job_keys(job) & failed_jobs:
            failed_jobs.add(job_key)
            failures.append((*job_key, "upstream materialization failed"))
            continue
        job_results, reason = _materialize_one_prepared_job(
            run_plan,
            prepared_by_job[job_key],
        )
        if reason is None:
            results.extend(job_results)
        else:
            failed_jobs.add(job_key)
            failures.append((*job_key, reason))
    return tuple(results), tuple(failures)


def _materialize_one_prepared_job(
    run_plan: ExecutableRunPlan,
    prepared_outputs: list[_PreparedOutput],
) -> tuple[list[_MaterializedOutputResult], str | None]:
    results: list[_MaterializedOutputResult] = []
    try:
        for prepared in prepared_outputs:
            final_path = _contained_canonical_output_path(
                run_plan.runtime_root,
                prepared.row.path,
            )
            final_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.lstat(final_path)
            except FileNotFoundError:
                _recheck_prepared_staging(prepared)
                try:
                    os.replace(prepared.staging_path, final_path)
                except OSError as exc:
                    if exc.errno == errno.EXDEV:
                        return [], "cross-filesystem publication unsupported"
                    raise
            else:
                _validate_existing_published_file(
                    final_path,
                    expected_digest=prepared.row.output_digest,
                )
            results.append(
                _MaterializedOutputResult(
                    row=prepared.row,
                    staging_path=prepared.staging_path,
                    file_size=prepared.file_size,
                )
            )
    except ValidationError:
        return [], "materialization validation failed"
    except OSError:
        return [], "materialization failed"
    return results, None


def _has_unprepared_fresh_parent(
    job: RunJob,
    prepared_keys: set[tuple[str, str, str]],
) -> bool:
    for record in job.input_records:
        if record.origin != "workflow_output" or record.registry_source_artifact_id is not None:
            continue
        parent_key = (
            record.source_step_name,
            record.source_output_name,
            record.source_address,
        )
        if parent_key not in prepared_keys:
            return True
    return False


def _validate_existing_published_file(path: Path, *, expected_digest: str) -> None:
    try:
        path_stat = os.lstat(path)
    except FileNotFoundError as exc:
        raise ValidationError(f"missing published output file: {path}") from exc
    if not stat.S_ISREG(path_stat.st_mode):
        raise ValidationError(f"published output path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise ValidationError(f"published output path has multiple hardlinks: {path}")
    if sha256_file_digest(path) != expected_digest:
        raise ValidationError("published output artifact digest mismatch")


def _recheck_prepared_staging(prepared: _PreparedOutput) -> None:
    try:
        current = os.lstat(prepared.staging_path)
    except FileNotFoundError as exc:
        raise ValidationError(
            f"prepared staged output is missing: {prepared.staging_path}"
        ) from exc
    if (
        not stat.S_ISREG(current.st_mode)
        or current.st_nlink != 1
        or current.st_nlink != prepared.staged_link_count
        or (current.st_dev, current.st_ino, current.st_size)
        != (prepared.staged_device, prepared.staged_inode, prepared.file_size)
    ):
        raise ValidationError(
            f"prepared staged output changed before materialization: {prepared.staging_path}"
        )


def _finalize_published_output_staging(
    results: tuple[_MaterializedOutputResult, ...],
) -> tuple[str, ...]:
    """Remove retained successful output staging after registry commit."""
    warnings: list[str] = []
    for staging_path in dict.fromkeys(result.staging_path for result in results):
        try:
            os.lstat(staging_path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            warnings.append(f"could not inspect published staging {staging_path}: {exc}")
            continue
        try:
            staging_path.unlink()
        except OSError as exc:
            warnings.append(f"could not remove published staging {staging_path}: {exc}")
    return tuple(warnings)


def _remove_expected_staged_outputs(run_plan: ExecutableRunPlan) -> None:
    publishable_outputs = _output_refs_by_key(run_plan.jobs)
    for spec in run_plan.published_outputs:
        key = (spec.step_name, spec.output_name, spec.address)
        try:
            output_ref = publishable_outputs[key]
        except KeyError as exc:
            raise ValidationError("run plan is missing a publishable output") from exc
        path = output_ref.staging_path
        if path.is_dir():
            raise ValidationError(f"staged output path is a directory: {path}")
        if path.exists() or path.is_symlink():
            path.unlink()


def _remove_expected_completion_receipts(run_plan: ExecutableRunPlan) -> None:
    for job in run_plan.jobs:
        path = run_plan.run_workspace / completion_receipt_relative_path(job.job_id)
        if path.is_dir():
            raise ValidationError(f"completion receipt path is a directory: {path}")
        if path.exists() or path.is_symlink():
            path.unlink()


def _write_run_workspace(
    run_plan: ExecutableRunPlan,
    *,
    invocation_token: str | None,
    supplied_input_paths: dict[str, str] | None = None,
    prepared_reused_inputs: _PreparedReusedInputs | None = None,
) -> None:
    (run_plan.run_workspace / "staging").mkdir(exist_ok=True)
    (run_plan.run_workspace / "logs").mkdir(exist_ok=True)
    payload = _run_plan_payload(
        run_plan,
        invocation_token=invocation_token,
        supplied_input_paths=supplied_input_paths,
        prepared_reused_inputs=prepared_reused_inputs,
    )
    try:
        write_json_atomic(run_plan.run_workspace / "run_plan.json", payload)
    except ExecutionEvidenceError as exc:
        raise ValidationError(str(exc)) from exc
    selected_outputs = [
        output_ref.staging_path_relative
        for output_ref in run_plan.selected_fresh_output_refs
    ]
    _write_text_file(
        run_plan.run_workspace / "selected_outputs.txt",
        "\n".join(selected_outputs) + "\n",
    )
    _write_text_file(
        run_plan.run_workspace / "Snakefile",
        _snakefile_text(run_plan, run_plan_payload=payload),
    )


def _write_reuse_only_workspace(
    run_plan: ExecutableRunPlan,
    *,
    invocation_token: str | None,
) -> None:
    _remove_stale_executor_file(run_plan.run_workspace / "Snakefile")
    _remove_stale_executor_file(run_plan.run_workspace / "selected_outputs.txt")
    try:
        write_json_atomic(
            run_plan.run_workspace / "run_plan.json",
            _run_plan_payload(run_plan, invocation_token=invocation_token),
        )
    except ExecutionEvidenceError as exc:
        raise ValidationError(str(exc)) from exc


def _preflight_publication_layout(run_plan: ExecutableRunPlan) -> None:
    """Validate canonical fresh-output destinations without mutating the workspace."""
    layout_root = run_plan.runtime_root / CANONICAL_OUTPUT_ROOT
    if layout_root.is_symlink() or not layout_root.is_dir():
        raise ValidationError("canonical output root must be a real directory")
    resolved_runtime_root = run_plan.runtime_root.resolve()
    resolved_layout_root = layout_root.resolve()
    if not _path_contains_or_same(resolved_runtime_root, resolved_layout_root):
        raise ValidationError("canonical output root must stay inside runtime dir")
    for spec in run_plan.published_outputs:
        if spec.request_bundle_digest is None:
            if run_plan.dry_run:
                continue
            raise ValidationError("publishable output request identity is unresolved")
        relative_directory = canonical_output_directory(
            context=spec.context,
            step_name=spec.step_name,
            address=spec.address,
            request_bundle_digest=spec.request_bundle_digest,
            output_name=spec.output_name,
        )
        _contained_canonical_output_path(
            run_plan.runtime_root,
            relative_directory,
        )


def _contained_canonical_output_path(runtime_root: Path, relative_path: str) -> Path:
    path = Path(relative_path)
    if path.is_absolute() or ".." in path.parts:
        raise ValidationError("canonical output path must be runtime-relative")
    expected_prefix = CANONICAL_OUTPUT_ROOT.parts
    if path.parts[: len(expected_prefix)] != expected_prefix:
        raise ValidationError("canonical output path must be under outputs/v1/")
    resolved_runtime_root = runtime_root.resolve()
    resolved_layout_root = (runtime_root / CANONICAL_OUTPUT_ROOT).resolve()
    resolved_path = (runtime_root / path).resolve()
    if not _path_contains_or_same(resolved_runtime_root, resolved_path):
        raise ValidationError("canonical output path must stay inside runtime dir")
    if not _path_contains_or_same(resolved_layout_root, resolved_path):
        raise ValidationError("canonical output path must stay inside outputs/v1/")
    return resolved_path


def _path_contains_or_same(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _prepare_run_workspace(run_plan: ExecutableRunPlan) -> None:
    run_plan.run_workspace.mkdir(parents=True, exist_ok=True)
    _remove_stale_executor_file(run_plan.run_workspace / "logs" / "snakemake.log")


def _remove_stale_executor_file(path: Path) -> None:
    if path.is_dir():
        raise ValidationError(f"executor-owned path is a directory: {path}")
    if path.exists() or path.is_symlink():
        path.unlink()


def _dry_run_forecast_input_paths(run_plan: ExecutableRunPlan) -> dict[str, str]:
    """Map forecast reused staging inputs to metadata-valid registered paths.

    Keys derive from the in-memory validated ``ReusedRunJobOutputRef``s, never
    from the serialized run plan. This forecast may refresh the planned
    candidate's registered path while checking identity, dependencies, output
    containment, existence, and size without hashing bytes. Real execution does
    not use this path: it resolves once under the runtime lock and hydration
    consumes those frozen exact occurrences without another resolver query.
    """
    candidates = _resolve_dry_run_forecast_bundles(run_plan)
    mapping: dict[str, str] = {}
    for output_ref in run_plan.reused_outputs:
        candidate = candidates[output_ref.source_artifact_id]
        mapping[output_ref.staging_path_relative] = os.path.relpath(
            run_plan.runtime_root / candidate.path,
            run_plan.run_workspace,
        ).replace(os.sep, "/")
    return mapping


def _prepare_reused_inputs(
    run_plan: ExecutableRunPlan,
) -> _PreparedReusedInputs:
    candidates = _exact_reused_candidates(run_plan)
    verified_occurrences: dict[tuple[Path, str, int], Path] = {}
    _verify_selected_reused_outputs(
        run_plan, candidates=candidates, verified_occurrences=verified_occurrences
    )
    prepared: list[_PreparedReusedInput] = []
    for output_ref in run_plan.reused_outputs:
        candidate = candidates[output_ref.source_artifact_id]
        source_path = _verify_reused_canonical_occurrence(
            run_plan,
            candidate,
            verified_occurrences=verified_occurrences,
        )
        output_ref.staging_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, output_ref.staging_path)
        if output_ref.staging_path.stat().st_size != candidate.file_size:
            raise ValidationError("hydrated artifact file size mismatch")
        if sha256_file_digest(output_ref.staging_path) != candidate.content_digest:
            raise ValidationError("hydrated artifact digest mismatch")
        prepared.append(
            _PreparedReusedInput(
                output_ref=output_ref,
                candidate=candidate,
                bound_occurrence_path=source_path,
                supplied_path=output_ref.staging_path,
            )
        )
    return _PreparedReusedInputs(candidates=candidates, inputs=tuple(prepared))


def _verify_selected_reused_outputs(
    run_plan: ExecutableRunPlan,
    *,
    candidates: dict[int, ReusableArtifactCandidate],
    verified_occurrences: dict[tuple[Path, str, int], Path],
) -> None:
    for selected_ref in run_plan.selected_reused_output_refs:
        for output_name, planned_artifact_id in selected_ref.planned_sibling_artifact_ids:
            try:
                candidate = candidates[planned_artifact_id]
            except KeyError as exc:
                raise ValidationError(
                    "selected reused bundle is missing its execution-time artifact"
                ) from exc
            if (
                candidate.step_name != selected_ref.step_name
                or candidate.output_name != output_name
                or candidate.address != selected_ref.address
            ):
                raise ValidationError(
                    "selected reused artifact has the wrong requested coordinate"
                )
            _verify_reused_canonical_occurrence(
                run_plan,
                candidate,
                verified_occurrences=verified_occurrences,
            )


def _verify_reused_canonical_occurrence(
    run_plan: ExecutableRunPlan,
    candidate: ReusableArtifactCandidate,
    *,
    verified_occurrences: dict[tuple[Path, str, int], Path],
) -> Path:
    """Verify one exact frozen canonical occurrence and return its lexical path."""
    if candidate.published_path != candidate.path:
        raise ValidationError("reused artifact publication path is inconsistent")
    if not candidate.path.endswith(candidate.extension):
        raise ValidationError("reused artifact extension is inconsistent")
    if candidate.output_hash != short_hash(candidate.content_digest):
        raise ValidationError("reused artifact content hash is inconsistent")
    expected_path = canonical_output_path(
        context=run_plan.context,
        step_name=candidate.step_name,
        address=candidate.address,
        request_bundle_digest=candidate.request_bundle_digest,
        output_name=candidate.output_name,
        output_hash=candidate.output_hash,
        declared_extension=candidate.extension,
    )
    if candidate.path != expected_path:
        raise ValidationError(
            "reused artifact canonical path does not match its request identity"
        )
    resolved_path = _contained_canonical_output_path(
        run_plan.runtime_root,
        candidate.path,
    )
    lexical_path = run_plan.runtime_root / candidate.path
    try:
        before = os.lstat(lexical_path)
    except FileNotFoundError as exc:
        raise ValidationError(
            f"reused artifact canonical occurrence is missing: {candidate.path}"
        ) from exc
    except OSError as exc:
        raise ValidationError(
            f"reused artifact canonical occurrence is unreadable: {candidate.path}"
        ) from exc
    if not stat.S_ISREG(before.st_mode):
        raise ValidationError(
            "reused artifact canonical occurrence is not a regular file"
        )
    if before.st_nlink != 1:
        raise ValidationError("reused artifact canonical occurrence has multiple links")
    if before.st_size != candidate.file_size:
        raise ValidationError("reused artifact canonical occurrence size mismatch")
    occurrence_key = (
        resolved_path,
        candidate.content_digest,
        candidate.file_size,
    )
    prior = verified_occurrences.get(occurrence_key)
    if prior is not None:
        return prior

    try:
        with lexical_path.open("rb") as handle:
            opened_before = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened_before.st_mode)
                or opened_before.st_nlink != 1
                or (opened_before.st_dev, opened_before.st_ino, opened_before.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise ValidationError(
                    "reused artifact canonical occurrence changed before verification"
                )
            observed_digest = _sha256_open_file(handle)
            opened_after = os.fstat(handle.fileno())
    except FileNotFoundError as exc:
        raise ValidationError(
            f"reused artifact canonical occurrence is missing: {candidate.path}"
        ) from exc
    except OSError as exc:
        raise ValidationError(
            f"reused artifact canonical occurrence is unreadable: {candidate.path}"
        ) from exc

    try:
        after = os.lstat(lexical_path)
    except FileNotFoundError as exc:
        raise ValidationError(
            "reused artifact canonical occurrence changed during verification"
        ) from exc
    except OSError as exc:
        raise ValidationError(
            f"reused artifact canonical occurrence is unreadable: {candidate.path}"
        ) from exc
    expected_stat = (before.st_dev, before.st_ino, before.st_size, before.st_nlink)
    if (
        not stat.S_ISREG(opened_after.st_mode)
        or opened_after.st_nlink != 1
        or (
            opened_after.st_dev,
            opened_after.st_ino,
            opened_after.st_size,
            opened_after.st_nlink,
        )
        != expected_stat
        or not stat.S_ISREG(after.st_mode)
        or after.st_nlink != 1
        or (after.st_dev, after.st_ino, after.st_size, after.st_nlink)
        != expected_stat
    ):
        raise ValidationError(
            "reused artifact canonical occurrence changed during verification"
        )
    if observed_digest != candidate.content_digest:
        raise ValidationError("reused artifact canonical occurrence digest mismatch")
    verified_occurrences[occurrence_key] = lexical_path
    return lexical_path


def _sha256_open_file(handle: BinaryIO) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def _resolve_dry_run_forecast_bundles(
    run_plan: ExecutableRunPlan,
) -> dict[int, ReusableArtifactCandidate]:
    refs_by_request: dict[
        ReusableArtifactBundleRequest,
        list[ReusedRunJobOutputRef],
    ] = {}
    for output_ref in run_plan.reused_validation_outputs:
        refs_by_request.setdefault(output_ref.reuse_request, []).append(output_ref)

    bundles: dict[
        ReusableArtifactBundleRequest,
        ReusableArtifactBundleCandidate,
    ] = {}
    for request, output_refs in refs_by_request.items():
        preferred_artifact_ids = output_refs[0].source_bundle_artifact_ids
        if any(
            output_ref.source_bundle_artifact_ids != preferred_artifact_ids
            for output_ref in output_refs[1:]
        ):
            raise ValidationError("reused outputs disagree on their planned bundle")
        bundle = resolve_reusable_artifact_bundle(
            run_plan.runtime_root / REGISTRY_DB_PATH,
            runtime_root=run_plan.runtime_root,
            request=request,
            preferred_artifact_ids=preferred_artifact_ids,
        )
        if bundle is None:
            raise ValidationError("reusable artifact bundle is no longer valid")
        bundles[request] = bundle
    resolved: dict[int, ReusableArtifactCandidate] = {}
    for request, output_refs in refs_by_request.items():
        planned_artifact_ids = output_refs[0].source_bundle_artifact_ids
        output_names = sorted(dict(request.sibling_outputs))
        if len(planned_artifact_ids) != len(output_names):
            raise ValidationError(
                "reused artifact bundle does not match its sibling contract"
            )
        bundle = bundles[request]
        for planned_artifact_id, output_name in zip(
            planned_artifact_ids,
            output_names,
            strict=True,
        ):
            candidate = bundle.output(output_name)
            prior = resolved.get(planned_artifact_id)
            if prior is not None and prior.artifact_id != candidate.artifact_id:
                raise ValidationError(
                    "reused artifact resolves inconsistently across bundle requests"
                )
            resolved[planned_artifact_id] = candidate
    return resolved


def _exact_reused_candidates(
    run_plan: ExecutableRunPlan,
) -> dict[int, ReusableArtifactCandidate]:
    """Return the sole under-lock resolved occurrences without querying again."""
    candidates: dict[int, ReusableArtifactCandidate] = {}
    for output_ref in run_plan.reused_validation_outputs:
        if output_ref.candidate.artifact_id != output_ref.source_artifact_id:
            raise ValidationError("frozen reused occurrence identity is inconsistent")
        for candidate in output_ref.bundle.outputs:
            prior = candidates.get(candidate.artifact_id)
            if prior is not None and prior != candidate:
                raise ValidationError("frozen reused occurrence is inconsistent")
            candidates[candidate.artifact_id] = candidate
    return candidates


def _run_snakemake(run_plan: ExecutableRunPlan, *, cores: int, dry_run: bool) -> int:
    command = [
        sys.executable,
        "-m",
        "snakemake",
        "--snakefile",
        "Snakefile",
        "--cores",
        str(cores),
        "--rerun-incomplete",
        "--keep-going",
        "--profile",
        "none",
        "--workflow-profile",
        "none",
    ]
    if dry_run:
        command.append("--dry-run")
    command.extend(
        output_ref.staging_path_relative
        for output_ref in run_plan.selected_fresh_output_refs
    )
    env = os.environ.copy()
    env.pop("SNAKEMAKE_PROFILE", None)
    env["XDG_CACHE_HOME"] = str(run_plan.run_workspace / ".cache")
    src_path = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = (
        src_path
        if not env.get("PYTHONPATH")
        else f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
    )
    try:
        result = subprocess.run(
            command,
            cwd=run_plan.run_workspace,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ValidationError("could not execute Python interpreter for Snakemake") from exc
    log_path = run_plan.run_workspace / "logs" / "snakemake.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "STDOUT\n"
        f"{result.stdout}\n"
        "STDERR\n"
        f"{result.stderr}\n",
        encoding="utf-8",
    )
    # Return the exit code without raising: a non-zero exit means at least one
    # job failed under --keep-going, but the jobs that finished are still
    # publishable. execute_run_plan decides success/partial/hard-error from the
    # filesystem + this code (§3.1). Only a launch failure (above) hard-aborts.
    return result.returncode


def _snakefile_text(
    run_plan: ExecutableRunPlan,
    *,
    run_plan_payload: dict[str, Any] | None = None,
) -> str:
    payload = run_plan_payload or _run_plan_payload(run_plan)
    payload_jobs = payload["jobs"]
    if run_plan.dry_run:
        lines = [
            "# Generated by NIPACT for a dry run. Do not edit.",
            "# Dry-run planning workspace — not intended for manual execution.",
            "",
        ]
    else:
        lines = [
            "# Generated by NIPACT. Do not edit.",
            "",
        ]
    for index, job in enumerate(run_plan.jobs):
        job_payload = payload_jobs[job.job_id]
        inputs = [
            "run_plan.json",
            *(
                path
                for paths in job_payload["inputs"].values()
                for path in paths
            ),
        ]
        outputs = list(job_payload["outputs"].values())
        shell_text = shlex.join(
            [
                sys.executable,
                "-m",
                "nipact.runtime",
                "run-job",
                "--run-plan",
                "run_plan.json",
                "--job-id",
                job.job_id,
            ]
        )
        lines.extend(
            [
                f"rule nipact_job_{index:04d}:",
                "    input:",
                *[f"        {json.dumps(path)}," for path in inputs],
                "    output:",
                *_snakefile_output_lines(outputs),
                f"    shell: {json.dumps(shell_text)}",
                "",
            ]
        )
    return "\n".join(lines)


def _snakefile_output_lines(outputs: list[str]) -> list[str]:
    if len(outputs) == 1:
        return [f"        {json.dumps(outputs[0])}"]
    return [f"        {json.dumps(path)}," for path in outputs]


def _run_plan_payload(
    run_plan: ExecutableRunPlan,
    *,
    invocation_token: str | None = None,
    supplied_input_paths: dict[str, str] | None = None,
    prepared_reused_inputs: _PreparedReusedInputs | None = None,
) -> dict[str, Any]:
    if supplied_input_paths is not None and prepared_reused_inputs is not None:
        raise ValidationError(
            "run plan cannot combine forecast and prepared reused-input paths"
        )
    prepared_payload, prepared_paths = _prepared_reused_input_payload(
        run_plan,
        prepared_reused_inputs,
    )
    path_substitutions = supplied_input_paths or prepared_paths
    return {
        "schema_version": RUN_PLAN_SCHEMA_VERSION,
        "invocation_token": invocation_token,
        "context": run_plan.context,
        "workflow": run_plan.workflow_name,
        "base_workflow": run_plan.base_workflow_name,
        "selected_step": run_plan.selected_step_name,
        "selected_output": run_plan.selected_output_name,
        "requested_address": run_plan.requested_address,
        "runtime_root": str(run_plan.runtime_root),
        "execution_population": _execution_population_payload(
            run_plan.execution_population
        ),
        "manifest_bindings": [
            _manifest_binding_payload(binding)
            for binding in run_plan.manifest_bindings
        ],
        "prepared_reused_inputs": prepared_payload,
        "jobs": {
            job.job_id: {
                "step_name": job.step_name,
                "address": job.address,
                "callable_ref": job.callable_ref,
                "request_bundle_digest": (
                    job.projection_state.request_bundle_digest
                    if isinstance(
                        job.projection_state,
                        ResolvedRequestBundleProjectionV3,
                    )
                    else None
                ),
                "declared_outputs": sorted(job.outputs),
                "completion_receipt_path": completion_receipt_relative_path(
                    job.job_id
                ),
                "outputs": {
                    output_name: output.staging_path_relative
                    for output_name, output in sorted(job.outputs.items())
                },
                "inputs": {
                    name: [path_substitutions.get(path, path) for path in paths]
                    for name, paths in job.inputs.items()
                },
                "input_records": [
                    _input_record_payload(
                        record,
                        supplied_input_path=path_substitutions.get(
                            record.input_path,
                            record.input_path,
                        ),
                    )
                    for record in job.input_records
                ],
                "params": job.params,
            }
            for job in run_plan.jobs
        },
        "selected_outputs": sorted(
            (
                {
                    "step_name": output_ref.step_name,
                    "output_name": output_ref.output_name,
                    "address": output_ref.address,
                }
                for output_ref in (
                    *run_plan.selected_fresh_output_refs,
                    *run_plan.selected_reused_output_refs,
                )
            ),
            key=lambda item: (
                item["step_name"], item["output_name"], item["address"]
            ),
        ),
        "selected_fresh_outputs": [
            {
                "step_name": output_ref.step_name,
                "output_name": output_ref.output_name,
                "address": output_ref.address,
                "staging_path": output_ref.staging_path_relative,
            }
            for output_ref in sorted(
                run_plan.selected_fresh_output_refs,
                key=lambda value: (value.step_name, value.output_name, value.address),
            )
        ],
        "selected_reused_outputs": [
            {
                "step_name": output_ref.step_name,
                "output_name": output_ref.output_name,
                "address": output_ref.address,
                "request_bundle_digest": (
                    output_ref.reuse_request.resolved_projection.request_bundle_digest
                ),
                "planned_sibling_artifacts": [
                    {"output_name": output_name, "artifact_id": artifact_id}
                    for output_name, artifact_id in output_ref.planned_sibling_artifact_ids
                ],
            }
            for output_ref in sorted(
                run_plan.selected_reused_output_refs,
                key=lambda value: (value.step_name, value.output_name, value.address),
            )
        ],
    }


def _prepared_reused_input_payload(
    run_plan: ExecutableRunPlan,
    prepared_reused_inputs: _PreparedReusedInputs | None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    if prepared_reused_inputs is None:
        return [], {}

    expected = {ref.source_artifact_id: ref for ref in run_plan.reused_outputs}
    if len(expected) != len(run_plan.reused_outputs):
        raise ValidationError("reused run inputs contain duplicate artifact IDs")

    prepared_by_artifact: dict[int, _PreparedReusedInput] = {}
    for prepared in prepared_reused_inputs.inputs:
        artifact_id = prepared.candidate.artifact_id
        if artifact_id in prepared_by_artifact:
            raise ValidationError("prepared reused inputs contain duplicate artifact IDs")
        output_ref = expected.get(artifact_id)
        if output_ref is None or output_ref != prepared.output_ref:
            raise ValidationError("prepared reused input does not match the executable plan")
        if prepared_reused_inputs.candidates.get(artifact_id) != prepared.candidate:
            raise ValidationError("prepared reused input candidate is inconsistent")
        expected_bound = run_plan.runtime_root / prepared.candidate.path
        if prepared.bound_occurrence_path != expected_bound:
            raise ValidationError("prepared reused input occurrence is inconsistent")
        prepared_by_artifact[artifact_id] = prepared

    if set(prepared_by_artifact) != set(expected):
        raise ValidationError("prepared reused inputs do not cover executable reuse")

    payload: list[dict[str, Any]] = []
    substitutions: dict[str, str] = {}
    for artifact_id, prepared in sorted(prepared_by_artifact.items()):
        supplied_path = os.path.relpath(
            prepared.supplied_path,
            run_plan.run_workspace,
        ).replace(os.sep, "/")
        prior = substitutions.setdefault(
            prepared.output_ref.staging_path_relative,
            supplied_path,
        )
        if prior != supplied_path:
            raise ValidationError("prepared reused input locator is inconsistent")
        payload.append(
            {
                "artifact_id": artifact_id,
                "bound_occurrence_path": _runtime_relative_path(
                    run_plan.runtime_root,
                    prepared.bound_occurrence_path,
                ),
                "supplied_path": supplied_path,
            }
        )
    return payload, substitutions


def _execution_population_payload(
    population: WorkflowPlanExecutionPopulation | None,
) -> dict[str, Any] | None:
    if population is None:
        return None
    return {
        "manifest_name": population.manifest_name,
        "manifest_value_schema": population.manifest_value_schema,
        "manifest_digest": population.manifest_digest,
        "manifest_hash": population.manifest_hash,
        "entity_ids": list(population.entity_ids),
        "entity_count": population.entity_count,
    }


def _manifest_binding_payload(
    binding: WorkflowPlanManifestBinding,
) -> dict[str, Any]:
    return {
        "step_name": binding.step_name,
        "manifest_usage_role": binding.manifest_usage_role,
        "manifest_name": binding.manifest_name,
        "manifest_value_schema": binding.manifest_value_schema,
        "manifest_digest": binding.manifest_digest,
        "manifest_hash": binding.manifest_hash,
        "entity_ids": list(binding.entity_ids),
        "entity_count": binding.entity_count,
    }


def _input_record_payload(
    record: ArtifactInputRow,
    *,
    supplied_input_path: str | None = None,
) -> dict[str, Any]:
    return {
        "binding_name": record.binding_name,
        "input_path": supplied_input_path or record.input_path,
        "dependency_role": record.dependency_role,
        "origin": record.origin,
        "source_step_name": record.source_step_name,
        "source_output_name": record.source_output_name,
        "source_address": record.source_address,
        "source_callable_ref": record.source_callable_ref,
        "source_parameters_json": record.source_parameters_json,
        "source_extension": record.source_extension,
        "source_execution_role": record.source_execution_role,
        "source_is_reused": record.source_is_reused,
        "source_artifact_path": record.source_artifact_path,
        "source_scope": record.source_scope,
        "source_name": record.source_name,
        "source_entity_id": record.source_entity_id,
        "source_content_digest": record.source_content_digest,
        "source_file_size": record.source_file_size,
        "manifest_value_schema": record.manifest_value_schema,
        "manifest_digest": record.manifest_digest,
        "edge_cardinality": record.edge_cardinality,
        "registry_source_artifact_id": record.registry_source_artifact_id,
        "source_input_records": [
            _input_record_payload(source_record)
            for source_record in record.source_input_records
        ],
    }


def _workflow_output_artifact_rows(
    run_plan: ExecutableRunPlan,
    *,
    published_results: tuple[_MaterializedOutputResult, ...],
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
    prepared_reused_inputs: _PreparedReusedInputs,
) -> tuple[WorkflowOutputArtifactRow, ...]:
    published_by_key = {
        (result.row.step_name, result.row.output_name, result.row.address): result
        for result in published_results
    }
    published_jobs = {
        (result.row.step_name, result.row.address) for result in published_results
    }
    selected_output_keys = {
        (output_ref.step_name, output_ref.output_name, output_ref.address)
        for output_ref in run_plan.selected_fresh_output_refs
    }
    reachable_job_ids = _reachable_job_ids(run_plan)
    rows: list[WorkflowOutputArtifactRow] = []
    for job in run_plan.jobs:
        if job.job_id not in reachable_job_ids:
            continue
        # Record exactly the jobs §3.2 published: publishing is job-atomic, so a
        # published job has every sibling in published_by_key, and a skipped job
        # contributes no rows. This keeps the recorded set identical to the
        # published set instead of diverging from staging-file presence.
        if (job.step_name, job.address) not in published_jobs:
            continue
        for output_name in job.outputs:
            output_ref = job.output_ref(output_name)
            key = (output_ref.step_name, output_ref.output_name, output_ref.address)
            published_result = published_by_key[key]
            staging_path = _runtime_relative_path(
                run_plan.runtime_root,
                output_ref.staging_path,
            )
            rows.append(
                WorkflowOutputArtifactRow(
                    step_name=output_ref.step_name,
                    output_name=output_ref.output_name,
                    address=output_ref.address,
                    job_id=output_ref.job_id,
                    path=published_result.row.path,
                    staging_path=staging_path,
                    published_path=published_result.row.path,
                    content_digest=published_result.row.output_digest,
                    output_hash=published_result.row.output_hash,
                    file_size=published_result.file_size,
                    extension=output_ref.declared_extension,
                    parameters_json=_compact_json(output_ref.params),
                    callable_ref=output_ref.callable_ref,
                    is_selected_output=key in selected_output_keys,
                    is_published=True,
                    input_records=tuple(
                        _actual_input_record(
                            record,
                            actual_reused_artifacts,
                            prepared_reused_inputs,
                            run_workspace=run_plan.run_workspace,
                        )
                        for record in output_ref.input_records
                    ),
                )
            )
    return tuple(rows)


def _actual_input_record(
    record: ArtifactInputRow,
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
    prepared_reused_inputs: _PreparedReusedInputs,
    *,
    run_workspace: Path,
    apply_supplied_locator: bool = True,
) -> ArtifactInputRow:
    nested = tuple(
        _actual_input_record(
            source_record,
            actual_reused_artifacts,
            prepared_reused_inputs,
            run_workspace=run_workspace,
            apply_supplied_locator=False,
        )
        for source_record in record.source_input_records
    )
    artifact_id = record.registry_source_artifact_id
    if artifact_id is None or record.origin == "source":
        return replace(record, source_input_records=nested)
    candidate = actual_reused_artifacts.get(artifact_id)
    if candidate is None:
        raise ValidationError(
            "executed run is missing a frozen reused artifact dependency"
        )
    input_path = record.input_path
    if apply_supplied_locator:
        matching = tuple(
            prepared
            for prepared in prepared_reused_inputs.inputs
            if prepared.candidate.artifact_id == artifact_id
        )
        if len(matching) != 1:
            raise ValidationError(
                "executed run is missing its prepared reused-input locator"
            )
        prepared = matching[0]
        if record.input_path != prepared.output_ref.staging_path_relative:
            raise ValidationError(
                "prepared reused-input locator does not match its input record"
            )
        input_path = os.path.relpath(
            prepared.supplied_path,
            run_workspace,
        ).replace(os.sep, "/")
    return replace(
        record,
        input_path=input_path,
        registry_source_artifact_id=candidate.artifact_id,
        source_extension=candidate.extension,
        source_input_records=nested,
    )


def _retained_projection_recipes(
    run_plan: ExecutableRunPlan,
    *,
    published_rows: tuple[PublishedOutputRow, ...],
) -> tuple[RetainedJobProjectionRecipe, ...]:
    published_jobs = {(row.step_name, row.address) for row in published_rows}
    reachable_job_ids = _reachable_job_ids(run_plan)
    return tuple(
        RetainedJobProjectionRecipe(
            step_name=job.step_name,
            address=job.address,
            output_names=tuple(job.outputs),
            projection_plan=job.projection_plan,
        )
        for job in run_plan.jobs
        if job.job_id in reachable_job_ids
        and (job.step_name, job.address) in published_jobs
    )


def _reused_projection_seeds(
    run_plan: ExecutableRunPlan,
    *,
    artifact_rows: tuple[WorkflowOutputArtifactRow, ...],
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
) -> tuple[ReusedProjectionSeed, ...]:
    required_coordinates = {
        RequestedOutputCoordinate(
            namespace=run_plan.context,
            step_name=record.source_step_name,
            output_name=record.source_output_name,
            address=record.source_address,
        )
        for artifact_row in artifact_rows
        for record in artifact_row.input_records
        if record.origin == "workflow_output"
        and record.registry_source_artifact_id is not None
        and record.source_step_name is not None
        and record.source_output_name is not None
        and record.source_address is not None
    }
    seeds: list[ReusedProjectionSeed] = []
    for output_ref in run_plan.reused_outputs:
        requested_output = RequestedOutputCoordinate(
            namespace=run_plan.context,
            step_name=output_ref.step_name,
            output_name=output_ref.output_name,
            address=output_ref.address,
        )
        if requested_output not in required_coordinates:
            continue
        if not isinstance(output_ref.projection_state, ResolvedRequestBundleProjectionV3):
            raise ValidationError("reused output has an unresolved request projection")
        try:
            actual_candidate = actual_reused_artifacts[output_ref.source_artifact_id]
        except KeyError as exc:
            raise ValidationError(
                "reused output is missing its execution-time artifact"
            ) from exc
        seeds.append(
            ReusedProjectionSeed(
                requested_output=requested_output,
                actual_artifact_id=actual_candidate.artifact_id,
                request_bundle_digest=actual_candidate.request_bundle_digest,
            )
        )
    if {seed.requested_output for seed in seeds} != required_coordinates:
        raise ValidationError("retained closure is missing a reused projection seed")
    return tuple(seeds)


def _reused_membership_intents(
    run_plan: ExecutableRunPlan,
    *,
    published_rows: tuple[PublishedOutputRow, ...],
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
) -> tuple[MembershipIntent, ...]:
    """Adopt coherent reused bundles in the retained successful closure."""
    retained_jobs = {(row.step_name, row.address) for row in published_rows}
    refs_by_planned_artifact_id: dict[int, ReusedRunJobOutputRef] = {}
    for output_ref in run_plan.reused_validation_outputs:
        for artifact_id in output_ref.source_bundle_artifact_ids:
            prior = refs_by_planned_artifact_id.get(artifact_id)
            if prior is not None and prior.reuse_request != output_ref.reuse_request:
                raise ValidationError(
                    "planned reused artifact belongs to conflicting bundle requests"
                )
            refs_by_planned_artifact_id[artifact_id] = output_ref

    pending = [
        record.registry_source_artifact_id
        for job in run_plan.jobs
        if (job.step_name, job.address) in retained_jobs
        for record in job.input_records
        if record.origin == "workflow_output"
        and record.registry_source_artifact_id is not None
    ]
    pending.extend(
        artifact_id
        for selected_ref in run_plan.selected_reused_output_refs
        for _output_name, artifact_id in selected_ref.planned_sibling_artifact_ids
    )
    seen_requests: set[ReusableArtifactBundleRequest] = set()
    intents_by_coordinate: dict[tuple[str, str, str], MembershipIntent] = {}
    while pending:
        planned_artifact_id = pending.pop()
        try:
            output_ref = refs_by_planned_artifact_id[planned_artifact_id]
        except KeyError as exc:
            raise ValidationError(
                "retained reused dependency is missing its planned bundle"
            ) from exc
        request = output_ref.reuse_request
        if request in seen_requests:
            continue
        seen_requests.add(request)

        output_names = sorted(dict(request.sibling_outputs))
        planned_ids = output_ref.source_bundle_artifact_ids
        if len(planned_ids) != len(output_names):
            raise ValidationError(
                "retained reused bundle does not match its sibling contract"
            )
        for sibling_artifact_id, output_name in zip(
            planned_ids,
            output_names,
            strict=True,
        ):
            try:
                candidate = actual_reused_artifacts[sibling_artifact_id]
            except KeyError as exc:
                raise ValidationError(
                    "retained reused bundle is missing its execution candidate"
                ) from exc
            if (
                candidate.step_name != request.step_name
                or candidate.output_name != output_name
                or candidate.address != request.address
            ):
                raise ValidationError(
                    "frozen reusable artifact has the wrong requested coordinate"
                )
            coordinate = (
                candidate.step_name,
                candidate.output_name,
                candidate.address,
            )
            intent = MembershipIntent(
                row=PublishedOutputRow(
                    context=run_plan.context,
                    workflow_name=run_plan.workflow_name,
                    step_name=candidate.step_name,
                    output_name=candidate.output_name,
                    address=candidate.address,
                    path=candidate.path,
                    output_digest=candidate.content_digest,
                    output_hash=candidate.output_hash,
                ),
                existing_artifact_id=candidate.artifact_id,
            )
            prior = intents_by_coordinate.get(coordinate)
            if (
                prior is not None
                and prior.existing_artifact_id != intent.existing_artifact_id
            ):
                raise ValidationError(
                    "retained reused bundles disagree on workflow membership"
                )
            intents_by_coordinate[coordinate] = intent

        pending.extend(
            record.registry_source_artifact_id
            for record in request.input_records
            if record.origin == "workflow_output"
            and record.registry_source_artifact_id is not None
        )

    return tuple(
        intents_by_coordinate[coordinate]
        for coordinate in sorted(intents_by_coordinate)
    )


def _selected_resolution_intents(
    run_plan: ExecutableRunPlan,
    *,
    published_rows: tuple[PublishedOutputRow, ...],
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
) -> tuple[SelectedOutputResolutionIntent, ...]:
    published_keys = {
        (row.step_name, row.output_name, row.address) for row in published_rows
    }
    fresh_intents = tuple(
        SelectedOutputResolutionIntent(
            context=run_plan.context,
            workflow_name=run_plan.workflow_name,
            step_name=output_ref.step_name,
            output_name=output_ref.output_name,
            address=output_ref.address,
            outcome=(
                "generated"
                if (
                    output_ref.step_name,
                    output_ref.output_name,
                    output_ref.address,
                )
                in published_keys
                else None
            ),
        )
        for output_ref in run_plan.selected_fresh_output_refs
    )
    reused_intents: list[SelectedOutputResolutionIntent] = []
    for output_ref in run_plan.selected_reused_output_refs:
        planned_ids = dict(output_ref.planned_sibling_artifact_ids)
        try:
            planned_artifact_id = planned_ids[output_ref.output_name]
            actual_candidate = actual_reused_artifacts[planned_artifact_id]
        except KeyError as exc:
            raise ValidationError(
                "selected reused output is missing its execution-time artifact"
            ) from exc
        if (
            actual_candidate.step_name != output_ref.step_name
            or actual_candidate.output_name != output_ref.output_name
            or actual_candidate.address != output_ref.address
        ):
            raise ValidationError(
                "selected reused artifact has the wrong requested coordinate"
            )
        reused_intents.append(
            SelectedOutputResolutionIntent(
                context=run_plan.context,
                workflow_name=run_plan.workflow_name,
                step_name=output_ref.step_name,
                output_name=output_ref.output_name,
                address=output_ref.address,
                outcome="reused",
                existing_artifact_id=actual_candidate.artifact_id,
            )
        )
    return tuple(
        sorted(
            (*fresh_intents, *reused_intents),
            key=lambda intent: (
                intent.step_name, intent.output_name, intent.address
            ),
        )
    )


def _environment_observation() -> EnvironmentObservationV1:
    return EnvironmentObservationV1(
        nipact_version=__version__,
        python_version=platform.python_version(),
        platform=platform.platform(),
        snakemake_version=metadata.version("snakemake"),
    )


def _reachable_job_ids(run_plan: ExecutableRunPlan) -> set[str]:
    return _reachable_job_ids_for_outputs(
        jobs=run_plan.jobs,
        selected_output_refs=run_plan.selected_fresh_output_refs,
    )


def _reachable_job_ids_for_outputs(
    *,
    jobs: tuple[RunJob, ...],
    selected_output_refs: tuple[RunJobOutputRef, ...],
) -> set[str]:
    output_refs_by_key = _output_refs_by_key(jobs)
    pending = [
        (output_ref.step_name, output_ref.output_name, output_ref.address)
        for output_ref in selected_output_refs
    ]
    seen_output_keys: set[tuple[str, str, str]] = set()
    reachable_job_ids: set[str] = set()
    while pending:
        key = pending.pop()
        if key in seen_output_keys:
            continue
        seen_output_keys.add(key)
        try:
            output_ref = output_refs_by_key[key]
        except KeyError as exc:
            raise ValidationError("run plan is missing a reachable output") from exc
        if output_ref.job_id in reachable_job_ids:
            continue
        reachable_job_ids.add(output_ref.job_id)
        for input_record in output_ref.input_records:
            if input_record.origin != "workflow_output":
                continue
            if input_record.registry_source_artifact_id is not None:
                continue
            if (
                input_record.source_step_name is None
                or input_record.source_output_name is None
                or input_record.source_address is None
            ):
                raise ValidationError("workflow input record is missing source output identity")
            pending.append(
                (
                    input_record.source_step_name,
                    input_record.source_output_name,
                    input_record.source_address,
                )
            )
    return reachable_job_ids


def _published_output_specs(
    *,
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
    jobs: tuple[RunJob, ...],
    reachable_job_ids: set[str],
) -> tuple[PublishedOutputSpec, ...]:
    specs: list[PublishedOutputSpec] = []
    for job in jobs:
        if job.job_id not in reachable_job_ids:
            continue
        request_bundle_digest = (
            job.projection_state.request_bundle_digest
            if isinstance(job.projection_state, ResolvedRequestBundleProjectionV3)
            else None
        )
        for output_name, output in job.outputs.items():
            specs.append(
                PublishedOutputSpec(
                    job_id=job.job_id,
                    context=loaded.context,
                    workflow_name=plan.workflow_name,
                    step_name=job.step_name,
                    output_name=output_name,
                    address=job.address,
                    declared_extension=output.declared_extension,
                    request_bundle_digest=request_bundle_digest,
                )
            )
    return tuple(specs)


def _run_manifest_binding_rows(run_plan: ExecutableRunPlan) -> tuple[RunManifestBindingRow, ...]:
    return tuple(
        RunManifestBindingRow(
            step_name=binding.step_name,
            manifest_usage_role=binding.manifest_usage_role,
            manifest_name=binding.manifest_name,
            manifest_value_schema=binding.manifest_value_schema,
            manifest_digest=binding.manifest_digest,
        )
        for binding in run_plan.manifest_bindings
    )


def _run_execution_population_row(
    run_plan: ExecutableRunPlan,
) -> RunExecutionPopulationRow | None:
    population = run_plan.execution_population
    if population is None:
        return None
    return RunExecutionPopulationRow(
        manifest_name=population.manifest_name,
        manifest_value_schema=population.manifest_value_schema,
        manifest_digest=population.manifest_digest,
    )


def _runtime_relative_path(runtime_root: Path, path: Path) -> str:
    resolved_root = runtime_root.resolve()
    resolved_path = path.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError as exc:
        raise ValidationError("runtime artifact path must stay inside runtime dir") from exc


def _compact_json(payload: dict[str, object]) -> str:
    try:
        return json.dumps(
            payload,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except ValueError as exc:
        raise ValidationError("workflow parameters must be finite JSON values") from exc


def _write_text_file(path: Path, content: str) -> None:
    if path.is_file() and path.read_text(encoding="utf-8") == content:
        return
    path.write_text(content, encoding="utf-8")


def _build_jobs(
    *,
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
    run_workspace: Path,
    requested_address: str | None,
    source_authorities: dict[
        LogicalSourceCoordinate,
        RegisteredSourceAuthority,
    ],
) -> tuple[tuple[RunJob, ...], dict[tuple[str, str, str], ReusedRunJobOutputRef]]:
    jobs: list[RunJob] = []
    source_snapshots = {
        coordinate: RegisteredSourceSnapshot(
            content_digest=record.authority.content_digest,
            file_size=record.authority.file_size,
            declared_extension=record.authority.declaration.declared_extension,
        )
        for coordinate, record in source_authorities.items()
    }
    selected_coordinates = set(
        selected_job_coordinates(
            plan=plan,
            requested_address=requested_address,
        )
    )
    projection_states_by_output: dict[
        RequestedOutputCoordinate,
        RequestBundleProjectionState,
    ] = {}
    outputs_by_artifact: dict[
        tuple[str, str, str],
        RunJobOutputRef | ReusedRunJobOutputRef,
    ] = {}
    reused_outputs_by_artifact: dict[tuple[str, str, str], ReusedRunJobOutputRef] = {}
    resolver_cache: dict[
        ReusableArtifactBundleRequest,
        ReusableArtifactBundleCandidate | None,
    ] = {}
    fresh_requests: dict[str, tuple[str, str]] = {}
    for step in plan.steps:
        outputs = _validated_step_outputs(step)
        addresses = _step_addresses(plan, step)
        for address in addresses:
            if (step.step_name, address) not in selected_coordinates:
                continue
            input_paths, input_records = _job_inputs(
                loaded,
                step,
                run_workspace=run_workspace,
                address=address,
                outputs_by_artifact=outputs_by_artifact,
                source_authorities=source_authorities,
            )
            job_outputs = _run_job_outputs(
                run_workspace=run_workspace,
                step=step,
                outputs=outputs,
                address=address,
            )
            projection_plan = _request_projection_plan(
                context=loaded.context,
                step=step,
                address=address,
                outputs=outputs,
                input_records=input_records,
            )
            projection_state = resolve_request_bundle_projection_plan(
                projection_plan,
                source_snapshots=source_snapshots,
                upstream_states=projection_states_by_output,
            )
            job = RunJob(
                job_id=_job_id(step.step_name, address, outputs),
                step_name=step.step_name,
                address=address,
                execution_role=step.execution_role,
                callable_ref=step.callable_ref,
                outputs=job_outputs,
                inputs=input_paths,
                input_records=input_records,
                params=dict(step.params),
                projection_plan=projection_plan,
                projection_state=projection_state,
            )
            for output_name in outputs:
                projection_states_by_output[
                    RequestedOutputCoordinate(
                        namespace=loaded.context,
                        step_name=step.step_name,
                        output_name=output_name,
                        address=address,
                    )
                ] = projection_state
            reused_refs: dict[str, ReusedRunJobOutputRef] | None = None
            if isinstance(projection_state, ResolvedRequestBundleProjectionV3):
                reused_refs = _reusable_output_refs_for_job(
                    loaded=loaded,
                    job=job,
                    resolver_cache=resolver_cache,
                )
            if reused_refs is not None:
                for output_name, output_ref in reused_refs.items():
                    key = (step.step_name, output_name, address)
                    outputs_by_artifact[key] = output_ref
                    reused_outputs_by_artifact[key] = output_ref
                continue
            if isinstance(projection_state, ResolvedRequestBundleProjectionV3):
                digest = projection_state.request_bundle_digest
                previous = fresh_requests.get(digest)
                if previous is not None:
                    raise ValidationError(
                        "selected plan contains duplicate equal fresh requests: "
                        f"{previous[0]}[{previous[1]}] and "
                        f"{step.step_name}[{address}]"
                    )
                fresh_requests[digest] = (step.step_name, address)
            jobs.append(job)
            for output_name in outputs:
                outputs_by_artifact[(step.step_name, output_name, address)] = job.output_ref(
                    output_name
                )

    return tuple(jobs), reused_outputs_by_artifact


def _request_projection_plan(
    *,
    context: str,
    step: WorkflowPlanStep,
    address: str,
    outputs: dict[str, Any],
    input_records: tuple[ArtifactInputRow, ...],
) -> RequestBundleProjectionPlanV3:
    return RequestBundleProjectionPlanV3(
        identity_contract_version=IDENTITY_CONTRACT_VERSION,
        namespace=context,
        step_contract=StepContract(
            step_contract_id=step.step_name,
            step_contract_version=step.step_contract_version,
            callable_ref=step.callable_ref,
            runner_contract_version=RUNNER_CONTRACT_VERSION,
        ),
        address=address,
        canonical_parameters=dict(step.params),
        role_labelled_binding_plans=_projection_binding_plans(
            context=context,
            input_records=input_records,
        ),
        result_affecting_settings={},
        determinism_contract="deterministic",
        output_contract=OutputContract(
            output_contract_version=OUTPUT_CONTRACT_VERSION,
            sibling_outputs=tuple(
                SiblingOutput(
                    output_name=output_name,
                    declared_extension=output.extension,
                )
                for output_name, output in outputs.items()
            ),
        ),
    )


def _projection_binding_plans(
    *,
    context: str,
    input_records: tuple[ArtifactInputRow, ...],
) -> tuple[
    SourceBindingPlan
    | UpstreamRequestedOutputBindingPlan
    | CollectionBindingPlan,
    ...,
]:
    records_by_binding: dict[str, list[ArtifactInputRow]] = {}
    for record in input_records:
        records_by_binding.setdefault(record.binding_name, []).append(record)

    binding_plans: list[
        SourceBindingPlan
        | UpstreamRequestedOutputBindingPlan
        | CollectionBindingPlan
    ] = []
    for binding_name, records in records_by_binding.items():
        origins = {record.origin for record in records}
        dependency_roles = {record.dependency_role for record in records}
        if len(origins) != 1 or len(dependency_roles) != 1:
            raise ValidationError(
                f"input binding {binding_name!r} has inconsistent dependency records"
            )
        origin = next(iter(origins))
        dependency_role = next(iter(dependency_roles))
        if origin == "source":
            if (
                len(records) != 1
                or records[0].source_scope is None
                or records[0].source_name is None
            ):
                raise ValidationError(
                    f"source input binding {binding_name!r} is malformed"
                )
            record = records[0]
            binding_plans.append(
                SourceBindingPlan(
                    role=binding_name,
                    source_coordinate=LogicalSourceCoordinate(
                        context=context,
                        scope=record.source_scope,
                        source_name=record.source_name,
                        entity_id=record.source_entity_id,
                    ),
                )
            )
            continue
        if origin != "workflow_output":
            raise ValidationError(
                f"input binding {binding_name!r} has unsupported origin {origin!r}"
            )

        requested_outputs = tuple(
            _requested_output_coordinate(context=context, record=record)
            for record in records
        )
        if dependency_role in {"source_input", "apply_input", "collective_fit"}:
            if len(requested_outputs) != 1:
                raise ValidationError(
                    f"scalar input binding {binding_name!r} must have one source"
                )
            binding_plans.append(
                UpstreamRequestedOutputBindingPlan(
                    role=binding_name,
                    requested_output=requested_outputs[0],
                )
            )
            continue
        if dependency_role in {"fit_input", "analysis_input"}:
            manifest_schemas = {record.manifest_value_schema for record in records}
            manifest_digests = {record.manifest_digest for record in records}
            if len(manifest_schemas) != 1 or len(manifest_digests) != 1:
                raise ValidationError(
                    f"collection input binding {binding_name!r} has inconsistent manifests"
                )
            manifest_value_schema = next(iter(manifest_schemas))
            manifest_digest = next(iter(manifest_digests))
            if manifest_value_schema is None or manifest_digest is None:
                raise ValidationError(
                    f"collection input binding {binding_name!r} is missing its "
                    "scientific manifest value"
                )
            binding_plans.append(
                CollectionBindingPlan(
                    role=binding_name,
                    collection_semantics="coordinate_set_v1",
                    manifest_value_schema=manifest_value_schema,
                    manifest_digest=manifest_digest,
                    members=requested_outputs,
                )
            )
            continue
        raise ValidationError(
            f"unsupported dependency role for projection: {dependency_role}"
        )
    return tuple(binding_plans)


def _requested_output_coordinate(
    *,
    context: str,
    record: ArtifactInputRow,
) -> RequestedOutputCoordinate:
    if (
        record.source_step_name is None
        or record.source_output_name is None
        or record.source_address is None
    ):
        raise ValidationError(
            f"workflow input binding {record.binding_name!r} is missing its coordinate"
        )
    return RequestedOutputCoordinate(
        namespace=context,
        step_name=record.source_step_name,
        output_name=record.source_output_name,
        address=record.source_address,
    )


def _run_job_outputs(
    *,
    run_workspace: Path,
    step: WorkflowPlanStep,
    outputs: dict[str, Any],
    address: str,
) -> dict[str, RunJobOutput]:
    return {
        output_name: RunJobOutput(
            output_name=output_name,
            declared_extension=output.extension,
            staging_path=(
                run_workspace
                / "staging"
                / step.step_name
                / output_name
                / f"{address}{output.extension}"
            ),
            staging_path_relative=(
                f"staging/{step.step_name}/{output_name}/{address}{output.extension}"
            ),
        )
        for output_name, output in outputs.items()
    }


def _reusable_output_refs_for_job(
    *,
    loaded: LoadedWorkflowProject,
    job: RunJob,
    resolver_cache: dict[
        ReusableArtifactBundleRequest,
        ReusableArtifactBundleCandidate | None,
    ],
) -> dict[str, ReusedRunJobOutputRef] | None:
    if not isinstance(job.projection_state, ResolvedRequestBundleProjectionV3):
        return None
    registry_path = loaded.runtime_root / REGISTRY_DB_PATH
    request = ReusableArtifactBundleRequest(
        context=loaded.context,
        step_name=job.step_name,
        address=job.address,
        resolved_projection=job.projection_state,
        sibling_outputs=tuple(
            sorted(
                (output_name, output.declared_extension)
                for output_name, output in job.outputs.items()
            )
        ),
        input_records=job.input_records,
    )
    if request in resolver_cache:
        bundle = resolver_cache[request]
    else:
        bundle = resolve_reusable_artifact_bundle(
            registry_path,
            runtime_root=loaded.runtime_root,
            request=request,
        )
        resolver_cache[request] = bundle
    if bundle is None:
        return None
    return {
        output_name: _reused_output_ref(
            runtime_root=loaded.runtime_root,
            output=output,
            job=job,
            candidate=bundle.output(output_name),
            bundle=bundle,
            request=request,
        )
        for output_name, output in job.outputs.items()
    }


def _reused_output_ref(
    *,
    runtime_root: Path,
    output: RunJobOutput,
    job: RunJob,
    candidate: ReusableArtifactCandidate,
    bundle: ReusableArtifactBundleCandidate,
    request: ReusableArtifactBundleRequest,
) -> ReusedRunJobOutputRef:
    source_path = runtime_root / candidate.path
    return ReusedRunJobOutputRef(
        step_name=job.step_name,
        output_name=output.output_name,
        address=job.address,
        execution_role=job.execution_role,
        callable_ref=job.callable_ref,
        parameters_json=_compact_json(job.params),
        declared_extension=output.declared_extension,
        staging_path=output.staging_path,
        staging_path_relative=output.staging_path_relative,
        source_path=source_path,
        source_path_relative=candidate.path,
        source_artifact_id=candidate.artifact_id,
        source_workflow_name=candidate.workflow_name,
        source_run_id=candidate.run_id,
        source_bundle_artifact_ids=tuple(
            output.artifact_id for output in bundle.outputs
        ),
        content_digest=candidate.content_digest,
        file_size=candidate.file_size,
        reuse_request=request,
        projection_plan=job.projection_plan,
        projection_state=job.projection_state,
        candidate=candidate,
        bundle=bundle,
    )


def _used_reused_outputs(
    *,
    jobs: tuple[RunJob, ...],
    reused_outputs_by_artifact: dict[tuple[str, str, str], ReusedRunJobOutputRef],
) -> tuple[ReusedRunJobOutputRef, ...]:
    used_ids = {
        record.registry_source_artifact_id
        for job in jobs
        for record in job.input_records
        if record.origin == "workflow_output"
        and record.registry_source_artifact_id is not None
    }
    return tuple(
        output_ref
        for output_ref in reused_outputs_by_artifact.values()
        if output_ref.source_artifact_id in used_ids
    )


def _reused_validation_outputs(
    *,
    jobs: tuple[RunJob, ...],
    reused_outputs_by_artifact: dict[tuple[str, str, str], ReusedRunJobOutputRef],
    selected_reused_output_refs: tuple[SelectedReusedBundleRef, ...] = (),
) -> tuple[ReusedRunJobOutputRef, ...]:
    refs_by_id = {
        output_ref.source_artifact_id: output_ref
        for output_ref in reused_outputs_by_artifact.values()
    }
    pending = [
        record.registry_source_artifact_id
        for job in jobs
        for record in job.input_records
        if record.origin == "workflow_output"
        and record.registry_source_artifact_id is not None
    ]
    pending.extend(
        artifact_id
        for selected_ref in selected_reused_output_refs
        for _output_name, artifact_id in selected_ref.planned_sibling_artifact_ids
    )
    seen_requests: set[ReusableArtifactBundleRequest] = set()
    validation_refs: list[ReusedRunJobOutputRef] = []
    while pending:
        artifact_id = pending.pop()
        try:
            output_ref = refs_by_id[artifact_id]
        except KeyError as exc:
            raise ValidationError(
                "reachable reused dependency is missing its planned bundle"
            ) from exc
        if output_ref.reuse_request in seen_requests:
            continue
        seen_requests.add(output_ref.reuse_request)
        validation_refs.append(output_ref)
        pending.extend(
            record.registry_source_artifact_id
            for record in output_ref.reuse_request.input_records
            if record.origin == "workflow_output"
            and record.registry_source_artifact_id is not None
        )
    return tuple(validation_refs)


def _output_refs_by_key(
    jobs: tuple[RunJob, ...],
) -> dict[tuple[str, str, str], RunJobOutputRef]:
    return {
        (job.step_name, output_name, job.address): job.output_ref(output_name)
        for job in jobs
        for output_name in job.outputs
    }


def _job_inputs(
    loaded: LoadedWorkflowProject,
    step: WorkflowPlanStep,
    *,
    run_workspace: Path,
    address: str,
    outputs_by_artifact: dict[
        tuple[str, str, str],
        RunJobOutputRef | ReusedRunJobOutputRef,
    ],
    source_authorities: dict[
        LogicalSourceCoordinate,
        RegisteredSourceAuthority,
    ],
) -> tuple[dict[str, tuple[str, ...]], tuple[ArtifactInputRow, ...]]:
    inputs: dict[str, tuple[str, ...]] = {}
    input_records: list[ArtifactInputRow] = []
    if step.execution_role == "source_import":
        for binding_name in step.source_inputs:
            declaration = source_declaration_for_binding(
                context=loaded.context,
                source_index=loaded.source_index,
                source_name=binding_name,
                address=address,
            )
            source_artifact_path = declaration.declared_path
            source_input_path = _source_input_path_for_workspace(
                runtime_root=loaded.runtime_root,
                run_workspace=run_workspace,
                source_artifact_path=source_artifact_path,
            )
            inputs[binding_name] = (source_input_path,)
            authority_record = source_authorities.get(declaration.coordinate)
            authority = (
                authority_record.authority if authority_record is not None else None
            )
            input_records.append(
                ArtifactInputRow(
                    binding_name=binding_name,
                    input_path=source_input_path,
                    dependency_role="source_input",
                    origin="source",
                    source_artifact_path=source_artifact_path,
                    source_scope=declaration.coordinate.scope,
                    source_name=declaration.coordinate.source_name,
                    source_entity_id=declaration.coordinate.entity_id,
                    source_content_digest=(
                        authority.content_digest if authority is not None else None
                    ),
                    source_file_size=(
                        authority.file_size if authority is not None else None
                    ),
                    source_extension=declaration.declared_extension,
                    registry_source_artifact_id=(
                        authority_record.artifact_id
                        if authority_record is not None
                        else None
                    ),
                    edge_cardinality=1,
                )
            )
    for input_name, step_input in step.inputs.items():
        if step_input.dependency_role in {"source_input", "apply_input"}:
            input_output = outputs_by_artifact[
                (step_input.source_step_name, step_input.source_output_name, address)
            ]
            inputs[input_name] = (input_output.staging_path_relative,)
            input_records.append(
                _workflow_input_record(
                    input_name=input_name,
                    input_path=input_output.staging_path_relative,
                    dependency_role=step_input.dependency_role,
                    source_output=input_output,
                    edge_cardinality=1,
                )
            )
        elif step_input.dependency_role == "collective_fit":
            cohort_address = _cohort_input_address(outputs_by_artifact, step_input)
            input_output = outputs_by_artifact[
                (
                    step_input.source_step_name,
                    step_input.source_output_name,
                    cohort_address,
                )
            ]
            inputs[input_name] = (input_output.staging_path_relative,)
            input_records.append(
                _workflow_input_record(
                    input_name=input_name,
                    input_path=input_output.staging_path_relative,
                    dependency_role=step_input.dependency_role,
                    source_output=input_output,
                    edge_cardinality=1,
                )
            )
        elif step_input.dependency_role in {"fit_input", "analysis_input"}:
            manifest_binding = step.manifest_binding
            if manifest_binding is None:
                raise ValidationError(
                    f"input {input_name!r} for step {step.step_name!r} requires "
                    "a scientific manifest binding"
                )
            allowed_addresses = set(manifest_binding.entity_ids)
            matching_outputs_with_addresses = [
                (_source_address, output_ref)
                for (
                    source_step,
                    source_output,
                    _source_address,
                ), output_ref in outputs_by_artifact.items()
                if source_step == step_input.source_step_name
                and source_output == step_input.source_output_name
                and _source_address in allowed_addresses
            ]
            matching_outputs_with_addresses.sort(key=lambda item: item[0])
            _validate_exact_scientific_fan_in(
                step_name=step.step_name,
                input_name=input_name,
                expected_addresses=manifest_binding.entity_ids,
                collected_addresses=tuple(
                    source_address
                    for source_address, _output_ref in matching_outputs_with_addresses
                ),
                output_addresses=tuple(
                    output_ref.address
                    for _source_address, output_ref in matching_outputs_with_addresses
                ),
            )
            matching_outputs = tuple(
                output_ref
                for _source_address, output_ref in matching_outputs_with_addresses
            )
            inputs[input_name] = tuple(
                output_ref.staging_path_relative
                for output_ref in matching_outputs
            )
            edge_cardinality = len(matching_outputs)
            input_records.extend(
                _workflow_input_record(
                    input_name=input_name,
                    input_path=output_ref.staging_path_relative,
                    dependency_role=step_input.dependency_role,
                    source_output=output_ref,
                    manifest_value_schema=(
                        manifest_binding.manifest_value_schema
                    ),
                    manifest_digest=manifest_binding.manifest_digest,
                    edge_cardinality=edge_cardinality,
                )
                for output_ref in matching_outputs
            )
        else:
            raise ValidationError(
                f"unsupported dependency role for execution: {step_input.dependency_role}"
            )
    return inputs, tuple(input_records)


def _validate_exact_scientific_fan_in(
    *,
    step_name: str,
    input_name: str,
    expected_addresses: tuple[str, ...],
    collected_addresses: tuple[str, ...],
    output_addresses: tuple[str, ...] | None = None,
) -> None:
    """Require one correctly addressed collected output per manifest member."""
    seen_addresses: set[str] = set()
    duplicate_address_set: set[str] = set()
    for address in collected_addresses:
        if address in seen_addresses:
            duplicate_address_set.add(address)
        seen_addresses.add(address)
    duplicate_addresses = sorted(duplicate_address_set)
    if duplicate_addresses:
        raise ValidationError(
            f"scientific fan-in {step_name}.{input_name} contains duplicate "
            f"addresses: {', '.join(duplicate_addresses)}"
        )
    expected = set(expected_addresses)
    collected = set(collected_addresses)
    missing = sorted(expected - collected)
    extra = sorted(collected - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if extra:
            details.append(f"extra: {', '.join(extra)}")
        raise ValidationError(
            f"scientific fan-in {step_name}.{input_name} does not exactly match "
            f"manifest membership ({'; '.join(details)})"
        )
    if output_addresses is not None and output_addresses != collected_addresses:
        raise ValidationError(
            f"scientific fan-in {step_name}.{input_name} contains a wrongly "
            "addressed upstream output"
        )


def _source_input_path_for_workspace(
    *,
    runtime_root: Path,
    run_workspace: Path,
    source_artifact_path: str,
) -> str:
    source_path = runtime_root / source_artifact_path
    if not source_path.is_file():
        raise ValidationError(f"missing source artifact: {source_artifact_path}")
    return os.path.relpath(source_path, run_workspace).replace(os.sep, "/")


def _workflow_input_record(
    *,
    input_name: str,
    input_path: str,
    dependency_role: str,
    source_output: RunJobOutputRef | ReusedRunJobOutputRef,
    manifest_value_schema: str | None = None,
    manifest_digest: str | None = None,
    edge_cardinality: int | None = None,
) -> ArtifactInputRow:
    source_input_records: tuple[ArtifactInputRow, ...] = ()
    if (
        isinstance(source_output, RunJobOutputRef)
        and source_output.execution_role == "source_import"
    ):
        source_input_records = source_output.input_records
    return ArtifactInputRow(
        binding_name=input_name,
        input_path=input_path,
        dependency_role=dependency_role,
        origin="workflow_output",
        source_step_name=source_output.step_name,
        source_output_name=source_output.output_name,
        source_address=source_output.address,
        source_callable_ref=source_output.callable_ref,
        source_parameters_json=_source_parameters_json(source_output),
        source_extension=source_output.declared_extension,
        source_execution_role=source_output.execution_role,
        source_is_reused=isinstance(source_output, ReusedRunJobOutputRef),
        manifest_value_schema=manifest_value_schema,
        manifest_digest=manifest_digest,
        edge_cardinality=edge_cardinality,
        registry_source_artifact_id=(
            source_output.source_artifact_id
            if isinstance(source_output, ReusedRunJobOutputRef)
            else None
        ),
        source_input_records=source_input_records,
    )


def _source_parameters_json(source_output: RunJobOutputRef | ReusedRunJobOutputRef) -> str:
    if isinstance(source_output, ReusedRunJobOutputRef):
        return source_output.parameters_json
    return _compact_json(source_output.params)


def _cohort_input_address(
    jobs_by_artifact: dict[
        tuple[str, str, str],
        RunJobOutputRef | ReusedRunJobOutputRef,
    ],
    step_input: Any,
) -> str:
    matches = [
        address
        for source_step, source_output, address in jobs_by_artifact
        if source_step == step_input.source_step_name
        and source_output == step_input.source_output_name
    ]
    if len(matches) != 1:
        raise ValidationError("collective_fit inputs require exactly one cohort artifact")
    return matches[0]


def _step_addresses(
    plan: WorkflowPlan,
    step: WorkflowPlanStep,
) -> tuple[str, ...]:
    output_scope = next(iter(_validated_step_outputs(step).values())).address_scope
    if output_scope == "entity":
        population = _required_execution_population(plan)
        return tuple(
            validate_path_token(entity_id, label="entity_id")
            for entity_id in population.entity_ids
        )
    if output_scope == "cohort":
        return ("cohort",)
    raise ValidationError(f"unsupported address_scope: {output_scope!r}")


def _validated_step_outputs(step: WorkflowPlanStep) -> dict[str, Any]:
    output_scopes = {output.address_scope for output in step.outputs.values()}
    if len(output_scopes) != 1:
        raise ValidationError(
            f"step {step.step_name!r} outputs must share one address_scope"
        )
    output_scope = next(iter(output_scopes))
    if output_scope != step.address_scope:
        raise ValidationError(
            f"step {step.step_name!r} output address_scope must match step address_scope"
        )
    return dict(step.outputs)


def _job_id(step_name: str, address: str, outputs: dict[str, Any]) -> str:
    if len(outputs) == 1:
        output_name = next(iter(outputs))
        return f"job__{step_name}__{output_name}__{address}"
    return f"job__{step_name}__outputs__{address}"


def _selected_addresses(
    plan: WorkflowPlan,
    selected_step: WorkflowPlanStep,
    *,
    requested_address: str | None = None,
) -> tuple[str, ...]:
    output = selected_step.outputs[plan.selected_output_name]
    if output.address_scope == "cohort":
        if requested_address is not None:
            raise ValidationError(
                f"selected output {plan.selected_output_name!r} of step "
                f"{selected_step.step_name!r} is cohort-addressed and cannot be "
                "targeted by entity address"
            )
        return ("cohort",)
    if output.address_scope == "entity":
        population = _required_execution_population(plan)
        if requested_address is not None:
            address = validate_path_token(requested_address, label="address")
            if address not in population.entity_ids:
                raise ValidationError(
                    f"address {address!r} is not a member of execution_population "
                    f"{population.manifest_name!r}"
                )
            return (address,)
        return tuple(
            validate_path_token(entity_id, label="entity_id")
            for entity_id in population.entity_ids
        )
    raise ValidationError(f"unsupported workflow step address_scope: {output.address_scope}")


def _required_execution_population(
    plan: WorkflowPlan,
) -> WorkflowPlanExecutionPopulation:
    if plan.execution_population is None:
        raise ValidationError("entity workflow steps require an execution_population")
    return plan.execution_population


def _plan_step(plan: WorkflowPlan, step_name: str) -> WorkflowPlanStep:
    for step in plan.steps:
        if step.step_name == step_name:
            return step
    raise ValidationError(f"workflow plan selected step is missing: {step_name}")
