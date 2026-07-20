"""Internal execution planning helpers for NIPACT workflow runs."""

from __future__ import annotations

import json
from importlib import metadata
import os
import platform
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from ._version import __version__
from .artifacts import output_filename
from .errors import ValidationError
from .hashing import sha256_file_digest, short_hash
from .identity import validate_path_token
from .projection import (
    IDENTITY_CONTRACT_VERSION,
    OUTPUT_CONTRACT_VERSION,
    RUNNER_CONTRACT_VERSION,
    CollectionBindingPlan,
    OutputContract,
    RequestBundleProjectionPlanV1,
    RequestBundleProjectionState,
    ResolvedRequestBundleProjectionV1,
    RequestedOutputCoordinate,
    SiblingOutput,
    SourceBindingPlan,
    SourceCoordinate,
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
    RunManifestBindingRow,
    SelectedOutputResolutionIntent,
    WorkflowOutputArtifactRow,
    record_workflow_run,
    read_context_runtime_path,
    read_registered_source_snapshots,
    resolve_reusable_artifact_bundle,
)
from .workflow import (
    LoadedWorkflowProject,
    SourceIndex,
    WorkflowPlan,
    WorkflowPlanManifestBinding,
    WorkflowPlanStep,
    compile_workflow_plan,
    load_workflow_project,
)

RunStatusCallback = Callable[[str], None]


@dataclass(frozen=True)
class PublishedOutputSpec:
    context: str
    workflow_name: str
    step_name: str
    output_name: str
    address: str
    declared_extension: str
    output_directory: Path
    output_directory_relative: str


@dataclass(frozen=True)
class _PublishedOutputResult:
    row: PublishedOutputRow
    created: bool


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
    projection_plan: RequestBundleProjectionPlanV1
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
    def projection_plan(self) -> RequestBundleProjectionPlanV1:
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
    projection_plan: RequestBundleProjectionPlanV1
    projection_state: RequestBundleProjectionState


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
class RunPlan:
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
    manifest_bindings: tuple[WorkflowPlanManifestBinding, ...]
    published_outputs: tuple[PublishedOutputSpec, ...]
    jobs: tuple[RunJob, ...]
    selected_fresh_output_refs: tuple[RunJobOutputRef, ...]
    selected_reused_output_refs: tuple[SelectedReusedBundleRef, ...]
    reused_outputs: tuple[ReusedRunJobOutputRef, ...]
    reused_validation_outputs: tuple[ReusedRunJobOutputRef, ...]
    # Reporting statistic: fresh jobs in the selected targets' reachable
    # closure. jobs stays population-wide; this is the executed forecast.
    reachable_job_count: int

    @property
    def selected_fresh_jobs(self) -> tuple[RunJob, ...]:
        return tuple(output_ref.job for output_ref in self.selected_fresh_output_refs)


@dataclass(frozen=True)
class RunOutcome:
    """Result of a best-effort job-atomic run."""

    published_count: int
    selected_generated_count: int
    selected_reused_count: int
    failed_jobs: tuple[tuple[str, str, str], ...]  # (step, address, coarse reason)
    all_selected_resolved: bool


def build_run_plan(
    *,
    project_dir: Path,
    context: str,
    workflow_name: str,
    step_name: str,
    address: str | None = None,
    dry_run: bool = False,
) -> RunPlan:
    """Build the internal execution plan without running anything."""
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
    selected_step = _plan_step(plan, plan.selected_step_name)
    _validated_step_outputs(selected_step)
    selected_output = selected_step.outputs[plan.selected_output_name]
    run_workspace = loaded.runtime_root / "runs" / context / plan.workflow_name / plan.selected_step_name
    if len(selected_step.outputs) > 1:
        run_workspace = run_workspace / selected_output.name
    addresses = _selected_addresses(loaded, plan, selected_step, requested_address=address)
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
    return RunPlan(
        project_root=loaded.project_root,
        runtime_root=loaded.runtime_root,
        context=context,
        workflow_name=plan.workflow_name,
        base_workflow_name=loaded.workflows[plan.workflow_name].base_workflow,
        selected_step_name=selected_step.step_name,
        selected_output_name=selected_output.name,
        requested_address=address,
        dry_run=dry_run,
        run_workspace=run_workspace,
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
    run_plan: RunPlan,
    *,
    cores: int = 1,
    status_callback: RunStatusCallback | None = None,
) -> RunOutcome:
    """Resolve a workflow plan, executing fresh selected work best-effort.

    Snakemake runs with ``--keep-going``, so a single failed job no longer aborts
    the run: every job whose declared outputs all landed in staging is published
    and recorded, the failures are skipped, and the survivors stay reusable. The
    hard errors are a real fresh-only run that published nothing while Snakemake
    exited non-zero, and a dry run whose Snakemake invocation exited non-zero.
    """
    if cores <= 0:
        raise ValidationError("cores must be a positive integer")
    _emit_status(status_callback, "building_workspace")
    has_fresh_selection = bool(run_plan.selected_fresh_output_refs)
    has_selected_reuse = bool(run_plan.selected_reused_output_refs)
    _prepare_run_workspace(run_plan)
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate] = {}
    if run_plan.dry_run:
        # Dry runs re-resolve the active reused closure using metadata only. A
        # fresh plan needs the registered input paths in its Snakefile; a
        # reuse-only plan writes no executor files at all.
        if not has_fresh_selection:
            _write_reuse_only_workspace(run_plan)
        if has_selected_reuse:
            _emit_status(status_callback, "validating_selected_reuse")
        reused_input_paths = _dry_run_reused_input_paths(run_plan)
        if has_fresh_selection:
            _write_run_workspace(run_plan, reused_input_paths=reused_input_paths)
    else:
        if has_fresh_selection:
            _write_run_workspace(run_plan)
        else:
            _write_reuse_only_workspace(run_plan)
        if has_selected_reuse:
            _emit_status(status_callback, "validating_selected_reuse")
        actual_reused_artifacts = _hydrate_reused_outputs(run_plan)
        if has_fresh_selection:
            _remove_expected_staged_outputs(run_plan)
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
        published_results, publish_failures = _publish_run_outputs(run_plan)
        published_results, prune_failures = _prune_orphan_published_jobs(
            run_plan,
            published_results,
        )
    else:
        published_results = ()
        publish_failures = ()
        prune_failures = ()
    failed_jobs = tuple(sorted(publish_failures + prune_failures))
    published_rows = tuple(result.row for result in published_results)
    created_rows = tuple(result.row for result in published_results if result.created)
    if not published_rows and returncode != 0 and not has_selected_reuse:
        log_path = run_plan.run_workspace / "logs" / "snakemake.log"
        raise ValidationError(
            f"Snakemake failed with exit code {returncode}; see {log_path}"
        )
    try:
        artifact_rows = _workflow_output_artifact_rows(
            run_plan,
            published_rows=published_rows,
            actual_reused_artifacts=actual_reused_artifacts,
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
            manifest_bindings=_run_manifest_binding_rows(run_plan),
            membership_intents=membership_intents,
        )
        _emit_status(status_callback, "registry_updated")
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
        )
    except Exception:
        _remove_published_output_files(run_plan.runtime_root, created_rows)
        raise


def _emit_status(callback: RunStatusCallback | None, event: str) -> None:
    if callback is not None:
        callback(event)


def publish_run_outputs(run_plan: RunPlan) -> tuple[PublishedOutputRow, ...]:
    """Publish planned outputs and return registry row facts."""
    results, _failed = _publish_run_outputs(run_plan)
    return tuple(result.row for result in results)


def _publish_run_outputs(
    run_plan: RunPlan,
) -> tuple[tuple[_PublishedOutputResult, ...], tuple[tuple[str, str, str], ...]]:
    """Publish declared outputs from reachable non-reused jobs, best-effort per job.

    The atomic unit is the job (step + address): a job publishes iff all of its
    declared sibling outputs are present in staging. A job with any missing or
    invalid sibling is skipped without rolling back the jobs that did publish.
    Returns the published results and a list of skipped jobs as
    ``(step, address, reason)``. Only plan-construction errors still raise.
    """
    publishable_outputs = _output_refs_by_key(run_plan.jobs)
    specs_by_job: dict[tuple[str, str], list[PublishedOutputSpec]] = {}
    for spec in run_plan.published_outputs:
        specs_by_job.setdefault((spec.step_name, spec.address), []).append(spec)
    results: list[_PublishedOutputResult] = []
    failed: list[tuple[str, str, str]] = []
    for (step_name, address), specs in specs_by_job.items():
        rows, reason = _publish_one_job(run_plan, specs, publishable_outputs)
        if reason is None:
            results.extend(rows)
        else:
            failed.append((step_name, address, reason))
    return tuple(results), tuple(failed)


def _publish_one_job(
    run_plan: RunPlan,
    specs: list[PublishedOutputSpec],
    publishable_outputs: dict[tuple[str, str, str], RunJobOutputRef],
) -> tuple[list[_PublishedOutputResult], str | None]:
    """Publish one job's siblings; return ``(rows, reason)``.

    ``reason`` is ``None`` when the whole job published, else a coarse skip
    category (``"missing staged output"`` or ``"digest mismatch"``).
    """
    preflight_rows: list[tuple[PublishedOutputSpec, RunJobOutputRef, str, str, str]] = []
    for spec in specs:
        key = (spec.step_name, spec.output_name, spec.address)
        try:
            output_ref = publishable_outputs[key]
        except KeyError as exc:
            raise ValidationError("run plan is missing a publishable output") from exc
        if not output_ref.staging_path.is_file():
            # A missing or non-file sibling means the producing job is incomplete
            # (its process did not write every declared output). Skip the whole
            # job rather than publish a partial set of siblings.
            return [], "missing staged output"
        output_digest = sha256_file_digest(output_ref.staging_path)
        output_hash = short_hash(output_digest)
        final_name = output_filename(
            address=spec.address,
            output_hash=output_hash,
            declared_extension=spec.declared_extension,
        )
        preflight_rows.append((spec, output_ref, output_digest, output_hash, final_name))

    results: list[_PublishedOutputResult] = []
    temp_paths: list[Path] = []
    try:
        for spec, output_ref, output_digest, output_hash, final_name in preflight_rows:
            final_path = spec.output_directory / final_name
            final_path.parent.mkdir(parents=True, exist_ok=True)
            final_path_existed = final_path.exists()
            if final_path_existed:
                _validate_existing_published_file(final_path, expected_digest=output_digest)
            else:
                temp_path = final_path.with_name(
                    f".{final_path.name}.{os.getpid()}.tmp"
                )
                temp_paths.append(temp_path)
                if temp_path.exists() or temp_path.is_symlink():
                    if not temp_path.is_file() and not temp_path.is_symlink():
                        raise ValidationError(f"temporary output path is not a file: {temp_path}")
                    temp_path.unlink()
                shutil.copy2(output_ref.staging_path, temp_path)
                _validate_existing_published_file(
                    temp_path,
                    expected_digest=output_digest,
                )
                os.replace(temp_path, final_path)
            row = PublishedOutputRow(
                context=spec.context,
                workflow_name=spec.workflow_name,
                step_name=spec.step_name,
                output_name=spec.output_name,
                address=spec.address,
                path=f"{spec.output_directory_relative}/{final_name}",
                output_digest=output_digest,
                output_hash=output_hash,
            )
            results.append(
                _PublishedOutputResult(
                    row=row,
                    created=not final_path_existed,
                )
            )
    except ValidationError:
        # Skip this job only. Any sibling already copied stays as an inert,
        # content-addressed orphan: it is never reusable without a registry row,
        # which a skipped job never gets, so no batch rollback is needed.
        return [], "digest mismatch"
    finally:
        for temp_path in temp_paths:
            if temp_path.is_file() or temp_path.is_symlink():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    return results, None


def _prune_orphan_published_jobs(
    run_plan: RunPlan,
    published_results: tuple[_PublishedOutputResult, ...],
) -> tuple[tuple[_PublishedOutputResult, ...], tuple[tuple[str, str, str], ...]]:
    """Drop published jobs whose fresh workflow-output parents were not published.

    Best-effort publishing can land a child while skipping a parent (e.g. a
    multi-output parent missing one sibling). Recording such an orphan would make
    ``record_workflow_run`` raise on the dangling dependency and roll back the
    entire partial record, so prune orphans to a fixpoint first (a child dropped
    this round can orphan its own children the next round). Reused parents are
    validated separately by ``record_workflow_run`` and need no pruning.
    """
    jobs_by_address = {(job.step_name, job.address): job for job in run_plan.jobs}
    published_keys = {
        (result.row.step_name, result.row.output_name, result.row.address)
        for result in published_results
    }
    dropped: set[tuple[str, str]] = set()
    changed = True
    while changed:
        changed = False
        for result in published_results:
            job_address = (result.row.step_name, result.row.address)
            if job_address in dropped:
                continue
            job = jobs_by_address.get(job_address)
            if job is None or not _has_unpublished_fresh_parent(job, published_keys):
                continue
            dropped.add(job_address)
            published_keys -= {
                key for key in published_keys if (key[0], key[2]) == job_address
            }
            changed = True
    survivors = tuple(
        result
        for result in published_results
        if (result.row.step_name, result.row.address) not in dropped
    )
    dropped_failures = tuple(
        (step_name, address, "upstream not published")
        for step_name, address in dropped
    )
    return survivors, dropped_failures


def _has_unpublished_fresh_parent(
    job: RunJob,
    published_keys: set[tuple[str, str, str]],
) -> bool:
    for record in job.input_records:
        if record.origin != "workflow_output" or record.registry_source_artifact_id is not None:
            continue
        parent_key = (
            record.source_step_name,
            record.source_output_name,
            record.source_address,
        )
        if parent_key not in published_keys:
            return True
    return False


def _validate_existing_published_file(path: Path, *, expected_digest: str) -> None:
    if path.is_dir():
        raise ValidationError(f"published output path is a directory: {path}")
    if not path.is_file():
        raise ValidationError(f"missing published output file: {path}")
    if sha256_file_digest(path) != expected_digest:
        raise ValidationError("published output artifact digest mismatch")


def _remove_expected_staged_outputs(run_plan: RunPlan) -> None:
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


def _remove_published_output_files(
    runtime_root: Path,
    rows: tuple[PublishedOutputRow, ...],
) -> None:
    outputs_root = (runtime_root / "outputs").resolve()
    for row in rows:
        relative_path = Path(row.path).expanduser()
        if relative_path.is_absolute() or ".." in relative_path.parts:
            continue
        output_path = (runtime_root / relative_path).resolve()
        try:
            output_path.relative_to(outputs_root)
        except ValueError:
            continue
        if output_path.is_file():
            try:
                output_path.unlink()
            except OSError:
                pass


def _write_run_workspace(
    run_plan: RunPlan,
    *,
    reused_input_paths: dict[str, str] | None = None,
) -> None:
    _prepare_run_workspace(run_plan)
    (run_plan.run_workspace / "staging").mkdir(exist_ok=True)
    (run_plan.run_workspace / "logs").mkdir(exist_ok=True)
    _write_json_file(run_plan.run_workspace / "run_plan.json", _run_plan_payload(run_plan))
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
        _snakefile_text(run_plan, reused_input_paths=reused_input_paths),
    )


def _write_reuse_only_workspace(run_plan: RunPlan) -> None:
    _prepare_run_workspace(run_plan)
    _remove_stale_executor_file(run_plan.run_workspace / "Snakefile")
    _remove_stale_executor_file(run_plan.run_workspace / "selected_outputs.txt")
    _write_json_file(run_plan.run_workspace / "run_plan.json", _run_plan_payload(run_plan))


def _prepare_run_workspace(run_plan: RunPlan) -> None:
    run_plan.run_workspace.mkdir(parents=True, exist_ok=True)
    _remove_stale_executor_file(run_plan.run_workspace / "logs" / "snakemake.log")


def _remove_stale_executor_file(path: Path) -> None:
    if path.is_dir():
        raise ValidationError(f"executor-owned path is a directory: {path}")
    if path.exists() or path.is_symlink():
        path.unlink()


def _dry_run_reused_input_paths(run_plan: RunPlan) -> dict[str, str]:
    """Map reachable reused staging inputs to validated registered source paths.

    Keys derive from the in-memory validated ``ReusedRunJobOutputRef``s, never
    from the serialized run plan. The mapped target is the freshly re-resolved
    candidate's registered path — not the planned snapshot — so a registry
    change between planning and execution is either revalidated or rejected.
    Resolution re-checks identity, dependencies, outputs/ containment,
    existence, and size without hashing bytes.
    """
    candidates = _reresolve_reused_bundles(run_plan)
    mapping: dict[str, str] = {}
    for output_ref in run_plan.reused_outputs:
        candidate = candidates[output_ref.source_artifact_id]
        mapping[output_ref.staging_path_relative] = os.path.relpath(
            run_plan.runtime_root / candidate.path,
            run_plan.run_workspace,
        ).replace(os.sep, "/")
    return mapping


def _hydrate_reused_outputs(
    run_plan: RunPlan,
) -> dict[int, ReusableArtifactCandidate]:
    candidates = _reresolve_reused_bundles(run_plan)
    verified_artifact_ids = _verify_selected_reused_outputs(
        run_plan,
        candidates=candidates,
    )
    for output_ref in run_plan.reused_outputs:
        candidate = candidates[output_ref.source_artifact_id]
        source_path = run_plan.runtime_root / candidate.path
        if not source_path.is_file():
            raise ValidationError(
                f"missing reusable artifact file: {candidate.path}"
            )
        if source_path.stat().st_size != candidate.file_size:
            raise ValidationError("reusable artifact file size mismatch during hydration")
        if (
            candidate.artifact_id not in verified_artifact_ids
            and sha256_file_digest(source_path) != candidate.content_digest
        ):
            raise ValidationError("reusable artifact digest mismatch during hydration")
        output_ref.staging_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, output_ref.staging_path)
        if output_ref.staging_path.stat().st_size != candidate.file_size:
            raise ValidationError("hydrated artifact file size mismatch")
        if sha256_file_digest(output_ref.staging_path) != candidate.content_digest:
            raise ValidationError("hydrated artifact digest mismatch")
    return candidates


def _verify_selected_reused_outputs(
    run_plan: RunPlan,
    *,
    candidates: dict[int, ReusableArtifactCandidate],
) -> set[int]:
    verified_artifact_ids: set[int] = set()
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
            if candidate.artifact_id in verified_artifact_ids:
                continue
            path = run_plan.runtime_root / candidate.path
            if not path.is_file():
                raise ValidationError(f"missing reusable artifact file: {candidate.path}")
            if path.stat().st_size != candidate.file_size:
                raise ValidationError("selected reused artifact file size mismatch")
            if sha256_file_digest(path) != candidate.content_digest:
                raise ValidationError("selected reused artifact digest mismatch")
            verified_artifact_ids.add(candidate.artifact_id)
    return verified_artifact_ids


def _reresolve_reused_bundles(
    run_plan: RunPlan,
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


def _run_snakemake(run_plan: RunPlan, *, cores: int, dry_run: bool) -> int:
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
    run_plan: RunPlan,
    *,
    reused_input_paths: dict[str, str] | None = None,
) -> str:
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
        inputs = ["run_plan.json", *job.inputs_as_relative_paths()]
        if reused_input_paths:
            # Substitution touches rule inputs only: a coordinate is either
            # reused (no producing rule) or fresh (rule output), so a reused
            # staging path can never collide with a fresh output line.
            inputs = [reused_input_paths.get(path, path) for path in inputs]
        outputs = [output.staging_path_relative for output in job.outputs.values()]
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


def _run_plan_payload(run_plan: RunPlan) -> dict[str, Any]:
    return {
        "context": run_plan.context,
        "workflow": run_plan.workflow_name,
        "base_workflow": run_plan.base_workflow_name,
        "selected_step": run_plan.selected_step_name,
        "selected_output": run_plan.selected_output_name,
        "requested_address": run_plan.requested_address,
        "runtime_root": str(run_plan.runtime_root),
        "jobs": {
            job.job_id: {
                "step_name": job.step_name,
                "address": job.address,
                "callable_ref": job.callable_ref,
                "outputs": {
                    output_name: output.staging_path_relative
                    for output_name, output in sorted(job.outputs.items())
                },
                "inputs": {name: list(paths) for name, paths in job.inputs.items()},
                "input_records": [
                    _input_record_payload(record) for record in job.input_records
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
        "reused_outputs": [
            {
                "step_name": output_ref.step_name,
                "output_name": output_ref.output_name,
                "address": output_ref.address,
                "staging_path": output_ref.staging_path_relative,
                "source_path": output_ref.source_path_relative,
                "source_artifact_id": output_ref.source_artifact_id,
                "source_workflow_name": output_ref.source_workflow_name,
                "source_run_id": output_ref.source_run_id,
                "content_digest": output_ref.content_digest,
                "file_size": output_ref.file_size,
            }
            for output_ref in run_plan.reused_outputs
        ],
    }


def _input_record_payload(record: ArtifactInputRow) -> dict[str, Any]:
    return {
        "binding_name": record.binding_name,
        "input_path": record.input_path,
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
        "manifest_digest": record.manifest_digest,
        "edge_cardinality": record.edge_cardinality,
        "registry_source_artifact_id": record.registry_source_artifact_id,
        "source_input_records": [
            _input_record_payload(source_record)
            for source_record in record.source_input_records
        ],
    }


def _workflow_output_artifact_rows(
    run_plan: RunPlan,
    *,
    published_rows: tuple[PublishedOutputRow, ...],
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
) -> tuple[WorkflowOutputArtifactRow, ...]:
    published_by_key = {
        (row.step_name, row.output_name, row.address): row
        for row in published_rows
    }
    published_jobs = {(row.step_name, row.address) for row in published_rows}
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
            published_row = published_by_key[key]
            digest = sha256_file_digest(output_ref.staging_path)
            output_hash = short_hash(digest)
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
                    path=published_row.path,
                    staging_path=staging_path,
                    published_path=published_row.path,
                    content_digest=digest,
                    output_hash=output_hash,
                    file_size=output_ref.staging_path.stat().st_size,
                    extension=output_ref.declared_extension,
                    parameters_json=_compact_json(output_ref.params),
                    callable_ref=output_ref.callable_ref,
                    is_selected_output=key in selected_output_keys,
                    is_published=True,
                    input_records=tuple(
                        _actual_input_record(record, actual_reused_artifacts)
                        for record in output_ref.input_records
                    ),
                )
            )
    return tuple(rows)


def _actual_input_record(
    record: ArtifactInputRow,
    actual_reused_artifacts: dict[int, ReusableArtifactCandidate],
) -> ArtifactInputRow:
    nested = tuple(
        _actual_input_record(source_record, actual_reused_artifacts)
        for source_record in record.source_input_records
    )
    artifact_id = record.registry_source_artifact_id
    if artifact_id is None:
        return replace(record, source_input_records=nested)
    candidate = actual_reused_artifacts.get(artifact_id)
    if candidate is None:
        raise ValidationError(
            "executed run is missing a re-resolved reused artifact dependency"
        )
    return replace(
        record,
        registry_source_artifact_id=candidate.artifact_id,
        source_extension=candidate.extension,
        source_input_records=nested,
    )


def _retained_projection_recipes(
    run_plan: RunPlan,
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
    run_plan: RunPlan,
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
        if not isinstance(output_ref.projection_state, ResolvedRequestBundleProjectionV1):
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
    run_plan: RunPlan,
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
                    "re-resolved reusable artifact has the wrong requested coordinate"
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
    run_plan: RunPlan,
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


def _reachable_job_ids(run_plan: RunPlan) -> set[str]:
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
        for output_name, output in job.outputs.items():
            output_directory_relative = (
                f"outputs/{loaded.context}/{plan.workflow_name}/{job.step_name}/{output_name}"
            )
            specs.append(
                PublishedOutputSpec(
                    context=loaded.context,
                    workflow_name=plan.workflow_name,
                    step_name=job.step_name,
                    output_name=output_name,
                    address=job.address,
                    declared_extension=output.declared_extension,
                    output_directory=loaded.runtime_root / output_directory_relative,
                    output_directory_relative=output_directory_relative,
                )
            )
    return tuple(specs)


def _run_manifest_binding_rows(run_plan: RunPlan) -> tuple[RunManifestBindingRow, ...]:
    return tuple(
        RunManifestBindingRow(
            step_name=binding.step_name,
            role=binding.role,
            manifest_name=binding.manifest_name,
            manifest_digest=binding.manifest_digest,
            manifest_hash=binding.manifest_hash,
            entity_count=binding.entity_count,
        )
        for binding in run_plan.manifest_bindings
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


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    try:
        content = json.dumps(
            payload,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
    except ValueError as exc:
        raise ValidationError("runtime JSON payload must be finite JSON values") from exc
    _write_text_file(path, content)


def _write_text_file(path: Path, content: str) -> None:
    if path.is_file() and path.read_text(encoding="utf-8") == content:
        return
    path.write_text(content, encoding="utf-8")


def _build_jobs(
    *,
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
    run_workspace: Path,
) -> tuple[tuple[RunJob, ...], dict[tuple[str, str, str], ReusedRunJobOutputRef]]:
    jobs: list[RunJob] = []
    source_snapshots = read_registered_source_snapshots(
        loaded.runtime_root / REGISTRY_DB_PATH,
        context=loaded.context,
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
    for step in plan.steps:
        outputs = _validated_step_outputs(step)
        addresses = _step_addresses(loaded, plan, step)
        for address in addresses:
            input_paths, input_records = _job_inputs(
                loaded,
                step,
                run_workspace=run_workspace,
                address=address,
                outputs_by_artifact=outputs_by_artifact,
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
            if isinstance(projection_state, ResolvedRequestBundleProjectionV1):
                reused_refs = _reusable_output_refs_for_job(
                    loaded=loaded,
                    job=job,
                )
            if reused_refs is not None:
                for output_name, output_ref in reused_refs.items():
                    key = (step.step_name, output_name, address)
                    outputs_by_artifact[key] = output_ref
                    reused_outputs_by_artifact[key] = output_ref
                continue
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
) -> RequestBundleProjectionPlanV1:
    return RequestBundleProjectionPlanV1(
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
            if len(records) != 1 or records[0].source_artifact_path is None:
                raise ValidationError(
                    f"source input binding {binding_name!r} is malformed"
                )
            binding_plans.append(
                SourceBindingPlan(
                    role=binding_name,
                    source_coordinate=SourceCoordinate(
                        namespace=context,
                        path=records[0].source_artifact_path,
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
            manifest_digests = {record.manifest_digest for record in records}
            if len(manifest_digests) != 1:
                raise ValidationError(
                    f"collection input binding {binding_name!r} has inconsistent manifests"
                )
            binding_plans.append(
                CollectionBindingPlan(
                    role=binding_name,
                    collection_semantics="coordinate_set_v1",
                    manifest_digest=next(iter(manifest_digests)),
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
) -> dict[str, ReusedRunJobOutputRef] | None:
    if not isinstance(job.projection_state, ResolvedRequestBundleProjectionV1):
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
    bundle = resolve_reusable_artifact_bundle(
        registry_path,
        runtime_root=loaded.runtime_root,
        request=request,
    )
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
        if record.registry_source_artifact_id is not None
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
        if record.registry_source_artifact_id is not None
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
            if record.registry_source_artifact_id is not None
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
) -> tuple[dict[str, tuple[str, ...]], tuple[ArtifactInputRow, ...]]:
    inputs: dict[str, tuple[str, ...]] = {}
    input_records: list[ArtifactInputRow] = []
    if step.execution_role == "source_import":
        for binding_name in step.source_inputs:
            source_artifact_path = _source_artifact_path_for_binding(
                loaded.source_index,
                binding_name=binding_name,
                address=address,
            )
            source_input_path = _source_input_path_for_workspace(
                runtime_root=loaded.runtime_root,
                run_workspace=run_workspace,
                source_artifact_path=source_artifact_path,
            )
            inputs[binding_name] = (source_input_path,)
            input_records.append(
                ArtifactInputRow(
                    binding_name=binding_name,
                    input_path=source_input_path,
                    dependency_role="source_input",
                    origin="source",
                    source_artifact_path=source_artifact_path,
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
            allowed_addresses: set[str] | None = None
            manifest_digest: str | None = None
            manifest_name: str | None = None
            if step.manifest_binding is not None:
                manifest_name = step.manifest_binding.manifest_name
                manifest = loaded.manifests[manifest_name]
                allowed_addresses = set(manifest.entity_ids)
                manifest_digest = manifest.manifest_digest
            matching_outputs = [
                output_ref
                for (
                    source_step,
                    source_output,
                    _source_address,
                ), output_ref in outputs_by_artifact.items()
                if source_step == step_input.source_step_name
                and source_output == step_input.source_output_name
                and (
                    allowed_addresses is None
                    or _source_address in allowed_addresses
                )
            ]
            matching_outputs = sorted(matching_outputs, key=lambda item: item.address)
            if not matching_outputs:
                manifest_label = (
                    f"manifest {manifest_name!r}"
                    if manifest_name is not None
                    else "no manifest binding"
                )
                raise ValidationError(
                    f"input {input_name!r} for step {step.step_name!r} selected "
                    "no upstream artifacts from "
                    f"{step_input.source_step_name}.{step_input.source_output_name} "
                    f"using {manifest_label}"
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
                    manifest_digest=manifest_digest,
                    edge_cardinality=edge_cardinality,
                )
                for output_ref in matching_outputs
            )
        else:
            raise ValidationError(
                f"unsupported dependency role for execution: {step_input.dependency_role}"
            )
    return inputs, tuple(input_records)


def _source_artifact_path_for_binding(
    source_index: SourceIndex,
    *,
    binding_name: str,
    address: str,
) -> str:
    global_path = source_index.global_bindings.get(binding_name)
    entity_path = source_index.entity_bindings.get(address, {}).get(binding_name)
    if global_path is not None and entity_path is not None:
        raise ValidationError(
            f"ambiguous source binding {binding_name!r} for address {address!r}"
        )
    if entity_path is not None:
        return entity_path
    if global_path is not None:
        return global_path
    raise ValidationError(
        f"missing source binding {binding_name!r} for address {address!r}"
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
    loaded: LoadedWorkflowProject,
    plan: WorkflowPlan,
    step: WorkflowPlanStep,
) -> tuple[str, ...]:
    output_scope = next(iter(_validated_step_outputs(step).values())).address_scope
    if output_scope == "entity":
        manifest_name = _entity_manifest_name(plan)
        return tuple(
            validate_path_token(entity_id, label="entity_id")
            for entity_id in loaded.manifests[manifest_name].entity_ids
        )
    if output_scope == "cohort":
        if step.manifest_binding is None:
            raise ValidationError(
                f"cohort step {step.step_name!r} must have a manifest binding"
            )
        return (step.manifest_binding.manifest_name,)
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
    loaded: LoadedWorkflowProject,
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
        if selected_step.manifest_binding is None:
            raise ValidationError(
                "cohort workflow steps must have a selected-step manifest binding"
            )
        return (selected_step.manifest_binding.manifest_name,)
    if output.address_scope == "entity":
        manifest_name = _entity_manifest_name(plan)
        try:
            manifest = loaded.manifests[manifest_name]
        except KeyError as exc:
            raise ValidationError(f"unknown manifest for entity step: {manifest_name}") from exc
        if requested_address is not None:
            address = validate_path_token(requested_address, label="address")
            if address not in manifest.entity_ids:
                raise ValidationError(
                    f"address {address!r} is not a member of source-population "
                    f"manifest {manifest_name!r}"
                )
            return (address,)
        return tuple(
            validate_path_token(entity_id, label="entity_id")
            for entity_id in manifest.entity_ids
        )
    raise ValidationError(f"unsupported workflow step address_scope: {output.address_scope}")


def _entity_manifest_name(plan: WorkflowPlan) -> str:
    source_population_bindings = [
        binding.manifest_name
        for binding in plan.manifest_bindings
        if binding.role == "source_population"
    ]
    if len(source_population_bindings) != 1:
        found = ", ".join(source_population_bindings) or "none"
        raise ValidationError(
            "entity workflow steps require exactly one source_population "
            f"manifest binding; found {len(source_population_bindings)}: {found}"
        )
    return source_population_bindings[0]


def _plan_step(plan: WorkflowPlan, step_name: str) -> WorkflowPlanStep:
    for step in plan.steps:
        if step.step_name == step_name:
            return step
    raise ValidationError(f"workflow plan selected step is missing: {step_name}")
