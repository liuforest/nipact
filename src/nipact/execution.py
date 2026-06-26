"""Internal execution planning helpers for NIPACT workflow runs."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .artifacts import output_filename
from .errors import ValidationError
from .hashing import sha256_file_digest, short_hash
from .identity import validate_path_token
from .registry import (
    ArtifactInputRow,
    PublishedOutputRow,
    REGISTRY_DB_PATH,
    ReusableArtifactCandidate,
    ReusableArtifactRequest,
    RunManifestBindingRow,
    WorkflowOutputArtifactRow,
    record_workflow_run,
    resolve_reusable_artifact,
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
    content_digest: str
    file_size: int
    reuse_request: ReusableArtifactRequest


@dataclass(frozen=True)
class RunPlan:
    project_root: Path
    runtime_root: Path
    context: str
    workflow_name: str
    base_workflow_name: str | None
    selected_step_name: str
    selected_output_name: str
    run_workspace: Path
    manifest_bindings: tuple[WorkflowPlanManifestBinding, ...]
    published_outputs: tuple[PublishedOutputSpec, ...]
    jobs: tuple[RunJob, ...]
    selected_jobs: tuple[RunJob, ...]
    selected_output_refs: tuple[RunJobOutputRef, ...]
    reused_outputs: tuple[ReusedRunJobOutputRef, ...]
    reuse_workflow_names: tuple[str, ...]


@dataclass(frozen=True)
class RunOutcome:
    """Result of a best-effort job-atomic run."""

    published_count: int
    failed_jobs: tuple[tuple[str, str, str], ...]  # (step, address, coarse reason)
    all_selected_published: bool


def build_run_plan(
    *,
    project_dir: Path,
    context: str,
    workflow_name: str,
    step_name: str,
) -> RunPlan:
    """Build the internal execution plan without running anything."""
    loaded = load_workflow_project(project_dir=project_dir, context=context)
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
    addresses = _selected_addresses(loaded, plan, selected_step)
    jobs, reused_outputs = _build_jobs(
        loaded=loaded,
        plan=plan,
        run_workspace=run_workspace,
        selected_step_name=selected_step.step_name,
        selected_output_name=selected_output.name,
        selected_addresses=addresses,
    )
    output_refs_by_key = _output_refs_by_key(jobs)
    selected_output_refs = tuple(
        output_refs_by_key[(selected_step.step_name, selected_output.name, address)]
        for address in addresses
    )
    selected_jobs = tuple(
        output_ref.job for output_ref in selected_output_refs
    )
    published_outputs = _published_output_specs(
        loaded=loaded,
        plan=plan,
        jobs=jobs,
        selected_output_refs=selected_output_refs,
    )
    return RunPlan(
        project_root=loaded.project_root,
        runtime_root=loaded.runtime_root,
        context=context,
        workflow_name=plan.workflow_name,
        base_workflow_name=loaded.workflows[plan.workflow_name].base_workflow,
        selected_step_name=selected_step.step_name,
        selected_output_name=selected_output.name,
        run_workspace=run_workspace,
        manifest_bindings=plan.manifest_bindings,
        published_outputs=published_outputs,
        jobs=jobs,
        selected_jobs=selected_jobs,
        selected_output_refs=selected_output_refs,
        reused_outputs=reused_outputs,
        reuse_workflow_names=_reuse_workflow_names(loaded, plan.workflow_name),
    )


def execute_run_plan(
    run_plan: RunPlan,
    *,
    cores: int = 1,
    dry_run: bool = False,
    status_callback: RunStatusCallback | None = None,
) -> RunOutcome:
    """Run a workflow plan through Snakemake and publish completed jobs best-effort.

    Snakemake runs with ``--keep-going``, so a single failed job no longer aborts
    the run: every job whose declared outputs all landed in staging is published
    and recorded, the failures are skipped, and the survivors stay reusable. A run
    that published nothing while Snakemake exited non-zero is the one hard error.
    """
    if cores <= 0:
        raise ValidationError("cores must be a positive integer")
    _emit_status(status_callback, "building_workspace")
    _write_run_workspace(run_plan)
    _hydrate_reused_outputs(run_plan)
    if not dry_run:
        _remove_expected_staged_outputs(run_plan)
    _emit_status(status_callback, "starting_snakemake")
    returncode = _run_snakemake(run_plan, cores=cores, dry_run=dry_run)
    _emit_status(status_callback, "snakemake_complete")
    if dry_run:
        return RunOutcome(
            published_count=0,
            failed_jobs=(),
            all_selected_published=True,
        )
    _emit_status(status_callback, "publishing_outputs")
    published_results, publish_failures = _publish_run_outputs(run_plan)
    published_results, prune_failures = _prune_orphan_published_jobs(
        run_plan,
        published_results,
    )
    failed_jobs = tuple(sorted(publish_failures + prune_failures))
    published_rows = tuple(result.row for result in published_results)
    created_rows = tuple(result.row for result in published_results if result.created)
    if not published_rows and returncode != 0:
        log_path = run_plan.run_workspace / "logs" / "snakemake.log"
        raise ValidationError(
            f"Snakemake failed with exit code {returncode}; see {log_path}"
        )
    try:
        artifact_rows = _workflow_output_artifact_rows(
            run_plan,
            published_rows=published_rows,
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
            manifest_bindings=_run_manifest_binding_rows(run_plan),
            published_outputs=published_rows,
            allowed_reused_workflow_names=run_plan.reuse_workflow_names,
        )
        _emit_status(status_callback, "registry_updated")
        selected_addresses = {
            (job.step_name, job.address) for job in run_plan.selected_jobs
        }
        published_addresses = {
            (result.row.step_name, result.row.address) for result in published_results
        }
        return RunOutcome(
            published_count=published_count,
            failed_jobs=failed_jobs,
            all_selected_published=selected_addresses <= published_addresses,
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


def _write_run_workspace(run_plan: RunPlan) -> None:
    run_plan.run_workspace.mkdir(parents=True, exist_ok=True)
    (run_plan.run_workspace / "staging").mkdir(exist_ok=True)
    (run_plan.run_workspace / "logs").mkdir(exist_ok=True)
    _write_json_file(run_plan.run_workspace / "run_plan.json", _run_plan_payload(run_plan))
    selected_outputs = [
        output_ref.staging_path_relative for output_ref in run_plan.selected_output_refs
    ]
    _write_text_file(
        run_plan.run_workspace / "selected_outputs.txt",
        "\n".join(selected_outputs) + "\n",
    )
    _write_text_file(
        run_plan.run_workspace / "Snakefile",
        _snakefile_text(run_plan),
    )


def _hydrate_reused_outputs(run_plan: RunPlan) -> None:
    for output_ref in run_plan.reused_outputs:
        candidate = resolve_reusable_artifact(
            run_plan.runtime_root / REGISTRY_DB_PATH,
            runtime_root=run_plan.runtime_root,
            request=output_ref.reuse_request,
        )
        if candidate is None or candidate.artifact_id != output_ref.source_artifact_id:
            raise ValidationError("reusable artifact is no longer valid")
        source_path = output_ref.source_path
        if not source_path.is_file():
            raise ValidationError(
                f"missing reusable artifact file: {output_ref.source_path_relative}"
            )
        if source_path.stat().st_size != output_ref.file_size:
            raise ValidationError("reusable artifact file size mismatch during hydration")
        if sha256_file_digest(source_path) != output_ref.content_digest:
            raise ValidationError("reusable artifact digest mismatch during hydration")
        output_ref.staging_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, output_ref.staging_path)
        if output_ref.staging_path.stat().st_size != output_ref.file_size:
            raise ValidationError("hydrated artifact file size mismatch")
        if sha256_file_digest(output_ref.staging_path) != output_ref.content_digest:
            raise ValidationError("hydrated artifact digest mismatch")


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
        output_ref.staging_path_relative for output_ref in run_plan.selected_output_refs
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


def _snakefile_text(run_plan: RunPlan) -> str:
    lines = [
        "# Generated by NIPACT. Do not edit.",
        "",
    ]
    for index, job in enumerate(run_plan.jobs):
        inputs = ["run_plan.json", *job.inputs_as_relative_paths()]
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
        "selected_outputs": [
            output_ref.staging_path_relative for output_ref in run_plan.selected_output_refs
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
) -> tuple[WorkflowOutputArtifactRow, ...]:
    published_by_key = {
        (row.step_name, row.output_name, row.address): row
        for row in published_rows
    }
    published_jobs = {(row.step_name, row.address) for row in published_rows}
    selected_output_keys = {
        (output_ref.step_name, output_ref.output_name, output_ref.address)
        for output_ref in run_plan.selected_output_refs
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
                    input_records=output_ref.input_records,
                )
            )
    return tuple(rows)


def _reachable_job_ids(run_plan: RunPlan) -> set[str]:
    return _reachable_job_ids_for_outputs(
        jobs=run_plan.jobs,
        selected_output_refs=run_plan.selected_output_refs,
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
    selected_output_refs: tuple[RunJobOutputRef, ...],
) -> tuple[PublishedOutputSpec, ...]:
    reachable_job_ids = _reachable_job_ids_for_outputs(
        jobs=jobs,
        selected_output_refs=selected_output_refs,
    )
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
    selected_step_name: str,
    selected_output_name: str,
    selected_addresses: tuple[str, ...],
) -> tuple[tuple[RunJob, ...], tuple[ReusedRunJobOutputRef, ...]]:
    jobs: list[RunJob] = []
    outputs_by_artifact: dict[
        tuple[str, str, str],
        RunJobOutputRef | ReusedRunJobOutputRef,
    ] = {}
    reused_outputs_by_artifact: dict[tuple[str, str, str], ReusedRunJobOutputRef] = {}
    selected_keys = {
        (selected_step_name, selected_output_name, address)
        for address in selected_addresses
    }

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
            )
            job_keys = {
                (step.step_name, output_name, address)
                for output_name in outputs
            }
            reused_refs: dict[str, ReusedRunJobOutputRef] | None = None
            if not job_keys.intersection(selected_keys):
                reused_refs = _reusable_output_refs_for_job(
                    loaded=loaded,
                    plan=plan,
                    run_workspace=run_workspace,
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

    missing_selected = sorted(selected_keys - set(outputs_by_artifact))
    if missing_selected:
        raise ValidationError("run plan is missing selected job(s)")
    used_reused_outputs = _used_reused_outputs(
        jobs=tuple(jobs),
        reused_outputs_by_artifact=reused_outputs_by_artifact,
    )
    return tuple(jobs), used_reused_outputs


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
    plan: WorkflowPlan,
    run_workspace: Path,
    job: RunJob,
) -> dict[str, ReusedRunJobOutputRef] | None:
    registry_path = loaded.runtime_root / REGISTRY_DB_PATH
    reuse_workflow_names = _reuse_workflow_names(loaded, plan.workflow_name)
    for workflow_name in reuse_workflow_names:
        candidates: dict[str, ReusableArtifactCandidate] = {}
        requests: dict[str, ReusableArtifactRequest] = {}
        for output_name, output in job.outputs.items():
            request = ReusableArtifactRequest(
                context=loaded.context,
                workflow_name=workflow_name,
                step_name=job.step_name,
                output_name=output_name,
                address=job.address,
                extension=output.declared_extension,
                callable_ref=job.callable_ref,
                parameters_json=_compact_json(job.params),
                input_records=job.input_records,
                allowed_workflow_names=reuse_workflow_names,
            )
            candidate = resolve_reusable_artifact(
                registry_path,
                runtime_root=loaded.runtime_root,
                request=request,
            )
            if candidate is None:
                break
            candidates[output_name] = candidate
            requests[output_name] = request
        if len(candidates) != len(job.outputs):
            continue
        if len({candidate.run_id for candidate in candidates.values()}) > 1:
            continue
        return {
            output_name: _reused_output_ref(
                runtime_root=loaded.runtime_root,
                output=job.outputs[output_name],
                job=job,
                candidate=candidate,
                request=requests[output_name],
            )
            for output_name, candidate in candidates.items()
        }
    return None


def _reused_output_ref(
    *,
    runtime_root: Path,
    output: RunJobOutput,
    job: RunJob,
    candidate: ReusableArtifactCandidate,
    request: ReusableArtifactRequest,
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
        content_digest=candidate.content_digest,
        file_size=candidate.file_size,
        reuse_request=request,
    )


def _reuse_workflow_names(
    loaded: LoadedWorkflowProject,
    workflow_name: str,
) -> tuple[str, ...]:
    names: list[str] = []
    current: str | None = workflow_name
    while current is not None:
        names.append(current)
        current = loaded.workflows[current].base_workflow
    return tuple(names)


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
) -> tuple[str, ...]:
    output = selected_step.outputs[plan.selected_output_name]
    if output.address_scope == "cohort":
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
