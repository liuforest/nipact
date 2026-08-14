"""Command-line interface for NIPACT."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence as PresentationSequence
from dataclasses import fields, is_dataclass
import json
from importlib.metadata import PackageNotFoundError, metadata, version
import os
from pathlib import Path
import sys
from time import perf_counter
from typing import Any, Sequence

from ._version import __version__

PACKAGE_NAME = "nipact"
FALLBACK_DESCRIPTION = (
    "Lightweight Snakemake wrapper and orchestrator for NIPACT workflows."
)


def _package_description() -> str:
    try:
        return metadata(PACKAGE_NAME).get("Summary") or FALLBACK_DESCRIPTION
    except PackageNotFoundError:
        return FALLBACK_DESCRIPTION


def _package_version() -> str:
    try:
        return version(PACKAGE_NAME)
    except PackageNotFoundError:
        return __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nipact",
        description=_package_description(),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_package_version()}",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command", required=True)

    init_parser = subparsers.add_parser(
        "init",
        help="initialize a packaged demo project",
        description="Initialize a packaged NIPACT demo project.",
    )
    init_parser.add_argument(
        "--demo",
        required=True,
        help="Packaged demo name. Supported demos: colors, fmri, dfc.",
    )
    init_parser.add_argument(
        "--project-dir",
        required=True,
        type=Path,
        help="Directory where project config, manifests, steps, and workflows are created.",
    )
    init_parser.add_argument(
        "--runtime-dir",
        required=True,
        type=Path,
        help="Directory where mutable demo runtime files are created.",
    )
    init_parser.add_argument(
        "--context",
        default=None,
        help="Context name to write into the generated project config. Defaults to --demo.",
    )

    validate_parser = subparsers.add_parser(
        "validate",
        help="validate a NIPACT project context",
        description="Validate a NIPACT project context without mutating runtime files.",
    )
    validate_parser.add_argument(
        "--project-dir",
        default=None,
        type=Path,
        help=(
            "Project directory containing nipact.yaml. If omitted, resolve from "
            "nipact.contexts.yaml or the current project root."
        ),
    )
    validate_parser.add_argument(
        "--context",
        required=True,
        help="Context name expected in nipact.yaml.",
    )

    gui_parser = subparsers.add_parser(
        "gui",
        help="serve the local provenance GUI",
        description="Serve the read-only local NIPACT provenance GUI.",
    )
    gui_parser.add_argument(
        "--context",
        required=True,
        help="Context name expected in nipact.yaml.",
    )
    gui_parser.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help=(
            "Project directory containing nipact.yaml. If omitted, resolve from "
            "nipact.contexts.yaml or the current project root."
        ),
    )
    gui_parser.add_argument(
        "--port",
        type=_positive_int,
        default=8765,
        help="Loopback port for the GUI server. Default: 8765.",
    )

    trace_parser = subparsers.add_parser(
        "trace",
        help="trace registered artifact provenance",
        description="Trace one registered artifact backward through its dependencies.",
    )
    _add_project_context_args(trace_parser)
    trace_parser.add_argument(
        "--artifact-id",
        type=int,
        default=None,
        help="Registry artifact primary key to trace.",
    )
    trace_parser.add_argument(
        "--file-path",
        default=None,
        help="Registered runtime-relative artifact path to trace.",
    )
    trace_parser.add_argument(
        "--workflow",
        default=None,
        help="Workflow name for workflow-coordinate selection.",
    )
    trace_parser.add_argument(
        "--step",
        default=None,
        help="Step name for workflow-coordinate selection.",
    )
    trace_parser.add_argument(
        "--output",
        default=None,
        help="Output name for workflow-coordinate selection.",
    )
    trace_parser.add_argument(
        "--address",
        default=None,
        help="Address for workflow-coordinate selection.",
    )
    trace_parser.add_argument(
        "--json",
        action="store_true",
        help="Print trace graph JSON instead of the text summary.",
    )

    registry_parser = subparsers.add_parser(
        "registry",
        help="manage the project registry",
        description="Manage the selected NIPACT project registry.",
    )
    registry_subparsers = registry_parser.add_subparsers(
        dest="registry_command",
        metavar="registry-command",
        required=True,
    )
    registry_migrate_parser = registry_subparsers.add_parser(
        "migrate",
        help="migrate an exact schema-18 registry to schema 19",
        description="migrate an exact schema-18 registry to schema 19",
    )
    _add_project_context_args(registry_migrate_parser)

    specifications_parser = subparsers.add_parser(
        "specifications",
        help="preview, freeze, run, or inspect specification snapshots",
        description="Work with immutable NIPACT specification snapshots.",
    )
    specifications_subparsers = specifications_parser.add_subparsers(
        dest="specifications_command",
        metavar="specifications-command",
        required=True,
    )

    specifications_preview_parser = specifications_subparsers.add_parser(
        "preview",
        help="compile a specification snapshot without persisting it",
        description="Compile and display a specification snapshot without persisting it.",
    )
    _add_specification_source_args(specifications_preview_parser)
    _add_project_context_args(specifications_preview_parser)

    specifications_freeze_parser = specifications_subparsers.add_parser(
        "freeze",
        help="freeze an immutable specification snapshot",
        description="Compile and freeze an immutable specification snapshot.",
    )
    _add_specification_source_args(specifications_freeze_parser)
    _add_project_context_args(specifications_freeze_parser)

    specifications_run_parser = specifications_subparsers.add_parser(
        "run",
        help="run frozen specification members",
        description="Run selected members from an immutable specification snapshot.",
    )
    specifications_run_parser.add_argument(
        "snapshot_digest",
        metavar="full-snapshot-digest",
        help="Full digest of the frozen specification snapshot.",
    )
    specifications_run_selection = (
        specifications_run_parser.add_mutually_exclusive_group(required=True)
    )
    specifications_run_selection.add_argument(
        "--member",
        help="Included specification member key to run.",
    )
    specifications_run_selection.add_argument(
        "--all",
        action="store_true",
        dest="all_members",
        help="Run all included members in canonical order.",
    )
    _add_project_context_args(specifications_run_parser)
    specifications_run_parser.add_argument(
        "--cores",
        type=_positive_int,
        default=1,
        help="Number of Snakemake cores to use. Default: 1.",
    )

    specifications_results_parser = specifications_subparsers.add_parser(
        "results",
        help="read exact results for a frozen specification snapshot",
        description="Read exact normalized results for a frozen specification snapshot.",
    )
    specifications_results_parser.add_argument(
        "snapshot_digest",
        metavar="full-snapshot-digest",
        help="Full digest of the frozen specification snapshot.",
    )
    _add_project_context_args(specifications_results_parser)

    workflow_parser = subparsers.add_parser(
        "workflow",
        help="work with declared workflows",
        description="Inspect declarations or run selected NIPACT workflow steps.",
    )
    workflow_subparsers = workflow_parser.add_subparsers(
        dest="workflow_command",
        metavar="workflow-command",
        required=True,
    )

    workflow_list_parser = workflow_subparsers.add_parser(
        "list",
        help="list declared workflows",
        description="List declared workflows for a NIPACT project context.",
    )
    _add_project_context_args(workflow_list_parser)

    workflow_steps_parser = workflow_subparsers.add_parser(
        "steps",
        help="list runnable workflow steps",
        description="List workflow steps that can be selected with --step.",
    )
    _add_project_context_args(workflow_steps_parser)
    workflow_steps_parser.add_argument(
        "--workflow",
        required=True,
        help="Workflow name.",
    )

    workflow_plan_parser = workflow_subparsers.add_parser(
        "plan",
        help="compile a read-only workflow step plan",
        description="Compile a selected workflow step into a read-only plan.",
    )
    _add_project_context_args(workflow_plan_parser)
    _add_workflow_step_args(workflow_plan_parser)

    workflow_graph_parser = workflow_subparsers.add_parser(
        "graph",
        help="print workflow step graph JSON",
        description="Print graph JSON for a selected workflow step.",
    )
    _add_project_context_args(workflow_graph_parser)
    _add_workflow_step_args(workflow_graph_parser)

    workflow_run_parser = workflow_subparsers.add_parser(
        "run",
        help="ensure a selected workflow output is available",
        description=(
            "Ensure a selected workflow output is available, reusing a valid "
            "result or executing missing work."
        ),
    )
    _add_project_context_args(workflow_run_parser)
    _add_workflow_step_args(workflow_run_parser)
    workflow_run_parser.add_argument(
        "--address",
        default=None,
        help=(
            "Source-population entity address to target. Planning still "
            "validates the full population, and a fresh cohort ancestor can "
            "still execute other entities' upstream jobs."
        ),
    )
    workflow_run_parser.add_argument(
        "--cores",
        type=_positive_int,
        default=1,
        help="Number of Snakemake cores to use. Default: 1.",
    )
    workflow_run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Forecast selected-output resolution and any required fresh "
            "Snakemake work in an isolated dry-run workspace without preparing "
            "reused artifacts, running jobs, or publishing outputs."
        ),
    )
    return parser


def _add_project_context_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--project-dir",
        default=None,
        type=Path,
        help=(
            "Project directory containing nipact.yaml. If omitted, resolve from "
            "nipact.contexts.yaml or the current project root."
        ),
    )
    parser.add_argument(
        "--context",
        required=True,
        help="Context name expected in nipact.yaml.",
    )


def _add_specification_source_args(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "registered_specification_key",
        nargs="?",
        metavar="registered-specification-key",
        help="Registered specification key.",
    )
    source.add_argument(
        "--file",
        type=Path,
        dest="specification_file",
        metavar="PATH",
        help="Path to an explicit strict specification-set file.",
    )


def _add_workflow_step_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workflow",
        required=True,
        help="Workflow name.",
    )
    parser.add_argument(
        "--step",
        required=True,
        help="Workflow step name.",
    )


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        relative = os.path.relpath(resolved, Path.cwd().resolve())
    except ValueError:
        return str(resolved)
    if relative == "." or not relative.startswith(f"..{os.sep}"):
        return relative
    return str(resolved)


def _print_pass(text: str) -> None:
    from .cli_feedback import CliFeedback

    CliFeedback().pass_line(text)


def _run_specifications_command(args: argparse.Namespace) -> int:
    from .errors import ValidationError

    project_dir = _resolve_project_dir_arg(args)
    if args.specifications_command == "preview":
        from .specification_execution import compile_specification_snapshot

        compiled = compile_specification_snapshot(
            project_dir=project_dir,
            context=args.context,
            source=_specification_source(args),
        )
        _print_json_document(_specification_preview_payload(compiled))
        return 0

    if args.specifications_command == "freeze":
        from .specification_execution import freeze_specification_snapshot

        result = freeze_specification_snapshot(
            project_dir=project_dir,
            context=args.context,
            source=_specification_source(args),
        )
        _print_json_document(
            {
                "schema": "nipact/specification-freeze/v1",
                "context": result.snapshot.context,
                "snapshot_digest": result.snapshot.snapshot_digest,
                "status": "inserted" if result.inserted else "verified_existing",
                "counts": _specification_counts(result.snapshot.members),
            }
        )
        return 0

    if args.specifications_command == "run":
        return _run_frozen_specification(args=args, project_dir=project_dir)

    if args.specifications_command == "results":
        from .project_context import resolve_project_context
        from .registry import read_specification_snapshot_projections

        resolved = resolve_project_context(
            project_dir=project_dir,
            context=args.context,
        )
        projections = read_specification_snapshot_projections(
            resolved.registry_path,
            context=resolved.context,
            snapshot_digest=args.snapshot_digest,
        )
        _print_json_document(
            {
                "schema": "nipact/specification-results/v1",
                "context": projections.context,
                "snapshot_digest": projections.snapshot_digest,
                "members": _presentation_value(projections.members),
                "attempts": _presentation_value(projections.attempts),
                "results": _presentation_value(projections.results),
                "result_source_basis": _presentation_value(
                    projections.result_source_basis
                ),
            }
        )
        return 0

    raise ValidationError(
        f"unknown specifications command: {args.specifications_command}"
    )


def _specification_source(args: argparse.Namespace) -> object:
    from .specification_loading import (
        ExplicitSpecificationSource,
        RegisteredSpecificationSource,
    )

    if args.specification_file is not None:
        return ExplicitSpecificationSource(path=args.specification_file)
    return RegisteredSpecificationSource(name=args.registered_specification_key)


def _specification_preview_payload(compiled: object) -> dict[str, Any]:
    snapshot = compiled.snapshot
    members = []
    for member in snapshot.members:
        row = json.loads(member.row.canonical_bytes)
        if type(row) is not dict:  # pragma: no cover - canonical row invariant.
            raise TypeError("canonical specification row must decode to an object")
        members.append(
            {
                "member_key": member.member_key,
                "disposition": member.disposition,
                "exclusion_reason": member.exclusion_reason,
                "row_digest": member.row.row_digest,
                "row": row,
                "expected_results": _presentation_value(member.expected_results),
            }
        )
    return {
        "schema": "nipact/specification-preview/v1",
        "context": snapshot.context,
        "specification_set": compiled.set_key,
        "snapshot_digest": snapshot.snapshot_digest,
        "persistence_performed": False,
        "source_policy": "reconcile_at_attempt",
        "counts": _specification_counts(snapshot.members),
        "equal_effective_member_groups": _presentation_value(
            compiled.equal_effective_member_groups
        ),
        "manifest_values": [
            {
                "value_schema": value.value_schema,
                "manifest_digest": value.manifest_digest,
                "entity_count": value.entity_count,
            }
            for value in snapshot.manifest_values
        ],
        "members": members,
    }


def _run_frozen_specification(*, args: argparse.Namespace, project_dir: Path) -> int:
    from .specification_execution import (
        run_all_specification_members,
        run_specification_member,
    )

    if args.member is not None:
        reached_members = (
            run_specification_member(
                project_dir=project_dir,
                context=args.context,
                snapshot_digest=args.snapshot_digest,
                member_key=args.member,
                cores=args.cores,
            ),
        )
        selection = {"mode": "member", "member_key": args.member}
        snapshot_digest = args.snapshot_digest
    else:
        result = run_all_specification_members(
            project_dir=project_dir,
            context=args.context,
            snapshot_digest=args.snapshot_digest,
            cores=args.cores,
        )
        reached_members = result.members
        selection = {"mode": "all", "member_key": None}
        snapshot_digest = result.snapshot_digest

    payload = {
        "schema": "nipact/specification-run/v1",
        "context": args.context,
        "snapshot_digest": snapshot_digest,
        "selection": selection,
        "cores": args.cores,
        "reached_members": [
            _specification_run_member_payload(member) for member in reached_members
        ],
    }
    _print_json_document(payload)
    return 0 if all(member.outcome == "complete" for member in reached_members) else 1


def _specification_run_member_payload(member: object) -> dict[str, Any]:
    ordinary = member.ordinary_outcome
    return {
        "member_key": member.attempt.member_key,
        "attempt_id": member.attempt.attempt_id,
        "outcome": member.outcome,
        "ordinary_outcome": {
            "published_outputs": ordinary.published_count,
            "published_bytes": ordinary.published_bytes,
            "selected_fresh_outputs": ordinary.selected_generated_count,
            "selected_reused_outputs": ordinary.selected_reused_count,
            "all_selected_resolved": ordinary.all_selected_resolved,
            "failed_jobs": [
                {
                    "step_name": step_name,
                    "address": address,
                    "reason": reason,
                }
                for step_name, address, reason in ordinary.failed_jobs
            ],
            "cleanup_warnings": list(ordinary.cleanup_warnings),
        },
    }


def _specification_counts(members: PresentationSequence[object]) -> dict[str, int]:
    included = sum(member.disposition == "included" for member in members)
    excluded = sum(member.disposition == "excluded" for member in members)
    return {
        "candidates": len(members),
        "included": included,
        "excluded": excluded,
    }


def _presentation_value(value: object) -> Any:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _presentation_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        converted: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("presentation mapping keys must be strings")
            converted[key] = _presentation_value(item)
        return converted
    if isinstance(value, PresentationSequence) and not isinstance(
        value, (bytes, bytearray, memoryview)
    ):
        return [_presentation_value(item) for item in value]
    raise TypeError(f"unsupported presentation value type: {type(value).__name__}")


def _print_json_document(payload: Mapping[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _run_workflow_command(args: argparse.Namespace) -> int | None:
    from .errors import ValidationError

    project_dir = _resolve_project_dir_arg(args)
    if args.workflow_command == "run":
        from .cli_feedback import CliFeedback
        from .execution import build_run_plan, execute_run_plan

        run_plan = build_run_plan(
            project_dir=project_dir,
            context=args.context,
            workflow_name=args.workflow,
            step_name=args.step,
            address=args.address,
            dry_run=args.dry_run,
        )
        feedback = CliFeedback()
        feedback.heading("NIPACT workflow run")
        feedback.line()
        feedback.key_value("context", run_plan.context)
        feedback.key_value("workflow", run_plan.workflow_name)
        feedback.key_value("step", run_plan.selected_step_name)
        feedback.key_value("selected_output", run_plan.selected_output_name)
        feedback.key_value(
            "address",
            "all" if run_plan.requested_address is None else run_plan.requested_address,
        )
        feedback.key_value("cores", args.cores)
        feedback.key_value("dry_run", run_plan.dry_run)
        feedback.key_value(
            "selected_outputs",
            len(run_plan.selected_fresh_output_refs)
            + len(run_plan.selected_reused_output_refs),
        )
        feedback.key_value(
            "planned_selected_fresh_outputs",
            len(run_plan.selected_fresh_output_refs),
        )
        feedback.key_value(
            "planned_selected_reused_outputs",
            len(run_plan.selected_reused_output_refs),
        )
        # planned_jobs counts compiled fresh jobs population-wide even for a
        # targeted run; planned_reachable_fresh_jobs and the reuse counters
        # below are closure-scoped.
        feedback.key_value("planned_jobs", len(run_plan.jobs))
        feedback.key_value(
            "planned_reachable_fresh_jobs", run_plan.reachable_job_count
        )
        feedback.key_value(
            "planned_reused_registered_artifacts",
            len(
                {
                    output_ref.source_artifact_id
                    for output_ref in run_plan.reused_outputs
                }
            ),
        )
        feedback.key_value("planned_reused_inputs", len(run_plan.reused_outputs))
        if not run_plan.dry_run:
            feedback.key_value(
                "existing_staged_outputs",
                sum(
                    1
                    for output_ref in run_plan.selected_fresh_output_refs
                    if output_ref.staging_path.is_file()
                ),
            )
        feedback.key_value("run_workspace", _display_path(run_plan.run_workspace))
        if run_plan.selected_fresh_output_refs:
            feedback.key_value(
                "snakemake_log",
                _display_path(run_plan.run_workspace / "logs" / "snakemake.log"),
            )
        if run_plan.dry_run:
            feedback.key_value(
                "note",
                "Dry run: reused registered artifacts are referenced at their "
                "metadata-checked registered paths; no reused-input preparation, "
                "publication, or registry update occurs. Explicit recomputation "
                "is not available in this release.",
            )
        else:
            feedback.key_value(
                "note",
                "Valid selected outputs are reused; missing work executes through "
                "Snakemake. Explicit recomputation is not available in this release.",
            )
        feedback.line()
        feedback.flush()

        active_spinner = None

        def status_callback(event: str) -> None:
            nonlocal active_spinner
            if event.startswith("sources_"):
                status, separator, count = event.removeprefix("sources_").partition(":")
                if separator and status in {"new", "changed", "unchanged"}:
                    feedback.line(f"Sources {status}: {count}")
            elif event == "building_workspace":
                feedback.line("Preparing run workspace...")
            elif event == "starting_snakemake":
                feedback.line("Starting Snakemake...")
                active_spinner = feedback.spinner(
                    "Snakemake is running",
                    started_at=started_at,
                )
                active_spinner.start()
            elif event == "snakemake_complete":
                if active_spinner is not None:
                    active_spinner.stop()
                    active_spinner = None
                feedback.line("Snakemake complete.")
            elif event == "validating_selected_reuse":
                feedback.line("Validating selected reused outputs...")
            elif event == "publishing_outputs":
                feedback.line("Publishing outputs...")
            elif event == "registry_updated":
                feedback.line("Registry updated.")
            feedback.flush()

        started_at = perf_counter()
        try:
            outcome = execute_run_plan(
                run_plan,
                cores=args.cores,
                status_callback=status_callback,
            )
        finally:
            if active_spinner is not None:
                active_spinner.stop()
        elapsed_seconds = perf_counter() - started_at

        feedback.line()
        if run_plan.dry_run:
            feedback.key_value("outputs_published", False)
            feedback.key_value("registry", "not_updated")
        else:
            feedback.key_value("published_outputs", outcome.published_count)
            feedback.key_value("published_bytes", outcome.published_bytes)
            feedback.key_value(
                "selected_generated_outputs", outcome.selected_generated_count
            )
            feedback.key_value(
                "selected_reused_outputs", outcome.selected_reused_count
            )
            feedback.key_value("registry", "updated")
            for step_name, address, reason in outcome.failed_jobs:
                feedback.key_value("failed_job", f"{step_name} {address} ({reason})")
            for warning in outcome.cleanup_warnings:
                feedback.key_value("warning", warning)
        feedback.key_value("elapsed_seconds", f"{elapsed_seconds:.3f}")
        if outcome.all_selected_resolved:
            feedback.pass_line("PASS: workflow run")
            return 0
        feedback.line("PARTIAL: workflow run", style="yellow")
        return 1

    from .workflow import (
        compile_workflow_plan,
        load_workflow_project,
        workflow_plan_to_graph,
    )

    loaded = load_workflow_project(project_dir=project_dir, context=args.context)

    if args.workflow_command == "list":
        for workflow_name in sorted(loaded.workflows):
            print(workflow_name)
        _print_pass("PASS: workflow list")
        return

    if args.workflow_command == "steps":
        workflow = loaded.workflows.get(args.workflow)
        if workflow is None:
            raise ValidationError(f"unknown workflow: {args.workflow}")
        for step_name in workflow.steps:
            if step_name in workflow.step_outputs:
                print(f"step={step_name} output={workflow.step_outputs[step_name]}")
        _print_pass("PASS: workflow steps")
        return

    if args.workflow_command == "plan":
        plan = compile_workflow_plan(
            loaded,
            workflow_name=args.workflow,
            step_name=args.step,
        )
        print(f"workflow={plan.workflow_name}")
        print(f"step={plan.selected_step_name}")
        print(f"selected_output={plan.selected_output_name}")
        print(f"steps={len(plan.steps)}")
        for step in plan.steps:
            outputs = ",".join(step.outputs)
            print(
                f"step={step.step_name} pattern={step.pattern_kind} "
                f"role={step.execution_role} outputs={outputs}"
            )
        if plan.execution_population is not None:
            population = plan.execution_population
            print(
                "execution_population "
                f"manifest={population.manifest_name} "
                f"manifest_value_schema={population.manifest_value_schema} "
                f"manifest_hash={population.manifest_hash} "
                f"entities={population.entity_count}"
            )
        for binding in plan.manifest_bindings:
            print(
                f"manifest_binding step={binding.step_name} "
                f"usage_role={binding.manifest_usage_role} "
                f"manifest={binding.manifest_name} "
                f"manifest_value_schema={binding.manifest_value_schema} "
                f"manifest_hash={binding.manifest_hash} entities={binding.entity_count}"
            )
        _print_workflow_overrides(loaded.workflows[plan.workflow_name].step_overrides)
        _print_workflow_warnings(plan.warnings)
        _print_pass("PASS: workflow plan")
        return

    if args.workflow_command == "graph":
        plan = compile_workflow_plan(
            loaded,
            workflow_name=args.workflow,
            step_name=args.step,
        )
        graph = workflow_plan_to_graph(plan)
        print(json.dumps(graph, indent=2, sort_keys=True))
        return

    raise ValidationError(f"unknown workflow command: {args.workflow_command}")


def _run_registry_command(args: argparse.Namespace) -> None:
    from .errors import ValidationError
    from .project_context import resolve_project_context_for_migration
    from .registry import REGISTRY_SCHEMA_VERSION, migrate_registry_db

    if args.registry_command != "migrate":
        raise ValidationError(f"unknown registry command: {args.registry_command}")

    project_dir = _resolve_project_dir_arg(args)
    resolved = resolve_project_context_for_migration(
        project_dir=project_dir,
        context=args.context,
    )
    result = migrate_registry_db(
        resolved.registry_path,
        context=resolved.context,
        runtime_root=resolved.runtime_root,
    )
    print(f"context={result.context}")
    print(f"registry={_display_path(result.registry_path)}")
    print(f"status={result.status}")
    if result.status == "migrated":
        print(f"from_schema={result.from_schema}")
        print(f"to_schema={result.to_schema}")
        if result.backup_path is None:  # pragma: no cover - internal invariant.
            raise RuntimeError("migrated registry result is missing its backup path")
        print(f"backup={_display_path(result.backup_path)}")
        print(
            "recovery=restore the backup manually before using schema-18 software"
        )
    else:
        print(f"schema={REGISTRY_SCHEMA_VERSION}")
    _print_pass("PASS: registry migrate")


def _run_trace_command(args: argparse.Namespace) -> None:
    project_dir = _resolve_project_dir_arg(args)
    registry_path = _trace_registry_path(
        project_dir=project_dir,
        context=args.context,
    )
    graph = _build_trace_graph(registry_path=registry_path, args=args)
    if args.json:
        print(json.dumps(graph, indent=2, sort_keys=True))
        return
    _print_trace_summary(graph)


def _run_gui_command(args: argparse.Namespace) -> None:
    from .gui.app import create_gui_app

    import uvicorn

    project_dir = _resolve_project_dir_arg(args)
    app = create_gui_app(project_dir=project_dir, context=args.context)
    url = f"http://127.0.0.1:{args.port}/"
    print(url)
    uvicorn.run(app, host="127.0.0.1", port=args.port)


def _build_trace_graph(*, registry_path: Path, args: argparse.Namespace) -> dict[str, Any]:
    from .errors import ValidationError
    from .trace import (
        build_trace_graph_for_artifact_id,
        build_trace_graph_for_path,
        build_trace_graph_for_workflow_coordinate,
    )

    has_artifact_id = args.artifact_id is not None
    has_file_path = args.file_path is not None
    coordinate_values = {
        "--workflow": args.workflow,
        "--step": args.step,
        "--output": args.output,
        "--address": args.address,
    }
    has_any_coordinate = any(value is not None for value in coordinate_values.values())
    has_coordinate = all(value is not None for value in coordinate_values.values())
    if has_any_coordinate and not has_coordinate:
        missing = [
            name for name, value in coordinate_values.items() if value is None
        ]
        raise ValidationError(
            "workflow-coordinate trace selector requires "
            "--workflow, --step, --output, and --address; "
            f"missing {', '.join(missing)}"
        )

    selector_count = sum((has_artifact_id, has_file_path, has_coordinate))
    if selector_count != 1:
        raise ValidationError(
            "provide exactly one trace selector: --artifact-id, --file-path, "
            "or --workflow/--step/--output/--address"
        )

    if has_artifact_id:
        return build_trace_graph_for_artifact_id(
            registry_path,
            artifact_id=args.artifact_id,
            context=args.context,
        )
    if has_file_path:
        return build_trace_graph_for_path(
            registry_path,
            context=args.context,
            artifact_path=args.file_path,
        )
    return build_trace_graph_for_workflow_coordinate(
        registry_path,
        context=args.context,
        workflow_name=args.workflow,
        step_name=args.step,
        output_name=args.output,
        address=args.address,
    )


def _trace_registry_path(*, project_dir: Path, context: str) -> Path:
    from .project_context import resolve_project_context

    return resolve_project_context(project_dir=project_dir, context=context).registry_path


def _resolve_project_dir_arg(args: argparse.Namespace) -> Path:
    from .context_index import resolve_project_dir

    return resolve_project_dir(project_dir=args.project_dir, context=args.context)


def _print_trace_summary(graph: dict[str, Any]) -> None:
    selected_artifact = _selected_trace_artifact(graph)
    print(f"artifact_id={_format_trace_value(graph['selected_artifact_id'])}")
    print(f"origin={_format_trace_value(selected_artifact['origin'])}")
    print(f"is_published={_format_trace_value(selected_artifact['is_published'])}")
    print(f"workflow={_format_trace_value(selected_artifact['workflow_name'])}")
    print(f"step={_format_trace_value(selected_artifact['step_name'])}")
    print(f"output={_format_trace_value(selected_artifact['output_name'])}")
    print(f"address={_format_trace_value(selected_artifact['address'])}")
    print(f"path={_format_trace_value(selected_artifact['path'])}")
    print(f"content_digest={_format_trace_value(selected_artifact['content_digest'])}")
    print(f"output_hash={_format_trace_value(selected_artifact['output_hash'])}")
    print(f"parameter_hash={_format_trace_value(selected_artifact['parameter_hash'])}")
    print(f"upstream_artifacts={max(0, len(graph['artifacts']) - 1)}")
    print(f"dependency_edges={len(graph['dependencies'])}")
    print(f"manifest_bindings={len(graph['manifest_bindings'])}")
    print(f"provenance_status={_format_trace_value(graph['provenance_status'])}")
    print(f"warnings={len(graph['warnings'])}")
    for warning in graph["warnings"]:
        print(
            f"warning {warning['warning_type']}: {warning['message']} "
            f"artifact_id={_format_trace_value(warning.get('artifact_id'))} "
            f"input_path={_format_trace_value(warning.get('input_path'))}"
        )
    _print_pass("PASS: trace")


def _selected_trace_artifact(graph: dict[str, Any]) -> dict[str, Any]:
    from .errors import ValidationError

    selected = [
        artifact for artifact in graph["artifacts"] if artifact.get("is_selected")
    ]
    if len(selected) != 1:
        raise ValidationError("trace graph has malformed selected artifact")
    return selected[0]


def _format_trace_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _print_workflow_overrides(overrides: dict[str, Any]) -> None:
    if not overrides:
        print("overrides=0")
        return
    for step_name in sorted(overrides):
        override = overrides[step_name]
        print(f"override step={step_name} params={_format_params(override.params)}")


def _print_workflow_warnings(warnings: Sequence[str]) -> None:
    if not warnings:
        print("warnings=0")
        return
    for warning in warnings:
        print(f"warning={warning}")


def _format_params(params: dict[str, Any]) -> str:
    if not params:
        return "-"
    return ",".join(
        f"{name}={json.dumps(value, sort_keys=True, separators=(',', ':'))}"
        for name, value in sorted(params.items())
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            from .project_setup import init_project
            from .context_index import (
                preflight_context_index_update,
                update_context_index,
            )

            effective_context = args.context or args.demo
            preflight_context_index_update(
                workspace_dir=Path.cwd(),
                context=effective_context,
                project_dir=args.project_dir,
            )

            result = init_project(
                demo=args.demo,
                project_dir=args.project_dir,
                runtime_dir=args.runtime_dir,
                context=args.context,
            )
            context_index_path = update_context_index(
                workspace_dir=Path.cwd(),
                context=result.context,
                project_dir=result.project_root,
            )
            print(f"project_root={_display_path(result.project_root)}")
            print(f"runtime_root={_display_path(result.runtime_root)}")
            print(f"context={result.context}")
            print(f"context_index={_display_path(context_index_path)}")
            print(f"demo={args.demo}")
            print(f"source_index={result.source_index}")
            print(f"manifest_count={result.manifest_count}")
            print(f"source_file_count={result.source_file_count}")
            if result.source_hash is not None and result.manifest_hash is not None:
                print("source_data=data/color_source.json")
                print("source_manifest=init")
                print("init_entities=200")
                print(f"manifest_hash={result.manifest_hash}")
                print(f"source_hash={result.source_hash}")
            print("databases=database/registry.db")
            print("runtime_dirs=data,database,outputs,manifests/generated")
            _print_pass("PASS: init")
        elif args.command == "validate":
            from .project_setup import validate_project

            project_dir = _resolve_project_dir_arg(args)
            result = validate_project(
                project_dir=project_dir,
                context=args.context,
            )
            print(f"context={result.context}")
            print(f"project_root={_display_path(result.project_root)}")
            print(f"runtime_root={_display_path(result.runtime_root)}")
            print(f"validated_manifests={result.manifest_count}")
            print(f"parsed_workflow_files={result.workflow_count}")
            print(f"parsed_step_files={result.step_count}")
            print(f"source_entities={result.source_entities}")
            print(f"published_outputs={result.published_outputs}")
            _print_pass("PASS: validate")
        elif args.command == "gui":
            _run_gui_command(args)
        elif args.command == "trace":
            _run_trace_command(args)
        elif args.command == "registry":
            _run_registry_command(args)
        elif args.command == "specifications":
            return _run_specifications_command(args)
        elif args.command == "workflow":
            return _run_workflow_command(args) or 0
        else:  # pragma: no cover - argparse enforces the command choices.
            parser.error("missing command")
    except Exception as exc:
        from .errors import NipactError
        from .project_setup import ProjectSetupError

        if isinstance(exc, (ProjectSetupError, NipactError)):
            print(f"error: {exc}", file=sys.stderr)
            return 1
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
