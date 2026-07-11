"""Command-line interface for NIPACT."""

from __future__ import annotations

import argparse
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
        help="run a selected workflow step",
        description="Run a selected workflow step through Snakemake.",
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
        help="Ask Snakemake to build the DAG without running jobs or publishing outputs.",
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


def _run_workflow_command(args: argparse.Namespace) -> int | None:
    from .errors import ValidationError

    project_dir = _resolve_project_dir_arg(args)
    if args.workflow_command == "run":
        from .cli_feedback import CliFeedback
        from .execution import build_run_plan, execute_run_plan
        from .project_setup import validate_project

        validate_project(project_dir=project_dir, context=args.context)
        run_plan = build_run_plan(
            project_dir=project_dir,
            context=args.context,
            workflow_name=args.workflow,
            step_name=args.step,
            address=args.address,
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
        feedback.key_value("dry_run", args.dry_run)
        feedback.key_value("selected_outputs", len(run_plan.selected_output_refs))
        # planned_jobs counts compiled fresh jobs population-wide even for a
        # targeted run; only the reuse counters below are closure-scoped.
        feedback.key_value("planned_jobs", len(run_plan.jobs))
        feedback.key_value(
            "planned_reused_registered_artifacts",
            len(
                {
                    output_ref.source_artifact_id
                    for output_ref in run_plan.reused_outputs
                }
            ),
        )
        feedback.key_value("planned_hydrated_inputs", len(run_plan.reused_outputs))
        feedback.key_value(
            "existing_staged_outputs",
            sum(
                1
                for output_ref in run_plan.selected_output_refs
                if output_ref.staging_path.is_file()
            ),
        )
        feedback.key_value("run_workspace", _display_path(run_plan.run_workspace))
        feedback.key_value(
            "snakemake_log",
            _display_path(run_plan.run_workspace / "logs" / "snakemake.log"),
        )
        feedback.key_value(
            "note",
            "Registered upstream artifacts can be hydrated into the current run "
            "when their identity and digest checks pass.",
        )
        feedback.line()
        feedback.flush()

        active_spinner = None

        def status_callback(event: str) -> None:
            nonlocal active_spinner
            if event == "building_workspace":
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
                dry_run=args.dry_run,
                status_callback=status_callback,
            )
        finally:
            if active_spinner is not None:
                active_spinner.stop()
        elapsed_seconds = perf_counter() - started_at

        feedback.line()
        if args.dry_run:
            feedback.key_value("outputs_published", False)
            feedback.key_value("registry", "not_updated")
        else:
            feedback.key_value("published_outputs", outcome.published_count)
            feedback.key_value("registry", "updated")
            for step_name, address, reason in outcome.failed_jobs:
                feedback.key_value("failed_job", f"{step_name} {address} ({reason})")
        feedback.key_value("elapsed_seconds", f"{elapsed_seconds:.3f}")
        if outcome.all_selected_published:
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
        for binding in plan.manifest_bindings:
            print(
                f"manifest_binding step={binding.step_name} role={binding.role} "
                f"manifest={binding.manifest_name} "
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
