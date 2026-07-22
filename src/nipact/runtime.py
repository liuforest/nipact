"""Internal file-dispatch runtime for generated NIPACT jobs."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Callable

from .execution_evidence import (
    RUN_PLAN_SCHEMA_VERSION,
    CompletionReceipt,
    completion_receipt_relative_path,
    validate_invocation_token,
    write_completion_receipt_atomic,
)

JobCallable = Callable[
    [dict[str, tuple[Path, ...]], dict[str, Path], dict[str, Any], str],
    None,
]


@dataclass(frozen=True)
class _PreparedReusedInputAuthority:
    artifact_id: int
    bound_occurrence: Path
    supplied_path: str


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nipact.runtime")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_job_parser = subparsers.add_parser("run-job")
    run_job_parser.add_argument("--run-plan", required=True)
    run_job_parser.add_argument("--job-id", required=True)
    args = parser.parse_args(argv)

    if args.command == "run-job":
        run_job(run_plan_path=Path(args.run_plan), job_id=args.job_id)
        return 0
    raise AssertionError("unreachable")


def run_job(*, run_plan_path: Path, job_id: str) -> None:
    """Run one job from a generated run plan."""
    run_plan = _read_json(run_plan_path)
    if run_plan.get("schema_version") != RUN_PLAN_SCHEMA_VERSION:
        raise RuntimeError("unsupported run-plan schema version")
    try:
        invocation_token = validate_invocation_token(
            run_plan.get("invocation_token")
        )
    except ValueError as exc:
        raise RuntimeError("run plan has no executable invocation token") from exc
    runtime_root = Path(_required_string(run_plan, "runtime_root")).expanduser().resolve()
    run_workspace = run_plan_path.parent.resolve()
    jobs = _required_mapping(run_plan, "jobs")
    job = jobs.get(job_id)
    if not isinstance(job, dict):
        raise RuntimeError(f"unknown runtime job: {job_id}")

    output_paths = _resolve_outputs(job, run_workspace=run_workspace)
    prepared_reused_inputs = _prepared_reused_input_authorities(
        run_plan,
        runtime_root=runtime_root,
        run_workspace=run_workspace,
    )
    input_paths = _resolve_inputs(
        job,
        run_workspace=run_workspace,
        runtime_root=runtime_root,
        prepared_reused_inputs=prepared_reused_inputs,
    )
    callable_obj = _load_callable(_required_string(job, "callable_ref"))
    callable_obj(
        inputs=input_paths,
        outputs=output_paths,
        params=dict(_required_mapping(job, "params")),
        address=_required_string(job, "address"),
    )
    for output_name, output_path in sorted(output_paths.items()):
        if not output_path.is_file():
            raise RuntimeError(
                f"runtime job did not create output {output_name!r}: {output_path}"
            )
    raw_declared_outputs = job.get("declared_outputs")
    if (
        not isinstance(raw_declared_outputs, list)
        or not all(isinstance(output, str) for output in raw_declared_outputs)
        or raw_declared_outputs != sorted(output_paths)
    ):
        raise RuntimeError("job declared_outputs do not match runtime outputs")
    receipt_relative = _required_string(job, "completion_receipt_path")
    if receipt_relative != completion_receipt_relative_path(job_id):
        raise RuntimeError("job completion receipt path is invalid")
    receipt_path = _resolve_relative_under(
        base=run_workspace,
        relative_path=receipt_relative,
        allowed_root=run_workspace / "receipts",
        label="completion receipt",
    )
    receipt = CompletionReceipt(
        invocation_token=invocation_token,
        job_id=job_id,
        request_bundle_digest=_required_string(job, "request_bundle_digest"),
        outputs=tuple(raw_declared_outputs),
    )
    write_completion_receipt_atomic(receipt_path, receipt)


def _resolve_outputs(
    job: dict[str, Any],
    *,
    run_workspace: Path,
) -> dict[str, Path]:
    raw_outputs = _required_mapping(job, "outputs")
    if not raw_outputs:
        raise RuntimeError("job outputs must not be empty")
    outputs: dict[str, Path] = {}
    for raw_name, raw_path in sorted(raw_outputs.items()):
        if not isinstance(raw_name, str) or not raw_name:
            raise RuntimeError("job output names must be non-empty strings")
        if not isinstance(raw_path, str) or not raw_path:
            raise RuntimeError(f"job output {raw_name!r} must be a non-empty path")
        output_path = _resolve_relative_under(
            base=run_workspace,
            relative_path=raw_path,
            allowed_root=run_workspace / "staging",
            label="job output",
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        outputs[raw_name] = output_path
    return outputs


def _resolve_inputs(
    job: dict[str, Any],
    *,
    run_workspace: Path,
    runtime_root: Path,
    prepared_reused_inputs: dict[int, _PreparedReusedInputAuthority],
) -> dict[str, tuple[Path, ...]]:
    raw_inputs = _required_mapping(job, "inputs")
    records = _input_records(job)
    expected_pairs: Counter[tuple[str, str]] = Counter()
    raw_paths_by_binding: dict[str, tuple[str, ...]] = {}
    for raw_name, raw_paths in sorted(raw_inputs.items()):
        if not isinstance(raw_name, str) or not raw_name:
            raise RuntimeError("job input binding names must be non-empty strings")
        if (
            not isinstance(raw_paths, list)
            or not raw_paths
            or not all(isinstance(path, str) and path for path in raw_paths)
        ):
            raise RuntimeError(f"job input {raw_name!r} must be a non-empty path list")
        paths = tuple(raw_paths)
        if len(paths) != len(set(paths)):
            raise RuntimeError(
                f"job input {raw_name!r} contains a duplicate input path"
            )
        raw_paths_by_binding[raw_name] = paths
        expected_pairs.update((raw_name, path) for path in paths)

    actual_pairs: Counter[tuple[str, str]] = Counter()
    records_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        binding_name = _required_string(record, "binding_name")
        input_path = _required_string(record, "input_path")
        pair = (binding_name, input_path)
        if pair in records_by_pair:
            raise RuntimeError("job input_records contain a duplicate input binding")
        actual_pairs[pair] += 1
        records_by_pair[pair] = record
    if actual_pairs != expected_pairs:
        raise RuntimeError("input_records must match job inputs")

    resolved: dict[str, tuple[Path, ...]] = {}
    for binding_name, input_paths in sorted(raw_paths_by_binding.items()):
        resolved[binding_name] = tuple(
            _resolve_input_path(
                records_by_pair[(binding_name, input_path)],
                input_path,
                run_workspace=run_workspace,
                runtime_root=runtime_root,
                prepared_reused_inputs=prepared_reused_inputs,
            )
            for input_path in input_paths
        )
    return resolved


def _resolve_input_path(
    record: dict[str, Any],
    input_path: str,
    *,
    run_workspace: Path,
    runtime_root: Path,
    prepared_reused_inputs: dict[int, _PreparedReusedInputAuthority],
) -> Path:
    origin = _required_string(record, "origin")
    if origin == "source":
        resolved_input = _resolve_relative_under(
            base=run_workspace,
            relative_path=input_path,
            allowed_root=runtime_root / "data",
            label="source input",
        )
        source_artifact_path = _required_string(record, "source_artifact_path")
        resolved_source = _resolve_relative_under(
            base=runtime_root,
            relative_path=source_artifact_path,
            allowed_root=runtime_root / "data",
            label="source artifact path",
        )
        if resolved_input != resolved_source:
            raise RuntimeError("source input path must match source_artifact_path")
        if not resolved_input.is_file():
            raise RuntimeError(f"missing source input: {source_artifact_path}")
        return resolved_input

    if origin == "workflow_output":
        artifact_id = record.get("registry_source_artifact_id")
        if artifact_id is not None:
            if (
                not isinstance(artifact_id, int)
                or isinstance(artifact_id, bool)
                or artifact_id <= 0
            ):
                raise RuntimeError(
                    "reused workflow input artifact ID must be a positive integer"
                )
            try:
                prepared = prepared_reused_inputs[artifact_id]
            except KeyError as exc:
                raise RuntimeError(
                    "reused workflow input has no prepared input authority"
                ) from exc
            if input_path != prepared.supplied_path:
                raise RuntimeError(
                    "reused workflow input path does not match prepared authority"
                )
            return _resolve_prepared_reused_input(
                prepared,
                run_workspace=run_workspace,
            )

        resolved_input = _resolve_relative_under(
            base=run_workspace,
            relative_path=input_path,
            allowed_root=run_workspace / "staging",
            label="workflow input",
        )
        if not resolved_input.is_file():
            raise RuntimeError(f"missing workflow input: {input_path}")
        return resolved_input

    raise RuntimeError(f"unsupported input origin: {origin}")


def _prepared_reused_input_authorities(
    run_plan: dict[str, Any],
    *,
    runtime_root: Path,
    run_workspace: Path,
) -> dict[int, _PreparedReusedInputAuthority]:
    raw_entries = run_plan.get("prepared_reused_inputs")
    if not isinstance(raw_entries, list):
        raise RuntimeError("prepared_reused_inputs must be a list")

    prepared: dict[int, _PreparedReusedInputAuthority] = {}
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict) or set(raw_entry) != {
            "artifact_id",
            "bound_occurrence_path",
            "supplied_path",
        }:
            raise RuntimeError("prepared reused-input fields are invalid")
        artifact_id = raw_entry.get("artifact_id")
        if (
            not isinstance(artifact_id, int)
            or isinstance(artifact_id, bool)
            or artifact_id <= 0
        ):
            raise RuntimeError("prepared reused-input artifact ID is invalid")
        if artifact_id in prepared:
            raise RuntimeError("prepared reused-input artifact IDs must be unique")
        bound_occurrence_path = _required_string(
            raw_entry,
            "bound_occurrence_path",
        )
        bound_occurrence = _resolve_relative_under(
            base=runtime_root,
            relative_path=bound_occurrence_path,
            allowed_root=runtime_root / "outputs" / "v1",
            label="bound reused occurrence",
        )
        supplied_path = _required_string(raw_entry, "supplied_path")
        prepared[artifact_id] = _PreparedReusedInputAuthority(
            artifact_id=artifact_id,
            bound_occurrence=bound_occurrence,
            supplied_path=supplied_path,
        )

    jobs = _required_mapping(run_plan, "jobs")
    referenced_ids: set[int] = set()
    for raw_job in jobs.values():
        if not isinstance(raw_job, dict):
            raise RuntimeError("run plan jobs must be objects")
        for record in _input_records(raw_job):
            if record.get("origin") != "workflow_output":
                continue
            artifact_id = record.get("registry_source_artifact_id")
            if artifact_id is None:
                continue
            if (
                not isinstance(artifact_id, int)
                or isinstance(artifact_id, bool)
                or artifact_id <= 0
            ):
                raise RuntimeError(
                    "reused workflow input artifact ID must be a positive integer"
                )
            referenced_ids.add(artifact_id)
    if referenced_ids != set(prepared):
        raise RuntimeError(
            "prepared reused inputs must exactly cover reused workflow inputs"
        )
    return prepared


def _resolve_prepared_reused_input(
    prepared: _PreparedReusedInputAuthority,
    *,
    run_workspace: Path,
) -> Path:
    if "\\" in prepared.supplied_path:
        raise RuntimeError("reused workflow input must use POSIX path separators")
    relative_path = Path(prepared.supplied_path)
    if relative_path.is_absolute():
        raise RuntimeError("reused workflow input must be a run-plan-relative path")
    lexical_path = run_workspace / relative_path
    if lexical_path.is_symlink():
        raise RuntimeError("reused workflow input must not be a symlink")
    resolved_input = lexical_path.resolve()
    staging_root = (run_workspace / "staging").resolve()
    if resolved_input != prepared.bound_occurrence and not _path_contains_or_same(
        staging_root,
        resolved_input,
    ):
        raise RuntimeError(
            "reused workflow input does not match its bound canonical occurrence"
        )
    if not resolved_input.is_file():
        raise RuntimeError(f"missing reused workflow input: {prepared.supplied_path}")
    return resolved_input


def _resolve_relative_under(
    *,
    base: Path,
    relative_path: str,
    allowed_root: Path,
    label: str,
) -> Path:
    if "\\" in relative_path:
        raise RuntimeError(f"{label} must use POSIX path separators")
    path = Path(relative_path)
    if path.is_absolute():
        raise RuntimeError(f"{label} must be a run-plan-relative path")
    resolved = (base / path).resolve()
    if not _path_contains_or_same(allowed_root.resolve(), resolved):
        raise RuntimeError(f"{label} resolved outside its allowed runtime root")
    return resolved


def _input_records(job: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    records = job.get("input_records")
    if not isinstance(records, list):
        raise RuntimeError("job input_records must be a list")
    parsed: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeError("job input_records entries must be objects")
        parsed.append(record)
    return tuple(parsed)


def _load_callable(callable_ref: str) -> JobCallable:
    module_name, separator, function_name = callable_ref.partition(":")
    if not separator or not module_name or not function_name:
        raise RuntimeError("callable_ref must use module:function format")
    module = import_module(module_name)
    value = getattr(module, function_name, None)
    if not callable(value):
        raise RuntimeError(f"callable_ref does not resolve to a function: {callable_ref}")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON file must contain an object: {path}")
    return payload


def _required_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"{key} must be an object")
    return value


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{key} must be a non-empty string")
    return value


def _path_contains_or_same(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
