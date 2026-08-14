from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest
import yaml

import nipact.context_index as context_index_module
import nipact.execution as ordinary_execution_module
import nipact.project_context as project_context_module
import nipact.registry as registry_module
import nipact.specification_execution as execution_module
from conftest import RegistryV18Fixture
from nipact.cli import build_parser, main
from nipact.errors import ValidationError
from nipact.execution import RunOutcome
from nipact.project_context import ResolvedProjectContext
from nipact.registry import (
    SpecificationAttemptProjection,
    SpecificationAttemptRef,
    SpecificationFailureDiagnostic,
    SpecificationMemberProjection,
    SpecificationResultProjection,
    SpecificationResultSourceBasisProjection,
    SpecificationSnapshotProjections,
)
from nipact.specification_canonical import decode_specification_snapshot
from nipact.specification_execution import (
    SpecificationFreezeResult,
    SpecificationMemberRunResult,
    SpecificationRunResult,
    compile_specification_snapshot,
)
from nipact.specification_loading import (
    ExplicitSpecificationSource,
    RegisteredSpecificationSource,
)
from test_registry_migration import _runtime_inventory
from test_specification_execution import _run_jobs_in_process
from test_specification_registry import (
    _specification_payload,
    configure_specification_project,
    prepare_v19,
)


_DIGEST = "d" * 64
_CANONICAL_GOLDEN = (
    Path(__file__).parent / "fixtures/specification_canonical/core-snapshot.json"
)

def _plain(value: object) -> object:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _plain(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_plain(item) for item in value]
    raise AssertionError(f"unsupported test projection type: {type(value).__name__}")


@pytest.mark.parametrize(
    ("argv", "leaf", "source", "selection", "cores"),
    (
        (
            ["specifications", "preview", "registered", "--context", "analysis"],
            "preview",
            "registered",
            None,
            None,
        ),
        (
            [
                "specifications",
                "freeze",
                "--file",
                "members.yaml",
                "--context",
                "analysis",
            ],
            "freeze",
            Path("members.yaml"),
            None,
            None,
        ),
        (
            [
                "specifications",
                "run",
                _DIGEST,
                "--member",
                "member-a",
                "--context",
                "analysis",
                "--cores",
                "3",
            ],
            "run",
            None,
            "member-a",
            3,
        ),
        (
            ["specifications", "results", _DIGEST, "--context", "analysis"],
            "results",
            None,
            None,
            None,
        ),
    ),
)
def test_specification_parser_accepts_each_public_leaf(
    argv: list[str],
    leaf: str,
    source: str | Path | None,
    selection: str | None,
    cores: int | None,
) -> None:
    args = build_parser().parse_args(argv)

    assert args.command == "specifications"
    assert args.specifications_command == leaf
    assert args.context == "analysis"
    if leaf in {"preview", "freeze"}:
        actual_source = args.registered_specification_key or args.specification_file
        assert actual_source == source
    if leaf == "run":
        assert args.member == selection
        assert args.all_members is False
        assert args.cores == cores


@pytest.mark.parametrize(
    "argv",
    (
        ["specifications", "preview", "--context", "analysis"],
        [
            "specifications",
            "preview",
            "registered",
            "--file",
            "members.yaml",
            "--context",
            "analysis",
        ],
        ["specifications", "freeze", "--context", "analysis"],
        [
            "specifications",
            "freeze",
            "registered",
            "--file",
            "members.yaml",
            "--context",
            "analysis",
        ],
        ["specifications", "run", _DIGEST, "--context", "analysis"],
        [
            "specifications",
            "run",
            _DIGEST,
            "--member",
            "member-a",
            "--all",
            "--context",
            "analysis",
        ],
        [
            "specifications",
            "run",
            _DIGEST,
            "--all",
            "--context",
            "analysis",
            "--cores",
            "0",
        ],
    ),
)
def test_specification_parser_rejects_invalid_source_selection_and_cores(
    argv: list[str],
) -> None:
    with pytest.raises(SystemExit) as caught:
        build_parser().parse_args(argv)

    assert caught.value.code == 2


def test_specification_help_advertises_only_the_bounded_v1_group(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as caught:
        main(["specifications", "--help"])

    assert caught.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    for leaf in ("preview", "freeze", "run", "results"):
        assert leaf in help_text
    for deferred in ("status", "retry", "resume", "validate", "list"):
        assert deferred not in help_text


@pytest.mark.parametrize(
    ("leaf", "selector", "expected_source"),
    (
        ("preview", ["registered"], RegisteredSpecificationSource("registered")),
        (
            "freeze",
            ["--file", "explicit.yaml"],
            ExplicitSpecificationSource(Path("explicit.yaml")),
        ),
    ),
)
def test_preview_and_freeze_route_one_selected_source_to_one_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    leaf: str,
    selector: list[str],
    expected_source: object,
) -> None:
    snapshot = decode_specification_snapshot(_CANONICAL_GOLDEN.read_bytes())
    project_dir = tmp_path / "resolved-project"
    calls: list[tuple[str, Path, str, object]] = []

    def resolve_project_dir(*, project_dir: Path | None, context: str) -> Path:
        assert project_dir == Path("selected-project")
        assert context == "canonical"
        return tmp_path / "resolved-project"

    def compile_route(
        *, project_dir: Path, context: str, source: object
    ) -> object:
        calls.append(("preview", project_dir, context, source))
        return SimpleNamespace(
            set_key="golden-set",
            equal_effective_member_groups=(("member-a", "member-b"),),
            snapshot=snapshot,
        )

    def freeze_route(
        *, project_dir: Path, context: str, source: object
    ) -> SpecificationFreezeResult:
        calls.append(("freeze", project_dir, context, source))
        return SpecificationFreezeResult(snapshot=snapshot, inserted=True)

    monkeypatch.setattr(context_index_module, "resolve_project_dir", resolve_project_dir)
    monkeypatch.setattr(execution_module, "compile_specification_snapshot", compile_route)
    monkeypatch.setattr(execution_module, "freeze_specification_snapshot", freeze_route)
    monkeypatch.chdir(tmp_path)

    assert main(
        [
            "specifications",
            leaf,
            *selector,
            "--context",
            "canonical",
            "--project-dir",
            "selected-project",
        ]
    ) == 0

    assert calls == [(leaf, project_dir, "canonical", expected_source)]
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    if leaf == "preview":
        assert set(payload) == {
            "schema",
            "context",
            "specification_set",
            "snapshot_digest",
            "persistence_performed",
            "source_policy",
            "counts",
            "equal_effective_member_groups",
            "manifest_values",
            "members",
        }
        assert payload["schema"] == "nipact/specification-preview/v1"
        assert payload["specification_set"] == "golden-set"
    else:
        expected_payload = {
            "schema": "nipact/specification-freeze/v1",
            "context": "canonical",
            "snapshot_digest": snapshot.snapshot_digest,
            "status": "inserted",
            "counts": {"candidates": 2, "included": 1, "excluded": 1},
        }
        assert set(payload) == {
            "schema",
            "context",
            "snapshot_digest",
            "status",
            "counts",
        }
        assert captured.out == (
            json.dumps(expected_payload, indent=2, sort_keys=True) + "\n"
        )
        assert payload == expected_payload


def test_preview_on_static_v18_is_complete_and_runtime_non_mutating(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = registry_v18_fixture
    configure_specification_project(fixture)
    expected = compile_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("compact"),
    )
    before = _runtime_inventory(fixture.runtime_dir)
    scientific_sources = {
        fixture.runtime_dir / "data/source/entity_001.txt",
        fixture.runtime_dir / "data/source/entity_002.txt",
    }
    original_open = Path.open

    def forbidden(*_args: object, **_kwargs: object) -> object:
        pytest.fail("preview reached a registry or mutation boundary")

    def guarded_open(path: Path, *args: object, **kwargs: object) -> object:
        if path in scientific_sources:
            pytest.fail("preview read scientific source bytes")
        return original_open(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", forbidden)
        patch.setattr(execution_module, "resolve_project_context", forbidden)
        patch.setattr(execution_module, "acquire_mutating_runtime_lock", forbidden)
        patch.setattr(Path, "open", guarded_open)
        assert main(
            [
                "specifications",
                "preview",
                "compact",
                "--project-dir",
                str(fixture.project_dir),
                "--context",
                fixture.context,
            ]
        ) == 0

    captured = capsys.readouterr()
    assert captured.err == ""
    expected_payload = {
        "schema": "nipact/specification-preview/v1",
        "context": fixture.context,
        "specification_set": "compact-freeze",
        "snapshot_digest": expected.snapshot.snapshot_digest,
        "persistence_performed": False,
        "source_policy": "reconcile_at_attempt",
        "counts": {"candidates": 2, "included": 1, "excluded": 1},
        "equal_effective_member_groups": [],
        "manifest_values": [
            {
                "value_schema": value.value_schema,
                "manifest_digest": value.manifest_digest,
                "entity_count": value.entity_count,
            }
            for value in expected.snapshot.manifest_values
        ],
        "members": [
            {
                "member_key": member.member_key,
                "disposition": member.disposition,
                "exclusion_reason": member.exclusion_reason,
                "row_digest": member.row.row_digest,
                "row": json.loads(member.row.canonical_bytes),
                "expected_results": _plain(member.expected_results),
            }
            for member in expected.snapshot.members
        ],
    }
    assert captured.out == (
        json.dumps(expected_payload, indent=2, sort_keys=True) + "\n"
    )
    payload = json.loads(captured.out)
    assert set(payload) == {
        "schema",
        "context",
        "specification_set",
        "snapshot_digest",
        "persistence_performed",
        "source_policy",
        "counts",
        "equal_effective_member_groups",
        "manifest_values",
        "members",
    }
    assert payload == expected_payload
    assert _runtime_inventory(fixture.runtime_dir) == before
    assert not (fixture.runtime_dir / ".nipact-mutating.lock").exists()


def test_valid_v18_freeze_requires_migration_without_any_runtime_change(
    registry_v18_fixture: RegistryV18Fixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = registry_v18_fixture
    configure_specification_project(fixture)
    before = _runtime_inventory(fixture.runtime_dir)

    assert main(
        [
            "specifications",
            "freeze",
            "compact",
            "--project-dir",
            str(fixture.project_dir),
            "--context",
            fixture.context,
        ]
    ) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "error: registry.db schema version 18 requires explicit migration; run "
        "'nipact registry migrate --context CONTEXT --project-dir PROJECT_DIR'\n"
    )
    assert _runtime_inventory(fixture.runtime_dir) == before
    assert not (fixture.runtime_dir / ".nipact-mutating.lock").exists()


def _member_run_result(
    *, member_key: str, attempt_id: int, outcome: str
) -> SpecificationMemberRunResult:
    return SpecificationMemberRunResult(
        attempt=SpecificationAttemptRef(
            attempt_id=attempt_id,
            context="analysis",
            snapshot_digest=_DIGEST,
            member_key=member_key,
        ),
        outcome=outcome,  # type: ignore[arg-type]
        ordinary_outcome=RunOutcome(
            published_count=2,
            published_bytes=120,
            selected_generated_count=1,
            selected_reused_count=1,
            failed_jobs=(
                ()
                if outcome == "complete"
                else (("model", "cohort", f"{outcome} result"),)
            ),
            cleanup_warnings=("retained diagnostic",) if outcome == "failed" else (),
            all_selected_resolved=outcome == "complete",
        ),
    )


@pytest.mark.parametrize(
    ("mode", "outcomes", "expected_exit"),
    (
        ("member", ("complete",), 0),
        ("member", ("partial",), 1),
        ("member", ("failed",), 1),
        ("all", ("complete", "partial"), 1),
        ("all", (), 0),
    ),
)
def test_run_projects_returned_members_and_exit_status_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: str,
    outcomes: tuple[str, ...],
    expected_exit: int,
) -> None:
    project_dir = tmp_path / "project"
    members = tuple(
        _member_run_result(
            member_key=f"member-{index + 1}",
            attempt_id=40 + index,
            outcome=outcome,
        )
        for index, outcome in enumerate(outcomes)
    )
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        context_index_module,
        "resolve_project_dir",
        lambda **_kwargs: project_dir,
    )

    def run_member(**kwargs: object) -> SpecificationMemberRunResult:
        calls.append(("member", kwargs))
        assert len(members) == 1
        return members[0]

    def run_all(**kwargs: object) -> SpecificationRunResult:
        calls.append(("all", kwargs))
        return SpecificationRunResult(snapshot_digest=_DIGEST, members=members)

    monkeypatch.setattr(execution_module, "run_specification_member", run_member)
    monkeypatch.setattr(execution_module, "run_all_specification_members", run_all)
    selection = ["--member", "member-1"] if mode == "member" else ["--all"]

    assert main(
        [
            "specifications",
            "run",
            _DIGEST,
            *selection,
            "--context",
            "analysis",
            "--project-dir",
            str(project_dir),
            "--cores",
            "2",
        ]
    ) == expected_exit

    assert [name for name, _kwargs in calls] == [mode]
    assert calls[0][1] == {
        "project_dir": project_dir,
        "context": "analysis",
        "snapshot_digest": _DIGEST,
        **({"member_key": "member-1"} if mode == "member" else {}),
        "cores": 2,
    }
    expected_payload = {
        "schema": "nipact/specification-run/v1",
        "context": "analysis",
        "snapshot_digest": _DIGEST,
        "selection": {
            "mode": mode,
            "member_key": "member-1" if mode == "member" else None,
        },
        "cores": 2,
        "reached_members": [
            {
                "member_key": member.attempt.member_key,
                "attempt_id": member.attempt.attempt_id,
                "outcome": member.outcome,
                "ordinary_outcome": {
                    "published_outputs": 2,
                    "published_bytes": 120,
                    "selected_fresh_outputs": 1,
                    "selected_reused_outputs": 1,
                    "all_selected_resolved": member.outcome == "complete",
                    "failed_jobs": (
                        []
                        if member.outcome == "complete"
                        else [
                            {
                                "step_name": "model",
                                "address": "cohort",
                                "reason": f"{member.outcome} result",
                            }
                        ]
                    ),
                    "cleanup_warnings": (
                        ["retained diagnostic"]
                        if member.outcome == "failed"
                        else []
                    ),
                },
            }
            for member in members
        ],
    }
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == (
        json.dumps(expected_payload, indent=2, sort_keys=True) + "\n"
    )
    payload = json.loads(captured.out)
    assert set(payload) == {
        "schema",
        "context",
        "snapshot_digest",
        "selection",
        "cores",
        "reached_members",
    }
    assert payload == expected_payload


def test_raised_run_failure_uses_error_path_without_partial_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        context_index_module,
        "resolve_project_dir",
        lambda **_kwargs: tmp_path / "project",
    )
    monkeypatch.setattr(
        execution_module,
        "run_specification_member",
        lambda **_kwargs: (_ for _ in ()).throw(ValidationError("member failed")),
    )

    assert main(
        [
            "specifications",
            "run",
            _DIGEST,
            "--member",
            "member-1",
            "--context",
            "analysis",
        ]
    ) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error: member failed\n"


def _member_projection(member: object, *, member_key: str | None = None) -> object:
    return SpecificationMemberProjection(
        member_key=member.member_key if member_key is None else member_key,
        row_digest=member.row.row_digest,
        disposition=member.disposition,
        exclusion_reason=member.exclusion_reason,
        workflow_selector=member.row.workflow_selector,
        decision_coordinates=member.row.decision_coordinates,
        effective_declaration=member.row.effective_declaration,
        expected_results=member.expected_results,
    )


def _result_projection(
    *,
    attempt_id: int,
    member_key: str,
    descriptor: object,
    artifact_id: int | None,
    producing_run_id: int | None = None,
) -> SpecificationResultProjection:
    present = artifact_id is not None
    return SpecificationResultProjection(
        attempt_id=attempt_id,
        member_key=member_key,
        role=descriptor.role,
        step_name=descriptor.step_name,
        output_name=descriptor.output_name,
        address=descriptor.address,
        artifact_id=artifact_id,
        producing_run_id=producing_run_id if present else None,
        producing_workflow_name="producer" if present else None,
        artifact_path=(f"artifacts/{artifact_id}.json" if present else None),
        content_digest=(f"{artifact_id:064x}" if present else None),
        output_hash=(f"output-{artifact_id}" if present else None),
        file_size=artifact_id if present else None,
        extension=".json" if present else None,
        request_bundle_digest=(f"{artifact_id + 1:064x}" if present else None),
        current_publication_path=(
            f"outputs/{descriptor.role}.json" if present else None
        ),
    )


def test_results_serializes_the_complete_typed_projection_without_inference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = decode_specification_snapshot(_CANONICAL_GOLDEN.read_bytes())
    included = snapshot.members[0]
    excluded = snapshot.members[1]
    members = (
        _member_projection(included),
        _member_projection(included, member_key="member-c-never-attempted"),
        _member_projection(excluded),
    )
    attempts = (
        SpecificationAttemptProjection(10, "member-a", "t10", None, None, None, None),
        SpecificationAttemptProjection(
            11,
            "member-a",
            "t11",
            "t11-end",
            "failed",
            None,
            SpecificationFailureDiagnostic("planning", "invalid member plan"),
        ),
        SpecificationAttemptProjection(
            12, "member-a", "t12", "t12-end", "partial", 32, None
        ),
        SpecificationAttemptProjection(
            13, "member-a", "t13", "t13-end", "complete", 33, None
        ),
        SpecificationAttemptProjection(
            14, "member-a", "t14", "t14-end", "complete", 34, None
        ),
    )
    first, second = included.expected_results
    results = (
        _result_projection(
            attempt_id=10,
            member_key="member-a",
            descriptor=first,
            artifact_id=None,
        ),
        _result_projection(
            attempt_id=11,
            member_key="member-a",
            descriptor=first,
            artifact_id=None,
        ),
        _result_projection(
            attempt_id=12,
            member_key="member-a",
            descriptor=first,
            artifact_id=301,
            producing_run_id=22,
        ),
        _result_projection(
            attempt_id=12,
            member_key="member-a",
            descriptor=second,
            artifact_id=None,
        ),
        _result_projection(
            attempt_id=13,
            member_key="member-a",
            descriptor=first,
            artifact_id=401,
            producing_run_id=23,
        ),
        _result_projection(
            attempt_id=13,
            member_key="member-a",
            descriptor=second,
            artifact_id=402,
            producing_run_id=23,
        ),
        _result_projection(
            attempt_id=14,
            member_key="member-a",
            descriptor=first,
            artifact_id=401,
            producing_run_id=23,
        ),
        _result_projection(
            attempt_id=14,
            member_key="member-a",
            descriptor=second,
            artifact_id=402,
            producing_run_id=23,
        ),
    )
    source_basis = (
        SpecificationResultSourceBasisProjection(
            attempt_id=13,
            member_key="member-a",
            role=first.role,
            address=first.address,
            result_artifact_id=401,
            source_scope="entity",
            source_name="seed",
            source_entity_id="entity_001",
            source_occurrence_path="data/source/entity_001.json",
            source_content_digest="a" * 64,
            source_file_size=17,
            source_extension=".json",
        ),
    )
    aggregate = SpecificationSnapshotProjections(
        snapshot_digest=snapshot.snapshot_digest,
        context="canonical",
        members=members,
        attempts=attempts,
        results=results,
        result_source_basis=source_basis,
    )
    resolved = ResolvedProjectContext(
        project_root=tmp_path / "project",
        runtime_root=tmp_path / "runtime",
        registry_path=tmp_path / "runtime/database/registry.db",
        context="canonical",
    )
    reads: list[tuple[Path, str, str]] = []

    monkeypatch.setattr(
        context_index_module,
        "resolve_project_dir",
        lambda **_kwargs: resolved.project_root,
    )
    monkeypatch.setattr(
        project_context_module,
        "resolve_project_context",
        lambda **_kwargs: resolved,
    )

    def read_projection(
        path: Path, *, context: str, snapshot_digest: str
    ) -> SpecificationSnapshotProjections:
        reads.append((path, context, snapshot_digest))
        return aggregate

    monkeypatch.setattr(
        registry_module,
        "read_specification_snapshot_projections",
        read_projection,
    )

    assert main(
        [
            "specifications",
            "results",
            snapshot.snapshot_digest,
            "--context",
            "canonical",
        ]
    ) == 0

    assert reads == [
        (resolved.registry_path, "canonical", snapshot.snapshot_digest)
    ]
    expected_payload = {
        "schema": "nipact/specification-results/v1",
        "context": "canonical",
        "snapshot_digest": snapshot.snapshot_digest,
        "members": _plain(members),
        "attempts": _plain(attempts),
        "results": _plain(results),
        "result_source_basis": _plain(source_basis),
    }
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == (
        json.dumps(expected_payload, indent=2, sort_keys=True) + "\n"
    )
    payload = json.loads(captured.out)
    assert set(payload) == {
        "schema",
        "context",
        "snapshot_digest",
        "members",
        "attempts",
        "results",
        "result_source_basis",
    }
    assert all(
        set(member) == {
            "member_key",
            "row_digest",
            "disposition",
            "exclusion_reason",
            "workflow_selector",
            "decision_coordinates",
            "effective_declaration",
            "expected_results",
        }
        for member in payload["members"]
    )
    assert all(
        set(attempt) == {
            "attempt_id",
            "member_key",
            "started_at",
            "finished_at",
            "outcome",
            "selecting_run_id",
            "failure",
        }
        for attempt in payload["attempts"]
    )
    assert any(attempt["failure"] is not None for attempt in payload["attempts"])
    assert all(
        failure is None or set(failure) == {"stage", "summary"}
        for failure in (attempt["failure"] for attempt in payload["attempts"])
    )
    assert all(
        set(result) == {
            "attempt_id",
            "member_key",
            "role",
            "step_name",
            "output_name",
            "address",
            "artifact_id",
            "producing_run_id",
            "producing_workflow_name",
            "artifact_path",
            "content_digest",
            "output_hash",
            "file_size",
            "extension",
            "request_bundle_digest",
            "current_publication_path",
        }
        for result in payload["results"]
    )
    assert all(
        set(basis) == {
            "attempt_id",
            "member_key",
            "role",
            "address",
            "result_artifact_id",
            "source_scope",
            "source_name",
            "source_entity_id",
            "source_occurrence_path",
            "source_content_digest",
            "source_file_size",
            "source_extension",
        }
        for basis in payload["result_source_basis"]
    )
    assert payload == expected_payload
    assert payload["attempts"][0]["outcome"] is None
    assert payload["attempts"][1]["selecting_run_id"] is None
    assert payload["results"][0]["artifact_id"] is None
    assert payload["results"][3]["current_publication_path"] is None
    assert payload["attempts"][3]["selecting_run_id"] == 33
    assert payload["results"][4]["producing_run_id"] == 23
    assert payload["results"][4]["artifact_id"] == payload["results"][6]["artifact_id"]
    assert payload["members"][0]["effective_declaration"]["steps"][1]["params"] == {
        "alpha": 0.0,
        "metadata": {
            "enabled": True,
            "label": "café",
            "nothing": None,
            "values": [1, 2.5],
        },
    }


def test_compact_cli_preview_freeze_run_member_results_smoke(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    payload = _specification_payload()
    fixed = payload["fixed"]
    assert isinstance(fixed, dict)
    fixed["manifest_bindings"] = []
    fixed["target"] = {"step": "fixture_transform", "output": "left"}
    fixed["results"] = {
        "left": {"step": "fixture_transform", "output": "left"},
        "right": {"step": "fixture_transform", "output": "right"},
    }
    (fixture.project_dir / "specifications/compact.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    expected = compile_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("compact"),
    ).snapshot
    base = ["--project-dir", str(fixture.project_dir), "--context", fixture.context]

    assert main(["specifications", "preview", "compact", *base]) == 0
    preview = json.loads(capsys.readouterr().out)
    included = next(
        member
        for member in preview["members"]
        if member["disposition"] == "included"
    )
    assert preview["snapshot_digest"] == expected.snapshot_digest
    assert preview["persistence_performed"] is False

    assert main(["specifications", "freeze", "compact", *base]) == 0
    frozen = json.loads(capsys.readouterr().out)
    assert frozen == {
        "schema": "nipact/specification-freeze/v1",
        "context": fixture.context,
        "snapshot_digest": preview["snapshot_digest"],
        "status": "inserted",
        "counts": preview["counts"],
    }

    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        _run_jobs_in_process,
    )
    assert main(
        [
            "specifications",
            "run",
            frozen["snapshot_digest"],
            "--member",
            included["member_key"],
            *base,
        ]
    ) == 0
    run = json.loads(capsys.readouterr().out)
    reached = run["reached_members"]
    assert run["snapshot_digest"] == frozen["snapshot_digest"]
    assert run["selection"] == {
        "mode": "member",
        "member_key": included["member_key"],
    }
    assert len(reached) == 1
    assert reached[0]["outcome"] == "complete"
    assert reached[0]["ordinary_outcome"]["selected_fresh_outputs"] == 2
    assert reached[0]["ordinary_outcome"]["selected_reused_outputs"] == 0
    assert reached[0]["ordinary_outcome"]["all_selected_resolved"] is True

    assert main(
        ["specifications", "results", frozen["snapshot_digest"], *base]
    ) == 0
    results = json.loads(capsys.readouterr().out)
    assert results["snapshot_digest"] == run["snapshot_digest"]
    assert [attempt["attempt_id"] for attempt in results["attempts"]] == [
        reached[0]["attempt_id"]
    ]
    assert results["attempts"][0]["outcome"] == "complete"
    selected_results = [
        result
        for result in results["results"]
        if result["attempt_id"] == reached[0]["attempt_id"]
    ]
    assert {(result["role"], result["address"]) for result in selected_results} == {
        ("left", "entity_001"),
        ("left", "entity_002"),
        ("right", "entity_001"),
        ("right", "entity_002"),
    }
    assert all(result["artifact_id"] is not None for result in selected_results)
    assert results["result_source_basis"]
    assert all(
        basis["attempt_id"] == reached[0]["attempt_id"]
        for basis in results["result_source_basis"]
    )
