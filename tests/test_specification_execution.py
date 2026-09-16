from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
import sqlite3
import traceback
from typing import Iterator

import pytest
import yaml

import nipact.execution as ordinary_execution_module
import nipact.runtime_lock as runtime_lock_module
import nipact.specification_execution as execution_module
from conftest import RegistryV18Fixture
from nipact.errors import ValidationError
from nipact.execution import RunOutcome, build_run_plan, execute_run_plan
from nipact.hashing import sha256_file_digest
from nipact.registry import (
    SpecificationAttemptRef,
    insert_or_verify_specification_snapshot,
    read_specification_attempt_outcome,
    read_specification_snapshot_projections,
)
from nipact.runtime import run_job
from nipact.specification_canonical import canonicalize_specification_snapshot
from nipact.specification_compiler import compile_specification
from nipact.specification_execution import (
    CompiledSpecificationSnapshot,
    SpecificationMemberRunResult,
    compile_specification_snapshot,
    freeze_specification_snapshot,
    run_all_specification_members,
    run_specification_member,
)
from nipact.specification_loading import (
    ExplicitSpecificationSource,
    RegisteredSpecificationSource,
)
from nipact.workflow import load_workflow_project
from test_specification_adapter import _write_project as _write_adapter_project
from test_specification_registry import (
    _attempt_row,
    _attempt_results,
    _freeze_state,
    _persist_snapshot,
    _ordinary_state,
    _specification_payload,
    _table_rows,
    build_entity_snapshot,
    build_snapshot,
    configure_specification_project,
    prepare_v19,
)


def test_compile_specification_snapshot_reports_equal_effective_members(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    specification_path = configure_specification_project(fixture)
    payload = _specification_payload()
    payload.pop("dimensions")
    payload.pop("combine")
    payload.pop("exclude")
    payload["members"] = [
        {
            "key": "member-z",
            "decision_coordinates": {"analysis_label": "z"},
            "disposition": "included",
            "set": [
                {
                    "step": "fixture_transform",
                    "parameter": "variant",
                    "value": "base",
                }
            ],
        },
        {
            "key": "member-a",
            "decision_coordinates": {"analysis_label": "a"},
            "disposition": "included",
        },
    ]
    payload["expected_counts"] = {
        "candidates": 2,
        "included": 2,
        "excluded": 0,
    }
    specification_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    assert compile_specification(payload, {}).equivalent_member_groups == ()

    compiled = compile_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("compact"),
    )

    assert compiled.loaded_project.project_root == fixture.project_dir
    assert compiled.loaded_project.runtime_root == fixture.runtime_dir
    assert compiled.set_key == "compact-freeze"
    assert tuple(member.member_key for member in compiled.snapshot.members) == (
        "member-a",
        "member-z",
    )
    assert compiled.equal_effective_member_groups == (("member-a", "member-z"),)


def test_freeze_uses_one_compilation_and_exact_preflight_order(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    compiled = compile_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("compact"),
    )
    resolved = execution_module.resolve_project_context
    resolved_context = resolved(
        project_dir=fixture.project_dir,
        context=fixture.context,
    )
    events: list[str] = []

    def compile_once(**kwargs: object) -> CompiledSpecificationSnapshot:
        events.append("compile")
        assert kwargs == {
            "project_dir": fixture.project_dir,
            "context": fixture.context,
            "source": RegisteredSpecificationSource("compact"),
        }
        return compiled

    def resolve_once(**kwargs: object) -> object:
        events.append("resolve")
        assert kwargs == {
            "project_dir": fixture.project_dir,
            "context": fixture.context,
        }
        return resolved_context

    @contextmanager
    def tracked_lock(runtime_root: Path) -> Iterator[None]:
        events.append("lock-enter")
        assert runtime_root == fixture.runtime_dir
        yield
        events.append("lock-exit")

    def persist_exact(
        registry_path: Path,
        *,
        runtime_root: Path,
        snapshot: object,
    ) -> bool:
        events.append("persist")
        assert registry_path == fixture.registry_path
        assert runtime_root == fixture.runtime_dir
        assert snapshot is compiled.snapshot
        return True

    monkeypatch.setattr(execution_module, "compile_specification_snapshot", compile_once)
    monkeypatch.setattr(execution_module, "resolve_project_context", resolve_once)
    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", tracked_lock)
    monkeypatch.setattr(
        execution_module,
        "insert_or_verify_specification_snapshot",
        persist_exact,
    )

    result = freeze_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("compact"),
    )

    assert result.snapshot is compiled.snapshot
    assert result.inserted is True
    assert events == ["compile", "resolve", "lock-enter", "persist", "lock-exit"]


@pytest.mark.parametrize("route", ("fresh", "migrated"))
def test_registered_and_explicit_freeze_share_identity_and_exact_replay(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
) -> None:
    fixture = registry_v18_fixture
    expected = prepare_v19(fixture, route=route)
    specification_path = fixture.project_dir / "specifications/compact.yaml"
    scientific_source = fixture.runtime_dir / "data/source/entity_002.txt"
    before_ordinary = _ordinary_state(fixture.registry_path)
    original_open = Path.open
    original_lock = execution_module.acquire_mutating_runtime_lock
    original_insert = execution_module.insert_or_verify_specification_snapshot
    lock_depth = 0
    lock_calls = 0
    insert_calls = 0

    def guarded_open(
        path: Path,
        *args: object,
        **kwargs: object,
    ) -> object:
        if path == scientific_source:
            pytest.fail("freeze read scientific source content")
        return original_open(path, *args, **kwargs)

    @contextmanager
    def tracked_lock(runtime_root: Path) -> Iterator[None]:
        nonlocal lock_calls, lock_depth
        lock_calls += 1
        assert lock_depth == 0
        with original_lock(runtime_root):
            lock_depth += 1
            try:
                yield
            finally:
                lock_depth -= 1

    def forbidden_nested_lock(_runtime_root: Path) -> object:
        pytest.fail("registry persistence acquired a nested runtime lock")

    def checked_insert(*args: object, **kwargs: object) -> bool:
        nonlocal insert_calls
        insert_calls += 1
        assert lock_depth == 1
        if insert_calls == 2:
            kwargs["_fault_hook"] = lambda checkpoint: pytest.fail(
                f"exact replay entered insertion checkpoint {checkpoint}"
            )
        return original_insert(*args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", tracked_lock)
    monkeypatch.setattr(
        runtime_lock_module,
        "acquire_mutating_runtime_lock",
        forbidden_nested_lock,
    )
    monkeypatch.setattr(
        execution_module,
        "insert_or_verify_specification_snapshot",
        checked_insert,
    )

    first = freeze_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("compact"),
    )
    after_first = _table_rows(fixture.registry_path)
    original_document_bytes = specification_path.read_bytes()
    specification_path.write_bytes(
        original_document_bytes + b"\n# Equivalent explicit transport bytes.\n"
    )
    assert specification_path.read_bytes() != original_document_bytes
    second = freeze_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=ExplicitSpecificationSource(specification_path),
    )

    assert first.snapshot == expected
    assert second.snapshot == expected
    assert first.snapshot.snapshot_digest == second.snapshot.snapshot_digest
    assert first.inserted is True
    assert second.inserted is False
    assert lock_calls == 2
    assert insert_calls == 2
    assert lock_depth == 0
    assert _table_rows(fixture.registry_path) == after_first
    assert _ordinary_state(fixture.registry_path) == before_ordinary
    assert b"specifications/compact.yaml" not in first.snapshot.canonical_bytes
    assert str(specification_path).encode() not in first.snapshot.canonical_bytes


def test_compiler_failure_precedes_lock_and_registry_mutation(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    specification_path = configure_specification_project(fixture)
    invalid = _specification_payload()
    invalid["expected_counts"] = {
        "candidates": 3,
        "included": 2,
        "excluded": 1,
    }
    specification_path.write_text(
        yaml.safe_dump(invalid, sort_keys=False),
        encoding="utf-8",
    )
    before = _table_rows(fixture.registry_path)
    lock_path = fixture.runtime_dir / ".nipact-mutating.lock"
    assert not lock_path.exists()

    def forbidden(*_args: object, **_kwargs: object) -> object:
        pytest.fail("invalid specification reached the mutation boundary")

    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", forbidden)
    monkeypatch.setattr(execution_module, "resolve_project_context", forbidden)
    monkeypatch.setattr(
        execution_module,
        "insert_or_verify_specification_snapshot",
        forbidden,
    )

    with pytest.raises(ValidationError, match="expected_counts"):
        freeze_specification_snapshot(
            project_dir=fixture.project_dir,
            context=fixture.context,
            source=RegisteredSpecificationSource("compact"),
        )

    assert _table_rows(fixture.registry_path) == before
    assert not lock_path.exists()


def _run_jobs_in_process(
    executable_plan: object,
    *,
    cores: int,
    dry_run: bool,
) -> int:
    assert cores > 0
    assert dry_run is False
    run_plan_path = executable_plan.run_workspace / "run_plan.json"
    for job in executable_plan.jobs:
        run_job(run_plan_path=run_plan_path, job_id=job.job_id)
    return 0


def test_specification_member_fresh_complete_records_exact_results(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)
    original_lock = execution_module.acquire_mutating_runtime_lock
    original_read = execution_module.read_specification_snapshot
    original_load = execution_module.load_workflow_project
    lock_calls = 0
    read_calls = 0
    load_calls = 0
    seen_cores: list[int] = []

    @contextmanager
    def tracked_lock(runtime_root: Path) -> Iterator[None]:
        nonlocal lock_calls
        lock_calls += 1
        with original_lock(runtime_root):
            yield

    def tracked_read(*args: object, **kwargs: object) -> object:
        nonlocal read_calls
        read_calls += 1
        return original_read(*args, **kwargs)

    def tracked_load(*args: object, **kwargs: object) -> object:
        nonlocal load_calls
        load_calls += 1
        return original_load(*args, **kwargs)

    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", tracked_lock)
    monkeypatch.setattr(execution_module, "read_specification_snapshot", tracked_read)
    monkeypatch.setattr(execution_module, "load_workflow_project", tracked_load)

    def run_jobs(
        executable_plan: object,
        *,
        cores: int,
        dry_run: bool,
    ) -> int:
        seen_cores.append(cores)
        return _run_jobs_in_process(
            executable_plan,
            cores=cores,
            dry_run=dry_run,
        )

    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        run_jobs,
    )
    monkeypatch.setattr(
        ordinary_execution_module,
        "execute_run_plan",
        lambda *_args, **_kwargs: pytest.fail("runner used the public singular wrapper"),
    )
    monkeypatch.setattr(
        execution_module,
        "freeze_specification_snapshot",
        lambda **_kwargs: pytest.fail("runner used the freeze service"),
    )
    events: list[str] = []

    result = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
        cores=2,
        status_callback=events.append,
    )

    assert result.outcome == "complete"
    assert result.ordinary_outcome.all_selected_resolved
    assert result.ordinary_outcome.selected_generated_count == 2
    assert lock_calls == read_calls == load_calls == 1
    assert seen_cores == [2]
    assert events[:3] == ["sources_new:2", "sources_changed:0", "sources_unchanged:0"]
    assert events[-1] == "registry_updated"
    assert {
        (role, address)
        for role, address, _artifact_id in _attempt_results(
            fixture.registry_path,
            result.attempt.attempt_id,
        )
    } == {
        ("left", "entity_001"),
        ("left", "entity_002"),
        ("right", "entity_001"),
        ("right", "entity_002"),
    }


def test_specification_member_migrated_mixed_reuse_and_later_direct_reuse(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="migrated")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)
    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        _run_jobs_in_process,
    )

    selected = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )

    assert selected.outcome == "complete"
    assert selected.ordinary_outcome.selected_generated_count == 1
    assert selected.ordinary_outcome.selected_reused_count == 1
    result_artifact_ids = {
        artifact_id
        for _role, _address, artifact_id in _attempt_results(
            fixture.registry_path,
            selected.attempt.attempt_id,
        )
    }
    result_rows = _attempt_results(
        fixture.registry_path,
        selected.attempt.attempt_id,
    )
    entity_001_ids = {
        artifact_id
        for _role, address, artifact_id in result_rows
        if address == "entity_001"
    }
    entity_002_ids = {
        artifact_id
        for _role, address, artifact_id in result_rows
        if address == "entity_002"
    }
    selecting_run_id = _attempt_row(
        fixture.registry_path,
        selected.attempt.attempt_id,
    )[6]
    with sqlite3.connect(fixture.registry_path) as connection:
        producer_by_artifact = {
            int(artifact_id): int(run_id)
            for artifact_id, run_id in connection.execute(
                "SELECT artifact_id, run_id FROM artifacts WHERE artifact_id IN "
                f"({','.join('?' for _ in result_artifact_ids)})",
                tuple(sorted(result_artifact_ids)),
            )
        }
        artifact_count_after_first = int(
            connection.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
        )
    assert all(
        producer_by_artifact[artifact_id] != selecting_run_id
        for artifact_id in entity_001_ids
    )
    assert all(
        producer_by_artifact[artifact_id] == selecting_run_id
        for artifact_id in entity_002_ids
    )
    repeated = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )
    repeated_artifact_ids = {
        artifact_id
        for _role, _address, artifact_id in _attempt_results(
            fixture.registry_path,
            repeated.attempt.attempt_id,
        )
    }
    assert repeated.outcome == "complete"
    assert repeated.attempt.attempt_id != selected.attempt.attempt_id
    assert repeated.ordinary_outcome.selected_generated_count == 0
    assert repeated.ordinary_outcome.selected_reused_count == 2
    assert repeated_artifact_ids == result_artifact_ids
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0] == (
            artifact_count_after_first
        )

    direct = execute_run_plan(
        build_run_plan(
            project_dir=fixture.project_dir,
            context=fixture.context,
            workflow_name="base",
            step_name="fixture_transform",
        ),
        cores=1,
    )
    assert direct.selected_generated_count == 0
    assert direct.selected_reused_count == 1
    assert direct.all_selected_resolved
    assert result_artifact_ids
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0] == (
            artifact_count_after_first
        )


def test_specification_preappend_rejections_do_not_mutate(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    _persist_snapshot(fixture, snapshot)
    included = next(
        member for member in snapshot.members if member.disposition == "included"
    )
    excluded = next(
        member for member in snapshot.members if member.disposition == "excluded"
    )
    before = _freeze_state(fixture.registry_path)

    for overrides, message in (
        ({"cores": 0}, "cores"),
        ({"cores": True}, "cores"),
        ({"context": "../unsafe"}, "context"),
        ({"snapshot_digest": snapshot.snapshot_digest[:16]}, "snapshot_digest"),
        ({"member_key": "../unsafe"}, "member key"),
    ):
        arguments: dict[str, object] = {
            "project_dir": fixture.project_dir,
            "context": fixture.context,
            "snapshot_digest": snapshot.snapshot_digest,
            "member_key": included.member_key,
        }
        arguments.update(overrides)
        with pytest.raises(ValidationError, match=message):
            run_specification_member(**arguments)

    with pytest.raises(ValidationError, match="context mismatch"):
        run_specification_member(
            project_dir=fixture.project_dir,
            context="other_context",
            snapshot_digest=snapshot.snapshot_digest,
            member_key=included.member_key,
        )

    original_load = execution_module.load_workflow_project
    original_open = Path.open
    append_calls = 0

    def guarded_open(
        path: Path,
        *args: object,
        **kwargs: object,
    ) -> object:
        if path == fixture.project_dir / "specifications/compact.yaml":
            pytest.fail("member execution read the authoring specification")
        return original_open(path, *args, **kwargs)

    def forbidden_append(*_args: object, **_kwargs: object) -> object:
        nonlocal append_calls
        append_calls += 1
        pytest.fail("pre-append rejection inserted an attempt")

    monkeypatch.setattr(
        execution_module,
        "append_specification_member_attempt",
        forbidden_append,
    )
    monkeypatch.setattr(Path, "open", guarded_open)
    with pytest.raises(ValidationError, match="unknown specification member"):
        run_specification_member(
            project_dir=fixture.project_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member_key="unknown",
        )
    with pytest.raises(ValidationError, match="excluded specification member"):
        run_specification_member(
            project_dir=fixture.project_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member_key=excluded.member_key,
        )

    transform_path = fixture.project_dir / "steps/fixture_transform.yaml"
    transform = yaml.safe_load(transform_path.read_text(encoding="utf-8"))
    transform["step_contract_version"] = "2"
    transform_path.write_text(
        yaml.safe_dump(transform, sort_keys=False),
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="does not match frozen"):
        run_specification_member(
            project_dir=fixture.project_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member_key=included.member_key,
        )
    assert append_calls == 0

    transform["step_contract_version"] = "1"
    transform_path.write_text(
        yaml.safe_dump(transform, sort_keys=False),
        encoding="utf-8",
    )
    loaded = original_load(project_dir=fixture.project_dir, context=fixture.context)
    monkeypatch.setattr(
        execution_module,
        "load_workflow_project",
        lambda **_kwargs: replace(
            loaded,
            runtime_root=fixture.runtime_dir / "other-runtime",
        ),
    )
    with pytest.raises(ValidationError, match="resolved project boundary"):
        run_specification_member(
            project_dir=fixture.project_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member_key=included.member_key,
        )
    assert _freeze_state(fixture.registry_path) == before


def test_specification_lock_contention_precedes_member_mutation(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)
    before = _table_rows(fixture.registry_path)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        pytest.fail("lock contention reached the member mutation boundary")

    with runtime_lock_module.acquire_mutating_runtime_lock(fixture.runtime_dir):
        monkeypatch.setattr(execution_module, "read_specification_snapshot", forbidden)
        monkeypatch.setattr(execution_module, "load_workflow_project", forbidden)
        monkeypatch.setattr(
            execution_module,
            "append_specification_member_attempt",
            forbidden,
        )
        with pytest.raises(runtime_lock_module.RuntimeLockUnavailableError):
            run_specification_member(
                project_dir=fixture.project_dir,
                context=fixture.context,
                snapshot_digest=snapshot.snapshot_digest,
                member_key=member.member_key,
            )

    assert _table_rows(fixture.registry_path) == before


def test_specification_source_change_reconciles_at_attempt(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)
    source = fixture.runtime_dir / "data/source/entity_002.txt"
    initial_digest = sha256_file_digest(source)
    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        _run_jobs_in_process,
    )

    first = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )

    source.write_text("changed after freeze\n", encoding="utf-8")
    changed_digest = sha256_file_digest(source)

    second = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )

    assert first.outcome == second.outcome == "complete"
    assert first.attempt.snapshot_digest == snapshot.snapshot_digest
    assert second.attempt.snapshot_digest == snapshot.snapshot_digest
    assert initial_digest != changed_digest
    projections = read_specification_snapshot_projections(
        fixture.registry_path,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
    )
    entity_002_digests = {
        attempt_id: {
            basis.source_content_digest
            for basis in projections.result_source_basis
            if basis.attempt_id == attempt_id
            and basis.source_entity_id == "entity_002"
        }
        for attempt_id in (
            first.attempt.attempt_id,
            second.attempt.attempt_id,
        )
    }
    assert entity_002_digests == {
        first.attempt.attempt_id: {initial_digest},
        second.attempt.attempt_id: {changed_digest},
    }


def test_specification_projection_preserves_cross_workflow_reuse_and_lineage(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    config_path = fixture.project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    workflows = config["workflows"]
    assert isinstance(workflows, dict)
    for workflow_name in ("producer_alias", "consumer_alias"):
        workflow_path = fixture.project_dir / f"workflows/{workflow_name}.yaml"
        workflow_path.write_text(
            yaml.safe_dump(
                {
                    "workflow_name": workflow_name,
                    "base_workflow": "base",
                    "step_overrides": {},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        workflows[workflow_name] = f"workflows/{workflow_name}.yaml"
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    analysis_path = fixture.project_dir / "steps/fixture_analysis.yaml"
    analysis = yaml.safe_load(analysis_path.read_text(encoding="utf-8"))
    del analysis["outputs"]["detail"]
    analysis_path.write_text(
        yaml.safe_dump(analysis, sort_keys=False),
        encoding="utf-8",
    )

    loaded = load_workflow_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    )
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=compile_specification(
            {
                "schema": "nipact/specification-set/v1",
                "specification_set": {"key": "cross-workflow-reuse"},
                "libraries": [],
                "fixed": {
                    "workflow": "consumer_alias",
                    "target": {
                        "step": "fixture_analysis",
                        "output": "summary",
                    },
                    "results": {
                        "summary": {
                            "step": "fixture_analysis",
                            "output": "summary",
                        }
                    },
                },
                "members": [
                    {
                        "key": "member-000001",
                        "decision_coordinates": {"workflow": "consumer_alias"},
                        "disposition": "included",
                    }
                ],
                "expected_counts": {
                    "candidates": 1,
                    "included": 1,
                    "excluded": 0,
                },
            },
            {},
        ),
    )
    member = _persist_snapshot(fixture, snapshot)
    producer_plan = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="producer_alias",
        step_name="fixture_analysis",
    )
    consumer_plan = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="consumer_alias",
        step_name="fixture_analysis",
    )
    producer_analysis = next(
        job for job in producer_plan.jobs if job.step_name == "fixture_analysis"
    )
    consumer_analysis = next(
        job for job in consumer_plan.jobs if job.step_name == "fixture_analysis"
    )
    assert producer_analysis.projection_plan == consumer_analysis.projection_plan
    assert producer_analysis.projection_state == consumer_analysis.projection_state
    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        _run_jobs_in_process,
    )

    producer = execute_run_plan(producer_plan)

    assert producer.selected_generated_count == 1
    with sqlite3.connect(fixture.registry_path) as connection:
        producer_run_id, producer_artifact_id = connection.execute(
            """
            SELECT run.run_id, publication.artifact_id
            FROM workflow_runs AS run
            JOIN published_outputs AS publication
              ON publication.context = run.context
             AND publication.workflow_name = run.workflow_name
            WHERE run.context = ? AND run.workflow_name = 'producer_alias'
              AND publication.step_name = 'fixture_analysis'
              AND publication.output_name = 'summary'
              AND publication.address = 'cohort'
            """,
            (fixture.context,),
        ).fetchone()
    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        lambda *_args, **_kwargs: pytest.fail(
            "exact cross-workflow reuse ran Snakemake"
        ),
    )

    selected = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )

    assert selected.outcome == "complete"
    assert selected.ordinary_outcome.selected_generated_count == 0
    assert selected.ordinary_outcome.selected_reused_count == 1
    projections = read_specification_snapshot_projections(
        fixture.registry_path,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
    )
    attempt = projections.attempts[0]
    result = projections.results[0]
    assert attempt.selecting_run_id != producer_run_id
    with sqlite3.connect(fixture.registry_path) as connection:
        selecting_workflow = connection.execute(
            "SELECT workflow_name FROM workflow_runs WHERE run_id = ?",
            (attempt.selecting_run_id,),
        ).fetchone()[0]
    assert selecting_workflow == "consumer_alias"
    assert result.artifact_id == producer_artifact_id
    assert result.producing_run_id == producer_run_id
    assert result.producing_workflow_name == "producer_alias"
    # The member selected this artifact under `consumer_alias`, which has no
    # ordinary run and therefore no current publication. The same artifact
    # being current under `producer_alias` is a different workflow coordinate
    # and deliberately does not satisfy the member's own coordinate.
    assert result.current_publication_path is None
    basis = tuple(
        row
        for row in projections.result_source_basis
        if row.attempt_id == attempt.attempt_id
        and row.role == result.role
        and row.address == result.address
    )
    assert len(basis) == 1
    assert basis[0].source_entity_id == "entity_001"
    assert basis[0].source_content_digest == sha256_file_digest(
        fixture.runtime_dir / "data/source/entity_001.txt"
    )

    # Running the member left the producer's publication exactly as it was and
    # claimed no coordinate of its own, so the projection is stable on re-read.
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute(
            """
            SELECT workflow_name, artifact_id FROM published_outputs
            WHERE context = ? AND step_name = 'fixture_analysis'
              AND output_name = 'summary' AND address = 'cohort'
            """,
            (fixture.context,),
        ).fetchall() == [("producer_alias", producer_artifact_id)]

    assert (
        read_specification_snapshot_projections(
            fixture.registry_path,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
        )
        == projections
    )


@pytest.mark.parametrize(
    ("failure_kind", "expected_stage"),
    (
        ("structural", "planning"),
        ("planning", "planning"),
        ("source_callback", "planning"),
        ("execution", "execution"),
        ("lower_callback", "execution"),
        ("acceptance", "acceptance"),
    ),
)
def test_specification_postappend_failure_stages(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
    expected_stage: str,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)
    original = RuntimeError(f"{failure_kind} broke")
    status_callback = None

    def raise_original() -> None:
        if failure_kind == "planning":
            try:
                raise LookupError("explicit root cause")
            except LookupError as cause:
                raise original from cause
        raise original

    if failure_kind == "structural":
        monkeypatch.setattr(
            execution_module,
            "_build_run_plan_from_loaded_project",
            lambda **_kwargs: (_ for _ in ()).throw(original),
        )
    elif failure_kind == "planning":
        monkeypatch.setattr(
            ordinary_execution_module,
            "reconcile_manifest_and_source_authorities",
            lambda *_args, **_kwargs: raise_original(),
        )
    elif failure_kind == "source_callback":
        def fail_source_callback(event: str) -> None:
            if event.startswith("sources_"):
                raise_original()

        status_callback = fail_source_callback
    elif failure_kind == "acceptance":
        monkeypatch.setattr(
            ordinary_execution_module,
            "_run_snakemake",
            _run_jobs_in_process,
        )
        monkeypatch.setattr(
            ordinary_execution_module,
            "record_workflow_run",
            lambda *_args, **_kwargs: raise_original(),
        )
    elif failure_kind == "lower_callback":
        def fail_lower_callback(event: str) -> None:
            if event == "building_workspace":
                raise_original()

        status_callback = fail_lower_callback
    else:
        monkeypatch.setattr(
            ordinary_execution_module,
            "_run_snakemake",
            lambda *_args, **_kwargs: raise_original(),
        )

    with pytest.raises(RuntimeError, match=f"{failure_kind} broke") as caught:
        run_specification_member(
            project_dir=fixture.project_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member_key=member.member_key,
            status_callback=status_callback,
        )
    assert caught.value is original
    formatted = "".join(traceback.format_exception(caught.value))
    assert "_SpecificationExecutionStageError" not in formatted
    if failure_kind == "planning":
        assert "LookupError: explicit root cause" in formatted
        assert isinstance(caught.value.__cause__, LookupError)
    attempts = _attempt_rows(fixture.registry_path)
    assert len(attempts) == 1
    assert attempts[0][5] == "failed"
    assert attempts[0][7:] == (
        expected_stage,
        f"RuntimeError: {failure_kind} broke",
    )
    assert _attempt_results(fixture.registry_path, int(attempts[0][0])) == ()


def _attempt_rows(database: Path) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(database) as connection:
        return tuple(
            connection.execute(
                """
                SELECT attempt_id, snapshot_digest, member_key, started_at,
                       finished_at, outcome, selecting_run_id,
                       failure_stage, failure_summary
                FROM specification_member_attempts ORDER BY attempt_id
                """
            )
        )


def test_specification_normal_terminal_outcomes(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)
    zero = RunOutcome(
        published_count=0,
        selected_generated_count=0,
        selected_reused_count=0,
        failed_jobs=(("fixture_transform", "entity_001", "missing output"),),
        all_selected_resolved=False,
    )
    monkeypatch.setattr(
        execution_module,
        "_execute_run_plan_already_locked",
        lambda *_args, **_kwargs: zero,
    )

    first = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )
    second = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )

    assert first.outcome == second.outcome == "failed"
    assert first.attempt.attempt_id != second.attempt.attempt_id
    for result in (first, second):
        row = _attempt_row(fixture.registry_path, result.attempt.attempt_id)
        assert row[5:] == (
            "failed",
            None,
            "execution",
            "ordinary execution accepted no required specification results",
        )


@pytest.mark.parametrize(
    ("survivor_mode", "expected_outcome", "expected_results"),
    (
        ("upstream_only", "failed", set()),
        (
            "one_target_address",
            "partial",
            {("left", "entity_001"), ("right", "entity_001")},
        ),
    ),
)
def test_specification_transaction_owned_terminal_outcomes(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    survivor_mode: str,
    expected_outcome: str,
    expected_results: set[tuple[str, str]],
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)

    def run_selected_jobs(
        executable_plan: object,
        *,
        cores: int,
        dry_run: bool,
    ) -> int:
        assert cores > 0
        assert dry_run is False
        run_plan_path = executable_plan.run_workspace / "run_plan.json"
        for job in executable_plan.jobs:
            should_run = (
                job.step_name == "fixture_source"
                if survivor_mode == "upstream_only"
                else job.address == "entity_001"
            )
            if should_run:
                run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        return 0

    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        run_selected_jobs,
    )
    result = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )

    assert result.outcome == expected_outcome
    row = _attempt_row(fixture.registry_path, result.attempt.attempt_id)
    assert isinstance(row[6], int)
    actual_results = {
        (role, address)
        for role, address, _artifact_id in _attempt_results(
            fixture.registry_path,
            result.attempt.attempt_id,
        )
    }
    assert actual_results == expected_results
    if expected_outcome == "failed":
        assert row[7:] == (
            "execution",
            "ordinary execution accepted no required specification results",
        )
        assert result.ordinary_outcome.selected_generated_count == 0
    else:
        assert row[7:] == (None, None)
        assert result.ordinary_outcome.selected_generated_count == 1
    assert read_specification_attempt_outcome(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        attempt=result.attempt,
        member=member,
    ) == expected_outcome


def test_all_specification_members_runs_two_topologies_under_one_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_adapter_project(tmp_path, monkeypatch)
    loaded = load_workflow_project(project_dir=project_dir, context="adapter")
    payload = {
        "schema": "nipact/specification-set/v1",
        "specification_set": {"key": "two-factories"},
        "libraries": [],
        "fixed": {},
        "members": [
            {
                "key": "member-000001",
                "decision_coordinates": {"factory": "base"},
                "disposition": "included",
                "workflow": "base",
                "set": [
                    {"step": "model", "parameter": "suffix", "value": "-a"}
                ],
                "execution_population": "selected",
                "manifest_bindings": [
                    {
                        "step": "model",
                        "role": "analysis_population",
                        "manifest": "selected",
                    }
                ],
                "target": {"step": "model", "output": "estimate"},
                "results": {
                    "reported_diagnostics": {
                        "step": "model",
                        "output": "diagnostics",
                    },
                    "reported_estimate": {
                        "step": "model",
                        "output": "estimate",
                    },
                },
            },
            {
                "key": "member-000002",
                "decision_coordinates": {"factory": "alternative"},
                "disposition": "included",
                "workflow": "alternative",
                "set": [
                    {
                        "step": "alt_model",
                        "parameter": "suffix",
                        "value": "-a",
                    }
                ],
                "execution_population": "selected",
                "manifest_bindings": [
                    {
                        "step": "alt_model",
                        "role": "analysis_population",
                        "manifest": "selected",
                    }
                ],
                "target": {"step": "alt_model", "output": "alt_estimate"},
                "results": {
                    "reported_diagnostics": {
                        "step": "alt_model",
                        "output": "alt_diagnostics",
                    },
                    "reported_estimate": {
                        "step": "alt_model",
                        "output": "alt_estimate",
                    },
                },
            },
        ],
        "expected_counts": {"candidates": 2, "included": 2, "excluded": 0},
    }
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=compile_specification(payload, {}),
    )
    registry_path = runtime_dir / "database/registry.db"
    assert insert_or_verify_specification_snapshot(
        registry_path,
        runtime_root=runtime_dir,
        snapshot=snapshot,
    )
    original_lock = execution_module.acquire_mutating_runtime_lock
    original_read = execution_module.read_specification_snapshot
    original_load = execution_module.load_workflow_project
    calls = {"lock": 0, "read": 0, "load": 0}

    @contextmanager
    def tracked_lock(runtime_root: Path) -> Iterator[None]:
        calls["lock"] += 1
        with original_lock(runtime_root):
            yield

    def tracked_read(*args: object, **kwargs: object) -> object:
        calls["read"] += 1
        return original_read(*args, **kwargs)

    def tracked_load(*args: object, **kwargs: object) -> object:
        calls["load"] += 1
        return original_load(*args, **kwargs)

    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", tracked_lock)
    monkeypatch.setattr(execution_module, "read_specification_snapshot", tracked_read)
    monkeypatch.setattr(execution_module, "load_workflow_project", tracked_load)
    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        _run_jobs_in_process,
    )

    result = run_all_specification_members(
        project_dir=project_dir,
        context="adapter",
        snapshot_digest=snapshot.snapshot_digest,
    )

    assert calls == {"lock": 1, "read": 1, "load": 1}
    assert [member.outcome for member in result.members] == ["complete", "complete"]
    assert [member.attempt.member_key for member in result.members] == [
        "member-000001",
        "member-000002",
    ]
    assert [
        member.ordinary_outcome.selected_generated_count for member in result.members
    ] == [1, 1]
    with sqlite3.connect(registry_path) as connection:
        attempts = connection.execute(
            """
            SELECT attempt.member_key, attempt.outcome, run.workflow_name
            FROM specification_member_attempts AS attempt
            JOIN workflow_runs AS run ON run.run_id = attempt.selecting_run_id
            ORDER BY attempt.attempt_id
            """
        ).fetchall()
        shared_upstream = connection.execute(
            """
            SELECT DISTINCT dependent.step_name, source.artifact_id
            FROM artifact_dependencies AS dependency
            JOIN artifacts AS dependent
              ON dependent.artifact_id = dependency.dependent_artifact_id
            JOIN artifacts AS source
              ON source.artifact_id = dependency.source_artifact_id
            WHERE dependent.step_name IN ('model', 'alt_model')
              AND source.step_name = 'import_seed'
            ORDER BY dependent.step_name
            """
        ).fetchall()
        import_count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM artifacts
                WHERE origin = 'workflow_output' AND step_name = 'import_seed'
                """
            ).fetchone()[0]
        )
    assert attempts == [
        ("member-000001", "complete", "base"),
        ("member-000002", "complete", "alternative"),
    ]
    assert len(shared_upstream) == 2
    assert shared_upstream[0][1] == shared_upstream[1][1]
    assert import_count == 1
    result_sets = [
        {
            (role, address, artifact_id)
            for role, address, artifact_id in _attempt_results(
                registry_path,
                member.attempt.attempt_id,
            )
        }
        for member in result.members
    ]
    assert [
        {(role, address) for role, address, _artifact_id in rows}
        for rows in result_sets
    ] == [
        {
            ("reported_diagnostics", "entity_001"),
            ("reported_estimate", "entity_001"),
        },
        {
            ("reported_diagnostics", "entity_001"),
            ("reported_estimate", "entity_001"),
        },
    ]
    assert {row[2] for row in result_sets[0]}.isdisjoint(
        {row[2] for row in result_sets[1]}
    )


def test_all_specification_members_uses_canonical_stop_policy(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    payload = _specification_payload()
    payload["dimensions"] = {
        "variant": {
            "set": {"step": "fixture_transform", "parameter": "variant"},
            "values": ["base", "excluded", "second", "third"],
        }
    }
    payload["exclude"] = [
        {
            "when": {"variant": "excluded"},
            "reason": "interleaved excluded member",
        }
    ]
    payload["expected_counts"] = {
        "candidates": 4,
        "included": 3,
        "excluded": 1,
    }
    snapshot = build_snapshot(fixture, payload=payload)
    assert insert_or_verify_specification_snapshot(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        snapshot=snapshot,
    )
    included = tuple(
        member for member in snapshot.members if member.disposition == "included"
    )
    original_lock = execution_module.acquire_mutating_runtime_lock
    original_read = execution_module.read_specification_snapshot
    original_load = execution_module.load_workflow_project
    calls = {"lock": 0, "read": 0, "load": 0}

    @contextmanager
    def tracked_lock(runtime_root: Path) -> Iterator[None]:
        calls["lock"] += 1
        with original_lock(runtime_root):
            yield

    def tracked_read(*args: object, **kwargs: object) -> object:
        calls["read"] += 1
        return original_read(*args, **kwargs)

    def tracked_load(*args: object, **kwargs: object) -> object:
        calls["load"] += 1
        return original_load(*args, **kwargs)

    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", tracked_lock)
    monkeypatch.setattr(execution_module, "read_specification_snapshot", tracked_read)
    monkeypatch.setattr(execution_module, "load_workflow_project", tracked_load)
    visited: list[str] = []
    forwarded: list[tuple[int, object]] = []
    outcomes = iter(("complete", "partial"))

    def callback(_event: str) -> None:
        pass

    def fake_member(
        *,
        member: object,
        cores: int,
        status_callback: object,
        **_kwargs: object,
    ) -> object:
        visited.append(member.member_key)
        forwarded.append((cores, status_callback))
        outcome = next(outcomes)
        return SpecificationMemberRunResult(
            attempt=SpecificationAttemptRef(
                attempt_id=len(visited),
                context=fixture.context,
                snapshot_digest=snapshot.snapshot_digest,
                member_key=member.member_key,
            ),
            outcome=outcome,
            ordinary_outcome=RunOutcome(
                published_count=0,
                selected_generated_count=0,
                selected_reused_count=0,
                failed_jobs=(),
                all_selected_resolved=outcome == "complete",
            ),
        )

    monkeypatch.setattr(execution_module, "_run_replayed_member", fake_member)
    result = run_all_specification_members(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        cores=3,
        status_callback=callback,
    )

    assert visited == [member.member_key for member in included[:2]]
    assert forwarded == [(3, callback), (3, callback)]
    assert [member.outcome for member in result.members] == ["complete", "partial"]
    assert calls == {"lock": 1, "read": 1, "load": 1}

    empty_payload = _specification_payload()
    empty_payload["dimensions"] = {
        "variant": {
            "set": {"step": "fixture_transform", "parameter": "variant"},
            "values": ["base"],
        }
    }
    empty_payload["exclude"] = [
        {"when": {"variant": "base"}, "reason": "empty included set"}
    ]
    empty_payload["expected_counts"] = {
        "candidates": 1,
        "included": 0,
        "excluded": 1,
    }
    empty = build_snapshot(fixture, payload=empty_payload)
    assert insert_or_verify_specification_snapshot(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        snapshot=empty,
    )
    monkeypatch.setattr(
        execution_module,
        "load_workflow_project",
        lambda **_kwargs: pytest.fail("empty included set loaded the ordinary project"),
    )
    empty_result = run_all_specification_members(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=empty.snapshot_digest,
    )
    assert empty_result.snapshot_digest == empty.snapshot_digest
    assert empty_result.members == ()
    assert calls == {"lock": 2, "read": 2, "load": 1}


def test_specification_interruption_and_postcommit_callback_boundaries(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    snapshot = build_entity_snapshot(fixture)
    member = _persist_snapshot(fixture, snapshot)

    class StopNow(BaseException):
        pass

    with monkeypatch.context() as patch:
        patch.setattr(
            execution_module,
            "_execute_run_plan_already_locked",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(StopNow()),
        )
        with pytest.raises(StopNow):
            run_specification_member(
                project_dir=fixture.project_dir,
                context=fixture.context,
                snapshot_digest=snapshot.snapshot_digest,
                member_key=member.member_key,
            )
    interrupted = _attempt_rows(fixture.registry_path)[0]
    assert interrupted[4:] == (None, None, None, None, None)

    monkeypatch.setattr(
        ordinary_execution_module,
        "_run_snakemake",
        _run_jobs_in_process,
    )

    def postcommit_failure(event: str) -> None:
        if event == "registry_updated":
            raise RuntimeError("callback after commit")

    with pytest.raises(RuntimeError, match="callback after commit"):
        run_specification_member(
            project_dir=fixture.project_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member_key=member.member_key,
            status_callback=postcommit_failure,
        )
    committed_attempt = _attempt_rows(fixture.registry_path)[1]
    committed_ref = SpecificationAttemptRef(
        attempt_id=int(committed_attempt[0]),
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )
    assert read_specification_attempt_outcome(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        attempt=committed_ref,
        member=member,
    ) == "complete"
