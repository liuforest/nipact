from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
import yaml

import nipact.runtime_lock as runtime_lock_module
import nipact.specification_execution as execution_module
from conftest import RegistryV18Fixture
from nipact.errors import ValidationError
from nipact.specification_execution import freeze_specification_snapshot
from nipact.specification_loading import (
    ExplicitSpecificationSource,
    RegisteredSpecificationSource,
)
from test_specification_registry import (
    _ordinary_state,
    _specification_payload,
    _table_rows,
    prepare_v19,
)


def test_registered_and_explicit_freeze_share_identity_and_exact_replay(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    expected = prepare_v19(fixture, route="fresh")
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
    prepare_v19(fixture, route="fresh")
    specification_path = fixture.project_dir / "specifications/compact.yaml"
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
