from __future__ import annotations

import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Callable

import pytest
import yaml

from conftest import RegistryV18Fixture
from nipact.errors import ValidationError
from nipact.registry import (
    initialize_registry_db,
    insert_or_verify_specification_snapshot,
    migrate_registry_db,
    read_specification_snapshot,
)
from nipact.specification_canonical import (
    CanonicalSpecificationSnapshot,
    canonicalize_specification_snapshot,
)
from nipact.specification_compiler import compile_specification
from nipact.workflow import load_workflow_project


_SPECIFICATION_TABLES = (
    "specification_snapshots",
    "specification_members",
    "specification_snapshot_manifest_values",
    "specification_expected_results",
    "specification_member_attempts",
    "specification_attempt_results",
)
_MIGRATION_GUIDANCE = "requires explicit migration"


def _specification_payload() -> dict[str, object]:
    return {
        "schema": "nipact/specification-set/v1",
        "specification_set": {"key": "compact-freeze"},
        "libraries": [],
        "fixed": {
            "workflow": "base",
            "execution_population": "expanded",
            "manifest_bindings": [
                {
                    "step": "fixture_analysis",
                    "role": "analysis_population",
                    "manifest": "expanded",
                }
            ],
            "target": {"step": "fixture_analysis", "output": "summary"},
            "results": {
                "summary": {"step": "fixture_analysis", "output": "summary"}
            },
        },
        "dimensions": {
            "variant": {
                "set": {"step": "fixture_transform", "parameter": "variant"},
                "values": ["base", "changed"],
            }
        },
        "combine": {"product": ["variant"]},
        "exclude": [
            {
                "when": {"variant": "changed"},
                "reason": "Prospectively excluded compact case.",
            }
        ],
        "expected_counts": {"candidates": 2, "included": 1, "excluded": 1},
    }


def configure_specification_project(fixture: RegistryV18Fixture) -> Path:
    """Add one two-entity specification without touching registry authority."""
    source_index_path = fixture.project_dir / "sources.yaml"
    source_index = yaml.safe_load(source_index_path.read_text(encoding="utf-8"))
    source_index["entities"]["entity_002"] = {
        "source_value": "data/source/entity_002.txt"
    }
    source_index_path.write_text(
        yaml.safe_dump(source_index, sort_keys=False),
        encoding="utf-8",
    )
    second_source = fixture.runtime_dir / "data/source/entity_002.txt"
    second_source.write_text("second compact source\n", encoding="utf-8")

    expanded_manifest = fixture.project_dir / "manifests/expanded.yaml"
    expanded_manifest.write_text(
        yaml.safe_dump(
            {
                "description": "Two-entity compact freeze cohort",
                "entities": ["entity_001", "entity_002"],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    specification_path = fixture.project_dir / "specifications/compact.yaml"
    specification_path.parent.mkdir()
    specification_path.write_text(
        yaml.safe_dump(_specification_payload(), sort_keys=False),
        encoding="utf-8",
    )

    config_path = fixture.project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["manifests"]["expanded"] = "manifests/expanded.yaml"
    config["specifications"] = {"compact": "specifications/compact.yaml"}
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return specification_path


def prepare_v19(
    fixture: RegistryV18Fixture,
    *,
    route: str,
) -> CanonicalSpecificationSnapshot:
    if route == "fresh":
        loaded = load_workflow_project(
            project_dir=fixture.project_dir,
            context=fixture.context,
        )
        fixture.registry_path.unlink()
        initialize_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
            manifests=loaded.manifests,
            manifest_paths={
                name: path.relative_to(fixture.project_dir).as_posix()
                for name, path in loaded.manifest_paths.items()
            },
        )
    elif route == "migrated":
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )
    else:  # pragma: no cover - test helper contract
        raise AssertionError(route)

    configure_specification_project(fixture)
    return build_snapshot(fixture)


def build_snapshot(
    fixture: RegistryV18Fixture,
    *,
    payload: dict[str, object] | None = None,
) -> CanonicalSpecificationSnapshot:
    loaded = load_workflow_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    )
    return canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=compile_specification(
            _specification_payload() if payload is None else payload,
            {},
        ),
    )


def _table_rows(
    database: Path,
    *,
    include: Callable[[str], bool] = lambda _name: True,
) -> dict[str, tuple[tuple[object, ...], ...]]:
    with sqlite3.connect(database) as connection:
        tables = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
            if include(str(row[0]))
        )
        state = {
            table: tuple(
                sorted(
                    connection.execute(f'SELECT * FROM "{table}"').fetchall(),
                    key=repr,
                )
            )
            for table in tables
        }
        state["sqlite_sequence"] = tuple(
            connection.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        )
    return state


def _ordinary_state(database: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    return _table_rows(
        database,
        include=lambda name: (
            not name.startswith("specification_") and name != "manifest_values"
        ),
    )


def _freeze_state(database: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    return _table_rows(
        database,
        include=lambda name: (
            name.startswith("specification_") or name == "manifest_values"
        ),
    )


def _manifest_values(database: Path) -> set[tuple[object, ...]]:
    with sqlite3.connect(database) as connection:
        return set(
            connection.execute(
                """
                SELECT value_schema, manifest_digest, canonical_body, entity_count
                FROM manifest_values
                """
            )
        )


def _runtime_without_database(runtime_root: Path) -> tuple[tuple[str, str, bytes | None], ...]:
    inventory: list[tuple[str, str, bytes | None]] = []
    for path in sorted(runtime_root.rglob("*")):
        relative = path.relative_to(runtime_root).as_posix()
        if relative == ".nipact-mutating.lock" or relative.startswith("database/"):
            continue
        if path.is_symlink():
            inventory.append((relative, "symlink", str(path.readlink()).encode()))
        elif path.is_dir():
            inventory.append((relative, "directory", None))
        else:
            inventory.append((relative, "file", path.read_bytes()))
    return tuple(inventory)


def _assert_raw_aggregate(
    database: Path,
    snapshot: CanonicalSpecificationSnapshot,
) -> None:
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT snapshot_digest, context, canonical_bytes "
            "FROM specification_snapshots"
        ).fetchall() == [
            (snapshot.snapshot_digest, snapshot.context, snapshot.canonical_bytes)
        ]
        assert connection.execute(
            """
            SELECT member_key, row_digest, disposition, exclusion_reason
            FROM specification_members ORDER BY member_key
            """
        ).fetchall() == [
            (
                member.member_key,
                member.row.row_digest,
                member.disposition,
                member.exclusion_reason,
            )
            for member in snapshot.members
        ]
        assert connection.execute(
            """
            SELECT value_schema, manifest_digest
            FROM specification_snapshot_manifest_values
            ORDER BY value_schema, manifest_digest
            """
        ).fetchall() == [
            (value.value_schema, value.manifest_digest)
            for value in snapshot.manifest_values
        ]
        assert connection.execute(
            """
            SELECT member_key, role, step_name, output_name, address
            FROM specification_expected_results
            ORDER BY member_key, role, address, step_name, output_name
            """
        ).fetchall() == sorted(
            (
                (
                    member.member_key,
                    result.role,
                    result.step_name,
                    result.output_name,
                    result.address,
                )
                for member in snapshot.members
                for result in member.expected_results
            ),
            key=lambda row: (row[0], row[1], row[4], row[2], row[3]),
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM specification_member_attempts"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM specification_attempt_results"
        ).fetchone() == (0,)


@pytest.mark.parametrize("route", ["fresh", "migrated"])
def test_primary_persistence_is_exact_and_changes_only_manifest_value_authority(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
) -> None:
    fixture = registry_v18_fixture
    snapshot = prepare_v19(fixture, route=route)
    before_ordinary = _ordinary_state(fixture.registry_path)
    before_values = _manifest_values(fixture.registry_path)
    before_runtime = _runtime_without_database(fixture.runtime_dir)
    source_path = fixture.runtime_dir / "data/source/entity_002.txt"

    def forbidden_source_open(path: Path, *_args: object, **_kwargs: object) -> object:
        if path == source_path:
            pytest.fail("snapshot persistence read scientific source content")
        return original_open(path, *_args, **_kwargs)

    def forbidden_process(*_args: object, **_kwargs: object) -> object:
        pytest.fail("snapshot persistence launched an execution process")

    original_open = Path.open
    with monkeypatch.context() as guard:
        guard.setattr(Path, "open", forbidden_source_open)
        guard.setattr(subprocess, "run", forbidden_process)
        assert insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=snapshot,
        )

    assert read_specification_snapshot(
        fixture.registry_path,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
    ) == snapshot
    _assert_raw_aggregate(fixture.registry_path, snapshot)
    assert _ordinary_state(fixture.registry_path) == before_ordinary
    assert _runtime_without_database(fixture.runtime_dir) == before_runtime

    after_values = _manifest_values(fixture.registry_path)
    expected_added = {
        (
            value.value_schema,
            value.manifest_digest,
            value.canonical_body,
            value.entity_count,
        )
        for value in snapshot.manifest_values
    }
    assert expected_added.isdisjoint(before_values)
    assert after_values - before_values == expected_added
    assert before_values <= after_values

    def snapshot_with_reason(reason: str) -> CanonicalSpecificationSnapshot:
        payload = _specification_payload()
        exclusions = payload["exclude"]
        assert isinstance(exclusions, list)
        assert isinstance(exclusions[0], dict)
        exclusions[0]["reason"] = reason
        return build_snapshot(fixture, payload=payload)

    second = snapshot_with_reason("Second reviewed compact exclusion.")
    assert second.snapshot_digest != snapshot.snapshot_digest
    assert second.manifest_values == snapshot.manifest_values
    values_before_second = _manifest_values(fixture.registry_path)
    assert insert_or_verify_specification_snapshot(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        snapshot=second,
    )
    assert _manifest_values(fixture.registry_path) == values_before_second

    third = snapshot_with_reason("Third reviewed compact exclusion.")
    assert third.snapshot_digest not in {
        snapshot.snapshot_digest,
        second.snapshot_digest,
    }
    assert third.manifest_values == snapshot.manifest_values
    value = snapshot.manifest_values[0]
    with sqlite3.connect(fixture.registry_path) as connection:
        connection.execute(
            """
            UPDATE manifest_values SET entity_count = entity_count + 1
            WHERE value_schema = ? AND manifest_digest = ?
            """,
            (value.value_schema, value.manifest_digest),
        )
    corrupted = _freeze_state(fixture.registry_path)
    with pytest.raises(ValidationError, match="manifest value does not match"):
        insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=third,
        )
    assert _freeze_state(fixture.registry_path) == corrupted


@pytest.mark.parametrize(
    "checkpoint",
    [
        "after_manifest_values",
        "after_snapshot",
        "after_members",
        "after_manifest_associations",
        "after_expected_results",
        "after_verification",
    ],
)
def test_every_insertion_checkpoint_rolls_back_the_whole_aggregate(
    registry_v18_fixture: RegistryV18Fixture,
    checkpoint: str,
) -> None:
    fixture = registry_v18_fixture
    snapshot = prepare_v19(fixture, route="fresh")
    before = _freeze_state(fixture.registry_path)

    def fail_at_checkpoint(current: str) -> None:
        if current == checkpoint:
            raise RuntimeError(current)

    with pytest.raises(RuntimeError, match=checkpoint):
        insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=snapshot,
            _fault_hook=fail_at_checkpoint,
        )

    assert _freeze_state(fixture.registry_path) == before
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize(
    "corruption",
    [
        "parent-bytes",
        "member",
        "manifest-association",
        "manifest-value",
        "expected-result",
        "surplus-result",
    ],
)
def test_reader_and_replay_reject_corrupt_aggregate_without_repair(
    registry_v18_fixture: RegistryV18Fixture,
    corruption: str,
) -> None:
    fixture = registry_v18_fixture
    snapshot = prepare_v19(fixture, route="fresh")
    assert insert_or_verify_specification_snapshot(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        snapshot=snapshot,
    )
    member = snapshot.members[0]
    result = member.expected_results[0]
    value = snapshot.manifest_values[0]

    with sqlite3.connect(fixture.registry_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        if corruption == "parent-bytes":
            connection.execute(
                "UPDATE specification_snapshots SET canonical_bytes = ?",
                (sqlite3.Binary(snapshot.canonical_bytes + b" "),),
            )
        elif corruption == "member":
            connection.execute(
                """
                UPDATE specification_members SET row_digest = ?
                WHERE snapshot_digest = ? AND member_key = ?
                """,
                ("f" * 64, snapshot.snapshot_digest, member.member_key),
            )
        elif corruption == "manifest-association":
            connection.execute(
                """
                DELETE FROM specification_snapshot_manifest_values
                WHERE snapshot_digest = ? AND value_schema = ? AND manifest_digest = ?
                """,
                (snapshot.snapshot_digest, value.value_schema, value.manifest_digest),
            )
        elif corruption == "manifest-value":
            connection.execute(
                """
                UPDATE manifest_values SET entity_count = entity_count + 1
                WHERE value_schema = ? AND manifest_digest = ?
                """,
                (value.value_schema, value.manifest_digest),
            )
        elif corruption == "expected-result":
            connection.execute(
                """
                UPDATE specification_expected_results SET step_name = 'fixture_transform'
                WHERE snapshot_digest = ? AND member_key = ?
                  AND role = ? AND address = ?
                """,
                (
                    snapshot.snapshot_digest,
                    member.member_key,
                    result.role,
                    result.address,
                ),
            )
        else:
            connection.execute(
                """
                INSERT INTO specification_expected_results (
                    snapshot_digest, member_key, role,
                    step_name, output_name, address
                ) VALUES (?, ?, 'surplus', ?, ?, ?)
                """,
                (
                    snapshot.snapshot_digest,
                    member.member_key,
                    result.step_name,
                    result.output_name,
                    f"{result.address}-surplus",
                ),
            )

    corrupted = _freeze_state(fixture.registry_path)
    with pytest.raises(ValidationError):
        read_specification_snapshot(
            fixture.registry_path,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
        )
    assert _freeze_state(fixture.registry_path) == corrupted
    with pytest.raises(ValidationError):
        insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=snapshot,
        )
    assert _freeze_state(fixture.registry_path) == corrupted


@pytest.mark.parametrize(
    "case",
    [
        "unknown-digest",
        "wrong-context",
        "wrong-runtime",
        "copied-registry",
        "missing-registry",
        "symlink-registry",
        "nonregular-registry",
        "v18-read",
        "v18-write",
    ],
)
def test_ownership_and_schema_boundaries_fail_before_snapshot_mutation(
    registry_v18_fixture: RegistryV18Fixture,
    tmp_path: Path,
    case: str,
) -> None:
    fixture = registry_v18_fixture
    if case.startswith("v18-"):
        configure_specification_project(fixture)
        snapshot = build_snapshot(fixture)
    else:
        snapshot = prepare_v19(fixture, route="fresh")

    if case == "wrong-context":
        assert insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=snapshot,
        )
    before = _freeze_state(fixture.registry_path)
    preserved_database = fixture.registry_path

    if case == "unknown-digest":
        operation = lambda: read_specification_snapshot(
            fixture.registry_path,
            context=fixture.context,
            snapshot_digest="0" * 64,
        )
    elif case == "wrong-context":
        operation = lambda: read_specification_snapshot(
            fixture.registry_path,
            context="other",
            snapshot_digest=snapshot.snapshot_digest,
        )
    elif case in {"wrong-runtime", "copied-registry"}:
        copied_runtime = tmp_path / "copied-runtime"
        copied_registry = copied_runtime / "database/registry.db"
        copied_registry.parent.mkdir(parents=True)
        shutil.copy2(fixture.registry_path, copied_registry)
        copied_before = _freeze_state(copied_registry)
        if case == "wrong-runtime":
            operation = lambda: insert_or_verify_specification_snapshot(
                fixture.registry_path,
                runtime_root=copied_runtime,
                snapshot=snapshot,
            )
        else:
            operation = lambda: insert_or_verify_specification_snapshot(
                copied_registry,
                runtime_root=copied_runtime,
                snapshot=snapshot,
            )
    elif case in {"missing-registry", "symlink-registry", "nonregular-registry"}:
        preserved_database = fixture.registry_path.with_name("preserved-registry.db")
        fixture.registry_path.replace(preserved_database)
        preserved_before = _freeze_state(preserved_database)
        if case == "symlink-registry":
            fixture.registry_path.symlink_to(preserved_database.name)
        elif case == "nonregular-registry":
            fixture.registry_path.mkdir()
        operation = lambda: insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=snapshot,
        )
    elif case == "v18-read":
        operation = lambda: read_specification_snapshot(
            fixture.registry_path,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
        )
    else:
        operation = lambda: insert_or_verify_specification_snapshot(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            snapshot=snapshot,
        )

    match = _MIGRATION_GUIDANCE if case.startswith("v18-") else None
    with pytest.raises(ValidationError, match=match):
        operation()

    if case in {"wrong-runtime", "copied-registry"}:
        assert _freeze_state(copied_registry) == copied_before
    if case in {"missing-registry", "symlink-registry", "nonregular-registry"}:
        assert _freeze_state(preserved_database) == preserved_before
        if case == "missing-registry":
            assert not fixture.registry_path.exists()
        elif case == "symlink-registry":
            assert fixture.registry_path.is_symlink()
        else:
            assert fixture.registry_path.is_dir()
    assert _freeze_state(preserved_database) == before
