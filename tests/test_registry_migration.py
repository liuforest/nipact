from __future__ import annotations

import multiprocessing
import shutil
import sqlite3
from pathlib import Path
from typing import Callable

import pytest

import nipact.execution as execution_module
import nipact.registry as registry_module
from conftest import (
    RegistryV18Fixture,
    RegistryV19Fixture,
    registry_schema_signature,
)
from nipact.cli import main
from nipact.errors import ValidationError
from nipact.registry import (
    REGISTRY_SCHEMA_VERSION,
    REGISTRY_V18_BACKUP_FILENAME,
    initialize_registry_db,
    migrate_registry_db,
)
from nipact.runtime_lock import (
    RUNTIME_LOCK_FILENAME,
    RuntimeLockUnavailableError,
    acquire_mutating_runtime_lock,
)


_NEW_TABLES = {
    "specification_attempt_results",
    "specification_expected_results",
    "specification_member_attempts",
    "specification_members",
    "specification_snapshot_manifest_values",
    "specification_snapshots",
}
_NEW_INDEXES = {
    "specification_attempt_results_artifact_idx",
    "specification_member_attempts_member_idx",
    "specification_snapshot_manifest_values_value_idx",
}
_EXPECTED_COLUMNS = {
    "specification_snapshots": (
        ("snapshot_digest", "TEXT", 0, 1),
        ("context", "TEXT", 1, 0),
        ("canonical_bytes", "BLOB", 1, 0),
    ),
    "specification_members": (
        ("snapshot_digest", "TEXT", 1, 1),
        ("member_key", "TEXT", 1, 2),
        ("row_digest", "TEXT", 1, 0),
        ("disposition", "TEXT", 1, 0),
        ("exclusion_reason", "TEXT", 0, 0),
    ),
    "specification_snapshot_manifest_values": (
        ("snapshot_digest", "TEXT", 1, 1),
        ("value_schema", "TEXT", 1, 2),
        ("manifest_digest", "TEXT", 1, 3),
    ),
    "specification_expected_results": (
        ("snapshot_digest", "TEXT", 1, 1),
        ("member_key", "TEXT", 1, 2),
        ("role", "TEXT", 1, 3),
        ("step_name", "TEXT", 1, 0),
        ("output_name", "TEXT", 1, 0),
        ("address", "TEXT", 1, 4),
    ),
    "specification_member_attempts": (
        ("attempt_id", "INTEGER", 0, 1),
        ("snapshot_digest", "TEXT", 1, 0),
        ("member_key", "TEXT", 1, 0),
        ("started_at", "TEXT", 1, 0),
        ("finished_at", "TEXT", 0, 0),
        ("outcome", "TEXT", 0, 0),
        ("selecting_run_id", "INTEGER", 0, 0),
        ("failure_stage", "TEXT", 0, 0),
        ("failure_summary", "TEXT", 0, 0),
    ),
    "specification_attempt_results": (
        ("attempt_id", "INTEGER", 1, 1),
        ("snapshot_digest", "TEXT", 1, 0),
        ("member_key", "TEXT", 1, 0),
        ("role", "TEXT", 1, 2),
        ("address", "TEXT", 1, 3),
        ("artifact_id", "INTEGER", 1, 0),
    ),
}
_EXPECTED_FOREIGN_KEYS = {
    "specification_snapshots": {
        ("contexts", "RESTRICT", (("context", "context"),)),
    },
    "specification_members": {
        (
            "specification_snapshots",
            "RESTRICT",
            (("snapshot_digest", "snapshot_digest"),),
        ),
    },
    "specification_snapshot_manifest_values": {
        (
            "specification_snapshots",
            "RESTRICT",
            (("snapshot_digest", "snapshot_digest"),),
        ),
        (
            "manifest_values",
            "RESTRICT",
            (("value_schema", "value_schema"), ("manifest_digest", "manifest_digest")),
        ),
    },
    "specification_expected_results": {
        (
            "specification_members",
            "RESTRICT",
            (("snapshot_digest", "snapshot_digest"), ("member_key", "member_key")),
        ),
    },
    "specification_member_attempts": {
        (
            "specification_members",
            "RESTRICT",
            (("snapshot_digest", "snapshot_digest"), ("member_key", "member_key")),
        ),
        ("workflow_runs", "RESTRICT", (("selecting_run_id", "run_id"),)),
    },
    "specification_attempt_results": {
        ("artifacts", "RESTRICT", (("artifact_id", "artifact_id"),)),
        (
            "specification_member_attempts",
            "RESTRICT",
            (
                ("attempt_id", "attempt_id"),
                ("snapshot_digest", "snapshot_digest"),
                ("member_key", "member_key"),
            ),
        ),
        (
            "specification_expected_results",
            "RESTRICT",
            (
                ("snapshot_digest", "snapshot_digest"),
                ("member_key", "member_key"),
                ("role", "role"),
                ("address", "address"),
            ),
        ),
    },
}
_MIGRATION_GUIDANCE = (
    "registry.db schema version 18 requires explicit migration; run "
    "'nipact registry migrate --context CONTEXT --project-dir PROJECT_DIR'"
)


def _backup_path(fixture: RegistryV18Fixture) -> Path:
    return fixture.registry_path.with_name(REGISTRY_V18_BACKUP_FILENAME)


def _schema_version(database: Path) -> int:
    with sqlite3.connect(database) as connection:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])


def _logical_state(
    database: Path,
) -> tuple[dict[str, tuple[tuple[object, ...], ...]], tuple[tuple[object, ...], ...]]:
    with sqlite3.connect(database) as connection:
        table_names = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        )
        rows = {
            table: tuple(
                sorted(
                    connection.execute(f'SELECT * FROM "{table}"').fetchall(),
                    key=repr,
                )
            )
            for table in table_names
        }
        sequences = tuple(
            connection.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        )
    return rows, sequences


def _runtime_inventory(runtime_dir: Path) -> tuple[tuple[str, str, bytes | None], ...]:
    inventory: list[tuple[str, str, bytes | None]] = []
    for path in sorted(runtime_dir.rglob("*")):
        relative = path.relative_to(runtime_dir).as_posix()
        if path.is_symlink():
            inventory.append((relative, "symlink", str(path.readlink()).encode()))
        elif path.is_dir():
            inventory.append((relative, "directory", None))
        else:
            inventory.append((relative, "file", path.read_bytes()))
    return tuple(inventory)


def _fresh_v19(tmp_path: Path, *, context: str = "fresh") -> tuple[Path, Path]:
    runtime_dir = tmp_path / "fresh-runtime"
    registry_path = runtime_dir / "database/registry.db"
    registry_path.parent.mkdir(parents=True)
    initialize_registry_db(
        registry_path,
        context=context,
        runtime_root=runtime_dir,
        manifests={},
        manifest_paths={},
    )
    return runtime_dir, registry_path


def _foreign_key_shape(
    signature: dict[str, object],
    table: str,
) -> set[tuple[str, str, tuple[tuple[str, str], ...]]]:
    table_signature = signature["tables"][table]  # type: ignore[index]
    return {
        (
            str(item["table"]),
            str(item["on_delete"]),
            tuple(
                (str(column["from"]), str(column["to"]))
                for column in item["columns"]
            ),
        )
        for item in table_signature["foreign_keys"]
    }


def test_fresh_and_migrated_v19_have_the_same_exact_additive_schema(
    tmp_path: Path,
    registry_v19_fixture: RegistryV19Fixture,
) -> None:
    _runtime_dir, fresh_database = _fresh_v19(tmp_path)
    fresh = registry_schema_signature(fresh_database)
    migrated = registry_schema_signature(registry_v19_fixture.registry_path)
    assert fresh == migrated
    assert fresh["user_version"] == REGISTRY_SCHEMA_VERSION

    table_names = {
        str(item["name"])
        for item in fresh["objects"]
        if item["type"] == "table"
    }
    assert {name for name in table_names if name.startswith("specification_")} == (
        _NEW_TABLES
    )
    index_names = {
        str(item["name"])
        for item in fresh["objects"]
        if item["type"] == "index" and str(item["name"]).startswith("specification_")
    }
    assert index_names == _NEW_INDEXES

    for table, expected in _EXPECTED_COLUMNS.items():
        actual = fresh["tables"][table]["columns"]  # type: ignore[index]
        assert tuple(
            (row["name"], row["type"], row["notnull"], row["pk"])
            for row in actual
        ) == expected
        assert _foreign_key_shape(fresh, table) == _EXPECTED_FOREIGN_KEYS[table]

    with sqlite3.connect(fresh_database) as connection:
        assert {
            table: connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            for table in _NEW_TABLES
        } == {table: 0 for table in _NEW_TABLES}
    with sqlite3.connect(registry_v19_fixture.registry_path) as connection:
        assert {
            table: connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            for table in _NEW_TABLES
        } == {table: 0 for table in _NEW_TABLES}


def _seed_specification_rows(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(
        "INSERT INTO specification_snapshots VALUES (?, 'fresh', ?)",
        ("a" * 64, sqlite3.Binary(b"snapshot")),
    )
    connection.execute(
        "INSERT INTO specification_members VALUES (?, 'member', ?, 'included', NULL)",
        ("a" * 64, "b" * 64),
    )
    connection.execute(
        "INSERT INTO specification_expected_results VALUES "
        "(?, 'member', 'estimate', 'step', 'output', 'entity')",
        ("a" * 64,),
    )
    connection.execute(
        "INSERT INTO specification_member_attempts "
        "(snapshot_digest, member_key, started_at) VALUES (?, 'member', 'start')",
        ("a" * 64,),
    )
    connection.execute(
        """
        INSERT INTO artifacts (
            origin, context, path, content_digest, file_size, extension,
            source_scope, source_name, source_st_dev, source_st_ino,
            source_st_size, source_st_mtime_ns, source_st_ctime_ns, created_at
        ) VALUES (
            'source', 'fresh', 'data/source/seed.txt', ?, 0, '.txt',
            'global', 'seed', 0, 0, 0, 0, 0, 'now'
        )
        """,
        ("c" * 64,),
    )


@pytest.mark.parametrize(
    ("case", "statement", "parameters"),
    [
        (
            "digest",
            "INSERT INTO specification_snapshots VALUES ('bad', 'fresh', ?)",
            (sqlite3.Binary(b"x"),),
        ),
        (
            "blob",
            "INSERT INTO specification_snapshots VALUES (?, 'fresh', 'text')",
            ("d" * 64,),
        ),
        (
            "included-reason",
            "INSERT INTO specification_members VALUES "
            "(?, 'bad', ?, 'included', 'reason')",
            ("a" * 64, "d" * 64),
        ),
        (
            "excluded-reason",
            "INSERT INTO specification_members VALUES "
            "(?, 'bad', ?, 'excluded', NULL)",
            ("a" * 64, "d" * 64),
        ),
        (
            "duplicate-row",
            "INSERT INTO specification_members VALUES "
            "(?, 'duplicate', ?, 'included', NULL)",
            ("a" * 64, "b" * 64),
        ),
        (
            "missing-manifest-parent",
            "INSERT INTO specification_snapshot_manifest_values VALUES "
            "(?, 'entity_set_v1', ?)",
            ("a" * 64, "d" * 64),
        ),
        (
            "duplicate-port-role",
            "INSERT INTO specification_expected_results VALUES "
            "(?, 'member', 'other', 'step', 'output', 'entity')",
            ("a" * 64,),
        ),
        (
            "terminal-attempt",
            "INSERT INTO specification_member_attempts "
            "(snapshot_digest, member_key, started_at, finished_at, outcome) "
            "VALUES (?, 'member', 'start', 'finish', 'complete')",
            ("a" * 64,),
        ),
        (
            "result-artifact",
            "INSERT INTO specification_attempt_results VALUES "
            "(1, ?, 'member', 'estimate', 'entity', 999)",
            ("a" * 64,),
        ),
        (
            "result-ownership",
            "INSERT INTO specification_attempt_results VALUES "
            "(1, ?, 'wrong', 'estimate', 'entity', 1)",
            ("a" * 64,),
        ),
        (
            "delete-restrict",
            "DELETE FROM specification_snapshots WHERE snapshot_digest = ?",
            ("a" * 64,),
        ),
    ],
)
def test_v19_schema_enforces_representative_constraints(
    tmp_path: Path,
    case: str,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    del case
    _runtime_dir, database = _fresh_v19(tmp_path)
    with sqlite3.connect(database) as connection:
        _seed_specification_rows(connection)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(statement, parameters)


def test_valid_v19_noop_is_read_only_and_creates_no_infrastructure(
    tmp_path: Path,
) -> None:
    runtime_dir, database = _fresh_v19(tmp_path)
    before = database.read_bytes()

    result = migrate_registry_db(
        database,
        context="fresh",
        runtime_root=runtime_dir,
    )

    assert result.status == "already-current"
    assert database.read_bytes() == before
    assert not database.with_name(REGISTRY_V18_BACKUP_FILENAME).exists()
    assert not (runtime_dir / RUNTIME_LOCK_FILENAME).exists()


def test_invalid_v19_noop_fails_without_backup_lock_or_further_mutation(
    tmp_path: Path,
) -> None:
    runtime_dir, database = _fresh_v19(tmp_path)
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX specification_member_attempts_member_idx")
    before = database.read_bytes()

    with pytest.raises(ValidationError, match="exact V19 schema"):
        migrate_registry_db(database, context="fresh", runtime_root=runtime_dir)

    assert database.read_bytes() == before
    assert not database.with_name(REGISTRY_V18_BACKUP_FILENAME).exists()
    assert not (runtime_dir / RUNTIME_LOCK_FILENAME).exists()


def test_missing_live_registry_fails_without_creating_it(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    database = runtime_dir / "database/registry.db"
    database.parent.mkdir(parents=True)

    with pytest.raises(ValidationError, match="must be a real file"):
        migrate_registry_db(database, context="missing", runtime_root=runtime_dir)

    assert not database.exists()
    assert not database.with_name(REGISTRY_V18_BACKUP_FILENAME).exists()
    assert not (runtime_dir / RUNTIME_LOCK_FILENAME).exists()


@pytest.mark.parametrize("escape", [False, True], ids=["inside", "escape"])
def test_live_registry_symlink_is_rejected_without_target_mutation(
    registry_v18_fixture: RegistryV18Fixture,
    escape: bool,
) -> None:
    fixture = registry_v18_fixture
    target = (
        fixture.runtime_dir.parent / "outside-registry.db"
        if escape
        else fixture.registry_path.with_name("target.db")
    )
    shutil.copy2(fixture.registry_path, target)
    target_before = target.read_bytes()
    fixture.registry_path.unlink()
    fixture.registry_path.symlink_to(target)

    with pytest.raises(ValidationError, match="must be a real file"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )

    assert fixture.registry_path.is_symlink()
    assert target.read_bytes() == target_before
    assert not _backup_path(fixture).exists()
    assert not (fixture.runtime_dir / RUNTIME_LOCK_FILENAME).exists()


@pytest.mark.parametrize("escape", [False, True], ids=["inside", "escape"])
def test_database_directory_symlink_is_rejected_without_target_mutation(
    registry_v18_fixture: RegistryV18Fixture,
    escape: bool,
) -> None:
    fixture = registry_v18_fixture
    database_dir = fixture.registry_path.parent
    target_dir = (
        fixture.runtime_dir.parent / "outside-database"
        if escape
        else fixture.runtime_dir / "real-database"
    )
    target_dir.mkdir()
    target_database = target_dir / fixture.registry_path.name
    shutil.copy2(fixture.registry_path, target_database)
    target_before = target_database.read_bytes()
    fixture.registry_path.unlink()
    database_dir.rmdir()
    database_dir.symlink_to(target_dir, target_is_directory=True)

    with pytest.raises(ValidationError, match="database directory must be a real"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )

    assert database_dir.is_symlink()
    assert target_database.read_bytes() == target_before
    assert not target_database.with_name(REGISTRY_V18_BACKUP_FILENAME).exists()
    assert not (fixture.runtime_dir / RUNTIME_LOCK_FILENAME).exists()


def _mutate_v18_preflight_case(database: Path, case: str) -> str:
    with sqlite3.connect(database) as connection:
        if case in {"version-low", "version-high"}:
            version = 17 if case == "version-low" else 20
            connection.execute(f"PRAGMA user_version = {version}")
            return f"found {version}"
        if case == "missing-object":
            connection.execute("DROP INDEX published_outputs_artifact_id_idx")
            return "exact V18 schema"
        if case == "extra-object":
            connection.execute("CREATE TABLE unexpected_object (value TEXT)")
            return "exact V18 schema"
        if case == "altered-column":
            connection.execute("ALTER TABLE contexts ADD COLUMN unexpected TEXT")
            return "exact V18 schema"
        if case == "altered-constraint":
            connection.execute("PRAGMA writable_schema = ON")
            connection.execute(
                """
                UPDATE sqlite_master
                SET sql = replace(
                    sql,
                    'storage_layout_version = 1',
                    'storage_layout_version IN (1)'
                )
                WHERE type = 'table' AND name = 'contexts'
                """
            )
            connection.execute("PRAGMA writable_schema = OFF")
            return "exact V18 schema"
        if case == "foreign-key":
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                """
                INSERT INTO manifest_declarations VALUES (
                    'v18_fixture', 'bad', 'bad.yaml', 'entity_set_v1', ?
                )
                """,
                ("d" * 64,),
            )
            return "foreign-key violations"
        if case == "context":
            connection.execute(
                "UPDATE contexts SET runtime_path = '/wrong' WHERE context = 'v18_fixture'"
            )
            return "context binding is incompatible"
    raise AssertionError(f"unknown preflight case: {case}")


@pytest.mark.parametrize(
    "case",
    [
        "version-low",
        "version-high",
        "missing-object",
        "extra-object",
        "altered-column",
        "altered-constraint",
        "foreign-key",
        "context",
    ],
)
def test_v18_preflight_rejects_unsupported_or_nonexact_inputs_before_backup(
    registry_v18_fixture: RegistryV18Fixture,
    case: str,
) -> None:
    fixture = registry_v18_fixture
    message = _mutate_v18_preflight_case(fixture.registry_path, case)
    before = _logical_state(fixture.registry_path)

    with pytest.raises(ValidationError, match=message):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )

    assert _logical_state(fixture.registry_path) == before
    assert not _backup_path(fixture).exists()


def test_integrity_preflight_failure_creates_no_backup(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    before = _logical_state(fixture.registry_path)

    def reject_integrity(*_args: object, **_kwargs: object) -> None:
        raise ValidationError("registry migration preflight failed integrity_check")

    monkeypatch.setattr(
        registry_module,
        "_validate_migration_preflight",
        reject_integrity,
    )
    with pytest.raises(ValidationError, match="failed integrity_check"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )
    assert _logical_state(fixture.registry_path) == before
    assert not _backup_path(fixture).exists()


@pytest.mark.parametrize(
    "kind",
    ["file", "directory", "symlink", "dangling-symlink"],
)
def test_backup_collision_is_rejected_without_replacing_the_entry(
    registry_v18_fixture: RegistryV18Fixture,
    kind: str,
) -> None:
    fixture = registry_v18_fixture
    backup = _backup_path(fixture)
    if kind == "file":
        backup.write_bytes(b"keep")
    elif kind == "directory":
        backup.mkdir()
    elif kind == "symlink":
        target = backup.with_name("existing-backup-target")
        target.write_bytes(b"target")
        backup.symlink_to(target)
    else:
        backup.symlink_to(backup.with_name("missing-backup-target"))
    before = _logical_state(fixture.registry_path)
    was_symlink = backup.is_symlink()
    link_target = backup.readlink() if was_symlink else None
    file_bytes = backup.read_bytes() if backup.is_file() and not was_symlink else None

    with pytest.raises(ValidationError, match="backup already exists"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )

    assert _logical_state(fixture.registry_path) == before
    assert backup.is_symlink() == was_symlink
    if was_symlink:
        assert backup.readlink() == link_target
    elif file_bytes is not None:
        assert backup.read_bytes() == file_bytes
    else:
        assert backup.is_dir()


def test_backup_and_live_registry_capture_committed_wal_state(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    digest = "d" * 64
    writer = sqlite3.connect(fixture.registry_path)
    try:
        assert writer.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint = 0")
        writer.execute(
            "INSERT INTO manifest_values VALUES ('probe_v1', ?, 'probe', 1)",
            (digest,),
        )
        writer.commit()
        assert fixture.registry_path.with_name(f"{fixture.registry_path.name}-wal").is_file()

        result = migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )
    finally:
        writer.close()

    assert result.backup_path is not None
    for database in (fixture.registry_path, result.backup_path):
        with sqlite3.connect(database) as connection:
            assert connection.execute(
                "SELECT canonical_body FROM manifest_values "
                "WHERE value_schema = 'probe_v1' AND manifest_digest = ?",
                (digest,),
            ).fetchone() == ("probe",)


@pytest.mark.parametrize("failure", ["creation", "validation"])
def test_backup_failure_removes_only_the_new_destination_and_preserves_live_v18(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    fixture = registry_v18_fixture
    backup = _backup_path(fixture)
    before = _logical_state(fixture.registry_path)
    if failure == "creation":

        def fail_creation(
            _source: sqlite3.Connection,
            destination: Path,
        ) -> None:
            destination.write_bytes(b"partial")
            raise OSError("injected copy failure")

        monkeypatch.setattr(
            registry_module,
            "_create_registry_migration_backup",
            fail_creation,
        )
    else:
        original = registry_module._validate_migration_registry_file

        def fail_validation(path: Path, **kwargs: object) -> None:
            if path == backup:
                raise ValidationError("injected backup validation failure")
            original(path, **kwargs)

        monkeypatch.setattr(
            registry_module,
            "_validate_migration_registry_file",
            fail_validation,
        )

    with pytest.raises(ValidationError, match="backup (creation|validation) failed"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )

    assert not backup.exists()
    assert _logical_state(fixture.registry_path) == before
    assert _schema_version(fixture.registry_path) == 18


@pytest.mark.parametrize(
    "checkpoint",
    [
        "after_backup",
        *(f"after_ddl_{index:02d}" for index in range(1, 10)),
        "after_user_version",
        "before_commit",
    ],
)
def test_every_precommit_checkpoint_rolls_back_to_exact_v18(
    registry_v18_fixture: RegistryV18Fixture,
    checkpoint: str,
) -> None:
    fixture = registry_v18_fixture
    before_signature = registry_schema_signature(fixture.registry_path)
    before_state = _logical_state(fixture.registry_path)

    def inject(name: str) -> None:
        if name == checkpoint:
            raise RuntimeError(f"injected {name}")

    with pytest.raises(ValidationError, match="failed before commit"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
            fault_hook=inject,
        )

    backup = _backup_path(fixture)
    assert registry_schema_signature(fixture.registry_path) == before_signature
    assert registry_schema_signature(backup) == before_signature
    assert _logical_state(fixture.registry_path) == before_state
    assert _logical_state(backup) == before_state


def test_after_commit_failure_reports_ambiguity_and_keeps_v19_plus_v18_backup(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    before_signature = registry_schema_signature(fixture.registry_path)
    before_state = _logical_state(fixture.registry_path)

    def inject(name: str) -> None:
        if name == "after_commit":
            raise RuntimeError("injected after commit")

    with pytest.raises(ValidationError, match="committed but completion reporting failed"):
        migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
            fault_hook=inject,
        )

    backup = _backup_path(fixture)
    assert _schema_version(fixture.registry_path) == REGISTRY_SCHEMA_VERSION
    assert registry_schema_signature(backup) == before_signature
    assert _logical_state(backup) == before_state
    rows, sequences = _logical_state(fixture.registry_path)
    assert all(rows[table] == () for table in _NEW_TABLES)
    assert sequences == before_state[1]


def _hold_runtime_lock(
    runtime_root: str,
    ready: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
) -> None:
    with acquire_mutating_runtime_lock(Path(runtime_root)):
        ready.set()
        if not release.wait(timeout=10):
            raise RuntimeError("timed out waiting to release test lock")


def test_runtime_lock_contention_prevents_backup_and_ddl(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    before = _logical_state(fixture.registry_path)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    process = context.Process(
        target=_hold_runtime_lock,
        args=(str(fixture.runtime_dir), ready, release),
    )
    process.start()
    try:
        assert ready.wait(timeout=10)
        with pytest.raises(RuntimeLockUnavailableError, match="already in use"):
            migrate_registry_db(
                fixture.registry_path,
                context=fixture.context,
                runtime_root=fixture.runtime_dir,
            )
    finally:
        release.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)

    assert process.exitcode == 0
    assert _logical_state(fixture.registry_path) == before
    assert not _backup_path(fixture).exists()


def _project_args(fixture: RegistryV18Fixture) -> list[str]:
    return [
        "--project-dir",
        str(fixture.project_dir),
        "--context",
        fixture.context,
    ]


@pytest.mark.parametrize(
    "command",
    [
        "list",
        "steps",
        "plan",
        "graph",
    ],
)
def test_declaration_only_commands_succeed_on_v18_without_registry_open(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    fixture = registry_v18_fixture
    argv = ["workflow", command, *_project_args(fixture)]
    if command == "steps":
        argv += ["--workflow", "base"]
    elif command in {"plan", "graph"}:
        argv += ["--workflow", "base", "--step", "fixture_analysis"]

    def reject(*_args: object, **_kwargs: object) -> object:
        pytest.fail(f"declaration-only command {command} opened the registry")

    monkeypatch.setattr(registry_module, "_connect", reject)
    monkeypatch.setattr(registry_module, "_connect_readonly", reject)

    assert main(argv) == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "command",
    ["validate", "trace", "gui", "run", "dry-run"],
)
def test_registry_dependent_commands_reject_v18_before_any_mutation(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    fixture = registry_v18_fixture
    if command == "validate":
        argv = ["validate", *_project_args(fixture)]
    elif command == "trace":
        argv = [
            "trace",
            *_project_args(fixture),
            "--artifact-id",
            str(fixture.selected_artifact_id),
        ]
    elif command == "gui":
        argv = ["gui", *_project_args(fixture)]
    else:
        argv = [
            "workflow",
            "run",
            *_project_args(fixture),
            "--workflow",
            "base",
            "--step",
            "fixture_analysis",
        ]
        if command == "dry-run":
            argv.append("--dry-run")

    def reject(*_args: object, **_kwargs: object) -> object:
        pytest.fail(f"{command} crossed its V18 rejection boundary")

    monkeypatch.setattr(execution_module, "_run_snakemake", reject)
    monkeypatch.setattr(execution_module, "acquire_mutating_runtime_lock", reject)
    monkeypatch.setattr("uvicorn.run", reject)
    before = _runtime_inventory(fixture.runtime_dir)

    assert main(argv) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f"error: {_MIGRATION_GUIDANCE}\n"
    assert _runtime_inventory(fixture.runtime_dir) == before
    assert not (fixture.runtime_dir / "runs").exists()
    assert not (fixture.runtime_dir / RUNTIME_LOCK_FILENAME).exists()


def test_registry_migrate_resolves_project_from_context_index(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = registry_v18_fixture
    workspace = fixture.project_dir.parent
    (workspace / "nipact.contexts.yaml").write_text(
        "contexts:\n"
        f"  {fixture.context}:\n"
        "    project_dir: project\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(workspace)

    assert main(["registry", "migrate", "--context", fixture.context]) == 0

    output = capsys.readouterr().out.splitlines()
    assert f"context={fixture.context}" in output
    assert "status=migrated" in output
    assert "from_schema=18" in output
    assert "to_schema=19" in output
    assert output[-1] == "PASS: registry migrate"
