from __future__ import annotations

import builtins
import json
import os
import sqlite3
from pathlib import Path

import pytest

import nipact.execution as execution_module
from conftest import (
    RegistryV18Fixture,
    RegistryV19Fixture,
    registry_schema_signature,
)
from nipact.execution import build_run_plan, execute_run_plan
from nipact.hashing import sha256_file_digest
from nipact.project_setup import validate_project
from nipact.registry import (
    REGISTRY_SCHEMA_VERSION,
    REGISTRY_V18_SCHEMA_SIGNATURE_SHA256,
    migrate_registry_db,
    registry_schema_signature as production_registry_schema_signature,
    registry_schema_signature_digest,
)
from nipact.trace import build_trace_graph_for_artifact_id
from registry_v18_fixture_runtime import (
    fixture_analysis_file,
    fixture_source_file,
    fixture_transform_file,
)


_FIXTURE_ROOT = Path(__file__).parent / "fixtures/registry_v18"
_APPLICATION_TABLES = (
    "artifact_dependencies",
    "artifacts",
    "contexts",
    "manifest_declarations",
    "manifest_values",
    "parameters",
    "published_outputs",
    "request_bundle_projections",
    "run_execution_population",
    "run_manifest_bindings",
    "workflow_runs",
)
_MANIFEST_SCHEMA = "entity_set_v1"
_MANIFEST_DIGEST = "ffffea2a8dcf44d4db1f54d3dd36f9755b22c54e04919745e22956bb0b1d8c23"
_SOURCE_DIGEST = "45e8f93b1f72302e7d14f405c7a101472a47af2aa307248b1710afdb348bfef9"


def _application_rows(database: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with sqlite3.connect(database) as connection:
        return {
            table: tuple(
                sorted(
                    connection.execute(f'SELECT * FROM "{table}"').fetchall(),
                    key=repr,
                )
            )
            for table in _APPLICATION_TABLES
        }


def _frozen_output(*, step_name: str, output_name: str) -> Path:
    matches = tuple(
        (_FIXTURE_ROOT / "runtime/outputs/v1/v18_fixture" / step_name).glob(
            f"**/{output_name}/*.json"
        )
    )
    assert len(matches) == 1
    return matches[0]


def test_registry_v18_fixture_callables_produce_frozen_bytes(tmp_path: Path) -> None:
    source_output = tmp_path / "source.json"
    fixture_source_file(
        inputs={
            "source_value": (
                _FIXTURE_ROOT / "runtime/data/source/entity_001.txt",
            )
        },
        outputs={"source_value": source_output},
        params={},
        address="entity_001",
    )
    assert source_output.read_bytes() == _frozen_output(
        step_name="fixture_source",
        output_name="source_value",
    ).read_bytes()

    left_output = tmp_path / "left.json"
    right_output = tmp_path / "right.json"
    fixture_transform_file(
        inputs={"source_value": (source_output,)},
        outputs={"left": left_output, "right": right_output},
        params={"variant": "base"},
        address="entity_001",
    )
    assert left_output.read_bytes() == _frozen_output(
        step_name="fixture_transform",
        output_name="left",
    ).read_bytes()
    assert right_output.read_bytes() == _frozen_output(
        step_name="fixture_transform",
        output_name="right",
    ).read_bytes()

    summary_output = tmp_path / "summary.json"
    fixture_analysis_file(
        inputs={"left_values": (left_output,), "right_values": (right_output,)},
        outputs={"summary": summary_output},
        params={},
        address="cohort",
    )
    assert summary_output.read_bytes() == _frozen_output(
        step_name="fixture_analysis",
        output_name="summary",
    ).read_bytes()


def test_registry_v18_schema_signature_is_non_self_referential(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    expected = json.loads(
        (_FIXTURE_ROOT / "schema-signature.json").read_text(encoding="utf-8")
    )
    assert expected["user_version"] == 18
    assert registry_schema_signature(registry_v18_fixture.registry_path) == expected
    with sqlite3.connect(registry_v18_fixture.registry_path) as connection:
        production = production_registry_schema_signature(connection)
    assert production == expected
    assert registry_schema_signature_digest(production) == (
        REGISTRY_V18_SCHEMA_SIGNATURE_SHA256
    )


def test_registry_v18_fixture_has_complete_rows_and_files(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 18
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in _APPLICATION_TABLES
        }
        rows = connection.execute(
            """
            SELECT origin, path, content_digest, file_size
            FROM artifacts
            ORDER BY artifact_id
            """
        ).fetchall()
        run_plan_path, run_plan_digest = connection.execute(
            "SELECT run_plan_path, run_plan_digest FROM workflow_runs"
        ).fetchone()
    assert all(count > 0 for count in counts.values())
    assert counts == {
        "artifact_dependencies": 5,
        "artifacts": 5,
        "contexts": 1,
        "manifest_declarations": 1,
        "manifest_values": 1,
        "parameters": 3,
        "published_outputs": 4,
        "request_bundle_projections": 3,
        "run_execution_population": 1,
        "run_manifest_bindings": 1,
        "workflow_runs": 1,
    }
    for origin, relative_path, digest, file_size in rows:
        path = fixture.runtime_dir / str(relative_path)
        assert path.is_file()
        assert path.stat().st_size == file_size
        assert sha256_file_digest(path) == digest
        if origin == "workflow_output":
            assert path.is_relative_to(fixture.runtime_dir / "outputs/v1")
    assert run_plan_digest
    assert not (fixture.runtime_dir / str(run_plan_path)).exists()


def test_migrated_registry_preserves_current_readers_and_trace(
    registry_v19_fixture: RegistryV19Fixture,
) -> None:
    fixture = registry_v19_fixture
    result = validate_project(project_dir=fixture.project_dir, context=fixture.context)
    assert (result.manifest_count, result.workflow_count, result.step_count) == (1, 2, 3)
    assert (result.source_entities, result.published_outputs) == (1, 4)
    graph = build_trace_graph_for_artifact_id(
        fixture.registry_path,
        artifact_id=fixture.selected_artifact_id,
        context=fixture.context,
    )
    assert graph["provenance_status"] == "complete"
    assert graph["warnings"] == []
    assert {row["artifact_id"] for row in graph["artifacts"]} == {1, 2, 3, 4, 5}
    assert len(graph["dependencies"]) == 5
    sources = [row for row in graph["artifacts"] if row["origin"] == "source"]
    assert [
        (
            row["source_scope"],
            row["source_name"],
            row["source_entity_id"],
            row["content_digest"],
        )
        for row in sources
    ] == [
        ("entity", "source_value", "entity_001", _SOURCE_DIGEST)
    ]
    dependencies = {
        (row["source_artifact_id"], row["dependent_artifact_id"], row["binding_name"]): row
        for row in graph["dependencies"]
    }
    direct_source = dependencies[(1, 2, "source_value")]
    assert direct_source["dependency_role"] == "source_input"
    assert direct_source["source_content_digest"] == _SOURCE_DIGEST
    assert direct_source["source_occurrence_path"] == "data/source/entity_001.txt"
    assert direct_source["source_scope"] == "entity"
    assert direct_source["source_name"] == "source_value"
    assert direct_source["source_entity_id"] == "entity_001"
    assert {
        (row["source_artifact_id"], row["dependent_artifact_id"], row["dependency_role"])
        for row in graph["dependencies"]
    } == {
        (1, 2, "source_input"),
        (2, 3, "source_input"),
        (2, 4, "source_input"),
        (3, 5, "analysis_input"),
        (4, 5, "analysis_input"),
    }
    for key in ((3, 5, "left_values"), (4, 5, "right_values")):
        assert dependencies[key]["manifest_value_schema"] == _MANIFEST_SCHEMA
        assert dependencies[key]["manifest_digest"] == _MANIFEST_DIGEST
        assert dependencies[key]["edge_cardinality"] == 1
    assert graph["execution_populations"] == [
        {
            "run_id": 1,
            "workflow_name": "base",
            "manifest_name": "cohort",
            "manifest_value_schema": _MANIFEST_SCHEMA,
            "manifest_digest": _MANIFEST_DIGEST,
            "manifest_hash": _MANIFEST_DIGEST[:16],
            "entity_count": 1,
        }
    ]
    assert graph["manifest_bindings"] == [
        {
            "run_id": 1,
            "workflow_name": "base",
            "step_name": "fixture_analysis",
            "manifest_usage_role": "analysis_population",
            "manifest_name": "cohort",
            "manifest_value_schema": _MANIFEST_SCHEMA,
            "manifest_digest": _MANIFEST_DIGEST,
            "manifest_hash": _MANIFEST_DIGEST[:16],
            "entity_count": 1,
        }
    ]


def _scientific_file_bytes(runtime_dir: Path) -> dict[Path, bytes]:
    return {
        path.relative_to(runtime_dir): path.read_bytes()
        for root in (runtime_dir / "data", runtime_dir / "outputs")
        for path in root.rglob("*")
        if path.is_file()
    }


def _sqlite_sequence(database: Path) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(database) as connection:
        return tuple(connection.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name"))


def test_registry_migration_preserves_rows_sequences_and_scientific_files(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    rows_before = _application_rows(fixture.registry_path)
    sequences_before = _sqlite_sequence(fixture.registry_path)
    files_before = _scientific_file_bytes(fixture.runtime_dir)
    protected = {
        (fixture.runtime_dir / relative).resolve() for relative in files_before
    }

    def check(value: object) -> None:
        if isinstance(value, int):
            return
        try:
            candidate = Path(os.path.abspath(os.fspath(value)))
        except TypeError:
            return
        if candidate in protected:
            pytest.fail(f"migration opened scientific file: {candidate}")

    original_path_open = Path.open
    original_open = builtins.open

    def guarded_path_open(self: Path, *args: object, **kwargs: object) -> object:
        check(self)
        return original_path_open(self, *args, **kwargs)

    def guarded_open(file: object, *args: object, **kwargs: object) -> object:
        check(file)
        return original_open(file, *args, **kwargs)

    with monkeypatch.context() as guard:
        guard.setattr(Path, "open", guarded_path_open)
        guard.setattr(builtins, "open", guarded_open)
        result = migrate_registry_db(
            fixture.registry_path,
            context=fixture.context,
            runtime_root=fixture.runtime_dir,
        )

    assert result.status == "migrated"
    assert result.backup_path is not None
    assert _application_rows(fixture.registry_path) == rows_before
    assert _application_rows(result.backup_path) == rows_before
    assert _sqlite_sequence(fixture.registry_path) == sequences_before
    assert _sqlite_sequence(result.backup_path) == sequences_before
    assert _scientific_file_bytes(fixture.runtime_dir) == files_before
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (
            REGISTRY_SCHEMA_VERSION,
        )
        assert all(
            connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] == 0
            for table in (
                "specification_snapshots",
                "specification_members",
                "specification_snapshot_manifest_values",
                "specification_expected_results",
                "specification_member_attempts",
                "specification_attempt_results",
            )
        )


def test_migrated_registry_preserves_reuse_and_parameter_divergence(
    registry_v19_fixture: RegistryV19Fixture,
) -> None:
    fixture = registry_v19_fixture
    base = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
    )
    assert base.selected_fresh_output_refs == ()
    assert len(base.selected_reused_output_refs) == 1
    assert base.selected_reused_output_refs[0].planned_sibling_artifact_ids == (
        ("summary", fixture.selected_artifact_id),
    )

    sibling = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_transform",
        address="entity_001",
    )
    assert sibling.selected_fresh_output_refs == ()
    assert sibling.selected_reused_output_refs[0].planned_sibling_artifact_ids == (
        ("left", 3),
        ("right", 4),
    )

    variant = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="parameter-variant",
        step_name="fixture_analysis",
    )
    assert {job.step_name for job in variant.jobs} == {
        "fixture_transform",
        "fixture_analysis",
    }
    assert {
        (output.step_name, output.output_name, output.address)
        for output in variant.reused_outputs
    } == {("fixture_source", "source_value", "entity_001")}
    assert variant.selected_reused_output_refs == ()


def test_migrated_registry_reuse_only_execution_records_selection_not_generation(
    registry_v19_fixture: RegistryV19Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v19_fixture
    before = _application_rows(fixture.registry_path)
    before_publications = before["published_outputs"]
    source_before = before["artifacts"][0]

    def reject_snakemake(*_args: object, **_kwargs: object) -> int:
        pytest.fail("reuse-only execution invoked Snakemake")

    monkeypatch.setattr(execution_module, "_run_snakemake", reject_snakemake)
    outcome = execute_run_plan(
        build_run_plan(
            project_dir=fixture.project_dir,
            context=fixture.context,
            workflow_name="base",
            step_name="fixture_analysis",
        ),
        cores=1,
    )
    after = _application_rows(fixture.registry_path)
    with sqlite3.connect(fixture.registry_path) as connection:
        new_run = connection.execute(
            """
            SELECT run_id, context, workflow_name, selected_step_name,
                   selected_output_name, base_workflow_name,
                   resolution_summary_json, is_current
            FROM workflow_runs
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        new_population = connection.execute(
            """
            SELECT run_id, manifest_name, manifest_value_schema, manifest_digest
            FROM run_execution_population
            WHERE run_id = ?
            """,
            (new_run[0],),
        ).fetchone()
        new_binding = connection.execute(
            """
            SELECT run_id, step_name, manifest_usage_role, manifest_name,
                   manifest_value_schema, manifest_digest
            FROM run_manifest_bindings
            WHERE run_id = ?
            """,
            (new_run[0],),
        ).fetchone()

    assert outcome.all_selected_resolved
    assert outcome.selected_generated_count == 0
    assert outcome.selected_reused_count == 1
    for table in (
        "artifact_dependencies",
        "parameters",
        "request_bundle_projections",
    ):
        assert after[table] == before[table]
    assert len(after["artifacts"]) == len(before["artifacts"])
    assert after["artifacts"][1:] == before["artifacts"][1:]
    assert after["artifacts"][0][:-1] == source_before[:-1]
    assert after["published_outputs"] == before_publications
    assert len(after["workflow_runs"]) == 2
    assert len(after["run_execution_population"]) == 2
    assert len(after["run_manifest_bindings"]) == 2
    assert [row[11] for row in after["workflow_runs"]] == [0, 1]
    assert new_run[:6] == (
        2,
        "v18_fixture",
        "base",
        "fixture_analysis",
        "summary",
        None,
    )
    assert json.loads(new_run[6]) == {
        "all_selected_resolved": True,
        "forced": False,
        "schema_version": 1,
        "selected_outputs": [
            {
                "address": "cohort",
                "context": "v18_fixture",
                "output_name": "summary",
                "resolution": {"artifact_id": 5, "outcome": "reused"},
                "step_name": "fixture_analysis",
                "workflow_name": "base",
            }
        ],
    }
    assert new_run[7] == 1
    assert new_population == (2, "cohort", _MANIFEST_SCHEMA, _MANIFEST_DIGEST)
    assert new_binding == (
        2,
        "fixture_analysis",
        "analysis_population",
        "cohort",
        _MANIFEST_SCHEMA,
        _MANIFEST_DIGEST,
    )


def _assert_autoincrement_state_allocates_above_frozen_sequences(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        sequences = dict(connection.execute("SELECT name, seq FROM sqlite_sequence"))
        assert sequences == {"artifacts": 5, "workflow_runs": 1, "parameters": 4}
        counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in ("artifacts", "workflow_runs", "parameters")
        }
        assert counts == {"artifacts": 5, "workflow_runs": 1, "parameters": 3}
        connection.execute("SAVEPOINT probes")
        run_id = connection.execute(
            """
            INSERT INTO workflow_runs (
                context, workflow_name, selected_step_name, selected_output_name,
                run_workspace, run_plan_path, run_plan_digest,
                resolution_summary_json, environment_observation_json,
                is_current, created_at
            ) VALUES (
                'v18_fixture', 'probe', 'probe', 'probe', 'runs/probe',
                'runs/probe/run_plan.json', ?, '{}', '{}', 0,
                '2026-08-13T00:00:00+00:00'
            )
            """,
            ("a" * 64,),
        ).lastrowid
        parameter_id = connection.execute(
            """
            INSERT INTO parameters (
                hash_version, parameter_hash, parameter_digest, step_name,
                parameters_json, created_at
            ) VALUES (1, ?, ?, 'probe', '{}', '2026-08-13T00:00:00+00:00')
            """,
            ("b" * 16, "b" * 64),
        ).lastrowid
        artifact_id = connection.execute(
            """
            INSERT INTO artifacts (
                origin, context, path, content_digest, file_size, extension,
                source_scope, source_name, source_st_dev, source_st_ino,
                source_st_size, source_st_mtime_ns, source_st_ctime_ns, created_at
            ) VALUES (
                'source', 'v18_fixture', 'data/source/probe.txt', ?, 0, '.txt',
                'global', 'probe', 0, 0, 0, 0, 0,
                '2026-08-13T00:00:00+00:00'
            )
            """,
            ("c" * 64,),
        ).lastrowid
        assert run_id is not None and run_id > sequences["workflow_runs"]
        assert parameter_id is not None and parameter_id > sequences["parameters"]
        assert artifact_id is not None and artifact_id > sequences["artifacts"]
        connection.execute("ROLLBACK TO probes")
        connection.execute("RELEASE probes")
        assert {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in ("artifacts", "workflow_runs", "parameters")
        } == counts
        assert dict(connection.execute("SELECT name, seq FROM sqlite_sequence")) == sequences


def test_registry_v18_autoincrement_state_allocates_above_frozen_sequences(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    _assert_autoincrement_state_allocates_above_frozen_sequences(
        registry_v18_fixture.registry_path
    )


def test_migrated_registry_autoincrement_state_allocates_above_frozen_sequences(
    registry_v19_fixture: RegistryV19Fixture,
) -> None:
    _assert_autoincrement_state_allocates_above_frozen_sequences(
        registry_v19_fixture.registry_path
    )
