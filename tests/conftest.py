"""Shared fixtures for the test suite.

``colors_registry`` builds a real ``colors`` demo registry (init → plan → execute
with a stubbed Snakemake) and yields the paths plus a concrete published
artifact id. Route/service tests that need a genuine ``build_trace_graph()``
input use it instead of rebuilding the recipe inline.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from nipact.cli import main
from nipact.execution import build_run_plan, execute_run_plan
from nipact.execution_evidence import CompletionReceipt, write_completion_receipt_atomic
from nipact.registry import (
    REGISTRY_DB_PATH,
    REGISTRY_SCHEMA_VERSION,
    REGISTRY_V18_BACKUP_FILENAME,
    list_artifacts,
    migrate_registry_db,
)


@dataclass(frozen=True)
class ColorsRegistry:
    project_dir: Path
    runtime_dir: Path
    registry_path: Path
    context: str
    root_artifact_id: int


@dataclass(frozen=True)
class RegistryV18Fixture:
    project_dir: Path
    runtime_dir: Path
    registry_path: Path
    context: str
    selected_artifact_id: int


@dataclass(frozen=True)
class RegistryV19Fixture:
    project_dir: Path
    runtime_dir: Path
    registry_path: Path
    context: str
    selected_artifact_id: int
    backup_path: Path


def _normalize_schema_sql(value: str) -> str:
    return " ".join(value.split())


def _pragma_rows(
    connection: sqlite3.Connection,
    statement: str,
) -> list[dict[str, object]]:
    return [dict(row) for row in connection.execute(statement)]


def _index_signatures(
    connection: sqlite3.Connection,
    *,
    table_name: str,
) -> list[dict[str, object]]:
    indexes: list[dict[str, object]] = []
    for row in _pragma_rows(connection, f'PRAGMA index_list("{table_name}")'):
        index_name = str(row["name"])
        indexes.append(
            {
                "name": index_name,
                "unique": row["unique"],
                "origin": row["origin"],
                "partial": row["partial"],
                "xinfo": sorted(
                    _pragma_rows(
                        connection,
                        f'PRAGMA index_xinfo("{index_name}")',
                    ),
                    key=lambda item: int(item["seqno"]),
                ),
            }
        )
    return sorted(indexes, key=lambda item: str(item["name"]))


def _foreign_key_signatures(
    connection: sqlite3.Connection,
    *,
    table_name: str,
) -> list[dict[str, object]]:
    grouped: dict[int, list[dict[str, object]]] = {}
    for row in _pragma_rows(
        connection,
        f'PRAGMA foreign_key_list("{table_name}")',
    ):
        grouped.setdefault(int(row["id"]), []).append(row)

    foreign_keys: list[dict[str, object]] = []
    for rows in grouped.values():
        ordered = sorted(rows, key=lambda item: int(item["seq"]))
        first = ordered[0]
        foreign_keys.append(
            {
                "table": first["table"],
                "on_update": first["on_update"],
                "on_delete": first["on_delete"],
                "match": first["match"],
                "columns": [
                    {
                        "sequence": row["seq"],
                        "from": row["from"],
                        "to": row["to"],
                    }
                    for row in ordered
                ],
            }
        )
    return sorted(
        foreign_keys,
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )


def registry_schema_signature(database: Path) -> dict[str, object]:
    """Return the independent test-side physical schema signature."""
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        objects = _pragma_rows(
            connection,
            """
            SELECT type, name, tbl_name
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """,
        )
        tables: dict[str, object] = {}
        for table_name in sorted(
            str(obj["name"]) for obj in objects if obj["type"] == "table"
        ):
            tables[table_name] = {
                "columns": _pragma_rows(
                    connection,
                    f'PRAGMA table_xinfo("{table_name}")',
                ),
                "foreign_keys": _foreign_key_signatures(
                    connection,
                    table_name=table_name,
                ),
                "indexes": _index_signatures(
                    connection,
                    table_name=table_name,
                ),
            }
        sql = {
            str(row["name"]): _normalize_schema_sql(str(row["sql"]))
            for row in connection.execute(
                """
                SELECT name, sql
                FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                  AND type IN ('table', 'index')
                  AND sql IS NOT NULL
                ORDER BY name
                """
            )
        }
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    return {
        "objects": objects,
        "sql": sql,
        "tables": tables,
        "user_version": user_version,
    }


@pytest.fixture
def registry_v18_fixture(tmp_path: Path) -> RegistryV18Fixture:
    """Install the static V18 compatibility fixture without production creators."""
    fixture_root = Path(__file__).parent / "fixtures/registry_v18"
    project_dir = tmp_path / "project"
    runtime_dir = tmp_path / "runtime"
    shutil.copytree(fixture_root / "project", project_dir)
    shutil.copytree(fixture_root / "runtime", runtime_dir)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    registry_path.parent.mkdir(parents=True)

    with sqlite3.connect(registry_path) as connection:
        connection.executescript(
            (fixture_root / "populated-registry.sql").read_text(encoding="utf-8")
        )

    source_path = runtime_dir / "data/source/entity_001.txt"
    source_stat = source_path.stat()
    with sqlite3.connect(registry_path) as connection:
        runtime_update = connection.execute(
            "UPDATE contexts SET runtime_path = ? WHERE context = ?",
            (str(runtime_dir.resolve()), "v18_fixture"),
        )
        source_update = connection.execute(
            """
            UPDATE artifacts
            SET source_st_dev = ?, source_st_ino = ?, source_st_size = ?,
                source_st_mtime_ns = ?, source_st_ctime_ns = ?
            WHERE context = ?
              AND origin = 'source'
              AND source_scope = 'entity'
              AND source_name = 'source_value'
              AND source_entity_id = 'entity_001'
            """,
            (
                source_stat.st_dev,
                source_stat.st_ino,
                source_stat.st_size,
                source_stat.st_mtime_ns,
                source_stat.st_ctime_ns,
                "v18_fixture",
            ),
        )
        assert runtime_update.rowcount == 1
        assert source_update.rowcount == 1

    return RegistryV18Fixture(
        project_dir=project_dir,
        runtime_dir=runtime_dir,
        registry_path=registry_path,
        context="v18_fixture",
        selected_artifact_id=5,
    )


@pytest.fixture
def registry_v19_fixture(
    registry_v18_fixture: RegistryV18Fixture,
) -> RegistryV19Fixture:
    """Migrate a copied static V18 fixture through the production migration."""
    fixture = registry_v18_fixture
    result = migrate_registry_db(
        fixture.registry_path,
        context=fixture.context,
        runtime_root=fixture.runtime_dir,
    )
    backup_path = fixture.registry_path.with_name(REGISTRY_V18_BACKUP_FILENAME)
    assert result.status == "migrated"
    assert result.from_schema == 18
    assert result.to_schema == REGISTRY_SCHEMA_VERSION
    assert result.backup_path == backup_path
    return RegistryV19Fixture(
        project_dir=fixture.project_dir,
        runtime_dir=fixture.runtime_dir,
        registry_path=fixture.registry_path,
        context=fixture.context,
        selected_artifact_id=fixture.selected_artifact_id,
        backup_path=backup_path,
    )


def _run_main_from(cwd: Path, argv: list[str]) -> int:
    old_cwd = Path.cwd()
    os.chdir(cwd)
    try:
        return main(argv)
    finally:
        os.chdir(old_cwd)


def _write_all_staged_outputs(run_plan: object) -> None:
    selected_keys = {
        (job.step_name, job.output_name, job.address)
        for job in run_plan.selected_fresh_jobs
    }
    for job in run_plan.jobs:
        job.staging_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "job_id": job.job_id,
            "step_name": job.step_name,
            "output_name": job.output_name,
            "address": job.address,
        }
        if (job.step_name, job.output_name, job.address) in selected_keys:
            payload["selected"] = True
        job.staging_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    execution_payload = json.loads(
        (run_plan.run_workspace / "run_plan.json").read_text(encoding="utf-8")
    )
    for job_id, job_payload in execution_payload["jobs"].items():
        write_completion_receipt_atomic(
            run_plan.run_workspace / job_payload["completion_receipt_path"],
            CompletionReceipt(
                invocation_token=execution_payload["invocation_token"],
                job_id=job_id,
                request_bundle_digest=job_payload["request_bundle_digest"],
                outputs=tuple(job_payload["declared_outputs"]),
            ),
        )


@pytest.fixture
def colors_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> ColorsRegistry:
    project_dir = tmp_path / "project"
    runtime_dir = tmp_path / "runtime"
    assert (
        _run_main_from(
            tmp_path,
            [
                "init",
                "--demo",
                "colors",
                "--project-dir",
                "project",
                "--runtime-dir",
                "runtime",
                "--context",
                "colors",
            ],
        )
        == 0
    )

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )
    def write_staged_outputs(*_args: object, **_kwargs: object) -> int:
        _write_all_staged_outputs(run_plan)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", write_staged_outputs)
    execute_run_plan(run_plan, cores=1)

    registry_path = runtime_dir / REGISTRY_DB_PATH
    selected = list_artifacts(
        registry_path,
        context="colors",
        origin="workflow_output",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        is_published=True,
    )[0]
    return ColorsRegistry(
        project_dir=project_dir,
        runtime_dir=runtime_dir,
        registry_path=registry_path,
        context="colors",
        root_artifact_id=selected.artifact_id,
    )
