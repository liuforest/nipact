import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest

import nipact.registry as registry
from nipact.cli import main
from nipact.errors import ValidationError
from nipact.execution import build_run_plan, execute_run_plan
from nipact.registry import (
    REGISTRY_DB_PATH,
    _open_registry_read_session,
    list_artifact_group_counts,
    list_artifacts,
    list_manifests,
    list_run_manifest_bindings,
    list_upstream_dependencies,
    read_artifact_by_id,
    read_artifact_by_id_for_context,
    read_artifact_by_path,
    read_current_published_artifact,
    read_manifest,
    read_published_outputs,
    read_registry_summary,
    resolve_registered_artifact_path,
)


def _run_main_from(cwd: Path, argv: list[str]) -> int:
    old_cwd = Path.cwd()
    os.chdir(cwd)
    try:
        return main(argv)
    finally:
        os.chdir(old_cwd)


def _init_demo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> tuple[Path, Path]:
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
    capsys.readouterr()
    return project_dir, runtime_dir


def _write_all_staged_outputs(run_plan: object) -> None:
    selected_keys = {
        (job.step_name, job.output_name, job.address)
        for job in run_plan.selected_jobs
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


def _successful_sector_run(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, object]:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
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
    assert execute_run_plan(run_plan, cores=1).published_count == len(run_plan.published_outputs)
    return project_dir, runtime_dir, run_plan


def test_registry_reads_initialized_source_artifact(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    source_artifacts = list_artifacts(registry_path, context="colors", origin="source")

    assert len(source_artifacts) == 1
    source = source_artifacts[0]
    assert source.origin == "source"
    assert source.path == "data/color_source.json"
    assert source.run_id is None
    assert source.source_metadata is not None
    assert source.source_metadata["entity_count"] == 200
    assert read_artifact_by_id(registry_path, source.artifact_id) == source
    assert (
        read_artifact_by_path(
            registry_path,
            context="colors",
            artifact_path="data/color_source.json",
        )
        == source
    )

    with pytest.raises(ValidationError, match="unknown registered artifact path"):
        read_artifact_by_path(
            registry_path,
            context="colors",
            artifact_path="data/not-present.json",
        )
    with pytest.raises(ValidationError, match="relative to runtime dir"):
        read_artifact_by_path(
            registry_path,
            context="colors",
            artifact_path=str(runtime_dir / "data/color_source.json"),
        )
    with pytest.raises(ValidationError, match="stay inside runtime dir"):
        read_artifact_by_path(
            registry_path,
            context="colors",
            artifact_path="data/../database/registry.db",
        )


def test_registry_manifest_helpers_and_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    manifests = list_manifests(registry_path, context="colors")
    manifest = read_manifest(registry_path, context="colors", manifest_name="init")
    summary = read_registry_summary(registry_path, context="colors")

    assert {row.name for row in manifests} >= {"init"}
    assert manifest.name == "init"
    assert manifest.manifest_body.startswith("color_000")
    assert manifest.entity_count == 200
    assert summary == {
        "manifest_count": len(manifests),
        "artifact_count": 1,
        "source_artifact_count": 1,
        "workflow_output_count": 0,
        "workflow_run_count": 0,
    }
    with pytest.raises(ValidationError, match="unknown manifest"):
        read_manifest(registry_path, context="colors", manifest_name="missing")
    with pytest.raises(ValidationError, match="unknown context"):
        read_registry_summary(registry_path, context="missing")


def test_registry_path_lookup_rejects_duplicate_rows(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    duplicate_path = "runs/colors/base/manual/staging/example/output/init.json"

    with sqlite3.connect(registry_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        for index in range(2):
            conn.execute(
                """
                INSERT INTO artifacts (
                    origin, context, workflow_name, step_name, output_name,
                    address, job_id, path, content_digest, output_hash,
                    file_size, extension, created_at
                )
                VALUES (
                    'workflow_output', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    "colors",
                    "base",
                    f"manual_step_{index}",
                    "output",
                    "init",
                    f"manual_job_{index}",
                    duplicate_path,
                    str(index) * 64,
                    str(index) * 16,
                    index,
                    ".json",
                    "2026-06-03T00:00:00+00:00",
                ),
            )

    with pytest.raises(ValidationError, match="ambiguous registered artifact path"):
        read_artifact_by_path(
            registry_path,
            context="colors",
            artifact_path=duplicate_path,
        )


def test_registry_reads_workflow_output_and_neighbors(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH

    selected = read_current_published_artifact(
        registry_path,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="init",
    )

    assert selected.is_selected_output is True
    assert selected.is_published is True
    assert selected.run_id is not None
    assert selected.parameter_hash is not None
    assert selected.parameters_json is not None
    assert set(json.loads(selected.parameters_json)) == {"arc_half_width", "min_radius"}
    assert read_artifact_by_id(registry_path, selected.artifact_id) == selected
    assert (
        read_artifact_by_path(
            registry_path,
            context="colors",
            artifact_path=selected.path,
        )
        == selected
    )
    assert (
        read_artifact_by_id_for_context(
            registry_path,
            context="colors",
            artifact_id=selected.artifact_id,
        )
        == selected
    )
    assert (
        resolve_registered_artifact_path(
            registry_path,
            context="colors",
            artifact_path=selected.staging_path,
        )
        == selected
    )

    published_lookup = read_published_outputs(
        runtime_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
    )
    workflow_artifacts = list_artifacts(
        registry_path,
        context="colors",
        origin="workflow_output",
        workflow_name="base",
    )
    published_artifacts = list_artifacts(
        registry_path,
        context="colors",
        origin="workflow_output",
        is_published=True,
    )
    upstream_edges = list_upstream_dependencies(
        registry_path,
        artifact_id=selected.artifact_id,
    )
    manifest_bindings = list_run_manifest_bindings(
        registry_path,
        run_id=selected.run_id,
    )

    assert {row["address"] for row in published_lookup} == {"init"}
    assert published_lookup[0]["path"] == selected.published_path
    assert len(workflow_artifacts) == len(run_plan.jobs)
    assert selected in published_artifacts
    assert len(published_artifacts) == len(run_plan.published_outputs)
    assert len(upstream_edges) == 200
    assert {edge.binding_name for edge in upstream_edges} == {"sector_label"}
    assert len(manifest_bindings) == len(run_plan.manifest_bindings)
    assert {binding.context for binding in manifest_bindings} == {"colors"}


def test_list_artifact_group_counts_matches_list_artifacts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH

    groups = list_artifact_group_counts(registry_path, context="colors")

    # Every group's count equals the number of rows list_artifacts returns for
    # the same coordinate, so the summary can't drift from the list.
    assert sum(group.artifact_count for group in groups) == len(
        list_artifacts(registry_path, context="colors")
    )
    for group in groups:
        assert group.artifact_count == len(
            list_artifacts(
                registry_path,
                context="colors",
                origin=group.origin,
                workflow_name=group.workflow_name,
                step_name=group.step_name,
                output_name=group.output_name,
            )
        )

    # The source group preserves its null coordinates rather than a sentinel.
    source_groups = [group for group in groups if group.origin == "source"]
    assert len(source_groups) == 1
    assert source_groups[0].workflow_name is None
    assert source_groups[0].step_name is None
    assert source_groups[0].output_name is None

    # Filters narrow the grouped population exactly as they narrow the list.
    filtered = list_artifact_group_counts(
        registry_path,
        context="colors",
        step_name="color_sector_analysis",
    )
    assert {group.step_name for group in filtered} == {"color_sector_analysis"}
    assert 0 < sum(group.artifact_count for group in filtered) < sum(
        group.artifact_count for group in groups
    )


def test_registry_context_safe_artifact_lookup_hides_foreign_contexts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    with sqlite3.connect(registry_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO contexts (context, runtime_path) VALUES (?, ?)",
            ("other", str(runtime_dir)),
        )
        cursor = conn.execute(
            """
            INSERT INTO artifacts (
                origin, context, path, content_digest, output_hash, file_size,
                extension, created_at
            )
            VALUES ('source', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "other",
                "data/other/source.json",
                "b" * 64,
                "b" * 16,
                1,
                ".json",
                "2026-06-04T00:00:00+00:00",
            ),
        )
        artifact_id = int(cursor.lastrowid)

    assert read_artifact_by_id(registry_path, artifact_id).context == "other"
    with pytest.raises(ValidationError, match="unknown registry artifact id"):
        read_artifact_by_id_for_context(
            registry_path,
            context="colors",
            artifact_id=artifact_id,
        )


def test_registry_reads_reject_unknown_ids_runs_and_schema(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    with pytest.raises(ValidationError, match="positive integer"):
        read_artifact_by_id(registry_path, 0)
    with pytest.raises(ValidationError, match="unknown registry artifact id"):
        read_artifact_by_id(registry_path, 999)
    with pytest.raises(ValidationError, match="unknown registry artifact id"):
        list_upstream_dependencies(registry_path, artifact_id=999)
    with pytest.raises(ValidationError, match="unknown workflow run id"):
        list_run_manifest_bindings(registry_path, run_id=999)
    with pytest.raises(ValidationError, match="unknown current published artifact"):
        read_current_published_artifact(
            registry_path,
            context="colors",
            workflow_name="base",
            step_name="color_sector_analysis",
            output_name="sector_counts",
            address="init",
        )

    incompatible_runtime = tmp_path / "incompatible"
    (incompatible_runtime / "database").mkdir(parents=True)
    incompatible_path = incompatible_runtime / REGISTRY_DB_PATH
    with sqlite3.connect(incompatible_path) as conn:
        conn.execute("PRAGMA user_version = 1")

    with pytest.raises(ValidationError, match="schema version is incompatible"):
        read_published_outputs(
            incompatible_runtime,
            context="colors",
            workflow_name="base",
            step_name="step",
            output_name="out",
        )


def test_registry_reads_translate_schema_read_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    def raise_schema_read_error(conn: sqlite3.Connection) -> None:
        raise sqlite3.DatabaseError("database disk image is malformed")

    monkeypatch.setattr(registry, "_validate_schema_version", raise_schema_read_error)

    with pytest.raises(ValidationError, match="registry.db is malformed"):
        read_artifact_by_id(registry_path, 1)
    with pytest.raises(ValidationError, match="registry.db is malformed"):
        list_upstream_dependencies(registry_path, artifact_id=1)
    with pytest.raises(ValidationError, match="registry.db is malformed"):
        list_run_manifest_bindings(registry_path, run_id=1)
    with pytest.raises(ValidationError, match="registry.db is malformed"):
        with _open_registry_read_session(registry_path):
            pass


def test_read_session_reads_match_public_helpers(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH
    selected = read_current_published_artifact(
        registry_path,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="init",
    )

    with _open_registry_read_session(registry_path) as session:
        assert session.read_artifact_by_id(
            selected.artifact_id
        ) == read_artifact_by_id(registry_path, selected.artifact_id)
        assert session.list_upstream_dependencies(
            artifact_id=selected.artifact_id
        ) == list_upstream_dependencies(
            registry_path,
            artifact_id=selected.artifact_id,
        )
        assert session.list_run_manifest_bindings(
            run_id=selected.run_id,
            context="colors",
        ) == list_run_manifest_bindings(
            registry_path,
            run_id=selected.run_id,
            context="colors",
        )


def test_read_session_opens_one_connection_and_validates_once(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH
    selected = read_current_published_artifact(
        registry_path,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="init",
    )
    upstream = list_upstream_dependencies(
        registry_path,
        artifact_id=selected.artifact_id,
    )
    assert len(upstream) > 1

    connect_calls: list[Path] = []
    validate_in_transaction: list[bool] = []
    real_connect = registry._connect_readonly_rows
    real_validate = registry._validate_schema_version

    @contextmanager
    def counting_connect(path: Path):
        connect_calls.append(path)
        with real_connect(path) as conn:
            yield conn

    def counting_validate(conn: sqlite3.Connection) -> None:
        validate_in_transaction.append(conn.in_transaction)
        real_validate(conn)

    monkeypatch.setattr(registry, "_connect_readonly_rows", counting_connect)
    monkeypatch.setattr(registry, "_validate_schema_version", counting_validate)

    with _open_registry_read_session(registry_path) as session:
        opened_conn = session._conn
        assert opened_conn.in_transaction is True
        session.read_artifact_by_id(selected.artifact_id)
        for edge in upstream:
            session.read_artifact_by_id(edge.source_artifact_id)
            session.list_upstream_dependencies(artifact_id=edge.source_artifact_id)
        session.list_run_manifest_bindings(run_id=selected.run_id, context="colors")

    # One connection and one schema validation regardless of how many hops ran.
    assert connect_calls == [registry_path]
    assert validate_in_transaction == [True]
    with pytest.raises(sqlite3.ProgrammingError):
        opened_conn.execute("SELECT 1")


def test_read_session_reports_unknown_ids_within_session(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    with _open_registry_read_session(registry_path) as session:
        with pytest.raises(ValidationError, match="positive integer"):
            session.read_artifact_by_id(0)
        with pytest.raises(ValidationError, match="unknown registry artifact id"):
            session.read_artifact_by_id(999)
        with pytest.raises(ValidationError, match="unknown registry artifact id"):
            session.list_upstream_dependencies(artifact_id=999)
        with pytest.raises(ValidationError, match="unknown workflow run id"):
            session.list_run_manifest_bindings(run_id=999)


def test_read_session_closes_connection_after_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    captured: dict[str, sqlite3.Connection] = {}

    with pytest.raises(RuntimeError, match="boom"):
        with _open_registry_read_session(registry_path) as session:
            captured["conn"] = session._conn
            raise RuntimeError("boom")

    conn = captured["conn"]
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_read_session_rejects_missing_database(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="missing database"):
        with _open_registry_read_session(tmp_path / "absent.db"):
            pass


def test_read_session_rejects_incompatible_schema(tmp_path: Path) -> None:
    incompatible_runtime = tmp_path / "incompatible"
    (incompatible_runtime / "database").mkdir(parents=True)
    incompatible_path = incompatible_runtime / REGISTRY_DB_PATH
    with sqlite3.connect(incompatible_path) as conn:
        conn.execute("PRAGMA user_version = 1")

    with pytest.raises(ValidationError, match="schema version is incompatible"):
        with _open_registry_read_session(incompatible_path):
            pass
