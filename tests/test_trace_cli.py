import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from nipact.cli import main
from nipact.registry import read_artifact_by_id

from conftest import ColorsRegistry


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


def _trace_base_args(project_dir: Path) -> list[str]:
    return [
        "--project-dir",
        str(project_dir),
        "--context",
        "colors",
    ]


def _insert_foreign_source_dependency(
    *,
    registry_path: Path,
    runtime_dir: Path,
    dependent_artifact_id: int,
) -> int:
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
                extension, source_scope, source_name, source_st_dev,
                source_st_ino, source_st_size, source_st_mtime_ns,
                source_st_ctime_ns, created_at
            )
            VALUES ('source', ?, ?, ?, ?, ?, ?, 'global', 'foreign_source',
                    1, 2, 1, 3, 4, ?)
            """,
            (
                "other",
                "data/other/source.json",
                "c" * 64,
                "c" * 16,
                1,
                ".json",
                "2026-06-04T00:00:00+00:00",
            ),
        )
        foreign_artifact_id = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO artifact_dependencies (
                dependent_artifact_id, source_artifact_id,
                source_content_digest, source_file_size, source_extension,
                input_path, binding_name, dependency_role,
                source_scope, source_name, source_occurrence_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'global', 'foreign_source', ?)
            """,
            (
                dependent_artifact_id,
                foreign_artifact_id,
                "c" * 64,
                1,
                ".json",
                "data/other/source.json",
                "foreign_source",
                "source_input",
                "data/other/source.json",
            ),
        )
    return foreign_artifact_id


def test_trace_command_prints_text_summary_for_artifact_id(
    colors_registry: ColorsRegistry,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact_id = colors_registry.root_artifact_id

    assert (
        main(
            [
                "trace",
                *_trace_base_args(colors_registry.project_dir),
                "--artifact-id",
                str(artifact_id),
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert lines[0] == f"artifact_id={artifact_id}"
    assert "origin=workflow_output" in lines
    assert "is_published=true" in lines
    assert "workflow=base" in lines
    assert "step=color_sector_analysis" in lines
    assert "output=sector_counts" in lines
    assert "address=cohort" in lines
    assert any(line.startswith("parameter_hash=") for line in lines)
    assert not any(line.startswith("parameters=") for line in lines)
    assert "provenance_status=complete" in lines
    assert "warnings=0" in lines
    assert lines[-1] == "PASS: trace"


def test_trace_command_does_not_mutate_registry_db(
    colors_registry: ColorsRegistry,
) -> None:
    registry_path = colors_registry.registry_path
    before_digest = hashlib.sha256(registry_path.read_bytes()).hexdigest()

    assert (
        main(
            [
                "trace",
                *_trace_base_args(colors_registry.project_dir),
                "--artifact-id",
                str(colors_registry.root_artifact_id),
            ]
        )
        == 0
    )

    after_digest = hashlib.sha256(registry_path.read_bytes()).hexdigest()
    assert after_digest == before_digest


def test_trace_command_context_guards_non_artifact_id_selectors(
    colors_registry: ColorsRegistry,
    capsys: pytest.CaptureFixture[str],
) -> None:
    registry_path = colors_registry.registry_path
    selected = read_artifact_by_id(registry_path, colors_registry.root_artifact_id)
    foreign_artifact_id = _insert_foreign_source_dependency(
        registry_path=registry_path,
        runtime_dir=colors_registry.runtime_dir,
        dependent_artifact_id=selected.artifact_id,
    )

    assert (
        main(
            [
                "trace",
                *_trace_base_args(colors_registry.project_dir),
                "--file-path",
                selected.path,
                "--json",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    graph = json.loads(captured.out)
    assert graph["selected_artifact_id"] == selected.artifact_id
    assert graph["provenance_status"] == "degraded"
    assert {
        "warning_type": "cross_context_dependency",
        "message": "dependency source artifact is outside the active context",
        "artifact_id": foreign_artifact_id,
        "input_path": "data/other/source.json",
    } in graph["warnings"]
    assert all(
        artifact["artifact_id"] != foreign_artifact_id
        for artifact in graph["artifacts"]
    )


@pytest.mark.parametrize(
    ("extra_args", "expected_error"),
    [
        (
            [],
            "provide exactly one trace selector",
        ),
        (
            ["--artifact-id", "1", "--file-path", "data/color_source.json"],
            "provide exactly one trace selector",
        ),
        (
            ["--workflow", "base", "--step", "color_sector_analysis"],
            "workflow-coordinate trace selector requires",
        ),
    ],
)
def test_trace_command_rejects_invalid_selector_shapes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    extra_args: list[str],
    expected_error: str,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    assert main(["trace", *_trace_base_args(project_dir), *extra_args]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert expected_error in captured.err
