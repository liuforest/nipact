import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from nipact.cli import main
from nipact.execution import build_run_plan, execute_run_plan
from nipact.registry import REGISTRY_DB_PATH, RegistryArtifact, list_artifacts


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
) -> tuple[Path, Path, RegistryArtifact]:
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
    return project_dir, runtime_dir, selected


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
    selected: RegistryArtifact,
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
                extension, created_at
            )
            VALUES ('source', ?, ?, ?, ?, ?, ?, ?)
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
                input_path, binding_name, dependency_role
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                selected.artifact_id,
                foreign_artifact_id,
                "c" * 64,
                1,
                ".json",
                "data/other/source.json",
                "foreign_source",
                "source_input",
            ),
        )
    return foreign_artifact_id


def test_trace_command_prints_text_summary_for_artifact_id(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir, selected = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )

    assert (
        main(
            [
                "trace",
                *_trace_base_args(project_dir),
                "--artifact-id",
                str(selected.artifact_id),
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert lines[0] == f"artifact_id={selected.artifact_id}"
    assert "origin=workflow_output" in lines
    assert "is_published=true" in lines
    assert "workflow=base" in lines
    assert "step=color_sector_analysis" in lines
    assert "output=sector_counts" in lines
    assert "address=init" in lines
    assert any(line.startswith("parameter_hash=") for line in lines)
    assert not any(line.startswith("parameters=") for line in lines)
    assert "provenance_status=complete" in lines
    assert "warnings=0" in lines
    assert lines[-1] == "PASS: trace"


def test_trace_command_json_output_is_json_only_for_file_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir, selected = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )

    assert (
        main(
            [
                "trace",
                *_trace_base_args(project_dir),
                "--file-path",
                selected.path,
                "--json",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    assert "PASS:" not in captured.out
    graph = json.loads(captured.out)
    assert graph["selected_artifact_id"] == selected.artifact_id
    assert graph["provenance_status"] == "complete"


def test_trace_command_does_not_mutate_registry_db(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, selected = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH
    before_digest = hashlib.sha256(registry_path.read_bytes()).hexdigest()

    assert (
        main(
            [
                "trace",
                *_trace_base_args(project_dir),
                "--artifact-id",
                str(selected.artifact_id),
            ]
        )
        == 0
    )

    capsys.readouterr()
    after_digest = hashlib.sha256(registry_path.read_bytes()).hexdigest()
    assert after_digest == before_digest


@pytest.mark.parametrize(
    "selector",
    [
        "file_path",
    ],
)
def test_trace_command_context_guards_non_artifact_id_selectors(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    selector: str,
) -> None:
    project_dir, runtime_dir, selected = _successful_sector_run(
        tmp_path,
        capsys,
        monkeypatch,
    )
    registry_path = runtime_dir / REGISTRY_DB_PATH
    foreign_artifact_id = _insert_foreign_source_dependency(
        registry_path=registry_path,
        runtime_dir=runtime_dir,
        selected=selected,
    )
    if selector == "file_path":
        selector_args = ["--file-path", selected.path]
    else:
        selector_args = [
            "--workflow",
            "base",
            "--step",
            "color_sector_analysis",
            "--output",
            "sector_counts",
            "--address",
            "init",
        ]

    assert (
        main(
            [
                "trace",
                *_trace_base_args(project_dir),
                *selector_args,
                "--json",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    graph = json.loads(captured.out)
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
