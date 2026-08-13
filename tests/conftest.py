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
from nipact.registry import REGISTRY_DB_PATH, list_artifacts


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
