"""Specification members must not disturb ordinary workflow membership."""

from __future__ import annotations

import json
import sqlite3

import pytest
import yaml

import nipact.execution as ordinary_execution_module
from conftest import RegistryV18Fixture
from nipact.cli import main
from nipact.execution import build_run_plan, execute_run_plan
from nipact.registry import read_specification_snapshot_projections
from test_specification_execution import _run_jobs_in_process
from test_specification_registry import _specification_payload, prepare_v19


def _query(fixture: RegistryV18Fixture, sql: str) -> list[tuple]:
    conn = sqlite3.connect(f"file:{fixture.registry_path}?mode=ro", uri=True)
    try:
        return [tuple(row) for row in conn.execute(sql)]
    finally:
        conn.close()


def _runs(fixture: RegistryV18Fixture) -> list[tuple]:
    return _query(
        fixture,
        """
        SELECT run_id, workflow_name, selected_step_name,
               selected_output_name, is_current
        FROM workflow_runs ORDER BY run_id
        """,
    )


def _published(fixture: RegistryV18Fixture) -> list[tuple]:
    return _query(
        fixture,
        """
        SELECT workflow_name, step_name, output_name, address, artifact_id
        FROM published_outputs
        ORDER BY step_name, output_name, address
        """,
    )


def _write_two_member_specification(fixture: RegistryV18Fixture) -> None:
    payload = _specification_payload()
    payload.pop("exclude", None)
    payload["expected_counts"] = {"candidates": 2, "included": 2, "excluded": 0}
    fixed = payload["fixed"]
    assert isinstance(fixed, dict)
    fixed["manifest_bindings"] = []
    fixed["target"] = {"step": "fixture_transform", "output": "left"}
    fixed["results"] = {
        "left": {"step": "fixture_transform", "output": "left"},
        "right": {"step": "fixture_transform", "output": "right"},
    }
    (fixture.project_dir / "specifications/compact.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False), encoding="utf-8"
    )


def test_specification_members_preserve_ordinary_current_and_published(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = registry_v18_fixture
    prepare_v19(fixture, route="fresh")
    _write_two_member_specification(fixture)
    monkeypatch.setattr(
        ordinary_execution_module, "_run_snakemake", _run_jobs_in_process
    )

    plan = build_run_plan(
        project_dir=fixture.project_dir,
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_transform",
        dry_run=False,
    )
    execute_run_plan(plan, cores=1)

    runs_before = _runs(fixture)
    published_before = _published(fixture)
    canonical_run_ids = {row[0] for row in runs_before}
    assert [row for row in runs_before if row[4] == 1] == runs_before

    base = ["--project-dir", str(fixture.project_dir), "--context", fixture.context]
    capsys.readouterr()
    assert main(["specifications", "freeze", "compact", *base]) == 0
    digest = json.loads(capsys.readouterr().out)["snapshot_digest"]
    assert main(["specifications", "run", digest, "--all", *base]) == 0
    capsys.readouterr()

    runs_after = _runs(fixture)
    member_runs = [row for row in runs_after if row[0] not in canonical_run_ids]

    # Members executed and were recorded as runs.
    assert len(member_runs) == 2

    # No member claims the declared workflow's current-run scope.
    assert all(row[4] == 0 for row in member_runs)

    # The canonical run remains the only current run, untouched.
    assert [row for row in runs_after if row[4] == 1] == runs_before

    # Ordinary published coordinates are byte-for-byte unchanged.
    assert _published(fixture) == published_before

    # The specification still recorded its own exact results:
    # two members x two result roles x the two-entity execution population.
    results = _query(
        fixture,
        "SELECT role, address, artifact_id FROM specification_attempt_results",
    )
    assert len(results) == 8
    assert {row[0] for row in results} == {"left", "right"}
    assert {row[1] for row in results} == {"entity_001", "entity_002"}

    # The member carrying the canonical parameter value resolves to the very
    # artifacts the ordinary run produced: reuse crosses the specification
    # boundary by request identity, so isolation costs no recomputation.
    canonical_targets = {
        row[4] for row in published_before if row[1] == "fixture_transform"
    }
    assert canonical_targets <= {row[2] for row in results}

    # Every recorded result points at a real published artifact, so member
    # outputs stay reusable by request identity without being published
    # into the declared workflow's coordinate space.
    artifact_ids = {row[2] for row in results}
    published_flags = _query(
        fixture,
        "SELECT artifact_id, is_published FROM artifacts "
        f"WHERE artifact_id IN ({','.join(str(i) for i in sorted(artifact_ids))})",
    )
    assert len(published_flags) == len(artifact_ids)
    assert all(flag == 1 for _, flag in published_flags)

    # Every attempt reached a successful terminal state.
    attempts = _query(
        fixture,
        "SELECT outcome, selecting_run_id FROM specification_member_attempts",
    )
    assert len(attempts) == 2
    assert all(outcome == "complete" for outcome, _ in attempts)
    assert {run_id for _, run_id in attempts} == {row[0] for row in member_runs}

    # Because members no longer publish their own results, a reported current
    # publication now means what it says: this member's exact artifact is also
    # the workflow's current published answer at that coordinate. The member
    # reproducing the canonical parameter reports one; the member that varies
    # it reports none, without either result becoming less resolved.
    projections = read_specification_snapshot_projections(
        fixture.registry_path,
        context=fixture.context,
        snapshot_digest=digest,
    )
    assert len(projections.results) == 8
    assert all(result.artifact_id is not None for result in projections.results)
    coinciding = {
        result.artifact_id
        for result in projections.results
        if result.current_publication_path is not None
    }
    assert coinciding == canonical_targets
    assert {
        result.current_publication_path
        for result in projections.results
        if result.current_publication_path is not None
    } == {
        result.artifact_path
        for result in projections.results
        if result.artifact_id in canonical_targets
    }
