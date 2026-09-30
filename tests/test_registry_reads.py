import json
import os
import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import nipact.registry as registry
from nipact.artifacts import canonical_output_path
from nipact.cli import main
from nipact.errors import ValidationError
from nipact.manifest import build_manifest
from nipact.registry import (
    EnvironmentObservationV1,
    MembershipIntent,
    PublishedOutputRow,
    RunExecutionPopulationRow,
    RunManifestBindingRow,
    SelectedOutputResolutionIntent,
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
    reconcile_manifest_and_source_authorities,
    read_registry_summary,
    record_workflow_run,
    resolve_registered_artifact_path,
)
from nipact.project_setup import ProjectSetupError, validate_project
from nipact.source_authority import (
    LogicalSourceCoordinate,
    SourceDeclaration,
    observe_source_authority,
)
from nipact.workflow import load_workflow_project

from conftest import publish_compact_colors


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


def _reconcile_colors_source(runtime_dir: Path) -> None:
    declaration = SourceDeclaration(
        coordinate=LogicalSourceCoordinate(
            "colors", "global", "colors_source", None
        ),
        declared_path="data/color_source.json",
        declared_extension=".json",
    )
    reconcile_manifest_and_source_authorities(
        runtime_dir / REGISTRY_DB_PATH,
        context="colors",
        manifests={},
        manifest_paths={},
        observations=(
            observe_source_authority(
                runtime_root=runtime_dir,
                declaration=declaration,
                registered=None,
            ),
        ),
    )


def test_manifest_and_source_authority_reconciliation_rolls_back_together(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    coordinate = LogicalSourceCoordinate(
        "colors", "global", "colors_source", None
    )
    declaration = SourceDeclaration(
        coordinate=coordinate,
        declared_path="data/color_source.json",
        declared_extension=".json",
    )
    observation = observe_source_authority(
        runtime_root=runtime_dir,
        declaration=declaration,
        registered=None,
    )
    reconcile_manifest_and_source_authorities(
        registry_path,
        context="colors",
        manifests={},
        manifest_paths={},
        observations=(observation,),
    )
    with sqlite3.connect(registry_path) as conn:
        manifest_name, declared_path, old_schema, old_digest = conn.execute(
            """
            SELECT manifest_name, declared_path,
                   last_validated_manifest_value_schema,
                   last_validated_manifest_digest
            FROM manifest_declarations
            WHERE context = 'colors'
            ORDER BY manifest_name
            LIMIT 1
            """
        ).fetchone()

    changed_manifest = build_manifest(
        description="Rollback probe",
        entities=("rollback_entity",),
    )
    relocated = replace(
        observation,
        declaration=SourceDeclaration(
            coordinate=coordinate,
            declared_path="data/relocated.json",
            declared_extension=".json",
        ),
    )
    with pytest.raises(ValidationError, match="source relocation is unsupported"):
        reconcile_manifest_and_source_authorities(
            registry_path,
            context="colors",
            manifests={manifest_name: changed_manifest},
            manifest_paths={manifest_name: declared_path},
            observations=(relocated,),
        )

    with sqlite3.connect(registry_path) as conn:
        current_reference = conn.execute(
            """
            SELECT last_validated_manifest_value_schema,
                   last_validated_manifest_digest
            FROM manifest_declarations
            WHERE context = 'colors' AND manifest_name = ?
            """,
            (manifest_name,),
        ).fetchone()
        changed_value_count = conn.execute(
            """
            SELECT COUNT(*) FROM manifest_values
            WHERE value_schema = ? AND manifest_digest = ?
            """,
            (
                changed_manifest.manifest_value_schema,
                changed_manifest.manifest_digest,
            ),
        ).fetchone()[0]
        source_path = conn.execute(
            """
            SELECT path FROM artifacts
            WHERE origin = 'source' AND context = 'colors'
              AND source_scope = 'global' AND source_name = 'colors_source'
            """
        ).fetchone()[0]
    assert current_reference == (old_schema, old_digest)
    assert changed_value_count == 0
    assert source_path == "data/color_source.json"


def test_registry_reads_reconciled_source_artifact(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    assert list_artifacts(registry_path, context="colors", origin="source") == []
    _reconcile_colors_source(runtime_dir)

    source_artifacts = list_artifacts(registry_path, context="colors", origin="source")

    assert len(source_artifacts) == 1
    source = source_artifacts[0]
    assert source.origin == "source"
    assert source.path == "data/color_source.json"
    assert source.run_id is None
    assert source.request_bundle_digest is None
    assert source.source_metadata is None
    assert source.source_scope == "global"
    assert source.source_name == "colors_source"
    assert source.source_entity_id is None
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
    _reconcile_colors_source(runtime_dir)

    manifests = list_manifests(registry_path, context="colors")
    manifest = read_manifest(registry_path, context="colors", manifest_name="init")
    summary = read_registry_summary(registry_path, context="colors")

    assert {row.name for row in manifests} >= {"init"}
    assert manifest.name == "init"
    assert manifest.canonical_body.startswith("color_000")
    assert manifest.manifest_value_schema == "entity_set_v1"
    assert manifest.first_entity_id == "color_000"
    assert manifest.last_entity_id == "color_199"
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


def test_registry_reads_schema_qualified_run_populations_and_bindings(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    with sqlite3.connect(registry_path) as conn:
        manifest_value_schema, manifest_digest = conn.execute(
            """
            SELECT last_validated_manifest_value_schema,
                   last_validated_manifest_digest
            FROM manifest_declarations
            WHERE context = 'colors' AND manifest_name = 'init'
            """
        ).fetchone()
    assert (
        record_workflow_run(
            registry_path,
            runtime_root=runtime_dir,
            context="colors",
            workflow_name="base",
            selected_step_name="color_sector_analysis",
            selected_output_name="sector_counts",
            run_workspace="runs/manual",
            run_plan_path="runs/manual/run_plan.json",
            run_plan_digest="a" * 64,
            artifacts=(),
            projection_recipes=(),
            reused_projection_seeds=(),
            selected_resolution_intents=(),
            environment_observation=EnvironmentObservationV1(
                nipact_version="test",
                python_version="test",
                platform="test",
                snakemake_version="test",
            ),
            execution_population=RunExecutionPopulationRow(
                manifest_name="init",
                manifest_value_schema=manifest_value_schema,
                manifest_digest=manifest_digest,
            ),
            manifest_bindings=(
                RunManifestBindingRow(
                    step_name="color_sector_analysis",
                    manifest_usage_role="fit_cohort",
                    manifest_name="init",
                    manifest_value_schema=manifest_value_schema,
                    manifest_digest=manifest_digest,
                ),
            ),
            membership_intents=(),
        )
        == 0
    )
    with sqlite3.connect(registry_path) as conn:
        run_id = conn.execute("SELECT MAX(run_id) FROM workflow_runs").fetchone()[0]

    with pytest.raises(
        ValidationError,
        match="selected-output resolution does not match selected output",
    ):
        record_workflow_run(
            registry_path,
            runtime_root=runtime_dir,
            context="colors",
            workflow_name="base",
            selected_step_name="color_sector_analysis",
            selected_output_name="sector_counts",
            run_workspace="runs/mismatch",
            run_plan_path="runs/mismatch/run_plan.json",
            run_plan_digest="c" * 64,
            artifacts=(),
            projection_recipes=(),
            reused_projection_seeds=(),
            selected_resolution_intents=(
                SelectedOutputResolutionIntent(
                    context="colors",
                    workflow_name="base",
                    step_name="color_features",
                    output_name="sector_counts",
                    address="cohort",
                    outcome=None,
                ),
            ),
            environment_observation=EnvironmentObservationV1(
                nipact_version="test",
                python_version="test",
                platform="test",
                snakemake_version="test",
            ),
            manifest_bindings=(),
            membership_intents=(),
        )
    with pytest.raises(ValidationError, match="FOREIGN KEY constraint failed"):
        record_workflow_run(
            registry_path,
            runtime_root=runtime_dir,
            context="colors",
            workflow_name="base",
            selected_step_name="color_sector_analysis",
            selected_output_name="sector_counts",
            run_workspace="runs/invalid",
            run_plan_path="runs/invalid/run_plan.json",
            run_plan_digest="b" * 64,
            artifacts=(),
            projection_recipes=(),
            reused_projection_seeds=(),
            selected_resolution_intents=(),
            environment_observation=EnvironmentObservationV1(
                nipact_version="test",
                python_version="test",
                platform="test",
                snakemake_version="test",
            ),
            execution_population=RunExecutionPopulationRow(
                manifest_name="missing",
                manifest_value_schema="entity_set_v1",
                manifest_digest="f" * 64,
            ),
            manifest_bindings=(),
            membership_intents=(),
        )
    with sqlite3.connect(registry_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 1
        assert conn.execute(
            "SELECT is_current FROM workflow_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone() == (1,)


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
            projection_json = json.dumps(
                {
                    "address": "init",
                    "canonical_parameters": {},
                    "determinism_contract": "deterministic",
                    "identity_contract_version": 1,
                    "namespace": "colors",
                    "output_contract": {
                        "output_contract_version": 1,
                        "sibling_outputs": [
                            {
                                "declared_extension": ".json",
                                "output_name": "output",
                            }
                        ],
                    },
                    "result_affecting_settings": {},
                    "role_labelled_bindings": [],
                    "step_contract": {
                        "callable_ref": "tests:manual",
                        "runner_contract_version": "1",
                        "step_contract_id": f"manual_step_{index}",
                        "step_contract_version": "1",
                    },
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            projection_digest = registry.sha256_digest(projection_json.encode("utf-8"))
            conn.execute(
                """
                INSERT INTO request_bundle_projections (
                    request_bundle_digest, projection_json
                )
                VALUES (?, ?)
                """,
                (projection_digest, projection_json),
            )
            conn.execute(
                """
                INSERT INTO artifacts (
                    origin, context, workflow_name, step_name, output_name,
                    address, job_id, path, content_digest, output_hash,
                    file_size, extension, request_bundle_digest, created_at
                )
                VALUES (
                    'workflow_output', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
                    projection_digest,
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, run_plan = publish_compact_colors(tmp_path, monkeypatch)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    declared_params = load_workflow_project(
        project_dir=project_dir,
        context="colors",
    ).steps["color_sector_analysis"].params
    (selected_job,) = run_plan.selected_fresh_jobs

    selected = read_current_published_artifact(
        registry_path,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="cohort",
    )

    assert selected.is_selected_output is True
    assert selected.is_published is True
    assert selected.run_id is not None
    assert selected.parameter_hash is not None
    assert selected.parameters_json is not None
    assert selected.request_bundle_digest is not None
    assert len(selected.request_bundle_digest) == 64
    assert declared_params
    assert set(json.loads(selected.parameters_json)) == set(declared_params)
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

    assert len(workflow_artifacts) == len(run_plan.jobs)
    assert selected in published_artifacts
    assert len(published_artifacts) == len(run_plan.published_outputs)
    assert len(upstream_edges) == run_plan.execution_population.entity_count
    assert {edge.binding_name for edge in upstream_edges} == {
        record.binding_name for record in selected_job.input_records
    }
    assert len(manifest_bindings) == len(run_plan.manifest_bindings)
    assert {binding.context for binding in manifest_bindings} == {"colors"}


def test_project_validation_accepts_cross_workflow_membership_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir, _run_plan = publish_compact_colors(tmp_path, monkeypatch)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    with sqlite3.connect(registry_path) as conn:
        shared_path = conn.execute(
            """
            SELECT path
            FROM published_outputs
            WHERE context = 'colors'
              AND workflow_name = 'base'
              AND step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
            """
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO published_outputs (
                context, workflow_name, step_name, output_name, address, path,
                output_digest, output_hash, artifact_id
            )
            SELECT context, 'red-qc-target', step_name, output_name, address, path,
                   output_digest, output_hash, artifact_id
            FROM published_outputs
            WHERE context = 'colors'
              AND workflow_name = 'base'
              AND step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
            """
        )

    target_hashes = 0
    real_sha256_file_digest = registry.artifact_content_facts

    def count_shared_artifact_hashes(path: Path, kind: str):
        nonlocal target_hashes
        if path == runtime_dir / shared_path:
            target_hashes += 1
        return real_sha256_file_digest(path, kind)

    monkeypatch.setattr(
        registry,
        "artifact_content_facts",
        count_shared_artifact_hashes,
    )
    result = validate_project(project_dir=project_dir, context="colors")
    assert result.published_outputs == len(_run_plan.published_outputs) + 1
    assert target_hashes == 1


def test_accepted_artifact_validation_hashes_shared_occurrence_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"shared artifact\n"
    digest = registry.sha256_digest(payload)
    output_hash = digest[:16]
    request_bundle_digest = "a" * 64
    relative_path = canonical_output_path(
        context="colors",
        step_name="example",
        address="subject",
        request_bundle_digest=request_bundle_digest,
        output_name="result",
        output_hash=output_hash,
        declared_extension=".json",
    )
    artifact_path = tmp_path / relative_path
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(payload)
    rows = [
        (
            artifact_id,
            "colors",
            "base",
            "example",
            "result",
            "subject",
            "workflow_output",
            1,
            relative_path,
            relative_path,
            digest,
            output_hash,
            len(payload),
            ".json",
            request_bundle_digest,
            "file",
            "sha256",
        )
        for artifact_id in (10, 11)
    ]
    loaded_project = SimpleNamespace(
        steps={"example": SimpleNamespace(outputs={"result": SimpleNamespace(extension=".json")})}
    )
    hash_calls = 0
    real_sha256_file_digest = registry.artifact_content_facts

    def count_hashes(path: Path, kind: str):
        nonlocal hash_calls
        hash_calls += 1
        return real_sha256_file_digest(path, kind)

    monkeypatch.setattr(registry, "artifact_content_facts", count_hashes)
    registry._validate_accepted_workflow_output_rows(
        rows,
        context="colors",
        runtime_root=tmp_path,
        loaded_workflow_project=loaded_project,
        contracts={request_bundle_digest: _example_retained_contract()},
        verified_occurrences=set(),
    )

    assert hash_calls == 1


@pytest.mark.parametrize(
    ("field_index", "bad_value", "message"),
    [
        (12, 999, "file size mismatch"),
        (13, ".txt", "retained output contract"),
    ],
)
def test_accepted_artifact_validation_checks_size_and_extension(
    tmp_path: Path,
    field_index: int,
    bad_value: object,
    message: str,
) -> None:
    payload = b"artifact\n"
    digest = registry.sha256_digest(payload)
    output_hash = digest[:16]
    request_bundle_digest = "a" * 64
    relative_path = canonical_output_path(
        context="colors",
        step_name="example",
        address="subject",
        request_bundle_digest=request_bundle_digest,
        output_name="result",
        output_hash=output_hash,
        declared_extension=".json",
    )
    artifact_path = tmp_path / relative_path
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(payload)
    row = list(
        (
            10,
            "colors",
            "base",
            "example",
            "result",
            "subject",
            "workflow_output",
            1,
            relative_path,
            relative_path,
            digest,
            output_hash,
            len(payload),
            ".json",
            request_bundle_digest,
            "file",
            "sha256",
        )
    )
    row[field_index] = bad_value
    loaded_project = SimpleNamespace(
        steps={"example": SimpleNamespace(outputs={"result": SimpleNamespace(extension=".json")})}
    )

    with pytest.raises(ValidationError, match=message):
        registry._validate_accepted_workflow_output_rows(
            [tuple(row)],
            context="colors",
            runtime_root=tmp_path,
            loaded_workflow_project=loaded_project,
            contracts={request_bundle_digest: _example_retained_contract()},
            verified_occurrences=set(),
        )


@pytest.mark.parametrize("artifact_state", ["intact", "missing", "corrupt"])
def test_project_validation_covers_accepted_artifact_without_membership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_state: str,
) -> None:
    project_dir, runtime_dir, run_plan = publish_compact_colors(tmp_path, monkeypatch)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    with sqlite3.connect(registry_path) as conn:
        artifact_id, relative_path = conn.execute(
            """
            SELECT artifact_id, published_path
            FROM artifacts
            WHERE context = 'colors'
              AND step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
              AND is_published = 1
            """
        ).fetchone()
        conn.execute(
            "DELETE FROM published_outputs WHERE artifact_id = ?",
            (artifact_id,),
        )

    artifact_path = runtime_dir / relative_path
    if artifact_state == "missing":
        artifact_path.unlink()
    elif artifact_state == "corrupt":
        artifact_path.write_bytes(b"x" * artifact_path.stat().st_size)

    if artifact_state == "intact":
        result = validate_project(project_dir=project_dir, context="colors")
        assert result.published_outputs == len(run_plan.published_outputs) - 1
    else:
        message = (
            "missing published output artifact"
            if artifact_state == "missing"
            else "published output artifact digest mismatch"
        )
        with pytest.raises(ProjectSetupError, match=message):
            validate_project(project_dir=project_dir, context="colors")


def test_selected_resolution_membership_invariant_covers_all_outcomes() -> None:
    row = PublishedOutputRow(
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        output_name="sector_counts",
        address="init",
        path="outputs/result.json",
        output_digest="a" * 64,
        output_hash="a" * 16,
    )

    def resolution(
        outcome: str | None,
        artifact_id: int | None = None,
    ) -> SelectedOutputResolutionIntent:
        return SelectedOutputResolutionIntent(
            context=row.context,
            workflow_name=row.workflow_name,
            step_name=row.step_name,
            output_name=row.output_name,
            address=row.address,
            outcome=outcome,
            existing_artifact_id=artifact_id,
        )

    registry._validate_selected_resolution_memberships(
        (resolution("generated"),),
        (MembershipIntent(row=row),),
    )
    registry._validate_selected_resolution_memberships(
        (resolution("reused", 7),),
        (MembershipIntent(row=row, existing_artifact_id=7),),
    )
    registry._validate_selected_resolution_memberships(
        (resolution(None),),
        (),
    )

    with pytest.raises(ValidationError, match="requires a fresh membership"):
        registry._validate_selected_resolution_memberships(
            (resolution("generated"),),
            (MembershipIntent(row=row, existing_artifact_id=7),),
        )
    with pytest.raises(ValidationError, match="same existing membership"):
        registry._validate_selected_resolution_memberships(
            (resolution("reused", 7),),
            (MembershipIntent(row=row, existing_artifact_id=8),),
        )
    with pytest.raises(ValidationError, match="cannot have a membership"):
        registry._validate_selected_resolution_memberships(
            (resolution(None),),
            (MembershipIntent(row=row),),
        )


def test_existing_membership_rejects_artifact_hash_inconsistent_with_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = publish_compact_colors(tmp_path, monkeypatch)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    with sqlite3.connect(registry_path) as conn:
        artifact_id, path, digest = conn.execute(
            """
            SELECT artifact_id, path, content_digest
            FROM artifacts
            WHERE step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
            """
        ).fetchone()
        bad_hash = "f" * 16
        assert bad_hash != digest[:16]
        conn.execute(
            "UPDATE artifacts SET output_hash = ? WHERE artifact_id = ?",
            (bad_hash, artifact_id),
        )
        with pytest.raises(
            ValidationError,
            match="existing membership artifact hash does not match digest",
        ):
            registry._insert_memberships(
                conn,
                intents=(
                    MembershipIntent(
                        row=PublishedOutputRow(
                            context="colors",
                            workflow_name="derived",
                            step_name="color_sector_analysis",
                            output_name="sector_counts",
                            address="cohort",
                            path=path,
                            output_digest=digest,
                            output_hash=bad_hash,
                        ),
                        existing_artifact_id=artifact_id,
                    ),
                ),
                artifact_ids={},
            )


def test_list_artifact_group_counts_matches_list_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = publish_compact_colors(tmp_path, monkeypatch)
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


def test_current_published_artifact_rejects_membership_hash_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir, _run_plan = publish_compact_colors(tmp_path, monkeypatch)
    registry_path = runtime_dir / REGISTRY_DB_PATH
    with sqlite3.connect(registry_path) as conn:
        conn.execute(
            """
            UPDATE published_outputs
            SET output_hash = ?
            WHERE context = 'colors'
              AND workflow_name = 'base'
              AND step_name = 'color_sector_analysis'
              AND output_name = 'sector_counts'
              AND address = 'cohort'
            """,
            ("f" * 16,),
        )

    with pytest.raises(ValidationError, match="unknown current published artifact"):
        read_current_published_artifact(
            registry_path,
            context="colors",
            workflow_name="base",
            step_name="color_sector_analysis",
            output_name="sector_counts",
            address="cohort",
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


def test_read_session_reports_unknown_ids_within_session(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / REGISTRY_DB_PATH

    with pytest.raises(ValidationError, match="positive integer"):
        read_artifact_by_id(registry_path, 0)
    with pytest.raises(ValidationError, match="unknown registry artifact id"):
        read_artifact_by_id(registry_path, 999)
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
        conn.execute("PRAGMA user_version = 14")

    with pytest.raises(ValidationError, match="schema version is incompatible"):
        with _open_registry_read_session(incompatible_path):
            pass


def test_retained_file_directory_contracts_coexist_after_public_transition(
    tmp_path, monkeypatch
):
    from directory_support import directory_project, run, rows, change_step
    import yaml
    from nipact.workflow import load_workflow_project

    project, runtime = directory_project(tmp_path, monkeypatch)
    path = project / "steps/mixed.yaml"
    original = yaml.safe_load(path.read_text())
    # Same callable and port, first as a legacy file; only the declaration changes.
    module = tmp_path / "importable/directory_runtime.py"
    module.write_text(
        module.read_text().replace(
            '    outputs["empty"].mkdir()',
            '    if outputs["empty"].suffix == ".txt":\n        outputs["empty"].write_text("historical")\n    else:\n        outputs["empty"].mkdir()',
        )
    )
    outputs = dict(original["outputs"])
    outputs["empty"] = {"extension": ".txt", "address_scope": "entity"}
    change_step(project, "mixed", outputs=outputs)
    assert run(project, address="sub_001").all_selected_resolved
    before = rows(runtime)
    deps = rows(runtime, "artifact_dependencies")
    old = next(row for row in before if row["output_name"] == "empty")
    change_step(project, "mixed", outputs=original["outputs"])

    def validate():
        return registry.validate_prepared_registry_db(
            runtime / "database/registry.db",
            context="mini",
            runtime_root=runtime,
            loaded_workflow_project=load_workflow_project(
                project_dir=project, context="mini"
            ),
        )

    validate()  # old membership uses its retained file contract
    assert run(project, address="sub_001").published_count == 3
    validate()  # both generations, after membership replacement
    assert [
        row
        for row in rows(runtime)
        if row["artifact_id"] in {old["artifact_id"] for old in before}
        and row["origin"] == "workflow_output"
    ] == [row for row in before if row["origin"] == "workflow_output"]
    assert rows(runtime, "artifact_dependencies")[: len(deps)] == deps
    assert (runtime / old["path"]).read_text() == "historical"
    change_step(project, "mixed", outputs=outputs)
    assert run(project, address="sub_001").selected_reused_count == 1
    validate()
    with sqlite3.connect(runtime / "database/registry.db") as conn:
        conn.execute(
            "UPDATE artifacts SET extension='.bad' WHERE artifact_id=?",
            (old["artifact_id"],),
        )
    with pytest.raises(ValidationError, match="retained output contract"):
        validate()


def _example_retained_contract():
    # This fixture is metadata-only; canonical serialization is covered by golden vectors.
    return SimpleNamespace(
        projection=SimpleNamespace(
            namespace="colors",
            address="subject",
            step_contract=SimpleNamespace(step_contract_id="example"),
            output_contract=SimpleNamespace(
                sibling_outputs=(
                    SimpleNamespace(
                        output_name="result", kind="file", declared_extension=".json"
                    ),
                )
            ),
        )
    )
