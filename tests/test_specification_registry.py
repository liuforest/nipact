from __future__ import annotations

import shutil
import sqlite3
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Callable

import pytest
import yaml

from conftest import RegistryV18Fixture
import nipact.registry as registry
from nipact.errors import ValidationError
from nipact.hashing import sha256_digest, short_hash
from nipact.projection import (
    IDENTITY_CONTRACT_VERSION,
    OUTPUT_CONTRACT_VERSION,
    RUNNER_CONTRACT_VERSION,
    OutputContract,
    RequestBundleProjectionPlanV3,
    SiblingOutput,
    StepContract,
)
from nipact.registry import (
    EnvironmentObservationV1,
    MembershipIntent,
    PublishedOutputRow,
    RetainedJobProjectionRecipe,
    SelectedOutputResolutionIntent,
    SpecificationAcceptanceIntent,
    SpecificationAttemptRef,
    SpecificationFailureDiagnostic,
    WorkflowOutputArtifactRow,
    append_specification_member_attempt,
    fail_specification_member_attempt,
    initialize_registry_db,
    insert_or_verify_specification_snapshot,
    migrate_registry_db,
    read_specification_attempt_outcome,
    read_specification_snapshot,
    record_workflow_run,
)
from nipact.specification_canonical import (
    CanonicalSpecificationMember,
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
                "summary": {"step": "fixture_analysis", "output": "summary"},
                "diagnostic": {"step": "fixture_analysis", "output": "detail"},
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
    analysis_path = fixture.project_dir / "steps/fixture_analysis.yaml"
    analysis = yaml.safe_load(analysis_path.read_text(encoding="utf-8"))
    analysis["outputs"]["detail"] = {
        "extension": ".json",
        "address_scope": "cohort",
    }
    analysis_path.write_text(
        yaml.safe_dump(analysis, sort_keys=False),
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


def build_entity_snapshot(
    fixture: RegistryV18Fixture,
) -> CanonicalSpecificationSnapshot:
    """Build a two-address, two-sibling member for partial-result checks."""
    payload = _specification_payload()
    fixed = payload["fixed"]
    assert isinstance(fixed, dict)
    fixed["manifest_bindings"] = []
    fixed["target"] = {"step": "fixture_transform", "output": "left"}
    fixed["results"] = {
        "left": {"step": "fixture_transform", "output": "left"},
        "right": {"step": "fixture_transform", "output": "right"},
    }
    return build_snapshot(fixture, payload=payload)


def _persist_snapshot(
    fixture: RegistryV18Fixture,
    snapshot: CanonicalSpecificationSnapshot,
) -> CanonicalSpecificationMember:
    assert insert_or_verify_specification_snapshot(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        snapshot=snapshot,
    )
    included = tuple(
        member for member in snapshot.members if member.disposition == "included"
    )
    assert len(included) == 1
    return included[0]


def _prepare_snapshot(
    fixture: RegistryV18Fixture,
    *,
    route: str = "fresh",
    entity_target: bool = False,
) -> tuple[CanonicalSpecificationSnapshot, CanonicalSpecificationMember]:
    snapshot = prepare_v19(fixture, route=route)
    if entity_target:
        snapshot = build_entity_snapshot(fixture)
    return snapshot, _persist_snapshot(fixture, snapshot)


def _append_attempt(
    fixture: RegistryV18Fixture,
    snapshot: CanonicalSpecificationSnapshot,
    member: CanonicalSpecificationMember,
) -> SpecificationAttemptRef:
    return append_specification_member_attempt(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member=member,
    )


def _fresh_job(
    *,
    context: str,
    workflow_name: str,
    step_name: str,
    address: str,
    output_names: tuple[str, ...],
    selected_output_name: str,
    token: str,
    accepted_outputs: frozenset[str] | None = None,
) -> tuple[
    tuple[WorkflowOutputArtifactRow, ...],
    RetainedJobProjectionRecipe,
    tuple[MembershipIntent, ...],
]:
    accepted = frozenset(output_names) if accepted_outputs is None else accepted_outputs
    artifacts: list[WorkflowOutputArtifactRow] = []
    memberships: list[MembershipIntent] = []
    for output_name in output_names:
        body = f"{token}:{step_name}:{output_name}:{address}\n".encode()
        digest = sha256_digest(body)
        path = f"outputs/test/{token}/{step_name}/{output_name}/{address}.json"
        artifact = WorkflowOutputArtifactRow(
            step_name=step_name,
            output_name=output_name,
            address=address,
            job_id=f"job__{token}__{step_name}__{address}",
            path=path,
            staging_path=f"runs/test/{token}/staging/{output_name}/{address}.json",
            published_path=path,
            content_digest=digest,
            output_hash=short_hash(digest),
            file_size=len(body),
            extension=".json",
            parameters_json=f'{{"test_token":"{token}"}}',
            callable_ref=f"fixture:{step_name}",
            is_selected_output=output_name == selected_output_name,
            is_published=True,
            input_records=(),
        )
        artifacts.append(artifact)
        if output_name in accepted:
            memberships.append(
                MembershipIntent(
                    row=PublishedOutputRow(
                        context=context,
                        workflow_name=workflow_name,
                        step_name=step_name,
                        output_name=output_name,
                        address=address,
                        path=path,
                        output_digest=digest,
                        output_hash=short_hash(digest),
                    )
                )
            )
    recipe = RetainedJobProjectionRecipe(
        step_name=step_name,
        address=address,
        output_names=output_names,
        projection_plan=RequestBundleProjectionPlanV3(
            identity_contract_version=IDENTITY_CONTRACT_VERSION,
            namespace=context,
            step_contract=StepContract(
                step_contract_id=step_name,
                step_contract_version="1",
                callable_ref=f"fixture:{step_name}",
                runner_contract_version=RUNNER_CONTRACT_VERSION,
            ),
            address=address,
            canonical_parameters={"test_token": token},
            role_labelled_binding_plans=(),
            result_affecting_settings={},
            determinism_contract="deterministic",
            output_contract=OutputContract(
                output_contract_version=OUTPUT_CONTRACT_VERSION,
                sibling_outputs=tuple(
                    SiblingOutput(
                        output_name=output_name,
                        declared_extension=".json",
                    )
                    for output_name in output_names
                ),
            ),
        ),
    )
    return tuple(artifacts), recipe, tuple(memberships)


def _selected_resolution(
    *,
    context: str,
    workflow_name: str,
    step_name: str,
    output_name: str,
    address: str,
    outcome: str | None,
    existing_artifact_id: int | None = None,
) -> SelectedOutputResolutionIntent:
    return SelectedOutputResolutionIntent(
        context=context,
        workflow_name=workflow_name,
        step_name=step_name,
        output_name=output_name,
        address=address,
        outcome=outcome,
        existing_artifact_id=existing_artifact_id,
    )


def _record(
    fixture: RegistryV18Fixture,
    *,
    token: str,
    selected_step_name: str,
    selected_output_name: str,
    artifacts: tuple[WorkflowOutputArtifactRow, ...] = (),
    recipes: tuple[RetainedJobProjectionRecipe, ...] = (),
    resolutions: tuple[SelectedOutputResolutionIntent, ...] = (),
    memberships: tuple[MembershipIntent, ...] = (),
    acceptance: SpecificationAcceptanceIntent | None = None,
    workflow_name: str = "base",
    fault_hook: Callable[[str], None] | None = None,
) -> int:
    return record_workflow_run(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        context=fixture.context,
        workflow_name=workflow_name,
        selected_step_name=selected_step_name,
        selected_output_name=selected_output_name,
        run_workspace=f"runs/test/{token}",
        run_plan_path=f"runs/test/{token}/run_plan.json",
        run_plan_digest=sha256_digest(token.encode()),
        artifacts=artifacts,
        projection_recipes=recipes,
        reused_projection_seeds=(),
        selected_resolution_intents=resolutions,
        environment_observation=EnvironmentObservationV1(
            nipact_version="test",
            python_version="test",
            platform="test",
            snakemake_version="test",
        ),
        manifest_bindings=(),
        membership_intents=memberships,
        specification_acceptance=acceptance,
        _fault_hook=fault_hook,
    )


def _attempt_row(database: Path, attempt_id: int) -> tuple[object, ...]:
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            """
            SELECT attempt_id, snapshot_digest, member_key, started_at,
                   finished_at, outcome, selecting_run_id,
                   failure_stage, failure_summary
            FROM specification_member_attempts WHERE attempt_id = ?
            """,
            (attempt_id,),
        ).fetchone()
    assert row is not None
    return row


def _attempt_results(
    database: Path,
    attempt_id: int,
) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(database) as connection:
        return tuple(
            connection.execute(
                """
                SELECT role, address, artifact_id
                FROM specification_attempt_results
                WHERE attempt_id = ? ORDER BY role, address
                """,
                (attempt_id,),
            )
        )


def _reused_memberships(
    database: Path,
    *,
    context: str,
    workflow_name: str,
    coordinates: frozenset[tuple[str, str, str]],
) -> tuple[MembershipIntent, ...]:
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            """
            SELECT step_name, output_name, address, path,
                   output_digest, output_hash, artifact_id
            FROM published_outputs
            WHERE context = ? AND workflow_name = ?
            ORDER BY step_name, output_name, address
            """,
            (context, workflow_name),
        ).fetchall()
    selected: list[MembershipIntent] = []
    for step_name, output_name, address, path, digest, output_hash, artifact_id in rows:
        if (step_name, output_name, address) not in coordinates:
            continue
        selected.append(
            MembershipIntent(
                row=PublishedOutputRow(
                    context=context,
                    workflow_name=workflow_name,
                    step_name=str(step_name),
                    output_name=str(output_name),
                    address=str(address),
                    path=str(path),
                    output_digest=str(digest),
                    output_hash=str(output_hash),
                ),
                existing_artifact_id=int(artifact_id),
            )
        )
    assert len(selected) == len(coordinates)
    return tuple(selected)


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


@pytest.mark.parametrize("route", ["fresh", "migrated"])
def test_attempt_append_returns_exact_null_ref_and_always_appends(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture, route=route)
    monkeypatch.setattr(registry, "_utc_now", lambda: "2026-08-13T10:00:00+00:00")

    first = _append_attempt(fixture, snapshot, member)
    second = _append_attempt(fixture, snapshot, member)

    assert first == SpecificationAttemptRef(
        attempt_id=first.attempt_id,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
        member_key=member.member_key,
    )
    assert second.attempt_id == first.attempt_id + 1
    assert second.context == first.context
    assert second.snapshot_digest == first.snapshot_digest
    assert second.member_key == first.member_key
    for attempt in (first, second):
        assert _attempt_row(fixture.registry_path, attempt.attempt_id) == (
            attempt.attempt_id,
            snapshot.snapshot_digest,
            member.member_key,
            "2026-08-13T10:00:00+00:00",
            None,
            None,
            None,
            None,
            None,
        )
        assert _attempt_results(fixture.registry_path, attempt.attempt_id) == ()


@pytest.mark.parametrize(
    "case",
    [
        "unknown-snapshot",
        "excluded-member",
        "mismatched-member",
        "wrong-context",
        "wrong-runtime-path",
        "v18",
    ],
)
def test_attempt_append_rejects_representative_ownership_boundaries(
    registry_v18_fixture: RegistryV18Fixture,
    case: str,
) -> None:
    fixture = registry_v18_fixture
    if case == "v18":
        configure_specification_project(fixture)
        snapshot = build_snapshot(fixture)
        member = next(item for item in snapshot.members if item.disposition == "included")
    else:
        snapshot, member = _prepare_snapshot(fixture)

    context = fixture.context
    runtime_root = fixture.runtime_dir
    digest = snapshot.snapshot_digest
    if case == "unknown-snapshot":
        digest = "0" * 64
    elif case == "excluded-member":
        member = next(item for item in snapshot.members if item.disposition == "excluded")
    elif case == "mismatched-member":
        payload = _specification_payload()
        fixed = payload["fixed"]
        assert isinstance(fixed, dict)
        fixed["results"] = {
            "renamed": {"step": "fixture_analysis", "output": "summary"},
            "diagnostic": {"step": "fixture_analysis", "output": "detail"},
        }
        other = build_snapshot(fixture, payload=payload)
        member = _persist_snapshot(fixture, other)
    elif case == "wrong-context":
        context = "other"
    elif case == "wrong-runtime-path":
        runtime_root = fixture.runtime_dir / "other"

    before = _freeze_state(fixture.registry_path)
    match = _MIGRATION_GUIDANCE if case == "v18" else None
    with pytest.raises(ValidationError, match=match):
        append_specification_member_attempt(
            fixture.registry_path,
            runtime_root=runtime_root,
            context=context,
            snapshot_digest=digest,
            member=member,
        )
    assert _freeze_state(fixture.registry_path) == before


def test_attempt_append_fault_rolls_back_row_and_sequence(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture)
    before = _freeze_state(fixture.registry_path)

    with pytest.raises(RuntimeError, match="after_attempt_insert"):
        append_specification_member_attempt(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            context=fixture.context,
            snapshot_digest=snapshot.snapshot_digest,
            member=member,
            _fault_hook=lambda stage: (_ for _ in ()).throw(RuntimeError(stage)),
        )

    assert _freeze_state(fixture.registry_path) == before


def test_conditional_failure_validates_diagnostics_and_fills_once(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture)
    monkeypatch.setattr(registry, "_utc_now", lambda: "2026-08-13T11:00:00+00:00")

    for stage in ("planning", "execution", "acceptance"):
        attempt = _append_attempt(fixture, snapshot, member)
        assert fail_specification_member_attempt(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            attempt=attempt,
            member=member,
            diagnostic=SpecificationFailureDiagnostic(
                stage=stage,  # type: ignore[arg-type]
                summary="  compact failure  ",
            ),
        )
        assert _attempt_row(fixture.registry_path, attempt.attempt_id)[4:] == (
            "2026-08-13T11:00:00+00:00",
            "failed",
            None,
            stage,
            "compact failure",
        )
        terminal = _freeze_state(fixture.registry_path)
        assert not fail_specification_member_attempt(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            attempt=attempt,
            member=member,
            diagnostic=SpecificationFailureDiagnostic(
                stage="execution",
                summary="must not overwrite",
            ),
        )
        assert _freeze_state(fixture.registry_path) == terminal

    invalid = (
        SpecificationFailureDiagnostic(stage="execution", summary="   "),
        SpecificationFailureDiagnostic(stage="execution", summary="é" * 4097),
        SpecificationFailureDiagnostic(
            stage="retry",  # type: ignore[arg-type]
            summary="unsupported stage",
        ),
    )
    for diagnostic in invalid:
        attempt = _append_attempt(fixture, snapshot, member)
        before = _freeze_state(fixture.registry_path)
        with pytest.raises(ValidationError):
            fail_specification_member_attempt(
                fixture.registry_path,
                runtime_root=fixture.runtime_dir,
                attempt=attempt,
                member=member,
                diagnostic=diagnostic,
            )
        assert _freeze_state(fixture.registry_path) == before


def test_conditional_failure_rolls_back_and_rejects_contradictory_or_wrong_refs(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture, route="migrated")
    diagnostic = SpecificationFailureDiagnostic(
        stage="execution",
        summary="compact failure",
    )
    attempt = _append_attempt(fixture, snapshot, member)
    before = _freeze_state(fixture.registry_path)
    with pytest.raises(RuntimeError, match="after_attempt_failure_update"):
        fail_specification_member_attempt(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            attempt=attempt,
            member=member,
            diagnostic=diagnostic,
            _fault_hook=lambda stage: (_ for _ in ()).throw(RuntimeError(stage)),
        )
    assert _freeze_state(fixture.registry_path) == before

    for wrong in (
        replace(attempt, attempt_id=attempt.attempt_id + 1000),
        replace(attempt, context="other"),
        replace(attempt, snapshot_digest="0" * 64),
    ):
        with pytest.raises(ValidationError):
            fail_specification_member_attempt(
                fixture.registry_path,
                runtime_root=fixture.runtime_dir,
                attempt=wrong,
                member=member,
                diagnostic=diagnostic,
            )
        assert _freeze_state(fixture.registry_path) == before

    result = member.expected_results[0]
    with sqlite3.connect(fixture.registry_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO specification_attempt_results (
                attempt_id, snapshot_digest, member_key,
                role, address, artifact_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                attempt.attempt_id,
                snapshot.snapshot_digest,
                member.member_key,
                result.role,
                result.address,
                fixture.selected_artifact_id,
            ),
        )
    contradictory = _freeze_state(fixture.registry_path)
    with pytest.raises(ValidationError):
        fail_specification_member_attempt(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            attempt=attempt,
            member=member,
            diagnostic=diagnostic,
        )
    assert _freeze_state(fixture.registry_path) == contradictory


@pytest.mark.parametrize(
    (
        "outcome",
        "result_count",
        "has_selecting_run",
        "expected_outcome",
        "is_valid",
    ),
    [
        (None, 0, False, None, True),
        ("failed", 0, False, "failed", True),
        ("failed", 0, True, "failed", True),
        ("partial", 1, True, "partial", True),
        ("complete", 2, True, "complete", True),
        (None, 1, False, None, False),
        ("failed", 1, False, None, False),
        ("partial", 0, True, None, False),
        ("partial", 2, True, None, False),
        ("complete", 1, True, None, False),
    ],
)
def test_specification_attempt_outcome_reader_validates_state_and_cardinality(
    registry_v18_fixture: RegistryV18Fixture,
    outcome: str | None,
    result_count: int,
    has_selecting_run: bool,
    expected_outcome: str | None,
    is_valid: bool,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture, route="migrated")
    attempt = _append_attempt(fixture, snapshot, member)
    assert len(member.expected_results) == 2

    with sqlite3.connect(fixture.registry_path) as connection:
        selecting_run_id = connection.execute(
            "SELECT run_id FROM workflow_runs ORDER BY run_id LIMIT 1"
        ).fetchone()[0]
        if outcome is not None:
            failure_stage = "execution" if outcome == "failed" else None
            failure_summary = "compact failure" if outcome == "failed" else None
            connection.execute(
                """
                UPDATE specification_member_attempts
                SET finished_at = ?, outcome = ?, selecting_run_id = ?,
                    failure_stage = ?, failure_summary = ?
                WHERE attempt_id = ?
                """,
                (
                    "2026-08-13T12:00:00+00:00",
                    outcome,
                    selecting_run_id if has_selecting_run else None,
                    failure_stage,
                    failure_summary,
                    attempt.attempt_id,
                ),
            )
        for descriptor in member.expected_results[:result_count]:
            connection.execute(
                """
                INSERT INTO specification_attempt_results (
                    attempt_id, snapshot_digest, member_key,
                    role, address, artifact_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt.attempt_id,
                    snapshot.snapshot_digest,
                    member.member_key,
                    descriptor.role,
                    descriptor.address,
                    fixture.selected_artifact_id,
                ),
            )

    if is_valid:
        assert read_specification_attempt_outcome(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            attempt=attempt,
            member=member,
        ) == expected_outcome
    else:
        with pytest.raises(ValidationError, match="result count is inconsistent"):
            read_specification_attempt_outcome(
                fixture.registry_path,
                runtime_root=fixture.runtime_dir,
                attempt=attempt,
                member=member,
            )


def test_scientific_acceptance_records_two_fresh_sibling_results(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture)
    attempt = _append_attempt(fixture, snapshot, member)
    artifacts, recipe, memberships = _fresh_job(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        address="cohort",
        output_names=("summary", "detail"),
        selected_output_name="summary",
        token="fresh-complete",
    )
    resolution = _selected_resolution(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        output_name="summary",
        address="cohort",
        outcome="generated",
    )

    assert _record(
        fixture,
        token="fresh-complete",
        selected_step_name="fixture_analysis",
        selected_output_name="summary",
        artifacts=artifacts,
        recipes=(recipe,),
        resolutions=(resolution,),
        memberships=memberships,
        acceptance=SpecificationAcceptanceIntent(attempt=attempt, member=member),
    ) == 2

    attempt_row = _attempt_row(fixture.registry_path, attempt.attempt_id)
    assert attempt_row[5] == "complete"
    assert isinstance(attempt_row[6], int)
    assert attempt_row[7:] == (None, None)
    results = _attempt_results(fixture.registry_path, attempt.attempt_id)
    assert {(role, address) for role, address, _artifact_id in results} == {
        ("summary", "cohort"),
        ("diagnostic", "cohort"),
    }
    assert len({artifact_id for _role, _address, artifact_id in results}) == 2


def test_reuse_records_exact_artifacts_and_separates_selecting_from_producing_run(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture)
    producing_attempt = _append_attempt(fixture, snapshot, member)
    artifacts, recipe, memberships = _fresh_job(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        address="cohort",
        output_names=("summary", "detail"),
        selected_output_name="summary",
        token="reuse-producer",
    )
    generated = _selected_resolution(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        output_name="summary",
        address="cohort",
        outcome="generated",
    )
    _record(
        fixture,
        token="reuse-producer",
        selected_step_name="fixture_analysis",
        selected_output_name="summary",
        artifacts=artifacts,
        recipes=(recipe,),
        resolutions=(generated,),
        memberships=memberships,
        acceptance=SpecificationAcceptanceIntent(
            attempt=producing_attempt,
            member=member,
        ),
    )
    coordinates = frozenset(
        {
            ("fixture_analysis", "summary", "cohort"),
            ("fixture_analysis", "detail", "cohort"),
        }
    )
    reused = _reused_memberships(
        fixture.registry_path,
        context=fixture.context,
        workflow_name="base",
        coordinates=coordinates,
    )
    selected = next(intent for intent in reused if intent.row.output_name == "summary")
    assert selected.existing_artifact_id is not None
    resolution = _selected_resolution(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        output_name="summary",
        address="cohort",
        outcome="reused",
        existing_artifact_id=selected.existing_artifact_id,
    )
    with sqlite3.connect(fixture.registry_path) as connection:
        before_counts = connection.execute(
            "SELECT (SELECT COUNT(*) FROM artifacts), "
            "(SELECT COUNT(*) FROM artifact_dependencies)"
        ).fetchone()

    attempt = _append_attempt(fixture, snapshot, member)
    assert _record(
        fixture,
        token="reuse-selector",
        selected_step_name="fixture_analysis",
        selected_output_name="summary",
        resolutions=(resolution,),
        memberships=reused,
        acceptance=SpecificationAcceptanceIntent(attempt=attempt, member=member),
    ) == 0

    results = _attempt_results(fixture.registry_path, attempt.attempt_id)
    reused_ids = {intent.existing_artifact_id for intent in reused}
    assert {artifact_id for _role, _address, artifact_id in results} == reused_ids
    selecting_run_id = _attempt_row(fixture.registry_path, attempt.attempt_id)[6]
    with sqlite3.connect(fixture.registry_path) as connection:
        after_counts = connection.execute(
            "SELECT (SELECT COUNT(*) FROM artifacts), "
            "(SELECT COUNT(*) FROM artifact_dependencies)"
        ).fetchone()
        producing_run_ids = {
            int(row[0])
            for row in connection.execute(
                "SELECT DISTINCT run_id FROM artifacts WHERE artifact_id IN (?, ?)",
                tuple(sorted(int(value) for value in reused_ids if value is not None)),
            )
        }
    assert after_counts == before_counts
    assert selecting_run_id not in producing_run_ids
    terminal = _freeze_state(fixture.registry_path)
    assert not fail_specification_member_attempt(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        attempt=attempt,
        member=member,
        diagnostic=SpecificationFailureDiagnostic(
            stage="execution",
            summary="must not overwrite complete",
        ),
    )
    assert _freeze_state(fixture.registry_path) == terminal


def test_partial_uses_only_current_acceptance_not_primary_or_stale_memberships(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture, entity_target=True)
    first_attempt = _append_attempt(fixture, snapshot, member)
    artifacts: list[WorkflowOutputArtifactRow] = []
    recipes: list[RetainedJobProjectionRecipe] = []
    memberships: list[MembershipIntent] = []
    resolutions: list[SelectedOutputResolutionIntent] = []
    for address in ("entity_001", "entity_002"):
        job_artifacts, recipe, job_memberships = _fresh_job(
            context=fixture.context,
            workflow_name="base",
            step_name="fixture_transform",
            address=address,
            output_names=("left", "right"),
            selected_output_name="left",
            token=f"full-{address}",
        )
        artifacts.extend(job_artifacts)
        recipes.append(recipe)
        memberships.extend(job_memberships)
        resolutions.append(
            _selected_resolution(
                context=fixture.context,
                workflow_name="base",
                step_name="fixture_transform",
                output_name="left",
                address=address,
                outcome="generated",
            )
        )
    _record(
        fixture,
        token="full-entity-results",
        selected_step_name="fixture_transform",
        selected_output_name="left",
        artifacts=tuple(artifacts),
        recipes=tuple(recipes),
        resolutions=tuple(resolutions),
        memberships=tuple(memberships),
        acceptance=SpecificationAcceptanceIntent(
            attempt=first_attempt,
            member=member,
        ),
    )
    assert _attempt_row(fixture.registry_path, first_attempt.attempt_id)[5] == "complete"

    selected_coordinate = frozenset(
        {("fixture_transform", "left", "entity_001")}
    )
    reused = _reused_memberships(
        fixture.registry_path,
        context=fixture.context,
        workflow_name="base",
        coordinates=selected_coordinate,
    )
    artifact_id = reused[0].existing_artifact_id
    assert artifact_id is not None
    second_attempt = _append_attempt(fixture, snapshot, member)
    _record(
        fixture,
        token="partial-entity-results",
        selected_step_name="fixture_transform",
        selected_output_name="left",
        resolutions=(
            _selected_resolution(
                context=fixture.context,
                workflow_name="base",
                step_name="fixture_transform",
                output_name="left",
                address="entity_001",
                outcome="reused",
                existing_artifact_id=artifact_id,
            ),
            _selected_resolution(
                context=fixture.context,
                workflow_name="base",
                step_name="fixture_transform",
                output_name="left",
                address="entity_002",
                outcome=None,
            ),
        ),
        memberships=reused,
        acceptance=SpecificationAcceptanceIntent(
            attempt=second_attempt,
            member=member,
        ),
    )

    assert _attempt_row(fixture.registry_path, second_attempt.attempt_id)[5] == "partial"
    assert _attempt_results(fixture.registry_path, second_attempt.attempt_id) == (
        ("left", "entity_001", artifact_id),
    )
    with sqlite3.connect(fixture.registry_path) as connection:
        stale_count = connection.execute(
            """
            SELECT COUNT(*) FROM published_outputs
            WHERE context = ? AND workflow_name = 'base'
              AND step_name = 'fixture_transform'
              AND NOT (output_name = 'left' AND address = 'entity_001')
            """,
            (fixture.context,),
        ).fetchone()[0]
    assert stale_count == 3
    terminal = _freeze_state(fixture.registry_path)
    assert not fail_specification_member_attempt(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        attempt=second_attempt,
        member=member,
        diagnostic=SpecificationFailureDiagnostic(
            stage="execution",
            summary="must not overwrite partial",
        ),
    )
    assert _freeze_state(fixture.registry_path) == terminal


def test_upstream_only_acceptance_derives_failed_and_requires_execution_stage(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture)
    attempt = _append_attempt(fixture, snapshot, member)
    artifacts, recipe, memberships = _fresh_job(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_transform",
        address="entity_002",
        output_names=("left",),
        selected_output_name="left",
        token="upstream-survivor",
    )
    assert _record(
        fixture,
        token="upstream-survivor",
        selected_step_name="fixture_analysis",
        selected_output_name="summary",
        artifacts=artifacts,
        recipes=(recipe,),
        memberships=memberships,
        acceptance=SpecificationAcceptanceIntent(
            attempt=attempt,
            member=member,
            failure=SpecificationFailureDiagnostic(
                stage="execution",
                summary="  no requested result survived  ",
            ),
        ),
    ) == 1
    row = _attempt_row(fixture.registry_path, attempt.attempt_id)
    assert row[5] == "failed"
    assert isinstance(row[6], int)
    assert row[7:] == ("execution", "no requested result survived")
    assert _attempt_results(fixture.registry_path, attempt.attempt_id) == ()
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute(
            """
            SELECT COUNT(*) FROM published_outputs
            WHERE context = ? AND workflow_name = 'base'
              AND step_name = 'fixture_transform'
              AND output_name = 'left' AND address = 'entity_002'
            """,
            (fixture.context,),
        ).fetchone() == (1,)
    terminal = _freeze_state(fixture.registry_path)
    assert not fail_specification_member_attempt(
        fixture.registry_path,
        runtime_root=fixture.runtime_dir,
        attempt=attempt,
        member=member,
        diagnostic=SpecificationFailureDiagnostic(
            stage="acceptance",
            summary="must not overwrite transaction failure",
        ),
    )
    assert _freeze_state(fixture.registry_path) == terminal

    wrong_attempt = _append_attempt(fixture, snapshot, member)
    wrong_artifacts, wrong_recipe, wrong_memberships = _fresh_job(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_transform",
        address="entity_001",
        output_names=("left",),
        selected_output_name="left",
        token="wrong-failure-stage",
    )
    before = _table_rows(fixture.registry_path)
    with pytest.raises(ValidationError):
        _record(
            fixture,
            token="wrong-failure-stage",
            selected_step_name="fixture_analysis",
            selected_output_name="summary",
            artifacts=wrong_artifacts,
            recipes=(wrong_recipe,),
            memberships=wrong_memberships,
            acceptance=SpecificationAcceptanceIntent(
                attempt=wrong_attempt,
                member=member,
                failure=SpecificationFailureDiagnostic(
                    stage="planning",
                    summary="wrong stage for transaction-owned failure",
                ),
            ),
        )
    assert _table_rows(fixture.registry_path) == before


@pytest.mark.parametrize(
    "case",
    [
        "attempt-context",
        "workflow",
        "target",
        "canonical-member",
        "terminal-attempt",
        "artifact-coordinate",
        "runtime-binding",
    ],
)
def test_scientific_acceptance_rejects_representative_mismatches_atomically(
    registry_v18_fixture: RegistryV18Fixture,
    tmp_path: Path,
    case: str,
) -> None:
    fixture = registry_v18_fixture
    route = "migrated" if case == "artifact-coordinate" else "fresh"
    snapshot, member = _prepare_snapshot(fixture, route=route)
    attempt = _append_attempt(fixture, snapshot, member)
    intent_attempt = attempt
    intent_member = member
    workflow_name = "base"
    selected_output_name = "summary"
    memberships: tuple[MembershipIntent, ...] = ()
    recording_fixture = fixture
    copied_before: dict[str, tuple[tuple[object, ...], ...]] | None = None
    if case == "attempt-context":
        intent_attempt = replace(attempt, context="other")
    elif case == "workflow":
        workflow_name = "parameter-variant"
    elif case == "target":
        selected_output_name = "detail"
    elif case == "canonical-member":
        payload = _specification_payload()
        fixed = payload["fixed"]
        assert isinstance(fixed, dict)
        fixed["results"] = {
            "renamed": {"step": "fixture_analysis", "output": "summary"},
            "diagnostic": {"step": "fixture_analysis", "output": "detail"},
        }
        other = build_snapshot(fixture, payload=payload)
        intent_member = _persist_snapshot(fixture, other)
    elif case == "terminal-attempt":
        assert fail_specification_member_attempt(
            fixture.registry_path,
            runtime_root=fixture.runtime_dir,
            attempt=attempt,
            member=member,
            diagnostic=SpecificationFailureDiagnostic(
                stage="planning",
                summary="already terminal",
            ),
        )
    elif case == "artifact-coordinate":
        with sqlite3.connect(fixture.registry_path) as connection:
            path, digest, output_hash = connection.execute(
                """
                SELECT path, content_digest, output_hash
                FROM artifacts WHERE artifact_id = ?
                """,
                (fixture.selected_artifact_id,),
            ).fetchone()
        memberships = (
            MembershipIntent(
                row=PublishedOutputRow(
                    context=fixture.context,
                    workflow_name="base",
                    step_name="fixture_analysis",
                    output_name="detail",
                    address="cohort",
                    path=str(path),
                    output_digest=str(digest),
                    output_hash=str(output_hash),
                ),
                existing_artifact_id=fixture.selected_artifact_id,
            ),
        )
    elif case == "runtime-binding":
        copied_runtime = tmp_path / "copied-runtime"
        copied_registry = copied_runtime / "database/registry.db"
        copied_registry.parent.mkdir(parents=True)
        shutil.copy2(fixture.registry_path, copied_registry)
        recording_fixture = replace(
            fixture,
            runtime_dir=copied_runtime,
            registry_path=copied_registry,
        )
        copied_before = _table_rows(copied_registry)

    before = _table_rows(fixture.registry_path)
    with pytest.raises(ValidationError):
        _record(
            recording_fixture,
            token=f"mismatch-{case}",
            workflow_name=workflow_name,
            selected_step_name="fixture_analysis",
            selected_output_name=selected_output_name,
            memberships=memberships,
            acceptance=SpecificationAcceptanceIntent(
                attempt=intent_attempt,
                member=intent_member,
                failure=SpecificationFailureDiagnostic(
                    stage="execution",
                    summary="expected mismatch",
                ),
            ),
        )
    assert _table_rows(fixture.registry_path) == before
    if copied_before is not None:
        assert _table_rows(recording_fixture.registry_path) == copied_before


@pytest.mark.parametrize(
    "checkpoint",
    [
        "after_workflow_run",
        "after_ordinary_acceptance",
        "after_specification_results",
        "after_specification_terminalization",
    ],
)
def test_scientific_acceptance_checkpoints_restore_all_registry_state(
    registry_v18_fixture: RegistryV18Fixture,
    checkpoint: str,
) -> None:
    fixture = registry_v18_fixture
    snapshot, member = _prepare_snapshot(fixture, route="migrated")
    attempt = _append_attempt(fixture, snapshot, member)
    artifacts, recipe, memberships = _fresh_job(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        address="cohort",
        output_names=("summary", "detail"),
        selected_output_name="summary",
        token=f"rollback-{checkpoint}",
    )
    resolution = _selected_resolution(
        context=fixture.context,
        workflow_name="base",
        step_name="fixture_analysis",
        output_name="summary",
        address="cohort",
        outcome="generated",
    )
    before = _table_rows(fixture.registry_path)

    def fail_at(current: str) -> None:
        if current == checkpoint:
            raise RuntimeError(current)

    with pytest.raises(RuntimeError, match=checkpoint):
        _record(
            fixture,
            token=f"rollback-{checkpoint}",
            selected_step_name="fixture_analysis",
            selected_output_name="summary",
            artifacts=artifacts,
            recipes=(recipe,),
            resolutions=(resolution,),
            memberships=memberships,
            acceptance=SpecificationAcceptanceIntent(
                attempt=attempt,
                member=member,
            ),
            fault_hook=fail_at,
        )

    assert _table_rows(fixture.registry_path) == before
    assert _attempt_row(fixture.registry_path, attempt.attempt_id)[4:] == (
        None,
        None,
        None,
        None,
        None,
    )


@pytest.mark.parametrize("explicit_none", [False, True])
def test_ordinary_default_bypasses_all_specification_state(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    explicit_none: bool,
) -> None:
    fixture = registry_v18_fixture
    snapshot, _member = _prepare_snapshot(fixture)
    specification_before = {
        name: rows
        for name, rows in _table_rows(
            fixture.registry_path,
            include=lambda name: name.startswith("specification_"),
        ).items()
        if name != "sqlite_sequence"
    }
    with sqlite3.connect(fixture.registry_path) as connection:
        runs_before = int(
            connection.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0]
        )

    def forbidden_specification_path(*_args: object, **_kwargs: object) -> object:
        pytest.fail("ordinary recording entered specification validation")

    monkeypatch.setattr(
        registry,
        "_validate_specification_acceptance_state",
        forbidden_specification_path,
    )
    monkeypatch.setattr(
        registry,
        "_validate_snapshot_registry_binding",
        forbidden_specification_path,
    )
    kwargs: dict[str, object] = {
        "runtime_root": fixture.runtime_dir,
        "context": fixture.context,
        "workflow_name": "base",
        "selected_step_name": "fixture_analysis",
        "selected_output_name": "summary",
        "run_workspace": "runs/test/ordinary",
        "run_plan_path": "runs/test/ordinary/run_plan.json",
        "run_plan_digest": sha256_digest(b"ordinary"),
        "artifacts": (),
        "projection_recipes": (),
        "reused_projection_seeds": (),
        "selected_resolution_intents": (),
        "environment_observation": EnvironmentObservationV1(
            nipact_version="test",
            python_version="test",
            platform="test",
            snakemake_version="test",
        ),
        "manifest_bindings": (),
        "membership_intents": (),
    }
    if explicit_none:
        kwargs["specification_acceptance"] = None
    assert record_workflow_run(fixture.registry_path, **kwargs) == 0  # type: ignore[arg-type]

    specification_after = {
        name: rows
        for name, rows in _table_rows(
            fixture.registry_path,
            include=lambda name: name.startswith("specification_"),
        ).items()
        if name != "sqlite_sequence"
    }
    with sqlite3.connect(fixture.registry_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM workflow_runs"
        ).fetchone() == (runs_before + 1,)
    assert specification_after == specification_before
    assert read_specification_snapshot(
        fixture.registry_path,
        context=fixture.context,
        snapshot_digest=snapshot.snapshot_digest,
    ) == snapshot
