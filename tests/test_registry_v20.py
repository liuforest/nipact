"""V20 metadata storage and real pre-directory scientific compatibility."""

from dataclasses import replace
from pathlib import Path
import sqlite3

import pytest

from conftest import RegistryV20Fixture, registry_schema_signature
from nipact.errors import ValidationError
from nipact.execution import build_run_plan, execute_run_plan
from nipact.hashing import DIRECTORY_DIGEST_SCHEME
from nipact.projection import (
    OutputContract,
    RequestBundleProjectionPlanV3,
    RequestedOutputCoordinate,
    SiblingOutput,
    StepContract,
    UpstreamRequestedOutputBindingPlan,
)
from nipact.registry import (
    ArtifactInputRow,
    EnvironmentObservationV1,
    RetainedJobProjectionRecipe,
    WorkflowOutputArtifactRow,
    initialize_registry_db,
    list_artifacts,
    list_upstream_dependencies,
    read_specification_snapshot,
    record_workflow_run,
    registry_schema_signature_digest,
)
from nipact.specification_execution import (
    freeze_specification_snapshot,
    run_specification_member,
)
from nipact.specification_loading import ExplicitSpecificationSource
from test_registry_v18_compatibility import _application_rows


def test_historical_v19_schema_and_exact_file_specification_reuse(
    registry_v20_fixture: RegistryV20Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v20_fixture
    assert registry_schema_signature_digest(
        registry_schema_signature(fixture.backup_path)
    ) == ("1921260bec90357e938a59cf9bca1ad3ce62c426c02e6cfd711ae6f46cae1919")
    before = _application_rows(fixture.registry_path)
    digest = "ce8a9390ee6d47a07d2ea62b137a84eff6c875500f09645169cef659819cb27d"
    snapshot = read_specification_snapshot(
        fixture.registry_path,
        context=fixture.context,
        snapshot_digest=digest,
    )
    assert snapshot.canonical_bytes == before["specification_snapshots"][0][2]
    replay = freeze_specification_snapshot(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=ExplicitSpecificationSource(fixture.project_dir / "compatibility.yaml"),
    )
    assert not replay.inserted
    assert replay.snapshot == snapshot

    def reject(*_args: object, **_kwargs: object) -> None:
        pytest.fail("historical reuse invoked Snakemake")

    monkeypatch.setattr("nipact.execution._run_snakemake", reject)
    member = run_specification_member(
        project_dir=fixture.project_dir,
        context=fixture.context,
        snapshot_digest=digest,
        member_key="member-000001",
    )
    ordinary = execute_run_plan(
        build_run_plan(
            project_dir=fixture.project_dir,
            context=fixture.context,
            workflow_name="base",
            step_name="fixture_transform",
        ),
        cores=1,
    )
    assert member.outcome == "complete"
    for outcome in (member.ordinary_outcome, ordinary):
        assert outcome.all_selected_resolved
        assert (outcome.selected_generated_count, outcome.selected_reused_count) == (
            0,
            1,
        )
    after = _application_rows(fixture.registry_path)
    for table in (
        "specification_snapshots",
        "specification_members",
        "specification_expected_results",
        "specification_snapshot_manifest_values",
        "artifact_dependencies",
        "parameters",
        "request_bundle_projections",
    ):
        assert after[table] == before[table]
    # Source reconciliation may refresh its observation time; produced science cannot change.
    assert after["artifacts"][1:] == before["artifacts"][1:]
    assert len(after["artifacts"]) == len(before["artifacts"])
    assert (
        after["specification_member_attempts"][0]
        == before["specification_member_attempts"][0]
    )
    assert (
        after["specification_attempt_results"][:2]
        == before["specification_attempt_results"]
    )
    with sqlite3.connect(fixture.registry_path) as conn:
        assert conn.execute(
            "SELECT role, artifact_id, run_id FROM specification_attempt_results "
            "JOIN artifacts USING(artifact_id) WHERE attempt_id = ? ORDER BY role",
            (member.attempt.attempt_id,),
        ).fetchall() == [("left", 3, 1), ("right", 4, 1)]
        assert conn.execute(
            "SELECT COUNT(*) FROM specification_member_attempts"
        ).fetchone() == (2,)


def _record_mixed_metadata(
    database: Path, runtime: Path, *, mismatch: bool = False
) -> None:
    tree = WorkflowOutputArtifactRow(
        step_name="producer",
        output_name="tree",
        address="entity",
        job_id="producer",
        path="runs/manual/tree",
        staging_path="runs/manual/tree",
        published_path=None,
        content_digest="a" * 64,
        output_hash="a" * 16,
        file_size=13,
        extension=None,
        parameters_json="{}",
        callable_ref="example:produce",
        is_selected_output=False,
        is_published=False,
        input_records=(),
        kind="directory",
        digest_scheme=DIRECTORY_DIGEST_SCHEME,
    )
    file = replace(
        tree,
        output_name="file",
        path="runs/manual/file.txt",
        staging_path="runs/manual/file.txt",
        content_digest="b" * 64,
        output_hash="b" * 16,
        extension=".txt",
        kind="file",
        digest_scheme="sha256",
    )
    consumer = replace(
        file,
        step_name="consumer",
        job_id="consumer",
        callable_ref="example:consume",
        input_records=(
            ArtifactInputRow(
                binding_name="tree",
                input_path=tree.path,
                dependency_role="source_input",
                origin="workflow_output",
                source_step_name="producer",
                source_output_name="tree",
                source_address="entity",
                source_is_reused=False,
            ),
        ),
    )
    recipes = []
    for step, outputs, bindings in (
        (
            "producer",
            (
                SiblingOutput(
                    "tree",
                    ".txt" if mismatch else None,
                    "file" if mismatch else "directory",
                ),
                SiblingOutput("file", ".txt"),
            ),
            (),
        ),
        (
            "consumer",
            (SiblingOutput("file", ".txt"),),
            (
                UpstreamRequestedOutputBindingPlan(
                    "tree",
                    RequestedOutputCoordinate("metadata", "producer", "tree", "entity"),
                ),
            ),
        ),
    ):
        plan = RequestBundleProjectionPlanV3(
            identity_contract_version=3,
            namespace="metadata",
            step_contract=StepContract(
                step,
                "1",
                "example:produce" if step == "producer" else "example:consume",
                "2",
            ),
            address="entity",
            canonical_parameters={},
            role_labelled_binding_plans=bindings,
            result_affecting_settings={},
            determinism_contract="deterministic",
            output_contract=OutputContract(
                2 if step == "producer" and not mismatch else 1, outputs
            ),
        )
        recipes.append(
            RetainedJobProjectionRecipe(
                step, "entity", tuple(o.output_name for o in outputs), plan
            )
        )
    record_workflow_run(
        database,
        runtime_root=runtime,
        context="metadata",
        workflow_name="base",
        selected_step_name="consumer",
        selected_output_name="file",
        run_workspace="runs/manual",
        run_plan_path="runs/manual/plan.json",
        run_plan_digest="c" * 64,
        artifacts=(tree, file, consumer),
        projection_recipes=recipes,
        reused_projection_seeds=(),
        selected_resolution_intents=(),
        environment_observation=EnvironmentObservationV1(
            "test", "test", "test", "test"
        ),
        manifest_bindings=(),
        membership_intents=(),
    )


def test_recorded_mixed_metadata_and_directory_dependency_round_trip(
    tmp_path: Path,
) -> None:
    database = tmp_path / "database/registry.db"
    database.parent.mkdir()
    initialize_registry_db(
        database,
        context="metadata",
        runtime_root=tmp_path,
        manifests={},
        manifest_paths={},
    )
    with pytest.raises(ValidationError, match="does not match output contract"):
        _record_mixed_metadata(database, tmp_path, mismatch=True)
    assert list_artifacts(database, context="metadata") == []
    _record_mixed_metadata(database, tmp_path)
    artifacts = {
        (a.step_name, a.output_name): a
        for a in list_artifacts(database, context="metadata")
    }
    tree = artifacts[("producer", "tree")]
    assert (tree.kind, tree.digest_scheme, tree.extension, tree.file_size) == (
        "directory",
        DIRECTORY_DIGEST_SCHEME,
        None,
        13,
    )
    file = artifacts[("producer", "file")]
    assert (file.kind, file.digest_scheme, file.extension) == ("file", "sha256", ".txt")
    (dependency,) = list_upstream_dependencies(
        database, artifact_id=artifacts[("consumer", "file")].artifact_id
    )
    assert (
        dependency.source_artifact_id,
        dependency.source_content_digest,
        dependency.source_file_size,
        dependency.source_extension,
    ) == (tree.artifact_id, "a" * 64, 13, None)


@pytest.mark.parametrize(
    "kind,scheme,extension",
    [
        ("unknown", "sha256", ".txt"),
        ("file", "unknown", ".txt"),
        ("file", "sha256", None),
        ("directory", "sha256", None),
        ("directory", DIRECTORY_DIGEST_SCHEME, ""),
    ],
)
def test_invalid_content_facts_rejected_by_records_and_storage(
    registry_v20_fixture: RegistryV20Fixture,
    kind: str,
    scheme: str,
    extension: str | None,
) -> None:
    fixture = registry_v20_fixture
    artifact = list_artifacts(fixture.registry_path, context=fixture.context)[-1]
    with pytest.raises(ValidationError, match="invalid artifact"):
        replace(artifact, kind=kind, digest_scheme=scheme, extension=extension)
    with sqlite3.connect(fixture.registry_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE artifacts SET kind=?, digest_scheme=?, extension=? WHERE artifact_id=?",
                (kind, scheme, extension, artifact.artifact_id),
            )


def test_external_sources_and_direct_source_dependency_extensions_stay_file_only(
    registry_v20_fixture: RegistryV20Fixture,
) -> None:
    fixture = registry_v20_fixture
    source = list_artifacts(fixture.registry_path, context=fixture.context)[0]
    with pytest.raises(ValidationError, match="source artifacts must be files"):
        replace(
            source,
            kind="directory",
            digest_scheme=DIRECTORY_DIGEST_SCHEME,
            extension=None,
        )
    with sqlite3.connect(fixture.registry_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE artifacts SET kind='directory', digest_scheme=?, extension=NULL WHERE artifact_id=1",
                (DIRECTORY_DIGEST_SCHEME,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE artifact_dependencies SET source_extension=NULL WHERE source_scope IS NOT NULL"
            )
