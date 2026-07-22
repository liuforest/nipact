import json
import os
from pathlib import Path
import sqlite3

import pytest
import yaml

import nipact.registry as registry_module
from nipact.cli import main
from nipact.artifacts import canonical_output_path
from nipact.examples.colors_processing_demo.model import DEFAULT_ENTITY_COUNT
from nipact.examples.colors_processing_demo.demo_names import (
    analysis_entity_ids,
    fit_cohort_entity_ids,
)
from nipact.examples.dynamic_functional_connectivity_demo import (
    project_template as dfc_template,
)
from nipact.examples.fmri_preprocessing_demo import project_template as fmri_template
from nipact.errors import ValidationError
from nipact.hashing import sha256_digest, sha256_file_digest, short_hash
from nipact.manifest import (
    MANIFEST_VALUE_SCHEMA,
    build_manifest,
    build_manifest_value,
)
from nipact.registry import REGISTRY_SCHEMA_VERSION
from nipact.source_authority import (
    LogicalSourceCoordinate,
    SourceDeclaration,
    observe_source_authority,
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
    output = capsys.readouterr().out
    assert "PASS: init" in output
    assert "manifest_hash=90ddcf303284f890" in output
    return project_dir, runtime_dir


def _init_named_demo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    *,
    demo: str,
) -> tuple[Path, Path]:
    project_dir = tmp_path / f"{demo}_project"
    runtime_dir = tmp_path / f"{demo}_runtime"

    assert (
        _run_main_from(
            tmp_path,
            [
                "init",
                "--demo",
                demo,
                "--project-dir",
                project_dir.name,
                "--runtime-dir",
                runtime_dir.name,
                "--context",
                demo,
            ],
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "PASS: init" in output
    assert "source_index=sources.yaml" in output
    return project_dir, runtime_dir


def _write_generic_prepared_project(
    tmp_path: Path,
    *,
    context: str = "mini",
) -> tuple[Path, Path]:
    project_dir = tmp_path / "generic_project"
    runtime_dir = tmp_path / "generic_runtime"
    (project_dir / "manifests").mkdir(parents=True)
    (project_dir / "steps").mkdir()
    (project_dir / "workflows").mkdir()
    runtime_dir.mkdir()

    _write_project_config(
        project_dir,
        {
            "context": context,
            "paths": {
                "runtime": "../generic_runtime",
            },
            "sources": {
                "index": "sources.yaml",
            },
            "manifests": {
                "subjects": "manifests/subjects.yaml",
            },
            "steps": {
                "directory": "steps",
            },
            "workflows": {
                "main": "workflows/main.yaml",
            },
        },
    )
    _write_yaml(
        project_dir / "sources.yaml",
        {
            "entities": {
                "sub_001": {
                    "source_text": "data/source/sub_001.txt",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "manifests/subjects.yaml",
        {
            "description": "Minimal generic prepared-project manifest",
            "entities": ["sub_001"],
        },
    )
    _write_yaml(
        project_dir / "steps/source_text.yaml",
        {
            "step_name": "source_text",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.colors_processing_demo.runtime:"
                "import_color_source_file"
            ),
            "source_inputs": ["source_text"],
            "outputs": {
                "raw_text": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "workflows/main.yaml",
        {
            "workflow_name": "main",
            "execution_population": "subjects",
            "steps": [
                {
                    "step_name": "source_text",
                    "output_name": "raw_text",
                },
            ],
        },
    )
    return project_dir, runtime_dir


def _write_project_config(project_dir: Path, payload: dict[str, object]) -> None:
    (project_dir / "nipact.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _read_project_config(project_dir: Path) -> dict[str, object]:
    payload = yaml.safe_load((project_dir / "nipact.yaml").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _assert_validate_passes(
    project_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "validated_manifests=2" in output
    assert "parsed_workflow_files=2" in output
    assert "parsed_step_files=8" in output
    assert "source_entities=200" in output
    assert "published_outputs=0" in output
    assert "PASS: validate" in output


def _insert_published_output(
    runtime_dir: Path,
    *,
    workflow_name: str = "base",
    step_name: str = "color_sector_analysis",
    output_name: str = "sector_counts",
    address: str = "init",
    extension: str = ".json",
    payload: dict[str, object] | None = None,
) -> tuple[Path, str, str]:
    if payload is None:
        payload = {"status": "ok", "address": address}
    projection_json = json.dumps(
        {
            "address": address,
            "canonical_parameters": {},
            "determinism_contract": "deterministic",
            "identity_contract_version": 3,
            "namespace": "colors",
            "output_contract": {
                "output_contract_version": 1,
                "sibling_outputs": [
                    {"declared_extension": extension, "output_name": output_name}
                ],
            },
            "result_affecting_settings": {},
            "role_labelled_bindings": [],
            "step_contract": {
                "callable_ref": "tests:manual",
                "runner_contract_version": "2",
                "step_contract_id": step_name,
                "step_contract_version": "1",
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    projection_digest = sha256_digest(projection_json.encode("utf-8"))
    staging_path = runtime_dir / f"runs/colors/manual/{address}{extension}"
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_digest = sha256_file_digest(staging_path)
    output_hash = short_hash(output_digest)
    relative_path = canonical_output_path(
        context="colors",
        step_name=step_name,
        address=address,
        request_bundle_digest=projection_digest,
        output_name=output_name,
        output_hash=output_hash,
        declared_extension=extension,
    )
    output_path = runtime_dir / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.rename(output_path)
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        run_id = int(
            conn.execute(
                """
                INSERT INTO workflow_runs (
                    context, workflow_name, selected_step_name,
                    selected_output_name, run_workspace, run_plan_path,
                    run_plan_digest, resolution_summary_json,
                    environment_observation_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "colors",
                    workflow_name,
                    step_name,
                    output_name,
                    "runs/colors/manual",
                    "runs/colors/manual/run_plan.json",
                    "a" * 64,
                    '{"all_selected_resolved":true,"forced":false,"schema_version":1,"selected_outputs":[]}',
                    '{"nipact_version":"test","platform":"test","profile_version":1,"python_version":"test","snakemake_version":"test"}',
                    "2026-07-19T00:00:00+00:00",
                ),
            ).lastrowid
        )
        parameter_id = int(
            conn.execute(
                """
                INSERT INTO parameters (
                    hash_version, parameter_hash, parameter_digest, step_name,
                    parameters_json, created_at
                )
                VALUES (1, ?, ?, ?, '{}', ?)
                """,
                (
                    "b" * 16,
                    "b" * 64,
                    step_name,
                    "2026-07-19T00:00:00+00:00",
                ),
            ).lastrowid
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO request_bundle_projections (
                request_bundle_digest, projection_json
            )
            VALUES (?, ?)
            """,
            (projection_digest, projection_json),
        )
        artifact_id = int(
            conn.execute(
                """
                INSERT INTO artifacts (
                    origin, run_id, context, workflow_name, step_name,
                    output_name, address, job_id, parameter_id, path,
                    is_selected_output, is_published, published_path,
                    staging_path, content_digest, output_hash, file_size,
                    extension, callable_ref, request_bundle_digest, created_at
                )
                VALUES (
                    'workflow_output', ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    run_id,
                    "colors",
                    workflow_name,
                    step_name,
                    output_name,
                    address,
                    f"{step_name}:{address}",
                    parameter_id,
                    relative_path,
                    relative_path,
                    f"runs/colors/manual/staging/{step_name}/{output_name}/{address}{extension}",
                    output_digest,
                    output_hash,
                    output_path.stat().st_size,
                    extension,
                    "tests:manual",
                    projection_digest,
                    "2026-07-19T00:00:00+00:00",
                ),
            ).lastrowid
        )
        conn.execute(
            """
            INSERT INTO published_outputs (
                context, workflow_name, step_name, output_name, address,
                path, output_digest, output_hash, artifact_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "colors",
                workflow_name,
                step_name,
                output_name,
                address,
                relative_path,
                output_digest,
                output_hash,
                artifact_id,
            ),
        )
    return output_path, output_digest, output_hash


def _assert_validate_fails(
    project_dir: Path,
    capsys: pytest.CaptureFixture[str],
    expected_error: str,
) -> None:
    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 1
    )
    assert expected_error in capsys.readouterr().err


def _assert_generic_validate_passes(
    project_dir: Path,
    capsys: pytest.CaptureFixture[str],
    *,
    context: str = "mini",
    published_outputs: int = 0,
) -> None:
    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                context,
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "validated_manifests=1" in output
    assert "parsed_workflow_files=1" in output
    assert "parsed_step_files=1" in output
    assert "source_entities=0" in output
    assert f"published_outputs={published_outputs}" in output
    assert "PASS: validate" in output


def test_init_creates_project_runtime_databases_and_validates(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)

    assert (project_dir / "nipact.yaml").is_file()
    assert (project_dir / "sources.yaml").is_file()
    assert (project_dir / "README.md").is_file()
    assert sorted(path.name for path in (project_dir / "manifests").glob("*.yaml")) == [
        "demo-40.yaml",
        "init.yaml",
    ]
    assert len(list((project_dir / "steps").glob("*.yaml"))) == 8
    assert len(list((project_dir / "workflows").glob("*.yaml"))) == 2
    for step_path in (project_dir / "steps").glob("*.yaml"):
        step_payload = yaml.safe_load(step_path.read_text(encoding="utf-8"))
        assert step_payload["step_contract_version"] == "1"

    config = _read_project_config(project_dir)
    assert config["sources"] == {
        "index": "sources.yaml",
    }
    assert config["manifests"] == {
        "init": "manifests/init.yaml",
        "demo-40": "manifests/demo-40.yaml",
    }
    source_index = yaml.safe_load(
        (project_dir / "sources.yaml").read_text(encoding="utf-8")
    )
    assert source_index == {
        "global": {
            "colors_source": "data/color_source.json",
        },
    }
    source_step = yaml.safe_load(
        (project_dir / "steps/color_source.yaml").read_text(encoding="utf-8")
    )
    assert source_step["source_inputs"] == ["colors_source"]

    init_manifest = yaml.safe_load(
        (project_dir / "manifests/init.yaml").read_text(encoding="utf-8")
    )
    assert set(init_manifest) == {"description", "entities"}
    assert init_manifest["description"] == "Full deterministic colors source population"
    assert init_manifest["entities"] == analysis_entity_ids()

    fit_manifest = yaml.safe_load(
        (project_dir / "manifests/demo-40.yaml").read_text(encoding="utf-8")
    )
    assert set(fit_manifest) == {"description", "entities"}
    assert fit_manifest["entities"] == fit_cohort_entity_ids()

    assert (runtime_dir / "data/color_source.json").is_file()
    assert (runtime_dir / "database/registry.db").is_file()
    assert not (runtime_dir / "database/analysis.db").exists()
    assert (runtime_dir / "outputs").is_dir()
    assert not (runtime_dir / "logs").exists()
    assert (runtime_dir / "manifests/generated").is_dir()

    source_payload = json.loads(
        (runtime_dir / "data/color_source.json").read_text(encoding="utf-8")
    )
    assert source_payload["metadata"]["entity_count"] == DEFAULT_ENTITY_COUNT
    assert [record["entity_id"] for record in source_payload["records"]] == analysis_entity_ids()

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        schema_version = conn.execute("PRAGMA user_version").fetchone()[0]
        context_row = conn.execute(
            """
            SELECT runtime_path, storage_layout_version
            FROM contexts
            WHERE context = 'colors'
            """
        ).fetchone()
        manifest_rows = conn.execute(
            """
            SELECT d.manifest_name, d.declared_path, v.value_schema,
                   v.entity_count, v.manifest_digest, v.canonical_body
            FROM manifest_declarations d
            JOIN manifest_values v
              ON v.value_schema = d.last_validated_manifest_value_schema
             AND v.manifest_digest = d.last_validated_manifest_digest
            ORDER BY d.manifest_name
            """
        ).fetchall()
        artifact_rows = conn.execute(
            """
            SELECT origin, context, path, content_digest, output_hash, file_size,
                   extension, source_metadata_json
            FROM artifacts
            ORDER BY artifact_id
            """
        ).fetchall()
        published_rows = conn.execute("SELECT * FROM published_outputs").fetchall()
    with registry_module._connect_readonly(runtime_dir / "database/registry.db") as conn:
        foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert schema_version == REGISTRY_SCHEMA_VERSION
    assert context_row == (str(runtime_dir), 1)
    assert foreign_keys == 1
    assert manifest_rows == [
        (
            "demo-40",
            "manifests/demo-40.yaml",
            MANIFEST_VALUE_SCHEMA,
            40,
            build_manifest_value(entities=fit_cohort_entity_ids()).manifest_digest,
            build_manifest_value(entities=fit_cohort_entity_ids()).canonical_body,
        ),
        (
            "init",
            "manifests/init.yaml",
            MANIFEST_VALUE_SCHEMA,
            200,
            build_manifest_value(entities=analysis_entity_ids()).manifest_digest,
            build_manifest_value(entities=analysis_entity_ids()).canonical_body,
        ),
    ]
    assert artifact_rows == []
    assert published_rows == []
    assert (runtime_dir / "outputs/v1").is_dir()
    assert list((runtime_dir / "outputs/v1").rglob("*")) == []

    _assert_validate_passes(project_dir, capsys)


def test_registry_manifest_values_deduplicate_and_reject_key_collisions(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "registry.db"
    manifest = build_manifest(description="Shared membership", entities=("a", "b"))
    manifests = {"first": manifest, "alias": manifest}
    manifest_paths = {
        "first": "manifests/first.yaml",
        "alias": "manifests/alias.yaml",
    }

    registry_module.initialize_prepared_demo_registry_db(
        registry_path,
        context="example",
        runtime_root=tmp_path,
        manifests=manifests,
        manifest_paths=manifest_paths,
    )
    with sqlite3.connect(registry_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM manifest_values").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM manifest_declarations").fetchone()[0]
            == 2
        )
        conn.execute(
            """
            UPDATE manifest_values
            SET canonical_body = 'different'
            WHERE value_schema = ? AND manifest_digest = ?
            """,
            (manifest.manifest_value_schema, manifest.manifest_digest),
        )

    with pytest.raises(ValidationError, match="digest does not match canonical body"):
        registry_module.read_manifest(
            registry_path,
            context="example",
            manifest_name="first",
        )
    with pytest.raises(
        ValidationError,
        match="manifest value does not match its schema-qualified key",
    ):
        registry_module.initialize_prepared_demo_registry_db(
            registry_path,
            context="example",
            runtime_root=tmp_path,
            manifests=manifests,
            manifest_paths=manifest_paths,
        )


@pytest.mark.parametrize(
    ("demo", "template", "step_count", "source_file_count"),
    [
        ("fmri", fmri_template, 2, 4),
        ("dfc", dfc_template, 3, 4),
    ],
)
def test_init_creates_prepared_neuro_demo_project_and_registry(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    demo: str,
    template: object,
    step_count: int,
    source_file_count: int,
) -> None:
    project_dir, runtime_dir = _init_named_demo(tmp_path, capsys, demo=demo)

    output = capsys.readouterr().out
    assert output == ""
    assert (project_dir / "nipact.yaml").is_file()
    assert (project_dir / "sources.yaml").is_file()
    assert (runtime_dir / "database/registry.db").is_file()
    assert (runtime_dir / "outputs").is_dir()
    assert (runtime_dir / "manifests/generated").is_dir()
    assert len(list((project_dir / "steps").glob("*.yaml"))) == step_count
    assert len(list((project_dir / "workflows").glob("*.yaml"))) == 1
    for step_path in (project_dir / "steps").glob("*.yaml"):
        step_payload = yaml.safe_load(step_path.read_text(encoding="utf-8"))
        assert step_payload["step_contract_version"] == "1"

    config = _read_project_config(project_dir)
    assert config["sources"] == {"index": "sources.yaml"}
    assert config["manifests"] == template.manifest_paths()
    assert sorted(
        path.relative_to(runtime_dir).as_posix()
        for path in runtime_dir.glob("data/**/*.npy")
    ) == sorted(template.source_file_paths())
    source_index = yaml.safe_load(
        (project_dir / "sources.yaml").read_text(encoding="utf-8")
    )
    assert source_index == template.source_index_payload()

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        schema_version = conn.execute("PRAGMA user_version").fetchone()[0]
        context_row = conn.execute(
            """
            SELECT runtime_path, storage_layout_version
            FROM contexts
            WHERE context = ?
            """,
            (demo,),
        ).fetchone()
        manifest_rows = conn.execute(
            """
            SELECT d.manifest_name, d.declared_path, v.value_schema,
                   v.entity_count
            FROM manifest_declarations d
            JOIN manifest_values v
              ON v.value_schema = d.last_validated_manifest_value_schema
             AND v.manifest_digest = d.last_validated_manifest_digest
            WHERE d.context = ?
            ORDER BY d.manifest_name
            """,
            (demo,),
        ).fetchall()
        artifact_rows = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE context = ?",
            (demo,),
        ).fetchone()[0]
    assert schema_version == REGISTRY_SCHEMA_VERSION
    assert context_row == (str(runtime_dir), 1)
    assert manifest_rows == [
        (
            name,
            template.manifest_paths()[name],
            MANIFEST_VALUE_SCHEMA,
            manifest.entity_count,
        )
        for name, manifest in sorted(template.build_manifests().items())
    ]
    assert artifact_rows == 0
    assert len(template.source_file_paths()) == source_file_count

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                demo,
            ]
        )
        == 0
    )
    validate_output = capsys.readouterr().out
    assert "validated_manifests=1" in validate_output
    assert "parsed_workflow_files=1" in validate_output
    assert f"parsed_step_files={step_count}" in validate_output
    assert "source_entities=0" in validate_output
    assert "published_outputs=0" in validate_output
    assert "PASS: validate" in validate_output


def test_validate_accepts_generic_prepared_project_without_registry_or_source_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _write_generic_prepared_project(tmp_path)

    assert not (runtime_dir / "database/registry.db").exists()
    assert not (runtime_dir / "data/source/sub_001.txt").exists()
    _assert_generic_validate_passes(project_dir, capsys)


def test_validate_fails_for_missing_project_config(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 1
    )
    assert "missing YAML file" in capsys.readouterr().err


def test_validate_fails_for_missing_manifest(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)
    (project_dir / "manifests/init.yaml").unlink()

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 1
    )
    assert "missing manifest file" in capsys.readouterr().err


def test_validate_fails_for_configured_manifest_escape(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)
    config = _read_project_config(project_dir)
    manifests = config["manifests"]
    assert isinstance(manifests, dict)
    manifests["init"] = "../outside.yaml"
    _write_project_config(project_dir, config)

    _assert_validate_fails(project_dir, capsys, "must stay inside project dir")


def test_validate_fails_for_missing_source_data(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    (runtime_dir / "data/color_source.json").unlink()

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 1
    )
    assert "missing JSON file" in capsys.readouterr().err


def test_validate_fails_for_changed_source_content(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    source_path = runtime_dir / "data/color_source.json"
    source_payload = json.loads(source_path.read_text(encoding="utf-8"))
    source_payload["records"][0]["value"] = 0.123
    source_path.write_text(json.dumps(source_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 1
    )
    assert "source data content" in capsys.readouterr().err


def test_validate_fails_for_malformed_database(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    (runtime_dir / "database/registry.db").write_text("not sqlite\n", encoding="utf-8")

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 1
    )
    assert "registry.db is malformed" in capsys.readouterr().err


def test_validate_accepts_source_before_first_authority_reconciliation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    assert (
        main(
            ["validate", "--project-dir", str(project_dir), "--context", "colors"]
        )
        == 0
    )
    assert "PASS: validate" in capsys.readouterr().out


def test_validate_accepts_registered_published_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    _insert_published_output(runtime_dir)

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "colors",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "published_outputs=1" in output
    assert "PASS: validate" in output


def test_validate_rejects_noncanonical_stored_request_projection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    _insert_published_output(runtime_dir)
    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        digest, projection_json = conn.execute(
            """
            SELECT request_bundle_digest, projection_json
            FROM request_bundle_projections
            """
        ).fetchone()
        conn.execute(
            """
            UPDATE request_bundle_projections
            SET projection_json = ?
            WHERE request_bundle_digest = ?
            """,
            (json.dumps(json.loads(projection_json), indent=2), digest),
        )

    _assert_validate_fails(project_dir, capsys, "not canonical JSON")


def test_validate_rejects_missing_upstream_request_projection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / "database/registry.db"
    payload = {
        "address": "init",
        "canonical_parameters": {},
        "determinism_contract": "deterministic",
            "identity_contract_version": 3,
        "namespace": "colors",
        "output_contract": {
            "output_contract_version": 1,
            "sibling_outputs": [
                {"declared_extension": ".json", "output_name": "output"}
            ],
        },
        "result_affecting_settings": {},
        "role_labelled_bindings": [
            {
                "output_name": "output",
                "role": "upstream",
                "upstream_request_bundle_digest": "f" * 64,
            }
        ],
        "step_contract": {
            "callable_ref": "tests:manual",
            "runner_contract_version": "2",
            "step_contract_id": "manual",
            "step_contract_version": "1",
        },
    }
    projection_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = sha256_digest(projection_json.encode("utf-8"))
    with sqlite3.connect(registry_path) as conn:
        conn.execute(
            """
            INSERT INTO request_bundle_projections (
                request_bundle_digest, projection_json
            )
            VALUES (?, ?)
            """,
            (digest, projection_json),
        )

    _assert_validate_fails(project_dir, capsys, "missing upstream projection")


def test_registry_v18_projection_observation_and_membership_constraints(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    registry_path = runtime_dir / "database/registry.db"
    observation = observe_source_authority(
        runtime_root=runtime_dir,
        declaration=SourceDeclaration(
            coordinate=LogicalSourceCoordinate(
                "colors", "global", "colors_source", None
            ),
            declared_path="data/color_source.json",
            declared_extension=".json",
        ),
        registered=None,
    )
    registry_module.reconcile_manifest_and_source_authorities(
        registry_path,
        context="colors",
        manifests={},
        manifest_paths={},
        observations=(observation,),
    )
    with sqlite3.connect(registry_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
        artifact_columns = {
            row[1]: row for row in conn.execute("PRAGMA table_info(artifacts)")
        }
        run_columns = {
            row[1]: row for row in conn.execute("PRAGMA table_info(workflow_runs)")
        }
        membership_columns = {
            row[1]: row for row in conn.execute("PRAGMA table_info(published_outputs)")
        }
        membership_foreign_keys = conn.execute(
            "PRAGMA foreign_key_list(published_outputs)"
        ).fetchall()
        membership_indexes = {
            row[1]: row for row in conn.execute("PRAGMA index_list(published_outputs)")
        }
        artifact_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'artifacts'"
        ).fetchone()[0]
        projection_columns = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info(request_bundle_projections)")
        }
        artifact_foreign_keys = conn.execute(
            "PRAGMA foreign_key_list(artifacts)"
        ).fetchall()
        dependency_columns = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info(artifact_dependencies)")
        }
        dependency_sql = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'artifact_dependencies'
            """
        ).fetchone()[0]
        manifest_value_count = conn.execute(
            "SELECT COUNT(*) FROM manifest_values"
        ).fetchone()[0]
        manifest_declaration_count = conn.execute(
            "SELECT COUNT(*) FROM manifest_declarations"
        ).fetchone()[0]

    assert "request_bundle_digest" in artifact_columns
    assert "identity_contract_version" not in artifact_columns
    assert "request_bundle_projection_json" not in artifact_columns
    assert set(projection_columns) == {"request_bundle_digest", "projection_json"}
    assert run_columns["resolution_summary_json"][3] == 1
    assert run_columns["environment_observation_json"][3] == 1
    assert membership_columns["artifact_id"][3] == 1
    artifact_fk = next(row for row in membership_foreign_keys if row[3] == "artifact_id")
    assert artifact_fk[2] == "artifacts"
    assert artifact_fk[6] == "RESTRICT"
    assert membership_indexes["published_outputs_artifact_id_idx"][2] == 0
    assert "origin = 'workflow_output'" in artifact_sql
    projection_fk = next(
        row for row in artifact_foreign_keys if row[3] == "request_bundle_digest"
    )
    assert projection_fk[2] == "request_bundle_projections"
    assert projection_fk[6] == "RESTRICT"
    assert "request_bundle_digest IS NOT NULL" in artifact_sql
    assert "origin = 'source'" in artifact_sql
    assert "request_bundle_digest IS NULL" in artifact_sql
    assert "manifest_value_schema" in dependency_columns
    assert "manifest_value_schema IS NULL" in dependency_sql
    assert "manifest_digest IS NULL" in dependency_sql
    assert manifest_value_count == 2
    assert manifest_declaration_count == 2

    with sqlite3.connect(registry_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        source_artifact_id = conn.execute(
            "SELECT artifact_id FROM artifacts WHERE origin = 'source' LIMIT 1"
        ).fetchone()[0]
        manifest_value_schema, manifest_digest = conn.execute(
            """
            SELECT last_validated_manifest_value_schema,
                   last_validated_manifest_digest
            FROM manifest_declarations
            WHERE context = 'colors' AND manifest_name = 'init'
            """
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            conn.execute(
                """
                INSERT INTO artifact_dependencies (
                    dependent_artifact_id, source_artifact_id,
                    source_content_digest, source_file_size, source_extension,
                    input_path, binding_name, dependency_role,
                    manifest_value_schema, manifest_digest
                )
                VALUES (?, ?, ?, 0, '.json', 'data/source.json', 'invalid_pair',
                        'source_input', ?, NULL)
                """,
                (
                    source_artifact_id,
                    source_artifact_id,
                    "e" * 64,
                    manifest_value_schema,
                ),
            )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"):
            conn.execute(
                """
                INSERT INTO artifact_dependencies (
                    dependent_artifact_id, source_artifact_id,
                    source_content_digest, source_file_size, source_extension,
                    input_path, binding_name, dependency_role,
                    manifest_value_schema, manifest_digest
                )
                VALUES (?, ?, ?, 0, '.json', 'data/source.json', 'missing_value',
                        'source_input', ?, ?)
                """,
                (
                    source_artifact_id,
                    source_artifact_id,
                    "e" * 64,
                    manifest_value_schema,
                    "f" * 64,
                ),
            )
        conn.execute(
            """
            INSERT INTO artifact_dependencies (
                dependent_artifact_id, source_artifact_id,
                source_content_digest, source_file_size, source_extension,
                input_path, binding_name, dependency_role,
                manifest_value_schema, manifest_digest
            )
            VALUES (?, ?, ?, 0, '.json', 'data/source.json', 'valid_value',
                    'source_input', ?, ?)
            """,
            (
                source_artifact_id,
                source_artifact_id,
                "e" * 64,
                manifest_value_schema,
                manifest_digest,
            ),
        )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            conn.execute(
                """
                INSERT INTO request_bundle_projections (
                    request_bundle_digest, projection_json
                )
                VALUES (?, '{}')
                """,
                ("A" * 64,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            conn.execute(
                """
                UPDATE artifacts
                SET request_bundle_digest = ?
                WHERE artifact_id = ?
                """,
                ("e" * 64, source_artifact_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            conn.execute(
                """
                INSERT INTO artifacts (
                    origin, context, path, content_digest, file_size, extension,
                    created_at
                )
                VALUES ('workflow_output', 'colors', 'outputs/missing.json', ?, 0,
                        '.json', '2026-07-19T00:00:00+00:00')
                """,
                ("e" * 64,),
            )


@pytest.mark.parametrize(
    ("output_artifact_path", "expected_error"),
    [
        ("../outside.json", "must be under outputs/v1/"),
        ("/tmp/outside.json", "must be relative to runtime dir"),
        ("data/outside.json", "must be under outputs/v1/"),
        (
            "outputs/colors/base/analysis/result/cohort.1234567890abcdef.json",
            "must be under outputs/v1/",
        ),
    ],
)
def test_validate_fails_for_published_output_path_escape(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    output_artifact_path: str,
    expected_error: str,
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    _insert_published_output(runtime_dir)
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        conn.execute(
            "UPDATE published_outputs SET path = ? WHERE context = ?",
            (output_artifact_path, "colors"),
        )

    _assert_validate_fails(project_dir, capsys, expected_error)
