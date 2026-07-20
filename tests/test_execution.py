import json
import os
import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import nipact.registry as registry_module
import nipact.execution as execution_module
from nipact.cli import main
from nipact.artifacts import output_filename, parse_output_filename
from nipact.errors import ValidationError
from nipact.execution import (
    _run_plan_payload,
    _run_snakemake,
    _snakefile_text,
    build_run_plan,
    execute_run_plan,
)
from nipact.hashing import is_valid_digest, sha256_digest, sha256_file_digest, short_hash
from nipact.projection import ResolvedRequestBundleProjectionV1
from nipact.registry import EnvironmentObservationV1
from nipact.runtime import run_job
from nipact.trace import build_trace_graph_for_workflow_coordinate


def _run_main_from(cwd: Path, argv: list[str]) -> int:
    old_cwd = Path.cwd()
    os.chdir(cwd)
    try:
        return main(argv)
    finally:
        os.chdir(old_cwd)


def _write_all_staged_outputs(
    run_plan: object,
    *,
    marker: str = "run",
    selected_payload: dict[str, object] | None = None,
) -> None:
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
            "marker": marker,
        }
        if (
            selected_payload is not None
            and (job.step_name, job.output_name, job.address) in selected_keys
        ):
            payload = dict(selected_payload)
            payload["marker"] = marker
        job.staging_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


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


def _read_yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_tiny_non_colors_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    module_dir = tmp_path / "importable"
    module_dir.mkdir()
    (module_dir / "mini_runtime.py").write_text(
        """
import json


def import_text_file(*, inputs, outputs, params, address):
    source_path = inputs["source_text"][0]
    output = outputs["raw_text"]
    output.write_text(
        json.dumps(
            {
                "address": address,
                "text": source_path.read_text(encoding="utf-8").strip(),
            },
            sort_keys=True,
        )
        + "\\n",
        encoding="utf-8",
    )


def uppercase_text_file(*, inputs, outputs, params, address):
    source_payload = json.loads(inputs["raw_text"][0].read_text(encoding="utf-8"))
    output = outputs["upper_text"]
    output.write_text(
        json.dumps(
            {
                "address": address,
                "text": source_payload["text"].upper(),
            },
            sort_keys=True,
        )
        + "\\n",
        encoding="utf-8",
    )
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    monkeypatch.setenv(
        "PYTHONPATH",
        str(module_dir)
        if not os.environ.get("PYTHONPATH")
        else f"{module_dir}{os.pathsep}{os.environ['PYTHONPATH']}",
    )

    project_dir = tmp_path / "mini_project"
    runtime_dir = tmp_path / "mini_runtime"
    (project_dir / "manifests").mkdir(parents=True)
    (project_dir / "steps").mkdir()
    (project_dir / "workflows").mkdir()
    (runtime_dir / "data/source").mkdir(parents=True)
    (runtime_dir / "database").mkdir()
    (runtime_dir / "outputs").mkdir()

    _write_yaml(
        project_dir / "nipact.yaml",
        {
            "context": "mini",
            "paths": {
                "runtime": "../mini_runtime",
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
                "sub_002": {
                    "source_text": "data/source/sub_002.txt",
                },
                "sub_unused": {
                    "source_text": "data/source/sub_unused.txt",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "manifests/subjects.yaml",
        {
            "description": "Tiny private Phase 18C smoke manifest",
            "entities": ["sub_001", "sub_002"],
        },
    )
    _write_yaml(
        project_dir / "steps/source_text.yaml",
        {
            "step_name": "source_text",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": "mini_runtime:import_text_file",
            "source_inputs": ["source_text"],
            "manifest_binding": {
                "role": "source_population",
                "manifest": "subjects",
            },
            "outputs": {
                "raw_text": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/uppercase_text.yaml",
        {
            "step_name": "uppercase_text",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "mini_runtime:uppercase_text_file",
            "inputs": {
                "raw_text": {
                    "artifact": "source_text.raw_text",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "upper_text": {
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
            "steps": [
                "source_text",
                {
                    "step_name": "uppercase_text",
                    "output_name": "upper_text",
                },
            ],
        },
    )

    for address, text in (
        ("sub_001", "alpha"),
        ("sub_002", "beta"),
        ("sub_unused", "unused"),
    ):
        (runtime_dir / f"data/source/{address}.txt").write_text(
            f"{text}\n",
            encoding="utf-8",
        )

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        registry_module._create_schema(conn)
        conn.execute(
            "INSERT INTO contexts (context, runtime_path) VALUES (?, ?)",
            ("mini", str(runtime_dir)),
        )
    return project_dir, runtime_dir


def _write_tiny_multi_output_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    module_dir = tmp_path / "multi_importable"
    module_dir.mkdir()
    (module_dir / "mini_multi_runtime.py").write_text(
        """
import json


def import_source_pair(*, inputs, outputs, params, address):
    source_path = inputs["source_text"][0]
    text = source_path.read_text(encoding="utf-8").strip()
    outputs["raw_text"].write_text(
        json.dumps({"address": address, "text": text}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )
    outputs["raw_qc"].write_text(
        json.dumps({"address": address, "length": len(text)}, sort_keys=True) + "\\n",
        encoding="utf-8",
    )


def import_missing_sibling(*, inputs, outputs, params, address):
    source_path = inputs["source_text"][0]
    outputs["raw_text"].write_text(
        json.dumps(
            {"address": address, "text": source_path.read_text(encoding="utf-8").strip()},
            sort_keys=True,
        )
        + "\\n",
        encoding="utf-8",
    )


def consume_qc_file(*, inputs, outputs, params, address):
    qc_payload = json.loads(inputs["source_qc"][0].read_text(encoding="utf-8"))
    outputs["qc_echo"].write_text(
        json.dumps(
            {
                "address": address,
                "source_output": "raw_qc",
                "length": qc_payload["length"],
            },
            sort_keys=True,
        )
        + "\\n",
        encoding="utf-8",
    )
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    monkeypatch.setenv(
        "PYTHONPATH",
        str(module_dir)
        if not os.environ.get("PYTHONPATH")
        else f"{module_dir}{os.pathsep}{os.environ['PYTHONPATH']}",
    )

    project_dir = tmp_path / "multi_project"
    runtime_dir = tmp_path / "multi_runtime"
    (project_dir / "manifests").mkdir(parents=True)
    (project_dir / "steps").mkdir()
    (project_dir / "workflows").mkdir()
    (runtime_dir / "data/source").mkdir(parents=True)
    (runtime_dir / "database").mkdir()
    (runtime_dir / "outputs").mkdir()

    _write_yaml(
        project_dir / "nipact.yaml",
        {
            "context": "multi",
            "paths": {
                "runtime": "../multi_runtime",
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
                "import_raw": "workflows/import_raw.yaml",
                "consume_qc": "workflows/consume_qc.yaml",
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
                "sub_002": {
                    "source_text": "data/source/sub_002.txt",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "manifests/subjects.yaml",
        {
            "description": "Tiny private Phase 19A multi-output manifest",
            "entities": ["sub_001", "sub_002"],
        },
    )
    _write_yaml(
        project_dir / "steps/source_text.yaml",
        {
            "step_name": "source_text",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": "mini_multi_runtime:import_source_pair",
            "source_inputs": ["source_text"],
            "manifest_binding": {
                "role": "source_population",
                "manifest": "subjects",
            },
            "outputs": {
                "raw_text": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
                "raw_qc": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "steps/qc_echo.yaml",
        {
            "step_name": "qc_echo",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "mini_multi_runtime:consume_qc_file",
            "inputs": {
                "source_qc": {
                    "artifact": "source_text.raw_qc",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "qc_echo": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    )
    _write_yaml(
        project_dir / "workflows/import_raw.yaml",
        {
            "workflow_name": "import_raw",
            "steps": [
                {
                    "step_name": "source_text",
                    "output_name": "raw_text",
                },
            ],
        },
    )
    _write_yaml(
        project_dir / "workflows/consume_qc.yaml",
        {
            "workflow_name": "consume_qc",
            "steps": [
                "source_text",
                {
                    "step_name": "qc_echo",
                    "output_name": "qc_echo",
                },
            ],
        },
    )

    for address, text in (("sub_001", "alpha"), ("sub_002", "beta")):
        (runtime_dir / f"data/source/{address}.txt").write_text(
            f"{text}\n",
            encoding="utf-8",
        )

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        registry_module._create_schema(conn)
        conn.execute(
            "INSERT INTO contexts (context, runtime_path) VALUES (?, ?)",
            ("multi", str(runtime_dir)),
        )
    return project_dir, runtime_dir


def test_output_filename_parses_declared_extension_not_path_suffix() -> None:
    filename = output_filename(
        address="sub-001",
        output_hash="1234567890abcdef",
        declared_extension=".nii.gz",
    )

    assert filename == "sub-001.1234567890abcdef.nii.gz"
    assert parse_output_filename(filename, declared_extension=".nii.gz") == (
        "sub-001",
        "1234567890abcdef",
    )


def test_build_run_plan_for_base_entity_step(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_local_transform",
    )

    assert run_plan.runtime_root == runtime_dir
    assert run_plan.workflow_name == "base"
    assert run_plan.selected_step_name == "color_local_transform"
    assert run_plan.selected_output_name == "local_color"
    assert run_plan.run_workspace == (
        runtime_dir / "runs/colors/base/color_local_transform"
    )
    assert len(run_plan.published_outputs) == len(run_plan.jobs)
    assert run_plan.published_outputs[0].address == "color_000"
    assert run_plan.published_outputs[-1].address == "color_199"
    assert (
        "color_local_transform",
        "local_color",
        "color_000",
    ) in {
        (spec.step_name, spec.output_name, spec.address)
        for spec in run_plan.published_outputs
    }
    assert len(run_plan.jobs) == 600
    assert run_plan.jobs[0].staging_path == (
        runtime_dir
        / "runs/colors/base/color_local_transform/staging/"
        "color_source/source_color/color_000.json"
    )
    assert run_plan.selected_fresh_jobs[0].staging_path == (
        runtime_dir
        / "runs/colors/base/color_local_transform/staging/"
        "color_local_transform/local_color/color_000.json"
    )


def test_build_run_plan_for_base_cohort_step(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )

    assert run_plan.run_workspace == (
        runtime_dir / "runs/colors/base/color_sector_analysis"
    )
    assert len(run_plan.published_outputs) == len(run_plan.jobs)
    assert (
        "color_sector_analysis",
        "sector_counts",
        "init",
    ) in {
        (spec.step_name, spec.output_name, spec.address)
        for spec in run_plan.published_outputs
    }
    assert len(run_plan.selected_fresh_jobs) == 1
    assert run_plan.selected_fresh_jobs[0].inputs["sector_label"][-1].endswith(
        "color_sector_label/sector_label/color_199.json"
    )


def test_build_run_plan_rejects_missing_source_binding(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)
    step_path = project_dir / "steps/color_source.yaml"
    step_payload = _read_yaml(step_path)
    step_payload["source_inputs"] = ["missing_source"]
    _write_yaml(step_path, step_payload)

    with pytest.raises(ValidationError, match="missing source binding 'missing_source'"):
        build_run_plan(
            project_dir=project_dir,
            context="colors",
            workflow_name="base",
            step_name="color_local_transform",
        )


def test_build_run_plan_with_address_selects_one_entity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        address="sub_001",
    )

    assert run_plan.requested_address == "sub_001"
    assert len(run_plan.selected_fresh_output_refs) == 1
    assert run_plan.selected_fresh_output_refs[0].address == "sub_001"
    assert run_plan.selected_reused_output_refs == ()
    assert len(run_plan.selected_fresh_jobs) == 1
    # Plan construction stays population-wide; selection narrows targets, not jobs.
    assert {job.address for job in run_plan.jobs} == {"sub_001", "sub_002"}
    assert {spec.address for spec in run_plan.published_outputs} == {"sub_001"}
    payload = _run_plan_payload(run_plan)
    assert payload["requested_address"] == "sub_001"
    assert payload["selected_outputs"] == [
        {
            "step_name": "uppercase_text",
            "output_name": "upper_text",
            "address": "sub_001",
        }
    ]
    assert payload["selected_fresh_outputs"] == [
        {
            "step_name": "uppercase_text",
            "output_name": "upper_text",
            "address": "sub_001",
            "staging_path": "staging/uppercase_text/upper_text/sub_001.json",
        }
    ]
    assert payload["selected_reused_outputs"] == []


def test_selected_output_partition_rejects_duplicates_and_missing_coordinates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        address="sub_001",
    )
    selected_ref = run_plan.selected_fresh_output_refs[0]

    with pytest.raises(ValidationError, match="coordinate is duplicated"):
        execution_module._validate_selected_output_partition(
            selected_step_name="uppercase_text",
            selected_output_name="upper_text",
            selected_addresses=("sub_001",),
            selected_fresh_output_refs=(selected_ref, selected_ref),
            selected_reused_output_refs=(),
        )
    with pytest.raises(ValidationError, match="do not cover"):
        execution_module._validate_selected_output_partition(
            selected_step_name="uppercase_text",
            selected_output_name="upper_text",
            selected_addresses=("sub_001",),
            selected_fresh_output_refs=(),
            selected_reused_output_refs=(),
        )


@pytest.mark.parametrize(
    ("registry_mutation", "message"),
    [
        ("missing_context", "unknown context: mini"),
        ("runtime_mismatch", "registry.db context runtime path is out of date"),
        ("incompatible_schema", "registry.db schema version is incompatible"),
    ],
)
def test_build_run_plan_validates_registry_binding_before_workspace_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    registry_mutation: str,
    message: str,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        if registry_mutation == "missing_context":
            conn.execute("DELETE FROM contexts WHERE context = 'mini'")
        elif registry_mutation == "runtime_mismatch":
            conn.execute(
                "UPDATE contexts SET runtime_path = ? WHERE context = 'mini'",
                (str(tmp_path / "other-runtime"),),
            )
        else:
            conn.execute("PRAGMA user_version = 14")

    with pytest.raises(ValidationError, match=message):
        build_run_plan(
            project_dir=project_dir,
            context="mini",
            workflow_name="main",
            step_name="uppercase_text",
            address="sub_001",
        )

    assert not (runtime_dir / "runs").exists()


def test_build_run_plan_without_address_selects_all_entities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
    )

    assert run_plan.requested_address is None
    assert [ref.address for ref in run_plan.selected_fresh_output_refs] == [
        "sub_001",
        "sub_002",
    ]
    assert {spec.address for spec in run_plan.published_outputs} == {"sub_001", "sub_002"}
    assert _run_plan_payload(run_plan)["requested_address"] is None


def test_build_run_plan_rejects_invalid_address_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)

    with pytest.raises(ValidationError, match="address cannot contain path separators"):
        build_run_plan(
            project_dir=project_dir,
            context="mini",
            workflow_name="main",
            step_name="uppercase_text",
            address="../sub_001",
        )
    assert not (runtime_dir / "runs").exists()


def test_build_run_plan_rejects_address_not_in_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)

    with pytest.raises(
        ValidationError,
        match="not a member of source-population manifest 'subjects'",
    ):
        build_run_plan(
            project_dir=project_dir,
            context="mini",
            workflow_name="main",
            step_name="uppercase_text",
            address="sub_unused",
        )
    assert not (runtime_dir / "runs").exists()


def test_build_run_plan_rejects_address_for_cohort_step(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)

    with pytest.raises(ValidationError, match="cohort-addressed"):
        build_run_plan(
            project_dir=project_dir,
            context="colors",
            workflow_name="base",
            step_name="color_sector_analysis",
            address="color_000",
        )
    assert not (runtime_dir / "runs/colors/base/color_sector_analysis").exists()


def test_targeted_runs_for_different_addresses_get_distinct_workspaces(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    step_workspace = runtime_dir / "runs" / "mini" / "main" / "uppercase_text"

    plans = {
        address: build_run_plan(
            project_dir=project_dir,
            context="mini",
            workflow_name="main",
            step_name="uppercase_text",
            address=address,
        )
        for address in ("sub_001", "sub_002")
    }

    for address, run_plan in plans.items():
        assert run_plan.run_workspace == step_workspace / "addresses" / address
    assert plans["sub_001"].run_workspace != plans["sub_002"].run_workspace
    # Staging paths derive from run_workspace, so partitioned runs cannot
    # overwrite each other's staged files.
    for address, run_plan in plans.items():
        for job in run_plan.selected_fresh_jobs:
            for output in job.outputs.values():
                assert output.staging_path.is_relative_to(
                    step_workspace / "addresses" / address
                )


def test_full_population_run_keeps_existing_workspace_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
    )

    assert run_plan.run_workspace == (
        runtime_dir / "runs" / "mini" / "main" / "uppercase_text"
    )
    assert "addresses" not in run_plan.run_workspace.parts


def test_dry_run_workspace_is_isolated_for_single_output_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    step_workspace = runtime_dir / "runs" / "mini" / "main" / "uppercase_text"

    full_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        dry_run=True,
    )
    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        address="sub_001",
        dry_run=True,
    )

    # dry-run is the final path component, after all address partitioning.
    assert full_plan.run_workspace == step_workspace / "dry-run"
    assert targeted_plan.run_workspace == (
        step_workspace / "addresses" / "sub_001" / "dry-run"
    )
    for run_plan in (full_plan, targeted_plan):
        assert run_plan.dry_run is True
        for job in run_plan.selected_fresh_jobs:
            for output in job.outputs.values():
                assert output.staging_path.is_relative_to(run_plan.run_workspace)


def test_dry_run_workspace_is_isolated_for_multi_output_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_multi_output_project(tmp_path, monkeypatch)
    output_workspace = (
        runtime_dir / "runs" / "multi" / "import_raw" / "source_text" / "raw_text"
    )

    full_plan = build_run_plan(
        project_dir=project_dir,
        context="multi",
        workflow_name="import_raw",
        step_name="source_text",
        dry_run=True,
    )
    targeted_plan = build_run_plan(
        project_dir=project_dir,
        context="multi",
        workflow_name="import_raw",
        step_name="source_text",
        address="sub_001",
        dry_run=True,
    )

    # dry-run is the final path component, after selected-output and address
    # partitioning.
    assert full_plan.run_workspace == output_workspace / "dry-run"
    assert targeted_plan.run_workspace == (
        output_workspace / "addresses" / "sub_001" / "dry-run"
    )
    for run_plan in (full_plan, targeted_plan):
        assert run_plan.dry_run is True
        for job in run_plan.selected_fresh_jobs:
            for output in job.outputs.values():
                assert output.staging_path.is_relative_to(run_plan.run_workspace)


def test_multi_output_run_registers_sibling_outputs_and_exact_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_multi_output_project(tmp_path, monkeypatch)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="multi",
        workflow_name="consume_qc",
        step_name="qc_echo",
    )

    assert execute_run_plan(run_plan, cores=1).published_count == len(run_plan.published_outputs)

    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        artifact_rows = conn.execute(
            """
            SELECT step_name, output_name, address, is_selected_output, is_published,
                   published_path, staging_path
            FROM artifacts
            WHERE origin = 'workflow_output'
            ORDER BY step_name, output_name, address
            """
        ).fetchall()
        projection_rows = conn.execute(
            """
            SELECT a.step_name, a.output_name, a.address,
                   a.request_bundle_digest, rp.projection_json
            FROM artifacts AS a
            JOIN request_bundle_projections AS rp
              ON rp.request_bundle_digest = a.request_bundle_digest
            WHERE a.origin = 'workflow_output'
            ORDER BY step_name, output_name, address
            """
        ).fetchall()
        stored_projection_rows = conn.execute(
            """
            SELECT request_bundle_digest, projection_json
            FROM request_bundle_projections
            ORDER BY request_bundle_digest
            """
        ).fetchall()
        source_snapshots = {
            path: (content_digest, file_size, extension)
            for path, content_digest, file_size, extension in conn.execute(
                """
                SELECT path, content_digest, file_size, extension
                FROM artifacts
                WHERE origin = 'source'
                """
            ).fetchall()
        }
        dependency_rows = conn.execute(
            """
            SELECT d.binding_name, d.input_path, s.step_name, s.output_name, s.address
            FROM artifact_dependencies d
            JOIN artifacts s ON s.artifact_id = d.source_artifact_id
            JOIN artifacts a ON a.artifact_id = d.dependent_artifact_id
            WHERE a.step_name = 'qc_echo'
            ORDER BY a.address
            """
        ).fetchall()
        published_rows = conn.execute(
            """
            SELECT step_name, output_name, address
            FROM published_outputs
            ORDER BY step_name, output_name, address
            """
        ).fetchall()

    artifact_keys = {
        (step, output, address)
        for step, output, address, *_rest in artifact_rows
    }
    assert set(published_rows) == artifact_keys
    assert all(is_valid_digest(row[3]) and row[4] for row in projection_rows)
    assert len(stored_projection_rows) == len({row[3] for row in projection_rows})
    assert {row[0] for row in stored_projection_rows} == {
        row[3] for row in projection_rows
    }
    assert all(
        sha256_digest(projection_json.encode("utf-8")) == digest
        for digest, projection_json in stored_projection_rows
    )
    source_projection_by_address: dict[str, set[str]] = {}
    for step_name, _output_name, address, _digest, projection_json in projection_rows:
        if step_name == "source_text":
            source_projection_by_address.setdefault(address, set()).add(projection_json)
    assert set(source_projection_by_address) == {"sub_001", "sub_002"}
    assert all(
        len(projections) == 1
        for projections in source_projection_by_address.values()
    )
    for projections in source_projection_by_address.values():
        projection = json.loads(next(iter(projections)))
        source_binding = projection["role_labelled_bindings"][0]
        source_path = source_binding["source_coordinate"]["path"]
        assert (
            source_binding["registered_content_digest"],
            source_binding["registered_file_size"],
            source_binding["declared_extension"],
        ) == source_snapshots[source_path]
    assert artifact_keys == {
        ("source_text", "raw_text", "sub_001"),
        ("source_text", "raw_text", "sub_002"),
        ("source_text", "raw_qc", "sub_001"),
        ("source_text", "raw_qc", "sub_002"),
        ("qc_echo", "qc_echo", "sub_001"),
        ("qc_echo", "qc_echo", "sub_002"),
    }
    sibling_rows = [
        row for row in artifact_rows if row[0] == "source_text" and row[1] == "raw_qc"
    ]
    assert all(row[3] == 0 and row[4] == 1 and row[5] is not None for row in sibling_rows)
    assert dependency_rows == [
        (
            "source_qc",
            "staging/source_text/raw_qc/sub_001.json",
            "source_text",
            "raw_qc",
            "sub_001",
        ),
        (
            "source_qc",
            "staging/source_text/raw_qc/sub_002.json",
            "source_text",
            "raw_qc",
            "sub_002",
        ),
    ]

    equivalent_plan = build_run_plan(
        project_dir=project_dir,
        context="multi",
        workflow_name="consume_qc",
        step_name="qc_echo",
    )
    for reused_output in equivalent_plan.reused_outputs:
        assert isinstance(
            reused_output.projection_state,
            ResolvedRequestBundleProjectionV1,
        )
        assert reused_output.projection_state.canonical_json in (
            source_projection_by_address[reused_output.address]
        )


def test_execute_run_plan_publishes_selected_outputs_without_real_snakemake(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )

    def write_staged_outputs(*_args: object, **_kwargs: object) -> int:
        _write_all_staged_outputs(
            run_plan,
            selected_payload={
                "analysis_manifest_name": "init",
                "analysis_manifest_digest": "0" * 64,
                "entity_count": 200,
                "red_arc_count": 8,
                "green_arc_count": 56,
                "blue_arc_count": 11,
                "other_count": 125,
                "red_minus_green": -48,
            },
        )
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", write_staged_outputs)
    monkeypatch.setattr(
        "nipact.execution._environment_observation",
        lambda: EnvironmentObservationV1(
            nipact_version="test-nipact",
            python_version="test-python",
            platform="test-platform",
            snakemake_version="test-snakemake",
        ),
    )
    events: list[str] = []

    outcome = execute_run_plan(
        run_plan,
        cores=2,
        status_callback=events.append,
    )
    assert outcome.published_count == len(run_plan.published_outputs)
    assert outcome.selected_generated_count == 1
    assert outcome.selected_reused_count == 0
    assert events == [
        "building_workspace",
        "starting_snakemake",
        "snakemake_complete",
        "publishing_outputs",
        "registry_updated",
    ]

    assert (run_plan.run_workspace / "Snakefile").is_file()
    assert (run_plan.run_workspace / "run_plan.json").is_file()
    assert (run_plan.run_workspace / "selected_outputs.txt").is_file()
    output_dir = runtime_dir / "outputs/colors/base/color_sector_analysis/sector_counts"
    outputs = sorted(output_dir.glob("*.json"))
    assert len(outputs) == 1
    assert outputs[0].name.startswith("init.")
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        rows = conn.execute(
            """
            SELECT workflow_name, step_name, output_name, address, path, artifact_id
            FROM published_outputs
            ORDER BY workflow_name, step_name, output_name, address
            """
        ).fetchall()
        provenance_counts = {
            "workflow_runs": conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            "parameters": conn.execute("SELECT COUNT(*) FROM parameters").fetchone()[0],
            "workflow_artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
            "manifest_bindings": conn.execute(
                "SELECT COUNT(*) FROM run_manifest_bindings"
            ).fetchone()[0],
            "source_edges": conn.execute(
                """
                SELECT COUNT(*)
                FROM artifact_dependencies
                WHERE binding_name = 'colors_source'
                """
            ).fetchone()[0],
        }
        selected_row = next(
            row
            for row in rows
            if row[:4]
            == ("base", "color_sector_analysis", "sector_counts", "init")
        )
        selected_artifact = conn.execute(
            """
            SELECT origin, path, is_selected_output, is_published, published_path,
                   staging_path
            FROM artifacts
            WHERE artifact_id = ?
            """,
            (selected_row[5],),
        ).fetchone()
        run_observations = conn.execute(
            """
            SELECT resolution_summary_json, environment_observation_json
            FROM workflow_runs
            WHERE is_current = 1
            """
        ).fetchone()
    assert len(rows) == len(run_plan.published_outputs)
    assert selected_row[:5] == (
        "base",
        "color_sector_analysis",
        "sector_counts",
        "init",
        f"outputs/colors/base/color_sector_analysis/sector_counts/{outputs[0].name}",
    )
    assert selected_row[5] is not None
    assert provenance_counts == {
        "workflow_runs": 1,
        "parameters": len({job.step_name for job in run_plan.jobs}),
        "workflow_artifacts": len(run_plan.jobs),
        "dependencies": sum(len(job.input_records) for job in run_plan.jobs),
        "manifest_bindings": len(run_plan.manifest_bindings),
        "source_edges": 200,
    }
    assert selected_artifact == (
        "workflow_output",
        f"outputs/colors/base/color_sector_analysis/sector_counts/{outputs[0].name}",
        1,
        1,
        f"outputs/colors/base/color_sector_analysis/sector_counts/{outputs[0].name}",
        "runs/colors/base/color_sector_analysis/staging/color_sector_analysis/sector_counts/init.json",
    )
    assert json.loads(run_observations[0]) == {
        "schema_version": 1,
        "forced": False,
        "all_selected_resolved": True,
        "selected_outputs": [
            {
                "context": "colors",
                "workflow_name": "base",
                "step_name": "color_sector_analysis",
                "output_name": "sector_counts",
                "address": "init",
                "resolution": {
                    "artifact_id": selected_row[5],
                    "outcome": "generated",
                },
            }
        ],
    }
    assert json.loads(run_observations[1]) == {
        "profile_version": 1,
        "nipact_version": "test-nipact",
        "python_version": "test-python",
        "platform": "test-platform",
        "snakemake_version": "test-snakemake",
    }
    assert main(["validate", "--project-dir", str(project_dir), "--context", "colors"]) == 0
    assert f"published_outputs={len(run_plan.published_outputs)}" in capsys.readouterr().out


def test_execute_run_plan_removes_published_files_when_registration_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=_project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )

    def write_staged_outputs(*_args: object, **_kwargs: object) -> int:
        _write_all_staged_outputs(run_plan)
        return 0

    def fail_registration(*_args: object, **_kwargs: object) -> int:
        raise ValidationError("registry write failed")

    monkeypatch.setattr("nipact.execution._run_snakemake", write_staged_outputs)
    monkeypatch.setattr("nipact.execution.record_workflow_run", fail_registration)

    with pytest.raises(ValidationError, match="registry write failed"):
        execute_run_plan(run_plan, cores=1)

    output_dir = runtime_dir / "outputs/colors/base/color_sector_analysis/sector_counts"
    assert list(output_dir.glob("*.json")) == []
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM published_outputs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 0


def test_selected_resolution_mismatch_rolls_back_run_recording(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    execute_run_plan(run_plan, cores=1)

    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        prior_run_id = conn.execute(
            "SELECT run_id FROM workflow_runs WHERE is_current = 1"
        ).fetchone()[0]
        prior_memberships = conn.execute(
            """
            SELECT context, workflow_name, step_name, output_name, address, artifact_id
            FROM published_outputs
            ORDER BY context, workflow_name, step_name, output_name, address
            """
        ).fetchall()
        prior_artifact_count = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
        ).fetchone()[0]

    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )

    real_selected_resolution_intents = execution_module._selected_resolution_intents

    def mismatched_selected_resolution_intents(*args: object, **kwargs: object):
        intents = real_selected_resolution_intents(*args, **kwargs)
        return (replace(intents[0], step_name="color_features"), *intents[1:])

    monkeypatch.setattr(
        "nipact.execution._selected_resolution_intents",
        mismatched_selected_resolution_intents,
    )

    with pytest.raises(
        ValidationError,
        match="selected-output resolution does not match selected output",
    ):
        execute_run_plan(run_plan, cores=1)

    with sqlite3.connect(registry_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 1
        assert conn.execute(
            "SELECT is_current FROM workflow_runs WHERE run_id = ?",
            (prior_run_id,),
        ).fetchone() == (1,)
        assert conn.execute(
            """
            SELECT context, workflow_name, step_name, output_name, address, artifact_id
            FROM published_outputs
            ORDER BY context, workflow_name, step_name, output_name, address
            """
        ).fetchall() == prior_memberships
        assert conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
        ).fetchone()[0] == prior_artifact_count


def test_tiny_non_colors_run_registers_used_sources_and_trace(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
    )

    def run_jobs_through_runtime(*_args: object, **_kwargs: object) -> int:
        run_plan_path = run_plan.run_workspace / "run_plan.json"
        for job in run_plan.jobs:
            run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", run_jobs_through_runtime)

    assert execute_run_plan(run_plan, cores=1).published_count == len(run_plan.published_outputs)

    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        source_rows = conn.execute(
            """
            SELECT path, content_digest, output_hash, file_size, extension,
                   source_metadata_json, run_id, workflow_name, step_name,
                   output_name, address, parameter_id, is_selected_output,
                   is_published, published_path, staging_path
            FROM artifacts
            WHERE origin = 'source'
            ORDER BY path
            """
        ).fetchall()
        source_edges = conn.execute(
            """
            SELECT d.binding_name, d.input_path, d.dependency_role, a.path
            FROM artifact_dependencies d
            JOIN artifacts a ON a.artifact_id = d.source_artifact_id
            WHERE a.origin = 'source'
            ORDER BY a.path
            """
        ).fetchall()

    used_source_paths = [
        "data/source/sub_001.txt",
        "data/source/sub_002.txt",
    ]
    assert [row[0] for row in source_rows] == used_source_paths
    for row, source_path in zip(source_rows, used_source_paths, strict=True):
        digest = sha256_file_digest(runtime_dir / source_path)
        assert row == (
            source_path,
            digest,
            short_hash(digest),
            (runtime_dir / source_path).stat().st_size,
            ".txt",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            0,
            None,
            None,
        )
    assert source_edges == [
        (
            "source_text",
            "../../../../data/source/sub_001.txt",
            "source_input",
            "data/source/sub_001.txt",
        ),
        (
            "source_text",
            "../../../../data/source/sub_002.txt",
            "source_input",
            "data/source/sub_002.txt",
        ),
    ]

    first_digest = source_rows[0][1]
    (runtime_dir / "data/source/sub_001.txt").write_text("changed\n", encoding="utf-8")

    assert execute_run_plan(run_plan, cores=1).published_count == len(run_plan.published_outputs)

    changed_digest = sha256_file_digest(runtime_dir / "data/source/sub_001.txt")
    with sqlite3.connect(registry_path) as conn:
        updated_row = conn.execute(
            """
            SELECT content_digest, output_hash, source_metadata_json
            FROM artifacts
            WHERE context = ? AND origin = 'source' AND path = ?
            """,
            ("mini", "data/source/sub_001.txt"),
        ).fetchone()
    assert changed_digest != first_digest
    assert updated_row == (changed_digest, short_hash(changed_digest), None)

    graph = build_trace_graph_for_workflow_coordinate(
        registry_path,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        output_name="upper_text",
        address="sub_001",
    )
    source_nodes = [
        artifact for artifact in graph["artifacts"] if artifact["origin"] == "source"
    ]
    assert graph["provenance_status"] == "complete"
    assert len(source_nodes) == 1
    assert source_nodes[0]["path"] == "data/source/sub_001.txt"
    assert source_nodes[0]["content_digest"] == changed_digest

    assert (
        main(
            [
                "validate",
                "--project-dir",
                str(project_dir),
                "--context",
                "mini",
            ]
        )
        == 0
    )
    assert "published_outputs=4" in capsys.readouterr().out


def test_run_snakemake_command_omits_keep_incomplete(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )
    captured: dict[str, list[str]] = {}

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        captured["command"] = list(command)
        return SimpleNamespace(stdout="", stderr="", returncode=0)

    monkeypatch.setattr("nipact.execution.subprocess.run", fake_run)
    _run_snakemake(run_plan, cores=1, dry_run=False)

    command = captured["command"]
    # A failed rule's output must be deleted by Snakemake (its default) so best-effort
    # publishing never sees a truncated file; --keep-incomplete would defeat that.
    assert "--keep-incomplete" not in command
    # Sanity-check we captured the real Snakemake command and the relied-on flags remain.
    assert "--keep-going" in command
    assert "--rerun-incomplete" in command


def test_failed_snakemake_run_does_not_update_registry(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_local_transform",
    )

    def ran_and_failed(*_args: object, **_kwargs: object) -> int:
        # A Snakemake subprocess that ran, exited non-zero, and left no staged
        # outputs: the §3.1 hard-error branch (publish nothing + non-zero exit).
        return 1

    monkeypatch.setattr("nipact.execution._run_snakemake", ran_and_failed)
    with pytest.raises(ValidationError, match="Snakemake failed with exit code 1"):
        execute_run_plan(run_plan, cores=1)

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        counts = {
            "published_outputs": conn.execute(
                "SELECT COUNT(*) FROM published_outputs"
            ).fetchone()[0],
            "workflow_runs": conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            "workflow_artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
            "manifest_bindings": conn.execute(
                "SELECT COUNT(*) FROM run_manifest_bindings"
            ).fetchone()[0],
        }
    assert counts == {
        "published_outputs": 0,
        "workflow_runs": 0,
        "workflow_artifacts": 0,
        "dependencies": 0,
        "manifest_bindings": 0,
    }


def test_partial_publish_records_surviving_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
    )

    def run_all_but_sub_002(*_args: object, **_kwargs: object) -> int:
        run_plan_path = run_plan.run_workspace / "run_plan.json"
        for job in run_plan.jobs:
            if job.address == "sub_002":
                continue  # simulate sub_002 failing under --keep-going
            run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        return 1

    monkeypatch.setattr("nipact.execution._run_snakemake", run_all_but_sub_002)

    # sub_001 publishes both of its jobs; sub_002 publishes nothing, and the run
    # records the survivors instead of rolling everything back.
    outcome = execute_run_plan(run_plan, cores=1)
    assert outcome.published_count == 2
    assert outcome.selected_generated_count == 1
    assert outcome.selected_reused_count == 0
    assert outcome.all_selected_resolved is False
    assert outcome.failed_jobs == (
        ("source_text", "sub_002", "missing staged output"),
        ("uppercase_text", "sub_002", "missing staged output"),
    )

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published = conn.execute(
            "SELECT step_name, output_name, address FROM published_outputs "
            "ORDER BY step_name, output_name, address"
        ).fetchall()
        artifacts = conn.execute(
            "SELECT step_name, output_name, address FROM artifacts "
            "WHERE origin = 'workflow_output' ORDER BY step_name, output_name, address"
        ).fetchall()
        workflow_run = conn.execute(
            "SELECT resolution_summary_json FROM workflow_runs"
        ).fetchone()
        selected_artifact_id = conn.execute(
            """
            SELECT artifact_id
            FROM artifacts
            WHERE step_name = 'uppercase_text'
              AND output_name = 'upper_text'
              AND address = 'sub_001'
            """
        ).fetchone()[0]
    expected = [
        ("source_text", "raw_text", "sub_001"),
        ("uppercase_text", "upper_text", "sub_001"),
    ]
    assert published == expected
    assert artifacts == expected
    resolution_summary = json.loads(workflow_run[0])
    assert resolution_summary["all_selected_resolved"] is False
    assert resolution_summary["forced"] is False
    assert resolution_summary["selected_outputs"] == [
        {
            "context": "mini",
            "workflow_name": "main",
            "step_name": "uppercase_text",
            "output_name": "upper_text",
            "address": "sub_001",
            "resolution": {
                "artifact_id": selected_artifact_id,
                "outcome": "generated",
            },
        },
        {
            "context": "mini",
            "workflow_name": "main",
            "step_name": "uppercase_text",
            "output_name": "upper_text",
            "address": "sub_002",
            "resolution": None,
        },
    ]


def test_projection_finalization_failure_rolls_back_current_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    active_plan: dict[str, object] = {}

    def run_active_plan(*_args: object, **_kwargs: object) -> int:
        run_plan = active_plan["value"]
        run_plan_path = run_plan.run_workspace / "run_plan.json"
        for job in run_plan.jobs:
            run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", run_active_plan)
    first_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        address="sub_001",
    )
    active_plan["value"] = first_plan
    assert execute_run_plan(first_plan, cores=1).all_selected_resolved

    registry_path = runtime_dir / "database/registry.db"
    with sqlite3.connect(registry_path) as conn:
        current_run_before = conn.execute(
            "SELECT run_id FROM workflow_runs WHERE is_current = 1"
        ).fetchone()[0]
        counts_before = (
            conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM published_outputs").fetchone()[0],
        )

    rerun_plan = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
        address="sub_001",
    )
    active_plan["value"] = rerun_plan
    monkeypatch.setattr(
        "nipact.execution._retained_projection_recipes",
        lambda *_args, **_kwargs: (),
    )
    with pytest.raises(
        ValidationError,
        match="retained artifact is missing its projection recipe",
    ):
        execute_run_plan(rerun_plan, cores=1)

    with sqlite3.connect(registry_path) as conn:
        assert conn.execute(
            "SELECT run_id FROM workflow_runs WHERE is_current = 1"
        ).fetchone()[0] == current_run_before
        assert (
            conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM published_outputs").fetchone()[0],
        ) == counts_before


def test_multi_output_partial_sibling_prunes_orphan_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_multi_output_project(tmp_path, monkeypatch)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="multi",
        workflow_name="consume_qc",
        step_name="qc_echo",
    )

    def run_then_drop_sub_002_raw_text(*_args: object, **_kwargs: object) -> int:
        run_plan_path = run_plan.run_workspace / "run_plan.json"
        for job in run_plan.jobs:
            run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        # sub_002's source_text loses one sibling (raw_text); raw_qc stays, and
        # qc_echo has already consumed it. The parent job is now incomplete.
        (
            run_plan.run_workspace / "staging/source_text/raw_text/sub_002.json"
        ).unlink()
        return 1

    monkeypatch.setattr(
        "nipact.execution._run_snakemake", run_then_drop_sub_002_raw_text
    )

    # sub_001 fully publishes (source_text x2 + qc_echo). For sub_002: source_text
    # is skipped for the missing sibling, and qc_echo (which consumed raw_qc) is
    # pruned as an orphan so it cannot trigger record_workflow_run's whole-batch
    # rollback. The independent survivor sub_001 is still recorded.
    outcome = execute_run_plan(run_plan, cores=1)
    assert outcome.published_count == 3
    assert outcome.all_selected_resolved is False
    # source_text/sub_002 skips on the missing sibling; qc_echo/sub_002 published
    # then prunes because its fresh parent did not.
    assert outcome.failed_jobs == (
        ("qc_echo", "sub_002", "upstream not published"),
        ("source_text", "sub_002", "missing staged output"),
    )

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        published = conn.execute(
            "SELECT step_name, output_name, address FROM published_outputs "
            "ORDER BY step_name, output_name, address"
        ).fetchall()
        artifacts = conn.execute(
            "SELECT step_name, output_name, address FROM artifacts "
            "WHERE origin = 'workflow_output' ORDER BY step_name, output_name, address"
        ).fetchall()
        workflow_runs = conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0]
    expected = [
        ("qc_echo", "qc_echo", "sub_001"),
        ("source_text", "raw_qc", "sub_001"),
        ("source_text", "raw_text", "sub_001"),
    ]
    assert published == expected
    assert artifacts == expected
    assert workflow_runs == 1


def test_rerun_reuses_partial_survivors_without_recompute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _write_tiny_non_colors_project(tmp_path, monkeypatch)

    plan_one = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
    )

    def run_all_but_sub_002(*_args: object, **_kwargs: object) -> int:
        run_plan_path = plan_one.run_workspace / "run_plan.json"
        for job in plan_one.jobs:
            if job.address == "sub_002":
                continue
            run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        return 1

    monkeypatch.setattr("nipact.execution._run_snakemake", run_all_but_sub_002)
    assert execute_run_plan(plan_one, cores=1).published_count == 2

    # Run 2: sub_001's upstream source_text is now reusable, so it is hydrated
    # rather than recomputed; only sub_002 and the always-rebuilt selected step
    # remain as jobs.
    plan_two = build_run_plan(
        project_dir=project_dir,
        context="mini",
        workflow_name="main",
        step_name="uppercase_text",
    )
    reused_keys = {
        (ref.step_name, ref.output_name, ref.address)
        for ref in plan_two.reused_outputs
    }
    job_addresses = {(job.step_name, job.address) for job in plan_two.jobs}
    assert ("source_text", "raw_text", "sub_001") in reused_keys
    assert ("source_text", "sub_001") not in job_addresses
    assert ("source_text", "sub_002") in job_addresses

    executed: list[str] = []

    def run_all(*_args: object, **_kwargs: object) -> int:
        run_plan_path = plan_two.run_workspace / "run_plan.json"
        for job in plan_two.jobs:
            executed.append(job.job_id)
            run_job(run_plan_path=run_plan_path, job_id=job.job_id)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", run_all)
    execute_run_plan(plan_two, cores=1)

    # The reused survivor was never re-executed.
    assert all(
        "source_text__raw_text__sub_001" not in job_id for job_id in executed
    )
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        addresses = conn.execute(
            "SELECT DISTINCT address FROM published_outputs "
            "WHERE step_name = 'uppercase_text' ORDER BY address"
        ).fetchall()
    assert [row[0] for row in addresses] == ["sub_001", "sub_002"]


def test_launch_failure_raises_without_publishing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )

    def raise_file_not_found(*_args: object, **_kwargs: object) -> object:
        raise FileNotFoundError("python interpreter is missing")

    monkeypatch.setattr("nipact.execution.subprocess.run", raise_file_not_found)
    with pytest.raises(ValidationError, match="could not execute Python interpreter"):
        execute_run_plan(run_plan, cores=1)

    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM published_outputs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 0


def test_dry_run_does_not_publish_outputs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        dry_run=True,
    )
    calls: list[bool] = []

    def dry_run_stub(*_args: object, dry_run: bool, **_kwargs: object) -> int:
        calls.append(dry_run)
        return 0

    monkeypatch.setattr("nipact.execution._run_snakemake", dry_run_stub)
    events: list[str] = []

    assert (
        execute_run_plan(
            run_plan,
            cores=1,
            status_callback=events.append,
        ).published_count
        == 0
    )

    assert calls == [True]
    assert events == [
        "building_workspace",
        "starting_snakemake",
        "snakemake_complete",
    ]
    assert (run_plan.run_workspace / "Snakefile").is_file()
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        counts = {
            "published_outputs": conn.execute(
                "SELECT COUNT(*) FROM published_outputs"
            ).fetchone()[0],
            "workflow_runs": conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            "workflow_artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
            "manifest_bindings": conn.execute(
                "SELECT COUNT(*) FROM run_manifest_bindings"
            ).fetchone()[0],
        }
    assert counts == {
        "published_outputs": 0,
        "workflow_runs": 0,
        "workflow_artifacts": 0,
        "dependencies": 0,
        "manifest_bindings": 0,
    }
    assert not (
        runtime_dir / "outputs/colors/base/color_sector_analysis/sector_counts"
    ).exists()


def test_snakefile_header_marks_dry_run_workspaces_only(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)
    real_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
    )
    dry_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        dry_run=True,
    )

    dry_text = _snakefile_text(dry_plan)
    assert dry_text.startswith(
        "# Generated by NIPACT for a dry run. Do not edit.\n"
        "# Dry-run planning workspace — not intended for manual execution.\n"
    )

    real_text = _snakefile_text(real_plan)
    assert real_text.startswith("# Generated by NIPACT. Do not edit.\n")
    assert "dry run" not in real_text
    assert "dry-run" not in real_text


def test_dry_run_fails_on_nonzero_snakemake_exit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, runtime_dir = _init_demo(tmp_path, capsys)
    run_plan = build_run_plan(
        project_dir=project_dir,
        context="colors",
        workflow_name="base",
        step_name="color_sector_analysis",
        dry_run=True,
    )

    def fail_dry_run(*_args: object, **_kwargs: object) -> int:
        return 1

    monkeypatch.setattr("nipact.execution._run_snakemake", fail_dry_run)

    with pytest.raises(
        ValidationError, match="Snakemake failed with exit code 1"
    ) as excinfo:
        execute_run_plan(run_plan, cores=1)

    assert str(run_plan.run_workspace / "logs" / "snakemake.log") in str(excinfo.value)
    with sqlite3.connect(runtime_dir / "database/registry.db") as conn:
        counts = {
            "published_outputs": conn.execute(
                "SELECT COUNT(*) FROM published_outputs"
            ).fetchone()[0],
            "workflow_runs": conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0],
            "workflow_artifacts": conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE origin = 'workflow_output'"
            ).fetchone()[0],
            "dependencies": conn.execute(
                "SELECT COUNT(*) FROM artifact_dependencies"
            ).fetchone()[0],
            "manifest_bindings": conn.execute(
                "SELECT COUNT(*) FROM run_manifest_bindings"
            ).fetchone()[0],
        }
    assert counts == {
        "published_outputs": 0,
        "workflow_runs": 0,
        "workflow_artifacts": 0,
        "dependencies": 0,
        "manifest_bindings": 0,
    }
    assert not (
        runtime_dir / "outputs/colors/base/color_sector_analysis/sector_counts"
    ).exists()
