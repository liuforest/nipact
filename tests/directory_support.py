"""Tiny real mixed-output project shared by lifecycle acceptance tests."""

import sqlite3
import yaml

from test_execution import _write_tiny_non_colors_project, _write_yaml
from nipact.execution import build_run_plan, execute_run_plan


def directory_project(tmp_path, monkeypatch):
    import sys

    monkeypatch.delitem(sys.modules, "directory_runtime", raising=False)
    project, runtime = _write_tiny_non_colors_project(tmp_path, monkeypatch)
    module = tmp_path / "importable/directory_runtime.py"
    module.write_text(
        """import json
from pathlib import Path


def produce(*, inputs, outputs, params, address):
    text = json.loads(inputs["raw"][0].read_text())["text"]
    outputs["report"].write_text(text)
    if params.get("failure") == "missing":
        return
    if params.get("failure") == "wrong_kind":
        outputs["maps"].write_text(text)
    else:
        outputs["maps"].mkdir()
        (outputs["maps"] / "nested/deep").mkdir(parents=True)
        (outputs["maps"] / "nested/deep/value.txt").write_text(text + params.get("suffix", ""))
        (outputs["maps"] / "empty").mkdir()
        (outputs["maps"] / ".hidden").write_text("hidden")
        (outputs["maps"] / ".snakemake_timestamp").write_text("scientific")
        if params.get("failure") == "link":
            (outputs["maps"] / "nested/bad").symlink_to("deep/value.txt")
    outputs["empty"].mkdir()
    if params.get("failure") == "raise":
        raise RuntimeError("provider failure")


def read(*, inputs, outputs, params, address):
    rows = []
    for tree in inputs["maps"]:
        assert sorted(p.relative_to(tree).as_posix() for p in tree.rglob("*")) == [".hidden", ".snakemake_timestamp", "empty", "nested", "nested/deep", "nested/deep/value.txt"]
        assert (tree / ".snakemake_timestamp").read_text() == "scientific"
        rows.append((tree / "nested/deep/value.txt").read_text())
    if "summaries" in inputs:
        assert [json.loads(p.read_text())[0] for p in inputs["summaries"]] == rows
    outputs["summary"].write_text(json.dumps(rows))
"""
    )
    file_output = {"extension": ".txt", "address_scope": "entity"}
    tree_output = {"kind": "directory", "address_scope": "entity"}
    for name, definition in {
        "mixed": {
            "callable": "directory_runtime:produce",
            "inputs": {
                "raw": {
                    "artifact": "source_text.raw_text",
                    "dependency_role": "source_input",
                }
            },
            "outputs": {
                "report": file_output,
                "maps": tree_output,
                "empty": tree_output,
            },
        },
        "reader": {
            "callable": "directory_runtime:read",
            "inputs": {
                "maps": {"artifact": "mixed.maps", "dependency_role": "source_input"}
            },
            "outputs": {"summary": file_output},
        },
        "collect": {
            "callable": "directory_runtime:read",
            "execution_role": "b_fit",
            "address_scope": "cohort",
            "manifest_binding": {"role": "fit_cohort", "manifest": "subjects"},
            "inputs": {
                "maps": {"artifact": "mixed.maps", "dependency_role": "fit_input"},
                "summaries": {
                    "artifact": "reader.summary",
                    "dependency_role": "fit_input",
                },
            },
            "outputs": {"summary": {"extension": ".json", "address_scope": "cohort"}},
        },
    }.items():
        _write_yaml(
            project / f"steps/{name}.yaml",
            {
                "step_name": name,
                "step_contract_version": "1",
                "pattern_kind": "pattern_a",
                "execution_role": "transform",
                "address_scope": "entity",
                **definition,
            },
        )
    _write_yaml(
        project / "workflows/main.yaml",
        {
            "workflow_name": "main",
            "execution_population": "subjects",
            "steps": [
                {"step_name": "source_text", "output_name": "raw_text"},
                {"step_name": "mixed", "output_name": "maps"},
                {"step_name": "reader", "output_name": "summary"},
                {"step_name": "collect", "output_name": "summary"},
            ],
        },
    )
    return project, runtime


def plan(project, step="mixed", address=None, dry_run=False):
    return build_run_plan(
        project_dir=project,
        context="mini",
        workflow_name="main",
        step_name=step,
        address=address,
        dry_run=dry_run,
    )


def run(project, step="mixed", address=None):
    return execute_run_plan(plan(project, step, address), cores=1)


def rows(runtime, table="artifacts"):
    with sqlite3.connect(runtime / "database/registry.db") as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")
        ]


def change_step(project, name, **changes):
    path = project / f"steps/{name}.yaml"
    payload = yaml.safe_load(path.read_text())
    payload.update(changes)
    _write_yaml(path, payload)
