import json
import os
from pathlib import Path

import pytest

from nipact._version import __version__
from nipact.cli import main
from nipact.execution import RunOutcome


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
            ],
        )
        == 0
    )
    capsys.readouterr()
    return project_dir, runtime_dir


def _workflow_base_args(project_dir: Path) -> list[str]:
    return [
        "--project-dir",
        str(project_dir),
        "--context",
        "colors",
    ]


def _workflow_context_args() -> list[str]:
    return [
        "--context",
        "colors",
    ]


def test_cli_version_prints_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"nipact {__version__}"


def test_cli_init_requires_demo_flag() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "init",
                "--project-dir",
                "project",
                "--runtime-dir",
                "runtime",
                "--context",
                "colors",
            ]
        )

    assert exc_info.value.code == 2


def test_cli_init_defaults_context_to_demo(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
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
            ],
        )
        == 0
    )

    output = capsys.readouterr().out
    assert "context=colors" in output
    assert "context_index=nipact.contexts.yaml" in output
    assert (project_dir / "nipact.yaml").read_text(encoding="utf-8").startswith(
        "context: colors\n"
    )
    assert (tmp_path / "nipact.contexts.yaml").read_text(encoding="utf-8") == (
        "contexts:\n"
        "  colors:\n"
        "    project_dir: project\n"
    )


def test_validate_resolves_project_root_fallback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)
    (tmp_path / "nipact.contexts.yaml").unlink()
    monkeypatch.chdir(project_dir)

    assert main(["validate", "--context", "colors"]) == 0

    assert "PASS: validate" in capsys.readouterr().out


def test_workflow_list_command(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    assert main(["workflow", "list", *_workflow_base_args(project_dir)]) == 0

    assert capsys.readouterr().out.splitlines() == [
        "base",
        "red-qc-target",
        "PASS: workflow list",
    ]


def test_workflow_steps_command(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    assert (
        main(
            [
                "workflow",
                "steps",
                *_workflow_base_args(project_dir),
                "--workflow",
                "base",
            ]
        )
        == 0
    )

    assert capsys.readouterr().out.splitlines() == [
        "step=color_local_transform output=local_color",
        "step=color_candidate_select output=selected_color",
        "step=color_cohort_fit output=cohort_fit",
        "step=color_cohort_apply output=cohort_color",
        "step=color_sector_analysis output=sector_counts",
        "PASS: workflow steps",
    ]


def test_workflow_plan_command(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    assert (
        main(
            [
                "workflow",
                "plan",
                *_workflow_base_args(project_dir),
                "--workflow",
                "base",
                "--step",
                "color_sector_analysis",
            ]
        )
        == 0
    )

    output = capsys.readouterr().out.splitlines()
    assert output[:4] == [
        "workflow=base",
        "step=color_sector_analysis",
        "selected_output=sector_counts",
        "steps=8",
    ]
    assert (
        "step=color_source pattern=pattern_a role=source_import outputs=source_color"
        in output
    )
    assert (
        "step=color_sector_analysis pattern=analysis role=analysis "
        "outputs=sector_counts"
    ) in output
    assert (
        "manifest_binding step=color_source role=source_population manifest=init "
        "manifest_hash=287318ee136c4518 entities=200"
    ) in output
    assert (
        "manifest_binding step=color_cohort_fit role=fit_cohort manifest=demo-40 "
        "manifest_hash=9db06e41af119408 entities=40"
    ) in output
    assert "overrides=0" in output
    assert output[-1] == "PASS: workflow plan"


def test_workflow_graph_command_prints_json_only(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    assert (
        main(
            [
                "workflow",
                "graph",
                *_workflow_base_args(project_dir),
                "--workflow",
                "base",
                "--step",
                "color_sector_analysis",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    assert "PASS:" not in captured.out
    graph = json.loads(captured.out)
    assert graph["workflow_name"] == "base"
    assert graph["selected_step_name"] == "color_sector_analysis"
    assert graph["terminal_step_kind"] == "analysis"
    assert len(graph["nodes"]) == 8
    assert {binding["role"] for binding in graph["manifest_bindings"]} == {
        "source_population",
        "fit_cohort",
        "analysis_cohort",
    }


def test_workflow_run_command_executes_step(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    def publish_stub(*_args: object, **kwargs: object) -> RunOutcome:
        status_callback = kwargs["status_callback"]
        assert kwargs["cores"] == 2
        assert kwargs["dry_run"] is False
        for event in (
            "building_workspace",
            "starting_snakemake",
            "snakemake_complete",
            "publishing_outputs",
            "registry_updated",
        ):
            status_callback(event)
        return RunOutcome(
            published_count=1,
            failed_jobs=(),
            all_selected_published=True,
        )

    monkeypatch.setattr("nipact.execution.execute_run_plan", publish_stub)
    clock = iter([10.0, 12.5])
    monkeypatch.setattr("nipact.cli.perf_counter", lambda: next(clock))
    monkeypatch.chdir(tmp_path)

    assert (
        main(
            [
                "workflow",
                "run",
                *_workflow_context_args(),
                "--workflow",
                "base",
                "--step",
                "color_sector_analysis",
                "--cores",
                "2",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    output = captured.out.splitlines()
    assert captured.err == ""
    assert "\r" not in captured.out
    assert "\r" not in captured.err
    assert output[:9] == [
        "NIPACT workflow run",
        "",
        "context=colors",
        "workflow=base",
        "step=color_sector_analysis",
        "selected_output=sector_counts",
        "cores=2",
        "dry_run=false",
        "selected_outputs=1",
    ]
    # These three values describe planner decisions before Snakemake runs. In
    # the fresh demo path nothing is hydrated, but the scheduled job count still
    # matters because it tells the user how much work the concrete DAG contains.
    assert output[9].startswith("planned_jobs=")
    assert int(output[9].split("=", maxsplit=1)[1]) > 0
    assert output[10] == "planned_reused_registered_artifacts=0"
    assert output[11] == "planned_hydrated_inputs=0"
    assert output[12] == "existing_staged_outputs=0"
    assert output[13].endswith("/runs/colors/base/color_sector_analysis")
    assert output[13].startswith("run_workspace=")
    assert output[14].endswith(
        "/runs/colors/base/color_sector_analysis/logs/snakemake.log"
    )
    assert output[14].startswith("snakemake_log=")
    assert output[15] == (
        "note=Registered upstream artifacts can be hydrated into the current "
        "run when their identity and digest checks pass."
    )
    assert output[16:] == [
        "",
        "Preparing run workspace...",
        "Starting Snakemake...",
        "Snakemake complete.",
        "Publishing outputs...",
        "Registry updated.",
        "",
        "published_outputs=1",
        "registry=updated",
        "elapsed_seconds=2.500",
        "PASS: workflow run",
    ]


def test_workflow_run_partial_publish_exits_non_zero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    def partial_stub(*_args: object, **_kwargs: object) -> RunOutcome:
        return RunOutcome(
            published_count=2,
            failed_jobs=(("color_sector_analysis", "sub_003", "missing staged output"),),
            all_selected_published=False,
        )

    monkeypatch.setattr("nipact.execution.execute_run_plan", partial_stub)
    monkeypatch.setattr("nipact.cli.perf_counter", iter([10.0, 12.5]).__next__)
    monkeypatch.chdir(tmp_path)

    exit_code = main(
        [
            "workflow",
            "run",
            *_workflow_context_args(),
            "--workflow",
            "base",
            "--step",
            "color_sector_analysis",
        ]
    )
    assert exit_code == 1

    output = capsys.readouterr().out.splitlines()
    assert "published_outputs=2" in output
    assert (
        "failed_job=color_sector_analysis sub_003 (missing staged output)" in output
    )
    assert "PARTIAL: workflow run" in output
    assert "PASS: workflow run" not in output


def test_workflow_run_rejects_non_positive_cores(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "workflow",
                "run",
                *_workflow_base_args(project_dir),
                "--workflow",
                "base",
                "--step",
                "color_sector_analysis",
                "--cores",
                "0",
            ]
        )

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "must be a positive integer" in captured.err


def test_workflow_run_command_errors_are_concise(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_dir, _runtime_dir = _init_demo(tmp_path, capsys)

    assert (
        main(
            [
                "workflow",
                "run",
                *_workflow_base_args(project_dir),
                "--workflow",
                "missing",
                "--step",
                "color_sector_analysis",
            ]
        )
        == 1
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error: unknown workflow: missing\n"
