import json
from pathlib import Path

import pytest

import nipact.execution_evidence as evidence_module
import nipact.runtime as runtime_module
from nipact.execution_evidence import (
    COMPLETION_RECEIPT_SCHEMA_VERSION,
    RUN_PLAN_SCHEMA_VERSION,
    CompletionReceipt,
    ExecutionEvidenceError,
    read_completion_receipt,
    write_completion_receipt_atomic,
)


def _write_runtime_plan(
    tmp_path: Path,
    *,
    invocation_token: str | None = "a" * 32,
    output_names: tuple[str, ...] = ("result",),
) -> tuple[Path, str]:
    workspace = tmp_path / "run"
    workspace.mkdir()
    job_id = "job__step__result__sub_001"
    outputs = {
        name: f"staging/step/{name}/sub_001.json" for name in output_names
    }
    payload = {
        "schema_version": RUN_PLAN_SCHEMA_VERSION,
        "invocation_token": invocation_token,
        "runtime_root": str(tmp_path),
        "prepared_reused_inputs": [],
        "jobs": {
            job_id: {
                "step_name": "step",
                "address": "sub_001",
                "callable_ref": "test:callable",
                "request_bundle_digest": "b" * 64,
                "declared_outputs": list(output_names),
                "completion_receipt_path": f"receipts/{job_id}.json",
                "outputs": outputs,
                "inputs": {},
                "input_records": [],
                "params": {},
            }
        },
    }
    path = workspace / "run_plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, job_id


def test_runtime_writes_atomic_receipt_after_complete_callable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(tmp_path)

    def callable_obj(*, inputs, outputs, params, address):
        assert inputs == {}
        assert params == {}
        assert address == "sub_001"
        outputs["result"].write_text("complete\n", encoding="utf-8")

    monkeypatch.setattr(runtime_module, "_load_callable", lambda _ref: callable_obj)

    runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)

    receipt = read_completion_receipt(
        run_plan_path.parent / f"receipts/{job_id}.json"
    )
    assert receipt == CompletionReceipt(
        invocation_token="a" * 32,
        job_id=job_id,
        request_bundle_digest="b" * 64,
        outputs=("result",),
    )


@pytest.mark.parametrize("failure", ["raise", "missing_sibling"])
def test_runtime_failure_leaves_no_completion_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(
        tmp_path,
        output_names=("first", "second"),
    )

    def callable_obj(*, inputs, outputs, params, address):
        outputs["first"].write_text("partial\n", encoding="utf-8")
        if failure == "raise":
            raise RuntimeError("callable failed")

    monkeypatch.setattr(runtime_module, "_load_callable", lambda _ref: callable_obj)

    with pytest.raises(RuntimeError):
        runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)

    assert not (run_plan_path.parent / f"receipts/{job_id}.json").exists()


def test_runtime_rejects_nonexecutable_forecast_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(tmp_path, invocation_token=None)
    monkeypatch.setattr(
        runtime_module,
        "_load_callable",
        lambda _ref: pytest.fail("callable must not be loaded"),
    )

    with pytest.raises(RuntimeError, match="no executable invocation token"):
        runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)


def test_runtime_rejects_run_plan_schema_v1(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(tmp_path)
    payload = json.loads(run_plan_path.read_text(encoding="utf-8"))
    payload["schema_version"] = 1
    run_plan_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        runtime_module,
        "_load_callable",
        lambda _ref: pytest.fail("callable must not be loaded"),
    )

    with pytest.raises(RuntimeError, match="unsupported run-plan schema version"):
        runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)


@pytest.mark.parametrize("delivery", ["copied", "direct"])
def test_runtime_resolves_prepared_reused_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    delivery: str,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(tmp_path)
    canonical = tmp_path / "outputs/v1/cache/upstream/sub_001/request/out/value.json"
    canonical.parent.mkdir(parents=True)
    canonical.write_text("upstream\n", encoding="utf-8")
    if delivery == "copied":
        supplied = "staging/upstream/out/sub_001.json"
        supplied_path = run_plan_path.parent / supplied
        supplied_path.parent.mkdir(parents=True)
        supplied_path.write_text("upstream\n", encoding="utf-8")
    else:
        supplied_path = canonical
        supplied = "../outputs/v1/cache/upstream/sub_001/request/out/value.json"

    payload = json.loads(run_plan_path.read_text(encoding="utf-8"))
    payload["prepared_reused_inputs"] = [
        {
            "artifact_id": 7,
            "bound_occurrence_path": canonical.relative_to(tmp_path).as_posix(),
            "supplied_path": supplied,
        }
    ]
    job = payload["jobs"][job_id]
    job["inputs"] = {"upstream": [supplied]}
    job["input_records"] = [
        {
            "binding_name": "upstream",
            "input_path": supplied,
            "origin": "workflow_output",
            "registry_source_artifact_id": 7,
        }
    ]
    run_plan_path.write_text(json.dumps(payload), encoding="utf-8")

    def callable_obj(*, inputs, outputs, params, address):
        assert inputs == {"upstream": (supplied_path.resolve(),)}
        outputs["result"].write_text("complete\n", encoding="utf-8")

    monkeypatch.setattr(runtime_module, "_load_callable", lambda _ref: callable_obj)

    runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)


@pytest.mark.parametrize("failure", ["missing_authority", "wrong_canonical_path"])
def test_runtime_rejects_mismatched_prepared_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(tmp_path)
    canonical = tmp_path / "outputs/v1/cache/upstream/sub_001/request/out/value.json"
    other = tmp_path / "outputs/v1/cache/upstream/sub_001/other/out/value.json"
    canonical.parent.mkdir(parents=True)
    other.parent.mkdir(parents=True)
    canonical.write_text("expected\n", encoding="utf-8")
    other.write_text("other\n", encoding="utf-8")
    supplied = "../outputs/v1/cache/upstream/sub_001/other/out/value.json"

    payload = json.loads(run_plan_path.read_text(encoding="utf-8"))
    if failure == "wrong_canonical_path":
        payload["prepared_reused_inputs"] = [
            {
                "artifact_id": 7,
                "bound_occurrence_path": canonical.relative_to(tmp_path).as_posix(),
                "supplied_path": supplied,
            }
        ]
    job = payload["jobs"][job_id]
    job["inputs"] = {"upstream": [supplied]}
    job["input_records"] = [
        {
            "binding_name": "upstream",
            "input_path": supplied,
            "origin": "workflow_output",
            "registry_source_artifact_id": 7,
        }
    ]
    run_plan_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        runtime_module,
        "_load_callable",
        lambda _ref: pytest.fail("callable must not be loaded"),
    )

    expected = (
        "exactly cover reused workflow inputs"
        if failure == "missing_authority"
        else "does not match its bound canonical occurrence"
    )
    with pytest.raises(RuntimeError, match=expected):
        runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)


def test_runtime_rejects_duplicate_input_binding_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_plan_path, job_id = _write_runtime_plan(tmp_path)
    input_path = run_plan_path.parent / "staging/upstream/out/sub_001.json"
    input_path.parent.mkdir(parents=True)
    input_path.write_text("upstream\n", encoding="utf-8")
    relative = "staging/upstream/out/sub_001.json"
    payload = json.loads(run_plan_path.read_text(encoding="utf-8"))
    job = payload["jobs"][job_id]
    job["inputs"] = {"upstream": [relative, relative]}
    job["input_records"] = [
        {
            "binding_name": "upstream",
            "input_path": relative,
            "origin": "workflow_output",
            "registry_source_artifact_id": None,
        },
        {
            "binding_name": "upstream",
            "input_path": relative,
            "origin": "workflow_output",
            "registry_source_artifact_id": None,
        },
    ]
    run_plan_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        runtime_module,
        "_load_callable",
        lambda _ref: pytest.fail("callable must not be loaded"),
    )

    with pytest.raises(RuntimeError, match="duplicate input path"):
        runtime_module.run_job(run_plan_path=run_plan_path, job_id=job_id)


def test_completion_receipt_atomic_writer_uses_same_directory_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "receipts/job.json"
    receipt = CompletionReceipt(
        invocation_token="a" * 32,
        job_id="job",
        request_bundle_digest="b" * 64,
        outputs=("result",),
    )
    replacements: list[tuple[Path, Path]] = []
    original_replace = evidence_module.os.replace

    def record_replace(source: Path, destination: Path) -> None:
        replacements.append((Path(source), Path(destination)))
        original_replace(source, destination)

    monkeypatch.setattr(evidence_module.os, "replace", record_replace)

    write_completion_receipt_atomic(path, receipt)

    assert len(replacements) == 1
    assert replacements[0][0].parent == path.parent
    assert replacements[0][1] == path
    assert read_completion_receipt(path) == receipt
    assert list(path.parent.glob("*.tmp")) == []


def test_atomic_writer_cleans_temporary_after_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "receipts/job.json"
    receipt = CompletionReceipt(
        invocation_token="a" * 32,
        job_id="job",
        request_bundle_digest="b" * 64,
        outputs=("result",),
    )
    monkeypatch.setattr(
        evidence_module.os,
        "replace",
        lambda *_args: (_ for _ in ()).throw(OSError("replace failed")),
    )

    with pytest.raises(OSError, match="replace failed"):
        write_completion_receipt_atomic(path, receipt)

    assert not path.exists()
    assert list(path.parent.iterdir()) == []


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": COMPLETION_RECEIPT_SCHEMA_VERSION + 1},
        {"invocation_token": "wrong"},
        {"request_bundle_digest": "wrong"},
        {"outputs": ["second", "first"]},
        {"extra": True},
    ],
)
def test_completion_receipt_rejects_mismatched_contract(
    change: dict[str, object],
) -> None:
    payload = CompletionReceipt(
        invocation_token="a" * 32,
        job_id="job",
        request_bundle_digest="b" * 64,
        outputs=("first", "second"),
    ).to_payload()
    payload.update(change)

    with pytest.raises(ExecutionEvidenceError):
        CompletionReceipt.from_payload(payload)


def test_completion_receipt_rejects_non_utf8_file(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    path.write_bytes(b"\xff")

    with pytest.raises(ExecutionEvidenceError, match="unreadable"):
        read_completion_receipt(path)
