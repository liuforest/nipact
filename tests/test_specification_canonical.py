from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import pytest

from nipact.errors import ValidationError
from nipact.manifest import build_manifest
from nipact.specification_canonical import (
    ROW_SCHEMA,
    SNAPSHOT_SCHEMA,
    canonicalize_specification_snapshot,
    decode_specification_row,
    decode_specification_snapshot,
    replay_specification_member,
)
from nipact.specification_compiler import (
    DecisionCoordinate,
    ExecutionPopulationWrite,
    ManifestBindingWrite,
    ParameterWrite,
    ProvisionalCompilation,
    ProvisionalMember,
    ResultWrite,
    TargetWrite,
)
from nipact.workflow import (
    LoadedWorkflowProject,
    ManifestBinding,
    SourceIndex,
    StepDefinition,
    StepInput,
    StepOutput,
    WorkflowDefinition,
    compile_workflow_plan,
)

_GOLDEN_DIR = Path(__file__).parent / "fixtures/specification_canonical"


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _project(*, context: str = "canonical") -> LoadedWorkflowProject:
    manifests = {
        "all": build_manifest(
            description="all",
            entities=["entity_002", "entity_001"],
        ),
        "selected": build_manifest(
            description="selected",
            entities=["entity_001"],
        ),
    }
    source = StepDefinition(
        name="source",
        pattern_kind="pattern_a",
        execution_role="source_import",
        address_scope="entity",
        callable_ref="example_runtime:source",
        step_contract_version="1",
        inputs={},
        source_inputs=("seed",),
        params={},
        outputs={
            "raw": StepOutput(name="raw", extension=".json", address_scope="entity")
        },
        manifest_binding=None,
        source_path=Path("/declarations/source.yaml"),
    )
    model = StepDefinition(
        name="model",
        pattern_kind="analysis",
        execution_role="analysis",
        address_scope="entity",
        callable_ref="example_runtime:model",
        step_contract_version="1",
        inputs={
            "raw": StepInput(
                name="raw",
                artifact="source.raw",
                dependency_role="analysis_input",
                source_step_name="source",
                source_output_name="raw",
            )
        },
        source_inputs=(),
        params={
            "alpha": 1.0,
            "metadata": {
                "enabled": True,
                "label": "café",
                "nothing": None,
                "values": [1, 2.5],
            },
        },
        outputs={
            "diagnostics": StepOutput(
                name="diagnostics",
                extension=".json",
                address_scope="entity",
            ),
            "estimate": StepOutput(
                name="estimate",
                extension=".json",
                address_scope="entity",
            ),
        },
        manifest_binding=ManifestBinding(
            role="analysis_population",
            manifest_name="all",
        ),
        source_path=Path("/declarations/model.yaml"),
    )
    alternative = replace(
        model,
        name="alternative_model",
        callable_ref="example_runtime:alternative_model",
        inputs={"input": replace(model.inputs["raw"], name="input")},
        outputs={
            "report": StepOutput(
                name="report",
                extension=".json",
                address_scope="entity",
            ),
            "trace": StepOutput(
                name="trace",
                extension=".json",
                address_scope="entity",
            ),
        },
        source_path=Path("/declarations/alternative_model.yaml"),
    )
    workflows = {
        "base": WorkflowDefinition(
            name="base",
            base_workflow=None,
            execution_population_name="all",
            steps=("source", "model"),
            step_outputs={"source": "raw", "model": "estimate"},
            step_overrides={},
            source_path=Path("/declarations/base.yaml"),
        ),
        "alternative": WorkflowDefinition(
            name="alternative",
            base_workflow=None,
            execution_population_name="all",
            steps=("source", "alternative_model"),
            step_outputs={"source": "raw", "alternative_model": "report"},
            step_overrides={},
            source_path=Path("/declarations/alternative.yaml"),
        ),
    }
    return LoadedWorkflowProject(
        project_root=Path("/project"),
        context=context,
        runtime_root=Path("/runtime"),
        source_index=SourceIndex(
            path=Path("/runtime/sources.yaml"),
            global_bindings={},
            entity_bindings={},
        ),
        manifests=manifests,
        manifest_paths={name: Path(f"/declarations/{name}.yaml") for name in manifests},
        steps={
            "source": source,
            "model": model,
            "alternative_model": alternative,
        },
        workflows=workflows,
    )


def _member(
    key: str,
    value: float,
    *,
    workflow: str = "base",
    disposition: str = "included",
) -> ProvisionalMember:
    if workflow == "base":
        step_name, target, diagnostics = "model", "estimate", "diagnostics"
    else:
        step_name, target, diagnostics = "alternative_model", "report", "trace"
    reason = None if disposition == "included" else "outside primary analysis"
    return ProvisionalMember(
        member_key=key,
        decision_coordinates=(DecisionCoordinate("alpha", value),),
        workflow_name=workflow,
        writes=(
            ParameterWrite(step_name, "alpha", value),
            ExecutionPopulationWrite("selected"),
            ManifestBindingWrite(step_name, "analysis_population", "selected"),
            TargetWrite(step_name, target),
            ResultWrite("diagnostics", step_name, diagnostics),
            ResultWrite("estimate", step_name, target),
        ),
        disposition=disposition,  # type: ignore[arg-type]
        exclusion_reason=reason,
    )


def _nested_member(key: str = "member-a") -> ProvisionalMember:
    nested = {
        "enabled": False,
        "typed_values": [True, 1, 1.0, 0.0, -0.0],
        "nested": {"labels": ["café", None]},
    }
    base = _member(key, 1.0)
    return replace(
        base,
        decision_coordinates=(DecisionCoordinate("configuration", deepcopy(nested)),),
        writes=(ParameterWrite("model", "metadata", nested), *base.writes[1:]),
    )


def _compilation(*members: ProvisionalMember) -> ProvisionalCompilation:
    return ProvisionalCompilation(
        set_key="ignored-observational-key",
        members=members,
        equivalent_member_groups=(),
    )


def _snapshot_payload(snapshot: Any) -> dict[str, Any]:
    return json.loads(snapshot.canonical_bytes)


def test_golden_payloads_decode_to_frozen_bytes_and_digests() -> None:
    expected = json.loads((_GOLDEN_DIR / "digests.json").read_text(encoding="utf-8"))

    for name, digest in expected.items():
        data = (_GOLDEN_DIR / name).read_bytes()
        assert not data.endswith(b"\n")
        if name == "core-snapshot.json":
            domain = b"nipact.specification.snapshot.v1\0"
            decoded = decode_specification_snapshot(data)
            assert decoded.snapshot_digest == digest
        else:
            domain = b"nipact.specification.row.v1\0"
            decoded = decode_specification_row(data)
            assert decoded.row_digest == digest
        assert hashlib.sha256(domain + data).hexdigest() == digest
        assert decoded.canonical_bytes == data

    core_snapshot = json.loads(
        (_GOLDEN_DIR / "core-snapshot.json").read_bytes()
    )
    embedded_core_row = _canonical_bytes(core_snapshot["members"][0]["row"])
    assert embedded_core_row == (_GOLDEN_DIR / "core-row.json").read_bytes()


def test_snapshot_freezes_rows_manifest_values_and_expected_results() -> None:
    loaded = _project()
    loaded_before = deepcopy(loaded)
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=_compilation(
            _member("member-b", -0.0, disposition="excluded"),
            _member("member-a", 0.0),
        ),
    )

    assert loaded == loaded_before
    assert snapshot.members[0].row.row_digest == (
        "10be7116a7ad9f6561c45db2c78a5606e6a6b467f664f970e71a8f5e36ee6c8b"
    )
    assert snapshot.snapshot_digest == (
        "b479af3d4df07fba505ba4912e945e29df7e9f71379e176fc3a0d9d38c66a949"
    )
    assert snapshot.canonical_bytes == _canonical_bytes(_snapshot_payload(snapshot))
    assert not snapshot.canonical_bytes.endswith(b"\n")
    assert [member.member_key for member in snapshot.members] == ["member-a", "member-b"]
    assert [member.disposition for member in snapshot.members] == ["included", "excluded"]
    assert len(snapshot.manifest_values) == 1
    assert snapshot.manifest_values[0] == loaded.manifests["selected"].value
    assert [
        (result.role, result.step_name, result.output_name, result.address)
        for result in snapshot.members[0].expected_results
    ] == [
        ("diagnostics", "model", "diagnostics", "entity_001"),
        ("estimate", "model", "estimate", "entity_001"),
    ]

    row_payload = _snapshot_payload(snapshot)["members"][0]["row"]
    assert row_payload["schema"] == ROW_SCHEMA
    assert "context" not in row_payload
    assert {write["type"] for write in row_payload["writes"]} == {
        "parameter",
        "execution_population",
        "manifest_binding",
        "target",
        "result",
    }
    assert row_payload["effective_declaration"]["steps"][1]["params"]["metadata"] == {
        "enabled": True,
        "label": "café",
        "nothing": None,
        "values": [1, 2.5],
    }
    assert _snapshot_payload(snapshot)["schema"] == SNAPSHOT_SCHEMA


def test_rows_are_context_neutral_and_signed_zero_remains_distinct() -> None:
    compilation = _compilation(_member("member-a", 0.0), _member("member-b", -0.0))
    first = canonicalize_specification_snapshot(
        loaded=_project(context="first"),
        compilation=compilation,
    )
    second = canonicalize_specification_snapshot(
        loaded=_project(context="second"),
        compilation=compilation,
    )

    assert [member.row.row_digest for member in first.members] == [
        member.row.row_digest for member in second.members
    ]
    assert first.snapshot_digest != second.snapshot_digest
    values = [member.row.decision_coordinates[0].value for member in first.members]
    assert [math.copysign(1.0, value) for value in values] == [1.0, -1.0]
    assert first.members[0].row.row_digest != first.members[1].row.row_digest
    same_context = canonicalize_specification_snapshot(
        loaded=_project(context="first"),
        compilation=replace(compilation, set_key="another-observational-key"),
    )
    assert same_context.canonical_bytes == first.canonical_bytes


def test_distinct_coordinates_may_share_an_effective_declaration() -> None:
    first = _member("member-a", 1.0)
    second = replace(
        first,
        member_key="member-b",
        decision_coordinates=(DecisionCoordinate("label", "same-effective-value"),),
    )

    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(first, second),
    )

    assert snapshot.members[0].row.effective_declaration == (
        snapshot.members[1].row.effective_declaration
    )
    assert snapshot.members[0].row.row_digest != snapshot.members[1].row.row_digest


def test_byte_identical_rows_fail_closed() -> None:
    first = _member("member-a", 1.0)

    with pytest.raises(ValidationError, match="byte-identical"):
        canonicalize_specification_snapshot(
            loaded=_project(),
            compilation=_compilation(first, replace(first, member_key="member-b")),
        )


def test_alternative_topology_is_compatible_when_result_signature_matches() -> None:
    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(
            _member("member-a", 1.0),
            _member("member-b", 2.0, workflow="alternative"),
        ),
    )

    assert [member.row.workflow_selector for member in snapshot.members] == [
        "base",
        "alternative",
    ]
    assert snapshot.members[1].row.row_digest == (
        "9d4716c71e5493c643eedf6d641503703d3e63b23f80071633369975e7e96e7c"
    )
    assert [
        (result.role, result.address)
        for result in snapshot.members[1].expected_results
    ] == [("diagnostics", "entity_001"), ("estimate", "entity_001")]
    assert snapshot.members[0].row.effective_declaration != (
        snapshot.members[1].row.effective_declaration
    )


def test_result_signature_mismatch_fails() -> None:
    mismatched = replace(
        _member("member-b", 2.0, workflow="alternative"),
        writes=(
            ParameterWrite("alternative_model", "alpha", 2.0),
            ExecutionPopulationWrite("selected"),
            ManifestBindingWrite(
                "alternative_model", "analysis_population", "selected"
            ),
            TargetWrite("alternative_model", "report"),
            ResultWrite("estimate", "alternative_model", "report"),
        ),
    )

    with pytest.raises(ValidationError, match="role/address-scope signature"):
        canonicalize_specification_snapshot(
            loaded=_project(),
            compilation=_compilation(_member("member-a", 1.0), mismatched),
        )


def test_population_identity_and_cardinality_are_not_signature_fields() -> None:
    broader = replace(
        _member("member-b", 2.0),
        writes=(
            ParameterWrite("model", "alpha", 2.0),
            ExecutionPopulationWrite("all"),
            ManifestBindingWrite("model", "analysis_population", "all"),
            TargetWrite("model", "estimate"),
            ResultWrite("diagnostics", "model", "diagnostics"),
            ResultWrite("estimate", "model", "estimate"),
        ),
    )

    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(_member("member-a", 1.0), broader),
    )

    assert [len(member.expected_results) for member in snapshot.members] == [2, 4]
    assert len(snapshot.manifest_values) == 2


def test_decoders_require_canonical_bytes_and_exact_embedded_digests() -> None:
    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(_member("member-a", 1.0)),
    )
    member = snapshot.members[0]

    assert decode_specification_row(member.row.canonical_bytes) == member.row
    assert decode_specification_snapshot(snapshot.canonical_bytes) == snapshot
    with pytest.raises(TypeError):
        member.row.effective_declaration.steps[1].params["alpha"] = 999
    metadata = member.row.effective_declaration.steps[1].params["metadata"]
    with pytest.raises((AttributeError, TypeError)):
        metadata["values"].append(3)
    with pytest.raises(TypeError):
        dict.__setitem__(member.row.effective_declaration.steps[1].params, "alpha", 999)
    with pytest.raises(TypeError):
        list.append(metadata["values"], 3)
    frozen_mapping = member.row.effective_declaration.steps[1].params
    frozen_list = metadata["values"]
    with pytest.raises(TypeError):
        frozen_mapping._values["alpha"] = 999
    with pytest.raises(TypeError, match="immutable"):
        frozen_mapping._values = {}
    with pytest.raises(TypeError):
        frozen_list._values[0] = 999
    with pytest.raises(TypeError, match="immutable"):
        frozen_list._values = ()
    with pytest.raises(TypeError, match="canonical decoder"):
        replace(member.row, row_digest="0" * 64)
    with pytest.raises(TypeError, match="snapshot decoder"):
        replace(snapshot, snapshot_digest="0" * 64)
    with pytest.raises(ValidationError, match="not canonical JSON"):
        decode_specification_row(member.row.canonical_bytes + b"\n")

    payload = _snapshot_payload(snapshot)
    payload["members"][0]["row_digest"] = "0" * 64
    with pytest.raises(ValidationError, match="row_digest"):
        decode_specification_snapshot(_canonical_bytes(payload))


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b'{"schema":1,"schema":2}', "duplicate JSON key"),
        (b'{"value":NaN}', "non-finite number"),
        (b'{"value":1e400}', "non-finite number"),
        (b'{"value":' + (b"9" * 5000) + b'}', "not valid JSON"),
    ],
)
def test_row_decoder_rejects_ambiguous_json(payload: bytes, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        decode_specification_row(payload)


def test_row_decoder_rejects_unknown_fields_and_broken_closure_references() -> None:
    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(_member("member-a", 1.0)),
    )
    payload = _snapshot_payload(snapshot)["members"][0]["row"]
    with_unknown = deepcopy(payload)
    with_unknown["unknown"] = True
    with pytest.raises(ValidationError, match="unknown field"):
        decode_specification_row(_canonical_bytes(with_unknown))

    broken_reference = deepcopy(payload)
    broken_reference["effective_declaration"]["steps"][1]["inputs"][0][
        "source_output_name"
    ] = "missing"
    with pytest.raises(ValidationError, match="undeclared source output"):
        decode_specification_row(_canonical_bytes(broken_reference))

    unrelated_step = deepcopy(payload)
    extra = deepcopy(unrelated_step["effective_declaration"]["steps"][0])
    extra["step_name"] = "unrelated"
    unrelated_step["effective_declaration"]["steps"].append(extra)
    with pytest.raises(ValidationError, match="reverse dependency closure"):
        decode_specification_row(_canonical_bytes(unrelated_step))


def test_frozen_json_equality_preserves_json_scalar_types_and_signed_zero() -> None:
    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(_nested_member()),
    )
    write = next(
        item
        for item in snapshot.members[0].row.writes
        if isinstance(item, ParameterWrite)
    )
    value = write.value
    exact = {
        "enabled": False,
        "typed_values": [True, 1, 1.0, 0.0, -0.0],
        "nested": {"labels": ["café", None]},
    }
    assert value == exact

    bool_as_int = deepcopy(exact)
    bool_as_int["typed_values"][0] = 1
    int_as_float = deepcopy(exact)
    int_as_float["typed_values"][1] = 1.0
    positive_zero = deepcopy(exact)
    positive_zero["typed_values"][-1] = 0.0
    assert value != bool_as_int
    assert value != int_as_float
    assert value != positive_zero


@pytest.mark.parametrize("change", ["missing", "extra"])
def test_snapshot_manifest_closure_is_exact(change: str) -> None:
    snapshot = canonicalize_specification_snapshot(
        loaded=_project(),
        compilation=_compilation(_member("member-a", 1.0)),
    )
    payload = _snapshot_payload(snapshot)
    if change == "missing":
        payload["manifest_values"] = []
        message = "missing execution-population value"
    else:
        payload["manifest_values"].append(
            {
                "value_schema": _project().manifests["all"].value.value_schema,
                "manifest_digest": _project().manifests["all"].value.manifest_digest,
                "canonical_body": _project().manifests["all"].value.canonical_body,
            }
        )
        payload["manifest_values"].sort(
            key=lambda item: (item["value_schema"], item["manifest_digest"])
        )
        message = "unreferenced manifest value"

    with pytest.raises(ValidationError, match=message):
        decode_specification_snapshot(_canonical_bytes(payload))


def test_replay_accepts_equal_declaration_and_rejects_excluded_or_drifted() -> None:
    loaded = _project()
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=_compilation(
            _member("member-a", 1.0),
            _member("member-b", 2.0, disposition="excluded"),
        ),
    )

    replayed = replay_specification_member(
        loaded=loaded,
        snapshot=snapshot,
        member_key="member-a",
    )
    assert replayed.member.member_key == "member-a"
    with pytest.raises(ValidationError, match="excluded"):
        replay_specification_member(
            loaded=loaded,
            snapshot=snapshot,
            member_key="member-b",
        )

    drifted_model = replace(
        loaded.steps["model"],
        callable_ref="example_runtime:changed_model",
    )
    drifted = replace(loaded, steps={**loaded.steps, "model": drifted_model})
    with pytest.raises(ValidationError, match="restore the matching project revision"):
        replay_specification_member(
            loaded=drifted,
            snapshot=snapshot,
            member_key="member-a",
        )


def test_replay_restores_plain_owned_json_before_ordinary_planning() -> None:
    loaded = _project()
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=_compilation(_nested_member()),
    )

    replayed = replay_specification_member(
        loaded=loaded,
        snapshot=snapshot,
        member_key="member-a",
    )
    coordinate_value = replayed.member.decision_coordinates[0].value
    assert type(coordinate_value) is dict
    assert type(coordinate_value["typed_values"]) is list
    write = next(
        item for item in replayed.member.writes if isinstance(item, ParameterWrite)
    )
    assert type(write.value) is dict
    assert type(write.value["nested"]) is dict
    assert type(write.value["nested"]["labels"]) is list

    plan = compile_workflow_plan(
        replayed.loaded_project,
        workflow_name="base",
        step_name="model",
    )
    metadata = plan.steps[-1].params["metadata"]
    assert type(metadata) is dict
    assert type(metadata["typed_values"]) is list


@pytest.mark.parametrize("missing", ["workflow", "parameter_destination"])
def test_replay_normalizes_reconstruction_failures(missing: str) -> None:
    loaded = _project()
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=_compilation(_member("member-a", 1.0)),
    )
    if missing == "workflow":
        drifted = replace(
            loaded,
            workflows={
                name: workflow
                for name, workflow in loaded.workflows.items()
                if name != "base"
            },
        )
    else:
        model = loaded.steps["model"]
        drifted = replace(
            loaded,
            steps={
                **loaded.steps,
                "model": replace(
                    model,
                    params={
                        name: value
                        for name, value in model.params.items()
                        if name != "alpha"
                    },
                ),
            },
        )

    with pytest.raises(
        ValidationError,
        match="restore the matching project revision",
    ) as raised:
        replay_specification_member(
            loaded=drifted,
            snapshot=snapshot,
            member_key="member-a",
        )
    assert isinstance(raised.value.__cause__, ValidationError)


def test_replay_compares_effective_values_after_writes_and_ignores_paths() -> None:
    loaded = _project()
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded,
        compilation=_compilation(_member("member-a", 1.0)),
    )

    model = loaded.steps["model"]
    overwritten_base = replace(
        loaded,
        project_root=Path("/moved/project"),
        runtime_root=Path("/moved/runtime"),
        source_index=replace(loaded.source_index, path=Path("/moved/sources.yaml")),
        steps={
            **loaded.steps,
            "model": replace(
                model,
                params={**model.params, "alpha": 999.0},
                source_path=Path("/moved/model.yaml"),
            ),
        },
        workflows={
            **loaded.workflows,
            "base": replace(
                loaded.workflows["base"],
                source_path=Path("/moved/base.yaml"),
            ),
        },
    )
    assert replay_specification_member(
        loaded=overwritten_base,
        snapshot=snapshot,
        member_key="member-a",
    ).member.member_key == "member-a"

    unwritten_drift = replace(
        loaded,
        steps={
            **loaded.steps,
            "model": replace(
                model,
                params={
                    **model.params,
                    "metadata": {**model.params["metadata"], "enabled": False},
                },
            ),
        },
    )
    with pytest.raises(ValidationError, match="restore the matching project revision"):
        replay_specification_member(
            loaded=unwritten_drift,
            snapshot=snapshot,
            member_key="member-a",
        )
