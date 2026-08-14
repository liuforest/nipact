"""Canonical byte contracts for finite specification snapshots."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import InitVar, dataclass
import json
import math
from types import MappingProxyType
from typing import Any

from .errors import ValidationError
from .hashing import is_valid_digest, sha256_digest
from .identity import validate_path_token
from .manifest import MANIFEST_VALUE_SCHEMA, ManifestValue
from .specification_adapter import (
    AppliedProvisionalMember,
    ProvisionalEffectiveDeclaration,
    apply_provisional_member,
)
from .specification_compiler import (
    _CanonicalJsonSequence,
    DecisionCoordinate,
    ExecutionPopulationWrite,
    ManifestBindingWrite,
    ParameterWrite,
    ProvisionalCompilation,
    ProvisionalMember,
    ResultWrite,
    SpecificationWrite,
    TargetWrite,
)
from .workflow import (
    ADDRESS_SCOPES,
    DEPENDENCY_ROLES,
    EXECUTION_ROLES,
    PATTERN_KINDS,
    LoadedWorkflowProject,
)

ROW_SCHEMA = "nipact/specification-row/v1"
SNAPSHOT_SCHEMA = "nipact/specification-snapshot/v1"

_ROW_DOMAIN_PREFIX = b"nipact.specification.row.v1\0"
_SNAPSHOT_DOMAIN_PREFIX = b"nipact.specification.snapshot.v1\0"
_CONSTRUCTION_TOKEN = object()
_REPLAY_MISMATCH = (
    "current workflow declaration does not match frozen specification member; "
    "restore the matching project revision or freeze a new snapshot"
)
_WRITE_TYPE_ORDER = {
    "parameter": 0,
    "execution_population": 1,
    "manifest_binding": 2,
    "target": 3,
    "result": 4,
}


class _FrozenDict(Mapping[str, Any]):
    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        object.__setattr__(self, "_values", MappingProxyType(dict(values)))

    def __setattr__(self, _name: str, _value: object) -> None:
        raise TypeError("canonical JSON values are immutable")

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __eq__(self, other: object) -> bool:
        return _typed_json_equal(self, other)

    def __deepcopy__(self, _memo: dict[int, object]) -> _FrozenDict:
        return self


class _FrozenList(_CanonicalJsonSequence):
    __slots__ = ("_values",)

    def __init__(self, values: Sequence[Any]) -> None:
        object.__setattr__(self, "_values", tuple(values))

    def __setattr__(self, _name: str, _value: object) -> None:
        raise TypeError("canonical JSON values are immutable")

    def __getitem__(self, index: int | slice) -> Any:
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)

    def __eq__(self, other: object) -> bool:
        return _typed_json_equal(self, other)

    def __deepcopy__(self, _memo: dict[int, object]) -> _FrozenList:
        return self


@dataclass(frozen=True)
class CanonicalManifestReference:
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str


@dataclass(frozen=True)
class CanonicalManifestBinding:
    manifest_usage_role: str
    manifest_name: str
    manifest_value_schema: str
    manifest_digest: str


@dataclass(frozen=True)
class CanonicalStepInput:
    name: str
    dependency_role: str
    source_step_name: str
    source_output_name: str


@dataclass(frozen=True)
class CanonicalStepOutput:
    name: str
    extension: str
    address_scope: str


@dataclass(frozen=True)
class CanonicalEffectiveStep:
    step_name: str
    pattern_kind: str
    execution_role: str
    address_scope: str
    callable_ref: str
    step_contract_version: str
    inputs: tuple[CanonicalStepInput, ...]
    source_inputs: tuple[str, ...]
    params: dict[str, Any]
    outputs: tuple[CanonicalStepOutput, ...]
    manifest_binding: CanonicalManifestBinding | None

    __hash__ = None


@dataclass(frozen=True)
class CanonicalEffectiveResult:
    role: str
    step_name: str
    output_name: str
    address_scope: str


@dataclass(frozen=True)
class CanonicalEffectiveDeclaration:
    target_step_name: str
    target_output_name: str
    steps: tuple[CanonicalEffectiveStep, ...]
    execution_population: CanonicalManifestReference | None
    results: tuple[CanonicalEffectiveResult, ...]

    __hash__ = None


@dataclass(frozen=True)
class CanonicalSpecificationRow:
    decision_coordinates: tuple[DecisionCoordinate, ...]
    workflow_selector: str
    writes: tuple[SpecificationWrite, ...]
    effective_declaration: CanonicalEffectiveDeclaration
    canonical_bytes: bytes
    row_digest: str
    _construction_token: InitVar[object] = None

    __hash__ = None

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _CONSTRUCTION_TOKEN:
            raise TypeError("canonical rows must be created by the canonical decoder")


@dataclass(frozen=True)
class ExpectedResultDescriptor:
    role: str
    step_name: str
    output_name: str
    address: str


@dataclass(frozen=True)
class CanonicalSpecificationMember:
    member_key: str
    disposition: str
    exclusion_reason: str | None
    row: CanonicalSpecificationRow
    expected_results: tuple[ExpectedResultDescriptor, ...]
    _construction_token: InitVar[object] = None

    __hash__ = None

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _CONSTRUCTION_TOKEN:
            raise TypeError("canonical members must be created by the snapshot decoder")


@dataclass(frozen=True)
class CanonicalSpecificationSnapshot:
    context: str
    members: tuple[CanonicalSpecificationMember, ...]
    manifest_values: tuple[ManifestValue, ...]
    canonical_bytes: bytes
    snapshot_digest: str
    _construction_token: InitVar[object] = None

    __hash__ = None

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _CONSTRUCTION_TOKEN:
            raise TypeError("canonical snapshots must be created by the snapshot decoder")


def canonicalize_specification_snapshot(
    *,
    loaded: LoadedWorkflowProject,
    compilation: ProvisionalCompilation,
) -> CanonicalSpecificationSnapshot:
    """Apply provisional members and freeze one self-contained snapshot value."""
    if type(loaded) is not LoadedWorkflowProject:
        raise ValidationError("loaded must be a LoadedWorkflowProject")
    if type(compilation) is not ProvisionalCompilation:
        raise ValidationError("compilation must be a ProvisionalCompilation")

    rows_by_key: dict[str, CanonicalSpecificationRow] = {}
    members_by_key: dict[str, ProvisionalMember] = {}
    for member in compilation.members:
        if member.member_key in members_by_key:
            raise ValidationError(f"duplicate specification member key: {member.member_key}")
        applied = apply_provisional_member(loaded=loaded, member=member)
        effective = _effective_from_adapter(
            applied.effective_declaration,
            workflow_selector=member.workflow_name,
        )
        row_payload = {
            "schema": ROW_SCHEMA,
            "decision_coordinates": [
                {
                    "name": coordinate.name,
                    "value": _copy_json(coordinate.value, label="decision coordinate"),
                }
                for coordinate in sorted(
                    member.decision_coordinates,
                    key=lambda coordinate: coordinate.name,
                )
            ],
            "workflow_selector": member.workflow_name,
            "writes": [
                _write_payload(write)
                for write in sorted(member.writes, key=_write_sort_key)
            ],
            "effective_declaration": _effective_payload(effective),
        }
        row = decode_specification_row(_dump_json(row_payload))
        members_by_key[member.member_key] = member
        rows_by_key[member.member_key] = row

    manifest_values = _manifest_values_for_rows(
        loaded=loaded,
        rows=tuple(rows_by_key.values()),
    )
    snapshot_payload = {
        "schema": SNAPSHOT_SCHEMA,
        "context": loaded.context,
        "members": [
            {
                "member_key": member_key,
                "row": _row_payload(rows_by_key[member_key]),
                "row_digest": rows_by_key[member_key].row_digest,
                "disposition": members_by_key[member_key].disposition,
                "exclusion_reason": members_by_key[member_key].exclusion_reason,
            }
            for member_key in sorted(members_by_key)
        ],
        "manifest_values": [
            {
                "value_schema": value.value_schema,
                "manifest_digest": value.manifest_digest,
                "canonical_body": value.canonical_body,
            }
            for value in manifest_values
        ],
    }
    return decode_specification_snapshot(_dump_json(snapshot_payload))


def decode_specification_row(data: bytes) -> CanonicalSpecificationRow:
    """Strictly decode canonical row bytes and compute their value digest."""
    payload = _load_canonical_json(data, label="specification row")
    mapping = _exact_mapping(payload, label="specification row")
    return _row_from_payload(mapping, canonical_bytes=data)


def decode_specification_snapshot(data: bytes) -> CanonicalSpecificationSnapshot:
    """Strictly decode a snapshot and derive its expected-result projections."""
    payload = _load_canonical_json(data, label="specification snapshot")
    root = _exact_mapping(payload, label="specification snapshot")
    _fields(
        root,
        expected={"schema", "context", "members", "manifest_values"},
        label="specification snapshot",
    )
    if root["schema"] != SNAPSHOT_SCHEMA:
        raise ValidationError(f"specification snapshot schema must be {SNAPSHOT_SCHEMA!r}")
    context = _token(root["context"], label="specification snapshot context")

    manifest_values = _parse_manifest_values(root["manifest_values"])
    manifests_by_identity = {
        (value.value_schema, value.manifest_digest): value
        for value in manifest_values
    }

    member_payloads = _exact_list(root["members"], label="snapshot members")
    if not member_payloads:
        raise ValidationError("snapshot members cannot be empty")
    members: list[CanonicalSpecificationMember] = []
    seen_keys: set[str] = set()
    seen_rows: set[bytes] = set()
    referenced_manifests: set[tuple[str, str]] = set()
    previous_key: str | None = None
    common_signature: tuple[tuple[str, str], ...] | None = None
    for index, raw_member in enumerate(member_payloads):
        label = f"snapshot member {index}"
        member_payload = _exact_mapping(raw_member, label=label)
        _fields(
            member_payload,
            expected={
                "member_key",
                "row",
                "row_digest",
                "disposition",
                "exclusion_reason",
            },
            label=label,
        )
        member_key = _token(member_payload["member_key"], label=f"{label} key")
        if previous_key is not None and member_key <= previous_key:
            raise ValidationError("snapshot members must be sorted by unique member_key")
        previous_key = member_key
        if member_key in seen_keys:
            raise ValidationError(f"duplicate specification member key: {member_key}")
        seen_keys.add(member_key)

        row_mapping = _exact_mapping(member_payload["row"], label=f"{label} row")
        row_bytes = _dump_json(row_mapping)
        row = decode_specification_row(row_bytes)
        stored_row_digest = _digest(
            member_payload["row_digest"],
            label=f"{label} row_digest",
        )
        if stored_row_digest != row.row_digest:
            raise ValidationError(f"{label} row_digest does not match embedded row")
        if row.canonical_bytes in seen_rows:
            raise ValidationError("snapshot contains byte-identical specification rows")
        seen_rows.add(row.canonical_bytes)

        disposition = member_payload["disposition"]
        reason = member_payload["exclusion_reason"]
        if disposition == "included":
            if reason is not None:
                raise ValidationError(f"{label} included disposition requires null reason")
            exclusion_reason = None
        elif disposition == "excluded":
            exclusion_reason = _trimmed_string(reason, label=f"{label} exclusion_reason")
        else:
            raise ValidationError(f"{label} disposition must be 'included' or 'excluded'")

        row_references = _manifest_references(row.effective_declaration)
        referenced_manifests.update(
            (reference.manifest_value_schema, reference.manifest_digest)
            for reference in row_references
        )
        expected_results = _derive_expected_results(
            row.effective_declaration,
            manifests_by_identity=manifests_by_identity,
        )
        signature = tuple(
            (result.role, result.address_scope)
            for result in row.effective_declaration.results
        )
        if common_signature is None:
            common_signature = signature
        elif signature != common_signature:
            raise ValidationError(
                "snapshot members must share the same result role/address-scope signature"
            )
        members.append(
            CanonicalSpecificationMember(
                member_key=member_key,
                disposition=disposition,
                exclusion_reason=exclusion_reason,
                row=row,
                expected_results=expected_results,
                _construction_token=_CONSTRUCTION_TOKEN,
            )
        )

    supplied_manifests = set(manifests_by_identity)
    if referenced_manifests != supplied_manifests:
        missing = sorted(referenced_manifests - supplied_manifests)
        extra = sorted(supplied_manifests - referenced_manifests)
        if missing:
            raise ValidationError(
                "snapshot is missing referenced manifest value: "
                f"{missing[0][0]}:{missing[0][1]}"
            )
        raise ValidationError(
            "snapshot contains unreferenced manifest value: "
            f"{extra[0][0]}:{extra[0][1]}"
        )

    return CanonicalSpecificationSnapshot(
        context=context,
        members=tuple(members),
        manifest_values=manifest_values,
        canonical_bytes=data,
        snapshot_digest=sha256_digest(_SNAPSHOT_DOMAIN_PREFIX + data),
        _construction_token=_CONSTRUCTION_TOKEN,
    )


def replay_specification_member(
    *,
    loaded: LoadedWorkflowProject,
    snapshot: CanonicalSpecificationSnapshot,
    member_key: str,
) -> AppliedProvisionalMember:
    """Replay one included row and require its effective declaration to match."""
    if type(loaded) is not LoadedWorkflowProject:
        raise ValidationError("loaded must be a LoadedWorkflowProject")
    if type(snapshot) is not CanonicalSpecificationSnapshot:
        raise ValidationError("snapshot must be a CanonicalSpecificationSnapshot")
    if loaded.context != snapshot.context:
        raise ValidationError("loaded project context does not match specification snapshot")
    key = _token(member_key, label="specification member key")
    member = next((item for item in snapshot.members if item.member_key == key), None)
    if member is None:
        raise ValidationError(f"unknown specification member: {key}")
    if member.disposition == "excluded":
        raise ValidationError(f"excluded specification member cannot be replayed: {key}")

    try:
        provisional = ProvisionalMember(
            member_key=member.member_key,
            decision_coordinates=tuple(
                DecisionCoordinate(
                    name=coordinate.name,
                    value=_plain_json(coordinate.value),
                )
                for coordinate in member.row.decision_coordinates
            ),
            workflow_name=member.row.workflow_selector,
            writes=tuple(_owned_replay_write(write) for write in member.row.writes),
            disposition="included",
            exclusion_reason=None,
        )
        applied = apply_provisional_member(loaded=loaded, member=provisional)
        current = _effective_from_adapter(
            applied.effective_declaration,
            workflow_selector=member.row.workflow_selector,
        )
    except ValidationError as exc:
        raise ValidationError(_REPLAY_MISMATCH) from exc
    if _dump_json(_effective_payload(current)) != _dump_json(
        _effective_payload(member.row.effective_declaration)
    ):
        raise ValidationError(_REPLAY_MISMATCH)
    return applied


def _owned_replay_write(write: SpecificationWrite) -> SpecificationWrite:
    if isinstance(write, ParameterWrite):
        return ParameterWrite(
            step_name=write.step_name,
            parameter_name=write.parameter_name,
            value=_plain_json(write.value),
        )
    return deepcopy(write)


def _row_from_payload(
    payload: Mapping[str, Any],
    *,
    canonical_bytes: bytes,
) -> CanonicalSpecificationRow:
    _fields(
        payload,
        expected={
            "schema",
            "decision_coordinates",
            "workflow_selector",
            "writes",
            "effective_declaration",
        },
        label="specification row",
    )
    if payload["schema"] != ROW_SCHEMA:
        raise ValidationError(f"specification row schema must be {ROW_SCHEMA!r}")
    workflow_selector = _token(
        payload["workflow_selector"],
        label="workflow_selector",
    )
    coordinates = _parse_coordinates(payload["decision_coordinates"])
    writes = _parse_writes(payload["writes"])
    effective = _parse_effective_declaration(payload["effective_declaration"])
    _validate_write_alignment(writes, effective=effective)
    return CanonicalSpecificationRow(
        decision_coordinates=coordinates,
        workflow_selector=workflow_selector,
        writes=writes,
        effective_declaration=effective,
        canonical_bytes=canonical_bytes,
        row_digest=sha256_digest(_ROW_DOMAIN_PREFIX + canonical_bytes),
        _construction_token=_CONSTRUCTION_TOKEN,
    )


def _parse_coordinates(payload: object) -> tuple[DecisionCoordinate, ...]:
    values = _exact_list(payload, label="decision_coordinates")
    if not values:
        raise ValidationError("decision_coordinates cannot be empty")
    coordinates: list[DecisionCoordinate] = []
    previous_name: str | None = None
    for index, raw_coordinate in enumerate(values):
        label = f"decision_coordinates[{index}]"
        coordinate = _exact_mapping(raw_coordinate, label=label)
        _fields(coordinate, expected={"name", "value"}, label=label)
        name = _token(coordinate["name"], label=f"{label}.name")
        if previous_name is not None and name <= previous_name:
            raise ValidationError("decision_coordinates must be sorted by unique name")
        previous_name = name
        coordinates.append(
            DecisionCoordinate(
                name=name,
                value=_copy_json(coordinate["value"], label=f"{label}.value"),
            )
        )
    return tuple(coordinates)


def _parse_writes(payload: object) -> tuple[SpecificationWrite, ...]:
    values = _exact_list(payload, label="writes")
    if not values:
        raise ValidationError("writes cannot be empty")
    writes: list[SpecificationWrite] = []
    destinations: set[tuple[str, ...]] = set()
    for index, raw_write in enumerate(values):
        write = _parse_write(raw_write, label=f"writes[{index}]")
        destination = _write_destination(write)
        if destination in destinations:
            raise ValidationError(f"writes contains duplicate destination: {destination}")
        destinations.add(destination)
        writes.append(write)
    ordered = tuple(sorted(writes, key=_write_sort_key))
    if tuple(writes) != ordered:
        raise ValidationError("writes are not in canonical type/destination order")
    return ordered


def _parse_write(payload: object, *, label: str) -> SpecificationWrite:
    write = _exact_mapping(payload, label=label)
    write_type = write.get("type")
    if write_type == "parameter":
        _fields(
            write,
            expected={"type", "step_name", "parameter_name", "value"},
            label=label,
        )
        return ParameterWrite(
            step_name=_token(write["step_name"], label=f"{label}.step_name"),
            parameter_name=_token(
                write["parameter_name"],
                label=f"{label}.parameter_name",
            ),
            value=_copy_json(write["value"], label=f"{label}.value"),
        )
    if write_type == "execution_population":
        _fields(write, expected={"type", "manifest_name"}, label=label)
        return ExecutionPopulationWrite(
            manifest_name=_token(
                write["manifest_name"],
                label=f"{label}.manifest_name",
            )
        )
    if write_type == "manifest_binding":
        _fields(
            write,
            expected={"type", "step_name", "role", "manifest_name"},
            label=label,
        )
        return ManifestBindingWrite(
            step_name=_token(write["step_name"], label=f"{label}.step_name"),
            role=_token(write["role"], label=f"{label}.role"),
            manifest_name=_token(
                write["manifest_name"],
                label=f"{label}.manifest_name",
            ),
        )
    if write_type == "target":
        _fields(write, expected={"type", "step_name", "output_name"}, label=label)
        return TargetWrite(
            step_name=_token(write["step_name"], label=f"{label}.step_name"),
            output_name=_token(write["output_name"], label=f"{label}.output_name"),
        )
    if write_type == "result":
        _fields(
            write,
            expected={"type", "role", "step_name", "output_name"},
            label=label,
        )
        return ResultWrite(
            role=_token(write["role"], label=f"{label}.role"),
            step_name=_token(write["step_name"], label=f"{label}.step_name"),
            output_name=_token(write["output_name"], label=f"{label}.output_name"),
        )
    raise ValidationError(f"{label} contains unsupported write type: {write_type!r}")


def _parse_effective_declaration(payload: object) -> CanonicalEffectiveDeclaration:
    effective = _exact_mapping(payload, label="effective_declaration")
    _fields(
        effective,
        expected={"target", "steps", "execution_population", "results"},
        label="effective_declaration",
    )
    target = _exact_mapping(effective["target"], label="effective target")
    _fields(target, expected={"step_name", "output_name"}, label="effective target")
    target_step_name = _token(target["step_name"], label="effective target step_name")
    target_output_name = _token(
        target["output_name"],
        label="effective target output_name",
    )

    raw_steps = _exact_list(effective["steps"], label="effective steps")
    if not raw_steps:
        raise ValidationError("effective steps cannot be empty")
    steps = tuple(
        _parse_effective_step(raw_step, index=index)
        for index, raw_step in enumerate(raw_steps)
    )
    if len({step.step_name for step in steps}) != len(steps):
        raise ValidationError("effective steps contain duplicate step_name")

    execution_population = _parse_manifest_reference(
        effective["execution_population"],
        label="effective execution_population",
        allow_null=True,
    )
    results = _parse_effective_results(effective["results"])
    declaration = CanonicalEffectiveDeclaration(
        target_step_name=target_step_name,
        target_output_name=target_output_name,
        steps=steps,
        execution_population=execution_population,
        results=results,
    )
    _validate_effective_structure(declaration)
    return declaration


def _parse_effective_step(payload: object, *, index: int) -> CanonicalEffectiveStep:
    label = f"effective steps[{index}]"
    step = _exact_mapping(payload, label=label)
    _fields(
        step,
        expected={
            "step_name",
            "pattern_kind",
            "execution_role",
            "address_scope",
            "callable_ref",
            "step_contract_version",
            "inputs",
            "source_inputs",
            "params",
            "outputs",
            "manifest_binding",
        },
        label=label,
    )
    step_name = _token(step["step_name"], label=f"{label}.step_name")
    pattern_kind = _choice(
        step["pattern_kind"],
        allowed=PATTERN_KINDS,
        label=f"{label}.pattern_kind",
    )
    execution_role = _choice(
        step["execution_role"],
        allowed=EXECUTION_ROLES,
        label=f"{label}.execution_role",
    )
    address_scope = _choice(
        step["address_scope"],
        allowed=ADDRESS_SCOPES,
        label=f"{label}.address_scope",
    )
    callable_ref = _callable_ref(step["callable_ref"], label=f"{label}.callable_ref")
    step_contract_version = _nonempty_string(
        step["step_contract_version"],
        label=f"{label}.step_contract_version",
    )

    inputs = _parse_effective_inputs(step["inputs"], label=f"{label}.inputs")
    raw_source_inputs = _exact_list(
        step["source_inputs"],
        label=f"{label}.source_inputs",
    )
    source_inputs = tuple(
        _token(value, label=f"{label}.source_inputs[{item_index}]")
        for item_index, value in enumerate(raw_source_inputs)
    )
    if tuple(sorted(set(source_inputs))) != source_inputs:
        raise ValidationError(f"{label}.source_inputs must be sorted and unique")
    params_mapping = _exact_mapping(step["params"], label=f"{label}.params")
    params = _copy_json_mapping(params_mapping, label=f"{label}.params")
    outputs = _parse_effective_outputs(step["outputs"], label=f"{label}.outputs")
    manifest_binding = _parse_manifest_binding(
        step["manifest_binding"],
        label=f"{label}.manifest_binding",
    )
    return CanonicalEffectiveStep(
        step_name=step_name,
        pattern_kind=pattern_kind,
        execution_role=execution_role,
        address_scope=address_scope,
        callable_ref=callable_ref,
        step_contract_version=step_contract_version,
        inputs=inputs,
        source_inputs=source_inputs,
        params=params,
        outputs=outputs,
        manifest_binding=manifest_binding,
    )


def _parse_effective_inputs(payload: object, *, label: str) -> tuple[CanonicalStepInput, ...]:
    raw_inputs = _exact_list(payload, label=label)
    inputs: list[CanonicalStepInput] = []
    previous_name: str | None = None
    for index, raw_input in enumerate(raw_inputs):
        item_label = f"{label}[{index}]"
        item = _exact_mapping(raw_input, label=item_label)
        _fields(
            item,
            expected={
                "name",
                "dependency_role",
                "source_step_name",
                "source_output_name",
            },
            label=item_label,
        )
        name = _token(item["name"], label=f"{item_label}.name")
        if previous_name is not None and name <= previous_name:
            raise ValidationError(f"{label} must be sorted by unique name")
        previous_name = name
        inputs.append(
            CanonicalStepInput(
                name=name,
                dependency_role=_choice(
                    item["dependency_role"],
                    allowed=DEPENDENCY_ROLES,
                    label=f"{item_label}.dependency_role",
                ),
                source_step_name=_token(
                    item["source_step_name"],
                    label=f"{item_label}.source_step_name",
                ),
                source_output_name=_token(
                    item["source_output_name"],
                    label=f"{item_label}.source_output_name",
                ),
            )
        )
    return tuple(inputs)


def _parse_effective_outputs(
    payload: object,
    *,
    label: str,
) -> tuple[CanonicalStepOutput, ...]:
    raw_outputs = _exact_list(payload, label=label)
    outputs: list[CanonicalStepOutput] = []
    previous_name: str | None = None
    for index, raw_output in enumerate(raw_outputs):
        item_label = f"{label}[{index}]"
        item = _exact_mapping(raw_output, label=item_label)
        _fields(
            item,
            expected={"name", "extension", "address_scope"},
            label=item_label,
        )
        name = _token(item["name"], label=f"{item_label}.name")
        if previous_name is not None and name <= previous_name:
            raise ValidationError(f"{label} must be sorted by unique name")
        previous_name = name
        outputs.append(
            CanonicalStepOutput(
                name=name,
                extension=_nonempty_string(
                    item["extension"],
                    label=f"{item_label}.extension",
                ),
                address_scope=_choice(
                    item["address_scope"],
                    allowed=ADDRESS_SCOPES,
                    label=f"{item_label}.address_scope",
                ),
            )
        )
    return tuple(outputs)


def _parse_manifest_binding(
    payload: object,
    *,
    label: str,
) -> CanonicalManifestBinding | None:
    if payload is None:
        return None
    binding = _exact_mapping(payload, label=label)
    _fields(
        binding,
        expected={
            "manifest_usage_role",
            "manifest_name",
            "manifest_value_schema",
            "manifest_digest",
        },
        label=label,
    )
    return CanonicalManifestBinding(
        manifest_usage_role=_token(
            binding["manifest_usage_role"],
            label=f"{label}.manifest_usage_role",
        ),
        manifest_name=_token(binding["manifest_name"], label=f"{label}.manifest_name"),
        manifest_value_schema=_manifest_schema(
            binding["manifest_value_schema"],
            label=f"{label}.manifest_value_schema",
        ),
        manifest_digest=_digest(
            binding["manifest_digest"],
            label=f"{label}.manifest_digest",
        ),
    )


def _parse_manifest_reference(
    payload: object,
    *,
    label: str,
    allow_null: bool,
) -> CanonicalManifestReference | None:
    if payload is None:
        if allow_null:
            return None
        raise ValidationError(f"{label} cannot be null")
    reference = _exact_mapping(payload, label=label)
    _fields(
        reference,
        expected={"manifest_name", "manifest_value_schema", "manifest_digest"},
        label=label,
    )
    return CanonicalManifestReference(
        manifest_name=_token(
            reference["manifest_name"],
            label=f"{label}.manifest_name",
        ),
        manifest_value_schema=_manifest_schema(
            reference["manifest_value_schema"],
            label=f"{label}.manifest_value_schema",
        ),
        manifest_digest=_digest(
            reference["manifest_digest"],
            label=f"{label}.manifest_digest",
        ),
    )


def _parse_effective_results(payload: object) -> tuple[CanonicalEffectiveResult, ...]:
    raw_results = _exact_list(payload, label="effective results")
    if not raw_results:
        raise ValidationError("effective results cannot be empty")
    results: list[CanonicalEffectiveResult] = []
    previous_role: str | None = None
    ports: set[tuple[str, str]] = set()
    for index, raw_result in enumerate(raw_results):
        label = f"effective results[{index}]"
        item = _exact_mapping(raw_result, label=label)
        _fields(
            item,
            expected={"role", "step_name", "output_name", "address_scope"},
            label=label,
        )
        role = _token(item["role"], label=f"{label}.role")
        if previous_role is not None and role <= previous_role:
            raise ValidationError("effective results must be sorted by unique role")
        previous_role = role
        step_name = _token(item["step_name"], label=f"{label}.step_name")
        output_name = _token(item["output_name"], label=f"{label}.output_name")
        port = (step_name, output_name)
        if port in ports:
            raise ValidationError("effective results cannot map two roles to one port")
        ports.add(port)
        results.append(
            CanonicalEffectiveResult(
                role=role,
                step_name=step_name,
                output_name=output_name,
                address_scope=_choice(
                    item["address_scope"],
                    allowed=ADDRESS_SCOPES,
                    label=f"{label}.address_scope",
                ),
            )
        )
    return tuple(results)


def _validate_effective_structure(declaration: CanonicalEffectiveDeclaration) -> None:
    steps = {step.step_name: step for step in declaration.steps}
    positions = {step.step_name: index for index, step in enumerate(declaration.steps)}
    target_step = steps.get(declaration.target_step_name)
    if target_step is None:
        raise ValidationError("effective target step is absent from the closure")
    target_outputs = {output.name: output for output in target_step.outputs}
    target_output = target_outputs.get(declaration.target_output_name)
    if target_output is None:
        raise ValidationError("effective target output is absent from the target step")

    for step in declaration.steps:
        outputs = {output.name: output for output in step.outputs}
        if any(output.address_scope != step.address_scope for output in outputs.values()):
            raise ValidationError("effective step output address scope must match its step")
        for step_input in step.inputs:
            source = steps.get(step_input.source_step_name)
            if source is None:
                raise ValidationError("effective input references a step outside the closure")
            if positions[source.step_name] >= positions[step.step_name]:
                raise ValidationError("effective input source must precede its dependent step")
            if step_input.source_output_name not in {
                output.name for output in source.outputs
            }:
                raise ValidationError("effective input references an undeclared source output")

    required_steps = {declaration.target_step_name}
    pending = [declaration.target_step_name]
    while pending:
        step_name = pending.pop()
        for step_input in steps[step_name].inputs:
            source_name = step_input.source_step_name
            if source_name not in required_steps:
                required_steps.add(source_name)
                pending.append(source_name)
    if required_steps != set(steps):
        raise ValidationError(
            "effective steps must equal the target reverse dependency closure"
        )

    target_count = 0
    for result in declaration.results:
        if result.step_name != declaration.target_step_name:
            raise ValidationError("effective result ports must be siblings on the target step")
        output = target_outputs.get(result.output_name)
        if output is None:
            raise ValidationError("effective result references an undeclared output")
        if result.address_scope != output.address_scope:
            raise ValidationError("effective result address scope does not match its output")
        if result.address_scope != target_output.address_scope:
            raise ValidationError("effective result address scope does not match the target")
        if result.output_name == declaration.target_output_name:
            target_count += 1
    if target_count != 1:
        raise ValidationError("effective results must include the target exactly once")


def _validate_write_alignment(
    writes: tuple[SpecificationWrite, ...],
    *,
    effective: CanonicalEffectiveDeclaration,
) -> None:
    steps = {step.step_name: step for step in effective.steps}
    target_writes = [write for write in writes if isinstance(write, TargetWrite)]
    if len(target_writes) != 1 or (
        target_writes[0].step_name,
        target_writes[0].output_name,
    ) != (effective.target_step_name, effective.target_output_name):
        raise ValidationError("target write does not match the effective declaration")

    result_writes = tuple(
        (write.role, write.step_name, write.output_name)
        for write in writes
        if isinstance(write, ResultWrite)
    )
    effective_results = tuple(
        (result.role, result.step_name, result.output_name)
        for result in effective.results
    )
    if result_writes != effective_results:
        raise ValidationError("result writes do not match the effective declaration")

    for write in writes:
        if isinstance(write, ParameterWrite):
            step = steps.get(write.step_name)
            if step is None or write.parameter_name not in step.params:
                raise ValidationError("parameter write is absent from effective parameters")
            if not _json_equal(step.params[write.parameter_name], write.value):
                raise ValidationError("parameter write value does not match effective parameters")
        elif isinstance(write, ExecutionPopulationWrite):
            population = effective.execution_population
            if population is None or population.manifest_name != write.manifest_name:
                raise ValidationError(
                    "execution-population write does not match effective declaration"
                )
        elif isinstance(write, ManifestBindingWrite):
            step = steps.get(write.step_name)
            binding = None if step is None else step.manifest_binding
            if (
                binding is None
                or binding.manifest_usage_role != write.role
                or binding.manifest_name != write.manifest_name
            ):
                raise ValidationError(
                    "manifest-binding write does not match effective declaration"
                )


def _effective_from_adapter(
    declaration: ProvisionalEffectiveDeclaration,
    *,
    workflow_selector: str,
) -> CanonicalEffectiveDeclaration:
    if declaration.workflow_name != workflow_selector:
        raise ValidationError("adapter workflow does not match workflow_selector")
    step_bindings = tuple(
        step.manifest_binding
        for step in declaration.steps
        if step.manifest_binding is not None
    )
    if step_bindings != declaration.manifest_bindings:
        raise ValidationError(
            "adapter top-level manifest bindings do not match step-local bindings"
        )

    payload = {
        "target": {
            "step_name": declaration.target_step_name,
            "output_name": declaration.target_output_name,
        },
        "steps": [
            {
                "step_name": step.step_name,
                "pattern_kind": step.pattern_kind,
                "execution_role": step.execution_role,
                "address_scope": step.address_scope,
                "callable_ref": step.callable_ref,
                "step_contract_version": step.step_contract_version,
                "inputs": [
                    {
                        "name": name,
                        "dependency_role": step_input.dependency_role,
                        "source_step_name": step_input.source_step_name,
                        "source_output_name": step_input.source_output_name,
                    }
                    for name, step_input in sorted(step.inputs.items())
                    if _matching_name(name, step_input.name, label="step input")
                ],
                "source_inputs": sorted(step.source_inputs),
                "params": _copy_json_mapping(step.params, label="step parameters"),
                "outputs": [
                    {
                        "name": name,
                        "extension": output.extension,
                        "address_scope": output.address_scope,
                    }
                    for name, output in sorted(step.outputs.items())
                    if _matching_name(name, output.name, label="step output")
                ],
                "manifest_binding": (
                    None
                    if step.manifest_binding is None
                    else {
                        "manifest_usage_role": (
                            step.manifest_binding.manifest_usage_role
                        ),
                        "manifest_name": step.manifest_binding.manifest_name,
                        "manifest_value_schema": (
                            step.manifest_binding.manifest_value_schema
                        ),
                        "manifest_digest": step.manifest_binding.manifest_digest,
                    }
                ),
            }
            for step in declaration.steps
        ],
        "execution_population": (
            None
            if declaration.execution_population is None
            else {
                "manifest_name": declaration.execution_population.manifest_name,
                "manifest_value_schema": (
                    declaration.execution_population.manifest_value_schema
                ),
                "manifest_digest": declaration.execution_population.manifest_digest,
            }
        ),
        "results": [
            {
                "role": result.role,
                "step_name": result.step_name,
                "output_name": result.output_name,
                "address_scope": result.address_scope,
            }
            for result in sorted(declaration.results, key=lambda result: result.role)
        ],
    }
    return _parse_effective_declaration(payload)


def _matching_name(mapping_name: str, value_name: str, *, label: str) -> bool:
    if mapping_name != value_name:
        raise ValidationError(f"{label} mapping key does not match its declared name")
    return True


def _effective_payload(declaration: CanonicalEffectiveDeclaration) -> dict[str, Any]:
    return {
        "target": {
            "step_name": declaration.target_step_name,
            "output_name": declaration.target_output_name,
        },
        "steps": [
            {
                "step_name": step.step_name,
                "pattern_kind": step.pattern_kind,
                "execution_role": step.execution_role,
                "address_scope": step.address_scope,
                "callable_ref": step.callable_ref,
                "step_contract_version": step.step_contract_version,
                "inputs": [
                    {
                        "name": step_input.name,
                        "dependency_role": step_input.dependency_role,
                        "source_step_name": step_input.source_step_name,
                        "source_output_name": step_input.source_output_name,
                    }
                    for step_input in step.inputs
                ],
                "source_inputs": list(step.source_inputs),
                "params": deepcopy(step.params),
                "outputs": [
                    {
                        "name": output.name,
                        "extension": output.extension,
                        "address_scope": output.address_scope,
                    }
                    for output in step.outputs
                ],
                "manifest_binding": (
                    None
                    if step.manifest_binding is None
                    else {
                        "manifest_usage_role": step.manifest_binding.manifest_usage_role,
                        "manifest_name": step.manifest_binding.manifest_name,
                        "manifest_value_schema": (
                            step.manifest_binding.manifest_value_schema
                        ),
                        "manifest_digest": step.manifest_binding.manifest_digest,
                    }
                ),
            }
            for step in declaration.steps
        ],
        "execution_population": (
            None
            if declaration.execution_population is None
            else {
                "manifest_name": declaration.execution_population.manifest_name,
                "manifest_value_schema": (
                    declaration.execution_population.manifest_value_schema
                ),
                "manifest_digest": declaration.execution_population.manifest_digest,
            }
        ),
        "results": [
            {
                "role": result.role,
                "step_name": result.step_name,
                "output_name": result.output_name,
                "address_scope": result.address_scope,
            }
            for result in declaration.results
        ],
    }


def _row_payload(row: CanonicalSpecificationRow) -> dict[str, Any]:
    return {
        "schema": ROW_SCHEMA,
        "decision_coordinates": [
            {"name": coordinate.name, "value": deepcopy(coordinate.value)}
            for coordinate in row.decision_coordinates
        ],
        "workflow_selector": row.workflow_selector,
        "writes": [_write_payload(write) for write in row.writes],
        "effective_declaration": _effective_payload(row.effective_declaration),
    }


def _write_payload(write: SpecificationWrite) -> dict[str, Any]:
    if isinstance(write, ParameterWrite):
        return {
            "type": "parameter",
            "step_name": write.step_name,
            "parameter_name": write.parameter_name,
            "value": _copy_json(write.value, label="parameter write value"),
        }
    if isinstance(write, ExecutionPopulationWrite):
        return {"type": "execution_population", "manifest_name": write.manifest_name}
    if isinstance(write, ManifestBindingWrite):
        return {
            "type": "manifest_binding",
            "step_name": write.step_name,
            "role": write.role,
            "manifest_name": write.manifest_name,
        }
    if isinstance(write, TargetWrite):
        return {
            "type": "target",
            "step_name": write.step_name,
            "output_name": write.output_name,
        }
    if isinstance(write, ResultWrite):
        return {
            "type": "result",
            "role": write.role,
            "step_name": write.step_name,
            "output_name": write.output_name,
        }
    raise ValidationError(f"unsupported specification write: {type(write).__name__}")


def _write_destination(write: SpecificationWrite) -> tuple[str, ...]:
    if isinstance(write, ParameterWrite):
        return ("parameter", write.step_name, write.parameter_name)
    if isinstance(write, ExecutionPopulationWrite):
        return ("execution_population",)
    if isinstance(write, ManifestBindingWrite):
        return ("manifest_binding", write.step_name, write.role)
    if isinstance(write, TargetWrite):
        return ("target",)
    if isinstance(write, ResultWrite):
        return ("result", write.role)
    raise ValidationError(f"unsupported specification write: {type(write).__name__}")


def _write_sort_key(write: SpecificationWrite) -> tuple[Any, ...]:
    destination = _write_destination(write)
    return (_WRITE_TYPE_ORDER[destination[0]], *destination[1:])


def _manifest_references(
    declaration: CanonicalEffectiveDeclaration,
) -> tuple[CanonicalManifestReference, ...]:
    references: list[CanonicalManifestReference] = []
    if declaration.execution_population is not None:
        references.append(declaration.execution_population)
    for step in declaration.steps:
        binding = step.manifest_binding
        if binding is not None:
            references.append(
                CanonicalManifestReference(
                    manifest_name=binding.manifest_name,
                    manifest_value_schema=binding.manifest_value_schema,
                    manifest_digest=binding.manifest_digest,
                )
            )
    return tuple(references)


def _manifest_values_for_rows(
    *,
    loaded: LoadedWorkflowProject,
    rows: tuple[CanonicalSpecificationRow, ...],
) -> tuple[ManifestValue, ...]:
    values: dict[tuple[str, str], ManifestValue] = {}
    for row in rows:
        for reference in _manifest_references(row.effective_declaration):
            try:
                manifest = loaded.manifests[reference.manifest_name]
            except KeyError as exc:
                raise ValidationError(
                    "effective declaration references unknown manifest: "
                    f"{reference.manifest_name}"
                ) from exc
            value = manifest.value
            identity = (value.value_schema, value.manifest_digest)
            if identity != (
                reference.manifest_value_schema,
                reference.manifest_digest,
            ):
                raise ValidationError(
                    "effective manifest reference does not match its current value"
                )
            previous = values.get(identity)
            if previous is not None and previous != value:
                raise ValidationError("one manifest identity has unequal canonical bodies")
            values[identity] = value
    return tuple(values[key] for key in sorted(values))


def _parse_manifest_values(payload: object) -> tuple[ManifestValue, ...]:
    raw_values = _exact_list(payload, label="snapshot manifest_values")
    values: list[ManifestValue] = []
    previous_identity: tuple[str, str] | None = None
    for index, raw_value in enumerate(raw_values):
        label = f"snapshot manifest_values[{index}]"
        item = _exact_mapping(raw_value, label=label)
        _fields(
            item,
            expected={"value_schema", "manifest_digest", "canonical_body"},
            label=label,
        )
        value_schema = _manifest_schema(item["value_schema"], label=f"{label}.value_schema")
        manifest_digest = _digest(
            item["manifest_digest"],
            label=f"{label}.manifest_digest",
        )
        canonical_body = item["canonical_body"]
        if type(canonical_body) is not str:
            raise ValidationError(f"{label}.canonical_body must be a string")
        value = ManifestValue(
            value_schema=value_schema,
            manifest_digest=manifest_digest,
            canonical_body=canonical_body,
        )
        identity = (value.value_schema, value.manifest_digest)
        if previous_identity is not None and identity <= previous_identity:
            raise ValidationError("snapshot manifest_values must be sorted and unique")
        previous_identity = identity
        values.append(value)
    return tuple(values)


def _derive_expected_results(
    declaration: CanonicalEffectiveDeclaration,
    *,
    manifests_by_identity: Mapping[tuple[str, str], ManifestValue],
) -> tuple[ExpectedResultDescriptor, ...]:
    descriptors: list[ExpectedResultDescriptor] = []
    for result in declaration.results:
        if result.address_scope == "cohort":
            addresses = ("cohort",)
        else:
            population = declaration.execution_population
            if population is None:
                raise ValidationError(
                    "entity result requires an effective execution_population"
                )
            identity = (
                population.manifest_value_schema,
                population.manifest_digest,
            )
            try:
                addresses = manifests_by_identity[identity].entity_ids
            except KeyError as exc:
                raise ValidationError(
                    "entity result references a missing execution-population value"
                ) from exc
        descriptors.extend(
            ExpectedResultDescriptor(
                role=result.role,
                step_name=result.step_name,
                output_name=result.output_name,
                address=address,
            )
            for address in addresses
        )
    ordered = tuple(
        sorted(
            descriptors,
            key=lambda item: (
                item.role,
                item.address,
                item.step_name,
                item.output_name,
            ),
        )
    )
    natural_keys = {(item.role, item.address) for item in ordered}
    if len(natural_keys) != len(ordered):
        raise ValidationError("expected results contain duplicate role/address")
    return ordered


def _load_canonical_json(data: bytes, *, label: str) -> object:
    if type(data) is not bytes:
        raise ValidationError(f"{label} data must be bytes")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"{label} must be valid UTF-8") from exc

    def reject_constant(value: str) -> None:
        raise ValidationError(f"{label} contains non-finite number: {value}")

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValidationError(f"{label} contains non-finite number: {value}")
        return parsed

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"{label} contains duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        payload = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=finite_float,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValidationError(f"{label} is not valid JSON") from exc
    if _dump_json(payload) != data:
        raise ValidationError(f"{label} bytes are not canonical JSON")
    return payload


def _dump_json(payload: object) -> bytes:
    try:
        return json.dumps(
            _plain_json(payload),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, UnicodeEncodeError, ValueError) as exc:
        raise ValidationError("canonical payload contains an unsupported JSON value") from exc


def _copy_json(value: object, *, label: str) -> Any:
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError(f"{label} contains a non-finite float")
        return value
    if type(value) in {list, _FrozenList}:
        return _FrozenList(
            [_copy_json(item, label=label) for item in value]
        )
    if type(value) in {dict, _FrozenDict}:
        return _copy_json_mapping(value, label=label)
    raise ValidationError(f"{label} contains unsupported type: {type(value).__name__}")


def _copy_json_mapping(value: Mapping[str, object], *, label: str) -> Mapping[str, Any]:
    if any(type(key) is not str for key in value):
        raise ValidationError(f"{label} must contain only string keys")
    return _FrozenDict(
        {
            key: _copy_json(value[key], label=f"{label}.{key}")
            for key in sorted(value)
        }
    )


def _plain_json(value: object) -> object:
    if isinstance(value, _FrozenDict):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, _FrozenList):
        return [_plain_json(item) for item in value]
    if type(value) is dict:
        return {key: _plain_json(item) for key, item in value.items()}
    if type(value) is list:
        return [_plain_json(item) for item in value]
    return value


def _typed_json_equal(left: object, right: object) -> bool:
    left_is_mapping = type(left) in {dict, _FrozenDict}
    right_is_mapping = type(right) in {dict, _FrozenDict}
    if left_is_mapping or right_is_mapping:
        if not left_is_mapping or not right_is_mapping:
            return False
        left_mapping = left
        right_mapping = right
        assert isinstance(left_mapping, Mapping)
        assert isinstance(right_mapping, Mapping)
        return set(left_mapping) == set(right_mapping) and all(
            _typed_json_equal(left_mapping[key], right_mapping[key])
            for key in left_mapping
        )

    left_is_sequence = type(left) in {list, _FrozenList}
    right_is_sequence = type(right) in {list, _FrozenList}
    if left_is_sequence or right_is_sequence:
        if not left_is_sequence or not right_is_sequence:
            return False
        left_sequence = left
        right_sequence = right
        assert isinstance(left_sequence, Sequence)
        assert isinstance(right_sequence, Sequence)
        return len(left_sequence) == len(right_sequence) and all(
            _typed_json_equal(left_item, right_item)
            for left_item, right_item in zip(left_sequence, right_sequence, strict=True)
        )

    if type(left) is not type(right):
        return False
    if type(left) is float and left == 0.0:
        return math.copysign(1.0, left) == math.copysign(1.0, right)
    return left == right


def _json_equal(left: object, right: object) -> bool:
    return _dump_json(_copy_json(left, label="JSON value")) == _dump_json(
        _copy_json(right, label="JSON value")
    )


def _fields(mapping: Mapping[str, object], *, expected: set[str], label: str) -> None:
    unknown = sorted(set(mapping) - expected)
    if unknown:
        raise ValidationError(f"{label} contains unknown field: {unknown[0]}")
    missing = sorted(expected - set(mapping))
    if missing:
        raise ValidationError(f"{label} is missing required field: {missing[0]}")


def _exact_mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if type(value) not in {dict, _FrozenDict}:
        raise ValidationError(f"{label} must be an object")
    if any(type(key) is not str for key in value):
        raise ValidationError(f"{label} must contain only string keys")
    return value


def _exact_list(value: object, *, label: str) -> Sequence[Any]:
    if type(value) not in {list, _FrozenList}:
        raise ValidationError(f"{label} must be an array")
    return value


def _token(value: object, *, label: str) -> str:
    return validate_path_token(value, label=label)


def _nonempty_string(value: object, *, label: str) -> str:
    if type(value) is not str or not value:
        raise ValidationError(f"{label} must be a non-empty string")
    return value


def _trimmed_string(value: object, *, label: str) -> str:
    parsed = _nonempty_string(value, label=label)
    if parsed != parsed.strip():
        raise ValidationError(f"{label} must be trimmed")
    return parsed


def _choice(value: object, *, allowed: frozenset[str], label: str) -> str:
    if type(value) is not str or value not in allowed:
        raise ValidationError(f"{label} contains unsupported value: {value!r}")
    return value


def _callable_ref(value: object, *, label: str) -> str:
    parsed = _nonempty_string(value, label=label)
    if parsed.count(":") != 1 or any(not part for part in parsed.split(":")):
        raise ValidationError(f"{label} must use module:function syntax")
    return parsed


def _manifest_schema(value: object, *, label: str) -> str:
    if value != MANIFEST_VALUE_SCHEMA:
        raise ValidationError(f"{label} must be {MANIFEST_VALUE_SCHEMA!r}")
    return value


def _digest(value: object, *, label: str) -> str:
    if not is_valid_digest(value):
        raise ValidationError(f"{label} must be a full lowercase SHA-256 digest")
    return value
