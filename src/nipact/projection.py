"""Canonical requested-computation projection contracts."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeAlias

from .errors import ValidationError
from .hashing import is_valid_digest, sha256_digest
from .manifest import MANIFEST_VALUE_SCHEMA
from .source_authority import LogicalSourceCoordinate

IDENTITY_CONTRACT_VERSION = 3
OUTPUT_CONTRACT_VERSION = 1
RUNNER_CONTRACT_VERSION = "2"

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True)
class StepContract:
    step_contract_id: str
    step_contract_version: str
    callable_ref: str
    runner_contract_version: str


@dataclass(frozen=True)
class RegisteredSourceSnapshot:
    content_digest: str
    file_size: int
    declared_extension: str


@dataclass(frozen=True)
class RegisteredSourceBinding:
    role: str
    source_coordinate: LogicalSourceCoordinate
    registered_content_digest: str
    registered_file_size: int
    declared_extension: str


@dataclass(frozen=True)
class UpstreamRequestedOutputBinding:
    role: str
    upstream_request_bundle_digest: str
    output_name: str


@dataclass(frozen=True)
class CollectionBinding:
    role: str
    collection_semantics: str
    manifest_digest: str | None
    members: tuple[UpstreamRequestedOutputBinding, ...]
    manifest_value_schema: str | None = None


ProjectionBinding: TypeAlias = (
    RegisteredSourceBinding | UpstreamRequestedOutputBinding | CollectionBinding
)


@dataclass(frozen=True)
class SiblingOutput:
    output_name: str
    declared_extension: str


@dataclass(frozen=True)
class OutputContract:
    output_contract_version: int
    sibling_outputs: tuple[SiblingOutput, ...]


@dataclass(frozen=True)
class RequestBundleProjectionV3:
    identity_contract_version: int
    namespace: str
    step_contract: StepContract
    address: str
    canonical_parameters: JsonValue
    role_labelled_bindings: tuple[ProjectionBinding, ...]
    result_affecting_settings: dict[str, JsonValue]
    determinism_contract: str
    output_contract: OutputContract


@dataclass(frozen=True)
class ResolvedRequestBundleProjectionV3:
    identity_contract_version: int
    canonical_json: str
    request_bundle_digest: str


@dataclass(frozen=True)
class ValidatedStoredRequestBundleProjectionV3:
    resolved_projection: ResolvedRequestBundleProjectionV3
    direct_upstream_request_bundle_digests: tuple[str, ...]


@dataclass(frozen=True)
class RequestedOutputCoordinate:
    namespace: str
    step_name: str
    output_name: str
    address: str


@dataclass(frozen=True)
class SourceBindingPlan:
    role: str
    source_coordinate: LogicalSourceCoordinate


@dataclass(frozen=True)
class UpstreamRequestedOutputBindingPlan:
    role: str
    requested_output: RequestedOutputCoordinate


@dataclass(frozen=True)
class CollectionBindingPlan:
    role: str
    collection_semantics: str
    manifest_digest: str | None
    members: tuple[RequestedOutputCoordinate, ...]
    manifest_value_schema: str | None = None


ProjectionBindingPlan: TypeAlias = (
    SourceBindingPlan
    | UpstreamRequestedOutputBindingPlan
    | CollectionBindingPlan
)


@dataclass(frozen=True)
class RequestBundleProjectionPlanV3:
    """Planning recipe for a projection whose source snapshots may be absent."""

    identity_contract_version: int
    namespace: str
    step_contract: StepContract
    address: str
    canonical_parameters: JsonValue
    role_labelled_binding_plans: tuple[ProjectionBindingPlan, ...]
    result_affecting_settings: dict[str, JsonValue]
    determinism_contract: str
    output_contract: OutputContract


@dataclass(frozen=True)
class UnresolvedRequestBundleProjection:
    missing_source_coordinates: tuple[LogicalSourceCoordinate, ...]


RequestBundleProjectionState: TypeAlias = (
    ResolvedRequestBundleProjectionV3 | UnresolvedRequestBundleProjection
)


def resolve_request_bundle_projection_plan(
    plan: RequestBundleProjectionPlanV3,
    *,
    source_snapshots: Mapping[LogicalSourceCoordinate, RegisteredSourceSnapshot],
    upstream_states: Mapping[
        RequestedOutputCoordinate,
        RequestBundleProjectionState,
    ],
) -> RequestBundleProjectionState:
    """Resolve one planning recipe without reading the registry or filesystem."""
    if not isinstance(plan, RequestBundleProjectionPlanV3):
        raise ValidationError("plan must be a RequestBundleProjectionPlanV3")

    resolved_bindings: list[ProjectionBinding] = []
    missing_sources: set[LogicalSourceCoordinate] = set()
    for binding_plan in plan.role_labelled_binding_plans:
        if isinstance(binding_plan, SourceBindingPlan):
            snapshot = source_snapshots.get(binding_plan.source_coordinate)
            if snapshot is None:
                missing_sources.add(binding_plan.source_coordinate)
                continue
            resolved_bindings.append(
                RegisteredSourceBinding(
                    role=binding_plan.role,
                    source_coordinate=binding_plan.source_coordinate,
                    registered_content_digest=snapshot.content_digest,
                    registered_file_size=snapshot.file_size,
                    declared_extension=snapshot.declared_extension,
                )
            )
            continue

        if isinstance(binding_plan, UpstreamRequestedOutputBindingPlan):
            upstream_state = _required_upstream_state(
                binding_plan.requested_output,
                upstream_states=upstream_states,
            )
            if isinstance(upstream_state, UnresolvedRequestBundleProjection):
                missing_sources.update(upstream_state.missing_source_coordinates)
                continue
            resolved_bindings.append(
                UpstreamRequestedOutputBinding(
                    role=binding_plan.role,
                    upstream_request_bundle_digest=(
                        upstream_state.request_bundle_digest
                    ),
                    output_name=binding_plan.requested_output.output_name,
                )
            )
            continue

        if isinstance(binding_plan, CollectionBindingPlan):
            resolved_members: list[UpstreamRequestedOutputBinding] = []
            for requested_output in binding_plan.members:
                upstream_state = _required_upstream_state(
                    requested_output,
                    upstream_states=upstream_states,
                )
                if isinstance(upstream_state, UnresolvedRequestBundleProjection):
                    missing_sources.update(upstream_state.missing_source_coordinates)
                    continue
                resolved_members.append(
                    UpstreamRequestedOutputBinding(
                        role=binding_plan.role,
                        upstream_request_bundle_digest=(
                            upstream_state.request_bundle_digest
                        ),
                        output_name=requested_output.output_name,
                    )
                )
            if not missing_sources:
                resolved_bindings.append(
                    CollectionBinding(
                        role=binding_plan.role,
                        collection_semantics=binding_plan.collection_semantics,
                        manifest_value_schema=binding_plan.manifest_value_schema,
                        manifest_digest=binding_plan.manifest_digest,
                        members=tuple(resolved_members),
                    )
                )
            continue

        raise ValidationError("projection plan contains an unsupported binding")

    if missing_sources:
        return UnresolvedRequestBundleProjection(
            missing_source_coordinates=tuple(
                sorted(missing_sources, key=_source_coordinate_sort_key)
            )
        )

    projection = RequestBundleProjectionV3(
        identity_contract_version=plan.identity_contract_version,
        namespace=plan.namespace,
        step_contract=plan.step_contract,
        address=plan.address,
        canonical_parameters=plan.canonical_parameters,
        role_labelled_bindings=tuple(resolved_bindings),
        result_affecting_settings=plan.result_affecting_settings,
        determinism_contract=plan.determinism_contract,
        output_contract=plan.output_contract,
    )
    return canonicalize_request_bundle_projection(projection)


def _required_upstream_state(
    requested_output: RequestedOutputCoordinate,
    *,
    upstream_states: Mapping[
        RequestedOutputCoordinate,
        RequestBundleProjectionState,
    ],
) -> RequestBundleProjectionState:
    if not isinstance(requested_output, RequestedOutputCoordinate):
        raise ValidationError(
            "projection plan requested output must be a "
            "RequestedOutputCoordinate"
        )
    try:
        return upstream_states[requested_output]
    except KeyError as exc:
        raise ValidationError(
            "projection plan references an unavailable upstream requested output: "
            f"{requested_output.step_name}.{requested_output.output_name}"
            f"[{requested_output.address}]"
        ) from exc


def _source_coordinate_sort_key(
    coordinate: LogicalSourceCoordinate,
) -> tuple[str, str, str, str]:
    return (
        coordinate.context,
        coordinate.scope,
        coordinate.source_name,
        coordinate.entity_id or "",
    )


def canonicalize_request_bundle_projection(
    projection: RequestBundleProjectionV3,
) -> ResolvedRequestBundleProjectionV3:
    """Validate, serialize, and identify one V3 request projection."""
    if not isinstance(projection, RequestBundleProjectionV3):
        raise ValidationError("projection must be a RequestBundleProjectionV3")
    payload = _projection_payload(projection, path="projection")
    canonical_json = _dump_json(payload)
    return ResolvedRequestBundleProjectionV3(
        identity_contract_version=projection.identity_contract_version,
        canonical_json=canonical_json,
        request_bundle_digest=sha256_digest(canonical_json.encode("utf-8")),
    )


def validate_stored_request_bundle_projection_v3(
    *,
    request_bundle_digest: str,
    projection_json: str,
) -> ValidatedStoredRequestBundleProjectionV3:
    """Validate one stored V3 payload against its canonical bytes and digest."""
    digest = _require_digest(
        request_bundle_digest,
        path="request_bundle_digest",
    )
    if type(projection_json) is not str:
        raise ValidationError("stored request projection must be a JSON string")
    try:
        payload = json.loads(projection_json, object_pairs_hook=_unique_json_object)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValidationError("stored request projection is malformed JSON") from exc
    projection = _projection_from_payload(payload, path="projection")
    resolved = canonicalize_request_bundle_projection(projection)
    if projection_json != resolved.canonical_json:
        raise ValidationError("stored request projection is not canonical JSON")
    if digest != resolved.request_bundle_digest:
        raise ValidationError("stored request projection digest does not match payload")
    upstream_digests = sorted(
        {
            upstream_digest
            for binding in projection.role_labelled_bindings
            for upstream_digest in _binding_upstream_digests(binding)
        }
    )
    return ValidatedStoredRequestBundleProjectionV3(
        resolved_projection=resolved,
        direct_upstream_request_bundle_digests=tuple(upstream_digests),
    )


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ValidationError(
                f"stored request projection contains duplicate key {key!r}"
            )
        payload[key] = value
    return payload


def _projection_from_payload(payload: Any, *, path: str) -> RequestBundleProjectionV3:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={
            "identity_contract_version",
            "namespace",
            "step_contract",
            "address",
            "canonical_parameters",
            "role_labelled_bindings",
            "result_affecting_settings",
            "determinism_contract",
            "output_contract",
        },
    )
    bindings_payload = _require_list(
        obj["role_labelled_bindings"],
        path=f"{path}.role_labelled_bindings",
    )
    settings = obj["result_affecting_settings"]
    if type(settings) is not dict:
        raise ValidationError(f"{path}.result_affecting_settings must be an object")
    return RequestBundleProjectionV3(
        identity_contract_version=_require_int(
            obj["identity_contract_version"],
            path=f"{path}.identity_contract_version",
        ),
        namespace=_require_string(obj["namespace"], path=f"{path}.namespace"),
        step_contract=_step_contract_from_payload(
            obj["step_contract"], path=f"{path}.step_contract"
        ),
        address=_require_string(obj["address"], path=f"{path}.address"),
        canonical_parameters=_json_value(
            obj["canonical_parameters"], path=f"{path}.canonical_parameters"
        ),
        role_labelled_bindings=tuple(
            _binding_from_payload(binding, path=f"{path}.role_labelled_bindings[{index}]")
            for index, binding in enumerate(bindings_payload)
        ),
        result_affecting_settings=_json_value(
            settings, path=f"{path}.result_affecting_settings"
        ),
        determinism_contract=_require_string(
            obj["determinism_contract"], path=f"{path}.determinism_contract"
        ),
        output_contract=_output_contract_from_payload(
            obj["output_contract"], path=f"{path}.output_contract"
        ),
    )


def _step_contract_from_payload(payload: Any, *, path: str) -> StepContract:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={
            "step_contract_id",
            "step_contract_version",
            "callable_ref",
            "runner_contract_version",
        },
    )
    return StepContract(
        step_contract_id=_require_string(
            obj["step_contract_id"], path=f"{path}.step_contract_id"
        ),
        step_contract_version=_require_string(
            obj["step_contract_version"], path=f"{path}.step_contract_version"
        ),
        callable_ref=_require_string(obj["callable_ref"], path=f"{path}.callable_ref"),
        runner_contract_version=_require_string(
            obj["runner_contract_version"], path=f"{path}.runner_contract_version"
        ),
    )


def _binding_from_payload(payload: Any, *, path: str) -> ProjectionBinding:
    if type(payload) is not dict:
        raise ValidationError(f"{path} must be an object")
    if "source_coordinate" in payload:
        return _registered_source_from_payload(payload, path=path)
    if "upstream_request_bundle_digest" in payload:
        return _upstream_output_from_payload(payload, path=path)
    if "collection_semantics" in payload:
        return _collection_from_payload(payload, path=path)
    raise ValidationError(f"{path} has no recognized binding form")


def _registered_source_from_payload(
    payload: Any, *, path: str
) -> RegisteredSourceBinding:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={
            "role",
            "source_coordinate",
            "registered_content_digest",
            "registered_file_size",
            "declared_extension",
        },
    )
    coordinate = _require_object_shape(
        obj["source_coordinate"],
        path=f"{path}.source_coordinate",
        keys={"context", "scope", "source_name", "entity_id"},
    )
    entity_id_value = coordinate["entity_id"]
    if entity_id_value is not None:
        entity_id_value = _require_string(
            entity_id_value,
            path=f"{path}.source_coordinate.entity_id",
        )
    return RegisteredSourceBinding(
        role=_require_string(obj["role"], path=f"{path}.role"),
        source_coordinate=LogicalSourceCoordinate(
            context=_require_string(
                coordinate["context"], path=f"{path}.source_coordinate.context"
            ),
            scope=_require_string(
                coordinate["scope"], path=f"{path}.source_coordinate.scope"
            ),
            source_name=_require_string(
                coordinate["source_name"],
                path=f"{path}.source_coordinate.source_name",
            ),
            entity_id=entity_id_value,
        ),
        registered_content_digest=_require_digest(
            obj["registered_content_digest"],
            path=f"{path}.registered_content_digest",
        ),
        registered_file_size=_require_int(
            obj["registered_file_size"], path=f"{path}.registered_file_size"
        ),
        declared_extension=_require_string(
            obj["declared_extension"], path=f"{path}.declared_extension"
        ),
    )


def _upstream_output_from_payload(
    payload: Any, *, path: str
) -> UpstreamRequestedOutputBinding:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={"role", "upstream_request_bundle_digest", "output_name"},
    )
    return UpstreamRequestedOutputBinding(
        role=_require_string(obj["role"], path=f"{path}.role"),
        upstream_request_bundle_digest=_require_digest(
            obj["upstream_request_bundle_digest"],
            path=f"{path}.upstream_request_bundle_digest",
        ),
        output_name=_require_string(
            obj["output_name"], path=f"{path}.output_name"
        ),
    )


def _collection_from_payload(payload: Any, *, path: str) -> CollectionBinding:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={
            "role",
            "collection_semantics",
            "manifest_value_schema",
            "manifest_digest",
            "members",
        },
    )
    members = _require_list(obj["members"], path=f"{path}.members")
    manifest_value_schema, manifest_digest = _manifest_value_reference(
        value_schema=obj["manifest_value_schema"],
        manifest_digest=obj["manifest_digest"],
        path=path,
    )
    return CollectionBinding(
        role=_require_string(obj["role"], path=f"{path}.role"),
        collection_semantics=_require_string(
            obj["collection_semantics"], path=f"{path}.collection_semantics"
        ),
        manifest_value_schema=manifest_value_schema,
        manifest_digest=manifest_digest,
        members=tuple(
            _upstream_output_from_payload(member, path=f"{path}.members[{index}]")
            for index, member in enumerate(members)
        ),
    )


def _output_contract_from_payload(payload: Any, *, path: str) -> OutputContract:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={"output_contract_version", "sibling_outputs"},
    )
    siblings = _require_list(
        obj["sibling_outputs"], path=f"{path}.sibling_outputs"
    )
    return OutputContract(
        output_contract_version=_require_int(
            obj["output_contract_version"], path=f"{path}.output_contract_version"
        ),
        sibling_outputs=tuple(
            _sibling_output_from_payload(
                sibling, path=f"{path}.sibling_outputs[{index}]"
            )
            for index, sibling in enumerate(siblings)
        ),
    )


def _sibling_output_from_payload(payload: Any, *, path: str) -> SiblingOutput:
    obj = _require_object_shape(
        payload,
        path=path,
        keys={"output_name", "declared_extension"},
    )
    return SiblingOutput(
        output_name=_require_string(obj["output_name"], path=f"{path}.output_name"),
        declared_extension=_require_string(
            obj["declared_extension"], path=f"{path}.declared_extension"
        ),
    )


def _binding_upstream_digests(binding: ProjectionBinding) -> tuple[str, ...]:
    if isinstance(binding, UpstreamRequestedOutputBinding):
        return (binding.upstream_request_bundle_digest,)
    if isinstance(binding, CollectionBinding):
        return tuple(member.upstream_request_bundle_digest for member in binding.members)
    return ()


def _require_object_shape(
    value: Any, *, path: str, keys: set[str]
) -> dict[str, Any]:
    if type(value) is not dict:
        raise ValidationError(f"{path} must be an object")
    actual = set(value)
    if actual != keys:
        missing = sorted(keys - actual)
        unknown = sorted(actual - keys)
        details: list[str] = []
        if missing:
            details.append(f"missing keys {missing}")
        if unknown:
            details.append(f"unknown keys {unknown}")
        raise ValidationError(f"{path} has invalid shape: {', '.join(details)}")
    return value


def _require_list(value: Any, *, path: str) -> list[Any]:
    if type(value) is not list:
        raise ValidationError(f"{path} must be an array")
    return value


def _projection_payload(
    projection: RequestBundleProjectionV3,
    *,
    path: str,
) -> dict[str, Any]:
    identity_contract_version = _require_int(
        projection.identity_contract_version,
        path=f"{path}.identity_contract_version",
    )
    if identity_contract_version != IDENTITY_CONTRACT_VERSION:
        raise ValidationError(
            f"{path}.identity_contract_version must be "
            f"{IDENTITY_CONTRACT_VERSION}"
        )
    if type(projection.result_affecting_settings) is not dict:
        raise ValidationError(f"{path}.result_affecting_settings must be an object")
    _require_tuple(
        projection.role_labelled_bindings,
        path=f"{path}.role_labelled_bindings",
    )

    bindings = [
        _binding_payload(binding, path=f"{path}.role_labelled_bindings[{index}]")
        for index, binding in enumerate(projection.role_labelled_bindings)
    ]
    binding_roles = [binding["role"] for binding in bindings]
    if len(binding_roles) != len(set(binding_roles)):
        raise ValidationError(f"{path}.role_labelled_bindings contains duplicate roles")
    bindings.sort(key=_binding_sort_key)

    return {
        "identity_contract_version": identity_contract_version,
        "namespace": _require_string(
            projection.namespace,
            path=f"{path}.namespace",
        ),
        "step_contract": _step_contract_payload(
            projection.step_contract,
            path=f"{path}.step_contract",
        ),
        "address": _require_string(projection.address, path=f"{path}.address"),
        "canonical_parameters": _json_value(
            projection.canonical_parameters,
            path="canonical_parameters",
        ),
        "role_labelled_bindings": bindings,
        "result_affecting_settings": _json_value(
            projection.result_affecting_settings,
            path="result_affecting_settings",
        ),
        "determinism_contract": _require_string(
            projection.determinism_contract,
            path=f"{path}.determinism_contract",
        ),
        "output_contract": _output_contract_payload(
            projection.output_contract,
            path=f"{path}.output_contract",
        ),
    }


def _step_contract_payload(contract: StepContract, *, path: str) -> dict[str, Any]:
    if not isinstance(contract, StepContract):
        raise ValidationError(f"{path} must be a StepContract")
    return {
        "step_contract_id": _require_string(
            contract.step_contract_id,
            path=f"{path}.step_contract_id",
        ),
        "step_contract_version": _require_string(
            contract.step_contract_version,
            path=f"{path}.step_contract_version",
        ),
        "callable_ref": _require_string(
            contract.callable_ref,
            path=f"{path}.callable_ref",
        ),
        "runner_contract_version": _require_string(
            contract.runner_contract_version,
            path=f"{path}.runner_contract_version",
        ),
    }


def _binding_payload(binding: ProjectionBinding, *, path: str) -> dict[str, Any]:
    if isinstance(binding, RegisteredSourceBinding):
        return _registered_source_payload(binding, path=path)
    if isinstance(binding, UpstreamRequestedOutputBinding):
        return _upstream_output_payload(binding, path=path)
    if isinstance(binding, CollectionBinding):
        return _collection_payload(binding, path=path)
    raise ValidationError(f"{path} contains an unsupported binding")


def _registered_source_payload(
    binding: RegisteredSourceBinding,
    *,
    path: str,
) -> dict[str, Any]:
    if not isinstance(binding.source_coordinate, LogicalSourceCoordinate):
        raise ValidationError(
            f"{path}.source_coordinate must be a LogicalSourceCoordinate"
        )
    file_size = _require_int(
        binding.registered_file_size,
        path=f"{path}.registered_file_size",
    )
    if file_size < 0:
        raise ValidationError(f"{path}.registered_file_size must be non-negative")
    return {
        "role": _require_string(binding.role, path=f"{path}.role"),
        "source_coordinate": {
            "context": _require_string(
                binding.source_coordinate.context,
                path=f"{path}.source_coordinate.context",
            ),
            "scope": binding.source_coordinate.scope,
            "source_name": binding.source_coordinate.source_name,
            "entity_id": binding.source_coordinate.entity_id,
        },
        "registered_content_digest": _require_digest(
            binding.registered_content_digest,
            path=f"{path}.registered_content_digest",
        ),
        "registered_file_size": file_size,
        "declared_extension": _require_string(
            binding.declared_extension,
            path=f"{path}.declared_extension",
        ),
    }


def _upstream_output_payload(
    binding: UpstreamRequestedOutputBinding,
    *,
    path: str,
) -> dict[str, Any]:
    if not isinstance(binding, UpstreamRequestedOutputBinding):
        raise ValidationError(
            f"{path} must be an UpstreamRequestedOutputBinding"
        )
    return {
        "role": _require_string(binding.role, path=f"{path}.role"),
        "upstream_request_bundle_digest": _require_digest(
            binding.upstream_request_bundle_digest,
            path=f"{path}.upstream_request_bundle_digest",
        ),
        "output_name": _require_string(
            binding.output_name,
            path=f"{path}.output_name",
        ),
    }


def _collection_payload(binding: CollectionBinding, *, path: str) -> dict[str, Any]:
    _require_tuple(binding.members, path=f"{path}.members")
    members = [
        _upstream_output_payload(member, path=f"{path}.members[{index}]")
        for index, member in enumerate(binding.members)
    ]
    member_identities = [
        (
            member["upstream_request_bundle_digest"],
            member["output_name"],
        )
        for member in members
    ]
    if len(member_identities) != len(set(member_identities)):
        raise ValidationError(f"{path}.members contains duplicate requested outputs")
    members.sort(key=_requested_output_sort_key)
    manifest_value_schema, manifest_digest = _manifest_value_reference(
        value_schema=binding.manifest_value_schema,
        manifest_digest=binding.manifest_digest,
        path=path,
    )
    return {
        "role": _require_string(binding.role, path=f"{path}.role"),
        "collection_semantics": _require_string(
            binding.collection_semantics,
            path=f"{path}.collection_semantics",
        ),
        "manifest_value_schema": manifest_value_schema,
        "manifest_digest": manifest_digest,
        "members": members,
    }


def _output_contract_payload(
    contract: OutputContract,
    *,
    path: str,
) -> dict[str, Any]:
    if not isinstance(contract, OutputContract):
        raise ValidationError(f"{path} must be an OutputContract")
    output_contract_version = _require_int(
        contract.output_contract_version,
        path=f"{path}.output_contract_version",
    )
    if output_contract_version != OUTPUT_CONTRACT_VERSION:
        raise ValidationError(
            f"{path}.output_contract_version must be {OUTPUT_CONTRACT_VERSION}"
        )
    _require_tuple(contract.sibling_outputs, path=f"{path}.sibling_outputs")
    siblings: list[dict[str, str]] = []
    for index, sibling in enumerate(contract.sibling_outputs):
        if not isinstance(sibling, SiblingOutput):
            raise ValidationError(
                f"{path}.sibling_outputs[{index}] must be a SiblingOutput"
            )
        siblings.append(
            {
                "output_name": _require_string(
                    sibling.output_name,
                    path=f"{path}.sibling_outputs[{index}].output_name",
                ),
                "declared_extension": _require_string(
                    sibling.declared_extension,
                    path=f"{path}.sibling_outputs[{index}].declared_extension",
                ),
            }
        )
    output_names = [sibling["output_name"] for sibling in siblings]
    if len(output_names) != len(set(output_names)):
        raise ValidationError(f"{path}.sibling_outputs contains duplicate output names")
    siblings.sort(key=lambda sibling: (sibling["output_name"], _dump_json(sibling)))
    return {
        "output_contract_version": output_contract_version,
        "sibling_outputs": siblings,
    }


def _binding_sort_key(payload: dict[str, Any]) -> tuple[str, str, str]:
    payload_without_role = dict(payload)
    role = payload_without_role.pop("role")
    return role, _binding_kind(payload), _dump_json(payload_without_role)


def _binding_kind(payload: dict[str, Any]) -> str:
    if "source_coordinate" in payload:
        return "registered_source"
    if "upstream_request_bundle_digest" in payload:
        return "upstream_requested_output"
    if "collection_semantics" in payload:
        return "collection"
    raise ValidationError("binding payload has no recognized binding kind")


def _requested_output_sort_key(payload: dict[str, Any]) -> tuple[str, str]:
    return (
        payload["upstream_request_bundle_digest"],
        payload["output_name"],
    )


def _json_value(value: Any, *, path: str) -> Any:
    if value is None or type(value) is bool or type(value) is str:
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError(f"{path} must contain only finite floats")
        return value
    if type(value) is list:
        return [
            _json_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if type(value) is dict:
        for key in value:
            if type(key) is not str:
                raise ValidationError(f"{path} must contain only string object keys")
        return {
            key: _json_value(item, path=f"{path}.{key}")
            for key, item in value.items()
        }
    raise ValidationError(
        f"{path} contains unsupported value type: {type(value).__name__}"
    )


def _require_string(value: Any, *, path: str) -> str:
    if type(value) is not str or not value:
        raise ValidationError(f"{path} must be a non-empty string")
    return value


def _require_digest(value: Any, *, path: str) -> str:
    if not is_valid_digest(value):
        raise ValidationError(
            f"{path} must be a lowercase 64-character hexadecimal SHA-256 digest"
        )
    return value


def _manifest_value_reference(
    *,
    value_schema: Any,
    manifest_digest: Any,
    path: str,
) -> tuple[str | None, str | None]:
    if (value_schema is None) != (manifest_digest is None):
        raise ValidationError(
            f"{path}.manifest_value_schema and {path}.manifest_digest "
            "must both be null or both be present"
        )
    if value_schema is None:
        return None, None
    schema = _require_string(value_schema, path=f"{path}.manifest_value_schema")
    if schema != MANIFEST_VALUE_SCHEMA:
        raise ValidationError(
            f"{path}.manifest_value_schema must be {MANIFEST_VALUE_SCHEMA!r}"
        )
    return schema, _require_digest(manifest_digest, path=f"{path}.manifest_digest")


def _require_int(value: Any, *, path: str) -> int:
    if type(value) is not int:
        raise ValidationError(f"{path} must be an integer")
    return value


def _require_tuple(value: Any, *, path: str) -> tuple[Any, ...]:
    if type(value) is not tuple:
        raise ValidationError(f"{path} must be a structural tuple")
    return value


def _dump_json(payload: Any) -> str:
    return json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
