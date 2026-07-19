"""Canonical requested-computation projection contracts."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, TypeAlias

from .errors import ValidationError

IDENTITY_CONTRACT_VERSION = 1
OUTPUT_CONTRACT_VERSION = 1
RUNNER_CONTRACT_VERSION = "1"

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True)
class StepContract:
    step_contract_id: str
    step_contract_version: str
    callable_ref: str
    runner_contract_version: str


@dataclass(frozen=True)
class SourceCoordinate:
    namespace: str
    path: str


@dataclass(frozen=True)
class RegisteredSourceBinding:
    role: str
    source_coordinate: SourceCoordinate
    registered_content_digest: str
    registered_file_size: int
    declared_extension: str


@dataclass(frozen=True)
class UpstreamRequestedOutputBinding:
    role: str
    upstream_request_projection: RequestBundleProjectionV1
    output_name: str


@dataclass(frozen=True)
class CollectionBinding:
    role: str
    collection_semantics: str
    manifest_digest: str | None
    members: tuple[UpstreamRequestedOutputBinding, ...]


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
class RequestBundleProjectionV1:
    identity_contract_version: int
    namespace: str
    step_contract: StepContract
    address: str
    canonical_parameters: JsonValue
    role_labelled_bindings: tuple[ProjectionBinding, ...]
    result_affecting_settings: dict[str, JsonValue]
    determinism_contract: str
    output_contract: OutputContract


def canonical_projection_json(projection: RequestBundleProjectionV1) -> str:
    """Serialize one V1 request projection to its canonical JSON form."""
    if not isinstance(projection, RequestBundleProjectionV1):
        raise ValidationError("projection must be a RequestBundleProjectionV1")
    payload = _projection_payload(projection, path="projection")
    return _dump_json(payload)


def _projection_payload(
    projection: RequestBundleProjectionV1,
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
    if not isinstance(binding.source_coordinate, SourceCoordinate):
        raise ValidationError(f"{path}.source_coordinate must be a SourceCoordinate")
    file_size = _require_int(
        binding.registered_file_size,
        path=f"{path}.registered_file_size",
    )
    if file_size < 0:
        raise ValidationError(f"{path}.registered_file_size must be non-negative")
    return {
        "role": _require_string(binding.role, path=f"{path}.role"),
        "source_coordinate": {
            "namespace": _require_string(
                binding.source_coordinate.namespace,
                path=f"{path}.source_coordinate.namespace",
            ),
            "path": _require_string(
                binding.source_coordinate.path,
                path=f"{path}.source_coordinate.path",
            ),
        },
        "registered_content_digest": _require_string(
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
    if not isinstance(binding.upstream_request_projection, RequestBundleProjectionV1):
        raise ValidationError(
            f"{path}.upstream_request_projection must be a "
            "RequestBundleProjectionV1"
        )
    return {
        "role": _require_string(binding.role, path=f"{path}.role"),
        "upstream_request_projection": _projection_payload(
            binding.upstream_request_projection,
            path=f"{path}.upstream_request_projection",
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
    members.sort(key=_requested_output_sort_key)
    manifest_digest = binding.manifest_digest
    if manifest_digest is not None:
        manifest_digest = _require_string(
            manifest_digest,
            path=f"{path}.manifest_digest",
        )
    return {
        "role": _require_string(binding.role, path=f"{path}.role"),
        "collection_semantics": _require_string(
            binding.collection_semantics,
            path=f"{path}.collection_semantics",
        ),
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
    if "upstream_request_projection" in payload:
        return "upstream_requested_output"
    if "collection_semantics" in payload:
        return "collection"
    raise ValidationError("binding payload has no recognized binding kind")


def _requested_output_sort_key(payload: dict[str, Any]) -> tuple[str, str]:
    requested_output_identity = dict(payload)
    role = requested_output_identity.pop("role")
    return _dump_json(requested_output_identity), role


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
