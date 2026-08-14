"""Pure lowering for bounded finite specification sets."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from itertools import product
import math
from typing import Any, Literal

from .errors import ValidationError
from .identity import validate_path_token

type JsonValue = (
    None
    | bool
    | int
    | float
    | str
    | list["JsonValue"]
    | dict[str, "JsonValue"]
)
type Disposition = Literal["included", "excluded"]

_SPECIFICATION_SCHEMA = "nipact/specification-set/v1"
_LIBRARY_SCHEMA = "nipact/specification-library/v1"
_OPERATION_FIELDS = frozenset(
    {
        "workflow",
        "fragments",
        "set",
        "execution_population",
        "manifest_bindings",
        "target",
        "results",
    }
)


@dataclass(frozen=True, eq=False)
class DecisionCoordinate:
    name: str
    value: JsonValue

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, DecisionCoordinate)
            and self.name == other.name
            and _typed_key(self.value) == _typed_key(other.value)
        )

    __hash__ = None


@dataclass(frozen=True, eq=False)
class ParameterWrite:
    step_name: str
    parameter_name: str
    value: JsonValue

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ParameterWrite)
            and self.step_name == other.step_name
            and self.parameter_name == other.parameter_name
            and _typed_key(self.value) == _typed_key(other.value)
        )

    __hash__ = None


@dataclass(frozen=True)
class ExecutionPopulationWrite:
    manifest_name: str


@dataclass(frozen=True)
class ManifestBindingWrite:
    step_name: str
    role: str
    manifest_name: str


@dataclass(frozen=True)
class TargetWrite:
    step_name: str
    output_name: str


@dataclass(frozen=True)
class ResultWrite:
    role: str
    step_name: str
    output_name: str


type SpecificationWrite = (
    ParameterWrite
    | ExecutionPopulationWrite
    | ManifestBindingWrite
    | TargetWrite
    | ResultWrite
)


@dataclass(frozen=True)
class ProvisionalMember:
    member_key: str
    decision_coordinates: tuple[DecisionCoordinate, ...]
    workflow_name: str
    writes: tuple[SpecificationWrite, ...]
    disposition: Disposition
    exclusion_reason: str | None

    __hash__ = None


@dataclass(frozen=True)
class ProvisionalCompilation:
    set_key: str
    members: tuple[ProvisionalMember, ...]
    equivalent_member_groups: tuple[tuple[str, ...], ...]

    __hash__ = None


@dataclass(frozen=True)
class _Operations:
    workflow_name: str | None
    writes: tuple[SpecificationWrite, ...]


@dataclass(frozen=True)
class _DimensionOption:
    coordinate_value: JsonValue
    operations: _Operations


@dataclass(frozen=True)
class _Candidate:
    member_key: str | None
    coordinates: tuple[DecisionCoordinate, ...]
    operations: _Operations
    disposition: Disposition
    exclusion_reason: str | None


def compile_specification(
    specification: Mapping[str, Any],
    libraries: Mapping[str, Mapping[str, Any]],
) -> ProvisionalCompilation:
    """Lower strict semantic mappings without consulting project or runtime state."""
    root = _mapping(specification, label="specification")
    library_names = _parse_library_allowlist(root)
    fragments = _parse_libraries(libraries, allowed_names=library_names)
    set_key = _parse_set_key(root)
    fixed = _parse_operation_block(
        _required(root, "fixed", label="specification"),
        label="fixed",
        fragments=fragments,
    )

    has_members = "members" in root
    has_product_fields = "dimensions" in root or "combine" in root or "exclude" in root
    if has_members and has_product_fields:
        raise ValidationError("specification cannot mix explicit and product modes")
    if has_members:
        _fields(
            root,
            allowed={
                "schema",
                "specification_set",
                "libraries",
                "fixed",
                "members",
                "expected_counts",
            },
            required={
                "schema",
                "specification_set",
                "libraries",
                "fixed",
                "members",
                "expected_counts",
            },
            label="specification",
        )
        candidates = _compile_explicit_candidates(
            root["members"],
            fixed=fixed,
            fragments=fragments,
        )
    else:
        _fields(
            root,
            allowed={
                "schema",
                "specification_set",
                "libraries",
                "fixed",
                "dimensions",
                "combine",
                "exclude",
                "expected_counts",
            },
            required={
                "schema",
                "specification_set",
                "libraries",
                "fixed",
                "dimensions",
                "combine",
                "expected_counts",
            },
            label="specification",
        )
        candidates = _compile_product_candidates(
            dimensions_payload=root["dimensions"],
            combine_payload=root["combine"],
            exclusions_payload=root.get("exclude", []),
            fixed=fixed,
            fragments=fragments,
        )

    _validate_candidate_set(candidates)
    _check_expected_counts(root["expected_counts"], candidates=candidates)
    members = _materialize_members(candidates)
    return ProvisionalCompilation(
        set_key=set_key,
        members=members,
        equivalent_member_groups=_equivalent_member_groups(members),
    )


def _parse_library_allowlist(root: Mapping[str, Any]) -> tuple[str, ...]:
    if root.get("schema") != _SPECIFICATION_SCHEMA:
        raise ValidationError(f"specification schema must be {_SPECIFICATION_SCHEMA!r}")
    raw_names = _required(root, "libraries", label="specification")
    if type(raw_names) is not list:
        raise ValidationError("specification libraries must be a list")
    names: list[str] = []
    seen: set[str] = set()
    for raw_name in raw_names:
        name = validate_path_token(raw_name, label="specification library name")
        if name in seen:
            raise ValidationError(f"duplicate specification library: {name}")
        seen.add(name)
        names.append(name)
    return tuple(sorted(names))


def _parse_set_key(root: Mapping[str, Any]) -> str:
    payload = _mapping(
        _required(root, "specification_set", label="specification"),
        label="specification_set",
    )
    _fields(
        payload,
        allowed={"key"},
        required={"key"},
        label="specification_set",
    )
    return validate_path_token(payload["key"], label="specification set key")


def _parse_libraries(
    libraries: Mapping[str, Mapping[str, Any]],
    *,
    allowed_names: tuple[str, ...],
) -> dict[tuple[str, str], _Operations]:
    library_map = _mapping(libraries, label="libraries")
    supplied_names: set[str] = set()
    for raw_name in library_map:
        supplied_names.add(validate_path_token(raw_name, label="library name"))
    allowed_set = set(allowed_names)
    missing = sorted(allowed_set - supplied_names)
    extra = sorted(supplied_names - allowed_set)
    if missing:
        raise ValidationError(f"missing selected specification library: {missing[0]}")
    if extra:
        raise ValidationError(f"unselected specification library supplied: {extra[0]}")

    fragments: dict[tuple[str, str], _Operations] = {}
    for library_name in allowed_names:
        payload = _mapping(library_map[library_name], label=f"library {library_name!r}")
        _fields(
            payload,
            allowed={"schema", "fragments"},
            required={"schema", "fragments"},
            label=f"library {library_name!r}",
        )
        if payload["schema"] != _LIBRARY_SCHEMA:
            raise ValidationError(
                f"library {library_name!r} schema must be {_LIBRARY_SCHEMA!r}"
            )
        fragment_map = _mapping(
            payload["fragments"],
            label=f"library {library_name!r} fragments",
        )
        for raw_fragment_name, fragment_payload in sorted(
            fragment_map.items(), key=lambda item: str(item[0])
        ):
            fragment_name = validate_path_token(
                raw_fragment_name,
                label=f"library {library_name!r} fragment name",
            )
            fragments[(library_name, fragment_name)] = _parse_operation_block(
                fragment_payload,
                label=f"fragment {library_name}.{fragment_name}",
                fragments={},
                allow_fragments=False,
            )
    return fragments


def _compile_product_candidates(
    *,
    dimensions_payload: object,
    combine_payload: object,
    exclusions_payload: object,
    fixed: _Operations,
    fragments: Mapping[tuple[str, str], _Operations],
) -> tuple[_Candidate, ...]:
    dimension_map = _mapping(dimensions_payload, label="dimensions")
    if not dimension_map:
        raise ValidationError("product dimensions cannot be empty")
    dimensions: dict[str, tuple[_DimensionOption, ...]] = {}
    for raw_name, payload in sorted(dimension_map.items(), key=lambda item: str(item[0])):
        name = validate_path_token(raw_name, label="dimension name")
        dimensions[name] = _parse_dimension(
            payload,
            name=name,
            fragments=fragments,
        )

    combine = _mapping(combine_payload, label="combine")
    _fields(combine, allowed={"product"}, required={"product"}, label="combine")
    raw_product = combine["product"]
    if type(raw_product) is not list or not raw_product:
        raise ValidationError("combine.product must be a nonempty list")
    product_names = [
        validate_path_token(value, label="combine.product dimension")
        for value in raw_product
    ]
    if len(set(product_names)) != len(product_names):
        raise ValidationError("combine.product cannot repeat dimensions")
    if set(product_names) != set(dimensions):
        raise ValidationError("combine.product must list every dimension exactly once")

    axis_names = tuple(sorted(dimensions))
    raw_candidates: list[_Candidate] = []
    for selected in product(*(dimensions[name] for name in axis_names)):
        coordinates = tuple(
            DecisionCoordinate(name=name, value=_copy_json(option.coordinate_value, label=name))
            for name, option in zip(axis_names, selected, strict=True)
        )
        operations = _merge_operations(
            (fixed, *(option.operations for option in selected)),
            label=f"candidate {_coordinates_label(coordinates)}",
        )
        _validate_complete_operations(
            operations,
            label=f"candidate {_coordinates_label(coordinates)}",
        )
        raw_candidates.append(
            _Candidate(
                member_key=None,
                coordinates=coordinates,
                operations=operations,
                disposition="included",
                exclusion_reason=None,
            )
        )

    ordered = tuple(sorted(raw_candidates, key=_candidate_sort_key))
    exclusions = _parse_exclusions(
        exclusions_payload,
        dimensions=dimensions,
        candidates=ordered,
    )
    excluded_candidates: list[_Candidate] = []
    for candidate in ordered:
        matching = [rule for rule in exclusions if _exclusion_matches(rule[0], candidate)]
        if len(matching) > 1:
            raise ValidationError(
                f"candidate {_coordinates_label(candidate.coordinates)} matches multiple exclusions"
            )
        if matching:
            excluded_candidates.append(
                _Candidate(
                    member_key=None,
                    coordinates=candidate.coordinates,
                    operations=candidate.operations,
                    disposition="excluded",
                    exclusion_reason=matching[0][1],
                )
            )
        else:
            excluded_candidates.append(candidate)
    return tuple(excluded_candidates)


def _parse_dimension(
    payload: object,
    *,
    name: str,
    fragments: Mapping[tuple[str, str], _Operations],
) -> tuple[_DimensionOption, ...]:
    dimension = _mapping(payload, label=f"dimension {name!r}")
    if "choices" in dimension:
        _fields(
            dimension,
            allowed={"choices"},
            required={"choices"},
            label=f"dimension {name!r}",
        )
        choices = _mapping(dimension["choices"], label=f"dimension {name!r} choices")
        if not choices:
            raise ValidationError(f"dimension {name!r} choices cannot be empty")
        options: list[_DimensionOption] = []
        for raw_choice, choice_payload in sorted(
            choices.items(), key=lambda item: str(item[0])
        ):
            choice = validate_path_token(raw_choice, label=f"dimension {name!r} choice")
            options.append(
                _DimensionOption(
                    coordinate_value=choice,
                    operations=_parse_operation_block(
                        choice_payload,
                        label=f"dimension {name!r} choice {choice!r}",
                        fragments=fragments,
                    ),
                )
            )
        return tuple(options)

    _fields(
        dimension,
        allowed={"set", "values", "integer_range"},
        required={"set"},
        label=f"dimension {name!r}",
    )
    has_values = "values" in dimension
    has_range = "integer_range" in dimension
    if has_values == has_range:
        raise ValidationError(
            f"dimension {name!r} requires exactly one of values or integer_range"
        )
    destination = _parse_parameter_destination(
        dimension["set"],
        label=f"dimension {name!r} set",
    )
    if has_values:
        raw_values = dimension["values"]
        if type(raw_values) is not list or not raw_values:
            raise ValidationError(f"dimension {name!r} values must be a nonempty list")
        values = tuple(
            _copy_json(value, label=f"dimension {name!r} value")
            for value in raw_values
        )
    else:
        values = _parse_integer_range(
            dimension["integer_range"],
            label=f"dimension {name!r} integer_range",
        )
    typed_values = [_typed_key(value) for value in values]
    if len(set(typed_values)) != len(typed_values):
        raise ValidationError(f"dimension {name!r} contains duplicate values")
    return tuple(
        _DimensionOption(
            coordinate_value=_copy_json(value, label=f"dimension {name!r} value"),
            operations=_Operations(
                workflow_name=None,
                writes=(
                    ParameterWrite(
                        step_name=destination[0],
                        parameter_name=destination[1],
                        value=_copy_json(value, label=f"dimension {name!r} value"),
                    ),
                ),
            ),
        )
        for value in values
    )


def _parse_integer_range(payload: object, *, label: str) -> tuple[int, ...]:
    range_mapping = _mapping(payload, label=label)
    _fields(
        range_mapping,
        allowed={"start", "stop", "step", "endpoint"},
        required={"start", "stop", "step", "endpoint"},
        label=label,
    )
    start = _exact_integer(range_mapping["start"], label=f"{label}.start")
    stop = _exact_integer(range_mapping["stop"], label=f"{label}.stop")
    step = _exact_integer(range_mapping["step"], label=f"{label}.step")
    if step == 0:
        raise ValidationError(f"{label}.step cannot be zero")
    endpoint = range_mapping["endpoint"]
    if type(endpoint) is not str or endpoint not in {"inclusive", "exclusive"}:
        raise ValidationError(f"{label}.endpoint must be 'inclusive' or 'exclusive'")
    if start == stop:
        if endpoint == "inclusive":
            return (start,)
        raise ValidationError(f"{label} cannot be an empty exclusive range")
    if step > 0 and start > stop:
        raise ValidationError(f"{label} positive step requires start < stop")
    if step < 0 and start < stop:
        raise ValidationError(f"{label} negative step requires start > stop")
    if endpoint == "inclusive":
        distance = stop - start
        if distance % step != 0:
            raise ValidationError(f"{label} inclusive range must land exactly on stop")
        return tuple(range(start, stop + step, step))
    return tuple(range(start, stop, step))


def _compile_explicit_candidates(
    payload: object,
    *,
    fixed: _Operations,
    fragments: Mapping[tuple[str, str], _Operations],
) -> tuple[_Candidate, ...]:
    if type(payload) is not list or not payload:
        raise ValidationError("members must be a nonempty list")
    candidates: list[_Candidate] = []
    seen_keys: set[str] = set()
    seen_coordinates: set[tuple[Any, ...]] = set()
    metadata_fields = frozenset({"key", "decision_coordinates", "disposition", "reason"})
    for index, raw_member in enumerate(payload):
        label = f"member {index}"
        member = _mapping(raw_member, label=label)
        _fields(
            member,
            allowed=_OPERATION_FIELDS | metadata_fields,
            required={"key", "decision_coordinates", "disposition"},
            label=label,
        )
        key = validate_path_token(member["key"], label=f"{label} key")
        if key in seen_keys:
            raise ValidationError(f"duplicate explicit member key: {key}")
        seen_keys.add(key)
        coordinates = _parse_decision_coordinates(
            member["decision_coordinates"],
            label=f"{label} decision_coordinates",
        )
        coordinate_key = _coordinates_key(coordinates)
        if coordinate_key in seen_coordinates:
            raise ValidationError(f"duplicate explicit decision coordinates for member {key}")
        seen_coordinates.add(coordinate_key)
        disposition, reason = _parse_disposition(member, label=label)
        member_operations = _parse_operation_block(
            member,
            label=label,
            fragments=fragments,
            extra_fields=metadata_fields,
        )
        operations = _merge_operations((fixed, member_operations), label=label)
        _validate_complete_operations(operations, label=label)
        candidates.append(
            _Candidate(
                member_key=key,
                coordinates=coordinates,
                operations=operations,
                disposition=disposition,
                exclusion_reason=reason,
            )
        )
    return tuple(sorted(candidates, key=_candidate_sort_key))


def _parse_decision_coordinates(
    payload: object,
    *,
    label: str,
) -> tuple[DecisionCoordinate, ...]:
    coordinates = _mapping(payload, label=label)
    if not coordinates:
        raise ValidationError(f"{label} cannot be empty")
    parsed: list[DecisionCoordinate] = []
    for raw_name, value in sorted(coordinates.items(), key=lambda item: str(item[0])):
        name = validate_path_token(raw_name, label=f"{label} name")
        parsed.append(
            DecisionCoordinate(name=name, value=_copy_json(value, label=f"{label}.{name}"))
        )
    return tuple(parsed)


def _parse_disposition(
    member: Mapping[str, Any],
    *,
    label: str,
) -> tuple[Disposition, str | None]:
    disposition = member["disposition"]
    if disposition == "included":
        if "reason" in member:
            raise ValidationError(f"{label} included disposition forbids reason")
        return "included", None
    if disposition == "excluded":
        if "reason" not in member:
            raise ValidationError(f"{label} excluded disposition requires reason")
        return "excluded", _reason(member["reason"], label=f"{label} reason")
    raise ValidationError(f"{label} disposition must be 'included' or 'excluded'")


def _parse_exclusions(
    payload: object,
    *,
    dimensions: Mapping[str, tuple[_DimensionOption, ...]],
    candidates: tuple[_Candidate, ...],
) -> tuple[tuple[tuple[DecisionCoordinate, ...], str], ...]:
    if type(payload) is not list:
        raise ValidationError("exclude must be a list")
    rules: list[tuple[tuple[DecisionCoordinate, ...], str]] = []
    for index, raw_rule in enumerate(payload):
        label = f"exclude rule {index}"
        rule = _mapping(raw_rule, label=label)
        _fields(rule, allowed={"when", "reason"}, required={"when", "reason"}, label=label)
        when_mapping = _mapping(rule["when"], label=f"{label} when")
        if not when_mapping:
            raise ValidationError(f"{label} when cannot be empty")
        when: list[DecisionCoordinate] = []
        for raw_axis, raw_value in sorted(
            when_mapping.items(), key=lambda item: str(item[0])
        ):
            axis = validate_path_token(raw_axis, label=f"{label} axis")
            if axis not in dimensions:
                raise ValidationError(f"{label} references unknown dimension: {axis}")
            value = _copy_json(raw_value, label=f"{label} value")
            available = {
                _typed_key(option.coordinate_value) for option in dimensions[axis]
            }
            if _typed_key(value) not in available:
                raise ValidationError(f"{label} value is not declared for dimension {axis}")
            when.append(DecisionCoordinate(name=axis, value=value))
        parsed = (tuple(when), _reason(rule["reason"], label=f"{label} reason"))
        if not any(_exclusion_matches(parsed[0], candidate) for candidate in candidates):
            raise ValidationError(f"{label} does not match any candidate")
        rules.append(parsed)
    return tuple(sorted(rules, key=lambda rule: (_coordinates_key(rule[0]), rule[1])))


def _exclusion_matches(
    when: tuple[DecisionCoordinate, ...],
    candidate: _Candidate,
) -> bool:
    candidate_values = {
        coordinate.name: _typed_key(coordinate.value)
        for coordinate in candidate.coordinates
    }
    return all(candidate_values[item.name] == _typed_key(item.value) for item in when)


def _parse_operation_block(
    payload: object,
    *,
    label: str,
    fragments: Mapping[tuple[str, str], _Operations],
    allow_fragments: bool = True,
    extra_fields: frozenset[str] = frozenset(),
) -> _Operations:
    block = _mapping(payload, label=label)
    allowed = (_OPERATION_FIELDS if allow_fragments else _OPERATION_FIELDS - {"fragments"})
    _fields(block, allowed=allowed | extra_fields, required=set(), label=label)
    pieces: list[_Operations] = []
    if "fragments" in block:
        references = block["fragments"]
        if type(references) is not list:
            raise ValidationError(f"{label} fragments must be a list")
        parsed_references: list[tuple[str, str]] = []
        for index, raw_reference in enumerate(references):
            reference = _mapping(raw_reference, label=f"{label} fragment reference {index}")
            _fields(
                reference,
                allowed={"library", "fragment"},
                required={"library", "fragment"},
                label=f"{label} fragment reference {index}",
            )
            library_name = validate_path_token(
                reference["library"], label=f"{label} fragment library"
            )
            fragment_name = validate_path_token(
                reference["fragment"], label=f"{label} fragment name"
            )
            key = (library_name, fragment_name)
            if key not in fragments:
                raise ValidationError(
                    f"{label} references unknown or unselected fragment "
                    f"{library_name}.{fragment_name}"
                )
            parsed_references.append(key)
        if len(set(parsed_references)) != len(parsed_references):
            raise ValidationError(f"{label} contains duplicate fragment references")
        pieces.extend(fragments[key] for key in sorted(parsed_references))

    workflow_name: str | None = None
    if "workflow" in block:
        workflow_name = validate_path_token(block["workflow"], label=f"{label} workflow")
    writes: list[SpecificationWrite] = []
    if "set" in block:
        raw_writes = block["set"]
        if type(raw_writes) is not list:
            raise ValidationError(f"{label} set must be a list")
        for index, raw_write in enumerate(raw_writes):
            write = _mapping(raw_write, label=f"{label} set item {index}")
            _fields(
                write,
                allowed={"step", "parameter", "value"},
                required={"step", "parameter", "value"},
                label=f"{label} set item {index}",
            )
            writes.append(
                ParameterWrite(
                    step_name=validate_path_token(
                        write["step"], label=f"{label} set step"
                    ),
                    parameter_name=validate_path_token(
                        write["parameter"], label=f"{label} set parameter"
                    ),
                    value=_copy_json(write["value"], label=f"{label} set value"),
                )
            )
    if "execution_population" in block:
        writes.append(
            ExecutionPopulationWrite(
                manifest_name=validate_path_token(
                    block["execution_population"],
                    label=f"{label} execution_population",
                )
            )
        )
    if "manifest_bindings" in block:
        raw_bindings = block["manifest_bindings"]
        if type(raw_bindings) is not list:
            raise ValidationError(f"{label} manifest_bindings must be a list")
        for index, raw_binding in enumerate(raw_bindings):
            binding = _mapping(raw_binding, label=f"{label} manifest binding {index}")
            _fields(
                binding,
                allowed={"step", "role", "manifest"},
                required={"step", "role", "manifest"},
                label=f"{label} manifest binding {index}",
            )
            writes.append(
                ManifestBindingWrite(
                    step_name=validate_path_token(
                        binding["step"], label=f"{label} manifest binding step"
                    ),
                    role=validate_path_token(
                        binding["role"], label=f"{label} manifest binding role"
                    ),
                    manifest_name=validate_path_token(
                        binding["manifest"], label=f"{label} manifest binding manifest"
                    ),
                )
            )
    if "target" in block:
        target = _parse_port(block["target"], label=f"{label} target")
        writes.append(TargetWrite(step_name=target[0], output_name=target[1]))
    if "results" in block:
        results = _mapping(block["results"], label=f"{label} results")
        for raw_role, raw_port in sorted(results.items(), key=lambda item: str(item[0])):
            role = validate_path_token(raw_role, label=f"{label} result role")
            port = _parse_port(raw_port, label=f"{label} result {role!r}")
            writes.append(
                ResultWrite(role=role, step_name=port[0], output_name=port[1])
            )
    pieces.append(_Operations(workflow_name=workflow_name, writes=tuple(writes)))
    return _merge_operations(tuple(pieces), label=label)


def _parse_parameter_destination(payload: object, *, label: str) -> tuple[str, str]:
    destination = _mapping(payload, label=label)
    _fields(
        destination,
        allowed={"step", "parameter"},
        required={"step", "parameter"},
        label=label,
    )
    return (
        validate_path_token(destination["step"], label=f"{label} step"),
        validate_path_token(destination["parameter"], label=f"{label} parameter"),
    )


def _parse_port(payload: object, *, label: str) -> tuple[str, str]:
    port = _mapping(payload, label=label)
    _fields(port, allowed={"step", "output"}, required={"step", "output"}, label=label)
    return (
        validate_path_token(port["step"], label=f"{label} step"),
        validate_path_token(port["output"], label=f"{label} output"),
    )


def _merge_operations(
    pieces: tuple[_Operations, ...],
    *,
    label: str,
) -> _Operations:
    workflows = [piece.workflow_name for piece in pieces if piece.workflow_name is not None]
    if len(workflows) > 1:
        raise ValidationError(f"{label} writes workflow more than once")
    writes: list[SpecificationWrite] = []
    destinations: set[tuple[Any, ...]] = set()
    for piece in pieces:
        for write in piece.writes:
            destination = _write_destination(write)
            if destination in destinations:
                raise ValidationError(f"{label} writes destination more than once: {destination}")
            destinations.add(destination)
            writes.append(_copy_write(write))
    ordered_writes = tuple(sorted(writes, key=_write_sort_key))
    _validate_partial_result_ports(ordered_writes, label=label)
    return _Operations(
        workflow_name=workflows[0] if workflows else None,
        writes=ordered_writes,
    )


def _validate_partial_result_ports(
    writes: tuple[SpecificationWrite, ...],
    *,
    label: str,
) -> None:
    targets = [write for write in writes if isinstance(write, TargetWrite)]
    results = [write for write in writes if isinstance(write, ResultWrite)]
    result_ports = [(write.step_name, write.output_name) for write in results]
    if len(set(result_ports)) != len(result_ports):
        raise ValidationError(f"{label} assigns one result port to multiple roles")
    result_steps = {write.step_name for write in results}
    if len(result_steps) > 1:
        raise ValidationError(f"{label} result ports must be siblings on one step")
    if targets and result_steps and targets[0].step_name not in result_steps:
        raise ValidationError(f"{label} result ports must be siblings of the target")


def _validate_complete_operations(operations: _Operations, *, label: str) -> None:
    if operations.workflow_name is None:
        raise ValidationError(f"{label} requires exactly one workflow selector")
    targets = [write for write in operations.writes if isinstance(write, TargetWrite)]
    if len(targets) != 1:
        raise ValidationError(f"{label} requires exactly one target")
    results = [write for write in operations.writes if isinstance(write, ResultWrite)]
    if not results:
        raise ValidationError(f"{label} requires at least one result role")
    target_port = (targets[0].step_name, targets[0].output_name)
    if sum(
        (result.step_name, result.output_name) == target_port for result in results
    ) != 1:
        raise ValidationError(f"{label} results must include the target output exactly once")


def _validate_candidate_set(candidates: tuple[_Candidate, ...]) -> None:
    if not candidates:
        raise ValidationError("specification must contain at least one candidate")
    role_sets = {
        tuple(
            write.role
            for write in candidate.operations.writes
            if isinstance(write, ResultWrite)
        )
        for candidate in candidates
    }
    if len(role_sets) != 1:
        raise ValidationError("every candidate must expose the same result-role names")


def _check_expected_counts(payload: object, *, candidates: tuple[_Candidate, ...]) -> None:
    counts = _mapping(payload, label="expected_counts")
    required = {"candidates", "included", "excluded"}
    _fields(counts, allowed=required, required=required, label="expected_counts")
    parsed = {
        name: _nonnegative_integer(counts[name], label=f"expected_counts.{name}")
        for name in required
    }
    if parsed["candidates"] < 1:
        raise ValidationError("expected_counts.candidates must be at least one")
    if parsed["candidates"] != parsed["included"] + parsed["excluded"]:
        raise ValidationError("expected_counts candidates must equal included plus excluded")
    actual = {
        "candidates": len(candidates),
        "included": sum(candidate.disposition == "included" for candidate in candidates),
        "excluded": sum(candidate.disposition == "excluded" for candidate in candidates),
    }
    if parsed != actual:
        raise ValidationError(f"expected_counts mismatch: expected {parsed}, compiled {actual}")


def _materialize_members(candidates: tuple[_Candidate, ...]) -> tuple[ProvisionalMember, ...]:
    ordered = tuple(sorted(candidates, key=_candidate_sort_key))
    width = max(6, len(str(len(ordered))))
    members: list[ProvisionalMember] = []
    for index, candidate in enumerate(ordered, start=1):
        member_key = candidate.member_key or f"member-{index:0{width}d}"
        workflow_name = candidate.operations.workflow_name
        if workflow_name is None:
            raise AssertionError("complete specification candidate has no workflow selector")
        members.append(
            ProvisionalMember(
                member_key=member_key,
                decision_coordinates=tuple(
                    DecisionCoordinate(
                        name=coordinate.name,
                        value=_copy_json(coordinate.value, label=coordinate.name),
                    )
                    for coordinate in candidate.coordinates
                ),
                workflow_name=workflow_name,
                writes=tuple(_copy_write(write) for write in candidate.operations.writes),
                disposition=candidate.disposition,
                exclusion_reason=candidate.exclusion_reason,
            )
        )
    return tuple(members)


def _equivalent_member_groups(
    members: tuple[ProvisionalMember, ...],
) -> tuple[tuple[str, ...], ...]:
    grouped: dict[tuple[Any, ...], list[str]] = defaultdict(list)
    for member in members:
        grouped[
            (
                member.workflow_name,
                tuple(_write_semantic_key(write) for write in member.writes),
            )
        ].append(member.member_key)
    groups = [tuple(sorted(keys)) for keys in grouped.values() if len(keys) > 1]
    return tuple(sorted(groups))


def _candidate_sort_key(candidate: _Candidate) -> tuple[Any, ...]:
    return (_coordinates_key(candidate.coordinates), candidate.member_key or "")


def _coordinates_key(coordinates: tuple[DecisionCoordinate, ...]) -> tuple[Any, ...]:
    return tuple((item.name, _typed_key(item.value)) for item in coordinates)


def _coordinates_label(coordinates: tuple[DecisionCoordinate, ...]) -> str:
    return ", ".join(item.name for item in coordinates)


def _write_destination(write: SpecificationWrite) -> tuple[Any, ...]:
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
    raise TypeError(f"unsupported specification write: {type(write)!r}")


def _write_sort_key(write: SpecificationWrite) -> tuple[Any, ...]:
    if isinstance(write, ParameterWrite):
        return (0, write.step_name, write.parameter_name, _typed_key(write.value))
    if isinstance(write, ExecutionPopulationWrite):
        return (1, write.manifest_name)
    if isinstance(write, ManifestBindingWrite):
        return (2, write.step_name, write.role, write.manifest_name)
    if isinstance(write, TargetWrite):
        return (3, write.step_name, write.output_name)
    if isinstance(write, ResultWrite):
        return (4, write.role, write.step_name, write.output_name)
    raise TypeError(f"unsupported specification write: {type(write)!r}")


def _write_semantic_key(write: SpecificationWrite) -> tuple[Any, ...]:
    if isinstance(write, ParameterWrite):
        return (
            "parameter",
            write.step_name,
            write.parameter_name,
            _typed_key(write.value),
        )
    if isinstance(write, ExecutionPopulationWrite):
        return ("execution_population", write.manifest_name)
    if isinstance(write, ManifestBindingWrite):
        return ("manifest_binding", write.step_name, write.role, write.manifest_name)
    if isinstance(write, TargetWrite):
        return ("target", write.step_name, write.output_name)
    if isinstance(write, ResultWrite):
        return ("result", write.role, write.step_name, write.output_name)
    raise TypeError(f"unsupported specification write: {type(write)!r}")


def _copy_write(write: SpecificationWrite) -> SpecificationWrite:
    if isinstance(write, ParameterWrite):
        return ParameterWrite(
            step_name=write.step_name,
            parameter_name=write.parameter_name,
            value=_copy_json(write.value, label="parameter value"),
        )
    return write


def _typed_key(value: object) -> tuple[Any, ...]:
    if value is None:
        return (0,)
    if type(value) is bool:
        return (1, value)
    if type(value) is int:
        return (2, value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError("non-finite numbers are not supported")
        is_negative_zero = value == 0.0 and math.copysign(1.0, value) < 0.0
        return (3, value, is_negative_zero)
    if type(value) is str:
        return (4, value)
    if type(value) is list:
        return (5, tuple(_typed_key(item) for item in value))
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValidationError("mapping keys must be strings")
        return (
            6,
            tuple(
                (key, _typed_key(value[key]))
                for key in sorted(value)
            ),
        )
    raise ValidationError(f"unsupported specification value type: {type(value).__name__}")


def _copy_json(value: object, *, label: str) -> JsonValue:
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError(f"{label} cannot contain a non-finite number")
        return value
    if type(value) is list:
        return [_copy_json(item, label=label) for item in value]
    if isinstance(value, Mapping):
        copied: dict[str, JsonValue] = {}
        for key in sorted(value, key=str):
            if type(key) is not str:
                raise ValidationError(f"{label} mapping keys must be strings")
            copied[key] = _copy_json(value[key], label=label)
        return copied
    raise ValidationError(f"{label} contains unsupported value type: {type(value).__name__}")


def _reason(value: object, *, label: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValidationError(f"{label} must be a trimmed nonempty string")
    return value


def _exact_integer(value: object, *, label: str) -> int:
    if type(value) is not int:
        raise ValidationError(f"{label} must be an integer")
    return value


def _nonnegative_integer(value: object, *, label: str) -> int:
    parsed = _exact_integer(value, label=label)
    if parsed < 0:
        raise ValidationError(f"{label} cannot be negative")
    return parsed


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    if any(type(key) is not str for key in value):
        raise ValidationError(f"{label} mapping keys must be strings")
    return value


def _fields(
    mapping: Mapping[str, Any],
    *,
    allowed: set[str] | frozenset[str],
    required: set[str] | frozenset[str],
    label: str,
) -> None:
    unknown = sorted(set(mapping) - set(allowed))
    if unknown:
        raise ValidationError(f"{label} contains unknown field: {unknown[0]}")
    missing = sorted(set(required) - set(mapping))
    if missing:
        raise ValidationError(f"{label} is missing required field: {missing[0]}")


def _required(mapping: Mapping[str, Any], key: str, *, label: str) -> Any:
    try:
        return mapping[key]
    except KeyError as exc:
        raise ValidationError(f"{label} is missing required field: {key}") from exc
