from __future__ import annotations

from copy import deepcopy
import math
from pathlib import Path
from typing import Any

import pytest

from nipact.errors import ValidationError
from nipact.specification_compiler import (
    DecisionCoordinate,
    ExecutionPopulationWrite,
    ManifestBindingWrite,
    ParameterWrite,
    ProvisionalCompilation,
    ProvisionalMember,
    ResultWrite,
    TargetWrite,
    compile_specification,
)


def _base_spec(*, values: list[Any] | None = None) -> dict[str, Any]:
    axis_values = [1, 2] if values is None else values
    return {
        "schema": "nipact/specification-set/v1",
        "specification_set": {"key": "compact-demo"},
        "libraries": [],
        "fixed": {
            "workflow": "base.workflow",
            "target": {"step": "summary.step", "output": "estimate.out"},
            "results": {
                "estimate": {"step": "summary.step", "output": "estimate.out"}
            },
        },
        "dimensions": {
            "alpha": {
                "set": {"step": "model.step", "parameter": "alpha.value"},
                "values": axis_values,
            }
        },
        "combine": {"product": ["alpha"]},
        "exclude": [],
        "expected_counts": {
            "candidates": len(axis_values),
            "included": len(axis_values),
            "excluded": 0,
        },
    }


def _library(fragments: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": "nipact/specification-library/v1",
        "fragments": fragments,
    }


def _parameter_values(compilation: Any, parameter: str = "alpha.value") -> list[Any]:
    return [
        write.value
        for member in compilation.members
        for write in member.writes
        if isinstance(write, ParameterWrite) and write.parameter_name == parameter
    ]


def test_signed_zero_values_remain_distinct_and_order_deterministically() -> None:
    compiled = compile_specification(_base_spec(values=[-0.0, 0.0]), {})

    assert [
        math.copysign(1.0, member.decision_coordinates[0].value)
        for member in compiled.members
    ] == [1.0, -1.0]
    assert [member.member_key for member in compiled.members] == [
        "member-000001",
        "member-000002",
    ]


def test_compiles_representative_48_member_product_with_all_write_types() -> None:
    libraries = {
        "shared": _library(
            {
                "defaults": {
                    "set": [
                        {
                            "step": "feature.step",
                            "parameter": "lag.value",
                            "value": 5,
                        }
                    ],
                    "execution_population": "all.subjects",
                    "manifest_bindings": [
                        {
                            "step": "model.step",
                            "role": "analysis.population",
                            "manifest": "included.subjects",
                        }
                    ],
                },
                "linear": {
                    "set": [
                        {
                            "step": "model.step",
                            "parameter": "method.name",
                            "value": "linear",
                        }
                    ]
                },
                "robust": {
                    "set": [
                        {
                            "step": "model.step",
                            "parameter": "method.name",
                            "value": "robust",
                        }
                    ]
                },
            }
        )
    }
    specification = {
        "schema": "nipact/specification-set/v1",
        "specification_set": {"key": "family-48"},
        "libraries": ["shared"],
        "fixed": {
            "workflow": "analysis.workflow",
            "fragments": [{"library": "shared", "fragment": "defaults"}],
            "target": {"step": "summary.step", "output": "estimate.out"},
            "results": {
                "estimate": {"step": "summary.step", "output": "estimate.out"},
                "diagnostics": {
                    "step": "summary.step",
                    "output": "diagnostics.out",
                },
            },
        },
        "dimensions": {
            "feature.space": {
                "set": {"step": "feature.step", "parameter": "atlas.name"},
                "values": ["atlas-1", "atlas-2", "atlas-3", "atlas-4"],
            },
            "method": {
                "choices": {
                    "linear": {
                        "fragments": [{"library": "shared", "fragment": "linear"}]
                    },
                    "robust": {
                        "fragments": [{"library": "shared", "fragment": "robust"}]
                    },
                }
            },
            "window": {
                "set": {"step": "feature.step", "parameter": "window.size"},
                "integer_range": {
                    "start": 30,
                    "stop": 55,
                    "step": 5,
                    "endpoint": "inclusive",
                },
            },
        },
        "combine": {"product": ["window", "feature.space", "method"]},
        "exclude": [],
        "expected_counts": {"candidates": 48, "included": 48, "excluded": 0},
    }

    compiled = compile_specification(specification, libraries)

    assert compiled.set_key == "family-48"
    assert len(compiled.members) == 48
    assert compiled.members[0].member_key == "member-000001"
    assert compiled.members[-1].member_key == "member-000048"
    assert compiled.equivalent_member_groups == ()
    assert tuple(item.name for item in compiled.members[0].decision_coordinates) == (
        "feature.space",
        "method",
        "window",
    )
    assert {type(write) for write in compiled.members[0].writes} == {
        ParameterWrite,
        ExecutionPopulationWrite,
        ManifestBindingWrite,
        TargetWrite,
        ResultWrite,
    }
    assert sum(member.disposition == "included" for member in compiled.members) == 48


def test_explicit_roster_can_reproduce_compact_provisional_members() -> None:
    compact = _base_spec(values=[2, 1])
    compact_result = compile_specification(compact, {})
    explicit = {
        "schema": "nipact/specification-set/v1",
        "specification_set": {"key": "compact-demo"},
        "libraries": [],
        "fixed": deepcopy(compact["fixed"]),
        "members": [
            {
                "key": "member-000002",
                "decision_coordinates": {"alpha": 2},
                "disposition": "included",
                "set": [
                    {"step": "model.step", "parameter": "alpha.value", "value": 2}
                ],
            },
            {
                "key": "member-000001",
                "decision_coordinates": {"alpha": 1},
                "disposition": "included",
                "set": [
                    {"step": "model.step", "parameter": "alpha.value", "value": 1}
                ],
            },
        ],
        "expected_counts": {"candidates": 2, "included": 2, "excluded": 0},
    }

    assert compile_specification(explicit, {}) == compact_result


@pytest.mark.parametrize(
    ("range_payload", "expected"),
    [
        ({"start": 1, "stop": 5, "step": 2, "endpoint": "inclusive"}, [1, 3, 5]),
        ({"start": 1, "stop": 5, "step": 2, "endpoint": "exclusive"}, [1, 3]),
        ({"start": 5, "stop": 1, "step": -2, "endpoint": "inclusive"}, [5, 3, 1]),
        ({"start": 5, "stop": 1, "step": -2, "endpoint": "exclusive"}, [5, 3]),
        ({"start": 2, "stop": 2, "step": 1, "endpoint": "inclusive"}, [2]),
    ],
)
def test_integer_range_has_explicit_direction_and_endpoint(
    range_payload: dict[str, Any], expected: list[int]
) -> None:
    specification = _base_spec()
    specification["dimensions"]["alpha"] = {
        "set": {"step": "model.step", "parameter": "alpha.value"},
        "integer_range": range_payload,
    }
    specification["expected_counts"] = {
        "candidates": len(expected),
        "included": len(expected),
        "excluded": 0,
    }

    assert _parameter_values(compile_specification(specification, {})) == sorted(expected)


@pytest.mark.parametrize(
    "range_payload",
    [
        {"start": 1, "stop": 5, "step": 0, "endpoint": "inclusive"},
        {"start": 5, "stop": 1, "step": 1, "endpoint": "inclusive"},
        {"start": 1, "stop": 5, "step": -1, "endpoint": "inclusive"},
        {"start": 1, "stop": 6, "step": 2, "endpoint": "inclusive"},
        {"start": 1.0, "stop": 5, "step": 1, "endpoint": "inclusive"},
        {"start": True, "stop": 5, "step": 1, "endpoint": "inclusive"},
        {"start": 1, "stop": 5, "step": 1, "endpoint": "closed"},
        {"start": 1, "stop": 5, "step": 1, "endpoint": []},
        {"start": 2, "stop": 2, "step": 1, "endpoint": "exclusive"},
    ],
)
def test_integer_range_rejects_implicit_or_empty_semantics(
    range_payload: dict[str, Any],
) -> None:
    specification = _base_spec()
    specification["dimensions"]["alpha"] = {
        "set": {"step": "model.step", "parameter": "alpha.value"},
        "integer_range": range_payload,
    }

    with pytest.raises(ValidationError):
        compile_specification(specification, {})


def test_type_sensitive_values_are_distinct_and_deterministically_ordered() -> None:
    specification = _base_spec(
        values=[1.0, "1", 1, True, None, -0.5, 0.25, [1], {"b": 2, "a": 1}]
    )

    compiled = compile_specification(specification, {})

    assert [member.decision_coordinates[0].value for member in compiled.members] == [
        None,
        True,
        1,
        -0.5,
        0.25,
        1.0,
        "1",
        [1],
        {"a": 1, "b": 2},
    ]
    duplicate = _base_spec(values=[True, True])
    with pytest.raises(ValidationError, match="duplicate values"):
        compile_specification(duplicate, {})

    boolean = compile_specification(_base_spec(values=[True]), {})
    integer = compile_specification(_base_spec(values=[1]), {})
    floating = compile_specification(_base_spec(values=[1.0]), {})
    assert boolean != integer
    assert integer != floating


def test_library_references_are_qualified_flat_and_fully_validated() -> None:
    libraries = {
        "a": _library({"same": {"execution_population": "population-a"}}),
        "b": _library({"same": {"workflow": "base.workflow"}}),
    }
    specification = _base_spec()
    specification["libraries"] = ["b", "a"]
    specification["fixed"].pop("workflow")
    specification["fixed"]["fragments"] = [
        {"library": "a", "fragment": "same"},
        {"library": "b", "fragment": "same"},
    ]

    compiled = compile_specification(specification, libraries)

    assert len(compiled.members) == 2
    assert any(
        isinstance(write, ExecutionPopulationWrite)
        for write in compiled.members[0].writes
    )

    recursive = deepcopy(libraries)
    recursive["a"]["fragments"]["unused-invalid"] = {
        "fragments": [{"library": "b", "fragment": "same"}]
    }
    with pytest.raises(ValidationError, match="unknown field: fragments"):
        compile_specification(specification, recursive)

    with pytest.raises(ValidationError, match="unselected"):
        compile_specification(specification, {**libraries, "extra": _library({})})
    with pytest.raises(ValidationError, match="missing selected"):
        compile_specification(specification, {"a": libraries["a"]})

    unknown_reference = deepcopy(specification)
    unknown_reference["fixed"]["fragments"][0]["fragment"] = "missing"
    with pytest.raises(ValidationError, match="unknown or unselected fragment"):
        compile_specification(unknown_reference, libraries)

    duplicate_reference = deepcopy(specification)
    duplicate_reference["fixed"]["fragments"].append(
        {"library": "a", "fragment": "same"}
    )
    with pytest.raises(ValidationError, match="duplicate fragment references"):
        compile_specification(duplicate_reference, libraries)


def test_fragment_and_declaration_order_and_factoring_are_nonsemantic() -> None:
    direct = _base_spec(values=[2, 1])
    direct["fixed"]["set"] = [
        {"step": "model.step", "parameter": "beta", "value": {"z": 2, "a": 1}},
        {"step": "model.step", "parameter": "gamma", "value": "fixed"},
    ]
    factored = deepcopy(direct)
    factored["libraries"] = ["z-library", "a-library"]
    factored["fixed"]["set"] = []
    factored["fixed"]["fragments"] = [
        {"library": "z-library", "fragment": "gamma"},
        {"library": "a-library", "fragment": "beta"},
    ]
    factored["combine"]["product"] = list(reversed(factored["combine"]["product"]))
    libraries = {
        "z-library": _library(
            {
                "gamma": {
                    "set": [
                        {
                            "parameter": "gamma",
                            "value": "fixed",
                            "step": "model.step",
                        }
                    ]
                }
            }
        ),
        "a-library": _library(
            {
                "beta": {
                    "set": [
                        {
                            "value": {"a": 1, "z": 2},
                            "step": "model.step",
                            "parameter": "beta",
                        }
                    ]
                }
            }
        ),
    }

    assert compile_specification(factored, libraries) == compile_specification(direct, {})


def test_named_choices_preserve_equivalent_denominator_members() -> None:
    specification = _base_spec(values=[1])
    specification["dimensions"] = {
        "method": {"choices": {"second": {}, "first": {}}}
    }
    specification["combine"] = {"product": ["method"]}
    specification["expected_counts"] = {
        "candidates": 2,
        "included": 2,
        "excluded": 0,
    }

    compiled = compile_specification(specification, {})

    assert len(compiled.members) == 2
    assert compiled.equivalent_member_groups == (
        ("member-000001", "member-000002"),
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda spec: spec.update({"unknown": 1}), "unknown field"),
        (lambda spec: spec["specification_set"].update({"label": "x"}), "unknown field"),
        (lambda spec: spec["combine"].update({"zip": []}), "unknown field"),
        (lambda spec: spec["dimensions"]["alpha"].update({"default": 1}), "unknown field"),
        (
            lambda spec: spec["dimensions"]["alpha"]["set"].update({"path": "x"}),
            "unknown field",
        ),
        (lambda spec: spec["fixed"].update({"metadata": {}}), "unknown field"),
        (lambda spec: spec["fixed"]["target"].update({"index": 0}), "unknown field"),
        (
            lambda spec: spec["fixed"]["results"]["estimate"].update({"index": 0}),
            "unknown field",
        ),
        (lambda spec: spec["expected_counts"].update({"total": 2}), "unknown field"),
        (lambda spec: spec.update({"members": []}), "mix explicit and product"),
    ],
)
def test_unknown_fields_and_mode_mixtures_fail_closed(mutation: Any, message: str) -> None:
    specification = _base_spec()
    mutation(specification)

    with pytest.raises(ValidationError, match=message):
        compile_specification(specification, {})


@pytest.mark.parametrize(
    "case",
    [
        "library-envelope",
        "fragment-reference",
        "set-item",
        "manifest-binding",
        "integer-range",
        "exclusion-rule",
    ],
)
def test_nested_authoring_records_reject_unknown_fields(case: str) -> None:
    specification = _base_spec(values=[1])
    libraries: dict[str, Any] = {}
    if case == "library-envelope":
        specification["libraries"] = ["shared"]
        libraries = {"shared": _library({})}
        libraries["shared"]["unknown"] = True
    elif case == "fragment-reference":
        specification["libraries"] = ["shared"]
        specification["fixed"]["fragments"] = [
            {"library": "shared", "fragment": "defaults", "unknown": True}
        ]
        libraries = {"shared": _library({"defaults": {}})}
    elif case == "set-item":
        specification["fixed"]["set"] = [
            {
                "step": "model.step",
                "parameter": "beta",
                "value": 1,
                "unknown": True,
            }
        ]
    elif case == "manifest-binding":
        specification["fixed"]["manifest_bindings"] = [
            {
                "step": "model.step",
                "role": "population",
                "manifest": "included",
                "unknown": True,
            }
        ]
    elif case == "integer-range":
        specification["dimensions"]["alpha"] = {
            "set": {"step": "model.step", "parameter": "alpha.value"},
            "integer_range": {
                "start": 1,
                "stop": 2,
                "step": 1,
                "endpoint": "inclusive",
                "unknown": True,
            },
        }
    else:
        specification["exclude"] = [
            {"when": {"alpha": 1}, "reason": "Reviewed.", "unknown": True}
        ]

    with pytest.raises(ValidationError, match="unknown field"):
        compile_specification(specification, libraries)


def test_schema_and_product_membership_are_exact() -> None:
    invalid_specs = []
    wrong_schema = _base_spec()
    wrong_schema["schema"] = "nipact/specification-set/v2"
    invalid_specs.append(wrong_schema)
    repeated_axis = _base_spec()
    repeated_axis["combine"]["product"] = ["alpha", "alpha"]
    invalid_specs.append(repeated_axis)
    unknown_axis = _base_spec()
    unknown_axis["combine"]["product"] = ["alpha", "unknown"]
    invalid_specs.append(unknown_axis)
    empty_dimensions = _base_spec()
    empty_dimensions["dimensions"] = {}
    empty_dimensions["combine"]["product"] = []
    invalid_specs.append(empty_dimensions)

    for specification in invalid_specs:
        with pytest.raises(ValidationError):
            compile_specification(specification, {})

    selected = _base_spec()
    selected["libraries"] = ["shared"]
    with pytest.raises(ValidationError, match="schema"):
        compile_specification(
            selected,
            {"shared": {"schema": "nipact/specification-library/v2", "fragments": {}}},
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda spec: spec["fixed"].pop("workflow"),
        lambda spec: spec["fixed"].pop("target"),
        lambda spec: spec["fixed"].pop("results"),
        lambda spec: spec["fixed"]["results"].update(
            {"other": {"step": "other.step", "output": "other"}}
        ),
        lambda spec: spec["fixed"]["results"].update(
            {"same-port": {"step": "summary.step", "output": "estimate.out"}}
        ),
        lambda spec: spec["fixed"].update(
            {"results": {"other": {"step": "summary.step", "output": "other"}}}
        ),
    ],
)
def test_every_candidate_requires_one_selector_target_and_sibling_result_contract(
    mutation: Any,
) -> None:
    specification = _base_spec()
    mutation(specification)

    with pytest.raises(ValidationError):
        compile_specification(specification, {})


@pytest.mark.parametrize(
    "fixed_addition",
    [
        {"set": [{"step": "model.step", "parameter": "alpha.value", "value": 1}]},
        {"execution_population": "first"},
        {
            "manifest_bindings": [
                {"step": "model.step", "role": "population", "manifest": "first"}
            ]
        },
    ],
)
def test_duplicate_destinations_fail_independent_of_literal_value(
    fixed_addition: dict[str, Any],
) -> None:
    specification = _base_spec(values=[1])
    specification["fixed"].update(fixed_addition)
    if "execution_population" in fixed_addition:
        specification["dimensions"] = {
            "case": {
                "choices": {
                    "one": {"execution_population": "first"},
                    "two": {"execution_population": "second"},
                }
            }
        }
        specification["combine"] = {"product": ["case"]}
        specification["expected_counts"] = {
            "candidates": 2,
            "included": 2,
            "excluded": 0,
        }
    elif "manifest_bindings" in fixed_addition:
        specification["dimensions"] = {
            "case": {
                "choices": {
                    "one": {
                        "manifest_bindings": [
                            {
                                "step": "model.step",
                                "role": "population",
                                "manifest": "second",
                            }
                        ]
                    }
                }
            }
        }
        specification["combine"] = {"product": ["case"]}

    with pytest.raises(ValidationError, match="writes destination more than once"):
        compile_specification(specification, {})


def test_workflow_target_and_result_role_conflicts_are_never_overrides() -> None:
    conflict_cases = []
    workflow = _base_spec(values=[1])
    workflow["dimensions"] = {
        "case": {"choices": {"one": {"workflow": "base.workflow"}}}
    }
    workflow["combine"] = {"product": ["case"]}
    conflict_cases.append(workflow)

    target = _base_spec(values=[1])
    target["dimensions"] = {
        "case": {
            "choices": {
                "one": {"target": {"step": "summary.step", "output": "other"}}
            }
        }
    }
    target["combine"] = {"product": ["case"]}
    conflict_cases.append(target)

    result = _base_spec(values=[1])
    result["dimensions"] = {
        "case": {
            "choices": {
                "one": {
                    "results": {
                        "estimate": {"step": "summary.step", "output": "other"}
                    }
                }
            }
        }
    }
    result["combine"] = {"product": ["case"]}
    conflict_cases.append(result)

    for specification in conflict_cases:
        with pytest.raises(ValidationError):
            compile_specification(specification, {})


def test_candidate_result_role_sets_must_match() -> None:
    specification = _base_spec(values=[1])
    specification["dimensions"] = {
        "case": {
            "choices": {
                "plain": {},
                "diagnostic": {
                    "results": {
                        "diagnostics": {"step": "summary.step", "output": "diagnostics"}
                    }
                },
            }
        }
    }
    specification["combine"] = {"product": ["case"]}
    specification["expected_counts"] = {
        "candidates": 2,
        "included": 2,
        "excluded": 0,
    }

    with pytest.raises(ValidationError, match="same result-role names"):
        compile_specification(specification, {})


def test_exact_exclusions_keep_denominator_rows_and_are_order_independent() -> None:
    specification = _base_spec(values=[True, 1, 2])
    specification["exclude"] = [
        {"when": {"alpha": 2}, "reason": "Outside the declared support design."},
        {"when": {"alpha": True}, "reason": "Boolean sensitivity case."},
    ]
    specification["expected_counts"] = {
        "candidates": 3,
        "included": 1,
        "excluded": 2,
    }

    compiled = compile_specification(specification, {})
    reversed_rules = deepcopy(specification)
    reversed_rules["exclude"].reverse()

    assert compile_specification(reversed_rules, {}) == compiled
    assert [member.disposition for member in compiled.members] == [
        "excluded",
        "included",
        "excluded",
    ]


def test_exclusions_use_multi_axis_conjunctions_and_reject_partial_overlap() -> None:
    specification = _base_spec(values=[1, 2])
    specification["dimensions"]["method"] = {
        "choices": {"linear": {}, "robust": {}}
    }
    specification["combine"] = {"product": ["method", "alpha"]}
    specification["exclude"] = [
        {
            "when": {"alpha": 2, "method": "robust"},
            "reason": "One prospectively unsupported combination.",
        }
    ]
    specification["expected_counts"] = {
        "candidates": 4,
        "included": 3,
        "excluded": 1,
    }

    compiled = compile_specification(specification, {})
    assert sum(member.disposition == "excluded" for member in compiled.members) == 1

    overlapping = deepcopy(specification)
    overlapping["exclude"].append(
        {"when": {"method": "robust"}, "reason": "Overlaps the exact rule."}
    )
    overlapping["expected_counts"] = {
        "candidates": 4,
        "included": 2,
        "excluded": 2,
    }
    with pytest.raises(ValidationError, match="multiple exclusions"):
        compile_specification(overlapping, {})


@pytest.mark.parametrize(
    "mutate",
    [
        lambda spec: spec["exclude"].append(
            {"when": {"unknown": 1}, "reason": "Unknown axis."}
        ),
        lambda spec: spec["exclude"].append(
            {"when": {"alpha": 99}, "reason": "Unknown value."}
        ),
        lambda spec: spec["exclude"].append({"when": {}, "reason": "Empty rule."}),
        lambda spec: spec["exclude"].append(
            {"when": {"alpha": 1}, "reason": " leading whitespace"}
        ),
        lambda spec: spec["exclude"].extend(
            [
                {"when": {"alpha": 1}, "reason": "First."},
                {"when": {"alpha": 1}, "reason": "Second."},
            ]
        ),
        lambda spec: spec["expected_counts"].update({"candidates": 3, "included": 3}),
        lambda spec: spec["expected_counts"].update({"candidates": True}),
    ],
)
def test_exclusion_and_expected_count_guards_fail_closed(mutate: Any) -> None:
    specification = _base_spec(values=[1, 2])
    mutate(specification)

    with pytest.raises(ValidationError):
        compile_specification(specification, {})


def test_explicit_dispositions_keys_coordinates_and_role_sets_are_validated() -> None:
    specification = {
        "schema": "nipact/specification-set/v1",
        "specification_set": {"key": "explicit"},
        "libraries": [],
        "fixed": deepcopy(_base_spec()["fixed"]),
        "members": [
            {
                "key": "included",
                "decision_coordinates": {"case": "included"},
                "disposition": "included",
            },
            {
                "key": "excluded",
                "decision_coordinates": {"case": "excluded"},
                "disposition": "excluded",
                "reason": "Prospectively excluded.",
            },
        ],
        "expected_counts": {"candidates": 2, "included": 1, "excluded": 1},
    }
    compiled = compile_specification(specification, {})
    assert [member.member_key for member in compiled.members] == ["excluded", "included"]

    invalid_cases = []
    missing_reason = deepcopy(specification)
    missing_reason["members"][1].pop("reason")
    invalid_cases.append(missing_reason)
    included_reason = deepcopy(specification)
    included_reason["members"][0]["reason"] = "Not allowed."
    invalid_cases.append(included_reason)
    duplicate_key = deepcopy(specification)
    duplicate_key["members"][1]["key"] = "included"
    invalid_cases.append(duplicate_key)
    duplicate_coordinates = deepcopy(specification)
    duplicate_coordinates["members"][1]["decision_coordinates"] = {"case": "included"}
    invalid_cases.append(duplicate_coordinates)
    empty_coordinates = deepcopy(specification)
    empty_coordinates["members"][0]["decision_coordinates"] = {}
    invalid_cases.append(empty_coordinates)
    unknown_field = deepcopy(specification)
    unknown_field["members"][0]["label"] = "unknown"
    invalid_cases.append(unknown_field)

    for invalid in invalid_cases:
        with pytest.raises(ValidationError):
            compile_specification(invalid, {})


def test_inputs_are_unchanged_after_success_rejection_and_output_mutation() -> None:
    specification = _base_spec(values=[{"nested": [1, 2]}, {"nested": [3]}])
    original = deepcopy(specification)

    compiled = compile_specification(specification, {})

    assert specification == original
    coordinate_value = compiled.members[0].decision_coordinates[0].value
    assert isinstance(coordinate_value, dict)
    coordinate_value["nested"].append(99)
    parameter_value = next(
        write.value
        for write in compiled.members[0].writes
        if isinstance(write, ParameterWrite)
    )
    assert isinstance(parameter_value, dict)
    parameter_value["nested"].append(100)
    assert specification == original

    rejected = _base_spec(values=[1])
    rejected["fixed"]["set"] = [
        {"step": "model.step", "parameter": "alpha.value", "value": 1}
    ]
    rejected_original = deepcopy(rejected)
    with pytest.raises(ValidationError):
        compile_specification(rejected, {})
    assert rejected == rejected_original


def test_compiler_does_not_access_filesystem_or_import_execution_modules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("pure compiler attempted external access")

    monkeypatch.setattr(Path, "read_text", fail)
    monkeypatch.setattr(Path, "resolve", fail)
    monkeypatch.setattr("builtins.open", fail)
    monkeypatch.setattr("importlib.import_module", fail)

    assert len(compile_specification(_base_spec(), {}).members) == 2


def test_output_uses_structured_destinations_without_splitting_dotted_names() -> None:
    compiled = compile_specification(_base_spec(values=[1]), {})
    member = compiled.members[0]

    assert member.decision_coordinates == (DecisionCoordinate(name="alpha", value=1),)
    assert ParameterWrite("model.step", "alpha.value", 1) in member.writes
    assert TargetWrite("summary.step", "estimate.out") in member.writes
    assert ResultWrite("estimate", "summary.step", "estimate.out") in member.writes
    assert ProvisionalMember.__hash__ is None
    assert ProvisionalCompilation.__hash__ is None
