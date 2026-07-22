from dataclasses import fields, replace
from datetime import date
import hashlib
import json

import pytest

from nipact.errors import ValidationError
from nipact.manifest import MANIFEST_VALUE_SCHEMA
from nipact.source_authority import LogicalSourceCoordinate
from nipact.projection import (
    IDENTITY_CONTRACT_VERSION,
    OUTPUT_CONTRACT_VERSION,
    RUNNER_CONTRACT_VERSION,
    CollectionBinding,
    CollectionBindingPlan,
    OutputContract,
    ProjectionBindingPlan,
    RegisteredSourceBinding,
    RegisteredSourceSnapshot,
    RequestBundleProjectionV3,
    RequestBundleProjectionPlanV3,
    ResolvedRequestBundleProjectionV3,
    RequestedOutputCoordinate,
    SiblingOutput,
    SourceBindingPlan,
    StepContract,
    UnresolvedRequestBundleProjection,
    UpstreamRequestedOutputBinding,
    UpstreamRequestedOutputBindingPlan,
    canonicalize_request_bundle_projection,
    resolve_request_bundle_projection_plan,
    validate_stored_request_bundle_projection_v3,
)


def _source_binding(
    *,
    role: str = "t1w",
    context: str = "clms",
    scope: str = "entity",
    source_name: str = "t1w",
    entity_id: str | None = "aac_027_m00",
) -> RegisteredSourceBinding:
    return RegisteredSourceBinding(
        role=role,
        source_coordinate=LogicalSourceCoordinate(
            context=context,
            scope=scope,
            source_name=source_name,
            entity_id=entity_id,
        ),
        registered_content_digest="a" * 64,
        registered_file_size=123,
        declared_extension=".nii.gz",
    )


def _projection(
    *,
    address: str = "aac_027_m00",
    canonical_parameters: object | None = None,
    role_labelled_bindings: tuple[object, ...] | None = None,
    sibling_outputs: tuple[SiblingOutput, ...] | None = None,
) -> RequestBundleProjectionV3:
    return RequestBundleProjectionV3(
        identity_contract_version=IDENTITY_CONTRACT_VERSION,
        namespace="clms",
        step_contract=StepContract(
            step_contract_id="t1_synthseg",
            step_contract_version="1",
            callable_ref="clms.steps:t1_synthseg",
            runner_contract_version=RUNNER_CONTRACT_VERSION,
        ),
        address=address,
        canonical_parameters=(
            {"threads": 1, "threshold": 0.5}
            if canonical_parameters is None
            else canonical_parameters
        ),
        role_labelled_bindings=(
            (_source_binding(),)
            if role_labelled_bindings is None
            else role_labelled_bindings
        ),
        result_affecting_settings={},
        determinism_contract="deterministic",
        output_contract=OutputContract(
            output_contract_version=OUTPUT_CONTRACT_VERSION,
            sibling_outputs=(
                (
                    SiblingOutput(
                        output_name="segmentation",
                        declared_extension=".nii.gz",
                    ),
                )
                if sibling_outputs is None
                else sibling_outputs
            ),
        ),
    )


def _upstream_binding(
    *,
    role: str,
    address: str,
    output_name: str = "segmentation",
) -> UpstreamRequestedOutputBinding:
    return UpstreamRequestedOutputBinding(
        role=role,
        upstream_request_bundle_digest=_resolved(
            _projection(address=address)
        ).request_bundle_digest,
        output_name=output_name,
    )


def _resolved(
    projection: RequestBundleProjectionV3,
) -> ResolvedRequestBundleProjectionV3:
    return canonicalize_request_bundle_projection(projection)


def _canonical_json(projection: RequestBundleProjectionV3) -> str:
    return _resolved(projection).canonical_json


def _projection_plan(
    *binding_plans: ProjectionBindingPlan,
    address: str = "aac_027_m00",
    parameters: object | None = None,
) -> RequestBundleProjectionPlanV3:
    return RequestBundleProjectionPlanV3(
        identity_contract_version=IDENTITY_CONTRACT_VERSION,
        namespace="clms",
        step_contract=StepContract(
            step_contract_id="t1_synthseg",
            step_contract_version="1",
            callable_ref="clms.steps:t1_synthseg",
            runner_contract_version=RUNNER_CONTRACT_VERSION,
        ),
        address=address,
        canonical_parameters=(
            {"threads": 1} if parameters is None else parameters
        ),
        role_labelled_binding_plans=binding_plans,
        result_affecting_settings={},
        determinism_contract="deterministic",
        output_contract=OutputContract(
            output_contract_version=OUTPUT_CONTRACT_VERSION,
            sibling_outputs=(
                SiblingOutput("segmentation", ".nii.gz"),
                SiblingOutput("volumes", ".csv"),
            ),
        ),
    )


def test_canonical_projection_matches_golden_json() -> None:
    projection = _projection(
        canonical_parameters={"label": "café", "threshold": 0.5, "enabled": True}
    )

    resolved = _resolved(projection)

    assert resolved.canonical_json == (
        '{"address":"aac_027_m00","canonical_parameters":{"enabled":true,'
        '"label":"café","threshold":0.5},"determinism_contract":"deterministic",'
        '"identity_contract_version":3,"namespace":"clms","output_contract":'
        '{"output_contract_version":1,"sibling_outputs":[{"declared_extension":'
        '".nii.gz","output_name":"segmentation"}]},"result_affecting_settings":{},'
        '"role_labelled_bindings":[{"declared_extension":".nii.gz",'
        '"registered_content_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaa","registered_file_size":123,"role":"t1w",'
        '"source_coordinate":{"context":"clms","entity_id":"aac_027_m00",'
        '"scope":"entity","source_name":"t1w"}}],"step_contract":{"callable_ref":'
        '"clms.steps:t1_synthseg","runner_contract_version":"2",'
        '"step_contract_id":"t1_synthseg","step_contract_version":"1"}}'
    )
    assert (
        resolved.request_bundle_digest
        == "dc432e8256713c5a82dd53e11eb5c212963cd1ee73949336fffe723276d8a2e7"
    )


def test_projection_contract_versions_are_v3() -> None:
    assert IDENTITY_CONTRACT_VERSION == 3
    assert RUNNER_CONTRACT_VERSION == "2"


def test_canonical_projection_is_invariant_to_object_and_declared_set_order() -> None:
    first_member = _upstream_binding(role="images", address="subject-a")
    second_member = _upstream_binding(role="images", address="subject-b")
    first = _projection(
        canonical_parameters={"outer": {"a": 1, "b": 2}},
        role_labelled_bindings=(
            _source_binding(role="mask", source_name="mask"),
            CollectionBinding(
                role="images",
                collection_semantics="coordinate_set_v1",
                manifest_value_schema=MANIFEST_VALUE_SCHEMA,
                manifest_digest="b" * 64,
                members=(second_member, first_member),
            ),
        ),
        sibling_outputs=(
            SiblingOutput(output_name="table", declared_extension=".csv"),
            SiblingOutput(output_name="image", declared_extension=".nii.gz"),
        ),
    )
    second = _projection(
        canonical_parameters={"outer": {"b": 2, "a": 1}},
        role_labelled_bindings=(
            CollectionBinding(
                role="images",
                collection_semantics="coordinate_set_v1",
                manifest_value_schema=MANIFEST_VALUE_SCHEMA,
                manifest_digest="b" * 64,
                members=(first_member, second_member),
            ),
            _source_binding(role="mask", source_name="mask"),
        ),
        sibling_outputs=(
            SiblingOutput(output_name="image", declared_extension=".nii.gz"),
            SiblingOutput(output_name="table", declared_extension=".csv"),
        ),
    )

    assert _canonical_json(first) == _canonical_json(second)


def test_canonical_projection_preserves_declared_sequence_order() -> None:
    first = _projection(canonical_parameters={"labels": ["left", "right"]})
    second = _projection(canonical_parameters={"labels": ["right", "left"]})

    assert _canonical_json(first) != _canonical_json(second)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, '"value":true'),
        (1, '"value":1'),
        (1.0, '"value":1.0'),
        (0.0, '"value":0.0'),
        (-0.0, '"value":-0.0'),
    ],
)
def test_canonical_projection_preserves_scalar_types(
    value: object,
    expected: str,
) -> None:
    serialized = _canonical_json(
        _projection(canonical_parameters={"value": value})
    )

    assert expected in serialized


def test_canonical_projection_serializes_collection_without_manifest() -> None:
    first = _upstream_binding(role="images", address="subject-a")
    second = _upstream_binding(role="images", address="subject-b")
    binding = CollectionBinding(
        role="images",
        collection_semantics="coordinate_set_v1",
        manifest_digest=None,
        members=(
            second,
            first,
        ),
    )

    serialized = _canonical_json(
        _projection(role_labelled_bindings=(binding,))
    )

    assert '"manifest_digest":null' in serialized
    assert '"manifest_value_schema":null' in serialized
    payload = json.loads(serialized)
    members = payload["role_labelled_bindings"][0]["members"]
    assert [member["upstream_request_bundle_digest"] for member in members] == sorted(
        [
            first.upstream_request_bundle_digest,
            second.upstream_request_bundle_digest,
        ]
    )


@pytest.mark.parametrize(
    ("manifest_value_schema", "manifest_digest"),
    [
        (MANIFEST_VALUE_SCHEMA, None),
        (None, "b" * 64),
    ],
)
def test_collection_manifest_reference_requires_null_parity(
    manifest_value_schema: str | None,
    manifest_digest: str | None,
) -> None:
    binding = CollectionBinding(
        role="images",
        collection_semantics="coordinate_set_v1",
        manifest_value_schema=manifest_value_schema,
        manifest_digest=manifest_digest,
        members=(_upstream_binding(role="images", address="subject-a"),),
    )

    with pytest.raises(ValidationError, match="must both be null or both be present"):
        _canonical_json(_projection(role_labelled_bindings=(binding,)))


def test_collection_manifest_reference_rejects_unknown_schema() -> None:
    binding = CollectionBinding(
        role="images",
        collection_semantics="coordinate_set_v1",
        manifest_value_schema="entity_set_v2",
        manifest_digest="b" * 64,
        members=(_upstream_binding(role="images", address="subject-a"),),
    )

    with pytest.raises(ValidationError, match="must be 'entity_set_v1'"):
        _canonical_json(_projection(role_labelled_bindings=(binding,)))


def test_binding_role_is_identity_bearing() -> None:
    first = _projection(role_labelled_bindings=(_source_binding(role="image"),))
    second = _projection(role_labelled_bindings=(_source_binding(role="mask"),))

    assert _canonical_json(first) != _canonical_json(second)


def test_registered_source_binding_excludes_occurrence_path() -> None:
    payload = json.loads(_canonical_json(_projection()))

    assert payload["role_labelled_bindings"][0]["source_coordinate"] == {
        "context": "clms",
        "scope": "entity",
        "source_name": "t1w",
        "entity_id": "aac_027_m00",
    }


def test_canonical_projection_rejects_duplicate_top_level_binding_roles() -> None:
    with pytest.raises(ValidationError, match="duplicate roles"):
        _canonical_json(
            _projection(
                role_labelled_bindings=(
                    _source_binding(role="image", source_name="first"),
                    _source_binding(role="image", source_name="second"),
                )
            )
        )


def test_canonical_projection_rejects_duplicate_collection_members() -> None:
    member = _upstream_binding(role="images", address="subject-a")
    with pytest.raises(ValidationError, match="duplicate requested outputs"):
        _canonical_json(
            _projection(
                role_labelled_bindings=(
                    CollectionBinding(
                        role="images",
                        collection_semantics="coordinate_set_v1",
                        manifest_digest=None,
                        members=(member, member),
                    ),
                )
            )
        )


def test_canonical_projection_rejects_duplicate_sibling_output_names() -> None:
    with pytest.raises(ValidationError, match="duplicate output names"):
        _canonical_json(
            _projection(
                sibling_outputs=(
                    SiblingOutput("image", ".nii.gz"),
                    SiblingOutput("image", ".json"),
                )
            )
        )


@pytest.mark.parametrize(
    "digest",
    [
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        f"{'a' * 63} ",
    ],
)
@pytest.mark.parametrize("digest_field", ["upstream", "source", "manifest"])
def test_projection_rejects_invalid_full_digests(
    digest: str,
    digest_field: str,
) -> None:
    if digest_field == "upstream":
        binding: object = replace(
            _upstream_binding(role="image", address="subject-a"),
            upstream_request_bundle_digest=digest,
        )
    elif digest_field == "source":
        binding = replace(_source_binding(), registered_content_digest=digest)
    else:
        binding = CollectionBinding(
            role="images",
            collection_semantics="coordinate_set_v1",
            manifest_value_schema=MANIFEST_VALUE_SCHEMA,
            manifest_digest=digest,
            members=(_upstream_binding(role="images", address="subject-a"),),
        )

    with pytest.raises(ValidationError, match="64-character"):
        _canonical_json(_projection(role_labelled_bindings=(binding,)))


def _identity_matrix_projection(change: str) -> RequestBundleProjectionV3:
    first_binding = _source_binding(role="image", source_name="image")
    member = _upstream_binding(role="mask", address="subject-a")
    second_binding = CollectionBinding(
        role="mask",
        collection_semantics="coordinate_set_v1",
        manifest_value_schema=MANIFEST_VALUE_SCHEMA,
        manifest_digest="c" * 64,
        members=(member,),
    )
    base = _projection(
        canonical_parameters={"alpha": 1, "beta": 2},
        role_labelled_bindings=(first_binding, second_binding),
        sibling_outputs=(
            SiblingOutput("image", ".nii.gz"),
            SiblingOutput("metadata", ".json"),
        ),
    )
    if change == "namespace":
        return replace(base, namespace="other")
    if change == "step_contract_id":
        return replace(
            base,
            step_contract=replace(base.step_contract, step_contract_id="other"),
        )
    if change == "step_contract_version":
        return replace(
            base,
            step_contract=replace(base.step_contract, step_contract_version="2"),
        )
    if change == "callable_ref":
        return replace(
            base,
            step_contract=replace(base.step_contract, callable_ref="other:callable"),
        )
    if change == "runner_contract_version":
        return replace(
            base,
            step_contract=replace(base.step_contract, runner_contract_version="3"),
        )
    if change == "address":
        return replace(base, address="other")
    if change == "parameters":
        return replace(base, canonical_parameters={"alpha": 2, "beta": 2})
    if change == "source_digest":
        changed_binding = replace(first_binding, registered_content_digest="b" * 64)
        return replace(
            base,
            role_labelled_bindings=(changed_binding, second_binding),
        )
    if change == "source_context":
        changed_binding = replace(
            first_binding,
            source_coordinate=replace(first_binding.source_coordinate, context="other"),
        )
        return replace(base, role_labelled_bindings=(changed_binding, second_binding))
    if change == "source_name":
        changed_binding = replace(
            first_binding,
            source_coordinate=replace(
                first_binding.source_coordinate,
                source_name="other",
            ),
        )
        return replace(base, role_labelled_bindings=(changed_binding, second_binding))
    if change == "source_size":
        changed_binding = replace(first_binding, registered_file_size=124)
        return replace(base, role_labelled_bindings=(changed_binding, second_binding))
    if change == "source_extension":
        changed_binding = replace(first_binding, declared_extension=".mgz")
        return replace(base, role_labelled_bindings=(changed_binding, second_binding))
    if change == "manifest_digest":
        changed_binding = replace(second_binding, manifest_digest="d" * 64)
        return replace(base, role_labelled_bindings=(first_binding, changed_binding))
    if change == "collection_semantics":
        changed_binding = replace(second_binding, collection_semantics="ordered_v1")
        return replace(base, role_labelled_bindings=(first_binding, changed_binding))
    if change == "upstream_bundle_digest":
        changed_member = replace(
            member,
            upstream_request_bundle_digest="d" * 64,
        )
        changed_binding = replace(second_binding, members=(changed_member,))
        return replace(base, role_labelled_bindings=(first_binding, changed_binding))
    if change == "settings":
        return replace(base, result_affecting_settings={"locale": "C"})
    if change == "determinism":
        return replace(base, determinism_contract="noncacheable")
    if change == "output_extension":
        return replace(
            base,
            output_contract=replace(
                base.output_contract,
                sibling_outputs=(
                    SiblingOutput("image", ".mgz"),
                    SiblingOutput("metadata", ".json"),
                ),
            ),
        )
    if change == "output_name":
        return replace(
            base,
            output_contract=replace(
                base.output_contract,
                sibling_outputs=(
                    SiblingOutput("other", ".nii.gz"),
                    SiblingOutput("metadata", ".json"),
                ),
            ),
        )
    if change == "binding_order":
        return replace(base, role_labelled_bindings=(second_binding, first_binding))
    if change == "sibling_order":
        return replace(
            base,
            output_contract=replace(
                base.output_contract,
                sibling_outputs=tuple(reversed(base.output_contract.sibling_outputs)),
            ),
        )
    if change == "object_key_order":
        return replace(base, canonical_parameters={"beta": 2, "alpha": 1})
    raise AssertionError(f"unknown identity-matrix change: {change}")


@pytest.mark.parametrize(
    ("change", "same_identity"),
    [
        ("namespace", False),
        ("step_contract_id", False),
        ("step_contract_version", False),
        ("callable_ref", False),
        ("runner_contract_version", False),
        ("address", False),
        ("parameters", False),
        ("source_context", False),
        ("source_name", False),
        ("source_digest", False),
        ("source_size", False),
        ("source_extension", False),
        ("manifest_digest", False),
        ("collection_semantics", False),
        ("upstream_bundle_digest", False),
        ("settings", False),
        ("determinism", False),
        ("output_name", False),
        ("output_extension", False),
        ("binding_order", True),
        ("sibling_order", True),
        ("object_key_order", True),
    ],
)
def test_projection_identity_inclusion_and_exclusion_matrix(
    change: str,
    same_identity: bool,
) -> None:
    baseline = _identity_matrix_projection("object_key_order")
    baseline = replace(baseline, canonical_parameters={"alpha": 1, "beta": 2})
    variant = _identity_matrix_projection(change)

    assert (_canonical_json(baseline) == _canonical_json(variant)) is (
        same_identity
    )


def test_projection_contract_excludes_operational_and_retrospective_facts() -> None:
    field_names = {field.name for field in fields(RequestBundleProjectionV3)}

    assert field_names.isdisjoint(
        {
            "workflow_name",
            "run_id",
            "workspace",
            "cores",
            "environment_observation",
            "published_path",
            "artifact_id",
            "content_digest",
        }
    )


@pytest.mark.parametrize(
    "contract",
    ["identity", "output"],
)
def test_projection_rejects_unknown_contract_versions(
    contract: str,
) -> None:
    projection = _projection()
    if contract == "identity":
        projection = replace(projection, identity_contract_version=1)
    else:
        projection = replace(
            projection,
            output_contract=replace(
                projection.output_contract,
                output_contract_version=2,
            ),
        )
    expected_version = 3 if contract == "identity" else 1
    with pytest.raises(
        ValidationError,
        match=rf"contract_version must be {expected_version}",
    ):
        _canonical_json(projection)


@pytest.mark.parametrize(
    ("value", "path"),
    [
        (("left", "right"), "canonical_parameters.labels"),
        ({1: "value"}, "canonical_parameters"),
        (date(2026, 7, 19), "canonical_parameters.value"),
        ({"left", "right"}, "canonical_parameters.value"),
        (float("nan"), "canonical_parameters.value"),
        (float("inf"), "canonical_parameters.value"),
        (object(), "canonical_parameters.value"),
    ],
)
def test_canonical_projection_rejects_non_json_parameter_values(
    value: object,
    path: str,
) -> None:
    parameters = (
        value
        if isinstance(value, tuple) or isinstance(value, dict)
        else {"value": value}
    )
    if isinstance(value, tuple):
        parameters = {"labels": value}

    with pytest.raises(ValidationError, match=path.replace(".", r"\.")):
        _canonical_json(_projection(canonical_parameters=parameters))


def test_canonical_projection_rejects_invalid_result_affecting_setting() -> None:
    projection = replace(
        _projection(),
        result_affecting_settings={"locale": ("en", "CA")},
    )

    with pytest.raises(
        ValidationError,
        match=r"result_affecting_settings\.locale",
    ):
        _canonical_json(projection)


def test_projection_plan_resolves_registered_source_snapshot() -> None:
    coordinate = LogicalSourceCoordinate("clms", "entity", "t1w", "aac_027_m00")

    state = resolve_request_bundle_projection_plan(
        _projection_plan(SourceBindingPlan(role="t1w", source_coordinate=coordinate)),
        source_snapshots={
            coordinate: RegisteredSourceSnapshot(
                content_digest="b" * 64,
                file_size=456,
                declared_extension=".nii.gz",
            )
        },
        upstream_states={},
    )

    assert isinstance(state, ResolvedRequestBundleProjectionV3)
    payload = json.loads(state.canonical_json)
    assert payload["output_contract"]["sibling_outputs"] == [
        {"declared_extension": ".nii.gz", "output_name": "segmentation"},
        {"declared_extension": ".csv", "output_name": "volumes"},
    ]
    assert payload["role_labelled_bindings"] == [
        {
            "role": "t1w",
            "source_coordinate": {
                "context": "clms",
                "scope": "entity",
                "source_name": "t1w",
                "entity_id": "aac_027_m00",
            },
            "registered_content_digest": "b" * 64,
            "registered_file_size": 456,
            "declared_extension": ".nii.gz",
        }
    ]


def test_projection_plan_reports_missing_sources_in_deterministic_order() -> None:
    first = LogicalSourceCoordinate("clms", "global", "a", None)
    second = LogicalSourceCoordinate("clms", "global", "b", None)

    state = resolve_request_bundle_projection_plan(
        _projection_plan(
            SourceBindingPlan(role="second", source_coordinate=second),
            SourceBindingPlan(role="first", source_coordinate=first),
        ),
        source_snapshots={},
        upstream_states={},
    )

    assert state == UnresolvedRequestBundleProjection((first, second))


def test_projection_plan_propagates_transitive_unresolved_sources() -> None:
    missing = LogicalSourceCoordinate("clms", "global", "missing", None)
    upstream = RequestedOutputCoordinate(
        namespace="clms",
        step_name="source_import",
        output_name="image",
        address="aac_027_m00",
    )

    state = resolve_request_bundle_projection_plan(
        _projection_plan(
            UpstreamRequestedOutputBindingPlan(
                role="image",
                requested_output=upstream,
            )
        ),
        source_snapshots={},
        upstream_states={
            upstream: UnresolvedRequestBundleProjection((missing,))
        },
    )

    assert state == UnresolvedRequestBundleProjection((missing,))


@pytest.mark.parametrize("manifest_digest", [None, "c" * 64])
def test_projection_plan_resolves_collection_members(
    manifest_digest: str | None,
) -> None:
    first_coordinate = RequestedOutputCoordinate(
        "clms", "source_import", "image", "subject-a"
    )
    second_coordinate = RequestedOutputCoordinate(
        "clms", "source_import", "image", "subject-b"
    )
    first_projection = _resolved(_projection(address="subject-a"))
    second_projection = _resolved(_projection(address="subject-b"))

    state = resolve_request_bundle_projection_plan(
        _projection_plan(
            CollectionBindingPlan(
                role="images",
                collection_semantics="coordinate_set_v1",
                manifest_value_schema=(
                    MANIFEST_VALUE_SCHEMA if manifest_digest is not None else None
                ),
                manifest_digest=manifest_digest,
                members=(second_coordinate, first_coordinate),
            )
        ),
        source_snapshots={},
        upstream_states={
            first_coordinate: first_projection,
            second_coordinate: second_projection,
        },
    )

    assert isinstance(state, ResolvedRequestBundleProjectionV3)
    members = json.loads(state.canonical_json)["role_labelled_bindings"][0][
        "members"
    ]
    assert {
        (member["upstream_request_bundle_digest"], member["output_name"])
        for member in members
    } == {
        (first_projection.request_bundle_digest, "image"),
        (second_projection.request_bundle_digest, "image"),
    }


def test_projection_plan_rejects_unavailable_upstream_coordinate() -> None:
    upstream = RequestedOutputCoordinate(
        "clms", "source_import", "image", "aac_027_m00"
    )

    with pytest.raises(ValidationError, match="unavailable upstream"):
        resolve_request_bundle_projection_plan(
            _projection_plan(
                UpstreamRequestedOutputBindingPlan("image", upstream)
            ),
            source_snapshots={},
            upstream_states={},
        )


def test_projection_plan_is_not_a_canonical_projection() -> None:
    with pytest.raises(ValidationError, match="RequestBundleProjectionV3"):
        canonicalize_request_bundle_projection(
            _projection_plan()  # type: ignore[arg-type]
        )


def test_direct_projection_size_does_not_grow_with_lineage_depth() -> None:
    upstream = _resolved(_projection(address="root"))
    direct_sizes: list[int] = []

    for depth in range(1, 21):
        projection = _projection(
            address=f"level-{depth}",
            role_labelled_bindings=(
                UpstreamRequestedOutputBinding(
                    role="image",
                    upstream_request_bundle_digest=upstream.request_bundle_digest,
                    output_name="segmentation",
                ),
            ),
        )
        upstream = _resolved(projection)
        direct_sizes.append(len(upstream.canonical_json.encode("utf-8")))

    assert max(direct_sizes) - min(direct_sizes) < 10


def test_collection_projection_size_grows_linearly_with_direct_fan_in() -> None:
    def size_for(member_count: int) -> int:
        members = tuple(
            UpstreamRequestedOutputBinding(
                role="images",
                upstream_request_bundle_digest=f"{index:064x}",
                output_name="image",
            )
            for index in range(member_count)
        )
        return len(
            _canonical_json(
                _projection(
                    role_labelled_bindings=(
                        CollectionBinding(
                            role="images",
                            collection_semantics="coordinate_set_v1",
                            manifest_digest=None,
                            members=members,
                        ),
                    )
                )
            ).encode("utf-8")
        )

    one_member = size_for(1)
    ten_members = size_for(10)
    twenty_members = size_for(20)
    assert twenty_members - ten_members == 10 * (ten_members - one_member) // 9


def test_stored_projection_validator_round_trips_and_reports_direct_upstreams() -> None:
    first = _resolved(_projection(address="first"))
    second = _resolved(_projection(address="second"))
    resolved = _resolved(
        _projection(
            role_labelled_bindings=(
                UpstreamRequestedOutputBinding(
                    role="mask",
                    upstream_request_bundle_digest=second.request_bundle_digest,
                    output_name="segmentation",
                ),
                CollectionBinding(
                    role="images",
                    collection_semantics="coordinate_set_v1",
                    manifest_digest=None,
                    members=(
                        UpstreamRequestedOutputBinding(
                            role="images",
                            upstream_request_bundle_digest=first.request_bundle_digest,
                            output_name="segmentation",
                        ),
                        UpstreamRequestedOutputBinding(
                            role="images",
                            upstream_request_bundle_digest=second.request_bundle_digest,
                            output_name="segmentation",
                        ),
                    ),
                ),
            )
        )
    )

    validated = validate_stored_request_bundle_projection_v3(
        request_bundle_digest=resolved.request_bundle_digest,
        projection_json=resolved.canonical_json,
    )

    assert validated.resolved_projection == resolved
    assert validated.direct_upstream_request_bundle_digests == tuple(
        sorted({first.request_bundle_digest, second.request_bundle_digest})
    )


@pytest.mark.parametrize(
    ("projection_json", "digest", "error"),
    [
        ('{"a":1,"a":2}', "a" * 64, "duplicate key"),
        ("[]", "a" * 64, "must be an object"),
        ('{"unknown":1}', "a" * 64, "invalid shape"),
    ],
)
def test_stored_projection_validator_rejects_malformed_shapes(
    projection_json: str,
    digest: str,
    error: str,
) -> None:
    with pytest.raises(ValidationError, match=error):
        validate_stored_request_bundle_projection_v3(
            request_bundle_digest=digest,
            projection_json=projection_json,
        )


def test_stored_projection_validator_rejects_noncanonical_or_mismatched_payload() -> None:
    resolved = _resolved(_projection())
    noncanonical = json.dumps(json.loads(resolved.canonical_json), indent=2)

    with pytest.raises(ValidationError, match="not canonical"):
        validate_stored_request_bundle_projection_v3(
            request_bundle_digest=resolved.request_bundle_digest,
            projection_json=noncanonical,
        )
    with pytest.raises(ValidationError, match="does not match"):
        validate_stored_request_bundle_projection_v3(
            request_bundle_digest="f" * 64,
            projection_json=resolved.canonical_json,
        )


def test_stored_projection_validator_rejects_v2_payload() -> None:
    payload = json.loads(_resolved(_projection()).canonical_json)
    payload["identity_contract_version"] = 2
    projection_json = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    with pytest.raises(ValidationError, match="identity_contract_version must be 3"):
        validate_stored_request_bundle_projection_v3(
            request_bundle_digest=hashlib.sha256(
                projection_json.encode("utf-8")
            ).hexdigest(),
            projection_json=projection_json,
        )
