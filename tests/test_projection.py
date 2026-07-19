from dataclasses import replace
from datetime import date

import pytest

from nipact.errors import ValidationError
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
    RequestBundleProjectionV1,
    RequestBundleProjectionPlanV1,
    RequestedOutputCoordinate,
    SiblingOutput,
    SourceBindingPlan,
    SourceCoordinate,
    StepContract,
    UnresolvedRequestBundleProjection,
    UpstreamRequestedOutputBinding,
    UpstreamRequestedOutputBindingPlan,
    canonical_projection_json,
    resolve_request_bundle_projection_plan,
)


def _source_binding(
    *,
    role: str = "t1w",
    namespace: str = "clms",
    path: str = "data/aac_027_m00/t1w.nii.gz",
) -> RegisteredSourceBinding:
    return RegisteredSourceBinding(
        role=role,
        source_coordinate=SourceCoordinate(namespace=namespace, path=path),
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
) -> RequestBundleProjectionV1:
    return RequestBundleProjectionV1(
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
        upstream_request_projection=_projection(address=address),
        output_name=output_name,
    )


def _projection_plan(
    *binding_plans: ProjectionBindingPlan,
    address: str = "aac_027_m00",
    parameters: object | None = None,
) -> RequestBundleProjectionPlanV1:
    return RequestBundleProjectionPlanV1(
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

    assert canonical_projection_json(projection) == (
        '{"address":"aac_027_m00","canonical_parameters":{"enabled":true,'
        '"label":"café","threshold":0.5},"determinism_contract":"deterministic",'
        '"identity_contract_version":1,"namespace":"clms","output_contract":'
        '{"output_contract_version":1,"sibling_outputs":[{"declared_extension":'
        '".nii.gz","output_name":"segmentation"}]},"result_affecting_settings":{},'
        '"role_labelled_bindings":[{"declared_extension":".nii.gz",'
        '"registered_content_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaa","registered_file_size":123,"role":"t1w",'
        '"source_coordinate":{"namespace":"clms","path":'
        '"data/aac_027_m00/t1w.nii.gz"}}],"step_contract":{"callable_ref":'
        '"clms.steps:t1_synthseg","runner_contract_version":"1",'
        '"step_contract_id":"t1_synthseg","step_contract_version":"1"}}'
    )


def test_canonical_projection_is_invariant_to_object_and_declared_set_order() -> None:
    first_member = _upstream_binding(role="images", address="subject-a")
    second_member = _upstream_binding(role="images", address="subject-b")
    first = _projection(
        canonical_parameters={"outer": {"a": 1, "b": 2}},
        role_labelled_bindings=(
            _source_binding(role="mask", path="data/mask.nii.gz"),
            CollectionBinding(
                role="images",
                collection_semantics="coordinate_set_v1",
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
                manifest_digest="b" * 64,
                members=(first_member, second_member),
            ),
            _source_binding(role="mask", path="data/mask.nii.gz"),
        ),
        sibling_outputs=(
            SiblingOutput(output_name="image", declared_extension=".nii.gz"),
            SiblingOutput(output_name="table", declared_extension=".csv"),
        ),
    )

    assert canonical_projection_json(first) == canonical_projection_json(second)


def test_canonical_projection_preserves_declared_sequence_order() -> None:
    first = _projection(canonical_parameters={"labels": ["left", "right"]})
    second = _projection(canonical_parameters={"labels": ["right", "left"]})

    assert canonical_projection_json(first) != canonical_projection_json(second)


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
    serialized = canonical_projection_json(
        _projection(canonical_parameters={"value": value})
    )

    assert expected in serialized


def test_canonical_projection_serializes_collection_without_manifest() -> None:
    binding = CollectionBinding(
        role="images",
        collection_semantics="coordinate_set_v1",
        manifest_digest=None,
        members=(
            _upstream_binding(role="images", address="subject-b"),
            _upstream_binding(role="images", address="subject-a"),
        ),
    )

    serialized = canonical_projection_json(
        _projection(role_labelled_bindings=(binding,))
    )

    assert '"manifest_digest":null' in serialized
    assert serialized.index('"address":"subject-a"') < serialized.index(
        '"address":"subject-b"'
    )


def test_binding_role_is_identity_bearing() -> None:
    first = _projection(role_labelled_bindings=(_source_binding(role="image"),))
    second = _projection(role_labelled_bindings=(_source_binding(role="mask"),))

    assert canonical_projection_json(first) != canonical_projection_json(second)


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
        canonical_projection_json(_projection(canonical_parameters=parameters))


def test_canonical_projection_rejects_invalid_result_affecting_setting() -> None:
    projection = replace(
        _projection(),
        result_affecting_settings={"locale": ("en", "CA")},
    )

    with pytest.raises(
        ValidationError,
        match=r"result_affecting_settings\.locale",
    ):
        canonical_projection_json(projection)


def test_projection_plan_resolves_registered_source_snapshot() -> None:
    coordinate = SourceCoordinate("clms", "data/aac_027_m00/t1w.nii.gz")

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

    assert isinstance(state, RequestBundleProjectionV1)
    assert state.output_contract.sibling_outputs == (
        SiblingOutput("segmentation", ".nii.gz"),
        SiblingOutput("volumes", ".csv"),
    )
    assert state.role_labelled_bindings == (
        RegisteredSourceBinding(
            role="t1w",
            source_coordinate=coordinate,
            registered_content_digest="b" * 64,
            registered_file_size=456,
            declared_extension=".nii.gz",
        ),
    )


def test_projection_plan_reports_missing_sources_in_deterministic_order() -> None:
    first = SourceCoordinate("clms", "data/a.nii.gz")
    second = SourceCoordinate("clms", "data/b.nii.gz")

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
    missing = SourceCoordinate("clms", "data/missing.nii.gz")
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
    first_projection = _projection(address="subject-a")
    second_projection = _projection(address="subject-b")

    state = resolve_request_bundle_projection_plan(
        _projection_plan(
            CollectionBindingPlan(
                role="images",
                collection_semantics="coordinate_set_v1",
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

    assert isinstance(state, RequestBundleProjectionV1)
    assert state.role_labelled_bindings == (
        CollectionBinding(
            role="images",
            collection_semantics="coordinate_set_v1",
            manifest_digest=manifest_digest,
            members=(
                UpstreamRequestedOutputBinding(
                    role="images",
                    upstream_request_projection=second_projection,
                    output_name="image",
                ),
                UpstreamRequestedOutputBinding(
                    role="images",
                    upstream_request_projection=first_projection,
                    output_name="image",
                ),
            ),
        ),
    )


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
    with pytest.raises(ValidationError, match="RequestBundleProjectionV1"):
        canonical_projection_json(_projection_plan())  # type: ignore[arg-type]
