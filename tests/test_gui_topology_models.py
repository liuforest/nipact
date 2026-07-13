"""Model-contract tests for the observed-topology response (PR 2, commit 1).

These are pure validation tests over the response models in ``gui/models.py``.
They do not exercise the projector or the route (commits 2 and 3); they pin the
contract those later commits must produce.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from nipact.gui.models import (
    OBSERVED_TOPOLOGY_SCHEMA_VERSION,
    ObservedTopologyResponse,
    TopologyArtifactSlotNode,
    TopologyConsumesEdge,
    TopologyEdge,
    TopologyManifestBindingSummary,
    TopologyNode,
    TopologyProducesEdge,
    TopologySourceInputNode,
    TopologySourceRootNode,
    TopologyStepNode,
)

_NODE_ADAPTER = TypeAdapter(TopologyNode)
_EDGE_ADAPTER = TypeAdapter(TopologyEdge)


def _step_node() -> dict:
    return {
        "kind": "step",
        "node_id": "n0",
        "workflow_name": "wf",
        "step_name": "fit",
        "produced_registry_artifact_count": 3,
    }


def _artifact_slot_node() -> dict:
    return {
        "kind": "artifact_slot",
        "node_id": "n1",
        "workflow_name": "wf",
        "step_name": "fit",
        "output_name": "model",
        "registry_artifact_count": 5,
        "distinct_address_count": 2,
    }


def _source_input_node() -> dict:
    return {
        "kind": "source_input",
        "node_id": "n2",
        "workflow_name": "wf",
        "step_name": "fit",
        "binding_name": "bold",
        "dependency_role": "fit_input",
        "registry_artifact_count": 4,
    }


def _source_root_node() -> dict:
    return {
        "kind": "source_root",
        "node_id": "n3",
        "display_path": "sources/bold/sub-01.nii.gz",
        "registry_artifact_count": 1,
    }


def _consumes_edge() -> dict:
    return {
        "kind": "consumes",
        "edge_id": "e0",
        "source_node_id": "n1",
        "target_node_id": "n0",
        "workflow_name": "wf",
        "step_name": "fit",
        "binding_name": "bold",
        "dependency_role": "fit_input",
        "registry_dependency_count": 6,
    }


def _produces_edge() -> dict:
    return {
        "kind": "produces",
        "edge_id": "e1",
        "source_node_id": "n0",
        "target_node_id": "n1",
    }


def _manifest_binding() -> dict:
    return {
        "workflow_name": "wf",
        "step_name": "fit",
        "role": "source_population",
        "manifest_name": "cohort",
        "distinct_run_count": 2,
        "distinct_manifest_digest_count": 1,
        "manifest_digest": "deadbeef",
        "manifest_hash": "sha256:abc",
        "entity_count": 100,
    }


def _full_response() -> dict:
    return {
        "schema_version": OBSERVED_TOPOLOGY_SCHEMA_VERSION,
        "perspective": "observed",
        "scope": "ancestor_closure",
        "context": "demo",
        "root_artifact_id": 42,
        "root_node_id": "n1",
        "provenance_status": "complete",
        "summary": {
            "distinct_artifact_count": 9,
            "registry_dependency_count": 6,
            "node_count": 4,
            "edge_count": 2,
        },
        "nodes": [
            _step_node(),
            _artifact_slot_node(),
            _source_input_node(),
            _source_root_node(),
        ],
        "edges": [_consumes_edge(), _produces_edge()],
        "manifest_bindings": [_manifest_binding()],
        "warnings": [{"warning_type": "missing_artifact", "occurrence_count": 2}],
    }


# --- node/edge discriminator routing -----------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (_step_node(), TopologyStepNode),
        (_artifact_slot_node(), TopologyArtifactSlotNode),
        (_source_input_node(), TopologySourceInputNode),
        (_source_root_node(), TopologySourceRootNode),
    ],
)
def test_node_kind_selects_variant(payload, expected):
    assert isinstance(_NODE_ADAPTER.validate_python(payload), expected)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (_consumes_edge(), TopologyConsumesEdge),
        (_produces_edge(), TopologyProducesEdge),
    ],
)
def test_edge_kind_selects_variant(payload, expected):
    assert isinstance(_EDGE_ADAPTER.validate_python(payload), expected)


def test_unknown_node_kind_rejected():
    with pytest.raises(ValidationError):
        _NODE_ADAPTER.validate_python({**_step_node(), "kind": "mystery"})


# --- full response validation and round-trip ---------------------------------


def test_full_response_validates_and_round_trips():
    response = ObservedTopologyResponse.model_validate(_full_response())
    assert isinstance(response.nodes[0], TopologyStepNode)
    assert isinstance(response.nodes[3], TopologySourceRootNode)
    assert isinstance(response.edges[0], TopologyConsumesEdge)
    reparsed = ObservedTopologyResponse.model_validate(
        response.model_dump(mode="json")
    )
    assert reparsed == response


# --- extra="forbid" ----------------------------------------------------------


def test_extra_field_on_node_rejected():
    with pytest.raises(ValidationError):
        _NODE_ADAPTER.validate_python({**_step_node(), "surprise": 1})


def test_extra_field_on_response_rejected():
    with pytest.raises(ValidationError):
        ObservedTopologyResponse.model_validate({**_full_response(), "surprise": 1})


# --- Literal constraints -----------------------------------------------------


@pytest.mark.parametrize("field", ["perspective", "scope", "provenance_status"])
def test_literal_field_rejects_wrong_value(field):
    with pytest.raises(ValidationError):
        ObservedTopologyResponse.model_validate({**_full_response(), field: "nope"})


# --- non-negative count constraints ------------------------------------------


def test_negative_node_count_rejected():
    with pytest.raises(ValidationError):
        _NODE_ADAPTER.validate_python(
            {**_step_node(), "produced_registry_artifact_count": -1}
        )


def test_negative_edge_count_rejected():
    with pytest.raises(ValidationError):
        _EDGE_ADAPTER.validate_python(
            {**_consumes_edge(), "registry_dependency_count": -1}
        )


def test_negative_summary_count_rejected():
    payload = _full_response()
    payload["summary"]["node_count"] = -1
    with pytest.raises(ValidationError):
        ObservedTopologyResponse.model_validate(payload)


# --- grouped manifest summary nullable fields --------------------------------


def test_manifest_summary_allows_null_disagreeing_fields():
    binding = TopologyManifestBindingSummary.model_validate(
        {
            **_manifest_binding(),
            "manifest_digest": None,
            "manifest_hash": None,
            "entity_count": None,
        }
    )
    assert binding.manifest_digest is None
    assert binding.entity_count is None


def test_manifest_summary_rejects_negative_entity_count():
    with pytest.raises(ValidationError):
        TopologyManifestBindingSummary.model_validate(
            {**_manifest_binding(), "entity_count": -1}
        )


def test_manifest_summary_rejects_extra_field():
    with pytest.raises(ValidationError):
        TopologyManifestBindingSummary.model_validate(
            {**_manifest_binding(), "run_id": 7}
        )
