"""Service/route tests for the observed-topology endpoint (PR 2, commit 3).

These exercise ``GET /api/artifacts/{id}/topology`` end to end over a real
``colors`` registry (the ``colors_registry`` fixture): success projection,
unknown-root mapping, and context scoping. Per the design doc there is no
route-level test for internal model-validation errors — invalid projector
output legitimately surfaces as a server error, and is covered by the
projector/model tests, not here.
"""

from __future__ import annotations

import dataclasses

import pytest
from fastapi.testclient import TestClient

from nipact.gui.app import create_gui_app
from nipact.gui.models import ObservedTopologyResponse
from nipact.gui.project import resolve_gui_project
from nipact.gui.service import GuiApiError, GuiService

from conftest import ColorsRegistry


def _client(colors_registry: ColorsRegistry) -> TestClient:
    app = create_gui_app(
        project_dir=colors_registry.project_dir,
        context=colors_registry.context,
    )
    return TestClient(app)


def test_topology_route_returns_observed_projection(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    response = client.get(
        f"/api/artifacts/{colors_registry.root_artifact_id}/topology"
    )
    assert response.status_code == 200

    topology = ObservedTopologyResponse.model_validate(response.json())
    assert topology.perspective == "observed"
    assert topology.scope == "ancestor_closure"
    assert topology.context == colors_registry.context
    assert topology.root_artifact_id == colors_registry.root_artifact_id
    assert topology.root_node_id in {node.node_id for node in topology.nodes}
    assert topology.summary.node_count == len(topology.nodes)
    assert topology.summary.edge_count == len(topology.edges)


def test_topology_route_unknown_artifact_returns_404(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    absent_id = colors_registry.root_artifact_id + 100_000
    response = client.get(f"/api/artifacts/{absent_id}/topology")
    assert response.status_code == 404
    assert response.json()["code"] == "artifact_not_found"


def test_topology_service_is_context_scoped(
    colors_registry: ColorsRegistry,
) -> None:
    # a genuine artifact id, but read through a project bound to another
    # context, must not resolve: exercises the `AND a.context=?` guard.
    project = resolve_gui_project(
        project_dir=colors_registry.project_dir,
        context=colors_registry.context,
    )
    ghost = dataclasses.replace(project, context="ghost")
    service = GuiService(ghost)

    with pytest.raises(GuiApiError) as excinfo:
        service.topology(colors_registry.root_artifact_id)
    assert excinfo.value.status_code == 404
    assert excinfo.value.code == "artifact_not_found"
