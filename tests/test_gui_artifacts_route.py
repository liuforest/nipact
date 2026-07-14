"""Route/contract tests for ``GET /api/artifacts`` and its filter vocabulary.

These exercise the collection endpoint end to end over a real ``colors``
registry (the ``colors_registry`` fixture): the unfiltered population, a
supported filter narrowing that population, and the 422 ``unsupported_filter``
contract enforced by ``_reject_unsupported_query_params``.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from nipact.gui.app import create_gui_app

from conftest import ColorsRegistry


def _client(colors_registry: ColorsRegistry) -> TestClient:
    app = create_gui_app(
        project_dir=colors_registry.project_dir,
        context=colors_registry.context,
    )
    return TestClient(app)


def test_artifacts_route_returns_full_population_unfiltered(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    response = client.get("/api/artifacts")
    assert response.status_code == 200
    body = response.json()
    assert body["context"] == colors_registry.context
    assert len(body["artifacts"]) > 0


def test_artifacts_route_step_filter_narrows_population(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    full = client.get("/api/artifacts").json()["artifacts"]
    filtered = client.get(
        "/api/artifacts",
        params={"step": "color_sector_analysis"},
    ).json()["artifacts"]

    assert 0 < len(filtered) < len(full)
    assert {row["step_name"] for row in filtered} == {"color_sector_analysis"}


def test_artifacts_route_rejects_unsupported_filter(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    response = client.get("/api/artifacts", params={"bogus": "1"})
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "unsupported_filter"
    assert body["details"]["filters"] == ["bogus"]
