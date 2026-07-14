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


def test_artifacts_route_list_profile_omits_detail_only_fields(
    colors_registry: ColorsRegistry,
) -> None:
    # The list profile is slim: the unbounded source_metadata blob and the
    # derivable lineage_url ride only on the /{id} detail record, never on rows.
    client = _client(colors_registry)
    rows = client.get("/api/artifacts").json()["artifacts"]
    assert rows
    for row in rows:
        assert "source_metadata" not in row
        assert "lineage_url" not in row

    detail = client.get(f"/api/artifacts/{rows[0]['artifact_id']}").json()
    assert "source_metadata" in detail
    assert "lineage_url" not in detail


def test_artifact_resolve_route_returns_detail_profile(
    colors_registry: ColorsRegistry,
) -> None:
    # /resolve serves the full detail profile (source_metadata), like /{id} and
    # unlike the slim list rows — guards the ArtifactDetail response so a revert
    # to the summary payload cannot pass CI silently.
    client = _client(colors_registry)
    row = client.get("/api/artifacts").json()["artifacts"][0]
    response = client.get("/api/artifacts/resolve", params={"path": row["path"]})
    assert response.status_code == 200
    resolved = response.json()
    assert resolved["artifact_id"] == row["artifact_id"]
    assert "source_metadata" in resolved


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


def test_artifact_groups_route_returns_flat_coordinate_counts(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    groups = client.get("/api/artifacts/groups").json()["groups"]
    full = client.get("/api/artifacts").json()["artifacts"]

    assert len(groups) > 0
    # The summed group counts describe exactly the full artifact population.
    assert sum(group["artifact_count"] for group in groups) == len(full)

    # The source group carries null coordinates, not a display sentinel.
    source_groups = [group for group in groups if group["origin"] == "source"]
    assert len(source_groups) == 1
    assert source_groups[0]["workflow_name"] is None
    assert source_groups[0]["step_name"] is None
    assert source_groups[0]["output_name"] is None


def test_artifact_groups_route_step_filter_narrows_groups(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    full = client.get("/api/artifacts/groups").json()["groups"]
    filtered = client.get(
        "/api/artifacts/groups",
        params={"step": "color_sector_analysis"},
    ).json()["groups"]

    assert {group["step_name"] for group in filtered} == {"color_sector_analysis"}
    filtered_total = sum(group["artifact_count"] for group in filtered)
    assert 0 < filtered_total < sum(group["artifact_count"] for group in full)


def test_artifact_groups_route_resolves_before_artifact_id(
    colors_registry: ColorsRegistry,
) -> None:
    # "groups" must not be captured by /api/artifacts/{artifact_id}.
    client = _client(colors_registry)
    response = client.get("/api/artifacts/groups")
    assert response.status_code == 200
    assert "groups" in response.json()


def test_artifact_groups_route_rejects_unsupported_filter(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    response = client.get("/api/artifacts/groups", params={"bogus": "1"})
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "unsupported_filter"
    assert body["details"]["filters"] == ["bogus"]
