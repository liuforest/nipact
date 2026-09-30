"""Route/contract tests for ``GET /api/artifacts`` and its filter vocabulary.

These exercise the collection endpoint end to end over a real ``colors``
registry (the ``colors_registry`` fixture): the unfiltered population, a
supported filter narrowing that population, and the 422 ``unsupported_filter``
contract enforced by ``_reject_unsupported_query_params``.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from nipact.gui.app import create_gui_app

from conftest import ColorsRegistry


def _client(colors_registry: ColorsRegistry) -> TestClient:
    app = create_gui_app(
        project_dir=colors_registry.project_dir,
        context=colors_registry.context,
    )
    return TestClient(app)


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


@pytest.mark.parametrize(
    "route",
    ["/api/artifacts", "/api/artifacts/groups"],
    ids=["artifacts", "groups"],
)
def test_artifact_routes_reject_unsupported_filter(
    colors_registry: ColorsRegistry,
    route: str,
) -> None:
    client = _client(colors_registry)
    response = client.get(route, params={"bogus": "1"})
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "unsupported_filter"
    assert body["details"]["filters"] == ["bogus"]


def test_artifact_groups_route_returns_flat_coordinate_counts(
    colors_registry: ColorsRegistry,
) -> None:
    client = _client(colors_registry)
    # "groups" must not be captured by /api/artifacts/{artifact_id}.
    response = client.get("/api/artifacts/groups")
    assert response.status_code == 200
    groups = response.json()["groups"]
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


def test_directory_inspection_is_metadata_only_and_preserves_nullable_edges(
    tmp_path, monkeypatch
):
    from directory_support import directory_project, run, rows
    import nipact.artifacts as artifact_ops

    project, runtime = directory_project(tmp_path, monkeypatch)
    assert run(project, "reader", "sub_001").all_selected_resolved
    tree = next(row for row in rows(runtime) if row["output_name"] == "maps")
    consumer = next(row for row in rows(runtime) if row["step_name"] == "reader")
    client = TestClient(create_gui_app(project_dir=project, context="mini"))

    def forbidden(*args, **kwargs):
        pytest.fail("inspection read scientific payload")

    monkeypatch.setattr(artifact_ops, "artifact_content_facts", forbidden)
    original_iterdir = __import__("pathlib").Path.iterdir

    def guarded(path):
        if path.is_relative_to(runtime / "outputs"):
            forbidden()
        return original_iterdir(path)

    monkeypatch.setattr(__import__("pathlib").Path, "iterdir", guarded)
    detail = client.get(f'/api/artifacts/{tree["artifact_id"]}').json()
    assert (detail["kind"], detail["extension"], detail["file_size"]) == (
        "directory",
        None,
        21,
    )
    assert detail["digest_scheme"] == "nipact-directory-tree-sha256-v1"
    assert (
        client.get("/api/artifacts/resolve", params={"path": tree["path"]}).json()[
            "artifact_id"
        ]
        == tree["artifact_id"]
    )
    listing = client.get("/api/artifacts").json()["artifacts"]
    assert (
        next(row for row in listing if row["artifact_id"] == tree["artifact_id"])[
            "kind"
        ]
        == "directory"
    )
    from nipact.trace import build_trace_graph_for_artifact_id

    graph = build_trace_graph_for_artifact_id(
        runtime / "database/registry.db",
        artifact_id=consumer["artifact_id"],
        context="mini",
    )
    assert (
        next(
            row
            for row in graph["artifacts"]
            if row["artifact_id"] == tree["artifact_id"]
        )["extension"]
        is None
    )
    assert any(edge["source_extension"] is None for edge in graph["dependencies"])
