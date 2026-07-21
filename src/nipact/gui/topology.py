"""Pure projection of a trace graph into an observed topology (PR 2).

``build_observed_topology`` reduces the raw ``build_trace_graph()`` dict
(``trace.py``) to the aggregated topology payload validated by
``ObservedTopologyResponse`` (``gui/models.py``). It is a pure function over the
trace dict: no registry read, no SQL, no second traversal. The service layer
(PR 2, commit 3) validates its output before serialization.
"""

from __future__ import annotations

from typing import Any

from .models import OBSERVED_TOPOLOGY_SCHEMA_VERSION


def build_observed_topology(trace_graph: dict[str, Any]) -> dict[str, Any]:
    """Reduce a raw ``build_trace_graph()`` dict to the topology payload."""
    artifacts = trace_graph["artifacts"]
    dependencies = trace_graph["dependencies"]
    artifacts_by_id = {artifact["artifact_id"]: artifact for artifact in artifacts}

    steps, slots, source_inputs = _collect_nodes(
        artifacts,
        dependencies,
        artifacts_by_id,
    )

    root = artifacts_by_id[trace_graph["selected_artifact_id"]]
    source_root_key = (
        ("source_root", root["display_path"]) if root["origin"] == "source" else None
    )

    nodes, node_id_by_key, node_index_by_key = _assign_nodes(
        steps,
        slots,
        source_inputs,
        source_root_key,
        root,
    )

    edges = _build_edges(
        dependencies,
        slots,
        artifacts_by_id,
        node_id_by_key,
        node_index_by_key,
    )

    if source_root_key is not None:
        root_node_id = node_id_by_key[source_root_key]
    else:
        root_node_id = node_id_by_key[
            ("artifact_slot", root["workflow_name"], root["step_name"], root["output_name"])
        ]

    return {
        "schema_version": OBSERVED_TOPOLOGY_SCHEMA_VERSION,
        "perspective": "observed",
        "scope": "ancestor_closure",
        "context": trace_graph["context"],
        "root_artifact_id": trace_graph["selected_artifact_id"],
        "root_node_id": root_node_id,
        "provenance_status": trace_graph["provenance_status"],
        "summary": {
            "distinct_artifact_count": len(artifacts),
            "registry_dependency_count": len(dependencies),
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
        "nodes": nodes,
        "edges": edges,
        "execution_populations": _group_execution_populations(
            trace_graph["execution_populations"]
        ),
        "manifest_bindings": _group_manifest_bindings(
            trace_graph["manifest_bindings"]
        ),
        "warnings": _group_warnings(trace_graph["warnings"]),
    }


def _collect_nodes(
    artifacts: list[dict[str, Any]],
    dependencies: list[dict[str, Any]],
    artifacts_by_id: dict[int, dict[str, Any]],
) -> tuple[
    dict[tuple[str, str], set[int]],
    dict[tuple[str, str, str], dict[str, set[Any]]],
    dict[tuple[str, str, str, str], set[int]],
]:
    # steps and artifact slots come from workflow-output artifacts in the closure
    steps: dict[tuple[str, str], set[int]] = {}
    slots: dict[tuple[str, str, str], dict[str, set[Any]]] = {}
    for artifact in artifacts:
        if artifact["origin"] != "workflow_output":
            continue
        workflow_name = artifact["workflow_name"]
        step_name = artifact["step_name"]
        output_name = artifact["output_name"]
        steps.setdefault((workflow_name, step_name), set()).add(artifact["artifact_id"])
        slot = slots.setdefault(
            (workflow_name, step_name, output_name),
            {"artifacts": set(), "addresses": set()},
        )
        slot["artifacts"].add(artifact["artifact_id"])
        if artifact["address"] is not None:
            slot["addresses"].add(artifact["address"])

    # source-input coordinates are consumer-derived: read from the dependent
    # (consuming) artifact plus the dependency binding/role
    source_inputs: dict[tuple[str, str, str, str], set[int]] = {}
    for dependency in dependencies:
        source = artifacts_by_id.get(dependency["source_artifact_id"])
        if source is None or source["origin"] != "source":
            continue
        dependent = artifacts_by_id[dependency["dependent_artifact_id"]]
        key = (
            dependent["workflow_name"],
            dependent["step_name"],
            dependency["binding_name"],
            dependency["dependency_role"],
        )
        source_inputs.setdefault(key, set()).add(dependency["source_artifact_id"])

    return steps, slots, source_inputs


def _assign_nodes(
    steps: dict[tuple[str, str], set[int]],
    slots: dict[tuple[str, str, str], dict[str, set[Any]]],
    source_inputs: dict[tuple[str, str, str, str], set[int]],
    source_root_key: tuple[str, str] | None,
    root: dict[str, Any],
) -> tuple[
    list[dict[str, Any]],
    dict[tuple[Any, ...], str],
    dict[tuple[Any, ...], int],
]:
    # (canonical_key, node_dict); canonical_key is also the deterministic sort key
    records: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for (workflow_name, step_name), artifact_ids in steps.items():
        key = ("step", workflow_name, step_name)
        records.append(
            (
                key,
                {
                    "kind": "step",
                    "workflow_name": workflow_name,
                    "step_name": step_name,
                    "produced_registry_artifact_count": len(artifact_ids),
                },
            )
        )
    for (workflow_name, step_name, output_name), data in slots.items():
        key = ("artifact_slot", workflow_name, step_name, output_name)
        records.append(
            (
                key,
                {
                    "kind": "artifact_slot",
                    "workflow_name": workflow_name,
                    "step_name": step_name,
                    "output_name": output_name,
                    "registry_artifact_count": len(data["artifacts"]),
                    "distinct_address_count": len(data["addresses"]),
                },
            )
        )
    for (workflow_name, step_name, binding_name, role), source_ids in source_inputs.items():
        key = ("source_input", workflow_name, step_name, binding_name, role)
        records.append(
            (
                key,
                {
                    "kind": "source_input",
                    "workflow_name": workflow_name,
                    "step_name": step_name,
                    "binding_name": binding_name,
                    "dependency_role": role,
                    "registry_artifact_count": len(source_ids),
                },
            )
        )
    if source_root_key is not None:
        records.append(
            (
                source_root_key,
                {
                    "kind": "source_root",
                    "display_path": root["display_path"],
                    "registry_artifact_count": 1,
                },
            )
        )

    records.sort(key=lambda record: record[0])
    nodes: list[dict[str, Any]] = []
    node_id_by_key: dict[tuple[Any, ...], str] = {}
    node_index_by_key: dict[tuple[Any, ...], int] = {}
    for index, (key, node) in enumerate(records):
        node_id = f"n{index}"
        node_id_by_key[key] = node_id
        node_index_by_key[key] = index
        nodes.append({**node, "node_id": node_id})
    return nodes, node_id_by_key, node_index_by_key


def _build_edges(
    dependencies: list[dict[str, Any]],
    slots: dict[tuple[str, str, str], dict[str, set[Any]]],
    artifacts_by_id: dict[int, dict[str, Any]],
    node_id_by_key: dict[tuple[Any, ...], str],
    node_index_by_key: dict[tuple[Any, ...], int],
) -> list[dict[str, Any]]:
    # consumption edges aggregate physical dependency rows by
    # (source node, consuming step, binding, role)
    consumes: dict[tuple[tuple[Any, ...], tuple[Any, ...], str, str], int] = {}
    for dependency in dependencies:
        source = artifacts_by_id.get(dependency["source_artifact_id"])
        if source is None:
            # degraded missing-source row: omit the edge (no node to anchor it),
            # but it is still counted in summary registry_dependency_count
            continue
        dependent = artifacts_by_id[dependency["dependent_artifact_id"]]
        target_key = ("step", dependent["workflow_name"], dependent["step_name"])
        if source["origin"] == "workflow_output":
            source_key: tuple[Any, ...] = (
                "artifact_slot",
                source["workflow_name"],
                source["step_name"],
                source["output_name"],
            )
        else:
            source_key = (
                "source_input",
                dependent["workflow_name"],
                dependent["step_name"],
                dependency["binding_name"],
                dependency["dependency_role"],
            )
        agg_key = (
            source_key,
            target_key,
            dependency["binding_name"],
            dependency["dependency_role"],
        )
        consumes[agg_key] = consumes.get(agg_key, 0) + 1

    records: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for (source_key, target_key, binding_name, role), count in consumes.items():
        records.append(
            (
                (
                    "consumes",
                    node_index_by_key[source_key],
                    node_index_by_key[target_key],
                    binding_name,
                    role,
                ),
                {
                    "kind": "consumes",
                    "source_node_id": node_id_by_key[source_key],
                    "target_node_id": node_id_by_key[target_key],
                    "workflow_name": target_key[1],
                    "step_name": target_key[2],
                    "binding_name": binding_name,
                    "dependency_role": role,
                    "registry_dependency_count": count,
                },
            )
        )
    # one production edge per artifact slot: step -> slot
    for workflow_name, step_name, output_name in slots:
        step_key = ("step", workflow_name, step_name)
        slot_key = ("artifact_slot", workflow_name, step_name, output_name)
        records.append(
            (
                (
                    "produces",
                    node_index_by_key[step_key],
                    node_index_by_key[slot_key],
                    "",
                    "",
                ),
                {
                    "kind": "produces",
                    "source_node_id": node_id_by_key[step_key],
                    "target_node_id": node_id_by_key[slot_key],
                },
            )
        )

    records.sort(key=lambda record: record[0])
    return [
        {**edge, "edge_id": f"e{index}"} for index, (_, edge) in enumerate(records)
    ]


def _group_manifest_bindings(
    manifest_bindings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str, str], dict[str, set[Any]]] = {}
    for binding in manifest_bindings:
        key = (
            binding["workflow_name"],
            binding["step_name"],
            binding["manifest_usage_role"],
            binding["manifest_name"],
            binding["manifest_value_schema"],
        )
        group = groups.setdefault(
            key,
            {"run_ids": set(), "digests": set(), "hashes": set(), "entity_counts": set()},
        )
        group["run_ids"].add(binding["run_id"])
        group["digests"].add(binding["manifest_digest"])
        group["hashes"].add(binding["manifest_hash"])
        group["entity_counts"].add(binding["entity_count"])

    summaries: list[dict[str, Any]] = []
    for (
        workflow_name,
        step_name,
        manifest_usage_role,
        manifest_name,
        manifest_value_schema,
    ) in sorted(groups):
        group = groups[
            (
                workflow_name,
                step_name,
                manifest_usage_role,
                manifest_name,
                manifest_value_schema,
            )
        ]
        summaries.append(
            {
                "workflow_name": workflow_name,
                "step_name": step_name,
                "manifest_usage_role": manifest_usage_role,
                "manifest_name": manifest_name,
                "manifest_value_schema": manifest_value_schema,
                "distinct_run_count": len(group["run_ids"]),
                "distinct_manifest_digest_count": len(group["digests"]),
                "manifest_digest": _agreed_value(group["digests"]),
                "manifest_hash": _agreed_value(group["hashes"]),
                "entity_count": _agreed_value(group["entity_counts"]),
            }
        )
    return summaries


def _group_execution_populations(
    populations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], dict[str, set[Any]]] = {}
    for population in populations:
        key = (
            population["workflow_name"],
            population["manifest_name"],
            population["manifest_value_schema"],
        )
        group = groups.setdefault(
            key,
            {
                "run_ids": set(),
                "digests": set(),
                "hashes": set(),
                "entity_counts": set(),
            },
        )
        group["run_ids"].add(population["run_id"])
        group["digests"].add(population["manifest_digest"])
        group["hashes"].add(population["manifest_hash"])
        group["entity_counts"].add(population["entity_count"])

    summaries: list[dict[str, Any]] = []
    for workflow_name, manifest_name, manifest_value_schema in sorted(groups):
        group = groups[(workflow_name, manifest_name, manifest_value_schema)]
        summaries.append(
            {
                "workflow_name": workflow_name,
                "manifest_name": manifest_name,
                "manifest_value_schema": manifest_value_schema,
                "distinct_run_count": len(group["run_ids"]),
                "distinct_manifest_digest_count": len(group["digests"]),
                "manifest_digest": _agreed_value(group["digests"]),
                "manifest_hash": _agreed_value(group["hashes"]),
                "entity_count": _agreed_value(group["entity_counts"]),
            }
        )
    return summaries


def _group_warnings(warnings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for warning in warnings:
        warning_type = warning["warning_type"]
        counts[warning_type] = counts.get(warning_type, 0) + 1
    return [
        {"warning_type": warning_type, "occurrence_count": count}
        for warning_type, count in sorted(counts.items())
    ]


def _agreed_value(values: set[Any]) -> Any:
    # carry the value through only when the grouped rows agree; otherwise null
    return next(iter(values)) if len(values) == 1 else None
