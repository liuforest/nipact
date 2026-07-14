import type {
  ObservedTopologyResponse,
  TopologyArtifactSlotNode,
  TopologyConsumesEdge,
  TopologyNode,
  TraceArtifact,
  TraceDependency,
  TraceGraphResponse,
} from "../api/types";
import type { TopologyGraphSelection } from "./TopologyGraphCanvas";

// Client-side inverse of gui/topology.py::build_observed_topology. A selected
// topology coordinate maps back to the concrete raw records it aggregates. The
// filters below mirror that projection exactly so a full (unpaged) match set
// equals the coordinate's advertised count; see topologyDrilldown.test.ts.
export type TopologyInstanceResult =
  | { kind: "artifacts"; rows: TraceArtifact[] }
  | { kind: "dependencies"; rows: TraceDependency[] };

export function resolveTopologyInstances(
  topology: ObservedTopologyResponse,
  graph: TraceGraphResponse,
  selection: TopologyGraphSelection,
): TopologyInstanceResult | null {
  const artifactsById = new Map(
    graph.artifacts.map((artifact) => [artifact.artifact_id, artifact]),
  );
  if (selection.kind === "node") {
    const node = topology.nodes.find((n) => n.node_id === selection.node_id);
    return node ? resolveNode(node, graph, artifactsById) : null;
  }
  const edge = topology.edges.find((e) => e.edge_id === selection.edge_id);
  if (!edge) {
    return null;
  }
  if (edge.kind === "consumes") {
    // The consumes edge carries no source coordinate, only source_node_id, so
    // dereference it before restricting the source side (topology.py aggregates
    // by the *source node*, not just binding/role).
    const sourceNode =
      topology.nodes.find((n) => n.node_id === edge.source_node_id) ?? null;
    return {
      kind: "dependencies",
      rows: matchConsumesDependencies(edge, sourceNode, graph, artifactsById),
    };
  }
  // A produces edge is represented by the artifacts in its target slot, not by
  // separate dependency rows.
  const targetNode = topology.nodes.find(
    (n) => n.node_id === edge.target_node_id,
  );
  if (!targetNode || targetNode.kind !== "artifact_slot") {
    return null;
  }
  return { kind: "artifacts", rows: matchSlotArtifacts(targetNode, graph) };
}

function resolveNode(
  node: TopologyNode,
  graph: TraceGraphResponse,
  artifactsById: Map<number, TraceArtifact>,
): TopologyInstanceResult {
  switch (node.kind) {
    case "step":
      return {
        kind: "artifacts",
        rows: graph.artifacts.filter(
          (artifact) =>
            artifact.origin === "workflow_output" &&
            artifact.workflow_name === node.workflow_name &&
            artifact.step_name === node.step_name,
        ),
      };
    case "artifact_slot":
      return { kind: "artifacts", rows: matchSlotArtifacts(node, graph) };
    case "source_input": {
      // Distinct source artifacts referenced by dependencies whose consuming
      // artifact matches this coordinate; mirrors _collect_nodes source_inputs.
      const sourceIds = new Set<number>();
      for (const dependency of graph.dependencies) {
        const source = artifactsById.get(dependency.source_artifact_id);
        if (!source || source.origin !== "source") {
          continue;
        }
        const dependent = artifactsById.get(dependency.dependent_artifact_id);
        if (
          dependent &&
          dependent.workflow_name === node.workflow_name &&
          dependent.step_name === node.step_name &&
          dependency.binding_name === node.binding_name &&
          dependency.dependency_role === node.dependency_role
        ) {
          sourceIds.add(dependency.source_artifact_id);
        }
      }
      return {
        kind: "artifacts",
        rows: graph.artifacts.filter((artifact) =>
          sourceIds.has(artifact.artifact_id),
        ),
      };
    }
    case "source_root": {
      // A selected source root has a one-artifact backward closure: itself.
      const root = artifactsById.get(graph.selected_artifact_id);
      return { kind: "artifacts", rows: root ? [root] : [] };
    }
  }
}

function matchSlotArtifacts(
  node: TopologyArtifactSlotNode,
  graph: TraceGraphResponse,
): TraceArtifact[] {
  return graph.artifacts.filter(
    (artifact) =>
      artifact.origin === "workflow_output" &&
      artifact.workflow_name === node.workflow_name &&
      artifact.step_name === node.step_name &&
      artifact.output_name === node.output_name,
  );
}

function matchConsumesDependencies(
  edge: TopologyConsumesEdge,
  sourceNode: TopologyNode | null,
  graph: TraceGraphResponse,
  artifactsById: Map<number, TraceArtifact>,
): TraceDependency[] {
  return graph.dependencies.filter((dependency) => {
    const source = artifactsById.get(dependency.source_artifact_id);
    // Degraded missing-source rows are omitted, matching _build_edges (they are
    // not counted in the edge's registry_dependency_count either).
    if (!source) {
      return false;
    }
    const dependent = artifactsById.get(dependency.dependent_artifact_id);
    if (
      !dependent ||
      dependent.workflow_name !== edge.workflow_name ||
      dependent.step_name !== edge.step_name ||
      dependency.binding_name !== edge.binding_name ||
      dependency.dependency_role !== edge.dependency_role
    ) {
      return false;
    }
    if (!sourceNode) {
      return false;
    }
    if (sourceNode.kind === "artifact_slot") {
      return (
        source.origin === "workflow_output" &&
        source.workflow_name === sourceNode.workflow_name &&
        source.step_name === sourceNode.step_name &&
        source.output_name === sourceNode.output_name
      );
    }
    if (sourceNode.kind === "source_input") {
      // The source_input branch in _build_edges covers every non-workflow_output
      // source for this (step, binding, role) coordinate.
      return source.origin !== "workflow_output";
    }
    return false;
  });
}

export function searchInstanceRows(
  result: TopologyInstanceResult,
  searchText: string,
): TopologyInstanceResult {
  const term = searchText.trim().toLowerCase();
  if (!term) {
    return result;
  }
  if (result.kind === "artifacts") {
    return {
      kind: "artifacts",
      rows: result.rows.filter((row) =>
        artifactSearchValues(row).some((value) => value.includes(term)),
      ),
    };
  }
  return {
    kind: "dependencies",
    rows: result.rows.filter((row) =>
      dependencySearchValues(row).some((value) => value.includes(term)),
    ),
  };
}

function artifactSearchValues(artifact: TraceArtifact): string[] {
  return [
    artifact.artifact_id,
    artifact.origin,
    artifact.workflow_name,
    artifact.step_name,
    artifact.output_name,
    artifact.address,
    artifact.display_path,
  ]
    .filter((value): value is string | number => value !== null && value !== undefined)
    .map((value) => String(value).toLowerCase());
}

function dependencySearchValues(dependency: TraceDependency): string[] {
  return [
    dependency.source_artifact_id,
    dependency.dependent_artifact_id,
    dependency.binding_name,
    dependency.dependency_role,
    dependency.input_path,
  ].map((value) => String(value).toLowerCase());
}
