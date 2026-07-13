import type { ElementDefinition } from "cytoscape";
import type {
  ObservedTopologyResponse,
  TopologyEdge,
  TopologyNode,
} from "../api/types";

interface TopologyElementOptions {
  rootNodeId?: string | null;
  selectedNodeId?: string | null;
  selectedEdgeId?: string | null;
}

export function buildTopologyElements(
  topology: ObservedTopologyResponse,
  options: TopologyElementOptions = {},
): ElementDefinition[] {
  const rootNodeId = options.rootNodeId ?? topology.root_node_id;
  const elements: ElementDefinition[] = topology.nodes.map((node) => ({
    data: {
      ...node,
      id: node.node_id,
      label: topologyNodeLabel(node),
    },
    classes: [
      "topology-node",
      node.kind,
      node.node_id === rootNodeId ? "root-ui" : "",
      node.node_id === options.selectedNodeId ? "selected-ui" : "",
    ]
      .filter(Boolean)
      .join(" "),
  }));

  for (const edge of topology.edges) {
    elements.push({
      data: {
        ...edge,
        id: edge.edge_id,
        source: edge.source_node_id,
        target: edge.target_node_id,
        label: topologyEdgeLabel(edge),
      },
      classes: [
        "topology-edge",
        edge.kind,
        edge.edge_id === options.selectedEdgeId ? "selected-ui" : "",
      ]
        .filter(Boolean)
        .join(" "),
    });
  }

  return elements;
}

export function topologyNodeLabel(node: TopologyNode): string {
  switch (node.kind) {
    case "step":
      return withCount(node.step_name, node.produced_registry_artifact_count);
    case "artifact_slot":
      return withCount(node.output_name, node.registry_artifact_count);
    case "source_input":
      return withCount(node.binding_name, node.registry_artifact_count);
    case "source_root":
      return lastPathSegment(node.display_path);
  }
}

function topologyEdgeLabel(edge: TopologyEdge): string {
  return edge.kind === "consumes" ? edge.binding_name : "";
}

function withCount(label: string, count: number): string {
  return count > 1 ? `${label}\n×${count}` : label;
}

function lastPathSegment(path: string): string {
  const parts = path.split("/");
  return parts[parts.length - 1] || path;
}
