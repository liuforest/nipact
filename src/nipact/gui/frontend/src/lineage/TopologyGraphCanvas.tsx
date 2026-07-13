import type { LayoutOptions } from "cytoscape";
import type { ObservedTopologyResponse } from "../api/types";
import { GraphCanvasFrame } from "../graph/GraphCanvasFrame";
import { buildTopologyElements } from "./topologyAdapter";
import { topologyGraphStyle } from "./topologyStyle";

export type TopologyGraphSelection =
  | {
      kind: "node";
      node_id: string;
    }
  | {
      kind: "edge";
      edge_id: string;
    };

const topologyLayout = {
  name: "dagre",
  rankDir: "LR",
  nodeSep: 36,
  rankSep: 56,
} as LayoutOptions;

export function TopologyGraphCanvas({
  topology,
  onSelectionChange,
  selectedNodeId = null,
  selectedEdgeId = null,
}: {
  topology: ObservedTopologyResponse;
  onSelectionChange?: (selection: TopologyGraphSelection | null) => void;
  selectedNodeId?: string | null;
  selectedEdgeId?: string | null;
}) {
  return (
    <GraphCanvasFrame
      ariaLabel="Observed topology graph"
      elements={buildTopologyElements(topology, {
        selectedNodeId,
        selectedEdgeId,
      })}
      layout={topologyLayout}
      onElementSelect={
        onSelectionChange
          ? (data) => onSelectionChange(toTopologySelection(data))
          : undefined
      }
      stylesheet={topologyGraphStyle}
    />
  );
}

export function toTopologySelection(
  data: Record<string, unknown> | null,
): TopologyGraphSelection | null {
  if (!data) {
    return null;
  }
  // edges carry source_node_id/target_node_id; nodes do not.
  if (typeof data.source_node_id === "string" && typeof data.edge_id === "string") {
    return {
      kind: "edge",
      edge_id: data.edge_id,
    };
  }
  if (typeof data.node_id === "string") {
    return {
      kind: "node",
      node_id: data.node_id,
    };
  }
  return null;
}
