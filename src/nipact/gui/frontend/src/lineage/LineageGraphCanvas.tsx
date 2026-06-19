import type { LayoutOptions } from "cytoscape";
import type { TraceGraphResponse } from "../api/types";
import { GraphCanvasFrame } from "../graph/GraphCanvasFrame";
import { buildLineageElements } from "./cytoscapeAdapter";
import { lineageGraphStyle } from "./cytoscapeStyle";

export type LineageGraphSelection =
  | {
      kind: "artifact";
      artifact_id: number;
    }
  | {
      kind: "dependency";
      edge_id: string;
    };

const lineageLayout = {
  name: "dagre",
  rankDir: "LR",
  nodeSep: 36,
  rankSep: 56,
} as LayoutOptions;

export function LineageGraphCanvas({
  graph,
  onSelectionChange,
  searchArtifactIds = [],
  selectedArtifactId = null,
  selectedDependencyEdgeId = null,
}: {
  graph: TraceGraphResponse;
  onSelectionChange?: (selection: LineageGraphSelection | null) => void;
  searchArtifactIds?: readonly number[];
  selectedArtifactId?: number | null;
  selectedDependencyEdgeId?: string | null;
}) {
  return (
    <GraphCanvasFrame
      ariaLabel="Lineage graph"
      elements={buildLineageElements(graph, {
        selectedArtifactId,
        selectedDependencyEdgeId,
        searchArtifactIds,
      })}
      layout={lineageLayout}
      onElementSelect={
        onSelectionChange
          ? (data) => onSelectionChange(toLineageSelection(data))
          : undefined
      }
      stylesheet={lineageGraphStyle}
    />
  );
}

export function toLineageSelection(data: Record<string, unknown> | null): LineageGraphSelection | null {
  if (!data) {
    return null;
  }
  if (data.type === "artifact" && typeof data.artifact_id === "number") {
    return {
      kind: "artifact",
      artifact_id: data.artifact_id,
    };
  }
  if (
    data.type === "dependency" &&
    typeof data.edge_id === "string"
  ) {
    return {
      kind: "dependency",
      edge_id: data.edge_id,
    };
  }
  return null;
}
