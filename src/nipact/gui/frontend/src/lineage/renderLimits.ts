import type { ObservedTopologyResponse, TraceGraphResponse } from "../api/types";

// Rendering ceiling for the Cytoscape canvas / raw DataTables. Above this, the
// page shows a refusal panel instead of mounting the graph, so a pathological
// closure cannot hang the browser. Tunable; callers may override per view.
export const GRAPH_RENDER_ELEMENT_LIMIT = 2000;

export function traceGraphElementCount(graph: TraceGraphResponse): number {
  return (
    graph.artifacts.length +
    graph.dependencies.length +
    graph.manifest_bindings.length
  );
}

export function topologyElementCount(
  topology: ObservedTopologyResponse,
): number {
  return (
    topology.nodes.length +
    topology.edges.length +
    topology.manifest_bindings.length
  );
}

export function exceedsRenderLimit(
  count: number,
  limit: number = GRAPH_RENDER_ELEMENT_LIMIT,
): boolean {
  return count > limit;
}
