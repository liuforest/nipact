import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useParams } from "react-router-dom";
import { fetchArtifactLineage, fetchArtifactTopology } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import { LineageGraphExplorer } from "../lineage/LineageGraphExplorer";
import { TopologyGraphExplorer } from "../lineage/TopologyGraphExplorer";
import {
  GRAPH_RENDER_ELEMENT_LIMIT,
  exceedsRenderLimit,
  topologyElementCount,
  traceGraphElementCount,
} from "../lineage/renderLimits";

export function LineagePage({
  renderLimit = GRAPH_RENDER_ELEMENT_LIMIT,
}: {
  renderLimit?: number;
} = {}) {
  const artifactId = Number(useParams().artifactId);
  const [rawMode, setRawMode] = useState(false);
  const validId = Number.isInteger(artifactId) && artifactId > 0;

  const topologyQuery = useQuery({
    queryKey: queryKeys.topology(artifactId),
    queryFn: () => fetchArtifactTopology(artifactId),
    enabled: validId && !rawMode,
  });
  // Raw lineage is fetched only after the user explicitly opts in, so the
  // default page load never requests /lineage.
  const lineageQuery = useQuery({
    queryKey: queryKeys.lineage(artifactId),
    queryFn: () => fetchArtifactLineage(artifactId),
    enabled: validId && rawMode,
  });

  if (!validId) {
    return <ErrorPanel error={new Error("invalid artifact id")} />;
  }

  if (rawMode) {
    if (lineageQuery.isError) {
      return <ErrorPanel error={lineageQuery.error} />;
    }
    if (lineageQuery.isLoading || !lineageQuery.data) {
      return <LoadingPanel label="Loading lineage" />;
    }
    const graph = lineageQuery.data;
    if (exceedsRenderLimit(traceGraphElementCount(graph), renderLimit)) {
      return (
        <RenderRefusalPanel
          title="Raw lineage too large to render"
          elementCount={traceGraphElementCount(graph)}
          limit={renderLimit}
          onShowTopology={() => setRawMode(false)}
        />
      );
    }
    return (
      <div className="page-stack">
        <section className="panel">
          <button type="button" onClick={() => setRawMode(false)}>
            Back to observed topology
          </button>
        </section>
        <LineageGraphExplorer graph={graph} />
      </div>
    );
  }

  if (topologyQuery.isError) {
    return <ErrorPanel error={topologyQuery.error} />;
  }
  if (topologyQuery.isLoading || !topologyQuery.data) {
    return <LoadingPanel label="Loading topology" />;
  }
  const topology = topologyQuery.data;
  if (exceedsRenderLimit(topologyElementCount(topology), renderLimit)) {
    return (
      <RenderRefusalPanel
        title="Observed topology too large to render"
        elementCount={topologyElementCount(topology)}
        limit={renderLimit}
      />
    );
  }
  return (
    <TopologyGraphExplorer
      topology={topology}
      onShowRawLineage={() => setRawMode(true)}
    />
  );
}

function RenderRefusalPanel({
  title,
  elementCount,
  limit,
  onShowTopology,
}: {
  title: string;
  elementCount: number;
  limit: number;
  onShowTopology?: () => void;
}) {
  return (
    <section className="panel panel--warning" role="alert">
      <h1>{title}</h1>
      <p>
        This graph has {elementCount} elements, above the {limit}-element render
        limit. The graph is not drawn to keep the browser responsive. Trace a
        more specific artifact to narrow the closure.
      </p>
      {onShowTopology ? (
        <button type="button" onClick={onShowTopology}>
          Back to observed topology
        </button>
      ) : null}
    </section>
  );
}
