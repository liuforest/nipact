import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { fetchArtifactLineage, fetchArtifactTopology } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { ArtifactBreadcrumb } from "../components/ui/ArtifactBreadcrumb";
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
  const [instancesRequestedFor, setInstancesRequestedFor] = useState<
    number | null
  >(null);
  // Derive the latch from the current artifact so navigation never leaves a
  // stale `true` visible to useQuery during the first render of a new root — a
  // post-render effect reset would fire /lineage before it ran.
  const instancesRequested = instancesRequestedFor === artifactId;
  const validId = Number.isInteger(artifactId) && artifactId > 0;
  // Revisiting an artifact starts collapsed again.
  useEffect(() => {
    setInstancesRequestedFor(null);
  }, [artifactId]);

  const topologyQuery = useQuery({
    queryKey: queryKeys.topology(artifactId),
    queryFn: () => fetchArtifactTopology(artifactId),
    enabled: validId && !rawMode,
  });
  // Raw lineage is fetched only after the user explicitly opts in — either by
  // entering the raw view or by opening topology instance details. Both share
  // this cache entry, so switching views does not start a separate lineage query.
  const lineageQuery = useQuery({
    queryKey: queryKeys.lineage(artifactId),
    queryFn: () => fetchArtifactLineage(artifactId),
    enabled: validId && (rawMode || instancesRequested),
  });

  if (!validId) {
    return <ErrorPanel error={new Error("invalid artifact id")} />;
  }

  // The lineage breadcrumb is rendered above this branch switch, so it — and the
  // list + detail exits it provides — is present across the successful views and
  // both render-refusal states without any per-branch control props.
  const body = (function renderLineageBody() {
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
        <>
          <section className="panel">
            <button
              type="button"
              className="button"
              onClick={() => setRawMode(false)}
            >
              Back to observed topology
            </button>
          </section>
          <LineageGraphExplorer graph={graph} />
        </>
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
        onRequestInstances={() => setInstancesRequestedFor(artifactId)}
        instancesRequested={instancesRequested}
        instanceLineage={{
          isLoading: lineageQuery.isLoading,
          isError: lineageQuery.isError,
          error: lineageQuery.error,
          data: lineageQuery.data,
        }}
      />
    );
  })();

  return (
    <div className="page-stack">
      <ArtifactBreadcrumb artifactId={artifactId} view="lineage" />
      {body}
    </div>
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
        <button type="button" className="button" onClick={onShowTopology}>
          Back to observed topology
        </button>
      ) : null}
    </section>
  );
}
