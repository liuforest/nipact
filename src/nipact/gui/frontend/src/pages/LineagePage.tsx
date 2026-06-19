import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { fetchArtifactLineage } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import { LineageGraphExplorer } from "../lineage/LineageGraphExplorer";

export function LineagePage() {
  const artifactId = Number(useParams().artifactId);
  const query = useQuery({
    queryKey: queryKeys.lineage(artifactId),
    queryFn: () => fetchArtifactLineage(artifactId),
    enabled: Number.isInteger(artifactId) && artifactId > 0,
  });

  if (!Number.isInteger(artifactId) || artifactId <= 0) {
    return <ErrorPanel error={new Error("invalid artifact id")} />;
  }
  if (query.isLoading) {
    return <LoadingPanel label="Loading lineage" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading lineage" />;
  }
  return <LineageGraphExplorer graph={query.data} />;
}
