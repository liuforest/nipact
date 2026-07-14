import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { fetchManifests } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import type { ManifestSummary } from "../api/types";
import { DataTable } from "../components/ui/DataTable";
import { EmptyPanel } from "../components/ui/EmptyPanel";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import { PathValue } from "../components/ui/PathValue";

export function ManifestsPage() {
  const query = useQuery({
    queryKey: queryKeys.manifests,
    queryFn: fetchManifests,
  });

  if (query.isLoading) {
    return <LoadingPanel label="Loading manifests" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading manifests" />;
  }
  if (query.data.manifests.length === 0) {
    return <EmptyPanel title="No manifests" />;
  }

  return (
    <section className="panel">
      <h1>Manifests</h1>
      <DataTable<ManifestSummary>
        rows={query.data.manifests}
        getRowKey={(row) => row.name}
        columns={[
          {
            key: "name",
            label: "name",
            render: (row) => <Link to={`/manifests/${encodeURIComponent(row.name)}`}>{row.name}</Link>,
          },
          { key: "entities", label: "entities", render: (row) => row.entity_count },
          { key: "first", label: "first", render: (row) => row.first_entity_id },
          { key: "last", label: "last", render: (row) => row.last_entity_id },
          { key: "path", label: "path", render: (row) => <PathValue value={row.path} compact /> },
        ]}
      />
    </section>
  );
}
