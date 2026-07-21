import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { fetchManifest } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { IdentifierValue } from "../components/ui/IdentifierValue";
import { KeyValueGrid } from "../components/ui/KeyValueGrid";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import { PathValue } from "../components/ui/PathValue";

export function ManifestDetailPage() {
  const manifestName = useParams().manifestName ?? "";
  const query = useQuery({
    queryKey: queryKeys.manifest(manifestName),
    queryFn: () => fetchManifest(manifestName),
    enabled: manifestName.length > 0,
  });

  if (!manifestName) {
    return <ErrorPanel error={new Error("missing manifest name")} />;
  }
  if (query.isLoading) {
    return <LoadingPanel label="Loading manifest" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading manifest" />;
  }
  const manifest = query.data;

  return (
    <div className="page-stack">
      <section className="panel">
        <p className="eyebrow">Manifest</p>
        <h1>{manifest.name}</h1>
        <KeyValueGrid
          items={[
            { label: "path", value: <PathValue value={manifest.path} /> },
            { label: "entities", value: manifest.entity_count },
            { label: "first", value: manifest.first_entity_id },
            { label: "last", value: manifest.last_entity_id },
            { label: "value schema", value: manifest.manifest_value_schema },
            { label: "digest", value: <IdentifierValue value={manifest.manifest_digest} /> },
            { label: "hash", value: <IdentifierValue value={manifest.manifest_hash} /> },
          ]}
        />
      </section>
      <section className="panel">
        <h2>Body</h2>
        <pre className="json-block">{manifest.canonical_body}</pre>
      </section>
    </div>
  );
}
