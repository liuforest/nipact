import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { fetchArtifact } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import type { Artifact } from "../api/types";
import { ArtifactBreadcrumb } from "../components/ui/ArtifactBreadcrumb";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { IdentifierValue } from "../components/ui/IdentifierValue";
import { KeyValueGrid } from "../components/ui/KeyValueGrid";
import type { KeyValueItem } from "../components/ui/KeyValueGrid";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import { PathValue } from "../components/ui/PathValue";

export function ArtifactDetailPage() {
  const artifactId = Number(useParams().artifactId);
  const query = useQuery({
    queryKey: queryKeys.artifact(artifactId),
    queryFn: () => fetchArtifact(artifactId),
    enabled: Number.isInteger(artifactId) && artifactId > 0,
  });

  if (!Number.isInteger(artifactId) || artifactId <= 0) {
    return <ErrorPanel error={new Error("invalid artifact id")} />;
  }
  if (query.isLoading) {
    return <LoadingPanel label="Loading artifact" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading artifact" />;
  }
  const artifact = query.data;

  return (
    <div className="page-stack">
      <ArtifactBreadcrumb artifactId={artifact.artifact_id} view="detail" />
      <section className="panel">
        <p className="eyebrow">Artifact {artifact.artifact_id}</p>
        <h1>
          <PathValue value={artifact.display_path} />
        </h1>
        <div className="button-row">
          <Link className="button" to={`/artifacts/${artifact.artifact_id}/lineage`}>
            Trace
          </Link>
        </div>
      </section>
      <DetailSection title="Identity" items={identityItems(artifact)} />
      <DetailSection title="Paths" items={pathItems(artifact)} />
      <DetailSection title="Workflow And Run" items={workflowItems(artifact)} />
      <DetailSection title="Hashes And Parameters" items={hashItems(artifact)} />
      <DetailSection title="Entity Fields" items={entityItems(artifact)} />
      {artifact.source_metadata ? (
        <section className="panel">
          <h2>Source Metadata</h2>
          <pre className="json-block">{JSON.stringify(artifact.source_metadata, null, 2)}</pre>
        </section>
      ) : null}
    </div>
  );
}

function DetailSection({
  title,
  items,
}: {
  title: string;
  items: readonly KeyValueItem[];
}) {
  return (
    <section className="panel artifact-detail-section">
      <h2>{title}</h2>
      <KeyValueGrid items={items} />
    </section>
  );
}

function identityItems(artifact: Artifact): KeyValueItem[] {
  return [
    { label: "artifact id", value: <IdentifierValue value={artifact.artifact_id} /> },
    {
      label: "origin",
      value: (
        <Link to={filterPath({ origin: artifact.origin })}>{artifact.origin}</Link>
      ),
    },
    { label: "published", value: artifact.is_published ? "yes" : "no" },
    { label: "selected output", value: artifact.is_selected_output ? "yes" : "no" },
    { label: "extension", value: artifact.extension },
    { label: "file size", value: artifact.file_size },
    { label: "created", value: artifact.created_at },
  ];
}

function pathItems(artifact: Artifact): KeyValueItem[] {
  return [
    { label: "display path", value: <PathValue value={artifact.display_path} /> },
    { label: "registered path", value: <PathValue value={artifact.path} /> },
    { label: "published path", value: <PathValue value={artifact.published_path} /> },
    {
      label: "historical staging path (may not exist)",
      value: <PathValue value={artifact.staging_path} />,
    },
  ];
}

function workflowItems(artifact: Artifact): KeyValueItem[] {
  const { workflow_name: workflow, step_name: step, output_name: output } = artifact;
  return [
    {
      label: "workflow",
      value: workflow ? (
        <Link to={filterPath({ workflow })}>{workflow}</Link>
      ) : (
        workflow
      ),
    },
    {
      label: "step",
      value:
        workflow && step ? (
          <Link to={filterPath({ workflow, step })}>{step}</Link>
        ) : (
          step
        ),
    },
    {
      label: "output",
      value:
        workflow && step && output ? (
          <Link to={filterPath({ workflow, step, output })}>{output}</Link>
        ) : (
          output
        ),
    },
    { label: "address", value: artifact.address },
    { label: "run id", value: <IdentifierValue value={artifact.run_id} /> },
    { label: "job id", value: <IdentifierValue value={artifact.job_id} /> },
    { label: "artifact set", value: <IdentifierValue value={artifact.artifact_set_id} /> },
    { label: "workflow artifact ref", value: <IdentifierValue value={artifact.workflow_artifact_ref} /> },
    { label: "callable", value: artifact.callable_ref },
    { label: "software", value: artifact.software_ref },
  ];
}

function hashItems(artifact: Artifact): KeyValueItem[] {
  return [
    { label: "content digest", value: <IdentifierValue value={artifact.content_digest} /> },
    { label: "output hash", value: <IdentifierValue value={artifact.output_hash} /> },
    { label: "parameter hash", value: <IdentifierValue value={artifact.parameter_hash} /> },
    { label: "parameter digest", value: <IdentifierValue value={artifact.parameter_digest} /> },
  ];
}

// Build a link to the filtered artifacts list. Coordinate links use cumulative
// prefixes (workflow → workflow+step → workflow+step+output) so a step/output
// name can't collide across workflows.
function filterPath(filters: Record<string, string>): string {
  return `/artifacts?${new URLSearchParams(filters).toString()}`;
}

function entityItems(artifact: Artifact): KeyValueItem[] {
  return [
    { label: "subject", value: artifact.subject_id },
    { label: "session", value: artifact.session_id },
    { label: "task", value: artifact.task_name },
    { label: "run", value: artifact.run_label },
    { label: "datatype", value: artifact.datatype },
    { label: "suffix", value: artifact.suffix },
  ];
}
