import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { fetchSummary } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { KeyValueGrid } from "../components/ui/KeyValueGrid";
import { LoadingPanel } from "../components/ui/LoadingPanel";

export function OverviewPage() {
  const summaryQuery = useQuery({
    queryKey: queryKeys.summary,
    queryFn: fetchSummary,
  });

  if (summaryQuery.isLoading) {
    return <LoadingPanel label="Loading project summary" />;
  }
  if (summaryQuery.isError) {
    return <ErrorPanel error={summaryQuery.error} />;
  }
  if (!summaryQuery.data) {
    return <LoadingPanel label="Loading project summary" />;
  }
  const summary = summaryQuery.data;

  return (
    <div className="page-stack">
      <section className="panel">
        <p className="eyebrow">Context</p>
        <h1>{summary.context}</h1>
        <KeyValueGrid
          items={[
            { label: "workflows", value: summary.workflow_count },
            { label: "runnable steps", value: summary.runnable_step_count },
            { label: "manifests", value: summary.manifest_count },
            { label: "artifacts", value: summary.artifact_count },
            { label: "source artifacts", value: summary.source_artifact_count },
            { label: "workflow outputs", value: summary.workflow_output_count },
            { label: "current run scopes", value: summary.workflow_run_count },
          ]}
        />
      </section>
      <section className="panel link-grid" aria-label="GUI sections">
        <Link to="/workflows">Workflows</Link>
        <Link to="/artifacts">Artifacts</Link>
        <Link to="/manifests">Manifests</Link>
      </section>
    </div>
  );
}
