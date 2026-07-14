import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { fetchSummary } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { KeyValueGrid } from "../components/ui/KeyValueGrid";
import { LoadingPanel } from "../components/ui/LoadingPanel";

// A metric value linked to the page that lists what it counts. The visible text
// is just the number, so each link carries an explicit accessible name.
function MetricLink({
  to,
  value,
  label,
}: {
  to: string;
  value: number;
  label: string;
}) {
  return (
    <Link to={to} aria-label={`View ${value} ${label}`}>
      {value}
    </Link>
  );
}

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
            {
              label: "workflows",
              value: (
                <MetricLink
                  to="/workflows"
                  value={summary.workflow_count}
                  label="workflows"
                />
              ),
            },
            {
              label: "runnable steps",
              value: (
                <MetricLink
                  to="/workflows"
                  value={summary.runnable_step_count}
                  label="runnable steps"
                />
              ),
            },
            {
              label: "manifests",
              value: (
                <MetricLink
                  to="/manifests"
                  value={summary.manifest_count}
                  label="manifests"
                />
              ),
            },
            {
              label: "artifacts",
              value: (
                <MetricLink
                  to="/artifacts"
                  value={summary.artifact_count}
                  label="artifacts"
                />
              ),
            },
            {
              label: "source artifacts",
              value: (
                <MetricLink
                  to="/artifacts?origin=source"
                  value={summary.source_artifact_count}
                  label="source artifacts"
                />
              ),
            },
            {
              label: "workflow outputs",
              value: (
                <MetricLink
                  to="/artifacts?origin=workflow_output"
                  value={summary.workflow_output_count}
                  label="workflow outputs"
                />
              ),
            },
            { label: "current run scopes", value: summary.workflow_run_count },
          ]}
        />
      </section>
    </div>
  );
}
