import { useQuery } from "@tanstack/react-query";
import { fetchWorkflows } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import { DataTable } from "../components/ui/DataTable";
import { EmptyPanel } from "../components/ui/EmptyPanel";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import type { WorkflowSummary } from "../api/types";

export function WorkflowsPage() {
  const query = useQuery({
    queryKey: queryKeys.workflows,
    queryFn: fetchWorkflows,
  });

  if (query.isLoading) {
    return <LoadingPanel label="Loading workflows" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading workflows" />;
  }
  if (query.data.workflows.length === 0) {
    return <EmptyPanel title="No workflows" />;
  }

  return (
    <section className="panel">
      <h1>Workflows</h1>
      <DataTable<WorkflowSummary>
        rows={query.data.workflows}
        getRowKey={(row) => row.workflow_name}
        columns={[
          { key: "name", label: "workflow", render: (row) => row.workflow_name },
          { key: "steps", label: "steps", render: (row) => row.step_count },
          {
            key: "runnable",
            label: "runnable steps",
            render: (row) => (
              <div className="inline-list">
                {row.runnable_steps.map((step) => (
                  <span
                    key={`${row.workflow_name}:${step.step_name}:${step.output_name}`}
                  >
                    {step.step_name}.{step.output_name}
                  </span>
                ))}
              </div>
            ),
          },
        ]}
      />
    </section>
  );
}
