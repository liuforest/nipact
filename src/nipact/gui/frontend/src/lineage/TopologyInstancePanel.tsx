import { useEffect, useState } from "react";
import type { ObservedTopologyResponse, TraceGraphResponse } from "../api/types";
import { DataTable } from "../components/ui/DataTable";
import { IdentifierValue } from "../components/ui/IdentifierValue";
import { PathValue } from "../components/ui/PathValue";
import type { TopologyGraphSelection } from "./TopologyGraphCanvas";
import {
  resolveTopologyInstances,
  searchInstanceRows,
  type TopologyInstanceResult,
} from "./topologyDrilldown";

const PAGE_SIZE = 50;

export interface InstanceLineageState {
  isLoading: boolean;
  isError: boolean;
  error: unknown;
  data: TraceGraphResponse | undefined;
}

export function TopologyInstancePanel({
  topology,
  selection,
  instanceLineage,
}: {
  topology: ObservedTopologyResponse;
  selection: TopologyGraphSelection;
  instanceLineage: InstanceLineageState;
}) {
  const [searchText, setSearchText] = useState("");
  const [page, setPage] = useState(0);
  const selectionKey =
    selection.kind === "node"
      ? `node:${selection.node_id}`
      : `edge:${selection.edge_id}`;
  // Reset paging and search whenever the inspected element changes.
  useEffect(() => {
    setSearchText("");
    setPage(0);
  }, [selectionKey]);
  // Reset to the first page whenever the search term changes.
  useEffect(() => {
    setPage(0);
  }, [searchText]);

  if (instanceLineage.isError) {
    return (
      <div className="selection-panel" role="alert">
        <h3>Instances</h3>
        <p className="status-line">
          Could not load instance records: {errorMessage(instanceLineage.error)}
        </p>
      </div>
    );
  }
  const graph = instanceLineage.data;
  if (instanceLineage.isLoading || !graph) {
    return (
      <div className="selection-panel" role="status" aria-live="polite">
        <h3>Instances</h3>
        <p className="status-line">Loading instance records…</p>
      </div>
    );
  }

  const result = resolveTopologyInstances(topology, graph, selection);
  if (!result) {
    return (
      <div className="selection-panel">
        <h3>Instances</h3>
        <p className="muted-value">No instance records for this selection.</p>
      </div>
    );
  }
  const filtered = searchInstanceRows(result, searchText);
  const total = filtered.rows.length;
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  const start = safePage * PAGE_SIZE;
  const end = Math.min(start + PAGE_SIZE, total);

  return (
    <div className="selection-panel">
      <h3>Instances ({filtered.kind})</h3>
      <div className="graph-search-toolbar">
        <label className="search-field">
          <span>Search instances</span>
          <input
            type="search"
            value={searchText}
            onChange={(event) => setSearchText(event.target.value)}
            placeholder="Filter the records below..."
          />
        </label>
        <span className="status-line">
          {total === 0
            ? "No matching records."
            : `Showing ${start + 1}–${end} of ${total}`}
        </span>
      </div>
      {filtered.kind === "artifacts" ? (
        <ArtifactInstanceTable rows={filtered.rows.slice(start, end)} />
      ) : (
        <DependencyInstanceTable rows={filtered.rows.slice(start, end)} />
      )}
      <div className="graph-search-toolbar">
        <button
          type="button"
          disabled={safePage <= 0}
          onClick={() => setPage(safePage - 1)}
        >
          Previous
        </button>
        <span className="status-line">
          Page {safePage + 1} of {pageCount}
        </span>
        <button
          type="button"
          disabled={safePage >= pageCount - 1}
          onClick={() => setPage(safePage + 1)}
        >
          Next
        </button>
      </div>
    </div>
  );
}

function ArtifactInstanceTable({
  rows,
}: {
  rows: Extract<TopologyInstanceResult, { kind: "artifacts" }>["rows"];
}) {
  return (
    <DataTable
      rows={rows}
      getRowKey={(row) => row.artifact_id}
      columns={[
        { key: "id", label: "id", render: (row) => <IdentifierValue value={row.artifact_id} /> },
        { key: "origin", label: "origin", render: (row) => row.origin },
        { key: "step", label: "step", render: (row) => row.step_name ?? "source" },
        { key: "output", label: "output", render: (row) => row.output_name ?? "source" },
        { key: "address", label: "address", render: (row) => row.address ?? "none" },
        { key: "path", label: "path", render: (row) => <PathValue value={row.display_path} /> },
      ]}
    />
  );
}

function DependencyInstanceTable({
  rows,
}: {
  rows: Extract<TopologyInstanceResult, { kind: "dependencies" }>["rows"];
}) {
  return (
    <DataTable
      rows={rows}
      getRowKey={(row) => row.edge_id}
      columns={[
        { key: "source", label: "source", render: (row) => <IdentifierValue value={row.source_artifact_id} /> },
        { key: "dependent", label: "dependent", render: (row) => <IdentifierValue value={row.dependent_artifact_id} /> },
        { key: "reuse", label: "reuse", render: (row) => (row.is_reused_input ? "reused" : "not reused") },
        { key: "binding", label: "binding", render: (row) => row.binding_name },
        { key: "role", label: "role", render: (row) => row.dependency_role },
        { key: "set", label: "set", render: (row) => <IdentifierValue value={row.dependency_set_id} /> },
        { key: "cardinality", label: "cardinality", render: (row) => row.edge_cardinality ?? "none" },
        { key: "input", label: "input", render: (row) => <PathValue value={row.input_path} /> },
      ]}
    />
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "unknown error";
}
