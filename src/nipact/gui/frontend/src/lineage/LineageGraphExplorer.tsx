import { Suspense, lazy, useEffect, useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import type {
  TraceArtifact,
  TraceDependency,
  TraceGraphResponse,
  TraceManifestBinding,
} from "../api/types";
import { DataTable } from "../components/ui/DataTable";
import { IdentifierValue } from "../components/ui/IdentifierValue";
import { PathValue } from "../components/ui/PathValue";
import { WarningList } from "../components/ui/WarningList";
import type { LineageGraphSelection } from "./LineageGraphCanvas";
import {
  buildArtifactNeighborhood,
  findTraceArtifact,
  findTraceDependency,
  searchLineageArtifacts,
} from "./lineageInteraction";

const LineageGraphCanvas = lazy(async () => ({
  default: (await import("./LineageGraphCanvas")).LineageGraphCanvas,
}));

export function LineageGraphExplorer({ graph }: { graph: TraceGraphResponse }) {
  const [searchText, setSearchText] = useState("");
  const [selection, setSelection] = useState<LineageGraphSelection | null>(null);
  useEffect(() => {
    setSelection(null);
  }, [graph.selected_artifact_id]);
  const searchArtifactIds = useMemo(
    () => searchLineageArtifacts(graph, searchText),
    [graph, searchText],
  );
  const artifactsById = useMemo(
    () => new Map(graph.artifacts.map((artifact) => [artifact.artifact_id, artifact])),
    [graph.artifacts],
  );
  const selectedArtifact =
    selection?.kind === "artifact"
      ? findTraceArtifact(graph, selection.artifact_id)
      : null;
  const selectedDependency =
    selection?.kind === "dependency"
      ? findTraceDependency(graph, selection.edge_id)
      : null;
  const neighborhood = buildArtifactNeighborhood(
    graph,
    selectedArtifact?.artifact_id ?? null,
  );
  const selectedArtifactId = selection?.kind === "artifact" ? selection.artifact_id : null;
  const selectedDependencyEdgeId =
    selection?.kind === "dependency" ? selection.edge_id : null;
  const hasSearch = searchText.trim().length > 0;

  return (
    <div className="page-stack">
      <section className="panel">
        <p className="eyebrow">{graph.provenance_status}</p>
        <h1>Trace Artifact {graph.selected_artifact_id}</h1>
        <div className="graph-search-toolbar">
          <label className="search-field">
            <span>Search lineage</span>
            <input
              type="search"
              value={searchText}
              onChange={(event) => setSearchText(event.target.value)}
              placeholder="Artifact id, step, output, path, hash..."
            />
          </label>
          <span className="status-line" role="status">
            {hasSearch
              ? `${searchArtifactIds.length} artifact${searchArtifactIds.length === 1 ? "" : "s"} match`
              : "Search highlights matching artifacts on the canvas."}
          </span>
        </div>
        {hasSearch ? (
          <ul className="compact-list" aria-label="Search matches">
            {searchArtifactIds.map((id) => {
              const artifact = artifactsById.get(id);
              return (
                <li key={id}>
                  <button
                    type="button"
                    className="graph-element-select"
                    aria-pressed={selectedArtifactId === id}
                    onClick={() => setSelection({ kind: "artifact", artifact_id: id })}
                  >
                    Select artifact {id}
                    {artifact
                      ? ` · ${artifact.step_name ?? "source"}.${artifact.output_name ?? "source"}`
                      : ""}
                  </button>
                </li>
              );
            })}
          </ul>
        ) : null}
        <LineageLegend />
        <Suspense fallback={<GraphCanvasLoadingFallback />}>
          <LineageGraphCanvas
            graph={graph}
            onSelectionChange={setSelection}
            searchArtifactIds={searchArtifactIds}
            selectedArtifactId={selectedArtifactId}
            selectedDependencyEdgeId={selectedDependencyEdgeId}
          />
        </Suspense>
        <LineageSelectionPanel
          neighborhood={neighborhood}
          selectedArtifact={selectedArtifact}
          selectedDependency={selectedDependency}
        />
      </section>
      <WarningList warnings={graph.warnings} />
      <section className="panel">
        <h2>Manifest Bindings</h2>
        <DataTable<TraceManifestBinding>
          rows={graph.manifest_bindings}
          getRowKey={(row) => `${row.run_id}:${row.workflow_name}:${row.step_name}:${row.role}:${row.manifest_name}`}
          columns={[
            { key: "workflow", label: "workflow", render: (row) => row.workflow_name },
            { key: "step", label: "step", render: (row) => row.step_name },
            { key: "role", label: "role", render: (row) => row.role },
            { key: "manifest", label: "manifest", render: (row) => row.manifest_name },
            {
              key: "digest",
              label: "digest",
              render: (row) => <IdentifierValue value={row.manifest_digest} compact />,
            },
            { key: "entities", label: "entities", render: (row) => row.entity_count },
          ]}
        />
      </section>
      <section className="panel">
        <h2>Artifacts</h2>
        <DataTable<TraceArtifact>
          rows={graph.artifacts}
          getRowKey={(row) => row.artifact_id}
          columns={[
            {
              key: "select",
              label: "",
              render: (row) => (
                <button
                  type="button"
                  className="graph-element-select"
                  aria-pressed={selectedArtifactId === row.artifact_id}
                  onClick={() => setSelection({ kind: "artifact", artifact_id: row.artifact_id })}
                >
                  Select artifact {row.artifact_id}
                </button>
              ),
            },
            { key: "id", label: "id", render: (row) => <IdentifierValue value={row.artifact_id} compact /> },
            { key: "origin", label: "origin", render: (row) => row.origin },
            { key: "step", label: "step", render: (row) => row.step_name ?? "source" },
            { key: "output", label: "output", render: (row) => row.output_name ?? "source" },
            { key: "address", label: "address", render: (row) => row.address ?? "none" },
            { key: "path", label: "path", render: (row) => <PathValue value={row.display_path} compact /> },
          ]}
        />
      </section>
      <section className="panel">
        <h2>Dependencies</h2>
        <DataTable<TraceDependency>
          rows={graph.dependencies}
          getRowKey={(row) => row.edge_id}
          columns={[
            {
              key: "select",
              label: "",
              render: (row) => (
                <button
                  type="button"
                  className="graph-element-select"
                  aria-pressed={selectedDependencyEdgeId === row.edge_id}
                  onClick={() => setSelection({ kind: "dependency", edge_id: row.edge_id })}
                >
                  Select dependency {row.source_artifact_id} → {row.dependent_artifact_id}
                </button>
              ),
            },
            { key: "source", label: "source", render: (row) => <IdentifierValue value={row.source_artifact_id} compact /> },
            { key: "dependent", label: "dependent", render: (row) => <IdentifierValue value={row.dependent_artifact_id} compact /> },
            { key: "reuse", label: "reuse", render: (row) => dependencyReuseLabel(row) },
            { key: "binding", label: "binding", render: (row) => row.binding_name },
            { key: "role", label: "role", render: (row) => row.dependency_role },
            { key: "set", label: "set", render: (row) => <IdentifierValue value={row.dependency_set_id} compact /> },
            { key: "cardinality", label: "cardinality", render: (row) => row.edge_cardinality ?? "none" },
            { key: "input", label: "input", render: (row) => <PathValue value={row.input_path} compact /> },
          ]}
        />
      </section>
    </div>
  );
}

function GraphCanvasLoadingFallback() {
  return (
    <div className="graph-frame">
      <div className="graph-shell graph-shell--loading" role="status" aria-live="polite">
        <p className="status-line">Loading graph canvas</p>
      </div>
    </div>
  );
}

function LineageLegend() {
  return (
    <div className="graph-legend" aria-label="Lineage graph legend">
      <span><i className="legend-swatch legend-source" />Source artifact</span>
      <span><i className="legend-swatch legend-workflow" />Workflow output</span>
      <span><i className="legend-swatch legend-published" />Published artifact</span>
      <span><i className="legend-line" />Dependency</span>
    </div>
  );
}

function dependencyReuseLabel(dependency: TraceDependency): string {
  return dependency.is_reused_input ? "reused" : "not reused";
}

function LineageSelectionPanel({
  neighborhood,
  selectedArtifact,
  selectedDependency,
}: {
  neighborhood: ReturnType<typeof buildArtifactNeighborhood>;
  selectedArtifact: TraceArtifact | null;
  selectedDependency: TraceDependency | null;
}) {
  if (!selectedArtifact && !selectedDependency) {
    return (
      <div className="selection-panel selection-panel--empty">
        Select an artifact or dependency edge in the graph to inspect it.
      </div>
    );
  }
  if (selectedDependency) {
    return (
      <div className="selection-panel">
        <h2>Selected Dependency</h2>
        <dl className="detail-grid">
          <DetailItem label="edge">
            <IdentifierValue value={selectedDependency.edge_id} />
          </DetailItem>
          <DetailItem label="binding">{selectedDependency.binding_name}</DetailItem>
          <DetailItem label="role">{selectedDependency.dependency_role}</DetailItem>
          <DetailItem label="reuse">
            {dependencyReuseLabel(selectedDependency)}
          </DetailItem>
          <DetailItem label="input">
            <PathValue value={selectedDependency.input_path} />
          </DetailItem>
          <DetailItem label="source">
            <IdentifierValue value={selectedDependency.source_artifact_id} />{" "}
            <Link to={`/artifacts/${selectedDependency.source_artifact_id}`}>open source</Link>
          </DetailItem>
          <DetailItem label="dependent">
            <IdentifierValue value={selectedDependency.dependent_artifact_id} />{" "}
            <Link to={`/artifacts/${selectedDependency.dependent_artifact_id}`}>open dependent</Link>
          </DetailItem>
          <DetailItem label="set">
            <IdentifierValue value={selectedDependency.dependency_set_id} />
          </DetailItem>
          <DetailItem label="cardinality">
            {selectedDependency.edge_cardinality ?? "none"}
          </DetailItem>
        </dl>
      </div>
    );
  }
  if (!selectedArtifact) {
    return null;
  }
  return (
    <div className="selection-panel">
      <h2>Selected Artifact</h2>
      <dl className="detail-grid">
        <DetailItem label="artifact">
          <IdentifierValue value={selectedArtifact.artifact_id} />{" "}
          <Link to={`/artifacts/${selectedArtifact.artifact_id}`}>open detail</Link>
        </DetailItem>
        <DetailItem label="origin">{selectedArtifact.origin}</DetailItem>
        <DetailItem label="workflow">{selectedArtifact.workflow_name ?? "none"}</DetailItem>
        <DetailItem label="step">{selectedArtifact.step_name ?? "source"}</DetailItem>
        <DetailItem label="output">{selectedArtifact.output_name ?? "source"}</DetailItem>
        <DetailItem label="address">{selectedArtifact.address ?? "none"}</DetailItem>
        <DetailItem label="path">
          <PathValue value={selectedArtifact.display_path} />
        </DetailItem>
        <DetailItem label="published">
          <PathValue value={selectedArtifact.published_path} />
        </DetailItem>
        <DetailItem label="staging">
          <PathValue value={selectedArtifact.staging_path} />
        </DetailItem>
        <DetailItem label="parameter">
          <IdentifierValue value={selectedArtifact.parameter_hash} />
        </DetailItem>
        <DetailItem label="content">
          <IdentifierValue value={selectedArtifact.content_digest} />
        </DetailItem>
        <DetailItem label="output hash">
          <IdentifierValue value={selectedArtifact.output_hash} />
        </DetailItem>
      </dl>
      <div className="neighborhood-grid">
        <NeighborhoodList
          title={`Upstream (${neighborhood.upstreamArtifacts.length})`}
          artifacts={neighborhood.upstreamArtifacts}
        />
        <NeighborhoodList
          title={`Downstream (${neighborhood.downstreamArtifacts.length})`}
          artifacts={neighborhood.downstreamArtifacts}
        />
      </div>
    </div>
  );
}

function DetailItem({
  children,
  label,
}: {
  children: ReactNode;
  label: string;
}) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

function NeighborhoodList({
  artifacts,
  title,
}: {
  artifacts: TraceArtifact[];
  title: string;
}) {
  const visibleArtifacts = artifacts.slice(0, 5);
  const hiddenCount = artifacts.length - visibleArtifacts.length;
  return (
    <div>
      <h3>{title}</h3>
      {visibleArtifacts.length === 0 ? (
        <p className="muted-value">none</p>
      ) : (
        <ul className="compact-list">
          {visibleArtifacts.map((artifact) => (
            <li key={artifact.artifact_id}>
              <IdentifierValue value={artifact.artifact_id} />{" "}
              {artifact.step_name ?? "source"}.{artifact.output_name ?? "source"}
              {artifact.address ? ` ${artifact.address}` : ""}
            </li>
          ))}
        </ul>
      )}
      {hiddenCount > 0 ? (
        <p className="muted-value">{hiddenCount} more not shown</p>
      ) : null}
    </div>
  );
}
