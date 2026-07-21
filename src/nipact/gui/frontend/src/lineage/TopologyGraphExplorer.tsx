import { Suspense, lazy, useEffect, useMemo, useState, type ReactNode } from "react";
import type {
  ObservedTopologyResponse,
  TopologyEdge,
  TopologyExecutionPopulationSummary,
  TopologyManifestBindingSummary,
  TopologyNode,
} from "../api/types";
import { DataTable } from "../components/ui/DataTable";
import { IdentifierValue } from "../components/ui/IdentifierValue";
import { PathValue } from "../components/ui/PathValue";
import { WarningList } from "../components/ui/WarningList";
import type { TopologyGraphSelection } from "./TopologyGraphCanvas";
import {
  TopologyInstancePanel,
  type InstanceLineageState,
} from "./TopologyInstancePanel";

const TopologyGraphCanvas = lazy(async () => ({
  default: (await import("./TopologyGraphCanvas")).TopologyGraphCanvas,
}));

export function TopologyGraphExplorer({
  topology,
  onShowRawLineage,
  onRequestInstances,
  instancesRequested = false,
  instanceLineage,
}: {
  topology: ObservedTopologyResponse;
  onShowRawLineage?: () => void;
  onRequestInstances?: () => void;
  instancesRequested?: boolean;
  instanceLineage?: InstanceLineageState;
}) {
  const [selection, setSelection] = useState<TopologyGraphSelection | null>(null);
  useEffect(() => {
    setSelection(null);
  }, [topology.root_artifact_id]);

  const nodesById = useMemo(
    () => new Map(topology.nodes.map((node) => [node.node_id, node])),
    [topology.nodes],
  );
  const edgesById = useMemo(
    () => new Map(topology.edges.map((edge) => [edge.edge_id, edge])),
    [topology.edges],
  );

  const selectedNode =
    selection?.kind === "node" ? nodesById.get(selection.node_id) ?? null : null;
  const selectedEdge =
    selection?.kind === "edge" ? edgesById.get(selection.edge_id) ?? null : null;
  const selectedNodeId = selection?.kind === "node" ? selection.node_id : null;
  const selectedEdgeId = selection?.kind === "edge" ? selection.edge_id : null;
  const warnings = useMemo(
    () =>
      topology.warnings.map(
        (warning) => `${warning.warning_type} × ${warning.occurrence_count}`,
      ),
    [topology.warnings],
  );

  return (
    <div className="page-stack">
      <section className="panel">
        <p className="eyebrow">observed · {topology.provenance_status}</p>
        <h1>Observed Topology · Artifact {topology.root_artifact_id}</h1>
        {topology.provenance_status === "degraded" ? (
          <p className="status-line" role="status">
            Provenance is degraded. Review the warnings below; unresolved records
            may be absent from the rendered topology.
          </p>
        ) : null}
        {onShowRawLineage ? (
          <div className="graph-search-toolbar">
            <button
              type="button"
              className="button"
              onClick={onShowRawLineage}
            >
              Show raw lineage
            </button>
            <span className="status-line">
              The raw view lists every registry artifact and dependency row.
            </span>
          </div>
        ) : null}
        <TopologySummaryMetrics topology={topology} />
        <TopologyLegend />
        <Suspense fallback={<GraphCanvasLoadingFallback />}>
          <TopologyGraphCanvas
            topology={topology}
            onSelectionChange={setSelection}
            selectedNodeId={selectedNodeId}
            selectedEdgeId={selectedEdgeId}
          />
        </Suspense>
        <TopologyElementList
          topology={topology}
          selection={selection}
          onSelect={setSelection}
        />
        <TopologySelectionPanel
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          onRequestInstances={onRequestInstances}
          instancesRequested={instancesRequested}
        />
        {instancesRequested && selection && instanceLineage ? (
          <TopologyInstancePanel
            topology={topology}
            selection={selection}
            instanceLineage={instanceLineage}
          />
        ) : null}
      </section>
      <WarningList warnings={warnings} />
      <section className="panel">
        <h2>Execution Populations</h2>
        <DataTable<TopologyExecutionPopulationSummary>
          rows={topology.execution_populations}
          getRowKey={(row) => `${row.workflow_name}:${row.manifest_name}:${row.manifest_value_schema}`}
          columns={[
            { key: "workflow", label: "workflow", render: (row) => row.workflow_name },
            { key: "manifest", label: "manifest", render: (row) => row.manifest_name },
            { key: "schema", label: "schema", render: (row) => row.manifest_value_schema },
            { key: "runs", label: "runs", render: (row) => row.distinct_run_count },
            {
              key: "digest",
              label: "digest",
              render: (row) => row.manifest_digest === null ? "varies" : (
                <IdentifierValue value={row.manifest_digest} compact />
              ),
            },
            { key: "entities", label: "entities", render: (row) => row.entity_count ?? "varies" },
          ]}
        />
      </section>
      <section className="panel">
        <h2>Scientific Manifest Bindings</h2>
        <DataTable<TopologyManifestBindingSummary>
          rows={topology.manifest_bindings}
          getRowKey={(row) =>
            `${row.workflow_name}:${row.step_name}:${row.manifest_usage_role}:${row.manifest_name}`
          }
          columns={[
            { key: "workflow", label: "workflow", render: (row) => row.workflow_name },
            { key: "step", label: "step", render: (row) => row.step_name },
            { key: "role", label: "role", render: (row) => row.manifest_usage_role },
            { key: "manifest", label: "manifest", render: (row) => row.manifest_name },
            { key: "schema", label: "schema", render: (row) => row.manifest_value_schema },
            { key: "runs", label: "runs", render: (row) => row.distinct_run_count },
            {
              key: "digests",
              label: "digests",
              render: (row) => row.distinct_manifest_digest_count,
            },
            {
              key: "digest",
              label: "digest",
              render: (row) =>
                row.manifest_digest === null ? (
                  "varies"
                ) : (
                  <IdentifierValue value={row.manifest_digest} compact />
                ),
            },
            {
              key: "entities",
              label: "entities",
              render: (row) => row.entity_count ?? "varies",
            },
          ]}
        />
      </section>
    </div>
  );
}

function TopologySummaryMetrics({
  topology,
}: {
  topology: ObservedTopologyResponse;
}) {
  const { summary } = topology;
  return (
    <dl className="detail-grid">
      <DetailItem label="nodes">{summary.node_count}</DetailItem>
      <DetailItem label="edges">{summary.edge_count}</DetailItem>
      <DetailItem label="distinct artifacts">
        {summary.distinct_artifact_count}
      </DetailItem>
      <DetailItem label="dependency rows">
        {summary.registry_dependency_count}
      </DetailItem>
    </dl>
  );
}

function TopologyLegend() {
  return (
    <div className="graph-legend" aria-label="Observed topology graph legend">
      <span><i className="legend-swatch legend-workflow" />Workflow step</span>
      <span><i className="legend-swatch legend-published" />Artifact slot</span>
      <span><i className="legend-swatch legend-source" />Source input</span>
      <span><i className="legend-line" />Consumes / produces</span>
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

function TopologyElementList({
  topology,
  selection,
  onSelect,
}: {
  topology: ObservedTopologyResponse;
  selection: TopologyGraphSelection | null;
  onSelect: (selection: TopologyGraphSelection) => void;
}) {
  const nodesById = useMemo(
    () => new Map(topology.nodes.map((node) => [node.node_id, node])),
    [topology.nodes],
  );

  return (
    <div className="neighborhood-grid">
      <div>
        <h3>Nodes ({topology.nodes.length})</h3>
        <ul className="compact-list">
          {topology.nodes.map((node) => (
            <li key={node.node_id}>
              <button
                type="button"
                className="graph-element-select"
                aria-pressed={
                  selection?.kind === "node" && selection.node_id === node.node_id
                }
                onClick={() => onSelect({ kind: "node", node_id: node.node_id })}
              >
                {topologyNodeListLabel(node)}
              </button>
            </li>
          ))}
        </ul>
      </div>
      <div>
        <h3>Edges ({topology.edges.length})</h3>
        <ul className="compact-list">
          {topology.edges.map((edge) => (
            <li key={edge.edge_id}>
              <button
                type="button"
                className="graph-element-select"
                aria-pressed={
                  selection?.kind === "edge" && selection.edge_id === edge.edge_id
                }
                onClick={() => onSelect({ kind: "edge", edge_id: edge.edge_id })}
              >
                {topologyEdgeListLabel(edge, nodesById)}
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

function TopologySelectionPanel({
  selectedNode,
  selectedEdge,
  onRequestInstances,
  instancesRequested,
}: {
  selectedNode: TopologyNode | null;
  selectedEdge: TopologyEdge | null;
  onRequestInstances?: () => void;
  instancesRequested?: boolean;
}) {
  if (!selectedNode && !selectedEdge) {
    return (
      <div className="selection-panel selection-panel--empty">
        Select a node or edge in the graph or element list to inspect it.
      </div>
    );
  }
  // Lineage is fetched only when the user explicitly asks for instances; a bare
  // selection shows the aggregate coordinate above without requesting records.
  const instancesButton =
    onRequestInstances && !instancesRequested ? (
      <button type="button" className="button" onClick={onRequestInstances}>
        Show instances
      </button>
    ) : null;
  if (selectedEdge) {
    return (
      <div className="selection-panel">
        <h2>Selected Edge</h2>
        <dl className="detail-grid">
          <DetailItem label="edge">
            <IdentifierValue value={selectedEdge.edge_id} />
          </DetailItem>
          <DetailItem label="kind">{selectedEdge.kind}</DetailItem>
          <DetailItem label="source">
            <IdentifierValue value={selectedEdge.source_node_id} />
          </DetailItem>
          <DetailItem label="target">
            <IdentifierValue value={selectedEdge.target_node_id} />
          </DetailItem>
          {selectedEdge.kind === "consumes" ? (
            <>
              <DetailItem label="workflow">{selectedEdge.workflow_name}</DetailItem>
              <DetailItem label="step">{selectedEdge.step_name}</DetailItem>
              <DetailItem label="binding">{selectedEdge.binding_name}</DetailItem>
              <DetailItem label="role">{selectedEdge.dependency_role}</DetailItem>
              <DetailItem label="dependency rows">
                {selectedEdge.registry_dependency_count}
              </DetailItem>
            </>
          ) : null}
        </dl>
        {instancesButton}
      </div>
    );
  }
  if (!selectedNode) {
    return null;
  }
  return (
    <div className="selection-panel">
      <h2>Selected Node</h2>
      <dl className="detail-grid">
        <DetailItem label="node">
          <IdentifierValue value={selectedNode.node_id} />
        </DetailItem>
        <DetailItem label="kind">{selectedNode.kind}</DetailItem>
        {renderNodeDetails(selectedNode)}
      </dl>
      {instancesButton}
    </div>
  );
}

function renderNodeDetails(node: TopologyNode): ReactNode {
  switch (node.kind) {
    case "step":
      return (
        <>
          <DetailItem label="workflow">{node.workflow_name}</DetailItem>
          <DetailItem label="step">{node.step_name}</DetailItem>
          <DetailItem label="produced artifacts">
            {node.produced_registry_artifact_count}
          </DetailItem>
        </>
      );
    case "artifact_slot":
      return (
        <>
          <DetailItem label="workflow">{node.workflow_name}</DetailItem>
          <DetailItem label="step">{node.step_name}</DetailItem>
          <DetailItem label="output">{node.output_name}</DetailItem>
          <DetailItem label="artifact rows">
            {node.registry_artifact_count}
          </DetailItem>
          <DetailItem label="distinct addresses">
            {node.distinct_address_count}
          </DetailItem>
        </>
      );
    case "source_input":
      return (
        <>
          <DetailItem label="workflow">{node.workflow_name}</DetailItem>
          <DetailItem label="step">{node.step_name}</DetailItem>
          <DetailItem label="binding">{node.binding_name}</DetailItem>
          <DetailItem label="role">{node.dependency_role}</DetailItem>
          <DetailItem label="source artifacts">
            {node.registry_artifact_count}
          </DetailItem>
        </>
      );
    case "source_root":
      return (
        <>
          <DetailItem label="path">
            <PathValue value={node.display_path} />
          </DetailItem>
          <DetailItem label="source artifacts">
            {node.registry_artifact_count}
          </DetailItem>
        </>
      );
  }
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

function topologyNodeListLabel(node: TopologyNode): string {
  switch (node.kind) {
    case "step":
      return `step: ${node.workflow_name}.${node.step_name}`;
    case "artifact_slot":
      return `output: ${node.workflow_name}.${node.step_name}.${node.output_name}`;
    case "source_input":
      return `input: ${node.workflow_name}.${node.step_name}.${node.binding_name} (${node.dependency_role})`;
    case "source_root":
      return `source: ${node.display_path}`;
  }
}

function topologyEdgeListLabel(
  edge: TopologyEdge,
  nodesById: Map<string, TopologyNode>,
): string {
  if (edge.kind === "consumes") {
    return `consumes: ${edge.workflow_name}.${edge.step_name}.${edge.binding_name} (${edge.dependency_role})`;
  }
  const source = nodesById.get(edge.source_node_id);
  const target = nodesById.get(edge.target_node_id);
  return `produces: ${source ? topologyNodeCoordinate(source) : edge.source_node_id} → ${target ? topologyNodeCoordinate(target) : edge.target_node_id}`;
}

function topologyNodeCoordinate(node: TopologyNode): string {
  switch (node.kind) {
    case "step":
      return `${node.workflow_name}.${node.step_name}`;
    case "artifact_slot":
      return `${node.workflow_name}.${node.step_name}.${node.output_name}`;
    case "source_input":
      return `${node.workflow_name}.${node.step_name}.${node.binding_name} (${node.dependency_role})`;
    case "source_root":
      return node.display_path;
  }
}
