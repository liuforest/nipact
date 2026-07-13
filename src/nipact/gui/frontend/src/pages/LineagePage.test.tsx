import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { ObservedTopologyResponse, TraceGraphResponse } from "../api/types";
import type { TopologyGraphSelection } from "../lineage/TopologyGraphCanvas";
import { LineagePage } from "./LineagePage";

vi.mock("../lineage/TopologyGraphCanvas", () => ({
  TopologyGraphCanvas: ({
    onSelectionChange,
  }: {
    onSelectionChange?: (selection: TopologyGraphSelection | null) => void;
  }) => (
    <button
      type="button"
      data-testid="topology-canvas"
      onClick={() => onSelectionChange?.({ kind: "node", node_id: "n2" })}
    >
      Select topology node
    </button>
  ),
}));

vi.mock("../lineage/LineageGraphCanvas", () => ({
  LineageGraphCanvas: () => <div data-testid="lineage-canvas" />,
}));

const topology: ObservedTopologyResponse = {
  schema_version: 1,
  perspective: "observed",
  scope: "ancestor_closure",
  context: "colors",
  root_artifact_id: 2,
  root_node_id: "n2",
  provenance_status: "degraded",
  summary: {
    distinct_artifact_count: 2,
    registry_dependency_count: 1,
    node_count: 3,
    edge_count: 2,
  },
  nodes: [
    {
      kind: "source_input",
      node_id: "n0",
      workflow_name: "base",
      step_name: "finish",
      binding_name: "raw",
      dependency_role: "source_input",
      registry_artifact_count: 1,
    },
    {
      kind: "step",
      node_id: "n1",
      workflow_name: "base",
      step_name: "finish",
      produced_registry_artifact_count: 1,
    },
    {
      kind: "artifact_slot",
      node_id: "n2",
      workflow_name: "base",
      step_name: "finish",
      output_name: "result",
      registry_artifact_count: 1,
      distinct_address_count: 0,
    },
  ],
  edges: [
    {
      kind: "consumes",
      edge_id: "e0",
      source_node_id: "n0",
      target_node_id: "n1",
      workflow_name: "base",
      step_name: "finish",
      binding_name: "raw",
      dependency_role: "source_input",
      registry_dependency_count: 1,
    },
    {
      kind: "produces",
      edge_id: "e1",
      source_node_id: "n1",
      target_node_id: "n2",
    },
  ],
  manifest_bindings: [
    {
      workflow_name: "base",
      step_name: "finish",
      role: "analysis",
      manifest_name: "colors",
      distinct_run_count: 1,
      distinct_manifest_digest_count: 1,
      manifest_digest: "digest",
      manifest_hash: "hash",
      entity_count: 2,
    },
  ],
  warnings: [{ warning_type: "missing_artifact", occurrence_count: 2 }],
};

const graph: TraceGraphResponse = {
  schema_version: 1,
  context: "colors",
  selected_artifact_id: 2,
  provenance_status: "complete",
  artifacts: [
    {
      artifact_id: 2,
      origin: "workflow_output",
      run_id: 1,
      job_id: "job-1",
      artifact_set_id: null,
      path: "outputs/result.json",
      display_path: "outputs/result.json",
      is_selected: true,
      is_selected_output: true,
      is_published: true,
      published_path: "outputs/result.json",
      staging_path: "runs/result.json",
      workflow_name: "base",
      step_name: "finish",
      output_name: "result",
      address: "init",
      parameter_hash: "abc",
      content_digest: "2".repeat(64),
      output_hash: "2".repeat(16),
      file_size: 2,
      extension: ".json",
      subject_id: null,
      session_id: null,
      task_name: null,
      run_label: null,
      datatype: null,
      suffix: null,
      source_metadata: null,
      workflow_artifact_ref: "artifact:finish:result",
      callable_ref: "tests:finish",
      software_ref: "tests/0",
    },
  ],
  dependencies: [],
  manifest_bindings: [],
  warnings: [],
};

const multiWorkflowTopology: ObservedTopologyResponse = {
  ...topology,
  summary: { ...topology.summary, node_count: 5, edge_count: 3 },
  nodes: [
    ...topology.nodes,
    {
      kind: "step",
      node_id: "n3",
      workflow_name: "alternate",
      step_name: "finish",
      produced_registry_artifact_count: 1,
    },
    {
      kind: "artifact_slot",
      node_id: "n4",
      workflow_name: "alternate",
      step_name: "finish",
      output_name: "result",
      registry_artifact_count: 1,
      distinct_address_count: 0,
    },
  ],
  edges: [
    ...topology.edges,
    {
      kind: "produces",
      edge_id: "e2",
      source_node_id: "n3",
      target_node_id: "n4",
    },
  ],
};

function stubFetch({
  topologyResponse = topology,
  graphResponse = graph,
}: {
  topologyResponse?: ObservedTopologyResponse;
  graphResponse?: TraceGraphResponse;
} = {}) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/topology")) {
      return new Response(JSON.stringify(topologyResponse), { status: 200 });
    }
    if (url.includes("/lineage")) {
      return new Response(JSON.stringify(graphResponse), { status: 200 });
    }
    return new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function renderPage(renderLimit?: number) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/artifacts/2/lineage"]}>
        <Routes>
          <Route
            path="/artifacts/:artifactId/lineage"
            element={<LineagePage renderLimit={renderLimit} />}
          />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function requestedLineage(fetchMock: ReturnType<typeof stubFetch>): boolean {
  return fetchMock.mock.calls.some(([input]) =>
    String(input).includes("/lineage"),
  );
}

describe("LineagePage", () => {
  it("renders observed topology by default without requesting raw lineage", async () => {
    const fetchMock = stubFetch();

    renderPage();

    expect(
      await screen.findByRole("heading", { name: /Observed Topology/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Manifest Bindings" })).toBeInTheDocument();
    expect(screen.getByText("missing_artifact × 2")).toBeInTheDocument();
    expect(
      screen.getByText(
        "Provenance is degraded. Review the warnings below; unresolved records may be absent from the rendered topology.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByTitle("digest")).toBeInTheDocument();
    expect(requestedLineage(fetchMock)).toBe(false);
  });

  it("drives selection details from the accessible element list", async () => {
    stubFetch();

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );

    expect(screen.getByRole("heading", { name: "Selected Node" })).toBeInTheDocument();
    expect(screen.getByText("artifact_slot")).toBeInTheDocument();
  });

  it("drives the same selection details from the topology canvas", async () => {
    stubFetch();

    renderPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Select topology node" }),
    );

    expect(screen.getByRole("heading", { name: "Selected Node" })).toBeInTheDocument();
    expect(screen.getByText("artifact_slot")).toBeInTheDocument();
  });

  it("keeps repeated local names distinguishable in the accessible list", async () => {
    stubFetch({ topologyResponse: multiWorkflowTopology });

    renderPage();

    expect(
      await screen.findByRole("button", { name: "output: base.finish.result" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "step: base.finish" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "step: alternate.finish" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "output: alternate.finish.result" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "input: base.finish.raw (source_input)",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "consumes: base.finish.raw (source_input)",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "produces: alternate.finish → alternate.finish.result",
      }),
    ).toBeInTheDocument();
  });

  it("fetches raw lineage only after an explicit opt-in", async () => {
    const fetchMock = stubFetch();

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    expect(requestedLineage(fetchMock)).toBe(false);

    fireEvent.click(screen.getByRole("button", { name: "Show raw lineage" }));

    expect(
      await screen.findByRole("heading", { name: "Trace Artifact 2" }),
    ).toBeInTheDocument();
    expect(requestedLineage(fetchMock)).toBe(true);
  });

  it("refuses to render an oversized topology before mounting the explorer", async () => {
    stubFetch();

    renderPage(1);

    expect(
      await screen.findByRole("heading", {
        name: "Observed topology too large to render",
      }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Show raw lineage" })).toBeNull();
    expect(screen.queryByTestId("topology-canvas")).toBeNull();
  });

  it("counts manifest rows toward the topology render limit", async () => {
    stubFetch();

    renderPage(5);

    expect(
      await screen.findByRole("heading", {
        name: "Observed topology too large to render",
      }),
    ).toBeInTheDocument();
  });

  it("refuses oversized raw lineage after it is downloaded", async () => {
    const oversizedGraph: TraceGraphResponse = {
      ...graph,
      manifest_bindings: Array.from({ length: 6 }, (_, index) => ({
        run_id: index + 1,
        workflow_name: "base",
        step_name: "finish",
        role: "analysis",
        manifest_name: "colors",
        manifest_digest: `digest-${index}`,
        manifest_hash: `hash-${index}`,
        entity_count: 2,
      })),
    };
    stubFetch({ graphResponse: oversizedGraph });

    renderPage(6);
    fireEvent.click(
      await screen.findByRole("button", { name: "Show raw lineage" }),
    );

    expect(
      await screen.findByRole("heading", {
        name: "Raw lineage too large to render",
      }),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("lineage-canvas")).toBeNull();
  });
});
