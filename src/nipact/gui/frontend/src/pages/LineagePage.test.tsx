import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useNavigate } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type {
  ObservedTopologyResponse,
  TraceArtifact,
  TraceGraphResponse,
} from "../api/types";
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

// 120 workflow-output artifacts in the base.finish.result slot, so instance
// paging (page size 50) spans three pages.
const bigGraph: TraceGraphResponse = {
  ...graph,
  artifacts: Array.from(
    { length: 120 },
    (_, index): TraceArtifact => ({
      ...graph.artifacts[0],
      artifact_id: index + 1,
      address: `addr-${index + 1}`,
      display_path: `outputs/result-${index + 1}.json`,
    }),
  ),
};

function stubFetch({
  topologyResponse = topology,
  graphResponse = graph,
  lineageStatus = 200,
}: {
  topologyResponse?: ObservedTopologyResponse;
  graphResponse?: TraceGraphResponse;
  lineageStatus?: number;
} = {}) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/topology")) {
      return new Response(JSON.stringify(topologyResponse), { status: 200 });
    }
    if (url.includes("/lineage")) {
      if (lineageStatus !== 200) {
        return new Response(
          JSON.stringify({ code: "boom", message: "lineage failed" }),
          { status: lineageStatus },
        );
      }
      return new Response(JSON.stringify(graphResponse), { status: 200 });
    }
    return new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function lineageRequestCount(fetchMock: ReturnType<typeof stubFetch>): number {
  return fetchMock.mock.calls.filter(([input]) =>
    String(input).includes("/lineage"),
  ).length;
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

// Renders the page under a route whose :artifactId can change without
// remounting LineagePage, so per-root laziness is exercised across navigation.
function renderWithNavigation() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  function Harness() {
    const navigate = useNavigate();
    return (
      <>
        <button type="button" onClick={() => navigate("/artifacts/3/lineage")}>
          Go to artifact 3
        </button>
        <LineagePage />
      </>
    );
  }
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/artifacts/2/lineage"]}>
        <Routes>
          <Route path="/artifacts/:artifactId/lineage" element={<Harness />} />
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

  it("loads instances lazily and reuses the cached lineage for later selections", async () => {
    const fetchMock = stubFetch();

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    // Selecting an element alone must not request raw lineage.
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );
    expect(lineageRequestCount(fetchMock)).toBe(0);

    fireEvent.click(screen.getByRole("button", { name: "Show instances" }));
    expect(
      await screen.findByRole("heading", { name: /Instances/ }),
    ).toBeInTheDocument();
    expect(await screen.findByText("init")).toBeInTheDocument();
    expect(lineageRequestCount(fetchMock)).toBe(1);

    // Switching selection re-resolves from the cached response, no new fetch.
    fireEvent.click(
      screen.getByRole("button", {
        name: "input: base.finish.raw (source_input)",
      }),
    );
    await screen.findByRole("heading", { name: /Instances/ });
    expect(lineageRequestCount(fetchMock)).toBe(1);
  });

  it("does not auto-request lineage when navigating to a new root", async () => {
    const fetchMock = stubFetch();

    renderWithNavigation();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Show instances" }));
    await screen.findByRole("heading", { name: /Instances/ });
    expect(lineageRequestCount(fetchMock)).toBe(1);

    // Navigating to a different root must start collapsed: the stale latch from
    // artifact 2 must not enable the artifact-3 lineage query on first render.
    fireEvent.click(screen.getByRole("button", { name: "Go to artifact 3" }));
    await screen.findByRole("heading", { name: /Observed Topology/ });
    expect(lineageRequestCount(fetchMock)).toBe(1);
  });

  it("reuses the instance lineage cache when entering raw mode", async () => {
    const fetchMock = stubFetch();

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Show instances" }));
    await screen.findByRole("heading", { name: /Instances/ });
    expect(lineageRequestCount(fetchMock)).toBe(1);

    fireEvent.click(screen.getByRole("button", { name: "Show raw lineage" }));
    expect(
      await screen.findByRole("heading", { name: "Trace Artifact 2" }),
    ).toBeInTheDocument();
    // The raw view shares the lineage cache entry already populated for
    // instances, so it must not trigger a second fetch.
    expect(lineageRequestCount(fetchMock)).toBe(1);
  });

  it("pages instance records and resets to the first page on selection change", async () => {
    stubFetch({ graphResponse: bigGraph });

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Show instances" }));

    expect(await screen.findByText("addr-1")).toBeInTheDocument();
    expect(screen.getByText("addr-50")).toBeInTheDocument();
    expect(screen.queryByText("addr-51")).toBeNull();
    expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(await screen.findByText("addr-51")).toBeInTheDocument();
    expect(screen.getByText("addr-100")).toBeInTheDocument();
    expect(screen.queryByText("addr-1")).toBeNull();
    expect(screen.getByText("Page 2 of 3")).toBeInTheDocument();

    // Changing the inspected element resets paging back to the first page.
    fireEvent.click(screen.getByRole("button", { name: "step: base.finish" }));
    expect(await screen.findByText("addr-1")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();
  });

  it("filters instances by scoped search and resets paging", async () => {
    stubFetch({ graphResponse: bigGraph });

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Show instances" }));
    await screen.findByText("addr-1");

    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(await screen.findByText("Page 2 of 3")).toBeInTheDocument();

    fireEvent.change(
      screen.getByRole("searchbox", { name: "Search instances" }),
      { target: { value: "ADDR-117" } },
    );

    expect(await screen.findByText("addr-117")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 1")).toBeInTheDocument();
    expect(screen.queryByText("addr-1")).toBeNull();
  });

  it("keeps an instance load error inside the topology page", async () => {
    stubFetch({ lineageStatus: 500 });

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(
      screen.getByRole("button", { name: "output: base.finish.result" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Show instances" }));

    expect(
      await screen.findByText(/Could not load instance records/),
    ).toBeInTheDocument();
    // The topology view is still present; no full-page error panel replaces it.
    expect(
      screen.getByRole("heading", { name: /Observed Topology/ }),
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

  // The lineage breadcrumb renders above the branch switch, so its list + detail
  // exits are present on every valid-id view — success and both refusals alike.
  function expectLineageBreadcrumb() {
    const nav = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(within(nav).getByRole("link", { name: "Artifacts" })).toHaveAttribute(
      "href",
      "/artifacts",
    );
    expect(within(nav).getByRole("link", { name: "Artifact 2" })).toHaveAttribute(
      "href",
      "/artifacts/2",
    );
    // "Lineage" is the current page, not a link.
    expect(within(nav).getByText("Lineage")).toBeInTheDocument();
    expect(within(nav).queryByRole("link", { name: "Lineage" })).toBeNull();
  }

  it("shows the lineage breadcrumb on the observed topology view", async () => {
    stubFetch();

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    expectLineageBreadcrumb();
  });

  it("shows the lineage breadcrumb in raw mode", async () => {
    stubFetch();

    renderPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Show raw lineage" }),
    );
    await screen.findByRole("heading", { name: "Trace Artifact 2" });
    expectLineageBreadcrumb();
  });

  it("keeps the breadcrumb exits on the oversized-topology refusal", async () => {
    stubFetch();

    renderPage(1);

    await screen.findByRole("heading", {
      name: "Observed topology too large to render",
    });
    expectLineageBreadcrumb();
    // No unsafe "show raw anyway" bypass from the refusal state.
    expect(screen.queryByRole("button", { name: "Show raw lineage" })).toBeNull();
  });

  it("keeps the breadcrumb and back-to-topology exit on the oversized-raw refusal", async () => {
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

    await screen.findByRole("heading", {
      name: "Raw lineage too large to render",
    });
    expectLineageBreadcrumb();
    expect(
      screen.getByRole("button", { name: "Back to observed topology" }),
    ).toBeInTheDocument();
  });
});
