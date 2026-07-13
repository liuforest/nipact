import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { ObservedTopologyResponse, TraceGraphResponse } from "../api/types";
import { LineagePage } from "./LineagePage";

vi.mock("../lineage/TopologyGraphCanvas", () => ({
  TopologyGraphCanvas: () => <div data-testid="topology-canvas" />,
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
  provenance_status: "complete",
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

function stubFetch() {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/topology")) {
      return new Response(JSON.stringify(topology), { status: 200 });
    }
    if (url.includes("/lineage")) {
      return new Response(JSON.stringify(graph), { status: 200 });
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
    expect(requestedLineage(fetchMock)).toBe(false);
  });

  it("drives selection details from the accessible element list", async () => {
    stubFetch();

    renderPage();

    await screen.findByRole("heading", { name: /Observed Topology/ });
    fireEvent.click(screen.getByRole("button", { name: "output: finish.result" }));

    expect(screen.getByRole("heading", { name: "Selected Node" })).toBeInTheDocument();
    expect(screen.getByText("artifact_slot")).toBeInTheDocument();
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
});
