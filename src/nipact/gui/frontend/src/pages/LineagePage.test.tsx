import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { TraceGraphResponse } from "../api/types";
import type { LineageGraphSelection } from "../lineage/LineageGraphCanvas";
import { LineagePage } from "./LineagePage";

vi.mock("../lineage/LineageGraphCanvas", () => ({
  LineageGraphCanvas: ({
    onSelectionChange,
  }: {
    onSelectionChange?: (selection: LineageGraphSelection | null) => void;
  }) => (
    <button
      type="button"
      onClick={() =>
        onSelectionChange?.({
          kind: "artifact",
          artifact_id: 2,
        })
      }
    >
      Select artifact
    </button>
  ),
}));

const graph: TraceGraphResponse = {
  schema_version: 1,
  context: "colors",
  selected_artifact_id: 2,
  provenance_status: "complete",
  artifacts: [
    {
      artifact_id: 1,
      origin: "source",
      run_id: null,
      job_id: null,
      artifact_set_id: null,
      path: "data/source.json",
      display_path: "data/source.json",
      is_selected: false,
      is_selected_output: false,
      is_published: false,
      published_path: null,
      staging_path: null,
      workflow_name: null,
      step_name: null,
      output_name: null,
      address: null,
      parameter_hash: null,
      content_digest: "1".repeat(64),
      output_hash: null,
      file_size: 1,
      extension: ".json",
      subject_id: null,
      session_id: null,
      task_name: null,
      run_label: null,
      datatype: null,
      suffix: null,
      source_metadata: null,
      workflow_artifact_ref: null,
      callable_ref: null,
      software_ref: null,
    },
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
  dependencies: [
    {
      edge_id: "dependency:1:2:raw",
      source_artifact_id: 1,
      dependent_artifact_id: 2,
      is_reused_input: false,
      dependency_role: "source_input",
      binding_name: "raw",
      input_path: "data/source.json",
      source_content_digest: "0".repeat(64),
      source_file_size: 100,
      source_extension: ".json",
      dependency_set_id: null,
      manifest_digest: null,
      edge_cardinality: null,
    },
  ],
  manifest_bindings: [
    {
      run_id: 1,
      workflow_name: "base",
      step_name: "finish",
      role: "analysis",
      manifest_name: "colors",
      manifest_digest: "digest",
      manifest_hash: "hash",
      entity_count: 2,
    },
  ],
  warnings: [],
};

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/artifacts/2/lineage"]}>
        <Routes>
          <Route path="/artifacts/:artifactId/lineage" element={<LineagePage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("LineagePage", () => {
  it("renders search, selected artifact details, and manifest bindings", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(graph), { status: 200 })),
    );

    renderPage();

    expect(await screen.findByRole("heading", { name: "Trace Artifact 2" })).toBeInTheDocument();
    expect(screen.getByText("not reused")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "finish" } });
    expect(screen.getByText("1 artifact highlighted")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Select artifact" }));

    expect(screen.getByRole("heading", { name: "Selected Artifact" })).toBeInTheDocument();
    expect(screen.getByText("Upstream (1)")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Manifest Bindings" })).toBeInTheDocument();
    expect(screen.getByText("colors")).toBeInTheDocument();
  });
});
