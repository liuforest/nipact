import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { TraceArtifact, TraceDependency, TraceGraphResponse } from "../api/types";
import { LineageGraphExplorer } from "./LineageGraphExplorer";

vi.mock("./LineageGraphCanvas", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./LineageGraphCanvas")>()),
  LineageGraphCanvas: ({
    focusRequest,
    onSelectionChange,
  }: {
    focusRequest?: { elementId: string; token: number } | null;
    onSelectionChange?: (selection: { kind: "artifact"; artifact_id: number }) => void;
  }) => (
    <div
      data-testid="lineage-canvas"
      data-focus-element={focusRequest?.elementId ?? ""}
      data-focus-token={focusRequest?.token ?? ""}
    >
      <button
        type="button"
        onClick={() => onSelectionChange?.({ kind: "artifact", artifact_id: 2 })}
      >
        canvas tap artifact 2
      </button>
    </div>
  ),
}));

function makeArtifact(overrides: Partial<TraceArtifact> & { artifact_id: number }): TraceArtifact {
  return {
    origin: "workflow_output",
    run_id: 1,
    job_id: null,
    artifact_set_id: null,
    path: `outputs/result-${overrides.artifact_id}.json`,
    display_path: `outputs/result-${overrides.artifact_id}.json`,
    is_selected: false,
    is_selected_output: true,
    is_published: true,
    published_path: null,
    staging_path: null,
    workflow_name: "base",
    step_name: "step",
    output_name: "result",
    address: null,
    parameter_hash: null,
    content_digest: `${overrides.artifact_id}`.repeat(64),
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
    ...overrides,
  };
}

const dependency: TraceDependency = {
  edge_id: "e0",
  source_artifact_id: 1,
  dependent_artifact_id: 2,
  is_reused_input: false,
  dependency_role: "analysis_input",
  binding_name: "raw",
  input_path: "outputs/result-1.json",
  source_content_digest: "1".repeat(64),
  source_file_size: 1,
  source_extension: ".json",
  dependency_set_id: null,
  manifest_value_schema: null,
  manifest_digest: null,
  edge_cardinality: null,
};

const graph: TraceGraphResponse = {
  schema_version: 2,
  context: "colors",
  selected_artifact_id: 2,
  provenance_status: "complete",
  artifacts: [
    makeArtifact({ artifact_id: 1, step_name: "alpha", output_name: "out1" }),
    makeArtifact({ artifact_id: 2, step_name: "beta", output_name: "out2" }),
  ],
  dependencies: [dependency],
  execution_populations: [],
  manifest_bindings: [],
  warnings: [],
};

function renderExplorer() {
  return render(
    <MemoryRouter>
      <LineageGraphExplorer graph={graph} />
    </MemoryRouter>,
  );
}

describe("LineageGraphExplorer", () => {
  it("starts with an empty selection panel", () => {
    renderExplorer();
    expect(
      screen.getByText("Select an artifact or dependency edge in the graph to inspect it."),
    ).toBeInTheDocument();
  });

  it("selects an artifact from the table and exposes its detail link", () => {
    renderExplorer();

    const selectButton = screen.getByRole("button", { name: "Select artifact 1" });
    expect(selectButton).toHaveAttribute("aria-pressed", "false");

    fireEvent.click(selectButton);

    expect(screen.getByRole("heading", { name: "Selected Artifact" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Select artifact 1" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByRole("button", { name: "Select artifact 2" })).toHaveAttribute(
      "aria-pressed",
      "false",
    );
    expect(screen.getByRole("link", { name: "open detail" })).toHaveAttribute(
      "href",
      "/artifacts/1",
    );
  });

  it("selects a dependency from the table and links both endpoints", () => {
    renderExplorer();

    const selectButton = screen.getByRole("button", { name: "Select dependency 1 → 2" });
    fireEvent.click(selectButton);

    expect(screen.getByRole("heading", { name: "Selected Dependency" })).toBeInTheDocument();
    expect(selectButton).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("link", { name: "open source" })).toHaveAttribute(
      "href",
      "/artifacts/1",
    );
    expect(screen.getByRole("link", { name: "open dependent" })).toHaveAttribute(
      "href",
      "/artifacts/2",
    );
  });

  it("hides the search results list until a query is entered", () => {
    renderExplorer();

    expect(
      screen.getByText("Search highlights matching artifacts on the canvas."),
    ).toBeInTheDocument();
    expect(screen.queryByRole("list", { name: "Search matches" })).toBeNull();
  });

  it("lists matching artifacts and drives selection from a search result", () => {
    renderExplorer();

    fireEvent.change(screen.getByRole("searchbox", { name: "Search lineage" }), {
      target: { value: "alpha" },
    });

    expect(screen.getByText("1 artifact match")).toBeInTheDocument();
    const results = screen.getByRole("list", { name: "Search matches" });
    const resultButton = within(results).getByRole("button", {
      name: "Select artifact 1 · alpha.out1",
    });

    fireEvent.click(resultButton);

    expect(screen.getByRole("heading", { name: "Selected Artifact" })).toBeInTheDocument();
    expect(resultButton).toHaveAttribute("aria-pressed", "true");
  });

  it("pluralizes the match count", () => {
    renderExplorer();

    const searchbox = screen.getByRole("searchbox", { name: "Search lineage" });

    fireEvent.change(searchbox, { target: { value: "outputs" } });
    expect(screen.getByText("2 artifacts match")).toBeInTheDocument();

    fireEvent.change(searchbox, { target: { value: "no-such-artifact" } });
    expect(screen.getByText("0 artifacts match")).toBeInTheDocument();
  });

  it("requests focus with a bumping token from external controls", () => {
    renderExplorer();
    const canvas = screen.getByTestId("lineage-canvas");
    expect(canvas).toHaveAttribute("data-focus-token", "");

    fireEvent.click(screen.getByRole("button", { name: "Select artifact 1" }));
    expect(canvas).toHaveAttribute("data-focus-element", "artifact:1");
    expect(canvas).toHaveAttribute("data-focus-token", "1");

    fireEvent.click(screen.getByRole("button", { name: "Select dependency 1 → 2" }));
    expect(canvas).toHaveAttribute("data-focus-element", "e0");
    expect(canvas).toHaveAttribute("data-focus-token", "2");

    // Re-selecting the same element still refires (new token).
    fireEvent.click(screen.getByRole("button", { name: "Select dependency 1 → 2" }));
    expect(canvas).toHaveAttribute("data-focus-token", "3");
  });

  it("does not request focus when the canvas drives selection", () => {
    renderExplorer();
    const canvas = screen.getByTestId("lineage-canvas");

    fireEvent.click(screen.getByRole("button", { name: "canvas tap artifact 2" }));

    expect(screen.getByRole("heading", { name: "Selected Artifact" })).toBeInTheDocument();
    expect(screen.getByText("historical staging (may not exist)")).toBeInTheDocument();
    expect(canvas).toHaveAttribute("data-focus-token", "");
  });

  it("clears the focus request when the graph changes", () => {
    const { rerender } = renderExplorer();

    fireEvent.click(screen.getByRole("button", { name: "Select artifact 1" }));
    expect(screen.getByTestId("lineage-canvas")).toHaveAttribute("data-focus-token", "1");

    rerender(
      <MemoryRouter>
        <LineageGraphExplorer graph={{ ...graph, selected_artifact_id: 1 }} />
      </MemoryRouter>,
    );

    expect(screen.getByTestId("lineage-canvas")).toHaveAttribute("data-focus-token", "");
  });
});
