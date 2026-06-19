import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { Artifact, ArtifactsResponse } from "../api/types";
import { ArtifactsPage } from "./ArtifactsPage";

function artifact(overrides: Partial<Artifact>): Artifact {
  return {
    artifact_id: 1,
    origin: "workflow_output",
    run_id: 1,
    job_id: null,
    artifact_set_id: null,
    path: "runs/colors/base/step/output/color_000.json",
    display_path: "runs/colors/base/step/output/color_000.json",
    is_selected_output: false,
    is_published: false,
    published_path: null,
    staging_path: null,
    workflow_name: "base",
    step_name: "step",
    output_name: "output",
    address: "color_000",
    parameter_hash: null,
    parameter_digest: null,
    content_digest: "a".repeat(64),
    output_hash: null,
    file_size: 12,
    extension: ".json",
    subject_id: null,
    session_id: null,
    task_name: null,
    run_label: null,
    datatype: null,
    suffix: null,
    source_metadata: null,
    workflow_artifact_ref: "artifact:step:output",
    callable_ref: null,
    software_ref: null,
    created_at: "2026-06-04T00:00:00",
    lineage_url: "/api/artifacts/1/lineage",
    ...overrides,
  };
}

function renderPage(response: ArtifactsResponse) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify(response), { status: 200 })),
  );
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/artifacts"]}>
        <Routes>
          <Route path="/artifacts" element={<ArtifactsPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ArtifactsPage", () => {
  it("renders grouped artifact sections and row links", async () => {
    renderPage({
      context: "colors",
      artifacts: [
        artifact({
          artifact_id: 1,
          origin: "source",
          workflow_name: null,
          step_name: null,
          output_name: null,
          path: "data/color_source.json",
          display_path: "data/color_source.json",
          lineage_url: "/api/artifacts/1/lineage",
        }),
        artifact({
          artifact_id: 2,
          step_name: "color_sector_analysis",
          output_name: "sector_counts",
          is_selected_output: true,
          is_published: true,
          display_path: "outputs/colors/base/color_sector_analysis/sector_counts/color_000.json",
          lineage_url: "/api/artifacts/2/lineage",
        }),
      ],
    });

    expect(await screen.findByRole("heading", { name: "Artifacts" })).toBeInTheDocument();
    expect(screen.getByText("Showing 2 of 2 artifacts")).toBeInTheDocument();
    expect(screen.getAllByText("source").length).toBeGreaterThan(0);
    expect(screen.getByText("workflow_output")).toBeInTheDocument();
    expect(screen.getByText("step: color_sector_analysis")).toBeInTheDocument();
    expect(screen.getByText("output: sector_counts")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "2" })).toHaveAttribute("href", "/artifacts/2");
    expect(screen.getAllByRole("link", { name: "trace" })).toHaveLength(2);
    expect(screen.getByText("published")).toBeInTheDocument();
    expect(screen.getByText("selected output")).toBeInTheDocument();
  });

  it("filters artifacts within the loaded list", async () => {
    renderPage({
      context: "colors",
      artifacts: [
        artifact({ artifact_id: 2, step_name: "color_local_transform", output_name: "local_color" }),
        artifact({
          artifact_id: 3,
          step_name: "color_sector_analysis",
          output_name: "sector_counts",
          address: "color_123",
        }),
      ],
    });

    expect(await screen.findByText("Showing 2 of 2 artifacts")).toBeInTheDocument();

    fireEvent.change(screen.getByRole("searchbox", { name: "Search artifacts" }), {
      target: { value: "sector" },
    });

    expect(screen.getByText("Showing 1 of 2 artifacts")).toBeInTheDocument();
    expect(screen.getByText("step: color_sector_analysis")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "3" })).toHaveAttribute("href", "/artifacts/3");
    expect(screen.queryByText("step: color_local_transform")).not.toBeInTheDocument();
  });

  it("does not render rows for collapsed groups until search opens matches", async () => {
    renderPage({
      context: "colors",
      artifacts: [
        artifact({
          artifact_id: 4,
          step_name: "color_candidate_select",
          output_name: "selected_color",
          is_selected_output: false,
          is_published: false,
        }),
      ],
    });

    expect(await screen.findByText("workflow_output")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "4" })).not.toBeInTheDocument();

    fireEvent.change(screen.getByRole("searchbox", { name: "Search artifacts" }), {
      target: { value: "candidate" },
    });

    expect(screen.getByRole("link", { name: "4" })).toHaveAttribute("href", "/artifacts/4");
  });
});
