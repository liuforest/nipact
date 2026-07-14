import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { ArtifactDetail } from "../api/types";
import { ArtifactDetailPage } from "./ArtifactDetailPage";

function detail(overrides: Partial<ArtifactDetail> = {}): ArtifactDetail {
  return {
    artifact_id: 7,
    origin: "workflow_output",
    run_id: 1,
    job_id: null,
    artifact_set_id: null,
    path: "runs/colors/base/step/output/color_007.json",
    display_path: "runs/colors/base/step/output/color_007.json",
    is_selected_output: false,
    is_published: false,
    published_path: null,
    staging_path: null,
    workflow_name: "base",
    step_name: "step",
    output_name: "output",
    address: "color_007",
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
    workflow_artifact_ref: "artifact:step:output",
    callable_ref: null,
    software_ref: null,
    created_at: "2026-06-04T00:00:00",
    source_metadata: null,
    ...overrides,
  };
}

function renderPage(response: ArtifactDetail = detail()) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify(response), { status: 200 })),
  );
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/artifacts/7"]}>
        <Routes>
          <Route path="/artifacts/:artifactId" element={<ArtifactDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ArtifactDetailPage", () => {
  it("renders a detail breadcrumb back to the list, with the artifact as the current page", async () => {
    renderPage();

    const nav = await screen.findByRole("navigation", { name: "Breadcrumb" });
    expect(within(nav).getByRole("link", { name: "Artifacts" })).toHaveAttribute(
      "href",
      "/artifacts",
    );
    // The artifact segment is the current page, not a link.
    expect(within(nav).getByText("Artifact 7")).toBeInTheDocument();
    expect(within(nav).queryByRole("link", { name: "Artifact 7" })).toBeNull();
  });

  it("keeps exactly one Trace action pointing at the lineage route", async () => {
    renderPage();

    await screen.findByRole("navigation", { name: "Breadcrumb" });
    const traceLinks = screen.getAllByRole("link", { name: /trace/i });
    expect(traceLinks).toHaveLength(1);
    expect(traceLinks[0]).toHaveAttribute("href", "/artifacts/7/lineage");

    // The duplicate lower "Trace" panel is gone.
    expect(screen.queryByRole("heading", { name: "Trace" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Trace artifact" })).toBeNull();
  });
});
