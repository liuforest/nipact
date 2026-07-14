import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type {
  Artifact,
  ArtifactGroupCount,
  ArtifactGroupsResponse,
  ArtifactsResponse,
} from "../api/types";
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

function groupCount(overrides: Partial<ArtifactGroupCount>): ArtifactGroupCount {
  return {
    origin: "workflow_output",
    workflow_name: "base",
    step_name: "color_sector_analysis",
    output_name: "sector_counts",
    artifact_count: 1,
    ...overrides,
  };
}

// Route the fetch mock by path: the groups endpoint serves the browse tree, the
// collection endpoint serves lazily-loaded rows (and the search scope).
function renderPage(
  responses: { groups: ArtifactGroupsResponse; artifacts?: ArtifactsResponse },
  initialEntry = "/artifacts",
) {
  const fetchMock = vi.fn(async (input: string) => {
    if (input.startsWith("/api/artifacts/groups")) {
      return new Response(JSON.stringify(responses.groups), { status: 200 });
    }
    return new Response(
      JSON.stringify(
        responses.artifacts ?? { context: "colors", artifacts: [] },
      ),
      { status: 200 },
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route path="/artifacts" element={<ArtifactsPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return fetchMock;
}

describe("ArtifactsPage", () => {
  it("loads only the group summary on a bare page load, not the whole population", async () => {
    const fetchMock = renderPage({
      groups: {
        context: "colors",
        groups: [
          groupCount({
            origin: "source",
            workflow_name: null,
            step_name: null,
            output_name: null,
            artifact_count: 1,
          }),
          groupCount({ artifact_count: 4 }),
        ],
      },
    });

    expect(await screen.findByRole("heading", { name: "Artifacts" })).toBeInTheDocument();
    // The status line sums the group counts without holding any rows.
    expect(await screen.findByText("5 artifacts in 2 groups")).toBeInTheDocument();

    // Only the groups endpoint is hit; no bare /api/artifacts population fetch.
    const requested = fetchMock.mock.calls.map((call) => call[0]);
    expect(requested).toContain("/api/artifacts/groups");
    expect(requested.every((url) => url.startsWith("/api/artifacts/groups"))).toBe(true);

    // Groups render collapsed: coordinate labels are shown, rows are not.
    expect(screen.getByText("workflow_output")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "trace" })).not.toBeInTheDocument();
  });

  it("fetches a group's rows only when it is opened", async () => {
    const fetchMock = renderPage({
      groups: {
        context: "colors",
        groups: [groupCount({ artifact_count: 1 })],
      },
      artifacts: {
        context: "colors",
        artifacts: [artifact({ artifact_id: 42, step_name: "color_sector_analysis", output_name: "sector_counts" })],
      },
    });

    expect(await screen.findByText("workflow_output")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "42" })).not.toBeInTheDocument();

    // jsdom dispatches the <details> toggle event asynchronously, so each level
    // mounts a tick after its parent opens — await before clicking the next.
    fireEvent.click(screen.getByText("workflow_output"));
    fireEvent.click(await screen.findByText("workflow: base"));
    fireEvent.click(await screen.findByText("step: color_sector_analysis"));
    fireEvent.click(await screen.findByText("output: sector_counts"));

    expect(await screen.findByRole("link", { name: "42" })).toHaveAttribute(
      "href",
      "/artifacts/42",
    );

    // The leaf request carries the compound coordinate.
    const leafUrl = fetchMock.mock.calls
      .map((call) => call[0])
      .find((url) => url.startsWith("/api/artifacts?"));
    expect(leafUrl).toBeDefined();
    const params = new URLSearchParams((leafUrl ?? "").split("?")[1] ?? "");
    expect(params.get("workflow")).toBe("base");
    expect(params.get("step")).toBe("color_sector_analysis");
    expect(params.get("output")).toBe("sector_counts");
  });

  it("loads and filters the scope when a search is submitted", async () => {
    renderPage({
      groups: { context: "colors", groups: [groupCount({ artifact_count: 2 })] },
      artifacts: {
        context: "colors",
        artifacts: [
          artifact({ artifact_id: 2, step_name: "color_local_transform", output_name: "local_color" }),
          artifact({ artifact_id: 3, step_name: "color_sector_analysis", output_name: "sector_counts" }),
        ],
      },
    });

    expect(await screen.findByText(/Browsing artifact groups/)).toBeInTheDocument();

    fireEvent.change(screen.getByRole("searchbox", { name: "Search artifacts" }), {
      target: { value: "sector" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    expect(await screen.findByText("Showing 1 of 2 artifacts")).toBeInTheDocument();
    expect(screen.getByText("step: color_sector_analysis")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "3" })).toHaveAttribute("href", "/artifacts/3");
    expect(screen.queryByText("step: color_local_transform")).not.toBeInTheDocument();
  });

  it("reads filters from the URL and forwards them to the groups request", async () => {
    const fetchMock = renderPage(
      { groups: { context: "colors", groups: [groupCount({ artifact_count: 1 })] } },
      "/artifacts?workflow=base&step=color_sector_analysis",
    );

    expect(await screen.findByRole("heading", { name: "Artifacts" })).toBeInTheDocument();

    await waitFor(() => {
      expect(
        fetchMock.mock.calls.some((call) =>
          call[0].startsWith("/api/artifacts/groups?"),
        ),
      ).toBe(true);
    });
    const url = fetchMock.mock.calls
      .map((call) => call[0])
      .find((value) => value.startsWith("/api/artifacts/groups?"));
    const params = new URLSearchParams((url ?? "").split("?")[1] ?? "");
    expect(params.get("workflow")).toBe("base");
    expect(params.get("step")).toBe("color_sector_analysis");
  });

  it("shows the active-filter summary and offers a clear-filters link", async () => {
    renderPage(
      { groups: { context: "colors", groups: [groupCount({ artifact_count: 1 })] } },
      "/artifacts?workflow=base",
    );

    expect(await screen.findByText("workflow = base")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Clear filters" })).toHaveAttribute(
      "href",
      "/artifacts",
    );
  });
});
