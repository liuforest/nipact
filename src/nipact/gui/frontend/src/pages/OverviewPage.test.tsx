import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { SummaryResponse } from "../api/types";
import { OverviewPage } from "./OverviewPage";

function summary(overrides: Partial<SummaryResponse> = {}): SummaryResponse {
  return {
    context: "colors",
    workflow_count: 2,
    runnable_step_count: 5,
    manifest_count: 3,
    artifact_count: 142,
    source_artifact_count: 12,
    workflow_output_count: 130,
    workflow_run_count: 4,
    ...overrides,
  };
}

function renderPage(response: SummaryResponse = summary()) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify(response), { status: 200 })),
  );
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <OverviewPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("OverviewPage", () => {
  it("links each honest metric to its destination", async () => {
    renderPage();

    expect(
      await screen.findByRole("link", { name: "View 2 workflows" }),
    ).toHaveAttribute("href", "/workflows");
    expect(
      screen.getByRole("link", { name: "View 5 runnable steps" }),
    ).toHaveAttribute("href", "/workflows");
    expect(
      screen.getByRole("link", { name: "View 3 manifests" }),
    ).toHaveAttribute("href", "/manifests");
    expect(
      screen.getByRole("link", { name: "View 142 artifacts" }),
    ).toHaveAttribute("href", "/artifacts");
    expect(
      screen.getByRole("link", { name: "View 12 source artifacts" }),
    ).toHaveAttribute("href", "/artifacts?origin=source");
    expect(
      screen.getByRole("link", { name: "View 130 workflow outputs" }),
    ).toHaveAttribute("href", "/artifacts?origin=workflow_output");
  });

  it("leaves current run scopes as plain text, not a link", async () => {
    renderPage();

    expect(await screen.findByText("current run scopes")).toBeInTheDocument();
    // The count renders, but no link wraps it (there is no run page).
    expect(screen.queryByRole("link", { name: /run scopes/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /View 4/ })).not.toBeInTheDocument();
  });

  it("drops the redundant GUI-sections link grid that duplicated the top nav", async () => {
    renderPage();

    await screen.findByRole("link", { name: "View 2 workflows" });
    // The old duplicate nav exposed bare "Workflows"/"Artifacts"/"Manifests" links.
    expect(screen.queryByRole("link", { name: "Workflows" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Artifacts" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Manifests" })).not.toBeInTheDocument();
  });
});
