import { describe, expect, it, vi } from "vitest";
import { ApiError, fetchArtifact, fetchArtifacts } from "./client";

describe("api client", () => {
  it("requests the artifact route with the JSON accept header", async () => {
    const fetchMock = vi.fn(async () =>
      new Response(JSON.stringify({ artifact_id: 12 }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await fetchArtifact(12);

    expect(fetchMock).toHaveBeenCalledWith("/api/artifacts/12", {
      headers: { Accept: "application/json" },
    });
  });

  it("parses the NIPACT API error envelope", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({ code: "artifact_not_found", message: "missing" }),
          { status: 404 },
        ),
      ),
    );

    await expect(fetchArtifact(12)).rejects.toMatchObject({
      name: "ApiError",
      status: 404,
      code: "artifact_not_found",
      message: "404 artifact_not_found: missing",
    } satisfies Partial<ApiError>);
  });

  it("requests the bare artifacts collection when no filters are given", async () => {
    const fetchMock = vi.fn(async (_input: string) =>
      new Response(JSON.stringify({ context: "colors", artifacts: [] }), {
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await fetchArtifacts();

    expect(fetchMock.mock.calls[0][0]).toBe("/api/artifacts");
  });

  it("serializes supported artifact filters into the query string", async () => {
    const fetchMock = vi.fn(async (_input: string) =>
      new Response(JSON.stringify({ context: "colors", artifacts: [] }), {
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await fetchArtifacts({
      workflow: "base",
      step: "color_sector_analysis",
      is_published: true,
    });

    const url = fetchMock.mock.calls[0][0];
    expect(url.startsWith("/api/artifacts?")).toBe(true);
    const params = new URLSearchParams(url.split("?")[1] ?? "");
    expect(params.get("workflow")).toBe("base");
    expect(params.get("step")).toBe("color_sector_analysis");
    expect(params.get("is_published")).toBe("true");
  });
});
