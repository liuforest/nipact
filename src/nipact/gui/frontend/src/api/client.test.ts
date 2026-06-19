import { describe, expect, it, vi } from "vitest";
import { ApiError, fetchArtifact } from "./client";

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
});
