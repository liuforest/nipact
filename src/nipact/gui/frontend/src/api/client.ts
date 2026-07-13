import type {
  ApiErrorBody,
  Artifact,
  ArtifactsResponse,
  ManifestDetail,
  ManifestsResponse,
  ObservedTopologyResponse,
  SummaryResponse,
  TraceGraphResponse,
  WorkflowsResponse,
} from "./types";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: Record<string, unknown> | undefined;
  readonly url: string;

  constructor(status: number, body: ApiErrorBody, url: string) {
    super(`${status} ${body.code}: ${body.message}`);
    this.name = "ApiError";
    this.status = status;
    this.code = body.code;
    this.details = body.details;
    this.url = url;
  }
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    let body: ApiErrorBody = {
      code: "request_failed",
      message: response.statusText || `HTTP ${response.status}`,
    };
    try {
      const parsed = (await response.json()) as Partial<ApiErrorBody>;
      if (typeof parsed.code === "string" && typeof parsed.message === "string") {
        body = {
          code: parsed.code,
          message: parsed.message,
          details:
            parsed.details && typeof parsed.details === "object"
              ? (parsed.details as Record<string, unknown>)
              : undefined,
        };
      }
    } catch {
      // Keep the HTTP status fallback for non-JSON error responses.
    }
    throw new ApiError(response.status, body, path);
  }
  return (await response.json()) as T;
}

export function fetchSummary(): Promise<SummaryResponse> {
  return getJson<SummaryResponse>("/api/summary");
}

export function fetchWorkflows(): Promise<WorkflowsResponse> {
  return getJson<WorkflowsResponse>("/api/workflows");
}

export function fetchManifests(): Promise<ManifestsResponse> {
  return getJson<ManifestsResponse>("/api/manifests");
}

export function fetchManifest(manifestName: string): Promise<ManifestDetail> {
  return getJson<ManifestDetail>(`/api/manifests/${encodeURIComponent(manifestName)}`);
}

export function fetchArtifacts(): Promise<ArtifactsResponse> {
  return getJson<ArtifactsResponse>("/api/artifacts");
}

export function fetchArtifact(artifactId: number): Promise<Artifact> {
  return getJson<Artifact>(`/api/artifacts/${artifactId}`);
}

export function fetchArtifactLineage(
  artifactId: number,
): Promise<TraceGraphResponse> {
  return getJson<TraceGraphResponse>(`/api/artifacts/${artifactId}/lineage`);
}

export function fetchArtifactTopology(
  artifactId: number,
): Promise<ObservedTopologyResponse> {
  return getJson<ObservedTopologyResponse>(
    `/api/artifacts/${artifactId}/topology`,
  );
}
