import type { ArtifactFilters } from "./types";

export const queryKeys = {
  summary: ["summary"] as const,
  workflows: ["workflows"] as const,
  manifests: ["manifests"] as const,
  manifest: (manifestName: string) => ["manifest", manifestName] as const,
  artifacts: (filters: ArtifactFilters = {}) => ["artifacts", filters] as const,
  artifact: (artifactId: number) => ["artifact", artifactId] as const,
  lineage: (artifactId: number) => ["lineage", artifactId] as const,
  topology: (artifactId: number) => ["topology", artifactId] as const,
};
