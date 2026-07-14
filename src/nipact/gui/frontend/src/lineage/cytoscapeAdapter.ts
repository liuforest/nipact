import type { ElementDefinition } from "cytoscape";
import type { TraceGraphResponse } from "../api/types";

interface LineageElementOptions {
  selectedArtifactId?: number | null;
  selectedDependencyEdgeId?: string | null;
  searchArtifactIds?: readonly number[];
}

export function buildLineageElements(
  graph: TraceGraphResponse,
  options: LineageElementOptions = {},
): ElementDefinition[] {
  const searchArtifactIds = new Set(options.searchArtifactIds ?? []);
  const elements: ElementDefinition[] = graph.artifacts.map((artifact) => ({
    data: {
      id: artifactNodeId(artifact.artifact_id),
      label: artifactLabel(artifact),
      type: "artifact",
      artifact_id: artifact.artifact_id,
      origin: artifact.origin,
    },
      classes: [
        "artifact",
        artifact.origin,
        artifact.is_published ? "published" : "",
        artifact.artifact_id === options.selectedArtifactId ? "selected-ui" : "",
      searchArtifactIds.has(artifact.artifact_id) ? "search-match" : "",
    ]
      .filter(Boolean)
      .join(" "),
  }));

  const nodeIds = new Set(elements.map((element) => element.data.id));
  for (const dependency of graph.dependencies) {
    const source = artifactNodeId(dependency.source_artifact_id);
    const target = artifactNodeId(dependency.dependent_artifact_id);
    if (!nodeIds.has(source) || !nodeIds.has(target)) {
      continue;
    }
    elements.push({
      data: {
        id: dependency.edge_id,
        source,
        target,
        label: dependency.binding_name,
        type: "dependency",
        edge_id: dependency.edge_id,
        source_artifact_id: dependency.source_artifact_id,
        dependent_artifact_id: dependency.dependent_artifact_id,
        dependency_role: dependency.dependency_role,
        input_path: dependency.input_path,
      },
      classes: [
        dependency.edge_id === options.selectedDependencyEdgeId ? "selected-ui" : "",
        searchArtifactIds.has(dependency.source_artifact_id) ||
        searchArtifactIds.has(dependency.dependent_artifact_id)
          ? "search-match"
          : "",
      ]
        .filter(Boolean)
        .join(" "),
    });
  }

  return elements;
}

export function artifactNodeId(artifactId: number): string {
  return `artifact:${artifactId}`;
}

function artifactLabel(artifact: {
  step_name: string | null;
  output_name: string | null;
  address: string | null;
  path: string;
}): string {
  if (artifact.step_name && artifact.output_name) {
    return artifact.address
      ? `${artifact.step_name}.${artifact.output_name}\n${artifact.address}`
      : `${artifact.step_name}.${artifact.output_name}`;
  }
  const parts = artifact.path.split("/");
  return parts[parts.length - 1] || artifact.path;
}
