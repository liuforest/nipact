import type {
  TraceArtifact,
  TraceDependency,
  TraceGraphResponse,
} from "../api/types";

export interface ArtifactNeighborhood {
  upstreamArtifacts: TraceArtifact[];
  downstreamArtifacts: TraceArtifact[];
}

export function findTraceArtifact(
  graph: TraceGraphResponse,
  artifactId: number | null,
): TraceArtifact | null {
  if (artifactId === null) {
    return null;
  }
  return graph.artifacts.find((artifact) => artifact.artifact_id === artifactId) ?? null;
}

export function findTraceDependency(
  graph: TraceGraphResponse,
  edgeId: string | null,
): TraceDependency | null {
  if (!edgeId) {
    return null;
  }
  return graph.dependencies.find((dependency) => dependency.edge_id === edgeId) ?? null;
}

export function searchLineageArtifacts(
  graph: TraceGraphResponse,
  searchText: string,
): number[] {
  const term = searchText.trim().toLowerCase();
  if (!term) {
    return [];
  }
  return graph.artifacts
    .filter((artifact) => artifactSearchValues(artifact).some((value) => value.includes(term)))
    .map((artifact) => artifact.artifact_id);
}

export function buildArtifactNeighborhood(
  graph: TraceGraphResponse,
  artifactId: number | null,
): ArtifactNeighborhood {
  const empty: ArtifactNeighborhood = {
    upstreamArtifacts: [],
    downstreamArtifacts: [],
  };
  if (artifactId === null) {
    return empty;
  }

  const artifactsById = new Map(
    graph.artifacts.map((artifact) => [artifact.artifact_id, artifact]),
  );
  const upstreamDependencies = graph.dependencies.filter(
    (dependency) => dependency.dependent_artifact_id === artifactId,
  );
  const downstreamDependencies = graph.dependencies.filter(
    (dependency) => dependency.source_artifact_id === artifactId,
  );
  const uniqueArtifacts = (
    dependencies: TraceDependency[],
    artifactIdForDependency: (dependency: TraceDependency) => number,
  ): TraceArtifact[] => {
    const seenArtifactIds = new Set<number>();
    const artifacts: TraceArtifact[] = [];
    for (const dependency of dependencies) {
      const dependencyArtifactId = artifactIdForDependency(dependency);
      if (seenArtifactIds.has(dependencyArtifactId)) {
        continue;
      }
      const artifact = artifactsById.get(dependencyArtifactId);
      if (artifact) {
        seenArtifactIds.add(dependencyArtifactId);
        artifacts.push(artifact);
      }
    }
    return artifacts;
  };

  return {
    upstreamArtifacts: uniqueArtifacts(
      upstreamDependencies,
      (dependency) => dependency.source_artifact_id,
    ),
    downstreamArtifacts: uniqueArtifacts(
      downstreamDependencies,
      (dependency) => dependency.dependent_artifact_id,
    ),
  };
}

function artifactSearchValues(artifact: TraceArtifact): string[] {
  return [
    artifact.artifact_id,
    artifact.origin,
    artifact.path,
    artifact.display_path,
    artifact.published_path,
    artifact.staging_path,
    artifact.workflow_name,
    artifact.step_name,
    artifact.output_name,
    artifact.address,
    artifact.parameter_hash,
    artifact.content_digest,
    artifact.output_hash,
    artifact.job_id,
    artifact.artifact_set_id,
    artifact.subject_id,
    artifact.session_id,
    artifact.task_name,
    artifact.run_label,
    artifact.datatype,
    artifact.suffix,
    artifact.workflow_artifact_ref,
    artifact.callable_ref,
    artifact.software_ref,
  ]
    .filter((value): value is string | number => value !== null && value !== undefined)
    .map((value) => String(value).toLowerCase());
}
