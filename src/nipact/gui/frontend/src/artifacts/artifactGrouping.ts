import type { Artifact } from "../api/types";

export interface ArtifactOutputGroup {
  key: string;
  label: string;
  count: number;
  defaultOpen: boolean;
  artifacts: Artifact[];
}

export interface ArtifactStepGroup {
  key: string;
  label: string;
  count: number;
  defaultOpen: boolean;
  outputs: ArtifactOutputGroup[];
}

export interface ArtifactWorkflowGroup {
  key: string;
  label: string;
  count: number;
  defaultOpen: boolean;
  steps: ArtifactStepGroup[];
}

export interface ArtifactOriginGroup {
  key: string;
  label: string;
  count: number;
  defaultOpen: boolean;
  workflows: ArtifactWorkflowGroup[];
}

function groupKey(value: string | null | undefined, fallback: string): string {
  return value && value.trim() ? value : fallback;
}

function groupLabel(key: string): string {
  return key;
}

export function artifactGroupDefaultOpen(artifacts: readonly Artifact[]): boolean {
  return artifacts.some(
    (artifact) =>
      artifact.origin === "source" || artifact.is_selected_output || artifact.is_published,
  );
}

export function searchArtifacts(
  artifacts: readonly Artifact[],
  query: string,
): Artifact[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) {
    return [...artifacts];
  }

  return artifacts.filter((artifact) =>
    artifactSearchText(artifact).includes(normalizedQuery),
  );
}

function artifactSearchText(artifact: Artifact): string {
  const values = [
    artifact.artifact_id,
    artifact.origin,
    artifact.job_id,
    artifact.artifact_set_id,
    artifact.workflow_name,
    artifact.step_name,
    artifact.output_name,
    artifact.address,
    artifact.path,
    artifact.display_path,
    artifact.published_path,
    artifact.staging_path,
    artifact.subject_id,
    artifact.session_id,
    artifact.task_name,
    artifact.run_label,
    artifact.datatype,
    artifact.suffix,
    artifact.content_digest,
    artifact.output_hash,
    artifact.parameter_hash,
    artifact.parameter_digest,
    artifact.workflow_artifact_ref,
    artifact.callable_ref,
    artifact.software_ref,
  ];

  return values
    .filter((value) => value !== null && value !== undefined)
    .map((value) => String(value).toLowerCase())
    .join(" ");
}

export function groupArtifacts(
  artifacts: readonly Artifact[],
): ArtifactOriginGroup[] {
  const originGroups = new Map<string, Map<string, Map<string, Map<string, Artifact[]>>>>();

  for (const artifact of artifacts) {
    const origin = groupKey(artifact.origin, "unknown");
    const workflow = groupKey(artifact.workflow_name, artifact.origin === "source" ? "source" : "none");
    const step = groupKey(artifact.step_name, artifact.origin === "source" ? "source" : "none");
    const output = groupKey(artifact.output_name, artifact.origin === "source" ? "source" : "none");

    if (!originGroups.has(origin)) {
      originGroups.set(origin, new Map());
    }
    const workflowGroups = originGroups.get(origin)!;
    if (!workflowGroups.has(workflow)) {
      workflowGroups.set(workflow, new Map());
    }
    const stepGroups = workflowGroups.get(workflow)!;
    if (!stepGroups.has(step)) {
      stepGroups.set(step, new Map());
    }
    const outputGroups = stepGroups.get(step)!;
    if (!outputGroups.has(output)) {
      outputGroups.set(output, []);
    }
    outputGroups.get(output)!.push(artifact);
  }

  return Array.from(originGroups.entries()).map(([originKey, workflowMap]) => {
    const workflows: ArtifactWorkflowGroup[] = Array.from(workflowMap.entries()).map(
      ([workflowKey, stepMap]) => {
        const steps: ArtifactStepGroup[] = Array.from(stepMap.entries()).map(
          ([stepKey, outputMap]) => {
            const outputs: ArtifactOutputGroup[] = Array.from(outputMap.entries()).map(
              ([outputKey, outputArtifacts]) => ({
                key: outputKey,
                label: groupLabel(outputKey),
                count: outputArtifacts.length,
                defaultOpen: artifactGroupDefaultOpen(outputArtifacts),
                artifacts: outputArtifacts,
              }),
            );
            const stepArtifacts = outputs.flatMap((output) => output.artifacts);
            return {
              key: stepKey,
              label: groupLabel(stepKey),
              count: stepArtifacts.length,
              defaultOpen: artifactGroupDefaultOpen(stepArtifacts),
              outputs,
            };
          },
        );
        const workflowArtifacts = steps.flatMap((step) =>
          step.outputs.flatMap((output) => output.artifacts),
        );
        return {
          key: workflowKey,
          label: groupLabel(workflowKey),
          count: workflowArtifacts.length,
          defaultOpen: artifactGroupDefaultOpen(workflowArtifacts),
          steps,
        };
      },
    );
    const originArtifacts = workflows.flatMap((workflow) =>
      workflow.steps.flatMap((step) => step.outputs.flatMap((output) => output.artifacts)),
    );
    return {
      key: originKey,
      label: groupLabel(originKey),
      count: originArtifacts.length,
      defaultOpen: artifactGroupDefaultOpen(originArtifacts),
      workflows,
    };
  });
}
