import type { Artifact, ArtifactFilters, ArtifactGroupCount } from "../api/types";

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

// Count-tree groups: the browse view renders coordinate counts without holding
// any artifact rows. Each output leaf carries the exact filter coordinate used
// to lazily fetch its rows when the group is opened.
export interface ArtifactCountOutputGroup {
  key: string;
  label: string;
  count: number;
  coordinate: ArtifactFilters;
}

export interface ArtifactCountStepGroup {
  key: string;
  label: string;
  count: number;
  outputs: ArtifactCountOutputGroup[];
}

export interface ArtifactCountWorkflowGroup {
  key: string;
  label: string;
  count: number;
  steps: ArtifactCountStepGroup[];
}

export interface ArtifactCountOriginGroup {
  key: string;
  label: string;
  count: number;
  workflows: ArtifactCountWorkflowGroup[];
}

function groupKey(value: string | null | undefined, fallback: string): string {
  return value && value.trim() ? value : fallback;
}

function groupLabel(key: string): string {
  return key;
}

// The filter that selects exactly this leaf's rows via GET /api/artifacts.
// Source rows use the real `origin=source` filter, never the "source" display
// sentinel round-tripped as a coordinate (which the backend would 422).
function leafCoordinate(record: ArtifactGroupCount): ArtifactFilters {
  if (record.origin === "source") {
    return { origin: "source" };
  }
  const coordinate: ArtifactFilters = {};
  if (record.workflow_name !== null) {
    coordinate.workflow = record.workflow_name;
  }
  if (record.step_name !== null) {
    coordinate.step = record.step_name;
  }
  if (record.output_name !== null) {
    coordinate.output = record.output_name;
  }
  return coordinate;
}

// Build the origin → workflow → step → output count tree from the flat group
// records. Each record is already one distinct output-level coordinate (the
// backend GROUP BY guarantees uniqueness), so parent counts are summed here.
export function groupArtifactCounts(
  groups: readonly ArtifactGroupCount[],
): ArtifactCountOriginGroup[] {
  const originGroups = new Map<
    string,
    Map<string, Map<string, ArtifactCountOutputGroup[]>>
  >();

  for (const record of groups) {
    const isSource = record.origin === "source";
    const origin = groupKey(record.origin, "unknown");
    const workflow = groupKey(record.workflow_name, isSource ? "source" : "none");
    const step = groupKey(record.step_name, isSource ? "source" : "none");
    const output = groupKey(record.output_name, isSource ? "source" : "none");

    if (!originGroups.has(origin)) {
      originGroups.set(origin, new Map());
    }
    const workflowGroups = originGroups.get(origin)!;
    if (!workflowGroups.has(workflow)) {
      workflowGroups.set(workflow, new Map());
    }
    const stepGroups = workflowGroups.get(workflow)!;
    if (!stepGroups.has(step)) {
      stepGroups.set(step, []);
    }
    stepGroups.get(step)!.push({
      key: output,
      label: groupLabel(output),
      count: record.artifact_count,
      coordinate: leafCoordinate(record),
    });
  }

  const sumCounts = (items: readonly { count: number }[]): number =>
    items.reduce((total, item) => total + item.count, 0);

  return Array.from(originGroups.entries()).map(([originKey, workflowMap]) => {
    const workflows: ArtifactCountWorkflowGroup[] = Array.from(
      workflowMap.entries(),
    ).map(([workflowKey, stepMap]) => {
      const steps: ArtifactCountStepGroup[] = Array.from(stepMap.entries()).map(
        ([stepKey, outputs]) => ({
          key: stepKey,
          label: groupLabel(stepKey),
          count: sumCounts(outputs),
          outputs,
        }),
      );
      return {
        key: workflowKey,
        label: groupLabel(workflowKey),
        count: sumCounts(steps),
        steps,
      };
    });
    return {
      key: originKey,
      label: groupLabel(originKey),
      count: sumCounts(workflows),
      workflows,
    };
  });
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
