import { describe, expect, it } from "vitest";
import type { Artifact } from "../api/types";
import {
  artifactGroupDefaultOpen,
  groupArtifacts,
  searchArtifacts,
} from "./artifactGrouping";

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

describe("artifact grouping helpers", () => {
  it("groups artifacts by origin, workflow, step, and output", () => {
    const groups = groupArtifacts([
      artifact({ artifact_id: 1, origin: "source", workflow_name: null, step_name: null, output_name: null }),
      artifact({ artifact_id: 2, step_name: "color_local_transform", output_name: "local_color" }),
      artifact({ artifact_id: 3, step_name: "color_local_transform", output_name: "local_color" }),
    ]);

    expect(groups).toHaveLength(2);
    expect(groups[0].key).toBe("source");
    expect(groups[0].workflows[0].steps[0].outputs[0].artifacts).toHaveLength(1);
    expect(groups[1].key).toBe("workflow_output");
    expect(groups[1].workflows[0].steps[0].key).toBe("color_local_transform");
    expect(groups[1].workflows[0].steps[0].outputs[0].count).toBe(2);
  });

  it("searches common artifact identity, path, entity, and hash fields", () => {
    const artifacts = [
      artifact({ artifact_id: 2, address: "color_001", subject_id: "sub-01" }),
      artifact({
        artifact_id: 3,
        job_id: "job-sector-002",
        artifact_set_id: "set-sector",
        callable_ref: "nipact.examples.colors_processing_demo.runtime:sector_counts",
        software_ref: "nipact-demo/1",
        step_name: "color_sector_analysis",
        output_name: "sector_counts",
        path: "outputs/colors/base/color_sector_analysis/sector_counts/color_002.json",
        content_digest: "b".repeat(64),
      }),
    ];

    expect(searchArtifacts(artifacts, "sub-01")).toHaveLength(1);
    expect(searchArtifacts(artifacts, "sector_counts")).toHaveLength(1);
    expect(searchArtifacts(artifacts, "set-sector")).toHaveLength(1);
    expect(searchArtifacts(artifacts, "job-sector")).toHaveLength(1);
    expect(searchArtifacts(artifacts, "nipact-demo")).toHaveLength(1);
    expect(searchArtifacts(artifacts, "BBBB")).toHaveLength(1);
    expect(searchArtifacts(artifacts, "no-match")).toHaveLength(0);
  });

  it("opens source and selected or published output groups by default", () => {
    expect(artifactGroupDefaultOpen([artifact({ origin: "source" })])).toBe(true);
    expect(artifactGroupDefaultOpen([artifact({ is_selected_output: true })])).toBe(true);
    expect(artifactGroupDefaultOpen([artifact({ is_published: true })])).toBe(true);
    expect(artifactGroupDefaultOpen([artifact({})])).toBe(false);
  });
});
