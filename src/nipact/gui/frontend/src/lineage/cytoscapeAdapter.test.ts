import { describe, expect, it } from "vitest";
import type { TraceGraphResponse } from "../api/types";
import { buildLineageElements } from "./cytoscapeAdapter";
import {
  buildArtifactNeighborhood,
  findTraceArtifact,
  findTraceDependency,
  searchLineageArtifacts,
} from "./lineageInteraction";

const graph: TraceGraphResponse = {
  schema_version: 1,
  context: "colors",
  selected_artifact_id: 2,
  provenance_status: "complete",
  artifacts: [
    {
      artifact_id: 1,
      origin: "source",
      run_id: null,
      job_id: null,
      artifact_set_id: null,
      path: "data/source.json",
      display_path: "data/source.json",
      is_selected: false,
      is_selected_output: false,
      is_published: false,
      published_path: null,
      staging_path: null,
      workflow_name: null,
      step_name: null,
      output_name: null,
      address: null,
      parameter_hash: null,
      content_digest: "1".repeat(64),
      output_hash: null,
      file_size: 1,
      extension: ".json",
      subject_id: null,
      session_id: null,
      task_name: null,
      run_label: null,
      datatype: null,
      suffix: null,
      source_metadata: null,
      workflow_artifact_ref: null,
      callable_ref: null,
      software_ref: null,
    },
    {
      artifact_id: 2,
      origin: "workflow_output",
      run_id: 1,
      job_id: "job-1",
      artifact_set_id: null,
      path: "outputs/result.json",
      display_path: "outputs/result.json",
      is_selected: true,
      is_selected_output: true,
      is_published: true,
      published_path: "outputs/result.json",
      staging_path: "runs/result.json",
      workflow_name: "base",
      step_name: "finish",
      output_name: "result",
      address: "init",
      parameter_hash: "abc",
      content_digest: "2".repeat(64),
      output_hash: "2".repeat(16),
      file_size: 2,
      extension: ".json",
      subject_id: null,
      session_id: null,
      task_name: null,
      run_label: null,
      datatype: null,
      suffix: null,
      source_metadata: null,
      workflow_artifact_ref: "artifact:finish:result",
      callable_ref: "tests:finish",
      software_ref: "tests/0",
    },
  ],
  dependencies: [
    {
      edge_id: "dependency:1:2:raw",
      source_artifact_id: 1,
      dependent_artifact_id: 2,
      is_reused_input: false,
      dependency_role: "source_input",
      binding_name: "raw",
      input_path: "data/source.json",
      source_content_digest: "0".repeat(64),
      source_file_size: 100,
      source_extension: ".json",
      dependency_set_id: null,
      manifest_digest: null,
      edge_cardinality: null,
    },
  ],
  manifest_bindings: [],
  warnings: [],
};

describe("lineage graph adapter", () => {
  it("uses registry artifact ids for lineage nodes", () => {
    const elements = buildLineageElements(graph);

    expect(elements.some((element) => element.data.id === "artifact:1")).toBe(true);
    expect(elements.some((element) => element.data.id === "artifact:2")).toBe(true);
    expect(elements.some((element) => element.data.source === "artifact:1")).toBe(true);
    expect(
      elements.find((element) => element.data.id === "dependency:1:2:raw")?.data
        .dependency_role,
    ).toBe("source_input");
  });

  it("adds UI selection and search classes without hiding elements", () => {
    const elements = buildLineageElements(graph, {
      searchArtifactIds: [1],
      selectedArtifactId: 2,
      selectedDependencyEdgeId: "dependency:1:2:raw",
    });

    const sourceNode = elements.find((element) => element.data.id === "artifact:1");
    const selectedNode = elements.find((element) => element.data.id === "artifact:2");
    const selectedEdge = elements.find(
      (element) => element.data.id === "dependency:1:2:raw",
    );

    expect(elements).toHaveLength(3);
    expect(sourceNode?.classes?.toString()).toContain("search-match");
    expect(selectedNode?.classes?.toString()).toContain("selected-ui");
    expect(selectedEdge?.classes?.toString()).toContain("selected-ui");
    expect(selectedEdge?.classes?.toString()).toContain("search-match");
    expect(selectedEdge?.data.source_artifact_id).toBe(1);
    expect(selectedEdge?.data.dependent_artifact_id).toBe(2);
  });
});

describe("lineage interaction helpers", () => {
  it("searches artifact identity, path, workflow, and step values", () => {
    expect(searchLineageArtifacts(graph, "source.json")).toEqual([1]);
    expect(searchLineageArtifacts(graph, "finish")).toEqual([2]);
    expect(searchLineageArtifacts(graph, "artifact:finish:result")).toEqual([2]);
    expect(searchLineageArtifacts(graph, "")).toEqual([]);
  });

  it("finds selected graph records", () => {
    expect(findTraceArtifact(graph, 2)?.step_name).toBe("finish");
    expect(findTraceArtifact(graph, 999)).toBeNull();
    expect(findTraceDependency(graph, "dependency:1:2:raw")?.binding_name).toBe("raw");
    expect(findTraceDependency(graph, "missing")).toBeNull();
  });

  it("summarizes immediate artifact neighborhoods", () => {
    const neighborhood = buildArtifactNeighborhood(graph, 2);

    expect(neighborhood.upstreamArtifacts.map((artifact) => artifact.artifact_id)).toEqual([1]);
    expect(neighborhood.downstreamArtifacts).toEqual([]);
    expect(buildArtifactNeighborhood(graph, null).upstreamArtifacts).toEqual([]);
  });
});
