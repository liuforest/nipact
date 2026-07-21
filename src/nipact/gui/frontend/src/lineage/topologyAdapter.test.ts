import { describe, expect, it } from "vitest";
import type { ObservedTopologyResponse } from "../api/types";
import { buildTopologyElements, topologyNodeLabel } from "./topologyAdapter";

const topology: ObservedTopologyResponse = {
  schema_version: 2,
  perspective: "observed",
  scope: "ancestor_closure",
  context: "colors",
  root_artifact_id: 2,
  root_node_id: "n2",
  provenance_status: "complete",
  summary: {
    distinct_artifact_count: 2,
    registry_dependency_count: 3,
    node_count: 3,
    edge_count: 2,
  },
  nodes: [
    {
      kind: "source_input",
      node_id: "n0",
      workflow_name: "base",
      step_name: "finish",
      binding_name: "raw",
      dependency_role: "source_input",
      registry_artifact_count: 3,
    },
    {
      kind: "step",
      node_id: "n1",
      workflow_name: "base",
      step_name: "finish",
      produced_registry_artifact_count: 1,
    },
    {
      kind: "artifact_slot",
      node_id: "n2",
      workflow_name: "base",
      step_name: "finish",
      output_name: "result",
      registry_artifact_count: 1,
      distinct_address_count: 0,
    },
  ],
  edges: [
    {
      kind: "consumes",
      edge_id: "e0",
      source_node_id: "n0",
      target_node_id: "n1",
      workflow_name: "base",
      step_name: "finish",
      binding_name: "raw",
      dependency_role: "source_input",
      registry_dependency_count: 3,
    },
    {
      kind: "produces",
      edge_id: "e1",
      source_node_id: "n1",
      target_node_id: "n2",
    },
  ],
  execution_populations: [],
  manifest_bindings: [],
  warnings: [],
};

describe("topology graph adapter", () => {
  it("maps nodes and edges onto graph-local ids", () => {
    const elements = buildTopologyElements(topology);

    expect(elements.some((element) => element.data.id === "n0")).toBe(true);
    expect(elements.some((element) => element.data.id === "n1")).toBe(true);
    expect(elements.some((element) => element.data.id === "n2")).toBe(true);

    const consumes = elements.find((element) => element.data.id === "e0");
    expect(consumes?.data.source).toBe("n0");
    expect(consumes?.data.target).toBe("n1");
    expect(consumes?.data.label).toBe("raw");

    const produces = elements.find((element) => element.data.id === "e1");
    expect(produces?.data.source).toBe("n1");
    expect(produces?.data.target).toBe("n2");
    expect(produces?.data.label).toBe("");
  });

  it("marks the root node and applies selection classes", () => {
    const elements = buildTopologyElements(topology, {
      selectedNodeId: "n1",
      selectedEdgeId: "e0",
    });

    const rootNode = elements.find((element) => element.data.id === "n2");
    const selectedNode = elements.find((element) => element.data.id === "n1");
    const selectedEdge = elements.find((element) => element.data.id === "e0");

    expect(rootNode?.classes?.toString()).toContain("root-ui");
    expect(selectedNode?.classes?.toString()).toContain("selected-ui");
    expect(selectedEdge?.classes?.toString()).toContain("selected-ui");
  });

  it("labels nodes from coordinates and aggregate counts", () => {
    expect(
      topologyNodeLabel({
        kind: "source_input",
        node_id: "n0",
        workflow_name: "base",
        step_name: "finish",
        binding_name: "raw",
        dependency_role: "source_input",
        registry_artifact_count: 3,
      }),
    ).toBe("raw\n×3");
    expect(
      topologyNodeLabel({
        kind: "step",
        node_id: "n1",
        workflow_name: "base",
        step_name: "finish",
        produced_registry_artifact_count: 1,
      }),
    ).toBe("finish");
    expect(
      topologyNodeLabel({
        kind: "source_root",
        node_id: "n0",
        display_path: "data/inputs/source.json",
        registry_artifact_count: 1,
      }),
    ).toBe("source.json");
  });
});
