import { describe, expect, it } from "vitest";
import type {
  ObservedTopologyResponse,
  TraceArtifact,
  TraceDependency,
  TraceGraphResponse,
} from "../api/types";
import {
  resolveTopologyInstances,
  searchInstanceRows,
} from "./topologyDrilldown";

function artifact(overrides: Partial<TraceArtifact> & { artifact_id: number }): TraceArtifact {
  return {
    origin: "workflow_output",
    run_id: 1,
    job_id: null,
    artifact_set_id: null,
    path: `path/${overrides.artifact_id}`,
    display_path: `path/${overrides.artifact_id}`,
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
    content_digest: "0".repeat(64),
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
    ...overrides,
  };
}

function dependency(
  overrides: Partial<TraceDependency> & {
    edge_id: string;
    source_artifact_id: number;
    dependent_artifact_id: number;
    binding_name: string;
    dependency_role: string;
  },
): TraceDependency {
  return {
    is_reused_input: false,
    input_path: `inputs/${overrides.edge_id}`,
    source_content_digest: "0".repeat(64),
    source_file_size: 1,
    source_extension: ".json",
    dependency_set_id: null,
    manifest_digest: null,
    edge_cardinality: null,
    ...overrides,
  };
}

// A closure that exercises every coordinate: two source roots feeding a prep
// step, prep feeding a model step (workflow-output source), plus a second
// source input into the model step (distinct binding/role), a sibling workflow,
// and a degraded missing-source dependency.
const graph: TraceGraphResponse = {
  schema_version: 1,
  context: "colors",
  selected_artifact_id: 12,
  provenance_status: "degraded",
  artifacts: [
    artifact({ artifact_id: 1, origin: "source", display_path: "data/raw_a.csv" }),
    artifact({ artifact_id: 2, origin: "source", display_path: "data/raw_b.csv" }),
    artifact({ artifact_id: 10, workflow_name: "base", step_name: "prep", output_name: "clean", address: "addr1" }),
    artifact({ artifact_id: 11, workflow_name: "base", step_name: "prep", output_name: "clean", address: "addr2" }),
    artifact({ artifact_id: 12, workflow_name: "base", step_name: "model", output_name: "fit" }),
    artifact({ artifact_id: 20, workflow_name: "alt", step_name: "prep", output_name: "clean" }),
  ],
  dependencies: [
    dependency({ edge_id: "d1", source_artifact_id: 1, dependent_artifact_id: 10, binding_name: "raw", dependency_role: "source_input" }),
    dependency({ edge_id: "d2", source_artifact_id: 2, dependent_artifact_id: 11, binding_name: "raw", dependency_role: "source_input" }),
    dependency({ edge_id: "d3", source_artifact_id: 10, dependent_artifact_id: 12, binding_name: "clean", dependency_role: "fit_input" }),
    dependency({ edge_id: "d4", source_artifact_id: 11, dependent_artifact_id: 12, binding_name: "clean", dependency_role: "fit_input" }),
    dependency({ edge_id: "d5", source_artifact_id: 1, dependent_artifact_id: 12, binding_name: "cfg", dependency_role: "analysis_input" }),
    // A second fit_input into base.model under the SAME binding/role as d3/d4,
    // but sourced from the alt.prep.clean slot — a distinct consumes edge that
    // differs from e_clean only by its source node.
    dependency({ edge_id: "d7", source_artifact_id: 20, dependent_artifact_id: 12, binding_name: "clean", dependency_role: "fit_input" }),
    // degraded: source artifact 999 is not in the closure
    dependency({ edge_id: "d6", source_artifact_id: 999, dependent_artifact_id: 10, binding_name: "raw", dependency_role: "source_input" }),
  ],
  manifest_bindings: [],
  warnings: [],
};

// The projection gui/topology.py would produce for `graph` above. node_id/
// edge_id are display-only; only the structured coordinates and the source/
// target references are load-bearing.
const topology: ObservedTopologyResponse = {
  schema_version: 1,
  perspective: "observed",
  scope: "ancestor_closure",
  context: "colors",
  root_artifact_id: 12,
  root_node_id: "n_slot_bmf",
  provenance_status: "degraded",
  summary: {
    distinct_artifact_count: 6,
    registry_dependency_count: 7,
    node_count: 8,
    edge_count: 7,
  },
  nodes: [
    { kind: "step", node_id: "n_step_bp", workflow_name: "base", step_name: "prep", produced_registry_artifact_count: 2 },
    { kind: "step", node_id: "n_step_bm", workflow_name: "base", step_name: "model", produced_registry_artifact_count: 1 },
    { kind: "step", node_id: "n_step_ap", workflow_name: "alt", step_name: "prep", produced_registry_artifact_count: 1 },
    { kind: "artifact_slot", node_id: "n_slot_bpc", workflow_name: "base", step_name: "prep", output_name: "clean", registry_artifact_count: 2, distinct_address_count: 2 },
    { kind: "artifact_slot", node_id: "n_slot_bmf", workflow_name: "base", step_name: "model", output_name: "fit", registry_artifact_count: 1, distinct_address_count: 0 },
    { kind: "artifact_slot", node_id: "n_slot_apc", workflow_name: "alt", step_name: "prep", output_name: "clean", registry_artifact_count: 1, distinct_address_count: 0 },
    { kind: "source_input", node_id: "n_si_raw", workflow_name: "base", step_name: "prep", binding_name: "raw", dependency_role: "source_input", registry_artifact_count: 2 },
    { kind: "source_input", node_id: "n_si_cfg", workflow_name: "base", step_name: "model", binding_name: "cfg", dependency_role: "analysis_input", registry_artifact_count: 1 },
  ],
  edges: [
    { kind: "consumes", edge_id: "e_raw", source_node_id: "n_si_raw", target_node_id: "n_step_bp", workflow_name: "base", step_name: "prep", binding_name: "raw", dependency_role: "source_input", registry_dependency_count: 2 },
    { kind: "consumes", edge_id: "e_clean", source_node_id: "n_slot_bpc", target_node_id: "n_step_bm", workflow_name: "base", step_name: "model", binding_name: "clean", dependency_role: "fit_input", registry_dependency_count: 2 },
    { kind: "consumes", edge_id: "e_clean_alt", source_node_id: "n_slot_apc", target_node_id: "n_step_bm", workflow_name: "base", step_name: "model", binding_name: "clean", dependency_role: "fit_input", registry_dependency_count: 1 },
    { kind: "consumes", edge_id: "e_cfg", source_node_id: "n_si_cfg", target_node_id: "n_step_bm", workflow_name: "base", step_name: "model", binding_name: "cfg", dependency_role: "analysis_input", registry_dependency_count: 1 },
    { kind: "produces", edge_id: "e_p_bp", source_node_id: "n_step_bp", target_node_id: "n_slot_bpc" },
    { kind: "produces", edge_id: "e_p_bm", source_node_id: "n_step_bm", target_node_id: "n_slot_bmf" },
    { kind: "produces", edge_id: "e_p_ap", source_node_id: "n_step_ap", target_node_id: "n_slot_apc" },
  ],
  manifest_bindings: [],
  warnings: [],
};

function nodeByCoordinate(kind: string, predicate: (node: (typeof topology.nodes)[number]) => boolean) {
  const node = topology.nodes.find((n) => n.kind === kind && predicate(n));
  if (!node) {
    throw new Error(`fixture node not found: ${kind}`);
  }
  return node;
}

function ids(result: ReturnType<typeof resolveTopologyInstances>): number[] {
  if (!result || result.kind !== "artifacts") {
    throw new Error("expected artifact result");
  }
  return result.rows.map((row) => row.artifact_id);
}

function edgeIds(result: ReturnType<typeof resolveTopologyInstances>): string[] {
  if (!result || result.kind !== "dependencies") {
    throw new Error("expected dependency result");
  }
  return result.rows.map((row) => row.edge_id);
}

describe("resolveTopologyInstances", () => {
  it("maps a step node to its workflow-output artifacts", () => {
    const node = nodeByCoordinate("step", (n) => n.node_id === "n_step_bp");
    const result = resolveTopologyInstances(topology, graph, { kind: "node", node_id: node.node_id });
    expect(ids(result)).toEqual([10, 11]);
    // count invariant
    expect(result?.kind === "artifacts" && result.rows.length).toBe(
      node.kind === "step" ? node.produced_registry_artifact_count : -1,
    );
  });

  it("maps an artifact_slot node to its slot artifacts", () => {
    const node = nodeByCoordinate("artifact_slot", (n) => n.node_id === "n_slot_bpc");
    const result = resolveTopologyInstances(topology, graph, { kind: "node", node_id: node.node_id });
    expect(ids(result)).toEqual([10, 11]);
    expect(result?.kind === "artifacts" && result.rows.length).toBe(
      node.kind === "artifact_slot" ? node.registry_artifact_count : -1,
    );
  });

  it("maps a source_input node to distinct source artifacts", () => {
    const node = nodeByCoordinate("source_input", (n) => n.node_id === "n_si_raw");
    const result = resolveTopologyInstances(topology, graph, { kind: "node", node_id: node.node_id });
    // sources 1 and 2, deduped; the degraded d6 (source 999) contributes nothing
    expect(ids(result)).toEqual([1, 2]);
    expect(result?.kind === "artifacts" && result.rows.length).toBe(
      node.kind === "source_input" ? node.registry_artifact_count : -1,
    );
  });

  it("maps a source_root node to the selected root artifact only", () => {
    const rootGraph: TraceGraphResponse = { ...graph, selected_artifact_id: 1 };
    const rootTopology: ObservedTopologyResponse = {
      ...topology,
      root_artifact_id: 1,
      nodes: [
        { kind: "source_root", node_id: "n_root", display_path: "data/raw_a.csv", registry_artifact_count: 1 },
      ],
      edges: [],
    };
    const result = resolveTopologyInstances(rootTopology, rootGraph, { kind: "node", node_id: "n_root" });
    expect(ids(result)).toEqual([1]);
    expect(result?.kind === "artifacts" && result.rows.length).toBe(1);
  });

  it("maps a consumes edge to its physical dependency rows", () => {
    const result = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_raw" });
    expect(edgeIds(result)).toEqual(["d1", "d2"]);
    // count invariant excludes the degraded d6, matching registry_dependency_count
    const edge = topology.edges.find((e) => e.edge_id === "e_raw");
    expect(result?.kind === "dependencies" && result.rows.length).toBe(
      edge?.kind === "consumes" ? edge.registry_dependency_count : -1,
    );
  });

  it("dereferences the source node so sibling edges into the same step stay distinct", () => {
    // e_clean and e_cfg both target base.model but differ by binding/role and by
    // source node kind (artifact_slot vs source_input).
    const cleanResult = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_clean" });
    expect(edgeIds(cleanResult)).toEqual(["d3", "d4"]);
    const cfgResult = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_cfg" });
    expect(edgeIds(cfgResult)).toEqual(["d5"]);
  });

  it("isolates edges that share consumer, binding, and role but differ only by source slot", () => {
    // e_clean and e_clean_alt both feed base.model under binding "clean" as
    // fit_input; only source_node_id (base.prep.clean vs alt.prep.clean) tells
    // them apart. A resolver ignoring it would return d3, d4, d7 for both.
    const cleanResult = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_clean" });
    expect(edgeIds(cleanResult)).toEqual(["d3", "d4"]);
    const altResult = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_clean_alt" });
    expect(edgeIds(altResult)).toEqual(["d7"]);
    const altEdge = topology.edges.find((e) => e.edge_id === "e_clean_alt");
    expect(altResult?.kind === "dependencies" && altResult.rows.length).toBe(
      altEdge?.kind === "consumes" ? altEdge.registry_dependency_count : -1,
    );
  });

  it("resolves a produces edge through its target slot", () => {
    const result = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_p_bp" });
    expect(ids(result)).toEqual([10, 11]);
    const slot = topology.nodes.find((n) => n.node_id === "n_slot_bpc");
    expect(result?.kind === "artifacts" && result.rows.length).toBe(
      slot?.kind === "artifact_slot" ? slot.registry_artifact_count : -1,
    );
  });

  it("returns null for an unknown selection", () => {
    expect(resolveTopologyInstances(topology, graph, { kind: "node", node_id: "missing" })).toBeNull();
    expect(resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "missing" })).toBeNull();
  });
});

describe("searchInstanceRows", () => {
  const slotResult = resolveTopologyInstances(topology, graph, { kind: "node", node_id: "n_slot_bpc" });
  const consumesResult = resolveTopologyInstances(topology, graph, { kind: "edge", edge_id: "e_raw" });

  it("returns the input unchanged for a blank term", () => {
    expect(searchInstanceRows(slotResult!, "   ")).toBe(slotResult);
  });

  it("filters artifacts case-insensitively over visible fields", () => {
    const filtered = searchInstanceRows(slotResult!, "ADDR2");
    expect(filtered.kind === "artifacts" && filtered.rows.map((r) => r.artifact_id)).toEqual([11]);
  });

  it("filters dependencies over source/dependent/binding/role/input", () => {
    const byBinding = searchInstanceRows(consumesResult!, "raw");
    expect(byBinding.kind === "dependencies" && byBinding.rows.map((r) => r.edge_id)).toEqual(["d1", "d2"]);
    const byInput = searchInstanceRows(consumesResult!, "inputs/d1");
    expect(byInput.kind === "dependencies" && byInput.rows.map((r) => r.edge_id)).toEqual(["d1"]);
  });
});
