export interface ApiErrorBody {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

export interface SummaryResponse {
  context: string;
  workflow_count: number;
  runnable_step_count: number;
  manifest_count: number;
  artifact_count: number;
  source_artifact_count: number;
  workflow_output_count: number;
  workflow_run_count: number;
}

export interface RunnableWorkflowStep {
  step_name: string;
  output_name: string;
}

export interface WorkflowSummary {
  workflow_name: string;
  step_count: number;
  runnable_step_count: number;
  runnable_steps: RunnableWorkflowStep[];
}

export interface WorkflowsResponse {
  context: string;
  workflows: WorkflowSummary[];
}

export interface ManifestSummary {
  context: string;
  name: string;
  path: string;
  entity_count: number;
  first_entity_id: string;
  last_entity_id: string;
  manifest_digest: string;
  manifest_hash: string;
  source_artifact_path: string | null;
}

export interface ManifestDetail extends ManifestSummary {
  manifest_body: string;
}

export interface ManifestsResponse {
  context: string;
  manifests: ManifestSummary[];
}

export interface Artifact {
  artifact_id: number;
  origin: string;
  run_id: number | null;
  job_id: string | null;
  artifact_set_id: string | null;
  path: string;
  display_path: string;
  is_selected_output: boolean;
  is_published: boolean;
  published_path: string | null;
  staging_path: string | null;
  workflow_name: string | null;
  step_name: string | null;
  output_name: string | null;
  address: string | null;
  parameter_hash: string | null;
  parameter_digest: string | null;
  content_digest: string;
  output_hash: string | null;
  file_size: number;
  extension: string;
  subject_id: string | null;
  session_id: string | null;
  task_name: string | null;
  run_label: string | null;
  datatype: string | null;
  suffix: string | null;
  workflow_artifact_ref: string | null;
  callable_ref: string | null;
  software_ref: string | null;
  created_at: string;
}

// Full single-artifact record from GET /api/artifacts/{id}. source_metadata is
// an unbounded blob dropped from the list profile above (mirrors gui/models.py).
export interface ArtifactDetail extends Artifact {
  source_metadata: Record<string, unknown> | null;
}

export interface ArtifactsResponse {
  context: string;
  artifacts: Artifact[];
}

// One coordinate group with a row count, from GET /api/artifacts/groups. Source
// rows keep their null workflow/step/output coordinates (see gui/models.py).
export interface ArtifactGroupCount {
  origin: string;
  workflow_name: string | null;
  step_name: string | null;
  output_name: string | null;
  artifact_count: number;
}

export interface ArtifactGroupsResponse {
  context: string;
  groups: ArtifactGroupCount[];
}

// Supported query params for GET /api/artifacts. The vocabulary is fixed by the
// backend route (gui/app.py::artifacts); anything else is rejected with a 422
// `unsupported_filter`. Keep the keys aligned with `_reject_unsupported_query_params`.
export interface ArtifactFilters {
  origin?: string;
  workflow?: string;
  step?: string;
  output?: string;
  address?: string;
  is_selected_output?: boolean;
  is_published?: boolean;
}

export interface TraceArtifact {
  artifact_id: number;
  origin: string;
  run_id: number | null;
  job_id: string | null;
  artifact_set_id: string | null;
  path: string;
  display_path: string;
  is_selected: boolean;
  is_selected_output: boolean;
  is_published: boolean;
  published_path: string | null;
  staging_path: string | null;
  workflow_name: string | null;
  step_name: string | null;
  output_name: string | null;
  address: string | null;
  parameter_hash: string | null;
  content_digest: string;
  output_hash: string | null;
  file_size: number;
  extension: string;
  subject_id: string | null;
  session_id: string | null;
  task_name: string | null;
  run_label: string | null;
  datatype: string | null;
  suffix: string | null;
  source_metadata: Record<string, unknown> | null;
  workflow_artifact_ref: string | null;
  callable_ref: string | null;
  software_ref: string | null;
}

export interface TraceDependency {
  edge_id: string;
  source_artifact_id: number;
  dependent_artifact_id: number;
  is_reused_input: boolean;
  dependency_role: string;
  binding_name: string;
  input_path: string;
  source_content_digest: string;
  source_file_size: number;
  source_extension: string;
  dependency_set_id: string | null;
  manifest_digest: string | null;
  edge_cardinality: number | null;
}

export interface TraceManifestBinding {
  run_id: number;
  workflow_name: string;
  step_name: string;
  role: string;
  manifest_name: string;
  manifest_digest: string;
  manifest_hash: string;
  entity_count: number;
}

export interface TraceWarning {
  warning_type: string;
  message: string;
  artifact_id: number | null;
  input_path: string | null;
}

export interface TraceGraphResponse {
  schema_version: number;
  context: string;
  selected_artifact_id: number;
  provenance_status: string;
  artifacts: TraceArtifact[];
  dependencies: TraceDependency[];
  manifest_bindings: TraceManifestBinding[];
  warnings: TraceWarning[];
}

// --- Observed topology (PR 2) -----------------------------------------------
// Mirrors gui/models.py::ObservedTopologyResponse: the aggregated projection of
// a backward provenance closure. node_id/edge_id are graph-local display keys;
// the structured coordinate fields carry drill-down identity.

export interface TopologyStepNode {
  kind: "step";
  node_id: string;
  workflow_name: string;
  step_name: string;
  produced_registry_artifact_count: number;
}

export interface TopologyArtifactSlotNode {
  kind: "artifact_slot";
  node_id: string;
  workflow_name: string;
  step_name: string;
  output_name: string;
  registry_artifact_count: number;
  distinct_address_count: number;
}

export interface TopologySourceInputNode {
  kind: "source_input";
  node_id: string;
  workflow_name: string;
  step_name: string;
  binding_name: string;
  dependency_role: string;
  registry_artifact_count: number;
}

export interface TopologySourceRootNode {
  kind: "source_root";
  node_id: string;
  display_path: string;
  registry_artifact_count: number;
}

export type TopologyNode =
  | TopologyStepNode
  | TopologyArtifactSlotNode
  | TopologySourceInputNode
  | TopologySourceRootNode;

export interface TopologyConsumesEdge {
  kind: "consumes";
  edge_id: string;
  source_node_id: string;
  target_node_id: string;
  workflow_name: string;
  step_name: string;
  binding_name: string;
  dependency_role: string;
  registry_dependency_count: number;
}

export interface TopologyProducesEdge {
  kind: "produces";
  edge_id: string;
  source_node_id: string;
  target_node_id: string;
}

export type TopologyEdge = TopologyConsumesEdge | TopologyProducesEdge;

export interface TopologyManifestBindingSummary {
  workflow_name: string;
  step_name: string;
  role: string;
  manifest_name: string;
  distinct_run_count: number;
  distinct_manifest_digest_count: number;
  manifest_digest: string | null;
  manifest_hash: string | null;
  entity_count: number | null;
}

export interface TopologyWarningSummary {
  warning_type: string;
  occurrence_count: number;
}

export interface TopologySummary {
  distinct_artifact_count: number;
  registry_dependency_count: number;
  node_count: number;
  edge_count: number;
}

export interface ObservedTopologyResponse {
  schema_version: number;
  perspective: "observed";
  scope: "ancestor_closure";
  context: string;
  root_artifact_id: number;
  root_node_id: string;
  provenance_status: "complete" | "degraded";
  summary: TopologySummary;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
  manifest_bindings: TopologyManifestBindingSummary[];
  warnings: TopologyWarningSummary[];
}
