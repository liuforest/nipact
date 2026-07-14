import { useQuery } from "@tanstack/react-query";
import { useMemo, useState, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { fetchArtifacts } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import type { ArtifactFilters } from "../api/types";
import type { Artifact } from "../api/types";
import { groupArtifacts, searchArtifacts } from "../artifacts/artifactGrouping";
import type {
  ArtifactOriginGroup,
  ArtifactOutputGroup,
  ArtifactStepGroup,
  ArtifactWorkflowGroup,
} from "../artifacts/artifactGrouping";
import { DataTable } from "../components/ui/DataTable";
import { EmptyPanel } from "../components/ui/EmptyPanel";
import { ErrorPanel } from "../components/ui/ErrorPanel";
import { LoadingPanel } from "../components/ui/LoadingPanel";
import { PathValue } from "../components/ui/PathValue";

export function ArtifactsPage() {
  const [searchText, setSearchText] = useState("");
  const [searchParams] = useSearchParams();
  const filters = useMemo(
    () => readArtifactFilters(searchParams),
    [searchParams],
  );
  const activeFilters = useMemo(() => artifactFilterEntries(filters), [filters]);
  const query = useQuery({
    queryKey: queryKeys.artifacts(filters),
    queryFn: () => fetchArtifacts(filters),
  });
  const artifacts = query.data?.artifacts ?? [];
  const filteredArtifacts = useMemo(
    () => searchArtifacts(artifacts, searchText),
    [artifacts, searchText],
  );
  const groupedArtifacts = useMemo(
    () => groupArtifacts(filteredArtifacts),
    [filteredArtifacts],
  );
  const expandAllMatches = searchText.trim().length > 0;

  if (query.isLoading) {
    return <LoadingPanel label="Loading artifacts" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading artifacts" />;
  }

  const hasActiveFilters = activeFilters.length > 0;

  return (
    <div className="page-stack">
      <section className="panel">
        <h1>Artifacts</h1>
        <div className="artifact-toolbar">
          <label className="search-field">
            <span>Search artifacts</span>
            <input
              aria-label="Search artifacts"
              type="search"
              value={searchText}
              onChange={(event) => setSearchText(event.target.value)}
              placeholder="id, step, output, address, path, subject, hash"
            />
          </label>
          <p className="status-line">
            Showing {filteredArtifacts.length} of {artifacts.length} artifacts
          </p>
        </div>
        {hasActiveFilters ? (
          <div className="active-filters">
            <span className="active-filters-label">Filters:</span>
            {activeFilters.map((entry) => (
              <span className="active-filter" key={entry.key}>
                {`${entry.key} = ${entry.value}`}
              </span>
            ))}
            <Link className="clear-filters" to="/artifacts">
              Clear filters
            </Link>
          </div>
        ) : null}
      </section>
      {artifacts.length === 0 ? (
        <EmptyPanel
          title={
            hasActiveFilters ? "No artifacts match the active filters" : "No artifacts"
          }
        />
      ) : filteredArtifacts.length === 0 ? (
        <EmptyPanel title="No matching artifacts" />
      ) : (
        <section className="panel artifact-browser">
          {groupedArtifacts.map((originGroup) => (
            <ArtifactOriginSection
              key={originGroup.key}
              group={originGroup}
              forceOpen={expandAllMatches}
            />
          ))}
        </section>
      )}
    </div>
  );
}

const STRING_FILTER_KEYS = [
  "origin",
  "workflow",
  "step",
  "output",
  "address",
] as const;
const BOOLEAN_FILTER_KEYS = ["is_selected_output", "is_published"] as const;

// Read the supported artifact filters out of the URL query string. Unsupported
// params are ignored here (the backend rejects them with a 422); empty values
// are dropped so a stray `?workflow=` doesn't send an empty filter.
function readArtifactFilters(params: URLSearchParams): ArtifactFilters {
  const filters: ArtifactFilters = {};
  for (const key of STRING_FILTER_KEYS) {
    const value = params.get(key);
    if (value !== null && value !== "") {
      filters[key] = value;
    }
  }
  for (const key of BOOLEAN_FILTER_KEYS) {
    const value = params.get(key);
    if (value === "true") {
      filters[key] = true;
    } else if (value === "false") {
      filters[key] = false;
    }
  }
  return filters;
}

function artifactFilterEntries(
  filters: ArtifactFilters,
): { key: string; value: string }[] {
  const entries: { key: string; value: string }[] = [];
  for (const key of STRING_FILTER_KEYS) {
    const value = filters[key];
    if (value !== undefined) {
      entries.push({ key, value });
    }
  }
  for (const key of BOOLEAN_FILTER_KEYS) {
    const value = filters[key];
    if (value !== undefined) {
      entries.push({ key, value: String(value) });
    }
  }
  return entries;
}

function ArtifactOriginSection({
  group,
  forceOpen,
}: {
  group: ArtifactOriginGroup;
  forceOpen: boolean;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--origin"
      count={group.count}
      defaultOpen={group.defaultOpen}
      forceOpen={forceOpen}
      label={group.label}
    >
      {group.workflows.map((workflowGroup) => (
        <ArtifactWorkflowSection
          key={workflowGroup.key}
          group={workflowGroup}
          forceOpen={forceOpen}
        />
      ))}
    </ArtifactDisclosure>
  );
}

function ArtifactDisclosure({
  children,
  className,
  count,
  defaultOpen,
  forceOpen,
  label,
}: {
  children: ReactNode;
  className: string;
  count: number;
  defaultOpen: boolean;
  forceOpen: boolean;
  label: string;
}) {
  const [isOpen, setIsOpen] = useState(defaultOpen);
  const effectiveOpen = forceOpen || isOpen;
  return (
    <details
      className={className}
      open={effectiveOpen}
      onToggle={(event) => {
        if (!forceOpen) {
          setIsOpen(event.currentTarget.open);
        }
      }}
    >
      <summary>
        <span>{label}</span>
        <span className="artifact-group-count">{count}</span>
      </summary>
      {effectiveOpen ? children : null}
    </details>
  );
}

function ArtifactWorkflowSection({
  group,
  forceOpen,
}: {
  group: ArtifactWorkflowGroup;
  forceOpen: boolean;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--workflow"
      count={group.count}
      defaultOpen={group.defaultOpen}
      forceOpen={forceOpen}
      label={`workflow: ${group.label}`}
    >
      {group.steps.map((stepGroup) => (
        <ArtifactStepSection key={stepGroup.key} group={stepGroup} forceOpen={forceOpen} />
      ))}
    </ArtifactDisclosure>
  );
}

function ArtifactStepSection({
  group,
  forceOpen,
}: {
  group: ArtifactStepGroup;
  forceOpen: boolean;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--step"
      count={group.count}
      defaultOpen={group.defaultOpen}
      forceOpen={forceOpen}
      label={`step: ${group.label}`}
    >
      {group.outputs.map((outputGroup) => (
        <ArtifactOutputSection
          key={outputGroup.key}
          group={outputGroup}
          forceOpen={forceOpen}
        />
      ))}
    </ArtifactDisclosure>
  );
}

function ArtifactOutputSection({
  group,
  forceOpen,
}: {
  group: ArtifactOutputGroup;
  forceOpen: boolean;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--output"
      count={group.count}
      defaultOpen={group.defaultOpen}
      forceOpen={forceOpen}
      label={`output: ${group.label}`}
    >
      <ArtifactRows artifacts={group.artifacts} />
    </ArtifactDisclosure>
  );
}

function ArtifactRows({ artifacts }: { artifacts: readonly Artifact[] }) {
  return (
    <DataTable<Artifact>
      rows={artifacts}
      getRowKey={(row) => row.artifact_id}
      columns={[
        {
          key: "id",
          label: "id",
          render: (row) => <Link to={`/artifacts/${row.artifact_id}`}>{row.artifact_id}</Link>,
        },
        { key: "address", label: "address", render: (row) => row.address ?? "none" },
        { key: "path", label: "path", render: (row) => <PathValue value={row.display_path} /> },
        {
          key: "flags",
          label: "flags",
          render: (row) => <ArtifactFlags artifact={row} />,
        },
        {
          key: "lineage",
          label: "lineage",
          render: (row) => <Link to={`/artifacts/${row.artifact_id}/lineage`}>trace</Link>,
        },
      ]}
    />
  );
}

function ArtifactFlags({ artifact }: { artifact: Artifact }) {
  const flags = [
    artifact.origin === "source" ? "source" : null,
    artifact.is_published ? "published" : null,
    artifact.is_selected_output ? "selected output" : null,
  ].filter((flag): flag is string => Boolean(flag));

  if (flags.length === 0) {
    return <span className="muted-value">none</span>;
  }

  return (
    <span className="artifact-flags">
      {flags.map((flag) => (
        <span className="artifact-flag" key={flag}>
          {flag}
        </span>
      ))}
    </span>
  );
}
