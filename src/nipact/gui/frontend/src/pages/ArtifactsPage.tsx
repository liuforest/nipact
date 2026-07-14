import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { fetchArtifactGroups, fetchArtifacts } from "../api/client";
import { queryKeys } from "../api/queryKeys";
import type { ArtifactFilters } from "../api/types";
import type { Artifact } from "../api/types";
import {
  groupArtifactCounts,
  groupArtifacts,
  searchArtifacts,
} from "../artifacts/artifactGrouping";
import type {
  ArtifactCountOriginGroup,
  ArtifactCountStepGroup,
  ArtifactCountWorkflowGroup,
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
  const [searchParams] = useSearchParams();
  const filters = useMemo(
    () => readArtifactFilters(searchParams),
    [searchParams],
  );
  const activeFilters = useMemo(() => artifactFilterEntries(filters), [filters]);

  const [searchText, setSearchText] = useState("");
  const [submittedQuery, setSubmittedQuery] = useState<string | null>(null);

  // Changing the URL filters changes the scope a search ran against, so any
  // active search no longer applies — drop back to browse mode.
  useEffect(() => {
    setSubmittedQuery(null);
    setSearchText("");
  }, [filters]);

  const hasActiveFilters = activeFilters.length > 0;
  const searchActive = submittedQuery !== null;

  return (
    <div className="page-stack">
      <section className="panel">
        <h1>Artifacts</h1>
        <form
          className="artifact-toolbar"
          onSubmit={(event) => {
            event.preventDefault();
            const trimmed = searchText.trim();
            setSubmittedQuery(trimmed ? trimmed : null);
          }}
        >
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
          <div className="button-row">
            <button className="button" type="submit">
              Search
            </button>
            {searchActive ? (
              <button
                className="button"
                type="button"
                onClick={() => {
                  setSubmittedQuery(null);
                  setSearchText("");
                }}
              >
                Clear search
              </button>
            ) : null}
          </div>
        </form>
        <p className="status-line">
          {searchActive
            ? "Search covers the current filter scope."
            : "Browsing artifact groups. Open a group to load its rows."}
        </p>
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
      {searchActive ? (
        <ArtifactSearchResults filters={filters} query={submittedQuery} />
      ) : (
        <ArtifactBrowser filters={filters} hasActiveFilters={hasActiveFilters} />
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

// Browse mode: fetch coordinate counts and render the tree all-collapsed. A
// group's rows are fetched only when its disclosure is opened (see
// LazyArtifactRows), so a bare page load never pulls the whole population.
function ArtifactBrowser({
  filters,
  hasActiveFilters,
}: {
  filters: ArtifactFilters;
  hasActiveFilters: boolean;
}) {
  const query = useQuery({
    queryKey: queryKeys.artifactGroups(filters),
    queryFn: () => fetchArtifactGroups(filters),
  });

  if (query.isLoading) {
    return <LoadingPanel label="Loading artifacts" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading artifacts" />;
  }

  const records = query.data.groups;
  if (records.length === 0) {
    return (
      <EmptyPanel
        title={
          hasActiveFilters ? "No artifacts match the active filters" : "No artifacts"
        }
      />
    );
  }

  const groups = groupArtifactCounts(records);
  const totalArtifacts = groups.reduce((total, group) => total + group.count, 0);

  return (
    <>
      <p className="status-line">
        {totalArtifacts} artifacts in {records.length} groups
      </p>
      <section className="panel artifact-browser">
        {groups.map((originGroup) => (
          <ArtifactCountOriginSection
            key={originGroup.key}
            group={originGroup}
            filters={filters}
          />
        ))}
      </section>
    </>
  );
}

// Search mode: load the current filter scope once, then filter and group it
// client-side, expanding every match.
function ArtifactSearchResults({
  filters,
  query,
}: {
  filters: ArtifactFilters;
  query: string;
}) {
  const scope = useQuery({
    queryKey: queryKeys.artifacts(filters),
    queryFn: () => fetchArtifacts(filters),
  });

  if (scope.isLoading) {
    return <LoadingPanel label="Loading artifacts" />;
  }
  if (scope.isError) {
    return <ErrorPanel error={scope.error} />;
  }
  if (!scope.data) {
    return <LoadingPanel label="Loading artifacts" />;
  }

  const artifacts = scope.data.artifacts;
  const matches = searchArtifacts(artifacts, query);
  const grouped = groupArtifacts(matches);

  return (
    <>
      <p className="status-line">
        Showing {matches.length} of {artifacts.length} artifacts
      </p>
      {matches.length === 0 ? (
        <EmptyPanel title="No matching artifacts" />
      ) : (
        <section className="panel artifact-browser">
          {grouped.map((originGroup) => (
            <ArtifactOriginSection
              key={originGroup.key}
              group={originGroup}
              forceOpen
            />
          ))}
        </section>
      )}
    </>
  );
}

// --- Browse-mode count sections (lazy rows) ---------------------------------

function ArtifactCountOriginSection({
  group,
  filters,
}: {
  group: ArtifactCountOriginGroup;
  filters: ArtifactFilters;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--origin"
      count={group.count}
      defaultOpen={false}
      forceOpen={false}
      label={group.label}
    >
      {group.workflows.map((workflowGroup) => (
        <ArtifactCountWorkflowSection
          key={workflowGroup.key}
          group={workflowGroup}
          filters={filters}
        />
      ))}
    </ArtifactDisclosure>
  );
}

function ArtifactCountWorkflowSection({
  group,
  filters,
}: {
  group: ArtifactCountWorkflowGroup;
  filters: ArtifactFilters;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--workflow"
      count={group.count}
      defaultOpen={false}
      forceOpen={false}
      label={`workflow: ${group.label}`}
    >
      {group.steps.map((stepGroup) => (
        <ArtifactCountStepSection
          key={stepGroup.key}
          group={stepGroup}
          filters={filters}
        />
      ))}
    </ArtifactDisclosure>
  );
}

function ArtifactCountStepSection({
  group,
  filters,
}: {
  group: ArtifactCountStepGroup;
  filters: ArtifactFilters;
}) {
  return (
    <ArtifactDisclosure
      className="artifact-group artifact-group--step"
      count={group.count}
      defaultOpen={false}
      forceOpen={false}
      label={`step: ${group.label}`}
    >
      {group.outputs.map((outputGroup) => (
        <ArtifactDisclosure
          key={outputGroup.key}
          className="artifact-group artifact-group--output"
          count={outputGroup.count}
          defaultOpen={false}
          forceOpen={false}
          label={`output: ${outputGroup.label}`}
        >
          <LazyArtifactRows filters={{ ...filters, ...outputGroup.coordinate }} />
        </ArtifactDisclosure>
      ))}
    </ArtifactDisclosure>
  );
}

// Mounts only when its parent disclosure is open, so the leaf request fires on
// first open and is cached by react-query thereafter.
function LazyArtifactRows({ filters }: { filters: ArtifactFilters }) {
  const query = useQuery({
    queryKey: queryKeys.artifacts(filters),
    queryFn: () => fetchArtifacts(filters),
  });

  if (query.isLoading) {
    return <LoadingPanel label="Loading artifacts" />;
  }
  if (query.isError) {
    return <ErrorPanel error={query.error} />;
  }
  if (!query.data) {
    return <LoadingPanel label="Loading artifacts" />;
  }
  return <ArtifactRows artifacts={query.data.artifacts} />;
}

// --- Search-mode sections (artifact rows already loaded) --------------------

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
        { key: "path", label: "path", render: (row) => <PathValue value={row.display_path} compact /> },
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
