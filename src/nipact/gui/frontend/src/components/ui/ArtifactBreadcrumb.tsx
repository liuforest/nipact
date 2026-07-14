import { Link } from "react-router-dom";

interface Crumb {
  label: string;
  to?: string;
}

// Small artifact-scoped breadcrumb with two shapes: the artifact detail page
// (Artifacts / Artifact {id}) and the lineage views (Artifacts / Artifact {id} /
// Lineage). Crumbs with a destination are links; the final crumb is the current
// page and carries aria-current. Styling is deferred to PR 3 — this reuses only
// the existing link styles and needs no new CSS.
export function ArtifactBreadcrumb({
  artifactId,
  view,
}: {
  artifactId: number;
  view: "detail" | "lineage";
}) {
  const crumbs: Crumb[] =
    view === "detail"
      ? [
          { label: "Artifacts", to: "/artifacts" },
          { label: `Artifact ${artifactId}` },
        ]
      : [
          { label: "Artifacts", to: "/artifacts" },
          { label: `Artifact ${artifactId}`, to: `/artifacts/${artifactId}` },
          { label: "Lineage" },
        ];

  return (
    <nav aria-label="Breadcrumb" className="breadcrumb">
      {crumbs.map((crumb, index) => (
        <span key={crumb.label}>
          {index > 0 ? <span aria-hidden="true"> / </span> : null}
          {crumb.to ? (
            <Link to={crumb.to}>{crumb.label}</Link>
          ) : (
            <span aria-current="page">{crumb.label}</span>
          )}
        </span>
      ))}
    </nav>
  );
}
