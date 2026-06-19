import type cytoscape from "cytoscape";

export const lineageGraphStyle: cytoscape.StylesheetJson = [
  {
    selector: "node",
    style: {
      "background-color": "#ffffff",
      "border-color": "#64748b",
      "border-width": 1,
      color: "#111827",
      "font-size": 9,
      height: 48,
      label: "data(label)",
      "min-zoomed-font-size": 7,
      shape: "ellipse",
      "text-halign": "center",
      "text-valign": "center",
      "text-wrap": "wrap",
      width: 82,
    },
  },
  {
    selector: "node.source",
    style: {
      "background-color": "#eff6ff",
      "border-color": "#2563eb",
    },
  },
  {
    selector: "node.workflow_output",
    style: {
      "background-color": "#f8fafc",
    },
  },
  {
    selector: "node.selected-ui",
    style: {
      "border-color": "#111827",
      "border-width": 1,
      "underlay-color": "#fde047",
      "underlay-opacity": 0.35,
      "underlay-padding": "8px",
    },
  },
  {
    selector: "node.search-match",
    style: {
      "background-color": "#fff7ed",
      "border-color": "#c2410c",
      "border-width": 2,
    },
  },
  {
    selector: "node.published",
    style: {
      "border-style": "double",
    },
  },
  {
    selector: "edge",
    style: {
      "curve-style": "bezier",
      "font-size": 8,
      label: "data(label)",
      "line-color": "#94a3b8",
      "line-outline-color": "#ffffff",
      "line-outline-width": "2px",
      "min-zoomed-font-size": 7,
      "target-arrow-color": "#94a3b8",
      "target-arrow-shape": "triangle",
      "text-background-color": "#ffffff",
      "text-background-opacity": 0.85,
      "text-background-padding": "2px",
      width: "1.4px",
    },
  },
  {
    selector: "edge.selected-ui",
    style: {
      "line-color": "#111827",
      "line-outline-color": "#fde047",
      "line-outline-width": "4px",
      "target-arrow-color": "#111827",
      width: "2px",
    },
  },
  {
    selector: "edge.search-match",
    style: {
      "line-color": "#c2410c",
      "target-arrow-color": "#c2410c",
      width: "2.2px",
    },
  },
];
