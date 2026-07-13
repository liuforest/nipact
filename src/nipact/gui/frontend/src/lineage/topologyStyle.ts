import type cytoscape from "cytoscape";

export const topologyGraphStyle: cytoscape.StylesheetJson = [
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
      width: 92,
    },
  },
  {
    selector: "node.step",
    style: {
      "background-color": "#f1f5f9",
      "border-color": "#334155",
      shape: "round-rectangle",
    },
  },
  {
    selector: "node.artifact_slot",
    style: {
      "background-color": "#f8fafc",
      "border-color": "#64748b",
    },
  },
  {
    selector: "node.source_input",
    style: {
      "background-color": "#eff6ff",
      "border-color": "#2563eb",
      shape: "round-rectangle",
    },
  },
  {
    selector: "node.source_root",
    style: {
      "background-color": "#eff6ff",
      "border-color": "#2563eb",
    },
  },
  {
    selector: "node.root-ui",
    style: {
      "border-color": "#047857",
      "border-width": 3,
    },
  },
  {
    selector: "node.selected-ui",
    style: {
      "border-color": "#111827",
      "underlay-color": "#fde047",
      "underlay-opacity": 0.35,
      "underlay-padding": "8px",
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
    selector: "edge.produces",
    style: {
      "line-color": "#cbd5e1",
      "line-style": "dashed",
      "target-arrow-color": "#cbd5e1",
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
];
