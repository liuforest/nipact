import { useEffect, useMemo, useRef } from "react";
import cytoscape from "cytoscape";
import dagre from "cytoscape-dagre";
import type {
  Core,
  ElementDefinition,
  LayoutOptions,
  SingularElementReturnValue,
  StylesheetJson,
} from "cytoscape";
import { GraphControls } from "./GraphControls";
import { boundedFocusZoom } from "./focusZoom";

// Minimum zoom to treat a focused element as legible; tuned against real graphs (Loop A).
const MIN_LEGIBLE_ZOOM = 1;

let dagreRegistered = false;

function registerDagre() {
  if (!dagreRegistered) {
    cytoscape.use(dagre);
    dagreRegistered = true;
  }
}

function fitGraph(cy: Core) {
  cy.resize();
  cy.fit(undefined, 24);
}

function zoomGraph(cy: Core, factor: number) {
  const nextZoom = Math.max(cy.minZoom(), Math.min(cy.maxZoom(), cy.zoom() * factor));
  const extent = cy.extent();
  cy.zoom({
    level: nextZoom,
    position: {
      x: (extent.x1 + extent.x2) / 2,
      y: (extent.y1 + extent.y2) / 2,
    },
  });
}

function resetGraph(cy: Core, layout: LayoutOptions) {
  cy.layout(layout).run();
  fitGraph(cy);
}

function elementClasses(element: ElementDefinition): string {
  if (Array.isArray(element.classes)) {
    return element.classes.join(" ");
  }
  return element.classes ?? "";
}

function elementTopologyKey(elements: readonly ElementDefinition[]): string {
  return JSON.stringify(
    elements.map((element) => {
      const topology = { ...element };
      delete topology.classes;
      return topology;
    }),
  );
}

function syncElementClasses(cy: Core, elements: readonly ElementDefinition[]) {
  const classesById = new Map<string, string>();
  for (const element of elements) {
    if (typeof element.data.id === "string") {
      classesById.set(element.data.id, elementClasses(element));
    }
  }
  cy.batch(() => {
    cy.elements().forEach((element) => {
      const nextClasses = classesById.get(element.id());
      const currentClasses = element.classes().join(" ");
      if (nextClasses !== undefined && currentClasses !== nextClasses) {
        element.classes(nextClasses);
      }
    });
  });
}

function isInteractiveElement(element: SingularElementReturnValue): boolean {
  return element.data("interactive") !== false;
}

export function GraphCanvasFrame({
  ariaLabel,
  elements,
  focusRequest,
  layout,
  onElementSelect,
  stylesheet,
}: {
  ariaLabel: string;
  elements: ElementDefinition[];
  focusRequest?: { elementId: string; token: number } | null;
  layout: LayoutOptions;
  onElementSelect?: (data: Record<string, unknown> | null) => void;
  stylesheet: StylesheetJson;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);
  const layoutRef = useRef(layout);
  const onElementSelectRef = useRef(onElementSelect);
  const topologyKey = useMemo(() => elementTopologyKey(elements), [elements]);

  useEffect(() => {
    const cy = cyRef.current;
    if (cy) {
      syncElementClasses(cy, elements);
    }
  }, [elements]);

  useEffect(() => {
    onElementSelectRef.current = onElementSelect;
  }, [onElementSelect]);

  useEffect(() => {
    layoutRef.current = layout;
  }, [layout]);

  useEffect(() => {
    registerDagre();
    if (!containerRef.current) {
      return undefined;
    }
    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: stylesheet,
      layout: layoutRef.current,
      wheelSensitivity: 0.25,
    });
    cy.on("tap", "node, edge", (event) => {
      const handler = onElementSelectRef.current;
      if (handler && isInteractiveElement(event.target)) {
        handler(event.target.data() as Record<string, unknown>);
      }
    });
    cy.on("tap", (event) => {
      if (event.target === cy) {
        onElementSelectRef.current?.(null);
      }
    });
    fitGraph(cy);
    cyRef.current = cy;
    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [stylesheet, topologyKey]);

  useEffect(() => {
    if (!focusRequest) {
      return;
    }
    const cy = cyRef.current;
    if (!cy) {
      return;
    }
    const target = cy.getElementById(focusRequest.elementId);
    if (target.empty()) {
      return;
    }
    cy.animate(
      {
        center: { eles: target },
        zoom: boundedFocusZoom(cy.zoom(), MIN_LEGIBLE_ZOOM, cy.maxZoom()),
      },
      { duration: 200 },
    );
  }, [focusRequest]);

  const withGraph = (action: (cy: Core) => void) => {
    if (cyRef.current) {
      action(cyRef.current);
    }
  };

  return (
    <div className="graph-frame">
      <GraphControls
        onZoomIn={() => withGraph((cy) => zoomGraph(cy, 1.2))}
        onZoomOut={() => withGraph((cy) => zoomGraph(cy, 1 / 1.2))}
        onFit={() => withGraph(fitGraph)}
        onReset={() => withGraph((cy) => resetGraph(cy, layoutRef.current))}
      />
      <div className="graph-shell" role="region" aria-label={ariaLabel}>
        <div className="graph-canvas" ref={containerRef} />
      </div>
    </div>
  );
}
