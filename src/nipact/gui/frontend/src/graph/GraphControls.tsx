export function GraphControls({
  onZoomIn,
  onZoomOut,
  onFit,
  onReset,
}: {
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFit: () => void;
  onReset: () => void;
}) {
  return (
    <div className="graph-toolbar" role="toolbar" aria-label="Graph controls">
      <button
        aria-label="Zoom in"
        className="button graph-control-button"
        type="button"
        onClick={onZoomIn}
      >
        +
      </button>
      <button
        aria-label="Zoom out"
        className="button graph-control-button"
        type="button"
        onClick={onZoomOut}
      >
        -
      </button>
      <button className="button graph-control-button" type="button" onClick={onFit}>
        Fit
      </button>
      <button className="button graph-control-button" type="button" onClick={onReset}>
        Reset
      </button>
    </div>
  );
}
