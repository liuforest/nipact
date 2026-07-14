// Bounded focus zoom: centering on a single element should zoom IN to a legible
// level but never zoom out (keep the user's zoom if it is already closer) and
// never exceed the graph's max zoom. Fitting to a single element over-zooms badly.
export function boundedFocusZoom(
  currentZoom: number,
  minLegibleZoom: number,
  maxZoom: number,
): number {
  return Math.min(maxZoom, Math.max(currentZoom, minLegibleZoom));
}
