import { describe, expect, it } from "vitest";
import { boundedFocusZoom } from "./focusZoom";

describe("boundedFocusZoom", () => {
  it("raises a far-out zoom up to the legible minimum", () => {
    expect(boundedFocusZoom(0.4, 1, 3)).toBe(1);
  });

  it("keeps the current zoom when already closer than the minimum", () => {
    expect(boundedFocusZoom(1.8, 1, 3)).toBe(1.8);
  });

  it("never exceeds the max zoom", () => {
    expect(boundedFocusZoom(5, 1, 3)).toBe(3);
    expect(boundedFocusZoom(0.4, 4, 3)).toBe(3);
  });
});
