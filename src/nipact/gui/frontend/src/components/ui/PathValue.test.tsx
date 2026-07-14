import { render, screen, fireEvent } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PathValue } from "./PathValue";

const LONG_PATH = "runs/colors/base/step/output/color_007.json";

const writeText = vi.fn();

beforeEach(() => {
  writeText.mockReset().mockResolvedValue(undefined);
  Object.defineProperty(navigator, "clipboard", {
    value: { writeText },
    configurable: true,
  });
});

describe("PathValue", () => {
  it("renders 'none' for null, undefined, and empty", () => {
    const { rerender } = render(<PathValue value={null} />);
    expect(screen.getByText("none")).toBeInTheDocument();
    rerender(<PathValue value={undefined} />);
    expect(screen.getByText("none")).toBeInTheDocument();
    rerender(<PathValue value="" />);
    expect(screen.getByText("none")).toBeInTheDocument();
  });

  it("passes a short path through untouched in compact mode", () => {
    render(<PathValue value="data/x.json" compact />);
    expect(screen.getByText("data/x.json")).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("ellipsizes a long path in compact mode, keeping the trailing segments", () => {
    render(<PathValue value={LONG_PATH} compact />);
    expect(screen.getByText("…/output/color_007.json")).toBeInTheDocument();
    expect(screen.getByTitle(LONG_PATH)).toBeInTheDocument();
  });

  it("renders the full path in default (non-compact) mode", () => {
    render(<PathValue value={LONG_PATH} />);
    expect(screen.getByText(LONG_PATH)).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("copies the original path, not the ellipsized display, and shows feedback", async () => {
    render(<PathValue value={LONG_PATH} compact />);
    fireEvent.click(screen.getByRole("button", { name: "Copy path" }));
    expect(writeText).toHaveBeenCalledWith(LONG_PATH);
    expect(await screen.findByText("copied")).toBeInTheDocument();
  });

  it("surfaces a rejected clipboard write", async () => {
    writeText.mockRejectedValueOnce(new Error("denied"));
    render(<PathValue value={LONG_PATH} compact />);
    fireEvent.click(screen.getByRole("button", { name: "Copy path" }));
    expect(await screen.findByText("failed")).toBeInTheDocument();
  });
});
