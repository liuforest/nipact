import { render, screen, fireEvent } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { IdentifierValue } from "./IdentifierValue";

// A 64-char digest with a distinct head and tail so the middle-ellipsis is
// verifiably grabbing the right ends.
const LONG_DIGEST =
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const writeText = vi.fn();

beforeEach(() => {
  writeText.mockReset().mockResolvedValue(undefined);
  Object.defineProperty(navigator, "clipboard", {
    value: { writeText },
    configurable: true,
  });
});

describe("IdentifierValue", () => {
  it("renders 'none' for null, undefined, and empty", () => {
    const { rerender } = render(<IdentifierValue value={null} />);
    expect(screen.getByText("none")).toBeInTheDocument();
    rerender(<IdentifierValue value={undefined} />);
    expect(screen.getByText("none")).toBeInTheDocument();
    rerender(<IdentifierValue value="" />);
    expect(screen.getByText("none")).toBeInTheDocument();
  });

  it("passes numeric and short values through untouched even in compact mode", () => {
    const { rerender } = render(<IdentifierValue value={42} compact />);
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();

    // 16-char short hash is below the compaction threshold.
    rerender(<IdentifierValue value="0123456789abcdef" compact />);
    expect(screen.getByText("0123456789abcdef")).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("middle-ellipsizes a long identifier in compact mode, keeping head and tail", () => {
    render(<IdentifierValue value={LONG_DIGEST} compact />);
    expect(screen.getByText("01234567…89abcdef")).toBeInTheDocument();
    // The full value stays available via title, not only there.
    expect(screen.getByTitle(LONG_DIGEST)).toBeInTheDocument();
  });

  it("renders the full value in default (non-compact) mode", () => {
    render(<IdentifierValue value={LONG_DIGEST} />);
    expect(screen.getByText(LONG_DIGEST)).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("copies the original value, not the ellipsized display, and shows feedback", async () => {
    render(<IdentifierValue value={LONG_DIGEST} compact />);
    const copy = screen.getByRole("button", { name: "Copy identifier" });
    fireEvent.click(copy);
    expect(writeText).toHaveBeenCalledWith(LONG_DIGEST);
    expect(await screen.findByText("copied")).toBeInTheDocument();
  });

  it("surfaces a rejected clipboard write", async () => {
    writeText.mockRejectedValueOnce(new Error("denied"));
    render(<IdentifierValue value={LONG_DIGEST} compact />);
    fireEvent.click(screen.getByRole("button", { name: "Copy identifier" }));
    expect(await screen.findByText("failed")).toBeInTheDocument();
  });
});
