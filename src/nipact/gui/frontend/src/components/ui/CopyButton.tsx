import { useState } from "react";

type CopyStatus = "idle" | "copied" | "error";

const STATUS_LABEL: Record<CopyStatus, string> = {
  idle: "copy",
  copied: "copied",
  error: "failed",
};

// Recovery control for a middle-ellipsized value: copies the ORIGINAL full
// value (never the truncated display) and shows visible success/failure
// feedback. `label` is the accessible name; the visible text is the feedback.
export function CopyButton({ value, label }: { value: string; label: string }) {
  const [status, setStatus] = useState<CopyStatus>("idle");

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(value);
      setStatus("copied");
    } catch {
      setStatus("error");
    }
  }

  return (
    <button
      type="button"
      className="copy-button"
      onClick={handleCopy}
      aria-label={label}
    >
      {STATUS_LABEL[status]}
    </button>
  );
}
