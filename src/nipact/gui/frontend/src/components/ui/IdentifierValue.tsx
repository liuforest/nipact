import { CopyButton } from "./CopyButton";

// Only values longer than this ellipsize; 16-char short hashes and numeric ids
// pass through untouched. Full 64-char digests and long composite ids compact.
const COMPACT_MIN_LENGTH = 20;
const HEAD = 8;
const TAIL = 8;

function ellipsizeMiddle(value: string): string {
  return `${value.slice(0, HEAD)}…${value.slice(-TAIL)}`;
}

export function IdentifierValue({
  value,
  compact = false,
}: {
  value: string | number | null | undefined;
  compact?: boolean;
}) {
  if (value === null || value === undefined || value === "") {
    return <span className="muted-value">none</span>;
  }
  const full = String(value);
  if (compact && full.length > COMPACT_MIN_LENGTH) {
    return (
      <span className="compact-value">
        <code className="inline-code identifier-value" title={full}>
          {ellipsizeMiddle(full)}
        </code>
        <CopyButton value={full} label="Copy identifier" />
      </span>
    );
  }
  return (
    <code className="inline-code identifier-value" title={full}>
      {value}
    </code>
  );
}
