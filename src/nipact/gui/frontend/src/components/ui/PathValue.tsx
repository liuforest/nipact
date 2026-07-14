import { CopyButton } from "./CopyButton";

// Only paths longer than this ellipsize; shorter paths pass through untouched.
const COMPACT_MIN_LENGTH = 32;
const TAIL_SEGMENTS = 2;

// Keep the filename plus one leading directory: `…/leaf/file.ext`.
function ellipsizePath(value: string): string {
  const segments = value.split("/").filter(Boolean);
  if (segments.length <= TAIL_SEGMENTS) {
    return value;
  }
  const compact = `…/${segments.slice(-TAIL_SEGMENTS).join("/")}`;
  return compact.length < value.length ? compact : value;
}

export function PathValue({
  value,
  compact = false,
}: {
  value: string | null | undefined;
  compact?: boolean;
}) {
  if (!value) {
    return <span className="muted-value">none</span>;
  }
  if (compact && value.length > COMPACT_MIN_LENGTH) {
    const display = ellipsizePath(value);
    if (display !== value) {
      return (
        <span className="compact-value">
          <code className="inline-code path-value" title={value}>
            {display}
          </code>
          <CopyButton value={value} label="Copy path" />
        </span>
      );
    }
  }
  return (
    <code className="inline-code path-value" title={value}>
      {value}
    </code>
  );
}
