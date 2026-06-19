export function PathValue({ value }: { value: string | null | undefined }) {
  if (!value) {
    return <span className="muted-value">none</span>;
  }
  return (
    <code className="inline-code path-value" title={value}>
      {value}
    </code>
  );
}
