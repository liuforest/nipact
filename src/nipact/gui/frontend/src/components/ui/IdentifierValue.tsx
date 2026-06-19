export function IdentifierValue({
  value,
}: {
  value: string | number | null | undefined;
}) {
  if (value === null || value === undefined || value === "") {
    return <span className="muted-value">none</span>;
  }
  return (
    <code className="inline-code identifier-value" title={String(value)}>
      {value}
    </code>
  );
}
