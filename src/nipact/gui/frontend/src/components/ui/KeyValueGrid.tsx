export interface KeyValueItem {
  label: string;
  value: React.ReactNode;
}

export function KeyValueGrid({ items }: { items: readonly KeyValueItem[] }) {
  return (
    <dl className="detail-grid">
      {items.map((item) => (
        <div key={item.label}>
          <dt>{item.label}</dt>
          <dd>{item.value ?? "none"}</dd>
        </div>
      ))}
    </dl>
  );
}
