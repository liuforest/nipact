export function EmptyPanel({
  title,
  children,
}: {
  title: string;
  children?: React.ReactNode;
}) {
  return (
    <section className="panel panel--muted">
      <h2>{title}</h2>
      {children}
    </section>
  );
}
