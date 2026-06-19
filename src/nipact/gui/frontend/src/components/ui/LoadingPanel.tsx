export function LoadingPanel({ label = "Loading" }: { label?: string }) {
  return (
    <section className="panel panel--muted" aria-live="polite">
      <p className="status-line">{label}</p>
    </section>
  );
}
