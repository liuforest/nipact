export function WarningList({
  warnings,
}: {
  warnings: readonly (
    | string
    | {
        message: string;
        warning_type?: string;
        artifact_id?: number | null;
        input_path?: string | null;
      }
  )[];
}) {
  if (warnings.length === 0) {
    return null;
  }
  return (
    <section className="panel panel--warning">
      <h2>Warnings</h2>
      <ul>
        {warnings.map((warning, index) => {
          const details =
            typeof warning === "string"
              ? []
              : [
                  warning.warning_type,
                  warning.artifact_id == null
                    ? null
                    : `artifact_id=${warning.artifact_id}`,
                  warning.input_path == null
                    ? null
                    : `input_path=${warning.input_path}`,
                ].filter((value): value is string => value != null);
          const text =
            typeof warning === "string"
              ? warning
              : [warning.message, ...details].join(" | ");
          const key =
            typeof warning === "string"
              ? `${warning}-${index}`
              : `${warning.warning_type ?? "warning"}-${index}`;
          return (
            <li className="warning-list-item" key={key}>
              {text}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
