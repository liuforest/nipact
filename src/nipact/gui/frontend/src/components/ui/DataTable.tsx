import type { Key, ReactNode } from "react";

export interface DataTableColumn<Row> {
  key: string;
  label: string;
  render: (row: Row) => ReactNode;
}

export function DataTable<Row>({
  rows,
  columns,
  getRowKey,
}: {
  rows: readonly Row[];
  columns: readonly DataTableColumn<Row>[];
  getRowKey?: (row: Row, rowIndex: number) => Key;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>{column.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={getRowKey ? getRowKey(row, rowIndex) : rowIndex}>
              {columns.map((column) => (
                <td key={column.key}>{column.render(row)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
