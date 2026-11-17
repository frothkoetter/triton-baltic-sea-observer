type Props = {
  columns: string[];
  rows: Record<string, unknown>[];
  hostileColumn?: string;
  onRowClick?: (row: Record<string, unknown>) => void;
};

export function DataTable({ columns, rows, hostileColumn, onRowClick }: Props) {
  if (!rows.length) {
    return <p className="mono" style={{ color: "var(--text-muted)" }}>No rows</p>;
  }
  const cols = columns.length ? columns : Object.keys(rows[0]);
  return (
    <div style={{ overflow: "auto", maxHeight: 320 }}>
      <table className="data-table">
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              className={
                hostileColumn && row[hostileColumn] ? "hostile-row" : undefined
              }
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              style={onRowClick ? { cursor: "pointer" } : undefined}
            >
              {cols.map((c) => (
                <td key={c} className="mono">
                  {formatCell(row[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(v: unknown) {
  if (v == null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
