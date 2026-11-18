type Props = {
  columns: string[];
  rows: Record<string, unknown>[];
  hostileColumn?: string;
  /** Per displayed column: row field name to use as HTML title (full text tooltip). */
  titleFromColumn?: Record<string, string>;
  onRowClick?: (row: Record<string, unknown>) => void;
};

export function DataTable({ columns, rows, hostileColumn, titleFromColumn, onRowClick }: Props) {
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
              {cols.map((c) => {
                const titleKey = titleFromColumn?.[c];
                const title = titleKey ? formatCell(row[titleKey]) : undefined;
                return (
                  <td key={c} className="mono" title={title !== "—" ? title : undefined}>
                    {formatCell(row[c])}
                  </td>
                );
              })}
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
