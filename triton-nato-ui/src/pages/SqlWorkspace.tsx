import { useEffect, useState } from "react";
import { fetchJson, postQuery, type QueryResult } from "../api";
import { DataTable } from "../components/DataTable";

type Dataset = { name: string; tables: string[]; badge: string };

export function SqlWorkspacePage() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [starters, setStarters] = useState<Record<string, string>>({});
  const [tables, setTables] = useState<{ name: string }[]>([]);
  const [sql, setSql] = useState("SELECT * FROM defense.ships LIMIT 50");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    void (async () => {
      const [ds, tb] = await Promise.all([
        fetchJson<{ datasets: Dataset[]; starters: Record<string, string> }>("/api/datasets"),
        fetchJson<QueryResult>("/api/tables"),
      ]);
      setDatasets(ds.datasets);
      setStarters(ds.starters);
      setTables((tb.rows ?? []).map((r) => ({ name: String(r.name ?? r.tab_name ?? Object.values(r)[0]) })));
    })();
  }, []);

  async function run() {
    setLoading(true);
    setResult(null);
    const res = await postQuery(sql);
    setResult(res);
    setLoading(false);
  }

  return (
    <div className="sql-layout">
      <aside className="panel">
        <h2 className="panel-title">Dataset catalog (12)</h2>
        {datasets.map((d) => (
          <button
            key={d.name}
            type="button"
            className="catalog-item"
            onClick={() => setSql(starters[d.name]?.trim() ?? `-- ${d.name}\nSELECT * FROM defense.${d.tables[0]} LIMIT 100`)}
          >
            {d.name}
            <span className="badge">{d.badge}</span>
          </button>
        ))}
        <h2 className="panel-title" style={{ marginTop: 16 }}>
          Tables (defense)
        </h2>
        <ul style={{ margin: 0, paddingLeft: 16, fontSize: 12, color: "var(--text-muted)" }}>
          {tables.map((t) => (
            <li key={t.name} className="mono">
              {t.name}
            </li>
          ))}
        </ul>
      </aside>

      <div>
        <section className="panel">
          <h2 className="panel-title">SQL — read only</h2>
          <textarea
            className="sql-editor"
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            spellCheck={false}
          />
          <div className="sql-actions">
            <button type="button" className="btn-primary" disabled={loading} onClick={() => void run()}>
              Ausführen
            </button>
          </div>
          {result?.error && <div className="error-box">{result.error}</div>}
          {result?.ok && (
            <p style={{ fontSize: 11, color: "var(--text-muted)" }}>
              {result.rows?.length ?? 0} rows · source: {result.source}
            </p>
          )}
        </section>
        <section className="panel" style={{ marginTop: 8 }}>
          <h2 className="panel-title">Results</h2>
          {result?.ok ? (
            <DataTable columns={result.columns ?? []} rows={result.rows ?? []} />
          ) : (
            <p style={{ color: "var(--text-muted)", fontSize: 12 }}>Run a SELECT against the defense schema.</p>
          )}
        </section>
      </div>
    </div>
  );
}
