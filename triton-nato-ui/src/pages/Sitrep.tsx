import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";

type SitrepRow = {
  timestamp?: string;
  dtg?: string;
  pdf?: string;
  has_comparison?: boolean;
  row_count?: number;
  s3_pdf_uri?: string;
  precedence?: string;
};

export function SitrepPage() {
  const [rows, setRows] = useState<SitrepRow[]>([]);
  const [source, setSource] = useState("…");
  const [selectedPdf, setSelectedPdf] = useState<string | null>(null);
  const [pdfUrl, setPdfUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void (async () => {
      const res = await fetchJson<{ ok?: boolean; rows?: SitrepRow[]; source?: string; error?: string }>(
        "/api/sitrep/reports"
      );
      if (res.ok && res.rows) {
        setRows(res.rows);
        setSource(res.source ?? "unknown");
        if (res.rows[0]?.pdf) setSelectedPdf(res.rows[0].pdf);
      } else {
        setError(res.error ?? "Could not load SITREP index");
      }
    })();
  }, []);

  useEffect(() => {
    if (!selectedPdf) {
      setPdfUrl(null);
      return;
    }
    setError(null);
    setPdfUrl(`/api/sitrep/reports/${encodeURIComponent(selectedPdf)}/pdf`);
  }, [selectedPdf]);

  const latest = useMemo(() => rows[0], [rows]);

  return (
    <div className="sitrep-page">
      <div className="sitrep-toolbar">
        <span className="mono" style={{ fontSize: 11, color: "var(--text-muted)" }}>
          NATO SITREP · situation-awareness · source: {source}
        </span>
        {latest?.dtg ? (
          <span className="mono" style={{ fontSize: 11 }}>
            Latest DTG: {latest.dtg}
          </span>
        ) : null}
      </div>

      <div className="sitrep-grid">
        <aside className="panel sitrep-list-panel">
          <h2 className="panel-title">Reports ({rows.length})</h2>
          <ul className="sitrep-list">
            {rows.map((r) => (
              <li key={r.pdf ?? r.timestamp}>
                <button
                  type="button"
                  className={`sitrep-item ${selectedPdf === r.pdf ? "active" : ""}`}
                  onClick={() => r.pdf && setSelectedPdf(r.pdf)}
                >
                  <span className="mono">{r.dtg ?? r.timestamp}</span>
                  <span className="sitrep-meta">
                    {r.row_count ?? "—"} rows
                    {r.has_comparison ? " · Δ24/48h" : ""}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <section className="panel sitrep-viewer-panel">
          <h2 className="panel-title">{selectedPdf ?? "PDF"}</h2>
          {error ? <div className="error-box">{error}</div> : null}
          {pdfUrl ? (
            <iframe title="NATO SITREP PDF" className="sitrep-pdf-frame" src={pdfUrl} />
          ) : (
            <p className="mono" style={{ color: "var(--text-muted)" }}>
              Select a report.
            </p>
          )}
        </section>
      </div>
    </div>
  );
}