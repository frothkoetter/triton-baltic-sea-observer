import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "./api";
import { FilterRail } from "./components/FilterRail";
import { AnalysenPage } from "./pages/Analysen";
import { LagebildPage } from "./pages/Lagebild";
import { SqlWorkspacePage } from "./pages/SqlWorkspace";

type Zone = "lagebild" | "analysen" | "sql";

type Health = {
  dataSource: string;
  database: string;
  impala: { lastError?: string | null };
};

export default function App() {
  const [zone, setZone] = useState<Zone>("lagebild");
  const [health, setHealth] = useState<Health | null>(null);
  const [flag, setFlag] = useState("");
  const [vesselType, setVesselType] = useState("");
  const [shipName, setShipName] = useState("");
  const [filterOptions, setFilterOptions] = useState({
    flags: [] as string[],
    vesselTypes: [] as string[],
    shipNames: [] as string[],
  });

  useEffect(() => {
    void (async () => {
      const h = await fetchJson<Health>("/api/health");
      setHealth(h);
      const ais = await fetchJson<{ rows?: Record<string, unknown>[] }>("/api/ais");
      const rows = ais.rows ?? [];
      setFilterOptions({
        flags: uniq(rows.map((r) => String(r.flag ?? "")).filter(Boolean)),
        vesselTypes: uniq(rows.map((r) => String(r.vessel_type ?? "")).filter(Boolean)),
        shipNames: uniq(rows.map((r) => String(r.ship_name ?? "")).filter(Boolean)),
      });
    })();
  }, []);

  const zulu = useMemo(() => {
    return new Date().toISOString().slice(11, 19) + "Z";
  }, [zone]);

  const feedStatus =
    health?.dataSource === "impala" ? (
      <span className="status-ok">DATA FEED OK</span>
    ) : (
      <span className="status-warn">SAMPLE DATA</span>
    );

  return (
    <div className="app-root">
      <div className="classification-banner">NATO UNCLASSIFIED // TRAINING — TRITON BALTIC SEA OBSERVER</div>
      <header className="app-header">
        <div className="app-title">TRITON BALTIC SEA OBSERVER</div>
        <nav className="app-nav" aria-label="Main">
          {(
            [
              ["lagebild", "Lagebild"],
              ["analysen", "Analysen"],
              ["sql", "SQL"],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              type="button"
              className={`nav-btn ${zone === id ? "active" : ""}`}
              onClick={() => setZone(id)}
            >
              {label}
            </button>
          ))}
        </nav>
        <div className="mono" style={{ fontSize: 12, color: "var(--text-muted)" }}>
          Zulu: {zulu}
        </div>
      </header>

      <div className="app-body">
        <FilterRail
          {...filterOptions}
          flag={flag}
          vesselType={vesselType}
          shipName={shipName}
          onFlag={setFlag}
          onVesselType={setVesselType}
          onShipName={setShipName}
        />
        <main className="main-pane">
          {zone === "lagebild" && <LagebildPage />}
          {zone === "analysen" && <AnalysenPage />}
          {zone === "sql" && <SqlWorkspacePage />}
        </main>
      </div>

      <footer className="status-bar">
        <span>{feedStatus}</span>
        <span>AIS: {health?.dataSource === "impala" ? "LIVE" : "DEMO"}</span>
        <span>Schema: {health?.database ?? "defense"}</span>
        <span className="status-alert">Alerts: 2</span>
        {health?.impala?.lastError && health.dataSource !== "impala" && (
          <span title={health.impala.lastError}>Impala: offline</span>
        )}
      </footer>
      <div className="classification-banner">NATO UNCLASSIFIED // TRAINING</div>
    </div>
  );
}

function uniq(values: string[]) {
  return [...new Set(values)].sort();
}
