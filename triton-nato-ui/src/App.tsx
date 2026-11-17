import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "./api";
import { FilterRail } from "./components/FilterRail";
import { AnalysenPage } from "./pages/Analysen";
import { LagebildPage } from "./pages/Lagebild";
import { ProximityPage } from "./pages/Proximity";
import { NbaHitlPage } from "./pages/NbaHitl";
import { SqlWorkspacePage } from "./pages/SqlWorkspace";
import { LidarPage } from "./pages/Lidar";
import { SitrepPage } from "./pages/Sitrep";

type Zone = "lagebild" | "analysen" | "proximity" | "lidar" | "sitrep" | "sql" | "nba";

const ZONE_HASH: Record<Zone, string> = {
  lagebild: "",
  analysen: "analysen",
  proximity: "proximity",
  lidar: "lidar",
  sitrep: "sitrep",
  sql: "sql",
  nba: "nba",
};

function zoneFromHash(): Zone {
  const h = window.location.hash.replace(/^#/, "");
  if (
    h === "analysen" ||
    h === "proximity" ||
    h === "lidar" ||
    h === "sitrep" ||
    h === "sql" ||
    h === "nba"
  )
    return h;
  return "lagebild";
}

type Health = {
  dataSource: string;
  database: string;
  catalog?: string;
  impala: { lastError?: string | null; engine?: string | null };
  nba?: { trinoLive?: boolean; lastError?: string | null };
};

function isLiveFeed(source: string | undefined) {
  return source === "impala" || source === "trino";
}

export default function App() {
  const [zone, setZone] = useState<Zone>(() => zoneFromHash());

  useEffect(() => {
    const onHash = () => setZone(zoneFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  function navigate(next: Zone) {
    setZone(next);
    const hash = ZONE_HASH[next];
    window.location.hash = hash ? `#${hash}` : "";
  }
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

  const feedStatus = isLiveFeed(health?.dataSource) ? (
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
              ["proximity", "Schiffsnähe"],
              ["lidar", "LiDAR"],
              ["sitrep", "SITREP"],
              ["sql", "SQL"],
              ["nba", "NBA HITL"],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              type="button"
              className={`nav-btn ${zone === id ? "active" : ""}`}
              onClick={() => navigate(id)}
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
          {zone === "lagebild" && (
            <LagebildPage flag={flag} vesselType={vesselType} shipName={shipName} />
          )}
          {zone === "analysen" && <AnalysenPage />}
          {zone === "proximity" && <ProximityPage />}
          {zone === "lidar" && <LidarPage />}
          {zone === "sitrep" && <SitrepPage />}
          {zone === "sql" && <SqlWorkspacePage />}
          {zone === "nba" && <NbaHitlPage />}
        </main>
      </div>

      <footer className="status-bar">
        <span>{feedStatus}</span>
        <span>
          AIS: {isLiveFeed(health?.dataSource)
            ? `LIVE (${health?.dataSource})`
            : "DEMO"}
        </span>
        <span>
          Schema: {health?.catalog ? `${health.catalog}.` : ""}
          {health?.database ?? "defense"}
        </span>
        <span className="status-alert">Alerts: 2</span>
        {health?.impala?.lastError && !isLiveFeed(health?.dataSource) && (
          <span title={health.impala.lastError}>Warehouse: offline</span>
        )}
      </footer>
      <div className="classification-banner">NATO UNCLASSIFIED // TRAINING</div>
    </div>
  );
}

function uniq(values: string[]) {
  return [...new Set(values)].sort();
}
