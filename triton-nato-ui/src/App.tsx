import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchJson, getClientDataMode, setServerDataMode } from "./api";
import { TRITON_UI_BUILD } from "./lib/build-info";
import { FilterRail } from "./components/FilterRail";
import { AnalysenPage } from "./pages/Analysen";
import { LagebildPage } from "./pages/Lagebild";
import { ProximityPage } from "./pages/Proximity";
import { NbaHitlPage } from "./pages/NbaHitl";
import { AgentConsolePage } from "./pages/AgentConsole";
import { LidarPage } from "./pages/Lidar";
import { SitrepPage } from "./pages/Sitrep";

type Zone = "lagebild" | "analysen" | "proximity" | "lidar" | "sitrep" | "agents" | "nba";

const ZONE_HASH: Record<Zone, string> = {
  lagebild: "",
  analysen: "analysen",
  proximity: "proximity",
  lidar: "lidar",
  sitrep: "sitrep",
  agents: "agents",
  nba: "nba",
};

function zoneFromHash(): Zone {
  const h = window.location.hash.replace(/^#/, "");
  if (
    h === "analysen" ||
    h === "proximity" ||
    h === "lidar" ||
    h === "sitrep" ||
    h === "agents" ||
    h === "sql" /* legacy hash → agents */ ||
    h === "nba"
  )
    if (h === "sql") return "agents";
    return h;
  return "lagebild";
}

type Health = {
  dataSource: string;
  database: string;
  catalog?: string;
  dataMode?: "live" | "sample" | "auto";
  dataModeLocked?: boolean;
  sampleReason?: string | null;
  warehouseProbeError?: string | null;
  shouldUseSample?: boolean;
  trinoConfigured?: boolean;
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
  const [dataMode, setDataMode] = useState<"live" | "sample" | "auto">(() => getClientDataMode());
  const [modeBusy, setModeBusy] = useState(false);
  const [flag, setFlag] = useState("");
  const [vesselType, setVesselType] = useState("");
  const [shipName, setShipName] = useState("");
  const [filterOptions, setFilterOptions] = useState({
    flags: [] as string[],
    vesselTypes: [] as string[],
    shipNames: [] as string[],
  });

  const refreshHealth = useCallback(async () => {
    const h = await fetchJson<Health>("/api/health");
    setHealth(h);
    if (h.dataMode && h.dataMode !== dataMode && !h.dataModeLocked) {
      setDataMode(h.dataMode);
    }
  }, [dataMode]);

  useEffect(() => {
    void (async () => {
      await refreshHealth();
      const ais = await fetchJson<{ rows?: Record<string, unknown>[] }>("/api/ais");
      const rows = ais.rows ?? [];
      setFilterOptions({
        flags: uniq(rows.map((r) => String(r.flag ?? "")).filter(Boolean)),
        vesselTypes: uniq(rows.map((r) => String(r.vessel_type ?? "")).filter(Boolean)),
        shipNames: uniq(rows.map((r) => String(r.ship_name ?? "")).filter(Boolean)),
      });
    })();
  }, [refreshHealth]);

  async function switchDataMode(next: "live" | "sample") {
    if (health?.dataModeLocked || modeBusy) return;
    setModeBusy(true);
    try {
      const res = await setServerDataMode(next);
      if (res.ok) {
        setDataMode(next);
        await refreshHealth();
        window.location.reload();
      }
    } finally {
      setModeBusy(false);
    }
  }

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
        <div className="app-brand">
          <img
            className="app-logo"
            src="/images/tritonLogo.jpg"
            alt="Triton"
            width={120}
            height={40}
          />
          <div className="app-title">TRITON BALTIC SEA OBSERVER</div>
        </div>
        <nav className="app-nav" aria-label="Main">
          {(
            [
              ["lagebild", "Lagebild"],
              ["analysen", "Analysen"],
              ["proximity", "Schiffsnähe"],
              ["lidar", "LiDAR"],
              ["sitrep", "SITREP"],
              ["agents", "Agenten"],
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
          {zone === "analysen" && (
            <AnalysenPage flag={flag} vesselType={vesselType} shipName={shipName} />
          )}
          {zone === "proximity" && <ProximityPage />}
          {zone === "lidar" && <LidarPage />}
          {zone === "sitrep" && <SitrepPage />}
          {zone === "agents" && <AgentConsolePage />}
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
        <span className="data-mode-toggle" title={health?.warehouseProbeError ?? health?.sampleReason ?? ""}>
          Daten:
          <button
            type="button"
            className={`data-mode-btn ${dataMode === "live" ? "active" : ""}`}
            disabled={health?.dataModeLocked || modeBusy}
            onClick={() => void switchDataMode("live")}
          >
            Trino live
          </button>
          <button
            type="button"
            className={`data-mode-btn ${dataMode === "sample" ? "active" : ""}`}
            disabled={health?.dataModeLocked || modeBusy}
            onClick={() => void switchDataMode("sample")}
          >
            Demo
          </button>
        </span>
        {health?.warehouseProbeError && dataMode === "live" ? (
          <span className="status-warn" title={health.warehouseProbeError}>
            Trino: {health.trinoConfigured ? "Fehler" : "keine Credentials"}
          </span>
        ) : null}
        {health?.impala?.lastError && !isLiveFeed(health?.dataSource) && (
          <span title={health.impala.lastError}>Warehouse: offline</span>
        )}
        <span className="mono" title="UI build — if not 0.2.0-data-mode run npm run build">
          Build {TRITON_UI_BUILD}
        </span>
      </footer>
      <div className="classification-banner">NATO UNCLASSIFIED // TRAINING</div>
    </div>
  );
}

function uniq(values: string[]) {
  return [...new Set(values)].sort();
}
