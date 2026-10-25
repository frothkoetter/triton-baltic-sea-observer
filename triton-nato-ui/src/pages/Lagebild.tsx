import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import { DataTable } from "../components/DataTable";
import { TacticalMap } from "../components/TacticalMap";

export type AisRow = {
  mmsi: number;
  ship_name?: string;
  vessel_type?: string;
  flag?: string;
  latitude: number;
  longitude: number;
  speed?: number;
  course?: number;
  event_timestamp?: string;
  status?: string;
  sanctioned?: boolean;
};

type Kpis = {
  fleetUnits: number;
  personnel: number;
  surveillanceReports: number;
  alerts: number;
  source?: string;
};

export function LagebildPage() {
  const [ais, setAis] = useState<AisRow[]>([]);
  const [areas, setAreas] = useState<{ area_id: string; center_latitude: number; center_longitude: number }[]>([]);
  const [kpis, setKpis] = useState<Kpis | null>(null);
  const [source, setSource] = useState("…");
  const [layers, setLayers] = useState({ ais: true, areas: true, sanctions: true });

  useEffect(() => {
    void (async () => {
      const [aisRes, areaRes, kpiRes] = await Promise.all([
        fetchJson<{ rows?: AisRow[]; source?: string }>("/api/ais"),
        fetchJson<{ rows?: typeof areas; source?: string }>("/api/observation-areas"),
        fetchJson<Kpis & { ok: boolean }>("/api/kpis"),
      ]);
      setAis(aisRes.rows ?? []);
      setAreas(areaRes.rows ?? []);
      setKpis(kpiRes);
      setSource(aisRes.source ?? "unknown");
    })();
  }, []);

  const filtered = useMemo(() => ais.filter((r) => Number.isFinite(r.latitude) && Number.isFinite(r.longitude)), [ais]);

  return (
    <div>
      <div className="kpi-row">
        <div className="kpi">
          <div className="kpi-value">{kpis?.fleetUnits ?? "—"}</div>
          <div className="kpi-label">Flotte (105)</div>
        </div>
        <div className="kpi">
          <div className="kpi-value">{kpis?.personnel ?? "—"}</div>
          <div className="kpi-label">Personal (95)</div>
        </div>
        <div className="kpi">
          <div className="kpi-value">{kpis?.surveillanceReports ?? "—"}</div>
          <div className="kpi-label">Surveillance (119)</div>
        </div>
        <div className="kpi">
          <div className="kpi-value status-alert">{kpis?.alerts ?? 0}</div>
          <div className="kpi-label">Alerts</div>
        </div>
      </div>

      <div className="layer-toggles">
        {(["ais", "areas", "sanctions"] as const).map((key) => (
          <button
            key={key}
            type="button"
            className={`chip ${layers[key] ? "on" : ""}`}
            onClick={() => setLayers((l) => ({ ...l, [key]: !l[key] }))}
          >
            {key === "ais" ? "AIS Live" : key === "areas" ? "Restricted Areas" : "Sanctions overlay"}
          </button>
        ))}
        <span className="mono" style={{ fontSize: 11, color: "var(--text-muted)", alignSelf: "center" }}>
          Data: {source}
        </span>
      </div>

      <section className="panel">
        <h2 className="panel-title">Lagebild — AIS Layer (visual 123)</h2>
        <TacticalMap
          ships={layers.ais ? filtered : []}
          areas={layers.areas ? areas : []}
          showSanctions={layers.sanctions}
        />
      </section>

      <section className="panel" style={{ marginTop: 8 }}>
        <h2 className="panel-title">AIS Positions (102)</h2>
        <DataTable
          columns={["mmsi", "ship_name", "vessel_type", "flag", "latitude", "longitude", "speed", "event_timestamp"]}
          rows={filtered.slice(0, 50) as unknown as Record<string, unknown>[]}
        />
      </section>
    </div>
  );
}
