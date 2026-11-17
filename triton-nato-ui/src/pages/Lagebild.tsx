import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import { DataTable } from "../components/DataTable";
import { TacticalMap, type FleetPosition, type OutlierRow } from "../components/TacticalMap";

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

function normalizeFleet(row: Record<string, unknown>): FleetPosition | null {
  const lat = Number(row.latitude ?? row.Latitude);
  const lon = Number(row.longitude ?? row.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return {
    mmsi: String(row.mmsi ?? row.MMSI ?? ""),
    latitude: lat,
    longitude: lon,
    unit_name: row.unit_name ? String(row.unit_name) : undefined,
    unit_class: row.unit_class ? String(row.unit_class) : undefined,
    speed: Number(row.speed ?? row.Speed ?? NaN),
    course: Number(row.course ?? row.Course ?? NaN),
    status: row.status ? String(row.status) : undefined,
    operational_status: row.operational_status
      ? String(row.operational_status)
      : undefined,
    home_base: row.home_base ? String(row.home_base) : undefined,
    vessel_category: row.vessel_category ? String(row.vessel_category) : undefined,
    event_timestamp: String(row.event_timestamp ?? row.Event_Timestamp ?? ""),
  };
}

function normalizeOutlier(row: Record<string, unknown>): OutlierRow | null {
  const lat = Number(row.latitude ?? row.Latitude);
  const lon = Number(row.longitude ?? row.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return {
    mmsi: Number(row.mmsi ?? row.MMSI ?? row.id ?? row.ID),
    latitude: lat,
    longitude: lon,
    event_timestamp: String(row.event_timestamp ?? row.Event_Timestamp ?? ""),
    outlier_type: String(row.outlier_type ?? row.Outlier_Type ?? "Anomaly"),
    reason: String(row.reason ?? row.Reason ?? row.details ?? ""),
    severity_level: Number(row.severity_level ?? row.Severity_Level ?? 50),
    speed: Number(row.speed ?? row.Speed ?? NaN),
    harbour_name: row.harbour_name ? String(row.harbour_name) : undefined,
  };
}

export function LagebildPage() {
  const [ais, setAis] = useState<AisRow[]>([]);
  const [areas, setAreas] = useState<{ area_id: string; center_latitude: number; center_longitude: number }[]>([]);
  const [kpis, setKpis] = useState<Kpis | null>(null);
  const [source, setSource] = useState("…");
  const [outliers, setOutliers] = useState<OutlierRow[]>([]);
  const [germanFleet, setGermanFleet] = useState<FleetPosition[]>([]);
  const [russianFleet, setRussianFleet] = useState<FleetPosition[]>([]);
  const [layers, setLayers] = useState({
    ais: true,
    areas: true,
    sanctions: true,
    outliers: true,
    germanFleet: true,
    russianFleet: true,
  });

  useEffect(() => {
    void (async () => {
      const [aisRes, areaRes, outlierRes, fleetRes, kpiRes] = await Promise.all([
        fetchJson<{ rows?: AisRow[]; source?: string }>("/api/ais"),
        fetchJson<{ rows?: typeof areas; source?: string }>("/api/observation-areas"),
        fetchJson<{ rows?: OutlierRow[]; source?: string }>("/api/vessel-outliers"),
        fetchJson<{
          german?: Record<string, unknown>[];
          russian?: Record<string, unknown>[];
          source?: string;
        }>("/api/fleet-positions"),
        fetchJson<Kpis & { ok: boolean }>("/api/kpis"),
      ]);
      setAis(aisRes.rows ?? []);
      setAreas(areaRes.rows ?? []);
      setOutliers(
        (outlierRes.rows ?? [])
          .map((r) => normalizeOutlier(r as Record<string, unknown>))
          .filter((r): r is OutlierRow => r != null)
      );
      setGermanFleet(
        (fleetRes.german ?? [])
          .map((r) => normalizeFleet(r))
          .filter((r): r is FleetPosition => r != null)
      );
      setRussianFleet(
        (fleetRes.russian ?? [])
          .map((r) => normalizeFleet(r))
          .filter((r): r is FleetPosition => r != null)
      );
      setKpis(kpiRes);
      setSource(aisRes.source ?? fleetRes.source ?? "unknown");
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
        {(
          [
            "ais",
            "germanFleet",
            "russianFleet",
            "outliers",
            "areas",
            "sanctions",
          ] as const
        ).map((key) => (
          <button
            key={key}
            type="button"
            className={`chip ${layers[key] ? "on" : ""}`}
            onClick={() => setLayers((l) => ({ ...l, [key]: !l[key] }))}
          >
            {key === "ais"
              ? "AIS Live"
              : key === "germanFleet"
                ? `DEU Fleet (${germanFleet.length})`
                : key === "russianFleet"
                  ? `RUS Fleet (${russianFleet.length})`
                  : key === "outliers"
                    ? `AIS Outliers (${outliers.length})`
                    : key === "areas"
                      ? "Restricted Areas"
                      : "Sanctions overlay"}
          </button>
        ))}
        <span className="mono" style={{ fontSize: 11, color: "var(--text-muted)", alignSelf: "center" }}>
          Data: {source}
        </span>
      </div>

      <section className="panel">
        <h2 className="panel-title">Lagebild — AIS &amp; Anomalies (visual 123)</h2>
        <TacticalMap
          ships={layers.ais ? filtered : []}
          areas={layers.areas ? areas : []}
          outliers={layers.outliers ? outliers : []}
          germanFleet={layers.germanFleet ? germanFleet : []}
          russianFleet={layers.russianFleet ? russianFleet : []}
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
