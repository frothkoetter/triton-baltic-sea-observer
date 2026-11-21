import { useEffect, useMemo, useState } from "react";
import { fetchJson, type NbaIncident } from "../api";
import { DataTable } from "../components/DataTable";
import { RouteTrackMap } from "../components/RouteTrackMap";
import {
  TacticalMap,
  type FleetPosition,
  type ObservationArea,
  type OstseeInfraRow,
  type OstseePipelineRow,
  type OutlierRow,
} from "../components/TacticalMap";
import {
  filterAisRows,
  filterFleetBySelection,
  filterOutliersByAis,
  fleetLayersAllowed,
  type LagebildFilters,
} from "../lib/lagebild-filters";
import { LAGEBILD_KPI_DEFS } from "../lib/lagebild-kpis";
import { buildTracksQueryString, rowsToTracks, type PositionTrack } from "../lib/position-tracks";

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

type Props = LagebildFilters;

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

function normalizeOstseeInfra(row: Record<string, unknown>): OstseeInfraRow | null {
  const wkt = String(row.geometrie_wkt ?? row.Geometrie_Wkt ?? "").trim();
  if (!wkt) return null;
  return {
    id: String(row.id ?? row.Id ?? ""),
    name: String(row.name ?? row.Name ?? ""),
    typ: String(row.typ ?? row.Typ ?? ""),
    geometrie_wkt: wkt,
    status: row.status != null ? String(row.status) : undefined,
    medium: row.medium != null ? String(row.medium) : undefined,
  };
}

function normalizeOstseePipeline(row: Record<string, unknown>): OstseePipelineRow | null {
  const wkt = String(row.geometrie_wkt ?? row.Geometrie_Wkt ?? "").trim();
  if (!wkt) return null;
  return {
    pipeline_name: String(row.pipeline_name ?? row.Pipeline_Name ?? ""),
    medium: row.medium != null ? String(row.medium) : undefined,
    status: row.status != null ? String(row.status) : undefined,
    geo_typ: row.geo_typ != null ? String(row.geo_typ) : undefined,
    geometrie_wkt: wkt,
    quelle: row.quelle != null ? String(row.quelle) : undefined,
  };
}

function normalizeArea(row: Record<string, unknown>): ObservationArea | null {
  const lat = Number(row.center_latitude ?? row.Center_Latitude);
  const lon = Number(row.center_longitude ?? row.Center_Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return {
    area_id: String(row.area_id ?? row.Area_Id ?? ""),
    center_latitude: lat,
    center_longitude: lon,
    polygon: row.polygon != null ? String(row.polygon) : undefined,
    country: row.country != null ? String(row.country) : undefined,
    area_type: row.area_type != null ? String(row.area_type) : undefined,
    tier: row.tier != null ? String(row.tier) : undefined,
    source: row.source != null ? String(row.source) : undefined,
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

const filtersActive = (f: LagebildFilters) => Boolean(f.flag || f.vesselType || f.shipName);

export function LagebildPage({ flag, vesselType, shipName }: Props) {
  const filters = useMemo(
    () => ({ flag, vesselType, shipName }),
    [flag, vesselType, shipName]
  );

  const [ais, setAis] = useState<AisRow[]>([]);
  const [areas, setAreas] = useState<ObservationArea[]>([]);
  const [ostseeInfra, setOstseeInfra] = useState<OstseeInfraRow[]>([]);
  const [ostseePipelines, setOstseePipelines] = useState<OstseePipelineRow[]>([]);
  const [kpis, setKpis] = useState<Kpis | null>(null);
  const [source, setSource] = useState("…");
  const [trackSource, setTrackSource] = useState("…");
  const [outliers, setOutliers] = useState<OutlierRow[]>([]);
  const [germanFleet, setGermanFleet] = useState<FleetPosition[]>([]);
  const [russianFleet, setRussianFleet] = useState<FleetPosition[]>([]);
  const [tracks, setTracks] = useState<PositionTrack[]>([]);
  const [tracksLoading, setTracksLoading] = useState(false);
  const [nbaIncidents, setNbaIncidents] = useState<NbaIncident[]>([]);
  const [nbaSource, setNbaSource] = useState("…");
  const [layers, setLayers] = useState({
    ais: true,
    areas: true,
    infrastructure: true,
    sanctions: true,
    outliers: true,
    germanFleet: true,
    russianFleet: true,
    nba: true,
    routeTracks: true,
  });

  const nbaOnMap = useMemo(
    () =>
      nbaIncidents
        .map((row) => {
          const lat = Number(row.latitude);
          const lon = Number(row.longitude);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
          return {
            flow_uuid: row.flow_uuid,
            latitude: lat,
            longitude: lon,
            priority: row.priority,
            theatre: row.theatre,
            alert_type: row.alert_type,
            best_coa_id: row.best_coa_id,
            status: row.status,
          };
        })
        .filter(Boolean) as import("../components/TacticalMap").NbaIncidentMarker[],
    [nbaIncidents]
  );

  function openNbaIncident(flowUuid: string) {
    sessionStorage.setItem("triton_nba_selected_flow", flowUuid);
    window.location.hash = "nba";
    window.dispatchEvent(new HashChangeEvent("hashchange"));
  }

  useEffect(() => {
    void (async () => {
      const [aisRes, areaRes, infraRes, outlierRes, fleetRes, kpiRes, nbaRes] = await Promise.all([
        fetchJson<{ rows?: AisRow[]; source?: string }>("/api/ais"),
        fetchJson<{ rows?: ObservationArea[]; source?: string }>("/api/observation-areas"),
        fetchJson<{
          infrastruktur?: OstseeInfraRow[];
          pipelines?: OstseePipelineRow[];
          source?: string;
        }>("/api/ostsee-infrastructure"),
        fetchJson<{ rows?: OutlierRow[]; source?: string }>("/api/vessel-outliers"),
        fetchJson<{
          german?: Record<string, unknown>[];
          russian?: Record<string, unknown>[];
          source?: string;
        }>("/api/fleet-positions"),
        fetchJson<Kpis & { ok: boolean }>("/api/kpis"),
        fetchJson<{ rows?: NbaIncident[]; source?: string }>("/api/nba/incidents"),
      ]);
      setNbaIncidents(nbaRes.rows ?? []);
      setNbaSource(nbaRes.source ?? "…");
      setAis(aisRes.rows ?? []);
      setAreas(
        (areaRes.rows ?? [])
          .map((r) => normalizeArea(r as Record<string, unknown>))
          .filter((r): r is ObservationArea => r != null)
      );
      setOstseeInfra(
        (infraRes.infrastruktur ?? []).map((r) => normalizeOstseeInfra(r as Record<string, unknown>)).filter(Boolean) as OstseeInfraRow[]
      );
      setOstseePipelines(
        (infraRes.pipelines ?? [])
          .map((r) => normalizeOstseePipeline(r as Record<string, unknown>))
          .filter(Boolean) as OstseePipelineRow[]
      );
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

  useEffect(() => {
    if (!layers.routeTracks) {
      setTracks([]);
      return;
    }
    let cancelled = false;
    setTracksLoading(true);
    void (async () => {
      const qs = buildTracksQueryString({
        hours: 24,
        flag: filters.flag,
        vesselType: filters.vesselType,
        shipName: filters.shipName,
        ais: layers.ais,
        german: layers.germanFleet,
        russian: layers.russianFleet,
      });
      const res = await fetchJson<{
        rows?: Record<string, unknown>[];
        source?: string;
      }>(`/api/position-tracks?${qs}`);
      if (cancelled) return;
      setTracks(rowsToTracks(res.rows ?? []));
      setTrackSource(res.source ?? "unknown");
      setTracksLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [filters, layers.routeTracks, layers.ais, layers.germanFleet, layers.russianFleet]);

  const filteredAis = useMemo(() => filterAisRows(ais, filters), [ais, filters]);

  const filteredOutliers = useMemo(
    () => filterOutliersByAis(outliers, filteredAis, filters),
    [outliers, filteredAis, filters]
  );

  const fleetAllow = useMemo(() => fleetLayersAllowed(filters), [filters]);

  const filteredGerman = useMemo(
    () =>
      fleetAllow.german ? filterFleetBySelection(germanFleet, filters, ais) : [],
    [germanFleet, filters, ais, fleetAllow.german]
  );

  const filteredRussian = useMemo(
    () =>
      fleetAllow.russian ? filterFleetBySelection(russianFleet, filters, ais) : [],
    [russianFleet, filters, ais, fleetAllow.russian]
  );

  const trackLegend = useMemo(() => {
    const aisN = tracks.filter((t) => t.kind === "ais").length;
    const deN = tracks.filter((t) => t.kind === "deu_fleet").length;
    const ruN = tracks.filter((t) => t.kind === "rus_fleet").length;
    return { aisN, deN, ruN, total: tracks.length };
  }, [tracks]);

  return (
    <div>
      {filtersActive(filters) && (
        <div className="filter-active-banner" role="status">
          Filter Rail active — map, table, and tracks respect flag / type / ship (
          {filteredAis.length} AIS matches)
        </div>
      )}

      <div className="kpi-row" role="group" aria-label="Lagebild Kennzahlen">
        {LAGEBILD_KPI_DEFS.map((def) => {
          const raw = kpis?.[def.key];
          const display = raw == null ? "—" : Number(raw).toLocaleString("de-DE");
          const nbaCount = nbaIncidents.length;
          const tooltip =
            def.key === "alerts"
              ? `${def.detail} Aktuell ${nbaCount.toLocaleString("de-DE")} NBA incident(s) (${nbaSource}).`
              : def.detail;
          return (
            <div key={def.key} className="kpi" title={tooltip}>
              <div className={`kpi-value${def.alert ? " status-alert" : ""}`}>{display}</div>
              <div className="kpi-label">{def.title}</div>
              <div className="kpi-hint">{def.hint}</div>
              {def.key === "alerts" ? (
                <button
                  type="button"
                  className="kpi-nba-count"
                  title="nba_incidents · Tab NBA HITL"
                  onClick={() => {
                    window.location.hash = "nba";
                    window.dispatchEvent(new HashChangeEvent("hashchange"));
                  }}
                >
                  NBA incidents{" "}
                  <span className="kpi-nba-count-value">{nbaCount.toLocaleString("de-DE")}</span>
                </button>
              ) : null}
            </div>
          );
        })}
      </div>
      {kpis?.source ? (
        <p className="kpi-source-line mono" title="Datenquelle der KPI-Abfragen">
          KPI-Daten: {kpis.source}
        </p>
      ) : null}

      <div className="layer-toggles">
        {(
          [
            "ais",
            "germanFleet",
            "russianFleet",
            "outliers",
            "routeTracks",
            "areas",
            "infrastructure",
            "nba",
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
              ? `AIS Live (${filteredAis.length})`
              : key === "germanFleet"
                ? `DEU Fleet (${filteredGerman.length})`
                : key === "russianFleet"
                  ? `RUS Fleet (${filteredRussian.length})`
                  : key === "outliers"
                    ? `AIS Outliers (${filteredOutliers.length})`
                    : key === "routeTracks"
                      ? `Route tracks 24h (${trackLegend.total})`
                      : key === "areas"
                        ? `Restricted Areas (${areas.length})`
                        : key === "infrastructure"
                          ? `Pipelines & Cables (${ostseeInfra.length + ostseePipelines.length})`
                          : key === "nba"
                            ? `NBA incidents (${nbaOnMap.length})`
                            : "Sanctions overlay"}
          </button>
        ))}
        <span className="mono" style={{ fontSize: 11, color: "var(--text-muted)", alignSelf: "center" }}>
          Data: {source}
          {layers.nba ? ` · NBA: ${nbaSource}` : ""}
        </span>
      </div>

      <section className="panel">
        <h2 className="panel-title">Lagebild — AIS &amp; Anomalies (visual 123)</h2>
        <TacticalMap
          ships={layers.ais ? filteredAis : []}
          areas={areas}
          infrastruktur={ostseeInfra}
          pipelines={ostseePipelines}
          outliers={layers.outliers ? filteredOutliers : []}
          germanFleet={layers.germanFleet ? filteredGerman : []}
          russianFleet={layers.russianFleet ? filteredRussian : []}
          nbaIncidents={nbaOnMap}
          showNba={layers.nba}
          onNbaIncidentClick={openNbaIncident}
          showSanctions={layers.sanctions}
          showAreas={layers.areas}
          showInfrastructure={layers.infrastructure}
        />
      </section>

      <section className="panel" style={{ marginTop: 8 }}>
        <h2 className="panel-title">
          Position tracks — AIS events, DEU &amp; RUS fleet (24h)
        </h2>
        <p className="panel-sub" style={{ margin: "0 0 8px", fontSize: 12, color: "var(--text-muted)" }}>
          {layers.routeTracks ? (
            <>
              {tracksLoading ? "Loading tracks…" : `${trackLegend.total} routes`}
              {" · "}
              AIS {trackLegend.aisN} · DEU {trackLegend.deN} · RUS {trackLegend.ruN}
              {" · "}
              source {trackSource}
            </>
          ) : (
            "Enable “Route tracks 24h” to load histories from ais_events_ice, marine_vessel_status, rus_naval_fleet_positions."
          )}
        </p>
        {layers.routeTracks && <RouteTrackMap tracks={tracks} />}
      </section>

      <section className="panel" style={{ marginTop: 8 }}>
        <h2 className="panel-title">AIS Positions (102)</h2>
        <DataTable
          columns={["mmsi", "ship_name", "vessel_type", "flag", "latitude", "longitude", "speed", "event_timestamp"]}
          rows={filteredAis.slice(0, 50) as unknown as Record<string, unknown>[]}
        />
      </section>
    </div>
  );
}