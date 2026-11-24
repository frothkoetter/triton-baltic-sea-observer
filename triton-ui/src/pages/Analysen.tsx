import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { fetchJson, type QueryResult } from "../api";
import { aisAnalyticsQuery, type AisAnalyticsPayload } from "../lib/ais-analytics-api";
import type { LagebildFilters } from "../lib/lagebild-filters";
import { BuoyAnalyticsMap } from "../components/BuoyAnalyticsMap";
import { RestrictedZoneMap } from "../components/RestrictedZoneMap";
import { DataTable } from "../components/DataTable";
import { rowsToRestrictedTracks } from "../lib/restricted-tracks";
import type { ObservationArea } from "../components/TacticalMap";
import {
  buildClassificationScatter,
  confidenceColor,
  type ScatterPoint,
} from "../lib/buoy-analytics";
import {
  BLUE_FLEET_LABEL,
  BLUE_FLEET_STAMMDATEN,
  RED_FLEET_LABEL,
  RED_FLEET_STAMMDATEN,
} from "../lib/fleet-labels";

const TABS = [
  { id: "fleet", label: BLUE_FLEET_LABEL },
  { id: "rus", label: RED_FLEET_LABEL },
  { id: "ais", label: "AIS" },
  { id: "outliers", label: "AIS Outliers" },
  { id: "sanctions", label: "Sanctions" },
  { id: "buoys", label: "Buoys" },
  { id: "restricted", label: "Restricted Areas" },
] as const;

type TabId = (typeof TABS)[number]["id"];

const CHART_COLORS = ["#4DA3FF", "#8B9CAB", "#3DDC84", "#FFB020", "#E53935", "#FFD54F"];

const CHART_TOOLTIP_PROPS = {
  contentStyle: {
    background: "#1C2630",
    border: "1px solid #2A3540",
    color: "#E6EDF3",
  },
  labelStyle: { color: "#E6EDF3" },
  itemStyle: { color: "#8B9CAB" },
};

type AnalyticsPanel = {
  columns?: string[];
  rows?: Record<string, unknown>[];
};

type BuoyAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  panels?: {
    recent?: AnalyticsPanel;
    typeMix?: AnalyticsPanel;
    classificationGrid?: AnalyticsPanel;
    geoClusters?: AnalyticsPanel;
  };
};

type RusFleetAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  panels?: {
    unitDisposition?: AnalyticsPanel;
    activityMix?: AnalyticsPanel;
    categoryMix?: AnalyticsPanel;
    homeBaseMix?: AnalyticsPanel;
    restrictedUnits?: AnalyticsPanel;
    restrictedByArea?: AnalyticsPanel;
  };
};

type RestrictedAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  panels?: {
    kpi?: AnalyticsPanel;
    byZone?: AnalyticsPanel;
    byFlag?: AnalyticsPanel;
    topByType?: AnalyticsPanel;
    topVessels?: AnalyticsPanel;
    provocateurs?: AnalyticsPanel;
    routeTracks?: AnalyticsPanel;
  };
};

type VesselOutlierAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  panels?: {
    kpi?: AnalyticsPanel;
    byType?: AnalyticsPanel;
    bySeverity?: AnalyticsPanel;
    topMmsi?: AnalyticsPanel;
    recent?: AnalyticsPanel;
    hourly?: AnalyticsPanel;
  };
};

type SanctionedAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  panels?: {
    kpi?: AnalyticsPanel;
    registry?: AnalyticsPanel;
    destinationHistory?: AnalyticsPanel;
    topRoutes?: AnalyticsPanel;
    byReason?: AnalyticsPanel;
    byFlag?: AnalyticsPanel;
  };
};

type FleetAnalyticsPayload = {
  ok?: boolean;
  source?: string;
  fallbackReason?: string;
  panels?: {
    messagesFrom?: AnalyticsPanel;
    messagesTo?: AnalyticsPanel;
    operationalStatus?: AnalyticsPanel;
    systemStatus?: AnalyticsPanel;
    destinations?: AnalyticsPanel;
    unitStatus?: AnalyticsPanel;
    recentMessages?: AnalyticsPanel;
  };
};

function chartRows(rows: Record<string, unknown>[] | undefined) {
  if (!rows?.length) return [];
  return rows.map((r) => ({
    name: String(r.name ?? "—"),
    value: Number(r.value ?? 0),
  }));
}

function hourlyBars(rows: Record<string, unknown>[] | undefined) {
  if (!rows?.length) return [];
  return rows.map((r) => {
    const raw = r.hour_bucket ?? r.Hour_Bucket;
    const label =
      raw == null
        ? "—"
        : typeof raw === "string"
          ? raw.replace("T", " ").replace(/\+.*/, "").slice(0, 16)
          : String(raw);
    return { name: label, value: Number(r.event_count ?? 0) };
  });
}

function provocateurBars(rows: Record<string, unknown>[] | undefined) {
  if (!rows?.length) return [];
  return rows.map((r) => ({
    name: String(r.ship_name ?? r.mmsi ?? "—"),
    value: Number(r.zones_hit ?? r.reports_inside ?? 0),
  }));
}

type AnalysenPageProps = { filters: LagebildFilters };

export function AnalysenPage({ filters }: AnalysenPageProps) {
  const [tab, setTab] = useState<TabId>("fleet");
  const [fleetAnalytics, setFleetAnalytics] = useState<FleetAnalyticsPayload | null>(null);
  const [fleetRegistry, setFleetRegistry] = useState<QueryResult | null>(null);
  const [ais, setAis] = useState<QueryResult | null>(null);
  const [aisAnalytics, setAisAnalytics] = useState<AisAnalyticsPayload | null>(null);
  const [sanctionedAnalytics, setSanctionedAnalytics] = useState<SanctionedAnalyticsPayload | null>(null);
  const [restrictedAnalytics, setRestrictedAnalytics] = useState<RestrictedAnalyticsPayload | null>(null);
  const [observationAreas, setObservationAreas] = useState<ObservationArea[]>([]);
  const [buoyAnalytics, setBuoyAnalytics] = useState<BuoyAnalyticsPayload | null>(null);
  const [rusFleetAnalytics, setRusFleetAnalytics] = useState<RusFleetAnalyticsPayload | null>(null);
  const [rusFleetRegistry, setRusFleetRegistry] = useState<QueryResult | null>(null);
  const [outlierAnalytics, setOutlierAnalytics] = useState<VesselOutlierAnalyticsPayload | null>(null);

  const filterQs = aisAnalyticsQuery(filters);

  useEffect(() => {
    void (async () => {
      const [fa, fr, bu, ru, rfr, vo, s, rz, areas] = await Promise.all([
        fetchJson<FleetAnalyticsPayload>("/api/analyses/fleet-analytics"),
        fetchJson<QueryResult>("/api/analyses/fleet"),
        fetchJson<BuoyAnalyticsPayload>("/api/analyses/buoy-analytics"),
        fetchJson<RusFleetAnalyticsPayload>("/api/analyses/rus-fleet-analytics"),
        fetchJson<QueryResult>("/api/analyses/rus-fleet"),
        fetchJson<VesselOutlierAnalyticsPayload>("/api/analyses/vessel-outlier-analytics"),
        fetchJson<SanctionedAnalyticsPayload>("/api/analyses/sanctioned-analytics"),
        fetchJson<RestrictedAnalyticsPayload>("/api/analyses/restricted-analytics"),
        fetchJson<QueryResult & { rows?: ObservationArea[] }>("/api/observation-areas"),
      ]);
      setFleetAnalytics(fa);
      setFleetRegistry(fr);
      setBuoyAnalytics(bu);
      setRusFleetAnalytics(ru);
      setRusFleetRegistry(rfr);
      setOutlierAnalytics(vo);
      setSanctionedAnalytics(s);
      setRestrictedAnalytics(rz);
      setObservationAreas((areas.rows ?? []) as ObservationArea[]);
    })();
  }, []);

  useEffect(() => {
    void (async () => {
      const [aa, a] = await Promise.all([
        fetchJson<AisAnalyticsPayload>(`/api/analyses/ais-analytics${filterQs}`),
        fetchJson<QueryResult>(`/api/ais${filterQs}`),
      ]);
      setAisAnalytics(aa);
      setAis(a);
    })();
  }, [filterQs]);

  const p = fleetAnalytics?.panels;
  const bp = buoyAnalytics?.panels;
  const geoClusterRows =
    bp?.geoClusters?.rows ??
    (bp as { geo?: { rows?: Record<string, unknown>[] } })?.geo?.rows ??
    [];
  const geoClusters = geoClusterRows
    .map((r) => {
      const lat = Number(
        r.geo_position_lat ?? r.Geo_Position_Lat ?? r.latitude ?? r.Latitude
      );
      const lon = Number(
        r.geo_position_lon ?? r.Geo_Position_Lon ?? r.longitude ?? r.Longitude
      );
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
      return {
        geo_position_lat: lat,
        geo_position_lon: lon,
        record_count: Number(r.record_count ?? r.Record_Count ?? 0),
        sum_altitude: Number(r.sum_altitude ?? r.Sum_Altitude ?? 0),
      };
    })
    .filter(Boolean) as {
    geo_position_lat: number;
    geo_position_lon: number;
    record_count: number;
    sum_altitude: number;
  }[];
  const rp = rusFleetAnalytics?.panels;
  const rusUnits = rp?.unitDisposition?.rows ?? [];
  const rusKpis = {
    registered: rusUnits.length,
    atHome: rusUnits.filter((u) => u.at_home_port === true || u.at_home_port === "true").length,
    inRestricted: rp?.restrictedUnits?.rows?.length ?? 0,
    transit: rusUnits.filter((u) => String(u.activity) === "Transit").length,
  };
  const op = outlierAnalytics?.panels;
  const outlierKpi = op?.kpi?.rows?.[0];
  const ap = aisAnalytics?.panels;
  const aisKpi = ap?.kpi?.rows?.[0];
  const filterSummary = [
    filters.flags.length ? `Flag: ${filters.flags.join(", ")}` : "",
    filters.vesselTypes.length ? `Typ: ${filters.vesselTypes.join(", ")}` : "",
    filters.shipNames.length ? `AIS: ${filters.shipNames.join(", ")}` : "",
    filters.germanUnits.length ? `Blue: ${filters.germanUnits.join(", ")}` : "",
    filters.russianUnits.length ? `Red: ${filters.russianUnits.join(", ")}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  const sp = sanctionedAnalytics?.panels;
  const sanctionKpi = sp?.kpi?.rows?.[0];
  const registryRows = sp?.registry?.rows ?? [];
  const registryCols = [
    "name",
    "mmsi",
    "flag",
    "vessel_type",
    "destination",
    "sanction_reason_short",
    "linked_to",
    "latest_ais_time",
  ];

  return (
    <div>
      <div className="analyses-tabs">
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            className={`nav-btn ${tab === t.id ? "active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "fleet" && (
        <div className="analyses-fleet">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            {BLUE_FLEET_LABEL}:{" "}
            <code>german_navy_fleet</code> ⋈ <code>marine_messages</code> (
            <code>message_from</code>) ⋈ <code>marine_vessel_status</code> (MMSI). Daten:{" "}
            {fleetAnalytics?.source ?? "…"}
            {fleetAnalytics?.fallbackReason ? ` — fallback (${fleetAnalytics.fallbackReason})` : ""}
            {fleetRegistry?.deduped ? " · Stammdaten dedupliziert (MMSI)" : ""}
          </p>
          <section className="panel" style={{ marginBottom: 12 }}>
            <h2 className="panel-title">Stammdaten — {BLUE_FLEET_STAMMDATEN}</h2>
            <p className="panel-subtitle">
              Eine Zeile pro MMSI (Versorgungsschiffe/Tender u. a.). Quelle: {fleetRegistry?.source ?? "…"}
            </p>
            <DataTable
              columns={["class", "name", "mmsi", "jahr_in_dienst", "besatzung"]}
              rows={fleetRegistry?.rows ?? []}
            />
          </section>
          <div className="analyses-grid">
            <section className="panel">
              <h2 className="panel-title">STANAG outbound (message_from)</h2>
              <p className="panel-subtitle">Surveillance reports gesendet pro Einheit</p>
              <ChartBars data={chartRows(p?.messagesFrom?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">STANAG Empfänger (message_to)</h2>
              <p className="panel-subtitle">Zieladresse aus maritime_surveillance_reports</p>
              <ChartBars data={chartRows(p?.messagesTo?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Operational status</h2>
              <p className="panel-subtitle">Letzter marine_vessel_status pro MMSI</p>
              <ChartPie data={chartRows(p?.operationalStatus?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">System status</h2>
              <p className="panel-subtitle">AIS / Überwachungs-Subsystem</p>
              <ChartPie data={chartRows(p?.systemStatus?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Destination (reported)</h2>
              <p className="panel-subtitle">AIS-Zielhafen laut Statusmeldung</p>
              <ChartBars data={chartRows(p?.destinations?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Nav status mix</h2>
              <p className="panel-subtitle">AIS Navigationsstatus (letzte Meldung)</p>
              <ChartPie
                data={aggregate(p?.unitStatus?.rows ?? [], "nav_status").map((x) => ({
                  name: x.name,
                  value: x.value,
                }))}
              />
            </section>
          </div>
          <div className="analyses-grid">
            <section className="panel" style={{ gridColumn: "1 / -1" }}>
              <h2 className="panel-title">Flottenlage — Einheit + Meldungen + Status</h2>
              <p className="panel-subtitle">
                Join Stammdaten, Summe Outbound-STANAG, aktueller Ops/System/Destination
              </p>
              <DataTable
                columns={p?.unitStatus?.columns ?? []}
                rows={p?.unitStatus?.rows ?? []}
              />
            </section>
            <section className="panel" style={{ gridColumn: "1 / -1" }}>
              <h2 className="panel-title">Letzte STANAG-Meldungen ({BLUE_FLEET_LABEL})</h2>
              <DataTable
                columns={p?.recentMessages?.columns ?? []}
                rows={p?.recentMessages?.rows ?? []}
              />
            </section>
          </div>
        </div>
      )}

      {tab === "buoys" && (
        <div className="analyses-buoys">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            View <code>defense.buoy_near_harbours</code> (Buoy ⋈ Hafen ≤ 25 km). Quelle:{" "}
            {buoyAnalytics?.source ?? "…"}
            {buoyAnalytics?.fallbackReason ? ` — fallback (${buoyAnalytics.fallbackReason})` : ""}
          </p>
          <div className="analyses-grid analyses-buoys-grid">
            <section className="panel buoy-table-panel">
              <h2 className="panel-title">Buoys — letzte Detektionen</h2>
              <p className="panel-subtitle">Aktuelle Zeilen aus buoy_near_harbours</p>
              <DataTable columns={bp?.recent?.columns ?? []} rows={bp?.recent?.rows ?? []} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Objekt-Typen</h2>
              <p className="panel-subtitle">payload_object_type — Record Count</p>
              <ChartPie data={chartRows(bp?.typeMix?.rows)} />
            </section>
            <section className="panel buoy-map-panel">
              <h2 className="panel-title">Detektionen — Karte</h2>
              <p className="panel-subtitle">GROUP BY geo_position_lat, geo_position_lon — sum(altitude)</p>
              {geoClusters.length === 0 ? (
                <p className="text-muted" style={{ fontSize: 12, margin: "0 0 8px" }} role="status">
                  Keine Karteneinträge — prüfen Sie <code>defense.buoy_near_harbours</code> (Geo-Cluster-Abfrage
                  leer oder Panel <code>geoClusters</code> fehlt in der API-Antwort).
                </p>
              ) : null}
              <BuoyAnalyticsMap clusters={geoClusters} />
            </section>
            <section className="panel buoy-scatter-panel">
              <h2 className="panel-title">Typ × Klassifikation</h2>
              <p className="panel-subtitle">avg(payload_object_confidence) · Bubble = record_count</p>
              <ChartClassificationGrid
                rows={
                  bp?.classificationGrid?.rows ??
                  (bp as { grid?: { rows?: Record<string, unknown>[] } })?.grid?.rows ??
                  []
                }
              />
            </section>
          </div>
        </div>
      )}

      {tab === "rus" && (
        <div className="analyses-rus">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            {RED_FLEET_LABEL}: <code>rus_fleet</code> ⋈ <code>rus_naval_fleet_positions</code> — letzte Lage, 24h-Bewegung (
            max SOG), Heimathafen-Distanz, <code>observation_areas</code> (ST_Within). Quelle:{" "}
            {rusFleetAnalytics?.source ?? "…"}
            {rusFleetAnalytics?.fallbackReason ? ` — fallback (${rusFleetAnalytics.fallbackReason})` : ""}
            {rusFleetRegistry?.deduped ? " · Stammdaten dedupliziert (MMSI)" : ""}
          </p>
          <section className="panel" style={{ marginBottom: 12 }}>
            <h2 className="panel-title">Stammdaten — {RED_FLEET_STAMMDATEN}</h2>
            <p className="panel-subtitle">
              Eine Zeile pro MMSI (Red Fleet). Quelle: {rusFleetRegistry?.source ?? "…"}
            </p>
            <DataTable
              columns={[
                "class",
                "name",
                "mmsi",
                "jahr_in_dienst",
                "besatzung",
                "home_base",
                "vessel_category",
                "default_mission_state",
              ]}
              rows={rusFleetRegistry?.rows ?? []}
            />
          </section>
          <div className="kpi-row">
            <div className="kpi">
              <div className="kpi-value">{rusKpis.registered}</div>
              <div className="kpi-label">Einheiten (live)</div>
            </div>
            <div className="kpi">
              <div className="kpi-value status-ok mono">{rusKpis.atHome}</div>
              <div className="kpi-label">Am Heimathafen (&lt;12 km)</div>
            </div>
            <div className="kpi">
              <div className="kpi-value mono">{rusKpis.transit}</div>
              <div className="kpi-label">Transit (24h max SOG ≥4 kn)</div>
            </div>
            <div className="kpi">
              <div className="kpi-value status-alert mono">{rusKpis.inRestricted}</div>
              <div className="kpi-label">In Sperrgebiet (jetzt)</div>
            </div>
          </div>
          <div className="analyses-grid">
            <section className="panel">
              <h2 className="panel-title">Aktivität (24h)</h2>
              <p className="panel-subtitle">Heimathafen / Anker / Patrouille / Transit</p>
              <ChartPie data={chartRows(rp?.activityMix?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Schiffstypen (Stammdaten)</h2>
              <p className="panel-subtitle">vessel_category in rus_fleet</p>
              <ChartBars data={chartRows(rp?.categoryMix?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Heimatbasen</h2>
              <p className="panel-subtitle">home_base — Registry</p>
              <ChartBars data={chartRows(rp?.homeBaseMix?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">In Sperrgebieten</h2>
              <p className="panel-subtitle">Tier-A observation_areas — Einheiten pro AOI</p>
              <ChartBars data={chartRows(rp?.restrictedByArea?.rows)} />
            </section>
          </div>
          <div className="analyses-grid">
            <section className="panel" style={{ gridColumn: "1 / -1" }}>
              <h2 className="panel-title">Einheitenlage — Bewegung & Hafen</h2>
              <p className="panel-subtitle">
                Letzte Position, Distanz Heimathafen, max/Ø SOG (24h), Activity-Klasse
              </p>
              <DataTable columns={rp?.unitDisposition?.columns ?? []} rows={rusUnits} />
            </section>
            <section className="panel" style={{ gridColumn: "1 / -1" }}>
              <h2 className="panel-title">Aktuell in Restricted Areas</h2>
              <DataTable columns={rp?.restrictedUnits?.columns ?? []} rows={rp?.restrictedUnits?.rows ?? []} />
            </section>
          </div>
        </div>
      )}

      {tab === "ais" && (
        <div className="analyses-ais">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            Letzte AIS-Position pro MMSI (<code>ais_events_ice</code> LEFT JOIN <code>ships</code>).
            Ohne Treffer in <code>ships</code> → Flagge „Unknown (kein ships-Eintrag)“ (z. B. NSB-Simulator
            ohne <code className="mono">ships_nsb_trade_1000.sql</code> in Trino). Quelle:{" "}
            {aisAnalytics?.source ?? "…"}
            {filterSummary ? ` · Filter: ${filterSummary}` : " · Kein Filter aktiv"}
            {aisAnalytics?.fallbackReason ? ` — fallback (${aisAnalytics.fallbackReason})` : ""}
          </p>
          <div className="kpi-row">
            <div className="kpi">
              <div className="kpi-value">{Number(aisKpi?.total_ships ?? 0)}</div>
              <div className="kpi-label">Schiffe (unique MMSI)</div>
            </div>
            <div className="kpi">
              <div className="kpi-value mono" style={{ fontSize: 22 }}>
                {Number(aisKpi?.avg_sog ?? 0).toFixed(1)}
              </div>
              <div className="kpi-label">Ø SOG (kn)</div>
            </div>
            <div className="kpi">
              <div className="kpi-value mono" style={{ fontSize: 22 }}>
                {Number(aisKpi?.underway_count ?? 0)}
              </div>
              <div className="kpi-label">Underway (SOG ≥ 0,5)</div>
            </div>
            <div className="kpi" title="MMSI ohne passenden Eintrag in defense.ships">
              <div className="kpi-value mono" style={{ fontSize: 22 }}>
                {Number(aisKpi?.ohne_ships_eintrag ?? 0)}
              </div>
              <div className="kpi-label">Ohne ships-Eintrag</div>
            </div>
          </div>
          <div className="analyses-grid analyses-ais-grid">
            <section className="panel">
              <h2 className="panel-title">Schiffe nach Flagge</h2>
              <p className="panel-subtitle">Anzahl Einheiten pro Flaggenstaat</p>
              <ChartBars data={chartRows(ap?.byFlag?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Schiffe nach Typ</h2>
              <p className="panel-subtitle">vessel_type aus ships</p>
              <ChartPie data={chartRows(ap?.byType?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Top Zielhäfen</h2>
              <p className="panel-subtitle">AIS Destination (letzte Meldung)</p>
              <ChartBars data={chartRows(ap?.topDestinations?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Navigationsstatus</h2>
              <p className="panel-subtitle">AIS status</p>
              <ChartPie data={chartRows(ap?.byStatus?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Geschwindigkeit</h2>
              <p className="panel-subtitle">SOG-Bänder (letzte Position)</p>
              <ChartBars data={chartRows(ap?.bySpeedBand?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Ø SOG nach Flagge</h2>
              <p className="panel-subtitle">Durchschnittliche SOG pro Flaggenstaat</p>
              <ChartBars data={chartRows(ap?.avgSogByFlag?.rows)} />
            </section>
          </div>
          <section className="panel" style={{ marginTop: 8 }}>
            <h2 className="panel-title">AIS Snapshot (gefiltert)</h2>
            <p className="panel-subtitle">Entspricht Filter Rail — max. 500 Zeilen</p>
            <DataTable columns={ais?.columns ?? []} rows={(ais?.rows ?? []).slice(0, 120)} />
          </section>
        </div>
      )}

      {tab === "outliers" && (
        <div className="analyses-outliers">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            Trino-Pipeline <code>vessel_outliers</code> (24h, Ostsee-BBox, MMSI &gt; 0) — gleiche Basis wie
            Lagebild-Layer „Outliers“ (severity ≥ 70 in KPI-Alerts). Job:{" "}
            <code>cdw-analyse/outlier_detection_trino.sql</code>. Quelle: {outlierAnalytics?.source ?? "…"}
            {outlierAnalytics?.fallbackReason
              ? ` — fallback (${outlierAnalytics.fallbackReason})`
              : ""}
          </p>
          <div className="kpi-row">
            <div className="kpi">
              <div className="kpi-value">{Number(outlierKpi?.total_events ?? 0)}</div>
              <div className="kpi-label">Ereignisse (24h)</div>
            </div>
            <div className="kpi">
              <div className="kpi-value">{Number(outlierKpi?.unique_mmsi ?? 0)}</div>
              <div className="kpi-label">Betroffene MMSI</div>
            </div>
            <div className="kpi">
              <div className="kpi-value status-alert mono">{Number(outlierKpi?.high_severity ?? 0)}</div>
              <div className="kpi-label">Severity ≥ 70</div>
            </div>
            <div className="kpi">
              <div className="kpi-value mono">{Number(outlierKpi?.distinct_types ?? 0)}</div>
              <div className="kpi-label">Outlier-Typen</div>
            </div>
          </div>
          <div className="analyses-grid analyses-ais-grid">
            <section className="panel">
              <h2 className="panel-title">Nach Outlier-Typ</h2>
              <p className="panel-subtitle">Rendezvous, Teleportation, Pipeline …</p>
              <ChartPie data={chartRows(op?.byType?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Severity</h2>
              <p className="panel-subtitle">Bänder 0–100</p>
              <ChartBars data={chartRows(op?.bySeverity?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Ereignisse pro Stunde</h2>
              <p className="panel-subtitle">Letzte 24h (Trino date_trunc)</p>
              <ChartBars data={hourlyBars(op?.hourly?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Top MMSI</h2>
              <p className="panel-subtitle">Häufigkeit & max. Severity</p>
              <ChartBars
                data={(op?.topMmsi?.rows ?? []).slice(0, 12).map((r) => ({
                  name: String(r.ship_name ?? r.mmsi ?? "—"),
                  value: Number(r.event_count ?? 0),
                }))}
              />
            </section>
          </div>
          <section className="panel" style={{ marginTop: 8 }}>
            <h2 className="panel-title">Letzte Outlier (24h)</h2>
            <p className="panel-subtitle">
              Spalte <code>reason</code> — voller Regeltext aus der Trino-Detection
            </p>
            <DataTable
              columns={[
                "mmsi",
                "ship_name",
                "outlier_type",
                "severity_level",
                "event_timestamp",
                "latitude",
                "longitude",
                "speed",
                "status",
                "reason",
              ]}
              rows={op?.recent?.rows ?? []}
            />
          </section>
          <section className="panel" style={{ marginTop: 8 }}>
            <h2 className="panel-title">Top-MMSI — Detail</h2>
            <DataTable columns={op?.topMmsi?.columns ?? []} rows={op?.topMmsi?.rows ?? []} />
          </section>
        </div>
      )}

      {tab === "sanctions" && (
        <div className="analyses-ais">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            EU / OFAC list ⋈ letzte AIS-Position pro MMSI, plus AIS-Zielhafen-Historie. Quelle:{" "}
            {sanctionedAnalytics?.source ?? "…"}
            {sanctionedAnalytics?.fallbackReason
              ? ` — fallback (${sanctionedAnalytics.fallbackReason})`
              : ""}
          </p>
          <div className="kpi-row">
            <div className="kpi">
              <div className="kpi-value">{Number(sanctionKpi?.total_listed ?? 0)}</div>
              <div className="kpi-label">Gelistet</div>
            </div>
            <div className="kpi">
              <div className="kpi-value">{Number(sanctionKpi?.with_mmsi ?? 0)}</div>
              <div className="kpi-label">Mit MMSI</div>
            </div>
            <div className="kpi">
              <div className="kpi-value">{Number(sanctionKpi?.with_ais ?? 0)}</div>
              <div className="kpi-label">Mit AIS</div>
            </div>
            <div className="kpi">
              <div className="kpi-value">{Number(sanctionKpi?.with_destination ?? 0)}</div>
              <div className="kpi-label">Mit Zielhafen</div>
            </div>
          </div>
          <div className="analyses-grid analyses-ais-grid">
            <section className="panel">
              <h2 className="panel-title">Zielhäfen (Historie)</h2>
              <p className="panel-subtitle">AIS-Destination-Meldungen aller sanktionierten MMSI</p>
              <ChartBars data={chartRows(sp?.destinationHistory?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Top-Routen</h2>
              <p className="panel-subtitle">Folge von Zielhäfen je MMSI (prev → next)</p>
              <ChartBars data={chartRows(sp?.topRoutes?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Sanktionsgrund (kurz)</h2>
              <p className="panel-subtitle">Volltext im Tooltip der Tabelle unten</p>
              <ChartPie data={chartRows(sp?.byReason?.rows)} />
            </section>
            <section className="panel">
              <h2 className="panel-title">Nach Flagge</h2>
              <ChartPie data={chartRows(sp?.byFlag?.rows)} />
            </section>
          </div>
          <section className="panel" style={{ marginTop: 8 }}>
            <h2 className="panel-title">Sanktionierte Schiffe</h2>
            <p className="panel-subtitle">
              Spalte Reason gekürzt — Mausover zeigt den vollständigen Sanktionstext
            </p>
            <DataTable
              columns={registryCols}
              rows={registryRows}
              hostileColumn="name"
              titleFromColumn={{ sanction_reason_short: "sanction_reason" }}
            />
          </section>
        </div>
      )}

      {tab === "restricted" && (
        <div className="analyses-ais">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            AIS innerhalb Tier-A <code>observation_areas</code> (ST_Within). Quelle:{" "}
            {restrictedAnalytics?.source ?? "…"}
            {restrictedAnalytics?.fallbackReason
              ? ` — fallback (${restrictedAnalytics.fallbackReason})`
              : ""}
          </p>
          {(() => {
            const rp = restrictedAnalytics?.panels;
            const rk = rp?.kpi?.rows?.[0];
            const tracks = rowsToRestrictedTracks(rp?.routeTracks?.rows ?? []);
            return (
              <>
                <div className="kpi-row">
                  <div className="kpi">
                    <div className="kpi-value">{Number(rk?.unique_vessels ?? 0)}</div>
                    <div className="kpi-label">Schiffe (unique MMSI)</div>
                  </div>
                  <div className="kpi">
                    <div className="kpi-value">{Number(rk?.zones_with_traffic ?? 0)}</div>
                    <div className="kpi-label">Sperrgebiete mit Traffic</div>
                  </div>
                  <div className="kpi">
                    <div className="kpi-value">{Number(rk?.position_reports ?? 0)}</div>
                    <div className="kpi-label">AIS-Punkte in AOIs</div>
                  </div>
                  <div className="kpi">
                    <div className="kpi-value">{Number(rk?.vessel_zone_visits ?? 0)}</div>
                    <div className="kpi-label">Schiff×Zone Paare</div>
                  </div>
                </div>
                <div className="analyses-grid analyses-ais-grid">
                  <section className="panel">
                    <h2 className="panel-title">Schiffe pro Zone</h2>
                    <p className="panel-subtitle">Unique MMSI je area_id</p>
                    <ChartBars data={chartRows(rp?.byZone?.rows)} />
                  </section>
                  <section className="panel">
                    <h2 className="panel-title">Flaggen in Sperrgebieten</h2>
                    <ChartPie data={chartRows(rp?.byFlag?.rows)} />
                  </section>
                  <section className="panel">
                    <h2 className="panel-title">Schiffstypen</h2>
                    <ChartPie data={chartRows(rp?.topByType?.rows)} />
                  </section>
                  <section className="panel">
                    <h2 className="panel-title">Provokateure</h2>
                    <p className="panel-subtitle">Meiste Zonen + Berichte in AOIs</p>
                    <ChartBars data={provocateurBars(rp?.provocateurs?.rows)} />
                  </section>
                </div>
                <section className="panel restricted-map-panel" style={{ marginTop: 8 }}>
                  <h2 className="panel-title">Routen in Sperrgebieten</h2>
                  <p className="panel-subtitle">
                    Top 8 MMSI nach Zonentreffer — Polylines innerhalb Tier-A AOIs
                  </p>
                  <RestrictedZoneMap areas={observationAreas} tracks={tracks} />
                </section>
                <section className="panel" style={{ marginTop: 8 }}>
                  <h2 className="panel-title">Top-Schiffe (Name, Typ, Flagge)</h2>
                  <DataTable
                    columns={[
                      "ship_name",
                      "mmsi",
                      "vessel_type",
                      "flag",
                      "zones_hit",
                      "reports_inside",
                    ]}
                    rows={rp?.topVessels?.rows ?? []}
                    hostileColumn="ship_name"
                  />
                </section>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}

function aggregate(rows: Record<string, unknown>[], key: string) {
  const map = new Map<string, number>();
  for (const r of rows) {
    const k = String(r[key] ?? "Unknown");
    map.set(k, (map.get(k) ?? 0) + 1);
  }
  return [...map.entries()].map(([name, value]) => ({ name, value }));
}

function ChartBars({ data }: { data: { name: string; value: number }[] }) {
  if (!data.length) {
    return <p className="panel-subtitle">Keine Daten</p>;
  }
  return (
    <div style={{ height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <XAxis dataKey="name" tick={{ fill: "#8B9CAB", fontSize: 11 }} interval={0} angle={-25} textAnchor="end" height={60} />
          <YAxis tick={{ fill: "#8B9CAB", fontSize: 11 }} />
          <Tooltip {...CHART_TOOLTIP_PROPS} />
          <Bar dataKey="value" fill="#4DA3FF" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function ChartPie({ data }: { data: { name: string; value: number }[] }) {
  if (!data.length) {
    return <p className="panel-subtitle">Keine Daten</p>;
  }
  return (
    <div style={{ height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={data} dataKey="value" nameKey="name" innerRadius={50} outerRadius={90}>
            {data.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip {...CHART_TOOLTIP_PROPS} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

function ChartClassificationGrid({ rows }: { rows: Record<string, unknown>[] }) {
  const { points, typeLabels, classLabels } = buildClassificationScatter(rows);
  if (!points.length) {
    return <p className="panel-subtitle">Keine Daten</p>;
  }
  return (
    <div style={{ height: 320 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 8, right: 12, bottom: 48, left: 8 }}>
          <CartesianGrid stroke="#2a3540" strokeDasharray="3 3" />
          <XAxis
            type="number"
            dataKey="x"
            name="Typ"
            domain={[-0.5, typeLabels.length - 0.5]}
            ticks={typeLabels.map((_, i) => i)}
            tickFormatter={(i) => typeLabels[i] ?? ""}
            tick={{ fill: "#8B9CAB", fontSize: 9 }}
            interval={0}
            angle={-30}
            textAnchor="end"
            height={72}
          />
          <YAxis
            type="number"
            dataKey="y"
            name="Klassifikation"
            domain={[-0.5, classLabels.length - 0.5]}
            ticks={classLabels.map((_, i) => i)}
            tickFormatter={(i) => {
              const label = classLabels[i] ?? "";
              return label.length > 18 ? `${label.slice(0, 16)}…` : label;
            }}
            tick={{ fill: "#8B9CAB", fontSize: 9 }}
            width={120}
          />
          <ZAxis type="number" dataKey="z" range={[40, 520]} name="Records" />
          <Tooltip
            {...CHART_TOOLTIP_PROPS}
            cursor={{ strokeDasharray: "3 3" }}
            formatter={(value: number, name: string) => {
              if (name === "avg_confidence") return [`${value.toFixed(1)}%`, "Ø confidence"];
              if (name === "z") return [value, "Record count"];
              return [value, name];
            }}
            labelFormatter={(_, payload) => {
              const p = payload?.[0]?.payload as ScatterPoint | undefined;
              if (!p) return "";
              return `${p.payload_object_type} / ${p.payload_object_classification}`;
            }}
          />
          <Scatter data={points}>
            {points.map((pt, i) => (
              <Cell key={i} fill={confidenceColor(pt.avg_confidence)} fillOpacity={0.85} />
            ))}
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}