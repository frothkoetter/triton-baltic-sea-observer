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
import { DataTable } from "../components/DataTable";
import {
  buildClassificationScatter,
  confidenceColor,
  type ScatterPoint,
} from "../lib/buoy-analytics";

const TABS = [
  { id: "fleet", label: "Fleet" },
  { id: "buoys", label: "Buoys" },
  { id: "rus", label: "RUS Fleet" },
  { id: "ais", label: "AIS" },
  { id: "sanctions", label: "Sanctions" },
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

type AnalysenPageProps = LagebildFilters;

export function AnalysenPage({ flag, vesselType, shipName }: AnalysenPageProps) {
  const [tab, setTab] = useState<TabId>("fleet");
  const [fleetAnalytics, setFleetAnalytics] = useState<FleetAnalyticsPayload | null>(null);
  const [ais, setAis] = useState<QueryResult | null>(null);
  const [aisAnalytics, setAisAnalytics] = useState<AisAnalyticsPayload | null>(null);
  const [sanctions, setSanctions] = useState<QueryResult | null>(null);
  const [violations, setViolations] = useState<QueryResult | null>(null);
  const [buoyAnalytics, setBuoyAnalytics] = useState<BuoyAnalyticsPayload | null>(null);
  const [rusFleetAnalytics, setRusFleetAnalytics] = useState<RusFleetAnalyticsPayload | null>(null);

  const filterQs = aisAnalyticsQuery({ flag, vesselType, shipName });

  useEffect(() => {
    void (async () => {
      const [fa, bu, ru, s, v] = await Promise.all([
        fetchJson<FleetAnalyticsPayload>("/api/analyses/fleet-analytics"),
        fetchJson<BuoyAnalyticsPayload>("/api/analyses/buoy-analytics"),
        fetchJson<RusFleetAnalyticsPayload>("/api/analyses/rus-fleet-analytics"),
        fetchJson<QueryResult>("/api/analyses/sanctions"),
        fetchJson<QueryResult>("/api/analyses/violations"),
      ]);
      setFleetAnalytics(fa);
      setBuoyAnalytics(bu);
      setRusFleetAnalytics(ru);
      setSanctions(s);
      setViolations(v);
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
  const geoClusters = (bp?.geoClusters?.rows ?? []).map((r) => ({
    geo_position_lat: Number(r.geo_position_lat),
    geo_position_lon: Number(r.geo_position_lon),
    record_count: Number(r.record_count ?? 0),
    sum_altitude: Number(r.sum_altitude ?? 0),
  }));
  const rp = rusFleetAnalytics?.panels;
  const rusUnits = rp?.unitDisposition?.rows ?? [];
  const rusKpis = {
    registered: rusUnits.length,
    atHome: rusUnits.filter((u) => u.at_home_port === true || u.at_home_port === "true").length,
    inRestricted: rp?.restrictedUnits?.rows?.length ?? 0,
    transit: rusUnits.filter((u) => String(u.activity) === "Transit").length,
  };
  const ap = aisAnalytics?.panels;
  const aisKpi = ap?.kpi?.rows?.[0];
  const filterSummary = [flag && `Flag: ${flag}`, vesselType && `Typ: ${vesselType}`, shipName && `Schiff: ${shipName}`]
    .filter(Boolean)
    .join(" · ");
  const sanctionsByFlag = aggregate(sanctions?.rows ?? [], "Flag");

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
            DE Flotte:{" "}
            <code>german_navy_fleet</code> ⋈ <code>marine_messages</code> (
            <code>message_from</code>) ⋈ <code>marine_vessel_status</code> (MMSI). Daten:{" "}
            {fleetAnalytics?.source ?? "…"}
            {fleetAnalytics?.fallbackReason ? ` — fallback (${fleetAnalytics.fallbackReason})` : ""}
          </p>
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
              <h2 className="panel-title">Letzte STANAG-Meldungen (DE-Einheiten)</h2>
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
              <BuoyAnalyticsMap clusters={geoClusters} />
            </section>
            <section className="panel buoy-scatter-panel">
              <h2 className="panel-title">Typ × Klassifikation</h2>
              <p className="panel-subtitle">avg(payload_object_confidence) · Bubble = record_count</p>
              <ChartClassificationGrid rows={bp?.classificationGrid?.rows ?? []} />
            </section>
          </div>
        </div>
      )}

      {tab === "rus" && (
        <div className="analyses-rus">
          <p className="kpi-source-line mono" style={{ marginTop: 0 }}>
            <code>rus_fleet</code> ⋈ <code>rus_naval_fleet_positions</code> — letzte Lage, 24h-Bewegung (
            max SOG), Heimathafen-Distanz, <code>observation_areas</code> (ST_Within). Quelle:{" "}
            {rusFleetAnalytics?.source ?? "…"}
            {rusFleetAnalytics?.fallbackReason ? ` — fallback (${rusFleetAnalytics.fallbackReason})` : ""}
          </p>
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
            Letzte AIS-Position pro MMSI (<code>ais_events_ice</code> ⋈ <code>ships</code>). Quelle:{" "}
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

      {tab === "sanctions" && (
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">Sanctioned vessels</h2>
            <DataTable columns={sanctions?.columns ?? []} rows={sanctions?.rows ?? []} hostileColumn="Name" />
          </section>
          <section className="panel">
            <h2 className="panel-title">By flag</h2>
            <ChartPie data={sanctionsByFlag} />
          </section>
        </div>
      )}

      {tab === "restricted" && (
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">Area violations</h2>
            <DataTable columns={violations?.columns ?? []} rows={violations?.rows ?? []} />
          </section>
          <section className="panel">
            <h2 className="panel-title">Violations by area</h2>
            <ChartBars data={aggregate(violations?.rows ?? [], "area_id")} />
          </section>
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