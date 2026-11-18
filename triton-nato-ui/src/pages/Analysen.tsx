import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchJson, type QueryResult } from "../api";
import { DataTable } from "../components/DataTable";

const TABS = [
  { id: "fleet", label: "Fleet" },
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

export function AnalysenPage() {
  const [tab, setTab] = useState<TabId>("fleet");
  const [fleetAnalytics, setFleetAnalytics] = useState<FleetAnalyticsPayload | null>(null);
  const [ais, setAis] = useState<QueryResult | null>(null);
  const [sanctions, setSanctions] = useState<QueryResult | null>(null);
  const [violations, setViolations] = useState<QueryResult | null>(null);

  useEffect(() => {
    void (async () => {
      const [fa, a, s, v] = await Promise.all([
        fetchJson<FleetAnalyticsPayload>("/api/analyses/fleet-analytics"),
        fetchJson<QueryResult>("/api/ais"),
        fetchJson<QueryResult>("/api/analyses/sanctions"),
        fetchJson<QueryResult>("/api/analyses/violations"),
      ]);
      setFleetAnalytics(fa);
      setAis(a);
      setSanctions(s);
      setViolations(v);
    })();
  }, []);

  const p = fleetAnalytics?.panels;
  const aisByFlag = aggregate(ais?.rows ?? [], "flag");
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

      {tab === "ais" && (
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">Ships by flag</h2>
            <ChartBars data={aisByFlag} />
          </section>
          <section className="panel">
            <h2 className="panel-title">AIS positions (sample)</h2>
            <DataTable columns={ais?.columns ?? []} rows={(ais?.rows ?? []).slice(0, 80)} />
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