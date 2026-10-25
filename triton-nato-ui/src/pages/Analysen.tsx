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

export function AnalysenPage() {
  const [tab, setTab] = useState<TabId>("fleet");
  const [fleet, setFleet] = useState<QueryResult | null>(null);
  const [ais, setAis] = useState<QueryResult | null>(null);
  const [sanctions, setSanctions] = useState<QueryResult | null>(null);
  const [violations, setViolations] = useState<QueryResult | null>(null);

  useEffect(() => {
    void (async () => {
      const [f, a, s, v] = await Promise.all([
        fetchJson<QueryResult>("/api/analyses/fleet"),
        fetchJson<QueryResult>("/api/ais"),
        fetchJson<QueryResult>("/api/analyses/sanctions"),
        fetchJson<QueryResult>("/api/analyses/violations"),
      ]);
      setFleet(f);
      setAis(a);
      setSanctions(s);
      setViolations(v);
    })();
  }, []);

  const fleetByClass = aggregate(fleet?.rows ?? [], "class");
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
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">FlottenStatus table (128)</h2>
            <DataTable columns={fleet?.columns ?? []} rows={fleet?.rows ?? []} />
          </section>
          <section className="panel">
            <h2 className="panel-title">Units by class (117)</h2>
            <ChartBars data={fleetByClass} />
          </section>
        </div>
      )}

      {tab === "ais" && (
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">ShipsAIS trellis proxy (104)</h2>
            <ChartBars data={aisByFlag} />
          </section>
          <section className="panel">
            <h2 className="panel-title">AIS Positions (102)</h2>
            <DataTable columns={ais?.columns ?? []} rows={(ais?.rows ?? []).slice(0, 80)} />
          </section>
        </div>
      )}

      {tab === "sanctions" && (
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">SanctionedVessels (136)</h2>
            <DataTable columns={sanctions?.columns ?? []} rows={sanctions?.rows ?? []} hostileColumn="Name" />
          </section>
          <section className="panel">
            <h2 className="panel-title">By flag (97)</h2>
            <ChartPie data={sanctionsByFlag} />
          </section>
        </div>
      )}

      {tab === "restricted" && (
        <div className="analyses-grid">
          <section className="panel">
            <h2 className="panel-title">AreaViolations (96)</h2>
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
  return (
    <div style={{ height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <XAxis dataKey="name" tick={{ fill: "#8B9CAB", fontSize: 11 }} interval={0} angle={-25} textAnchor="end" height={60} />
          <YAxis tick={{ fill: "#8B9CAB", fontSize: 11 }} />
          <Tooltip contentStyle={{ background: "#1C2630", border: "1px solid #2A3540" }} />
          <Bar dataKey="value" fill="#4DA3FF" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function ChartPie({ data }: { data: { name: string; value: number }[] }) {
  return (
    <div style={{ height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={data} dataKey="value" nameKey="name" innerRadius={50} outerRadius={90}>
            {data.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip contentStyle={{ background: "#1C2630", border: "1px solid #2A3540" }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
