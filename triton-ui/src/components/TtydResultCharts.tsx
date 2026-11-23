import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const CHART_COLORS = ["#4DA3FF", "#8B9CAB", "#3DDC84", "#FFB020", "#E53935", "#FFD54F", "#B388FF", "#80CBC4"];

const CHART_TOOLTIP_PROPS = {
  contentStyle: { background: "#0d1824", border: "1px solid #2a4054", borderRadius: 6 },
  itemStyle: { color: "#e8f0f6" },
  labelStyle: { color: "#8B9CAB" },
};

export type ChartSeries = {
  nameKey: string;
  valueKey: string;
  data: { name: string; value: number }[];
  reason: string;
};

function toNumber(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

const VALUE_HINTS =
  /^(n|count|cnt|num|number|total|sum|value|amount|qty|quantity|events|records|rows|distinct_mmsi|mit_ships|ohne_ships)$/i;
const LABEL_HINTS =
  /^(label|name|type|class|source|reason|category|threat|data_source|trust_tag|outlier_type|threat_class|sanction_reason|transition|area_id|mmsi|geohash)$/i;

function scoreColumn(name: string, kind: "value" | "label"): number {
  const lower = name.toLowerCase();
  if (kind === "value") {
    if (VALUE_HINTS.test(lower)) return 10;
    if (/(count|total|sum|num|qty|_n$)/i.test(lower)) return 6;
    return 0;
  }
  if (LABEL_HINTS.test(lower)) return 10;
  if (/(name|label|type|class|reason|source|tag)/i.test(lower)) return 5;
  return 0;
}

/** Turn Trino rows into chartable {name, value}[] when a category + metric column exist. */
export function inferChartSeries(
  rows: Record<string, unknown>[],
  columns?: string[]
): ChartSeries | null {
  if (!rows?.length) return null;
  const cols = columns?.length ? columns : Object.keys(rows[0] ?? {});
  if (cols.length < 2) return null;

  let valueKey = "";
  let nameKey = "";
  let bestValue = -1;
  let bestName = -1;

  for (const c of cols) {
    const sample = rows.find((r) => toNumber(r[c]) != null);
    const numeric = Boolean(sample);
    const vs = scoreColumn(c, "value");
    const ls = scoreColumn(c, "label");
    if (numeric && vs >= bestValue) {
      bestValue = vs || (numeric ? 1 : 0);
      valueKey = c;
    }
    if (ls >= bestName) {
      bestName = ls;
      nameKey = c;
    }
  }

  if (!valueKey) {
    for (const c of cols) {
      if (rows.some((r) => toNumber(r[c]) != null)) {
        valueKey = c;
        break;
      }
    }
  }
  if (!nameKey || nameKey === valueKey) {
    for (const c of cols) {
      if (c !== valueKey) {
        nameKey = c;
        break;
      }
    }
  }
  if (!valueKey || !nameKey || nameKey === valueKey) return null;

  const numericShare = rows.filter((r) => toNumber(r[valueKey]) != null).length / rows.length;
  if (numericShare < 0.5) return null;

  const data: { name: string; value: number }[] = [];
  for (const row of rows) {
    const v = toNumber(row[valueKey]);
    if (v == null) continue;
    const name = String(row[nameKey] ?? "—").slice(0, 48);
    data.push({ name, value: v });
  }
  if (data.length < 1) return null;

  const geoCols = cols.filter((c) => /lat|lon|longitude|latitude|timestamp|event_time/i.test(c));
  if (cols.length >= 6 && geoCols.length >= 2 && data.length > 3) {
    return null;
  }

  return {
    nameKey,
    valueKey,
    data: data.slice(0, 24),
    reason: `${nameKey} × ${valueKey}`,
  };
}

type Props = {
  rows: Record<string, unknown>[];
  columns?: string[];
};

export function TtydResultCharts({ rows, columns }: Props) {
  const series = useMemo(() => inferChartSeries(rows, columns), [rows, columns]);
  const [mode, setMode] = useState<"pie" | "bar">("pie");

  if (!series || series.data.length < 1) return null;

  const preferPie = series.data.length <= 8 && series.data.every((d) => d.value >= 0);
  const chartMode = mode === "pie" && preferPie ? "pie" : "bar";

  return (
    <div className="dsc-charts">
      <div className="dsc-charts-toolbar">
        <span className="text-muted" style={{ fontSize: 11 }}>
          Visualisierung ({series.reason})
        </span>
        <span className="dsc-chart-toggle">
          <button
            type="button"
            className={chartMode === "pie" ? "dsc-chart-btn active" : "dsc-chart-btn"}
            disabled={!preferPie}
            onClick={() => setMode("pie")}
          >
            Kreis
          </button>
          <button
            type="button"
            className={chartMode === "bar" ? "dsc-chart-btn active" : "dsc-chart-btn"}
            onClick={() => setMode("bar")}
          >
            Balken
          </button>
        </span>
      </div>
      <div className="dsc-chart-panel">
        {chartMode === "pie" ? (
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie
                data={series.data}
                dataKey="value"
                nameKey="name"
                innerRadius={48}
                outerRadius={88}
                paddingAngle={1}
              >
                {series.data.map((_, i) => (
                  <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip {...CHART_TOOLTIP_PROPS} />
              <Legend
                wrapperStyle={{ fontSize: 10, color: "#8B9CAB" }}
                layout="vertical"
                align="right"
                verticalAlign="middle"
              />
            </PieChart>
          </ResponsiveContainer>
        ) : (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={series.data} margin={{ bottom: 8, left: 4, right: 8 }}>
              <XAxis
                dataKey="name"
                tick={{ fill: "#8B9CAB", fontSize: 10 }}
                interval={0}
                angle={series.data.length > 5 ? -28 : 0}
                textAnchor={series.data.length > 5 ? "end" : "middle"}
                height={series.data.length > 5 ? 72 : 36}
              />
              <YAxis tick={{ fill: "#8B9CAB", fontSize: 10 }} width={48} />
              <Tooltip {...CHART_TOOLTIP_PROPS} />
              <Bar dataKey="value" name={series.valueKey} fill="#4DA3FF" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}