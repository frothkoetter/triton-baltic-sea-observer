import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import { DataTable } from "../components/DataTable";
import { LidarMap } from "../components/LidarMap";
import { LidarPointPreview } from "../components/LidarPointPreview";
import type { LidarScan } from "../lib/lidar";
import { scanKey } from "../lib/lidar";

export function LidarPage() {
  const [scans, setScans] = useState<LidarScan[]>([]);
  const [source, setSource] = useState("…");
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [filterType, setFilterType] = useState("");

  useEffect(() => {
    void (async () => {
      const res = await fetchJson<{ rows?: LidarScan[]; source?: string; fallbackReason?: string }>(
        "/api/lidar/scans"
      );
      const rows = res.rows ?? [];
      setScans(rows);
      setSource(res.source ?? "unknown");
      if (rows[0]) setSelectedKey(scanKey(rows[0]));
    })();
  }, []);

  const types = useMemo(() => {
    return [...new Set(scans.map((s) => String(s.vessel_type ?? "")).filter(Boolean))].sort();
  }, [scans]);

  const filtered = useMemo(() => {
    if (!filterType) return scans;
    return scans.filter((s) => String(s.vessel_type) === filterType);
  }, [scans, filterType]);

  const selected = scans.find((s) => scanKey(s) === selectedKey) ?? null;

  return (
    <div className="lidar-page">
      <div className="lidar-toolbar">
        <span className="mono" style={{ fontSize: 11, color: "var(--text-muted)" }}>
          lidar_simulation_metadata · Data: {source}
        </span>
        <label className="lidar-filter">
          Vessel type
          <select value={filterType} onChange={(e) => setFilterType(e.target.value)}>
            <option value="">All</option>
            {types.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="lidar-grid">
        <section className="panel lidar-map-panel">
          <h2 className="panel-title">LiDAR scene centers (24h catalog)</h2>
          <LidarMap
            scans={filtered}
            selectedKey={selectedKey}
            onSelect={(s) => setSelectedKey(scanKey(s))}
          />
        </section>
        <section className="panel lidar-detail-panel">
          <h2 className="panel-title">Scene detail</h2>
          <LidarPointPreview scan={selected} />
        </section>
      </div>

      <section className="panel" style={{ marginTop: 8 }}>
        <h2 className="panel-title">Scans ({filtered.length})</h2>
        <DataTable
          columns={[
            "filename",
            "vessel_name",
            "vessel_type",
            "classification",
            "center_lat",
            "center_lon",
            "point_count",
            "ingested_at",
          ]}
          rows={filtered as unknown as Record<string, unknown>[]}
          onRowClick={(row) => {
            const fn = String(row.filename ?? "");
            const match = scans.find((s) => s.filename === fn);
            if (match) setSelectedKey(scanKey(match));
          }}
        />
      </section>
    </div>
  );
}