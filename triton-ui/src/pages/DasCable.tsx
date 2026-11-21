import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import { DataTable } from "../components/DataTable";
import { DasCableDetail } from "../components/DasCableDetail";
import { DasCableMap } from "../components/DasCableMap";
import type { DasCableSegment } from "../lib/das-cable";
import { segmentKey } from "../lib/das-cable";

export function DasCablePage() {
  const [segments, setSegments] = useState<DasCableSegment[]>([]);
  const [source, setSource] = useState("…");
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [filterCable, setFilterCable] = useState("");
  const [filterThreat, setFilterThreat] = useState("");

  useEffect(() => {
    void (async () => {
      const res = await fetchJson<{ rows?: DasCableSegment[]; source?: string }>("/api/das-cable/segments");
      const rows = res.rows ?? [];
      setSegments(rows);
      setSource(res.source ?? "unknown");
      if (rows[0]) setSelectedKey(segmentKey(rows[0]));
    })();
  }, []);

  const cables = useMemo(() => {
    return [...new Set(segments.map((s) => String(s.cable_asset ?? "")).filter(Boolean))].sort();
  }, [segments]);

  const threats = useMemo(() => {
    return [
      ...new Set(
        segments.map((s) => String(s.predicted_threat_type ?? s.threat_class ?? "")).filter(Boolean)
      ),
    ].sort();
  }, [segments]);

  const filtered = useMemo(() => {
    return segments.filter((s) => {
      if (filterCable && s.cable_asset !== filterCable) return false;
      if (filterThreat) {
        const t = s.predicted_threat_type ?? s.threat_class;
        if (t !== filterThreat) return false;
      }
      return true;
    });
  }, [segments, filterCable, filterThreat]);

  const selected = segments.find((s) => segmentKey(s) === selectedKey) ?? null;

  return (
    <div className="lidar-page">
      <div className="lidar-toolbar">
        <span className="mono" style={{ fontSize: 11, color: "var(--text-muted)" }}>
          das_cable_spectrogram_metadata · Data: {source}
        </span>
        <label className="lidar-filter">
          Cable
          <select value={filterCable} onChange={(e) => setFilterCable(e.target.value)}>
            <option value="">All</option>
            {cables.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </label>
        <label className="lidar-filter">
          Threat
          <select value={filterThreat} onChange={(e) => setFilterThreat(e.target.value)}>
            <option value="">All</option>
            {threats.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="lidar-grid">
        <section className="panel lidar-map-panel">
          <h2 className="panel-title">DAS segments along cables (catalog)</h2>
          <DasCableMap
            segments={filtered}
            selectedKey={selectedKey}
            onSelect={(s) => setSelectedKey(segmentKey(s))}
          />
        </section>
        <section className="panel lidar-detail-panel">
          <h2 className="panel-title">Segment detail</h2>
          <DasCableDetail segment={selected} />
        </section>
      </div>

      <section className="panel" style={{ marginTop: 8 }}>
        <h2 className="panel-title">Segments ({filtered.length})</h2>
        <DataTable
          columns={[
            "filename",
            "segment_index",
            "cable_asset",
            "threat_class",
            "predicted_threat_type",
            "chainage_m",
            "segment_lat",
            "segment_lon",
            "ingested_at",
          ]}
          rows={filtered as unknown as Record<string, unknown>[]}
          onRowClick={(row) => {
            const idx = Number(row.segment_index ?? 0);
            const fn = String(row.filename ?? row.hadoop_file_url ?? "");
            const match = segments.find(
              (s) => (s.filename === fn || s.hadoop_file_url === fn) && s.segment_index === idx
            );
            if (match) setSelectedKey(segmentKey(match));
          }}
        />
      </section>
    </div>
  );
}