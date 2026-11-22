import { useEffect, useMemo, useState } from "react";
import { fetchJson } from "../api";
import { DataTable } from "../components/DataTable";
import { DasCableDetail } from "../components/DasCableDetail";
import { DasCableMap } from "../components/DasCableMap";
import type { DasCableSegment } from "../lib/das-cable";
import {
  DAS_THREAT_CATALOG,
  segmentKey,
  segmentMatchesCableFilter,
} from "../lib/das-cable";
import {
  normalizeOstseeInfra,
  normalizeOstseePipeline,
  ostseeCableFilterOptions,
  type OstseeInfraRow,
  type OstseePipelineRow,
} from "../lib/ostsee-infra-map";

function segmentThreatLabel(seg: DasCableSegment): string {
  return String(seg.predicted_threat_type ?? seg.threat_class ?? "").trim();
}

export function DasCablePage() {
  const [segments, setSegments] = useState<DasCableSegment[]>([]);
  const [source, setSource] = useState("…");
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [filterCable, setFilterCable] = useState("");
  const [filterThreat, setFilterThreat] = useState("");
  const [ostseeInfra, setOstseeInfra] = useState<OstseeInfraRow[]>([]);
  const [ostseePipelines, setOstseePipelines] = useState<OstseePipelineRow[]>([]);

  useEffect(() => {
    void (async () => {
      const [segRes, infraRes] = await Promise.all([
        fetchJson<{ rows?: DasCableSegment[]; source?: string }>("/api/das-cable/segments"),
        fetchJson<{
          infrastruktur?: Record<string, unknown>[];
          pipelines?: Record<string, unknown>[];
        }>("/api/ostsee-infrastructure"),
      ]);
      const rows = segRes.rows ?? [];
      setSegments(rows);
      setSource(segRes.source ?? "unknown");
      if (rows[0]) setSelectedKey(segmentKey(rows[0]));
      setOstseeInfra(
        (infraRes.infrastruktur ?? [])
          .map((r) => normalizeOstseeInfra(r))
          .filter((r): r is OstseeInfraRow => r != null)
      );
      setOstseePipelines(
        (infraRes.pipelines ?? [])
          .map((r) => normalizeOstseePipeline(r))
          .filter((r): r is OstseePipelineRow => r != null)
      );
    })();
  }, []);

  const cables = useMemo(() => {
    const fromSegments = segments.map((s) => String(s.cable_asset ?? s.cable_name ?? "")).filter(Boolean);
    return ostseeCableFilterOptions(ostseeInfra, ostseePipelines, fromSegments);
  }, [segments, ostseeInfra, ostseePipelines]);

  const threats = useMemo(() => {
    const fromData = segments.map((s) => segmentThreatLabel(s)).filter(Boolean);
    return [...new Set([...DAS_THREAT_CATALOG, ...fromData])].sort();
  }, [segments]);

  const filtered = useMemo(() => {
    return segments.filter((s) => {
      if (!segmentMatchesCableFilter(s, filterCable)) return false;
      if (filterThreat) {
        const t = segmentThreatLabel(s);
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
            infrastruktur={ostseeInfra}
            pipelines={ostseePipelines}
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