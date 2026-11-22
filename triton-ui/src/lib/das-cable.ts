export type DasCableSegment = {
  hadoop_file_url?: string;
  segment_index?: number;
  cable_asset?: string;
  cable_name?: string;
  chainage_m?: number;
  segment_lat: number;
  segment_lon: number;
  threat_class?: string;
  predicted_threat_type?: string | null;
  model_confidence?: number | null;
  channel_count?: number;
  window_seconds?: number;
  fs_hz?: number;
  filename?: string;
  ingested_at?: string;
};

/** Client-side S3 URI for HDF5 preview (matches server das-segment.js). */
export function resolveDasHdfsUrl(seg: DasCableSegment | null | undefined): string | null {
  if (!seg) return null;
  const hadoop = String(seg.hadoop_file_url ?? "").trim();
  if (/^s3a?:\/\//i.test(hadoop)) return hadoop;
  const filename = String(seg.filename ?? "").trim();
  if (/^s3a?:\/\//i.test(filename)) return filename;
  return hadoop || null;
}

export function segmentKey(seg: DasCableSegment): string {
  return `${seg.hadoop_file_url ?? seg.filename ?? "?" }#${seg.segment_index ?? 0}`;
}

/** NiFi DasCableAcousticSimulator ground-truth catalog (das_acoustic_engine.THREAT_CLASSES). */
export const DAS_THREAT_CATALOG = [
  "ambient",
  "vessel_passage",
  "trawler_scrape",
  "anchor_drag",
  "anchor_slip",
  "anchor_drop_tow",
  "cui_sabotage_tap",
  "cable_strain_anomaly",
  "uboat_acoustic",
  "whale_vocalization",
  "hard_beep",
] as const;

export function segmentMatchesCableFilter(seg: DasCableSegment, filter: string): boolean {
  if (!filter) return true;
  const f = filter.trim().toLowerCase();
  const asset = String(seg.cable_asset ?? "").trim().toLowerCase();
  const name = String(seg.cable_name ?? "").trim().toLowerCase();
  const normFilter = f.replace(/\s+/g, "_");
  const normAsset = asset.replace(/\s+/g, "_");
  return (
    asset === f ||
    name === f ||
    normAsset === normFilter ||
    name.includes(f) ||
    asset.includes(normFilter) ||
    name.replace(/\s+/g, " ").includes(filter.trim())
  );
}

const THREAT_COLORS: Record<string, string> = {
  ambient: "#78909c",
  vessel_passage: "#42a5f5",
  trawler_scrape: "#ffb74d",
  anchor_drag: "#ff7043",
  anchor_slip: "#f4511e",
  anchor_drop_tow: "#c62828",
  cui_sabotage_tap: "#e53935",
  cable_strain_anomaly: "#ab47bc",
  uboat_acoustic: "#5e35b1",
  whale_vocalization: "#26a69a",
  hard_beep: "#fdd835",
};

export function dasThreatColor(threat: string | null | undefined): string {
  const t = String(threat ?? "ambient").toLowerCase();
  return THREAT_COLORS[t] ?? "#26c6da";
}