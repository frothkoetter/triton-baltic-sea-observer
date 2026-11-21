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

export function segmentKey(seg: DasCableSegment): string {
  return `${seg.hadoop_file_url ?? seg.filename ?? "?" }#${seg.segment_index ?? 0}`;
}

const THREAT_COLORS: Record<string, string> = {
  ambient: "#78909c",
  vessel_passage: "#42a5f5",
  trawler_scrape: "#ffb74d",
  anchor_drag: "#ff7043",
  cui_sabotage_tap: "#e53935",
  cable_strain_anomaly: "#ab47bc",
};

export function dasThreatColor(threat: string | null | undefined): string {
  const t = String(threat ?? "ambient").toLowerCase();
  return THREAT_COLORS[t] ?? "#26c6da";
}