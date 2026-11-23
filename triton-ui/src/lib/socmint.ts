import { latLonToQuadkey, THREAT_TILE_ZOOM } from "./bing-quadkey";

export type SocmintTrustTag =
  | "UNVERIFIED_SOCMINT"
  | "SENSOR_CORROBORATED"
  | "DISINFORMATION_CONFIRMED"
  | "LOW_CONFIDENCE"
  | string;

export type SocmintMessage = {
  user_name: string;
  user_username: string;
  tweet: string;
  ts: string;
  priority?: string;
  latitude: number;
  longitude: number;
  platform?: string;
  intel_category?: string;
  trust_tag?: SocmintTrustTag;
  disinformation_flag?: boolean;
  verification_notes?: string;
  scenario_id?: string;
  correlation_hint?: string;
  quadkey?: string;
};

export type ThreatHotTile = {
  quadkey: string;
  event_count: number;
  sev_p50: number;
  sev_p90: number;
  updated_at?: string;
};

export type SocmintDensityTile = {
  quadkey: string;
  post_count: number;
  disinfo_count: number;
  updated_at?: string;
};

export function trustTagColor(tag: SocmintTrustTag | undefined, disinfo?: boolean): string {
  if (disinfo || tag === "DISINFORMATION_CONFIRMED") return "#E53935";
  if (tag === "SENSOR_CORROBORATED") return "#3DDC84";
  if (tag === "LOW_CONFIDENCE") return "#8B9CAB";
  return "#FFD54F";
}

/** Map hover / detail: preserve NiFi newlines, cap length for tooltips. */
export function socmintTweetDisplay(text: string, maxLen?: number): string {
  const t = text.replace(/\r\n/g, "\n").trim();
  if (maxLen == null || t.length <= maxLen) return t;
  return `${t.slice(0, maxLen)}…`;
}

export function threatTileColor(sevP90: number): string {
  if (sevP90 >= 85) return "#FF5252";
  if (sevP90 >= 65) return "#FF9800";
  if (sevP90 >= 50) return "#FFB74D";
  return "#5C6BC0";
}

export function normalizeSocmintRow(row: Record<string, unknown>): SocmintMessage | null {
  const lat = Number(row.latitude ?? row.Latitude);
  const lon = Number(row.longitude ?? row.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const disRaw = row.disinformation_flag ?? row.Disinformation_Flag;
  const disinfo =
    disRaw === true ||
    disRaw === 1 ||
    String(disRaw).toLowerCase() === "true";
  const trust = String(row.trust_tag ?? row.Trust_Tag ?? "UNVERIFIED_SOCMINT");
  const quadkey =
    row.quadkey != null
      ? String(row.quadkey)
      : latLonToQuadkey(lat, lon, THREAT_TILE_ZOOM);
  return {
    user_name: String(row.user_name ?? row.User_Name ?? ""),
    user_username: String(row.user_username ?? row.User_Username ?? ""),
    tweet: String(row.tweet ?? row.Tweet ?? ""),
    ts: String(row.ts ?? row.TS ?? ""),
    priority: row.priority != null ? String(row.priority) : undefined,
    latitude: lat,
    longitude: lon,
    platform: row.platform != null ? String(row.platform) : undefined,
    intel_category: row.intel_category != null ? String(row.intel_category) : undefined,
    trust_tag: trust as SocmintTrustTag,
    disinformation_flag: disinfo,
    verification_notes:
      row.verification_notes != null ? String(row.verification_notes) : undefined,
    scenario_id: row.scenario_id != null ? String(row.scenario_id) : undefined,
    correlation_hint: row.correlation_hint != null ? String(row.correlation_hint) : undefined,
    quadkey,
  };
}

export function normalizeThreatTileRow(row: Record<string, unknown>): ThreatHotTile | null {
  const qk = String(row.quadkey ?? row.Quadkey ?? "").trim();
  if (!qk) return null;
  return {
    quadkey: qk,
    event_count: Number(row.event_count ?? row.Event_Count ?? 0),
    sev_p50: Number(row.sev_p50 ?? row.Sev_P50 ?? 0),
    sev_p90: Number(row.sev_p90 ?? row.Sev_P90 ?? 0),
    updated_at: row.updated_at != null ? String(row.updated_at) : undefined,
  };
}

export function normalizeSocmintDensityRow(row: Record<string, unknown>): SocmintDensityTile | null {
  const qk = String(row.quadkey ?? row.Quadkey ?? "").trim();
  if (!qk) return null;
  return {
    quadkey: qk,
    post_count: Number(row.post_count ?? row.Post_Count ?? 0),
    disinfo_count: Number(row.disinfo_count ?? row.Disinfo_Count ?? 0),
    updated_at: row.updated_at != null ? String(row.updated_at) : undefined,
  };
}