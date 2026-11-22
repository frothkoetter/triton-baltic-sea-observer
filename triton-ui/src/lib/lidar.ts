export type LidarScan = {
  filename: string;
  vessel_name?: string | null;
  vessel_type?: string | null;
  classification?: number | null;
  center_lat: number;
  center_lon: number;
  heading_deg?: number | null;
  point_count?: number | null;
  hadoop_file_url?: string | null;
  predicted_vessel_type?: string | null;
  model_confidence?: number | null;
  ingested_at?: string | null;
};

export function lidarMarkerColor(vesselType: string | null | undefined, classification?: number | null): string {
  if (classification === 0) return "#ffd54f";
  const t = String(vesselType ?? "").toLowerCase();
  if (t.includes("submarine")) return "#4da3ff";
  if (t.includes("corvette") || t.includes("frigate") || t.includes("landing")) return "#a371f7";
  if (t.includes("tanker") || t.includes("container") || t.includes("ferry")) return "#f0883e";
  if (t.includes("sailing") || t.includes("fishing")) return "#3fb950";
  return "#79c0ff";
}

export function classificationLabel(code: number | null | undefined): string {
  if (code == null) return "—";
  if (code === 0) return "0 — unclassified";
  if (code === 10) return "10 — water surface";
  return String(code);
}

export type PseudoPoint = { x: number; y: number; z: number; cls: number };

/** Deterministic pseudo point cloud for UI preview (not LAS decode). */
export function pseudoLidarPoints(
  seed: string,
  vesselType: string,
  pointBudget = 1800
): PseudoPoint[] {
  let h = 2166136261;
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  const rand = () => {
    h ^= h << 13;
    h ^= h >>> 17;
    h ^= h << 5;
    return (h >>> 0) / 4294967296;
  };

  const hullLen =
    vesselType.includes("submarine") ? 18 : vesselType.includes("frigate") ? 42 : vesselType.includes("tanker") ? 55 : 32;
  const hullBeam = vesselType.includes("submarine") ? 6 : 12;
  const out: PseudoPoint[] = [];

  for (let i = 0; i < pointBudget; i++) {
    const along = (rand() - 0.5) * hullLen;
    const across = (rand() - 0.5) * hullBeam;
    const jitterX = (rand() - 0.5) * 2;
    const jitterY = (rand() - 0.5) * 2;
    const jitterZ = (rand() - 0.5) * 4;
    const clsRoll = rand();
    let cls = 5;
    if (clsRoll > 0.92) cls = 10;
    else if (clsRoll > 0.85) cls = 2;
    else if (vesselType === "undefined" || vesselType === "") cls = 0;
    out.push({
      x: along + jitterX,
      y: across + jitterY,
      z: jitterZ + (rand() - 0.3) * 3,
      cls,
    });
  }
  return out;
}

export function scanKey(scan: LidarScan): string {
  return scan.filename || `${scan.center_lat},${scan.center_lon}`;
}

/** Accept API point arrays; tolerate legacy tuple-shaped JSON from old bridge. */
export function normalizeViewerPoints(raw: unknown): PseudoPoint[] {
  if (!Array.isArray(raw) || raw.length === 0) return [];
  const first = raw[0];
  if (typeof first === "number") return [];
  if (Array.isArray(first)) {
    return normalizeViewerPoints(first);
  }
  const out: PseudoPoint[] = [];
  for (const row of raw) {
    if (!row || typeof row !== "object") continue;
    const p = row as Record<string, unknown>;
    if (typeof p.x !== "number" || typeof p.y !== "number" || typeof p.z !== "number") continue;
    const cls = typeof p.cls === "number" ? p.cls : 0;
    out.push({ x: p.x, y: p.y, z: p.z, cls });
  }
  return out;
}