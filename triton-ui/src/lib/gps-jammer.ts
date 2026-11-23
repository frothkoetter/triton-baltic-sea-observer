import {
  encodeGeohash,
  geohashLeafletBounds,
  geohashMatchesPoint,
  isValidGeohashString,
} from "./geohash";

export type GpsJammingFilter = "all" | "jamming" | "monitor";

export type GpsJammerEvent = {
  geohash: string;
  latitude: number;
  longitude: number;
  event_timestamp?: string;
  adsb_nic?: number | null;
  signal_integrity?: number | null;
  jamming_indicator?: boolean;
  event_type?: string;
  reason?: string;
  harbour_name?: string;
};

function normalizeSignalIntegrity(raw: unknown): number | null {
  if (raw == null || raw === "") return null;
  let n = Number(raw);
  if (!Number.isFinite(n)) return null;
  if (n > 1 && n <= 100) n /= 100;
  return n;
}

function resolveEventGeohash(row: Record<string, unknown>, lat: number, lon: number): string {
  const candidates = [row.geohash, row.Geohash, row.id, row.ID];
  for (const raw of candidates) {
    if (!isValidGeohashString(raw)) continue;
    const gh = String(raw).trim().toLowerCase();
    if (geohashMatchesPoint(gh, lat, lon)) return gh;
  }
  return encodeGeohash(lat, lon, 6);
}

export function normalizeGpsJammerRow(row: Record<string, unknown>): GpsJammerEvent | null {
  const lat = Number(row.latitude ?? row.Latitude);
  const lon = Number(row.longitude ?? row.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const nicRaw = row.adsb_nic ?? row.ADSB_NIC;
  const sigRaw = row.signal_integrity ?? row.Signal_Integrity;
  const jamRaw = row.jamming_indicator ?? row.Jamming_Indicator;
  return {
    geohash: resolveEventGeohash(row, lat, lon),
    latitude: lat,
    longitude: lon,
    event_timestamp: row.event_timestamp
      ? String(row.event_timestamp)
      : row.timestamp
        ? String(row.timestamp)
        : undefined,
    adsb_nic: nicRaw == null || nicRaw === "" ? null : Number(nicRaw),
    signal_integrity: normalizeSignalIntegrity(sigRaw),
    jamming_indicator:
      jamRaw === true || jamRaw === 1 || String(jamRaw).toLowerCase() === "true"
        ? true
        : jamRaw === false || jamRaw === 0 || String(jamRaw).toLowerCase() === "false"
          ? false
          : undefined,
    event_type: String(row.event_type ?? "gps_jammer_event"),
    reason: row.reason != null ? String(row.reason) : undefined,
    harbour_name: row.harbour_name != null ? String(row.harbour_name) : undefined,
  };
}

/** Filter by jamming flag and max signal_integrity (inclusive; missing integrity passes). */
export function filterGpsJammerEvents(
  events: GpsJammerEvent[],
  opts: { jamming: GpsJammingFilter; maxSignalIntegrity: number }
): GpsJammerEvent[] {
  const cap = Math.min(1, Math.max(0, opts.maxSignalIntegrity));
  return events.filter((e) => {
    if (opts.jamming === "jamming" && e.jamming_indicator !== true) return false;
    if (opts.jamming === "monitor" && e.jamming_indicator === true) return false;
    const sig = e.signal_integrity;
    if (sig != null && Number.isFinite(sig) && sig > cap) return false;
    return true;
  });
}

/** One map feature per geohash — keep newest event_timestamp. */
export function collapseGpsJammerByGeohash(events: GpsJammerEvent[]): GpsJammerEvent[] {
  const byHash = new Map<string, GpsJammerEvent>();
  for (const e of events) {
    const key = e.geohash.trim().toLowerCase() || `pt:${e.latitude.toFixed(4)},${e.longitude.toFixed(4)}`;
    const prev = byHash.get(key);
    if (!prev) {
      byHash.set(key, e);
      continue;
    }
    const tsA = prev.event_timestamp ?? "";
    const tsB = e.event_timestamp ?? "";
    if (tsB.localeCompare(tsA) > 0) byHash.set(key, e);
  }
  return [...byHash.values()];
}

export function gpsJammerTileFillOpacity(signalIntegrity: number | null | undefined): number {
  if (signalIntegrity == null || !Number.isFinite(signalIntegrity)) return 0.42;
  return Math.min(0.72, Math.max(0.28, 0.82 - signalIntegrity * 0.5));
}

/** Geohash-6 cells are ~1 km; at zoom 6 they are sub-pixel — expand for display. */
export function minGpsJammerSpanDegrees(zoom: number): number {
  if (zoom >= 12) return 0.006;
  if (zoom >= 10) return 0.018;
  if (zoom >= 8) return 0.055;
  if (zoom >= 7) return 0.12;
  return 0.3;
}

export function expandLeafletBounds(
  bounds: [[number, number], [number, number]],
  minSpanDeg: number
): [[number, number], [number, number]] {
  const [[south, west], [north, east]] = bounds;
  const latMid = (south + north) / 2;
  const lonMid = (west + east) / 2;
  const halfLat = Math.max((north - south) / 2, minSpanDeg / 2);
  const halfLon = Math.max((east - west) / 2, minSpanDeg / 2);
  return [
    [latMid - halfLat, lonMid - halfLon],
    [latMid + halfLat, lonMid + halfLon],
  ];
}

/** Leaflet rectangle bounds for a jammer event (always tries lat/lon-backed geohash). */
export function gpsJammerLeafletBounds(
  event: GpsJammerEvent
): [[number, number], [number, number]] | null {
  const fromHash = geohashLeafletBounds(event.geohash);
  if (fromHash && geohashMatchesPoint(event.geohash, event.latitude, event.longitude)) {
    return fromHash;
  }
  const encoded = encodeGeohash(event.latitude, event.longitude, 6);
  return geohashLeafletBounds(encoded);
}

export function gpsJammerVisualLeafletBounds(
  event: GpsJammerEvent,
  zoom: number
): [[number, number], [number, number]] | null {
  const core = gpsJammerLeafletBounds(event);
  if (!core) return null;
  return expandLeafletBounds(core, minGpsJammerSpanDegrees(zoom));
}

export function countGpsJammerDrawableTiles(events: GpsJammerEvent[], zoom = 6): number {
  return events.reduce((n, e) => (gpsJammerVisualLeafletBounds(e, zoom) ? n + 1 : n), 0);
}