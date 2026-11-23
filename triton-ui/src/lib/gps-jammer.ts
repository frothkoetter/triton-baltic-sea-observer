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

export function normalizeGpsJammerRow(row: Record<string, unknown>): GpsJammerEvent | null {
  const lat = Number(row.latitude ?? row.Latitude);
  const lon = Number(row.longitude ?? row.Longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const nicRaw = row.adsb_nic ?? row.ADSB_NIC;
  const sigRaw = row.signal_integrity ?? row.Signal_Integrity;
  const jamRaw = row.jamming_indicator ?? row.Jamming_Indicator;
  return {
    geohash: String(row.geohash ?? row.Geohash ?? row.id ?? ""),
    latitude: lat,
    longitude: lon,
    event_timestamp: row.event_timestamp
      ? String(row.event_timestamp)
      : row.timestamp
        ? String(row.timestamp)
        : undefined,
    adsb_nic: nicRaw == null || nicRaw === "" ? null : Number(nicRaw),
    signal_integrity: sigRaw == null || sigRaw === "" ? null : Number(sigRaw),
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
  if (signalIntegrity == null || !Number.isFinite(signalIntegrity)) return 0.35;
  return Math.min(0.65, Math.max(0.12, 0.75 - signalIntegrity * 0.55));
}