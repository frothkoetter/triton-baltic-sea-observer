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