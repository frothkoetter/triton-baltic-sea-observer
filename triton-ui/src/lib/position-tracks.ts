export type TrackKind = "ais" | "deu_fleet" | "rus_fleet";

export type PositionTrack = {
  id: string;
  mmsi: string;
  kind: TrackKind;
  label: string;
  points: [number, number][];
  pointCount: number;
};

export function rowsToTracks(rows: Record<string, unknown>[]): PositionTrack[] {
  const buckets = new Map<string, PositionTrack & { _seen: Set<string> }>();

  for (const row of rows) {
    const lat = Number(row.latitude ?? row.Latitude);
    const lon = Number(row.longitude ?? row.Longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const mmsi = String(row.mmsi ?? row.MMSI ?? "");
    const kind = String(row.track_kind ?? "ais") as TrackKind;
    const key = `${kind}:${mmsi}`;
    let track = buckets.get(key);
    if (!track) {
      const label = String(
        row.ship_name ?? row.unit_name ?? row.Ship_Name ?? (mmsi || "unknown")
      );
      track = {
        id: key,
        mmsi,
        kind,
        label,
        points: [],
        pointCount: 0,
        _seen: new Set(),
      };
      buckets.set(key, track);
    }
    const dedupe = `${lat.toFixed(5)},${lon.toFixed(5)}`;
    if (track._seen.has(dedupe)) continue;
    track._seen.add(dedupe);
    track.points.push([lat, lon]);
    track.pointCount += 1;
  }

  return [...buckets.values()]
    .filter((t) => t.points.length >= 1)
    .map(({ _seen: _, ...rest }) => rest);
}

export type PositionTracksQuery = {
  hours?: number;
  flag?: string;
  vesselType?: string;
  shipName?: string;
  ais?: boolean;
  german?: boolean;
  russian?: boolean;
};

export function buildTracksQueryString(q: PositionTracksQuery): string {
  const params = new URLSearchParams();
  params.set("hours", String(q.hours ?? 24));
  if (q.flag) params.set("flag", q.flag);
  if (q.vesselType) params.set("vessel_type", q.vesselType);
  if (q.shipName) params.set("ship_name", q.shipName);
  if (q.ais === false) params.set("ais", "0");
  if (q.german === false) params.set("german", "0");
  if (q.russian === false) params.set("russian", "0");
  return params.toString();
}