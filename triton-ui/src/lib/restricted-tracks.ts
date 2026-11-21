import type { PositionTrack } from "./position-tracks";

/** One polyline per MMSI × restricted zone (AIS points inside AOI). */
export function rowsToRestrictedTracks(rows: Record<string, unknown>[]): PositionTrack[] {
  const buckets = new Map<string, PositionTrack & { _seen: Set<string> }>();

  for (const row of rows) {
    const lat = Number(row.latitude ?? row.Latitude);
    const lon = Number(row.longitude ?? row.Longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const mmsi = String(row.mmsi ?? row.MMSI ?? "");
    const areaId = String(row.area_id ?? "zone");
    const key = `ais:${mmsi}:${areaId}`;
    let track = buckets.get(key);
    if (!track) {
      const ship = String(row.ship_name ?? mmsi ?? "unknown");
      track = {
        id: key,
        mmsi,
        kind: "ais",
        label: `${ship} · ${areaId}`,
        points: [],
        trackPoints: [],
        pointCount: 0,
        startTimeMs: 0,
        endTimeMs: 0,
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
    .map(({ _seen: _, ...rest }) => {
      const trackPoints = rest.points.map((p, i) => ({
        lat: p[0],
        lon: p[1],
        timeMs: i,
      }));
      return {
        ...rest,
        trackPoints,
        startTimeMs: 0,
        endTimeMs: Math.max(0, trackPoints.length - 1),
      };
    });
}