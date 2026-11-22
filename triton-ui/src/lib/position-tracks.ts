export type TrackKind = "ais" | "deu_fleet" | "rus_fleet";

export type TrackPoint = {
  lat: number;
  lon: number;
  timeMs: number;
};

export type PositionTrack = {
  id: string;
  mmsi: string;
  kind: TrackKind;
  label: string;
  points: [number, number][];
  trackPoints: TrackPoint[];
  pointCount: number;
  startTimeMs: number;
  endTimeMs: number;
};

function parseRowTimeMs(row: Record<string, unknown>): number {
  const raw = row.event_timestamp ?? row.Event_Timestamp;
  if (raw == null || raw === "") return NaN;
  const t = Date.parse(String(raw));
  return Number.isFinite(t) ? t : NaN;
}

function finalizeTrack(
  track: Omit<PositionTrack, "points" | "trackPoints" | "startTimeMs" | "endTimeMs"> & {
    trackPoints: TrackPoint[];
  }
): PositionTrack {
  const sorted = [...track.trackPoints].sort((a, b) => a.timeMs - b.timeMs);
  const startTimeMs = sorted.length ? sorted[0].timeMs : 0;
  const endTimeMs = sorted.length ? sorted[sorted.length - 1].timeMs : 0;
  return {
    ...track,
    trackPoints: sorted,
    points: sorted.map((p) => [p.lat, p.lon] as [number, number]),
    pointCount: sorted.length,
    startTimeMs,
    endTimeMs,
  };
}

export function rowsToTracks(rows: Record<string, unknown>[]): PositionTrack[] {
  const buckets = new Map<
    string,
    Omit<PositionTrack, "points" | "trackPoints" | "startTimeMs" | "endTimeMs"> & {
      trackPoints: TrackPoint[];
      _seq: number;
    }
  >();

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
        pointCount: 0,
        trackPoints: [],
        _seq: 0,
      };
      buckets.set(key, track);
    }
    let timeMs = parseRowTimeMs(row);
    if (!Number.isFinite(timeMs)) {
      timeMs = track._seq;
      track._seq += 1;
    }
    track.trackPoints.push({ lat, lon, timeMs });
  }

  return [...buckets.values()]
    .filter((t) => t.trackPoints.length >= 1)
    .map(({ _seq: _, ...rest }) => finalizeTrack(rest));
}

import type { LagebildFilters } from "./lagebild-filters";
import { serializeFilterQueryParams } from "./lagebild-filters";

export type PositionTracksQuery = LagebildFilters & {
  hours?: number;
  ais?: boolean;
  german?: boolean;
  russian?: boolean;
};

export function buildTracksQueryString(q: PositionTracksQuery): string {
  const params = serializeFilterQueryParams(q);
  params.set("hours", String(q.hours ?? 24));
  if (q.ais === false) params.set("ais", "0");
  if (q.german === false) params.set("german", "0");
  if (q.russian === false) params.set("russian", "0");
  return params.toString();
}