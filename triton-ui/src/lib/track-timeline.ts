import type { PositionTrack, TrackPoint } from "./position-tracks";

export function formatTrackTime(ms: number, locale = "de-DE"): string {
  if (!Number.isFinite(ms)) return "—";
  return new Date(ms).toLocaleString(locale, {
    dateStyle: "short",
    timeStyle: "medium",
  });
}

export function trackTimeRange(tracks: PositionTrack[]): { min: number; max: number } | null {
  let min = Infinity;
  let max = -Infinity;
  for (const t of tracks) {
    if (t.trackPoints.length === 0) continue;
    min = Math.min(min, t.startTimeMs);
    max = Math.max(max, t.endTimeMs);
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) return null;
  return { min, max };
}

/** Polyline vertices from track start up to (and including) interpolated position at `timeMs`. */
export function trackPathUpToTime(
  track: PositionTrack,
  timeMs: number
): { path: [number, number][]; head: [number, number] | null } {
  const pts = track.trackPoints;
  if (pts.length === 0) return { path: [], head: null };
  if (timeMs < pts[0].timeMs) {
    return { path: [], head: [pts[0].lat, pts[0].lon] };
  }

  const path: [number, number][] = [];
  for (let i = 0; i < pts.length; i += 1) {
    const p = pts[i];
    if (p.timeMs > timeMs) break;
    path.push([p.lat, p.lon]);
  }

  const head = interpolateAtTime(pts, timeMs);
  if (head) {
    const last = path[path.length - 1];
    if (!last || last[0] !== head[0] || last[1] !== head[1]) {
      path.push(head);
    }
  }

  return { path, head: head ?? (path.length ? path[path.length - 1] : null) };
}

function interpolateAtTime(pts: TrackPoint[], timeMs: number): [number, number] | null {
  if (pts.length === 0) return null;
  const last = pts[pts.length - 1];
  if (timeMs >= last.timeMs) return [last.lat, last.lon];

  let i = 0;
  while (i < pts.length - 1 && pts[i + 1].timeMs <= timeMs) i += 1;
  const a = pts[i];
  const b = pts[i + 1];
  if (!b) return [a.lat, a.lon];
  if (b.timeMs <= a.timeMs) return [b.lat, b.lon];

  const frac = (timeMs - a.timeMs) / (b.timeMs - a.timeMs);
  const lat = a.lat + (b.lat - a.lat) * frac;
  const lon = a.lon + (b.lon - a.lon) * frac;
  return [lat, lon];
}