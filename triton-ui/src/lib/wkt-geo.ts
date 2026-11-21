/** OGC WKT (lon lat) → Leaflet [lat, lon] tuples. */

export type LatLng = [number, number];

function parseCoordPairs(body: string): LatLng[] {
  return body
    .split(",")
    .map((pair) => {
      const parts = pair.trim().split(/\s+/);
      if (parts.length < 2) return null;
      const lon = Number(parts[0]);
      const lat = Number(parts[1]);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
      return [lat, lon] as LatLng;
    })
    .filter((p): p is LatLng => p != null);
}

/** LINESTRING / MULTILINESTRING → one or more polylines. */
export function wktToLineStrings(wkt: string | null | undefined): LatLng[][] {
  if (!wkt?.trim()) return [];
  const s = wkt.trim();
  const lineMatch = /^LINESTRING\s*\((.+)\)\s*$/i.exec(s);
  if (lineMatch) {
    const pts = parseCoordPairs(lineMatch[1]);
    return pts.length >= 2 ? [pts] : [];
  }
  if (/^MULTILINESTRING/i.test(s)) {
    const lines: LatLng[][] = [];
    const re = /\(\s*([^()]+?)\s*\)/g;
    let m: RegExpExecArray | null;
    const body = s.replace(/^MULTILINESTRING\s*/i, "");
    while ((m = re.exec(body)) !== null) {
      const pts = parseCoordPairs(m[1]);
      if (pts.length >= 2) lines.push(pts);
    }
    return lines;
  }
  return [];
}

/** POLYGON outer ring (and optional holes) → rings for Leaflet Polygon. */
export function wktToPolygonRings(wkt: string | null | undefined): LatLng[][] {
  if (!wkt?.trim()) return [];
  const s = wkt.trim();
  const polyMatch = /^POLYGON\s*\(\s*(.+)\s*\)\s*$/is.exec(s);
  if (!polyMatch) return [];
  const rings: LatLng[][] = [];
  const re = /\(\s*([^()]+?)\s*\)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(polyMatch[1])) !== null) {
    const pts = parseCoordPairs(m[1]);
    if (pts.length >= 3) rings.push(pts);
  }
  return rings;
}