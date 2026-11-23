/** Geohash encode/decode (standard base32, matches NiFi GpsJammerSimulator). */

const BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";
const BITS = [16, 8, 4, 2, 1] as const;

export type GeohashBounds = {
  south: number;
  north: number;
  west: number;
  east: number;
};

export function geohashBounds(geohash: string): GeohashBounds | null {
  const gh = geohash.trim().toLowerCase();
  if (!gh) return null;
  let even = true;
  let latMin = -90;
  let latMax = 90;
  let lonMin = -180;
  let lonMax = 180;
  for (const ch of gh) {
    const cd = BASE32.indexOf(ch);
    if (cd < 0) return null;
    for (let mask = 16; mask > 0; mask >>= 1) {
      if (even) {
        const mid = (lonMin + lonMax) / 2;
        if (cd & mask) lonMin = mid;
        else lonMax = mid;
      } else {
        const mid = (latMin + latMax) / 2;
        if (cd & mask) latMin = mid;
        else latMax = mid;
      }
      even = !even;
    }
  }
  return { south: latMin, north: latMax, west: lonMin, east: lonMax };
}

/** Leaflet bounds: [[south, west], [north, east]] */
export function geohashLeafletBounds(
  geohash: string
): [[number, number], [number, number]] | null {
  const b = geohashBounds(geohash);
  if (!b) return null;
  return [
    [b.south, b.west],
    [b.north, b.east],
  ];
}

export function geohashCenter(bounds: GeohashBounds): { lat: number; lon: number } {
  return {
    lat: (bounds.south + bounds.north) / 2,
    lon: (bounds.west + bounds.east) / 2,
  };
}

/** True when decoded tile center is near the event point (rejects quadkeys / wrong id). */
export function geohashMatchesPoint(geohash: string, lat: number, lon: number, maxDeg = 0.35): boolean {
  const b = geohashBounds(geohash);
  if (!b) return false;
  const c = geohashCenter(b);
  return Math.abs(c.lat - lat) <= maxDeg && Math.abs(c.lon - lon) <= maxDeg;
}

export function encodeGeohash(lat: number, lon: number, precision = 6): string {
  let latMin = -90;
  let latMax = 90;
  let lonMin = -180;
  let lonMax = 180;
  let geohash = "";
  let even = true;
  let bit = 0;
  let ch = 0;
  while (geohash.length < precision) {
    if (even) {
      const mid = (lonMin + lonMax) / 2;
      if (lon > mid) {
        ch |= BITS[bit];
        lonMin = mid;
      } else {
        lonMax = mid;
      }
    } else {
      const mid = (latMin + latMax) / 2;
      if (lat > mid) {
        ch |= BITS[bit];
        latMin = mid;
      } else {
        latMax = mid;
      }
    }
    even = !even;
    if (bit < 4) {
      bit += 1;
    } else {
      geohash += BASE32[ch];
      bit = 0;
      ch = 0;
    }
  }
  return geohash;
}

export function isValidGeohashString(raw: unknown): raw is string {
  if (raw == null) return false;
  const gh = String(raw).trim().toLowerCase();
  return gh.length >= 4 && geohashBounds(gh) != null;
}