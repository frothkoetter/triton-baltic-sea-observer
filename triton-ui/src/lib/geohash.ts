/** Geohash → lat/lon bounding box (standard base32). */

const BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

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