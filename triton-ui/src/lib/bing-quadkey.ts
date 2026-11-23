/** Bing Maps tile quadkey helpers (zoom 11 ~ Ostsee threat / AIS baseline grid). */

export type LatLonBounds = {
  north: number;
  south: number;
  east: number;
  west: number;
};

const MAP_SIZE = 256;

export function latLonToTileXY(lat: number, lon: number, level: number): { x: number; y: number } {
  const latRad = (lat * Math.PI) / 180;
  const n = 2 ** level;
  const x = Math.floor(((lon + 180) / 360) * n);
  const y = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * n
  );
  return { x, y };
}

export function tileXYToQuadkey(tileX: number, tileY: number, level: number): string {
  let key = "";
  for (let i = level; i > 0; i--) {
    let digit = 0;
    const mask = 1 << (i - 1);
    if ((tileX & mask) !== 0) digit += 1;
    if ((tileY & mask) !== 0) digit += 2;
    key += String(digit);
  }
  return key;
}

export function latLonToQuadkey(lat: number, lon: number, level = 11): string {
  const { x, y } = latLonToTileXY(lat, lon, level);
  return tileXYToQuadkey(x, y, level);
}

/** Decode quadkey to tile indices (Microsoft Bing Maps algorithm). */
export function quadkeyToTileXY(quadkey: string): { x: number; y: number; level: number } {
  let tileX = 0;
  let tileY = 0;
  const level = quadkey.length;
  for (let i = level; i > 0; i--) {
    const mask = 1 << (i - 1);
    const ch = quadkey.charAt(level - i);
    if (ch === "1" || ch === "3") tileX |= mask;
    if (ch === "2" || ch === "3") tileY |= mask;
  }
  return { x: tileX, y: tileY, level };
}

function tileXToLon(x: number, level: number): number {
  const n = 2 ** level;
  return (x / n) * 360 - 180;
}

function tileYToLat(y: number, level: number): number {
  const n = 2 ** level;
  const yMerc = Math.PI * (1 - (2 * y) / n);
  return (180 / Math.PI) * Math.atan(Math.sinh(yMerc));
}

export function quadkeyBounds(quadkey: string): LatLonBounds {
  const { x, y, level } = quadkeyToTileXY(quadkey);
  return {
    west: tileXToLon(x, level),
    east: tileXToLon(x + 1, level),
    north: tileYToLat(y, level),
    south: tileYToLat(y + 1, level),
  };
}

export function quadkeyCenter(quadkey: string): { lat: number; lon: number } {
  const b = quadkeyBounds(quadkey);
  return { lat: (b.north + b.south) / 2, lon: (b.east + b.west) / 2 };
}

/** Leaflet bounds: [[south, west], [north, east]] */
export function quadkeyLeafletBounds(quadkey: string): [[number, number], [number, number]] {
  const b = quadkeyBounds(quadkey);
  return [
    [b.south, b.west],
    [b.north, b.east],
  ];
}

export const THREAT_TILE_ZOOM = 11;
export const MAP_SIZE_PX = MAP_SIZE;