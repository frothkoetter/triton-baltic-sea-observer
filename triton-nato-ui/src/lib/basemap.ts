/** Mapbox dark basemap (Leaflet tile URL template). */
export function mapboxDarkTileUrl(accessToken: string): string {
  const t = encodeURIComponent(accessToken.trim());
  return `https://api.mapbox.com/styles/v1/mapbox/dark-v11/tiles/256/{z}/{x}/{y}@2x?access_token=${t}`;
}

export const MAPBOX_ATTRIBUTION =
  '&copy; <a href="https://www.mapbox.com/">Mapbox</a>';