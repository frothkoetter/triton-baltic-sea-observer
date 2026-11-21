import { useEffect, useState } from "react";
import { TileLayer } from "react-leaflet";
import { MAPBOX_ATTRIBUTION, mapboxDarkTileUrl } from "../lib/basemap";

function buildTimeToken(): string {
  return String(import.meta.env.VITE_MAPBOX_TOKEN ?? "").trim();
}

/** CARTO basemaps now require an API key; prefer Mapbox via app env or Vite build. */
export function BasemapTileLayer() {
  const [token, setToken] = useState(buildTimeToken);

  useEffect(() => {
    if (token) return;
    let cancelled = false;
    fetch("/api/map-config")
      .then((r) => r.json())
      .then((body: { mapboxToken?: string }) => {
        if (cancelled) return;
        const fromApi = String(body.mapboxToken ?? "").trim();
        if (fromApi) setToken(fromApi);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [token]);

  if (token) {
    return (
      <TileLayer attribution={MAPBOX_ATTRIBUTION} url={mapboxDarkTileUrl(token)} />
    );
  }

  return (
    <TileLayer
      attribution='&copy; <a href="https://openstreetmap.org">OpenStreetMap</a>'
      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    />
  );
}