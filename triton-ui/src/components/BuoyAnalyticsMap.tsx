import { CircleMarker, MapContainer, Tooltip } from "react-leaflet";
import { BasemapTileLayer } from "./BasemapTileLayer";
import { MapInvalidateSize } from "./MapInvalidateSize";

export type BuoyGeoCluster = {
  geo_position_lat: number;
  geo_position_lon: number;
  record_count: number;
  sum_altitude?: number;
};

function clusterRadius(count: number): number {
  const n = Math.max(1, count);
  return Math.min(22, 4 + Math.log10(n) * 6);
}

function clusterColor(count: number): string {
  if (count >= 1500) return "#ffb020";
  if (count >= 800) return "#ff8c00";
  return "#45bac7";
}

type Props = {
  clusters: BuoyGeoCluster[];
};

export function BuoyAnalyticsMap({ clusters }: Props) {
  const center: [number, number] = [57.5, 18];
  return (
    <div className="map-wrap buoy-map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <MapInvalidateSize />
        <BasemapTileLayer />
        {clusters.map((c, i) => {
          const lat = Number(c.geo_position_lat);
          const lon = Number(c.geo_position_lon);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
          const count = Number(c.record_count) || 1;
          const color = clusterColor(count);
          const r = clusterRadius(count);
          const label =
            count >= 1000 ? `${(count / 1000).toFixed(1)}k` : String(Math.round(count));
          return (
            <CircleMarker
              key={`${lat}-${lon}-${i}`}
              center={[lat, lon]}
              radius={r}
              pathOptions={{
                color,
                fillColor: color,
                fillOpacity: 0.55,
                weight: 1,
              }}
            >
              <Tooltip permanent direction="center" className="buoy-cluster-label">
                {label}
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
}