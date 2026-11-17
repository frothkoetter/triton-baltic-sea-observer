import { CircleMarker, MapContainer, Tooltip } from "react-leaflet";
import type { AisRow } from "../pages/Lagebild";
import { BasemapTileLayer } from "./BasemapTileLayer";

type Area = { area_id: string; center_latitude: number; center_longitude: number };

export type FleetPosition = {
  mmsi: string;
  latitude: number;
  longitude: number;
  unit_name?: string;
  unit_class?: string;
  speed?: number;
  course?: number;
  status?: string;
  operational_status?: string;
  home_base?: string;
  vessel_category?: string;
  event_timestamp?: string;
};

export type OutlierRow = {
  mmsi: number;
  latitude: number;
  longitude: number;
  event_timestamp?: string;
  outlier_type?: string;
  reason?: string;
  severity_level?: number;
  speed?: number;
  harbour_name?: string;
};

type Props = {
  ships: AisRow[];
  areas: Area[];
  outliers: OutlierRow[];
  germanFleet: FleetPosition[];
  russianFleet: FleetPosition[];
  showSanctions: boolean;
};

export function TacticalMap({
  ships,
  areas,
  outliers,
  germanFleet,
  russianFleet,
  showSanctions,
}: Props) {
  const center: [number, number] = [55.5, 17.5];
  return (
    <div className="map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        {areas.map((a) => (
          <CircleMarker
            key={a.area_id}
            center={[a.center_latitude, a.center_longitude]}
            radius={14}
            pathOptions={{ color: "#FFD54F", fillOpacity: 0.15, weight: 1 }}
          >
            <Tooltip>{a.area_id}</Tooltip>
          </CircleMarker>
        ))}
        {germanFleet.map((u) => (
          <CircleMarker
            key={`de-${u.mmsi}-${u.event_timestamp}`}
            center={[u.latitude, u.longitude]}
            radius={10}
            pathOptions={{
              color: "#0D47A1",
              fillColor: "#42A5F5",
              fillOpacity: 0.9,
              weight: 2,
            }}
          >
            <Tooltip>
              <span className="mono">
                <strong>DEU · {u.unit_name ?? u.mmsi}</strong>
                <br />
                {u.unit_class}
                {u.operational_status ? ` · ${u.operational_status}` : ""}
                <br />
                SOG {Number(u.speed ?? 0).toFixed(1)} kn
              </span>
            </Tooltip>
          </CircleMarker>
        ))}
        {russianFleet.map((u) => (
          <CircleMarker
            key={`ru-${u.mmsi}-${u.event_timestamp}`}
            center={[u.latitude, u.longitude]}
            radius={10}
            pathOptions={{
              color: "#4A148C",
              fillColor: "#AB47BC",
              fillOpacity: 0.92,
              weight: 2,
            }}
          >
            <Tooltip>
              <span className="mono">
                <strong>RUS · {u.unit_name ?? u.mmsi}</strong>
                <br />
                {u.unit_class}
                {u.home_base ? ` · ${u.home_base}` : ""}
                <br />
                SOG {Number(u.speed ?? 0).toFixed(1)} kn
              </span>
            </Tooltip>
          </CircleMarker>
        ))}
        {ships.map((s) => {
          const hostile =
            showSanctions &&
            (s.sanctioned ||
              s.flag === "Russia" ||
              s.flag === "Liberia" ||
              String(s.ship_name).includes("HS "));
          const color = hostile ? "#E53935" : "#3DDC84";
          return (
            <CircleMarker
              key={`${s.mmsi}-${s.event_timestamp}`}
              center={[s.latitude, s.longitude]}
              radius={hostile ? 7 : 5}
              pathOptions={{ color, fillColor: color, fillOpacity: 0.85, weight: 1 }}
            >
              <Tooltip>
                <span className="mono">
                  {s.ship_name ?? s.mmsi}
                  <br />
                  {s.vessel_type} · {s.flag}
                </span>
              </Tooltip>
            </CircleMarker>
          );
        })}
        {outliers.map((o) => {
          const sev = Number(o.severity_level ?? 50);
          const color = sev >= 75 ? "#FF5252" : sev >= 55 ? "#FF9800" : "#FFB74D";
          return (
            <CircleMarker
              key={`outlier-${o.mmsi}-${o.event_timestamp}-${o.outlier_type}`}
              center={[o.latitude, o.longitude]}
              radius={9}
              pathOptions={{
                color: "#212121",
                fillColor: color,
                fillOpacity: 0.92,
                weight: 2,
              }}
            >
              <Tooltip>
                <span className="mono">
                  <strong>{o.outlier_type ?? "Anomaly"}</strong>
                  <br />
                  MMSI {o.mmsi}
                  {o.harbour_name ? ` · ${o.harbour_name}` : ""}
                  <br />
                  {(o.reason ?? "").slice(0, 120)}
                </span>
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
}
