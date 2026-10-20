import { CircleMarker, MapContainer, TileLayer, Tooltip } from "react-leaflet";
import type { AisRow } from "../pages/Lagebild";

type Area = { area_id: string; center_latitude: number; center_longitude: number };

type Props = {
  ships: AisRow[];
  areas: Area[];
  showSanctions: boolean;
};

export function TacticalMap({ ships, areas, showSanctions }: Props) {
  const center: [number, number] = [55.5, 17.5];
  return (
    <div className="map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <TileLayer
          attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        />
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
      </MapContainer>
    </div>
  );
}
