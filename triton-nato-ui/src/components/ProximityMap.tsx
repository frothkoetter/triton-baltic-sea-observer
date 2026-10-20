import { useEffect } from "react";
import {
  CircleMarker,
  MapContainer,
  Polyline,
  Popup,
  TileLayer,
  Tooltip,
  useMap,
} from "react-leaflet";
import type { ProximityEvent } from "../lib/proximity";
import { distanceLineColor, pairMidpoint } from "../lib/proximity";

const BALTIC_BOUNDS: [[number, number], [number, number]] = [
  [53.5, 10],
  [60.5, 30],
];

const SHIP_A_COLOR = "#ffb020";
const SHIP_B_COLOR = "#4da3ff";

type Props = {
  events: ProximityEvent[];
  selectedId: number | null;
};

function FlyToSelection({ events, selectedId }: Props) {
  const map = useMap();
  useEffect(() => {
    if (selectedId == null) return;
    const ev = events.find((e) => e.id === selectedId);
    if (!ev) return;
    const mid = pairMidpoint(ev);
    map.flyTo(mid, 9, { duration: 0.6 });
  }, [events, selectedId, map]);
  return null;
}

export function ProximityMap({ events, selectedId }: Props) {
  const center: [number, number] = [57, 20];

  return (
    <div className="map-wrap proximity-map-wrap">
      <MapContainer
        center={center}
        zoom={6}
        minZoom={5}
        maxBounds={BALTIC_BOUNDS}
        maxBoundsViscosity={0.85}
        style={{ height: "100%", width: "100%" }}
      >
        <TileLayer
          attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        />
        <FlyToSelection events={events} selectedId={selectedId} />
        {events.map((ev) => {
          const selected = ev.id === selectedId;
          const lineColor = distanceLineColor(ev.distance_km);
          const weight = selected ? 4 : 2;
          const opacity = selected ? 1 : 0.75;
          return (
            <Polyline
              key={`line-${ev.id}`}
              positions={[
                [ev.ship_a_lat, ev.ship_a_lon],
                [ev.ship_b_lat, ev.ship_b_lon],
              ]}
              pathOptions={{ color: lineColor, weight, opacity }}
            >
              <Popup>
                <div className="mono proximity-popup">
                  <strong>Schiff A:</strong> {ev.ship_a}
                  <br />
                  <strong>Schiff B:</strong> {ev.ship_b}
                  <br />
                  <strong>Abstand:</strong> {ev.distance_km.toFixed(3)} km
                  <br />
                  <strong>event_time:</strong> {ev.event_time}
                  <br />
                  <strong>ship_b_time:</strong> {ev.ship_b_time}
                </div>
              </Popup>
            </Polyline>
          );
        })}
        {events.flatMap((ev) => {
          const selected = ev.id === selectedId;
          const radius = selected ? 8 : 6;
          return [
            <CircleMarker
              key={`a-${ev.id}`}
              center={[ev.ship_a_lat, ev.ship_a_lon]}
              radius={radius}
              pathOptions={{
                color: SHIP_A_COLOR,
                fillColor: SHIP_A_COLOR,
                fillOpacity: selected ? 1 : 0.85,
                weight: selected ? 2 : 1,
              }}
            >
              <Tooltip permanent direction="top" offset={[0, -6]} className="proximity-mmsi-label">
                {ev.ship_a}
              </Tooltip>
            </CircleMarker>,
            <CircleMarker
              key={`b-${ev.id}`}
              center={[ev.ship_b_lat, ev.ship_b_lon]}
              radius={radius}
              pathOptions={{
                color: SHIP_B_COLOR,
                fillColor: SHIP_B_COLOR,
                fillOpacity: selected ? 1 : 0.85,
                weight: selected ? 2 : 1,
              }}
            >
              <Tooltip permanent direction="top" offset={[0, -6]} className="proximity-mmsi-label">
                {ev.ship_b}
              </Tooltip>
            </CircleMarker>,
          ];
        })}
      </MapContainer>
    </div>
  );
}
