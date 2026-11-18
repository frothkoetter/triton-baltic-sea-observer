import { Fragment, useMemo } from "react";
import { CircleMarker, MapContainer, Polygon, Polyline, Tooltip } from "react-leaflet";
import type { PositionTrack } from "../lib/position-tracks";
import { wktToPolygonRings } from "../lib/wkt-geo";
import type { ObservationArea } from "./TacticalMap";
import { BasemapTileLayer } from "./BasemapTileLayer";

const TRACK_COLORS = ["#E53935", "#FF6F00", "#7B1FA2", "#1565C0", "#2E7D32", "#00838F", "#C62828", "#6A1B9A"];

type Props = {
  areas: ObservationArea[];
  tracks: PositionTrack[];
};

export function RestrictedZoneMap({ areas, tracks }: Props) {
  const center: [number, number] = [57.2, 18.5];
  const areaRings = useMemo(() => {
    return areas
      .map((a) => {
        const rings = wktToPolygonRings(a.polygon ?? "");
        if (!rings.length) return null;
        return { area_id: a.area_id, rings, area: a };
      })
      .filter(Boolean) as {
      area_id: string;
      rings: [number, number][][];
      area: ObservationArea;
    }[];
  }, [areas]);

  return (
    <div className="map-wrap restricted-map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        {areaRings.map(({ area_id, rings, area }) =>
          rings.map((ring, ri) => (
            <Polygon
              key={`${area_id}-${ri}`}
              positions={ring}
              pathOptions={{
                color: "#E65100",
                fillColor: "#FF6F00",
                fillOpacity: 0.12,
                weight: 2,
                dashArray: area.area_type?.toLowerCase().includes("defence") ? undefined : "4 6",
              }}
            >
              <Tooltip sticky>
                <span className="mono">{area_id}</span>
                {area.country ? ` · ${area.country}` : ""}
              </Tooltip>
            </Polygon>
          ))
        )}
        {tracks.map((t, ti) => {
          const color = TRACK_COLORS[ti % TRACK_COLORS.length];
          const last = t.points[t.points.length - 1];
          const first = t.points[0];
          return (
            <Fragment key={t.id}>
              {t.points.length >= 2 && (
                <Polyline
                  positions={t.points}
                  pathOptions={{ color, weight: 3, opacity: 0.85 }}
                />
              )}
              {first && (
                <CircleMarker
                  center={first}
                  radius={4}
                  pathOptions={{ color, fillColor: "#fff", fillOpacity: 1, weight: 2 }}
                >
                  <Tooltip>
                    <span className="mono">Start · {t.label}</span>
                  </Tooltip>
                </CircleMarker>
              )}
              {last && (
                <CircleMarker
                  center={last}
                  radius={6}
                  pathOptions={{ color, fillColor: color, fillOpacity: 0.9, weight: 2 }}
                >
                  <Tooltip>
                    <span className="mono">
                      {t.label}
                      <br />
                      {t.pointCount} pts in zone
                    </span>
                  </Tooltip>
                </CircleMarker>
              )}
            </Fragment>
          );
        })}
      </MapContainer>
    </div>
  );
}