import { Fragment } from "react";
import { CircleMarker, MapContainer, Polyline, Tooltip } from "react-leaflet";
import type { PositionTrack, TrackKind } from "../lib/position-tracks";
import { BasemapTileLayer } from "./BasemapTileLayer";

const KIND_STYLE: Record<TrackKind, { line: string; dot: string }> = {
  ais: { line: "#3DDC84", dot: "#2E7D4E" },
  deu_fleet: { line: "#42A5F5", dot: "#0D47A1" },
  rus_fleet: { line: "#CE93D8", dot: "#4A148C" },
};

type Props = {
  tracks: PositionTrack[];
};

export function RouteTrackMap({ tracks }: Props) {
  const center: [number, number] = [55.5, 17.5];
  return (
    <div className="map-wrap map-wrap-tracks">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        {tracks.map((t) => {
          const style = KIND_STYLE[t.kind] ?? KIND_STYLE.ais;
          const last = t.points[t.points.length - 1];
          const first = t.points[0];
          const showLine = t.points.length >= 2;
          return (
            <Fragment key={t.id}>
              {showLine && (
                <Polyline
                  positions={t.points}
                  pathOptions={{
                    color: style.line,
                    weight: t.kind === "ais" ? 2 : 3,
                    opacity: 0.72,
                  }}
                />
              )}
              {first && (
                <CircleMarker
                  center={first}
                  radius={4}
                  pathOptions={{
                    color: style.dot,
                    fillColor: "#fff",
                    fillOpacity: 1,
                    weight: 2,
                  }}
                >
                  <Tooltip>
                    <span className="mono">
                      Start · {t.label}
                      <br />
                      {t.kind} · {t.mmsi}
                    </span>
                  </Tooltip>
                </CircleMarker>
              )}
              {last && (
                <CircleMarker
                  center={last}
                  radius={6}
                  pathOptions={{
                    color: style.dot,
                    fillColor: style.line,
                    fillOpacity: 0.9,
                    weight: 2,
                  }}
                >
                  <Tooltip>
                    <span className="mono">
                      Latest · {t.label}
                      <br />
                      {t.pointCount} points · {t.kind}
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