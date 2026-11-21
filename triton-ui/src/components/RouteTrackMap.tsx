import { Fragment } from "react";
import { CircleMarker, MapContainer, Polyline, Tooltip } from "react-leaflet";
import type { PositionTrack, TrackKind } from "../lib/position-tracks";
import { trackPathUpToTime } from "../lib/track-timeline";
import { BasemapTileLayer } from "./BasemapTileLayer";

const KIND_STYLE: Record<TrackKind, { line: string; dot: string; label: string }> = {
  ais: { line: "#3DDC84", dot: "#2E7D4E", label: "AIS" },
  deu_fleet: { line: "#42A5F5", dot: "#0D47A1", label: "DEU Fleet" },
  rus_fleet: { line: "#CE93D8", dot: "#4A148C", label: "RUS Fleet" },
};

type Props = {
  tracks: PositionTrack[];
  /** When set, only the trail up to this instant is drawn and the head marker follows the slider. */
  cursorTimeMs?: number | null;
  showStartMarkers?: boolean;
};

export function RouteTrackMap({
  tracks,
  cursorTimeMs = null,
  showStartMarkers = true,
}: Props) {
  const center: [number, number] = [55.5, 17.5];
  const scrub = cursorTimeMs != null && Number.isFinite(cursorTimeMs);

  return (
    <div className="map-wrap map-wrap-tracks">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        {tracks.map((t) => {
          const style = KIND_STYLE[t.kind] ?? KIND_STYLE.ais;
          const { path, head } = scrub
            ? trackPathUpToTime(t, cursorTimeMs)
            : { path: t.points, head: t.points[t.points.length - 1] ?? null };
          const first = t.points[0];
          const showLine = path.length >= 2;
          const showHead = head != null;

          return (
            <Fragment key={t.id}>
              {showLine && (
                <Polyline
                  positions={path}
                  pathOptions={{
                    color: style.line,
                    weight: t.kind === "ais" ? 2 : 3,
                    opacity: scrub ? 0.85 : 0.72,
                  }}
                />
              )}
              {showStartMarkers && first && (!scrub || path.length > 0) && (
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
                      {style.label} · {t.mmsi}
                    </span>
                  </Tooltip>
                </CircleMarker>
              )}
              {showHead && (
                <CircleMarker
                  center={head}
                  radius={scrub ? 7 : 6}
                  pathOptions={{
                    color: style.dot,
                    fillColor: style.line,
                    fillOpacity: 0.95,
                    weight: 2,
                  }}
                >
                  <Tooltip>
                    <span className="mono">
                      {scrub ? "Zeitpunkt" : "Latest"} · {t.label}
                      <br />
                      {t.pointCount} Punkte · {style.label}
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