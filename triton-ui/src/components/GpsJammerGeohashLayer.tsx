import { useMemo, useState } from "react";
import { Rectangle, Tooltip, useMap, useMapEvents } from "react-leaflet";
import {
  type GpsJammerEvent,
  gpsJammerTileFillOpacity,
  gpsJammerVisualLeafletBounds,
  minGpsJammerSpanDegrees,
} from "../lib/gps-jammer";

type Props = {
  events: GpsJammerEvent[];
};

export function GpsJammerGeohashLayer({ events }: Props) {
  const map = useMap();
  const [zoom, setZoom] = useState(() => map.getZoom());
  useMapEvents({
    zoomend: () => setZoom(map.getZoom()),
  });

  const minSpan = minGpsJammerSpanDegrees(zoom);

  const tiles = useMemo(() => {
    return events
      .map((j) => {
        const bounds = gpsJammerVisualLeafletBounds(j, zoom);
        if (!bounds) return null;
        return { event: j, bounds };
      })
      .filter((t): t is { event: GpsJammerEvent; bounds: [[number, number], [number, number]] } => t != null);
  }, [events, zoom]);

  return (
    <>
      {tiles.map(({ event: j, bounds }) => {
        const jamming = j.jamming_indicator !== false;
        const color = jamming ? "#E040FB" : "#BA68C8";
        const nic = j.adsb_nic;
        return (
          <Rectangle
            key={`gpsjam-gh-${j.geohash}-${j.event_timestamp ?? ""}`}
            bounds={bounds}
            pathOptions={{
              color: jamming ? "#F3E5F5" : "#CE93D8",
              weight: jamming ? 2 : 1,
              dashArray: jamming ? undefined : "4 3",
              fillColor: color,
              fillOpacity: gpsJammerTileFillOpacity(j.signal_integrity),
            }}
          >
            <Tooltip className="socmint-marker-tooltip" direction="top">
              <div className="socmint-tooltip">
                <div className="socmint-tooltip-meta mono">
                  <strong>GPS jammer · {j.geohash}</strong>
                  <br />
                  Jamming: {j.jamming_indicator === true ? "yes" : j.jamming_indicator === false ? "no" : "—"}
                  {nic != null && Number.isFinite(nic) ? ` · NIC ${nic}` : ""}
                </div>
                <p className="socmint-tooltip-body" style={{ margin: 0 }}>
                  {j.signal_integrity != null && Number.isFinite(j.signal_integrity)
                    ? `Signal integrity ${j.signal_integrity.toFixed(2)}`
                    : "Signal integrity —"}
                  {j.harbour_name ? ` · ${j.harbour_name}` : ""}
                  <br />
                  Anzeige mind. {minSpan.toFixed(2)}° (Zoom {zoom})
                  <br />
                  {(j.reason ?? (jamming ? "Probable jamming (NIC<5)" : "Monitor")).slice(0, 120)}
                </p>
              </div>
            </Tooltip>
          </Rectangle>
        );
      })}
    </>
  );
}