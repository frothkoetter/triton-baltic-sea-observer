import { useEffect } from "react";
import { CircleMarker, MapContainer, Tooltip, useMap } from "react-leaflet";
import type { DasCableSegment } from "../lib/das-cable";
import { dasThreatColor, segmentKey } from "../lib/das-cable";
import { BasemapTileLayer } from "./BasemapTileLayer";

type MapProps = {
  segments: DasCableSegment[];
  selectedKey: string | null;
  onSelect: (seg: DasCableSegment) => void;
};

function FlyToSelected({ segments, selectedKey }: Pick<MapProps, "segments" | "selectedKey">) {
  const map = useMap();
  useEffect(() => {
    if (!selectedKey) return;
    const seg = segments.find((s) => segmentKey(s) === selectedKey);
    if (!seg) return;
    map.flyTo([seg.segment_lat, seg.segment_lon], 8, { duration: 0.5 });
  }, [segments, selectedKey, map]);
  return null;
}

export function DasCableMap({ segments, selectedKey, onSelect }: MapProps) {
  const center: [number, number] = [57, 18];
  return (
    <div className="map-wrap lidar-map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        <FlyToSelected segments={segments} selectedKey={selectedKey} />
        {segments.map((seg) => {
          const key = segmentKey(seg);
          const selected = key === selectedKey;
          const label = seg.predicted_threat_type ?? seg.threat_class ?? "ambient";
          const color = dasThreatColor(label);
          const severity =
            label === "cui_sabotage_tap" || label === "anchor_drag" ? 11 : 8;
          return (
            <CircleMarker
              key={key}
              center={[seg.segment_lat, seg.segment_lon]}
              radius={selected ? severity + 2 : severity}
              pathOptions={{
                color: selected ? "#fff" : color,
                fillColor: color,
                fillOpacity: selected ? 0.95 : 0.78,
                weight: selected ? 2 : 1,
              }}
              eventHandlers={{
                click: () => onSelect(seg),
              }}
            >
              <Tooltip>
                <span className="mono">
                  {seg.cable_name ?? seg.cable_asset ?? "cable"}
                  <br />
                  {label}
                  <br />
                  ch {seg.chainage_m != null ? `${Math.round(seg.chainage_m)} m` : "—"}
                </span>
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
}