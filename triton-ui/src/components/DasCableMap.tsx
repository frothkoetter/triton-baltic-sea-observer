import { useEffect, useMemo } from "react";
import { CircleMarker, MapContainer, Polyline, Tooltip, useMap } from "react-leaflet";
import type { DasCableSegment, DasThreatLabelMode } from "../lib/das-cable";
import { dasThreatColor, segmentKey, segmentThreatLabel } from "../lib/das-cable";
import {
  buildOstseeInfraMapLines,
  type OstseeInfraRow,
  type OstseePipelineRow,
} from "../lib/ostsee-infra-map";
import { BasemapTileLayer } from "./BasemapTileLayer";

type MapProps = {
  segments: DasCableSegment[];
  selectedKey: string | null;
  onSelect: (seg: DasCableSegment) => void;
  infrastruktur?: OstseeInfraRow[];
  pipelines?: OstseePipelineRow[];
  threatLabelMode?: DasThreatLabelMode;
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

export function DasCableMap({
  segments,
  selectedKey,
  onSelect,
  infrastruktur = [],
  pipelines = [],
  threatLabelMode = "ground_truth",
}: MapProps) {
  const center: [number, number] = [57, 18];
  const infraLines = useMemo(
    () => buildOstseeInfraMapLines(infrastruktur, pipelines),
    [infrastruktur, pipelines]
  );

  return (
    <div className="map-wrap lidar-map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        {infraLines.map((item) =>
          item.lines.map((positions, idx) => (
            <Polyline key={`${item.key}-${idx}`} positions={positions} pathOptions={item.style}>
              {idx === 0 && (
                <Tooltip sticky>
                  <span className="mono">
                    <strong>{item.label}</strong>
                    <br />
                    {item.meta}
                  </span>
                </Tooltip>
              )}
            </Polyline>
          ))
        )}
        <FlyToSelected segments={segments} selectedKey={selectedKey} />
        {segments.map((seg) => {
          const key = segmentKey(seg);
          const selected = key === selectedKey;
          const label = segmentThreatLabel(seg, threatLabelMode);
          const color = dasThreatColor(label);
          const severity = label === "cui_sabotage_tap" || label === "anchor_drag" ? 11 : 8;
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
      {infraLines.length > 0 && (
        <div className="map-legend mono" style={{ fontSize: 10, marginTop: 4 }}>
          <span style={{ color: "#26C6DA" }}>▬</span> Seekabel / Strom ·{" "}
          <span style={{ color: "#FF6F00" }}>▬</span> Pipeline (EMODnet)
        </div>
      )}
    </div>
  );
}