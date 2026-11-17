import { useEffect } from "react";
import { CircleMarker, MapContainer, Tooltip, useMap } from "react-leaflet";
import type { LidarScan } from "../lib/lidar";
import { lidarMarkerColor, scanKey } from "../lib/lidar";
import { BasemapTileLayer } from "./BasemapTileLayer";

type MapProps = {
  scans: LidarScan[];
  selectedKey: string | null;
  onSelect: (scan: LidarScan) => void;
};

function FlyToSelected({ scans, selectedKey }: Pick<MapProps, "scans" | "selectedKey">) {
  const map = useMap();
  useEffect(() => {
    if (!selectedKey) return;
    const scan = scans.find((s) => scanKey(s) === selectedKey);
    if (!scan) return;
    map.flyTo([scan.center_lat, scan.center_lon], 8, { duration: 0.5 });
  }, [scans, selectedKey, map]);
  return null;
}

export function LidarMap({ scans, selectedKey, onSelect }: MapProps) {
  const center: [number, number] = [57, 18];
  return (
    <div className="map-wrap lidar-map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />
        <FlyToSelected scans={scans} selectedKey={selectedKey} />
        {scans.map((scan) => {
          const key = scanKey(scan);
          const selected = key === selectedKey;
          const color = lidarMarkerColor(scan.vessel_type, scan.classification);
          const radius = selected ? 11 : Math.min(9, 5 + Math.log10((scan.point_count ?? 1000) + 1));
          return (
            <CircleMarker
              key={key}
              center={[scan.center_lat, scan.center_lon]}
              radius={radius}
              pathOptions={{
                color: selected ? "#fff" : color,
                fillColor: color,
                fillOpacity: selected ? 0.95 : 0.75,
                weight: selected ? 2 : 1,
              }}
              eventHandlers={{
                click: () => onSelect(scan),
              }}
            >
              <Tooltip>
                <span className="mono">
                  {scan.vessel_name ?? "UNCLASSIFIED"}
                  <br />
                  {scan.vessel_type} · cls {scan.classification ?? "?"}
                  <br />
                  {scan.point_count?.toLocaleString() ?? "—"} pts
                </span>
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
}