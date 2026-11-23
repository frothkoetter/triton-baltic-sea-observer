import { useEffect, useMemo } from "react";
import { CircleMarker, MapContainer, Rectangle, Tooltip, useMap } from "react-leaflet";
import { BasemapTileLayer } from "./BasemapTileLayer";
import { MapInvalidateSize } from "./MapInvalidateSize";
import { quadkeyLeafletBounds } from "../lib/bing-quadkey";
import type { GpsJammerEvent } from "../lib/gps-jammer";
import {
  type SocmintDensityTile,
  type SocmintMessage,
  type ThreatHotTile,
  socmintTweetDisplay,
  threatTileColor,
  trustTagColor,
} from "../lib/socmint";

const BALTIC_CENTER: [number, number] = [57.5, 18.0];
const BALTIC_ZOOM = 6;

type Props = {
  messages: SocmintMessage[];
  threatTiles: ThreatHotTile[];
  densityTiles?: SocmintDensityTile[];
  gpsJammerEvents?: GpsJammerEvent[];
  selectedUsername?: string | null;
  selectedTs?: string | null;
  showThreatTiles?: boolean;
  showDensityTiles?: boolean;
  showGpsJammer?: boolean;
  showPosts?: boolean;
  onSelectMessage?: (msg: SocmintMessage) => void;
};

function FlyToSelection({
  messages,
  selectedUsername,
  selectedTs,
}: {
  messages: SocmintMessage[];
  selectedUsername?: string | null;
  selectedTs?: string | null;
}) {
  const map = useMap();
  useEffect(() => {
    if (!selectedUsername || !selectedTs) return;
    const hit = messages.find(
      (m) => m.user_username === selectedUsername && m.ts === selectedTs
    );
    if (hit) map.flyTo([hit.latitude, hit.longitude], 9, { duration: 0.5 });
  }, [messages, selectedUsername, selectedTs, map]);
  return null;
}

export function SocmintMap({
  messages,
  threatTiles,
  densityTiles = [],
  gpsJammerEvents = [],
  selectedUsername,
  selectedTs,
  showThreatTiles = true,
  showDensityTiles = true,
  showGpsJammer = false,
  showPosts = true,
  onSelectMessage,
}: Props) {
  const threatByKey = useMemo(() => {
    const m = new Map<string, ThreatHotTile>();
    for (const t of threatTiles) m.set(t.quadkey, t);
    return m;
  }, [threatTiles]);

  return (
    <div className="map-wrap socmint-map-wrap">
      <MapContainer
        center={BALTIC_CENTER}
        zoom={BALTIC_ZOOM}
        className="tactical-map"
        style={{ height: "100%", width: "100%" }}
      >
        <MapInvalidateSize />
        <BasemapTileLayer />
        <FlyToSelection
          messages={messages}
          selectedUsername={selectedUsername}
          selectedTs={selectedTs}
        />

        {showDensityTiles
          ? densityTiles.map((d) => {
              const bounds = quadkeyLeafletBounds(d.quadkey);
              return (
                <Rectangle
                  key={`soc-density-${d.quadkey}`}
                  bounds={bounds}
                  pathOptions={{
                    color: d.disinfo_count > 0 ? "#E53935" : "#4DA3FF",
                    weight: 1,
                    dashArray: "6 4",
                    fillColor: d.disinfo_count > 0 ? "#E53935" : "#4DA3FF",
                    fillOpacity: Math.min(0.35, 0.08 + d.post_count * 0.04),
                  }}
                >
                  <Tooltip>
                    <span className="mono">
                      <strong>SOCMINT tile {d.quadkey}</strong>
                      <br />
                      Posts {d.post_count} · IW {d.disinfo_count}
                    </span>
                  </Tooltip>
                </Rectangle>
              );
            })
          : null}

        {showThreatTiles
          ? threatTiles.map((t) => {
              const bounds = quadkeyLeafletBounds(t.quadkey);
              const fill = threatTileColor(t.sev_p90);
              return (
                <Rectangle
                  key={`threat-${t.quadkey}`}
                  bounds={bounds}
                  pathOptions={{
                    color: fill,
                    weight: 2,
                    fillColor: fill,
                    fillOpacity: Math.min(0.55, 0.15 + t.sev_p90 / 200),
                  }}
                >
                  <Tooltip>
                    <span className="mono">
                      <strong>Threat sketch · {t.quadkey}</strong>
                      <br />
                      events={t.event_count} · P50 {t.sev_p50.toFixed(0)} · P90{" "}
                      {t.sev_p90.toFixed(0)}
                    </span>
                  </Tooltip>
                </Rectangle>
              );
            })
          : null}

        {showGpsJammer
          ? gpsJammerEvents.map((j) => {
              const jamming = j.jamming_indicator !== false;
              const color = jamming ? "#E040FB" : "#BA68C8";
              const nic = j.adsb_nic;
              return (
                <CircleMarker
                  key={`soc-gpsjam-${j.geohash}-${j.event_timestamp}`}
                  center={[j.latitude, j.longitude]}
                  radius={jamming ? 12 : 9}
                  pathOptions={{
                    color: "#4A148C",
                    fillColor: color,
                    fillOpacity: 0.93,
                    weight: 2,
                  }}
                >
                  <Tooltip>
                    <span className="mono">
                      <strong>GPS jammer · {j.geohash || "tile"}</strong>
                      <br />
                      {nic != null && Number.isFinite(nic) ? `NIC ${nic}` : "NIC —"}
                      {j.signal_integrity != null && Number.isFinite(j.signal_integrity)
                        ? ` · integrity ${j.signal_integrity.toFixed(2)}`
                        : ""}
                      {j.harbour_name ? ` · ${j.harbour_name}` : ""}
                      <br />
                      {(j.reason ?? (jamming ? "Probable jamming (NIC<5)" : "Monitor")).slice(0, 140)}
                    </span>
                  </Tooltip>
                </CircleMarker>
              );
            })
          : null}

        {showPosts
          ? messages.map((m) => {
              const selected =
                m.user_username === selectedUsername && m.ts === selectedTs;
              const color = trustTagColor(m.trust_tag, m.disinformation_flag);
              const tile = m.quadkey ? threatByKey.get(m.quadkey) : undefined;
              return (
                <CircleMarker
                  key={`${m.user_username}-${m.ts}`}
                  center={[m.latitude, m.longitude]}
                  radius={selected ? 11 : 8}
                  pathOptions={{
                    color: selected ? "#FFFFFF" : "#212121",
                    weight: selected ? 3 : 2,
                    fillColor: color,
                    fillOpacity: 0.95,
                  }}
                  eventHandlers={{
                    click: () => onSelectMessage?.(m),
                  }}
                >
                  <Tooltip className="socmint-marker-tooltip" direction="top" offset={[0, -6]}>
                    <div className="socmint-tooltip">
                      <div className="socmint-tooltip-meta mono">
                        <strong>
                          @{m.user_username} · {m.platform ?? "x"}
                        </strong>
                        <br />
                        Trust: {m.trust_tag ?? "—"}
                        {m.disinformation_flag ? " · IW" : ""}
                        <br />
                        {m.intel_category ?? "—"} · Prio {m.priority ?? "—"}
                        {tile ? ` · tile P90 ${tile.sev_p90.toFixed(0)}` : ""}
                      </div>
                      <p className="socmint-tooltip-body">
                        {socmintTweetDisplay(m.tweet, 220)}
                      </p>
                    </div>
                  </Tooltip>
                </CircleMarker>
              );
            })
          : null}
      </MapContainer>
      <div className="map-legend map-legend-infra" aria-hidden>
        <span className="map-legend-item">
          <i className="map-legend-swatch" style={{ background: "#3DDC84" }} /> SENSOR_CORROBORATED
        </span>
        <span className="map-legend-item">
          <i className="map-legend-swatch" style={{ background: "#FFD54F" }} /> UNVERIFIED
        </span>
        <span className="map-legend-item">
          <i className="map-legend-swatch" style={{ background: "#E53935" }} /> DISINFORMATION
        </span>
        <span className="map-legend-item">
          <i
            className="map-legend-swatch"
            style={{ background: "#FF9800", opacity: 0.7 }}
          />{" "}
          Threat tile (P90)
        </span>
        <span className="map-legend-item">
          <i
            className="map-legend-swatch"
            style={{
              background: "transparent",
              border: "2px dashed #4DA3FF",
            }}
          />{" "}
          SOCMINT density tile
        </span>
        {showGpsJammer ? (
          <span className="map-legend-item">
            <i className="map-legend-swatch" style={{ background: "#E040FB" }} /> GPS jammer (NIC)
          </span>
        ) : null}
      </div>
    </div>
  );
}