import { Fragment, useMemo } from "react";
import { CircleMarker, MapContainer, Polygon, Polyline, Tooltip } from "react-leaflet";
import type { AisRow } from "../pages/Lagebild";
import {
  buildOstseeInfraMapLines,
  type OstseeInfraRow,
  type OstseePipelineRow,
} from "../lib/ostsee-infra-map";
import { wktToPolygonRings, type LatLng } from "../lib/wkt-geo";
import { BasemapTileLayer } from "./BasemapTileLayer";
import { GpsJammerGeohashLayer } from "./GpsJammerGeohashLayer";

export type ObservationArea = {
  area_id: string;
  center_latitude: number;
  center_longitude: number;
  polygon?: string | null;
  country?: string;
  area_type?: string;
  tier?: string;
  source?: string;
};

function areaStyle(area: ObservationArea): { color: string; fillColor: string } {
  const firing = (area.area_type ?? "").toLowerCase().includes("firing");
  return firing
    ? { color: "#E65100", fillColor: "#FF6F00" }
    : { color: "#1565C0", fillColor: "#42A5F5" };
}

export type { OstseeInfraRow, OstseePipelineRow } from "../lib/ostsee-infra-map";

export type FleetPosition = {
  mmsi: string;
  latitude: number;
  longitude: number;
  unit_name?: string;
  unit_class?: string;
  speed?: number;
  course?: number;
  status?: string;
  operational_status?: string;
  home_base?: string;
  vessel_category?: string;
  event_timestamp?: string;
};

export type NbaIncidentMarker = {
  flow_uuid: string;
  latitude: number;
  longitude: number;
  priority?: string;
  theatre?: string;
  alert_type?: string;
  best_coa_id?: string | null;
  status?: string;
};

export type OutlierRow = {
  mmsi: number;
  latitude: number;
  longitude: number;
  event_timestamp?: string;
  outlier_type?: string;
  reason?: string;
  severity_level?: number;
  speed?: number;
  harbour_name?: string;
};

export type GpsJammerMapEvent = {
  geohash: string;
  latitude: number;
  longitude: number;
  event_timestamp?: string;
  adsb_nic?: number | null;
  signal_integrity?: number | null;
  jamming_indicator?: boolean;
  reason?: string;
  harbour_name?: string;
};

type Props = {
  ships: AisRow[];
  areas: ObservationArea[];
  infrastruktur: OstseeInfraRow[];
  pipelines: OstseePipelineRow[];
  outliers: OutlierRow[];
  gpsJammerEvents?: GpsJammerMapEvent[];
  showGpsJammer?: boolean;
  germanFleet: FleetPosition[];
  russianFleet: FleetPosition[];
  nbaIncidents?: NbaIncidentMarker[];
  showNba?: boolean;
  onNbaIncidentClick?: (flowUuid: string) => void;
  showSanctions: boolean;
  showAreas: boolean;
  showInfrastructure: boolean;
};

export function TacticalMap({
  ships,
  areas,
  infrastruktur,
  pipelines,
  outliers,
  gpsJammerEvents = [],
  showGpsJammer = false,
  germanFleet,
  russianFleet,
  nbaIncidents = [],
  showNba = false,
  onNbaIncidentClick,
  showSanctions,
  showAreas,
  showInfrastructure,
}: Props) {
  const center: [number, number] = [55.5, 17.5];

  const areaPolygons = useMemo(() => {
    return areas.map((a) => ({
      id: a.area_id,
      center: [a.center_latitude, a.center_longitude] as LatLng,
      rings: a.polygon ? wktToPolygonRings(a.polygon) : [],
    }));
  }, [areas]);

  const infraLines = useMemo(() => {
    if (!showInfrastructure) return [];
    return buildOstseeInfraMapLines(infrastruktur, pipelines);
  }, [infrastruktur, pipelines, showInfrastructure]);

  return (
    <div className="map-wrap">
      <MapContainer center={center} zoom={6} style={{ height: "100%", width: "100%" }}>
        <BasemapTileLayer />

        {showAreas &&
          areaPolygons.map((a) => {
            const meta = areas.find((x) => x.area_id === a.id);
            const style = meta ? areaStyle(meta) : { color: "#E65100", fillColor: "#FF6F00" };
            return a.rings.length > 0 ? (
              <Fragment key={`poly-${a.id}`}>
                <Polygon
                  positions={a.rings}
                  pathOptions={{
                    color: style.color,
                    fillColor: style.fillColor,
                    fillOpacity: 0.14,
                    weight: 2,
                  }}
                >
                  <Tooltip sticky>
                    <span className="mono">
                      <strong>{a.id}</strong>
                      {meta?.area_type ? (
                        <>
                          <br />
                          {meta.country ?? "—"} · {meta.area_type}
                        </>
                      ) : null}
                      <br />
                      EMODnet Tier A
                    </span>
                  </Tooltip>
                </Polygon>
              </Fragment>
            ) : null;
          })}

        {showInfrastructure &&
          infraLines.map((item) =>
            item.lines.map((positions, idx) => (
              <Polyline
                key={`${item.key}-${idx}`}
                positions={positions}
                pathOptions={item.style}
              >
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

        {showAreas &&
          areas.map((a) => {
            const style = areaStyle(a);
            return (
              <CircleMarker
                key={a.area_id}
                center={[a.center_latitude, a.center_longitude]}
                radius={7}
                pathOptions={{
                  color: style.color,
                  fillOpacity: 0.25,
                  weight: 1,
                  fillColor: style.fillColor,
                }}
              >
                <Tooltip>{a.area_id}</Tooltip>
              </CircleMarker>
            );
          })}

        {germanFleet.map((u) => (
          <CircleMarker
            key={`de-${u.mmsi}-${u.event_timestamp}`}
            center={[u.latitude, u.longitude]}
            radius={10}
            pathOptions={{
              color: "#0D47A1",
              fillColor: "#42A5F5",
              fillOpacity: 0.9,
              weight: 2,
            }}
          >
            <Tooltip>
              <span className="mono">
                <strong>DEU · {u.unit_name ?? u.mmsi}</strong>
                <br />
                {u.unit_class}
                {u.operational_status ? ` · ${u.operational_status}` : ""}
                <br />
                SOG {Number(u.speed ?? 0).toFixed(1)} kn
              </span>
            </Tooltip>
          </CircleMarker>
        ))}
        {russianFleet.map((u) => (
          <CircleMarker
            key={`ru-${u.mmsi}-${u.event_timestamp}`}
            center={[u.latitude, u.longitude]}
            radius={10}
            pathOptions={{
              color: "#4A148C",
              fillColor: "#AB47BC",
              fillOpacity: 0.92,
              weight: 2,
            }}
          >
            <Tooltip>
              <span className="mono">
                <strong>RUS · {u.unit_name ?? u.mmsi}</strong>
                <br />
                {u.unit_class}
                {u.home_base ? ` · ${u.home_base}` : ""}
                <br />
                SOG {Number(u.speed ?? 0).toFixed(1)} kn
              </span>
            </Tooltip>
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
        {showNba &&
          nbaIncidents.map((inc) => {
            const flash = (inc.priority ?? "").toUpperCase() === "FLASH";
            const color = flash ? "#D32F2F" : "#7E57C2";
            return (
              <CircleMarker
                key={`nba-${inc.flow_uuid}`}
                center={[inc.latitude, inc.longitude]}
                radius={11}
                pathOptions={{
                  color: "#1A0A2E",
                  fillColor: color,
                  fillOpacity: 0.95,
                  weight: 2,
                }}
                eventHandlers={{
                  click: () => onNbaIncidentClick?.(inc.flow_uuid),
                }}
              >
                <Tooltip>
                  <span className="mono">
                    <strong>NBA · {inc.theatre ?? inc.alert_type ?? "incident"}</strong>
                    <br />
                    {inc.priority ?? "—"} · COA {inc.best_coa_id ?? "—"}
                    <br />
                    {inc.flow_uuid.slice(0, 8)}…
                  </span>
                </Tooltip>
              </CircleMarker>
            );
          })}
        {showGpsJammer ? <GpsJammerGeohashLayer events={gpsJammerEvents} /> : null}
        {outliers.map((o) => {
          const sev = Number(o.severity_level ?? 50);
          const color = sev >= 75 ? "#FF5252" : sev >= 55 ? "#FF9800" : "#FFB74D";
          return (
            <CircleMarker
              key={`outlier-${o.mmsi}-${o.event_timestamp}-${o.outlier_type}`}
              center={[o.latitude, o.longitude]}
              radius={9}
              pathOptions={{
                color: "#212121",
                fillColor: color,
                fillOpacity: 0.92,
                weight: 2,
              }}
            >
              <Tooltip>
                <span className="mono">
                  <strong>{o.outlier_type ?? "Anomaly"}</strong>
                  <br />
                  MMSI {o.mmsi}
                  {o.harbour_name ? ` · ${o.harbour_name}` : ""}
                  <br />
                  {(o.reason ?? "").slice(0, 120)}
                </span>
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
      {(showInfrastructure || showAreas || showGpsJammer) && (
        <div className="map-legend map-legend-infra" aria-hidden>
          <span className="map-legend-item">
            <i className="map-legend-swatch" style={{ background: "#FF6F00" }} /> Pipeline / Gas
          </span>
          <span className="map-legend-item">
            <i className="map-legend-swatch" style={{ background: "#26C6DA" }} /> Seekabel
          </span>
          {showGpsJammer ? (
            <span className="map-legend-item">
              <i className="map-legend-swatch" style={{ background: "#E040FB" }} /> GPS jammer (geohash)
            </span>
          ) : null}
          <span className="map-legend-item">
            <i
              className="map-legend-swatch map-legend-swatch-area"
              style={{ background: "rgba(255, 213, 79, 0.45)" }}
            />{" "}
            Tier A military / defence zones
          </span>
        </div>
      )}
    </div>
  );
}