/** Sample payloads when Impala is unreachable (Baltic Sea demo). */

import buoyAnalyticsPanels from "./data/buoy-analytics-sample.json" with { type: "json" };
import rusFleetAnalyticsPanels from "./data/rus-fleet-analytics-sample.json" with { type: "json" };

export const meta = {
  source: "sample",
  schema: "defense",
  message: "Impala unavailable — showing embedded demo data",
};

export const tables = [
  "ais_events_ice",
  "ships",
  "sanctioned_vessels",
  "german_navy_fleet",
  "marine_vessel_status",
  "baltic_sea_harbours",
  "buoy_data",
  "observation_areas",
  "ostsee_infrastruktur",
  "ostsee_pipelines",
  "area_violation",
  "maritime_surveillance_reports",
  "social_media_messages",
  "lagebild",
  "lidar_simulation_metadata",
  "das_cable_spectrogram_metadata",
];

/** Demo LiDAR scenes (matches NiFi LidarSimulator semantics). */
export const lidarScans = [
  {
    filename: "baltic_corvette_1042.las",
    vessel_name: "FGS Braunschweig (sim)",
    vessel_type: "corvette",
    classification: 5,
    center_lat: 54.52,
    center_lon: 18.54,
    heading_deg: 285,
    point_count: 42850,
    hadoop_file_url: "s3a://goes-se-sandbox/data/geospatial/transformer/baltic_corvette_1042.las",
    predicted_vessel_type: "corvette",
    model_confidence: 0.94,
    ingested_at: "2026-11-17T08:12:00Z",
  },
  {
    filename: "baltic_frigate_2201.las",
    vessel_name: "FGS Sachsen (sim)",
    vessel_type: "frigate",
    classification: 6,
    center_lat: 55.97,
    center_lon: 17.78,
    heading_deg: 190,
    point_count: 61200,
    hadoop_file_url: "s3a://goes-se-sandbox/data/geospatial/transformer/baltic_frigate_2201.las",
    predicted_vessel_type: "frigate",
    model_confidence: 0.91,
    ingested_at: "2026-11-17T07:45:00Z",
  },
  {
    filename: "baltic_submarine_881.las",
    vessel_name: "Contact Sierra (sim)",
    vessel_type: "submarine",
    classification: 7,
    center_lat: 56.65,
    center_lon: 19.71,
    heading_deg: 45,
    point_count: 18900,
    hadoop_file_url: "s3a://goes-se-sandbox/data/geospatial/transformer/baltic_submarine_881.las",
    predicted_vessel_type: "submarine",
    model_confidence: 0.88,
    ingested_at: "2026-11-17T06:30:00Z",
  },
  {
    filename: "baltic_tanker_3390.las",
    vessel_name: "UNCLASSIFIED",
    vessel_type: "oil_tanker",
    classification: 3,
    center_lat: 55.38,
    center_lon: 18.79,
    heading_deg: 120,
    point_count: 95400,
    hadoop_file_url: "s3a://goes-se-sandbox/data/geospatial/transformer/baltic_tanker_3390.las",
    predicted_vessel_type: "oil_tanker",
    model_confidence: 0.86,
    ingested_at: "2026-11-17T05:15:00Z",
  },
  {
    filename: "baltic_unclassified_000.las",
    vessel_name: "UNCLASSIFIED",
    vessel_type: "undefined",
    classification: 0,
    center_lat: 54.71,
    center_lon: 20.45,
    heading_deg: 75,
    point_count: 22100,
    hadoop_file_url: "s3a://goes-se-sandbox/data/geospatial/transformer/baltic_unclassified_000.las",
    predicted_vessel_type: null,
    model_confidence: null,
    ingested_at: "2026-11-17T04:02:00Z",
  },
];

/** Demo DAS cable spectrogram segments (NiFi DasCableAcousticSimulator). */
export const dasCableSegments = [
  {
    hadoop_file_url: "s3a://goes-se-sandbox/data/acoustic/das_balticconnector_6seg.h5",
    segment_index: 2,
    cable_asset: "BALTICCONNECTOR",
    cable_name: "Balticconnector",
    chainage_m: 84200,
    segment_lat: 59.78,
    segment_lon: 20.12,
    threat_class: "anchor_drag",
    predicted_threat_type: "anchor_drag",
    model_confidence: 0.91,
    channel_count: 48,
    window_seconds: 4,
    fs_hz: 2000,
    filename: "das_balticconnector_6seg.h5",
    ingested_at: "2026-11-21T09:30:00Z",
  },
  {
    hadoop_file_url: "s3a://goes-se-sandbox/data/acoustic/das_nord_stream_1_6seg.h5",
    segment_index: 4,
    cable_asset: "NORD_STREAM_1",
    cable_name: "Nord Stream 1",
    chainage_m: 156400,
    segment_lat: 55.62,
    segment_lon: 14.88,
    threat_class: "cui_sabotage_tap",
    predicted_threat_type: "cui_sabotage_tap",
    model_confidence: 0.88,
    channel_count: 48,
    window_seconds: 4,
    fs_hz: 2000,
    filename: "das_nord_stream_1_6seg.h5",
    ingested_at: "2026-11-21T08:55:00Z",
  },
  {
    hadoop_file_url: "s3a://goes-se-sandbox/data/acoustic/das_pcable_bsh_1_6seg.h5",
    segment_index: 1,
    cable_asset: "PCABLE_BSH_1",
    cable_name: "Research cable BSH-1",
    chainage_m: 12400,
    segment_lat: 54.41,
    segment_lon: 12.02,
    threat_class: "trawler_scrape",
    predicted_threat_type: "trawler_scrape",
    model_confidence: 0.76,
    channel_count: 48,
    window_seconds: 4,
    fs_hz: 2000,
    filename: "das_pcable_bsh_1_6seg.h5",
    ingested_at: "2026-11-21T07:40:00Z",
  },
  {
    hadoop_file_url: "s3a://goes-se-sandbox/data/acoustic/das_balticconnector_6seg.h5",
    segment_index: 0,
    cable_asset: "BALTICCONNECTOR",
    cable_name: "Balticconnector",
    chainage_m: 2100,
    segment_lat: 59.45,
    segment_lon: 21.8,
    threat_class: "vessel_passage",
    predicted_threat_type: "vessel_passage",
    model_confidence: 0.82,
    channel_count: 48,
    window_seconds: 4,
    fs_hz: 2000,
    filename: "das_balticconnector_6seg.h5",
    ingested_at: "2026-11-21T06:15:00Z",
  },
];

export const germanFleetPositions = [
  {
    mmsi: "MAR123411",
    unit_name: "FGS Braunschweig",
    unit_class: "Korvetten",
    latitude: 54.32,
    longitude: 10.12,
    speed: 12.5,
    course: 95,
    status: "Underway",
    operational_status: "Patrol",
    event_timestamp: "2026-11-17T18:00:00Z",
  },
  {
    mmsi: "MAR123404",
    unit_name: "FGS Sachsen",
    unit_class: "Fregatten",
    latitude: 54.09,
    longitude: 12.14,
    speed: 8.0,
    course: 210,
    status: "Underway",
    operational_status: "Exercise",
    event_timestamp: "2026-11-17T17:30:00Z",
  },
];

export const russianFleetPositions = [
  {
    mmsi: "RUS273842",
    unit_name: "Neustrashimy",
    unit_class: "Frigate",
    home_base: "Baltiysk",
    vessel_category: "Surface Combatant",
    latitude: 54.64,
    longitude: 19.82,
    speed: 6.2,
    course: 45,
    status: "Underway",
    event_timestamp: "2026-11-17T17:00:00Z",
  },
  {
    mmsi: "RUS273901",
    unit_name: "Ivan Gren",
    unit_class: "Landing Ship",
    home_base: "Kronstadt",
    vessel_category: "Amphibious",
    latitude: 59.99,
    longitude: 29.77,
    speed: 4.1,
    course: 180,
    status: "Moored",
    event_timestamp: "2026-11-17T16:45:00Z",
  },
];

export const gpsJammerEvents = [
  {
    geohash: "u4pruy",
    event_timestamp: "2026-11-22T15:12:00Z",
    latitude: 54.71,
    longitude: 20.48,
    adsb_nic: 2,
    signal_integrity: 0.18,
    jamming_indicator: true,
    event_type: "gps_jammer_event",
    reason: "Kaliningrad sector · NIC<5",
  },
  {
    geohash: "ud9k8h",
    event_timestamp: "2026-11-22T14:40:00Z",
    latitude: 59.93,
    longitude: 30.25,
    adsb_nic: 3,
    signal_integrity: 0.25,
    jamming_indicator: true,
    event_type: "gps_jammer_event",
    reason: "St Petersburg EW envelope",
  },
  {
    geohash: "u6tme5",
    event_timestamp: "2026-11-22T13:05:00Z",
    latitude: 57.18,
    longitude: 18.52,
    adsb_nic: 4,
    signal_integrity: 0.31,
    jamming_indicator: true,
    event_type: "gps_jammer_event",
    reason: "Gotland mobile jammer pattern",
  },
  {
    geohash: "u37q8m",
    event_timestamp: "2026-11-22T12:22:00Z",
    latitude: 54.35,
    longitude: 13.08,
    adsb_nic: 5,
    signal_integrity: 0.42,
    jamming_indicator: false,
    event_type: "gps_jammer_event",
    reason: "Marginal integrity — monitor",
  },
];

export const vesselOutliers = [
  {
    mmsi: 211234567,
    event_timestamp: "2026-11-17T18:12:00Z",
    latitude: 54.71,
    longitude: 13.08,
    speed: 0.8,
    course: 182,
    status: "Under way",
    outlier_type: "Rendezvous",
    reason: "STS candidate: 320 m from MMSI 211234568, both SOG < 4 kn",
    severity_level: 60,
  },
  {
    mmsi: 273445566,
    event_timestamp: "2026-11-17T17:40:00Z",
    latitude: 55.38,
    longitude: 18.79,
    speed: 22.1,
    course: 91,
    status: "Under way",
    outlier_type: "MMSI Teleportation",
    reason: "Implied hop speed 89 kn over 1.2 nm in 3.0 min",
    severity_level: 80,
  },
  {
    mmsi: 636022401,
    event_timestamp: "2026-11-17T16:05:00Z",
    latitude: 55.97,
    longitude: 17.78,
    speed: 12.0,
    course: 210,
    status: "Moored",
    outlier_type: "Misreported Status",
    reason: 'Status "Moored" with SOG 12.0 kn',
    severity_level: 55,
  },
];

function demoTrackPoints(lat, lon, n = 8, dLat = 0.08, dLon = 0.12) {
  const rows = [];
  for (let i = 0; i < n; i += 1) {
    rows.push({
      latitude: lat + dLat * i * 0.35,
      longitude: lon + dLon * Math.sin(i * 0.7) * 0.4 + dLon * i * 0.15,
    });
  }
  return rows;
}

/** Multi-point histories for `/api/position-tracks` sample mode. */
export function samplePositionTrackRows() {
  const rows = [];
  const pushSeries = (mmsi, meta, baseLat, baseLon, kind, tsBase) => {
    demoTrackPoints(baseLat, baseLon).forEach((p, i) => {
      rows.push({
        mmsi,
        ...meta,
        latitude: p.latitude,
        longitude: p.longitude,
        speed: 8 + i,
        course: 90 + i * 10,
        event_timestamp: new Date(Date.parse(tsBase) + i * 3600000).toISOString(),
        track_kind: kind,
      });
    });
  };
  pushSeries(
    123456007,
    { ship_name: "Blue Horizon", vessel_type: "Container", flag: "Singapore" },
    54.2,
    17.8,
    "ais",
    "2026-11-17T10:00:00Z"
  );
  pushSeries(
    636022401,
    { ship_name: "HS ATLANTICA", vessel_type: "Crude Oil Tanker", flag: "Liberia" },
    55.5,
    17.2,
    "ais",
    "2026-11-17T11:00:00Z"
  );
  pushSeries(
    "MAR123411",
    { ship_name: "FGS Braunschweig", vessel_type: "Korvetten", flag: "Germany" },
    54.0,
    10.5,
    "deu_fleet",
    "2026-11-17T12:00:00Z"
  );
  pushSeries(
    "MAR123404",
    { ship_name: "FGS Sachsen", vessel_type: "Fregatten", flag: "Germany" },
    54.2,
    11.8,
    "deu_fleet",
    "2026-11-17T12:30:00Z"
  );
  pushSeries(
    "RUS273842",
    { ship_name: "Neustrashimy", vessel_type: "Frigate", flag: "Russia" },
    54.5,
    19.2,
    "rus_fleet",
    "2026-11-17T13:00:00Z"
  );
  pushSeries(
    "RUS273901",
    { ship_name: "Ivan Gren", vessel_type: "Landing Ship", flag: "Russia" },
    59.5,
    28.9,
    "rus_fleet",
    "2026-11-17T13:30:00Z"
  );
  return rows;
}

export const aisShips = [
  { mmsi: 123456007, ship_name: "Blue Horizon", vessel_type: "Container", flag: "Singapore", latitude: 54.52, longitude: 18.54, speed: 12.4, course: 285, event_timestamp: "2025-10-15T08:12:00Z", status: "Underway using engine", destination: "Gdańsk" },
  { mmsi: 123456014, ship_name: "Maritime Queen", vessel_type: "Naval", flag: "United States", latitude: 54.09, longitude: 12.14, speed: 8.1, course: 45, event_timestamp: "2025-10-15T08:10:00Z", status: "Underway using engine", destination: "Kiel" },
  { mmsi: 123456040, ship_name: "Sea Eagle", vessel_type: "Tug", flag: "Germany", latitude: 54.32, longitude: 10.12, speed: 5.2, course: 120, event_timestamp: "2025-10-15T08:08:00Z", status: "Underway using engine", destination: "Rostock" },
  { mmsi: 636022401, ship_name: "HS ATLANTICA", vessel_type: "Crude Oil Tanker", flag: "Liberia", latitude: 55.97, longitude: 17.78, speed: 9.8, course: 190, event_timestamp: "2025-10-15T08:05:00Z", status: "Underway using engine", destination: "Primorsk", sanctioned: true },
  { mmsi: 123456028, ship_name: "Polaris", vessel_type: "Passenger", flag: "Sweden", latitude: 59.33, longitude: 18.07, speed: 14.0, course: 210, event_timestamp: "2025-10-15T08:03:00Z", status: "Underway using engine", destination: "Helsinki" },
  { mmsi: 123456031, ship_name: "Island Hopper", vessel_type: "Ferry", flag: "Greece", latitude: 55.70, longitude: 21.14, speed: 11.3, course: 340, event_timestamp: "2025-10-15T08:01:00Z", status: "Underway using engine", destination: "Klaipėda" },
  { mmsi: 123456012, ship_name: "Storm Breaker", vessel_type: "Salvage", flag: "Germany", latitude: 54.71, longitude: 20.45, speed: 0.3, course: 75, event_timestamp: "2025-10-15T07:58:00Z", status: "Moored", destination: "Gdańsk" },
  { mmsi: 123456041, ship_name: "Ocean Dream", vessel_type: "Container", flag: "United States", latitude: 56.95, longitude: 24.11, speed: 13.1, course: 160, event_timestamp: "2025-10-15T07:55:00Z", status: "Underway using engine", destination: "Riga" },
];

export const fleet = [
  { class: "Fregatten", name: "FGS Brandenburg", hull_number: "F215", year_commissioned: 1994, crew_size: 118 },
  { class: "Fregatten", name: "FGS Sachsen", hull_number: "F219", year_commissioned: 2004, crew_size: 230 },
  { class: "Korvetten", name: "FGS Braunschweig", hull_number: "F260", year_commissioned: 2008, crew_size: 65 },
  { class: "U-Boote", name: "FGS U36", hull_number: "S186", year_commissioned: 2016, crew_size: 22 },
  { class: "Minenjäger", name: "FGS Dillingen", hull_number: "M1065", year_commissioned: 1990, crew_size: 38 },
];

export const sanctions = [
  { Name: "HS ATLANTICA", MMSI: 636022401, Type: "Crude Oil Tanker", Flag: "Liberia", Sanction_Reason: "Blocked under E.O. 14024", Linked_To: "Hennesea Holdings Limited" },
  { Name: "Andromeda Star", MMSI: 273800007, IMO: 9402471, Type: "Crude Oil Tanker", Flag: "Russia", Sanction_Reason: "EU Annex XIII / irregular shipping practices", Linked_To: "EU" },
  { Name: "Lady R", MMSI: 273800012, IMO: 9161003, Type: "Crude Oil Tanker", Flag: "Russia", Sanction_Reason: "Defense sector goods transport", Linked_To: "EU" },
  { Name: "ARISTO", MMSI: 123456011, Type: "Chemical/Products Tanker", Flag: "Liberia", Sanction_Reason: "Blocked under E.O. 14024", Linked_To: "Hennesea Holdings Limited" },
];

/** Synthetic AIS destination reports for sanctioned MMSI (offline analytics). */
export const sanctionedAisDestHistory = [
  { mmsi: 636022401, destination: "Primorsk" },
  { mmsi: 636022401, destination: "Primorsk" },
  { mmsi: 636022401, destination: "Gdańsk" },
  { mmsi: 636022401, destination: "Gdańsk" },
  { mmsi: 636022401, destination: "Klaipėda" },
  { mmsi: 123456011, destination: "Rotterdam" },
  { mmsi: 123456011, destination: "Gdańsk" },
  { mmsi: 123456011, destination: "Gdańsk" },
];

export const areaViolations = [
  { mmsi: 636022401, area_id: "DE-Firing-Bornholm-Bight", flag: "Liberia", violation_time: "2025-10-14T22:15:00Z", ship_name: "HS ATLANTICA" },
  { mmsi: 123456029, area_id: "PL-Defence-Gdansk-Bight", flag: "Russia", violation_time: "2025-10-14T19:40:00Z", ship_name: "Crystal River" },
  { mmsi: 123456033, area_id: "SE-Firing-Gotland-East", flag: "Russia", violation_time: "2025-10-14T16:05:00Z", ship_name: "Mighty Ocean" },
];

/** AIS points inside Tier-A zones (offline map + analytics). */
export const restrictedRouteTracks = [
  {
    mmsi: 636022401,
    ship_name: "HS ATLANTICA",
    vessel_type: "Crude Oil Tanker",
    flag: "Liberia",
    area_id: "DE-Firing-Bornholm-Bight",
    latitude: 54.68,
    longitude: 18.42,
    event_timestamp: "2025-10-14T21:10:00Z",
  },
  {
    mmsi: 636022401,
    ship_name: "HS ATLANTICA",
    vessel_type: "Crude Oil Tanker",
    flag: "Liberia",
    area_id: "DE-Firing-Bornholm-Bight",
    latitude: 54.69,
    longitude: 18.48,
    event_timestamp: "2025-10-14T21:40:00Z",
  },
  {
    mmsi: 636022401,
    ship_name: "HS ATLANTICA",
    vessel_type: "Crude Oil Tanker",
    flag: "Liberia",
    area_id: "DE-Firing-Bornholm-Bight",
    latitude: 54.71,
    longitude: 18.52,
    event_timestamp: "2025-10-14T22:15:00Z",
  },
  {
    mmsi: 123456029,
    ship_name: "Crystal River",
    vessel_type: "Container",
    flag: "Russia",
    area_id: "PL-Defence-Gdansk-Bight",
    latitude: 54.42,
    longitude: 18.72,
    event_timestamp: "2025-10-14T19:00:00Z",
  },
  {
    mmsi: 123456029,
    ship_name: "Crystal River",
    vessel_type: "Container",
    flag: "Russia",
    area_id: "PL-Defence-Gdansk-Bight",
    latitude: 54.44,
    longitude: 18.78,
    event_timestamp: "2025-10-14T19:40:00Z",
  },
  {
    mmsi: 123456033,
    ship_name: "Mighty Ocean",
    vessel_type: "Tanker",
    flag: "Russia",
    area_id: "SE-Firing-Gotland-East",
    latitude: 57.18,
    longitude: 19.02,
    event_timestamp: "2025-10-14T15:30:00Z",
  },
  {
    mmsi: 123456033,
    ship_name: "Mighty Ocean",
    vessel_type: "Tanker",
    flag: "Russia",
    area_id: "SE-Firing-Gotland-East",
    latitude: 57.21,
    longitude: 19.08,
    event_timestamp: "2025-10-14T16:05:00Z",
  },
  {
    mmsi: 123456028,
    ship_name: "Polaris",
    vessel_type: "Passenger",
    flag: "Sweden",
    area_id: "DK-Firing-Kattegat",
    latitude: 58.12,
    longitude: 10.38,
    event_timestamp: "2025-10-14T14:00:00Z",
  },
  {
    mmsi: 123456028,
    ship_name: "Polaris",
    vessel_type: "Passenger",
    flag: "Sweden",
    area_id: "DK-Firing-Kattegat",
    latitude: 58.16,
    longitude: 10.46,
    event_timestamp: "2025-10-14T14:30:00Z",
  },
];

export const surveillance = [
  { message_id: "SR-2025-8841", class: "SURFACE", name: "Contact Alpha", message_timestamp: "2025-10-15T07:45:00Z", message_latitude: 55.02, message_longitude: 17.44 },
  { message_id: "SR-2025-8840", class: "SUBSURFACE", name: "Contact Bravo", message_timestamp: "2025-10-15T06:30:00Z", message_latitude: 56.65, message_longitude: 19.71 },
  { message_id: "SR-2025-8838", class: "AIR", name: "Track Charlie", message_timestamp: "2025-10-15T05:12:00Z", message_latitude: 54.35, message_longitude: 13.08 },
];

export const observationAreas = [
  {
    area_id: 'DK-Firing-Kattegat',
    country: 'Denmark',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 58.244337,
    center_longitude: 10.532022,
    polygon: 'POLYGON ((10.49869315 58.04946684, 10.2153316 58.3661459, 10.4986758 58.49948646, 10.88205235 58.04947026, 10.49869315 58.04946684))',
  },
  {
    area_id: 'DK-Firing-Skagerrak',
    country: 'Denmark',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 57.209163,
    center_longitude: 9.423227,
    polygon: 'POLYGON ((9.61534508 57.18276287, 9.53201035 57.11609303, 9.34866823 57.11609119, 9.24032727 57.1827591, 9.24032532 57.24109556, 9.39866234 57.31609659, 9.49866744 57.31609758, 9.61534508 57.18276287))',
  },
  {
    area_id: 'EE-Defence-West-Estonia-24.3',
    country: 'Estonia',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 59.589846,
    center_longitude: 24.324246,
    polygon: 'POLYGON ((24.1999995 59.62844144, 24.28833293 59.64999998, 24.44166705 59.64999996, 24.44253504 59.53333396, 24.20000014 59.53333299, 24.1999995 59.62844144))',
  },
  {
    area_id: 'FI-Defence-Helsinki-Approaches-24.2',
    country: 'Finland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 59.894373,
    center_longitude: 24.244816,
    polygon: 'POLYGON ((24.32780578 59.93760706, 24.35052746 59.92761237, 24.39514207 59.93798387, 24.440017 59.81177521, 24.08314521 59.77020252, 24.10748484 59.90306585, 24.00088921 59.92852552, 24.35922662 60.04672389, 24.38739852 60.01675102, 24.32780578 59.93760706))',
  },
  {
    area_id: 'FI-Defence-Helsinki-Approaches-24.8',
    country: 'Finland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 60.132663,
    center_longitude: 24.780723,
    polygon: 'POLYGON ((24.77655359 60.13488342, 24.79405615 60.13238383, 24.77156072 60.13072304, 24.77655359 60.13488342))',
  },
  {
    area_id: 'FI-Defence-Helsinki-Approaches-24.9',
    country: 'Finland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 60.099488,
    center_longitude: 24.913915,
    polygon: 'POLYGON ((24.90433281 60.09739061, 24.91656962 60.10405741, 24.9235089 60.1026671, 24.91211412 60.09461359, 24.90433281 60.09739061))',
  },
  {
    area_id: 'FI-Defence-Helsinki-Approaches-25.0',
    country: 'Finland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 60.139179,
    center_longitude: 25.036743,
    polygon: 'POLYGON ((25.08824168 60.15017469, 25.06078341 60.12412515, 24.9871583 60.1246151, 24.99405947 60.14367294, 25.04918214 60.15808036, 25.08824168 60.15017469))',
  },
  {
    area_id: 'FI-Defence-Helsinki-Approaches-25.1',
    country: 'Finland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 60.040248,
    center_longitude: 25.07008,
    polygon: 'POLYGON ((24.99623325 59.97705018, 25.02841691 60.0780288, 25.00377424 60.1042976, 25.0105273 60.11099832, 25.07892312 60.11143437, 25.12811078 60.09125666, 25.13444965 59.97907647, 24.99623325 59.97705018))',
  },
  {
    area_id: 'DE-Firing-Bay-Kiel',
    country: 'Germany',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 54.994677,
    center_longitude: 13.859742,
    polygon: 'POLYGON ((13.6638317 55.08349735, 14.01154432 55.07507442, 14.10434318 54.91666771, 13.66319123 54.91660231, 13.6638317 55.08349735))',
  },
  {
    area_id: 'DE-Firing-Bornholm-Bight',
    country: 'Germany',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 55.208644,
    center_longitude: 15.541589,
    polygon: 'POLYGON ((15.24968165 55.50025778, 15.8335062 55.50032123, 15.83319975 54.91690263, 15.24994627 54.91690263, 15.24968165 55.50025778))',
  },
  {
    area_id: 'DE-Firing-Ostsee-Central',
    country: 'Germany',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 54.534106,
    center_longitude: 14.137062,
    polygon: 'POLYGON ((13.85830781 54.63054244, 13.95713398 54.66653683, 14.33556346 54.66679063, 14.28407192 54.33299984, 14.13368999 54.33347574, 14.1327382 54.44959343, 13.99948839 54.45054522, 13.85830781 54.63054244))',
  },
  {
    area_id: 'DE-Firing-Putlos-Todendorf',
    country: 'Germany',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 54.432617,
    center_longitude: 10.678147,
    polygon: 'POLYGON ((10.69445155 54.30651862, 10.63040295 54.35148339, 10.54585243 54.37770264, 10.46612971 54.45316955, 10.45185295 54.49143128, 10.45927687 54.50799233, 10.59976024 54.54339871, 10.72425364 54.53540372, 10.82933064 54.485416, 10.87158987 54.44810605, 10.866293 54.35444186, 10.76969178 54.30722723, 10.69445155 54.30651862))',
  },
  {
    area_id: 'PL-Defence-Gdansk-Bight',
    country: 'Poland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 54.512213,
    center_longitude: 19.232221,
    polygon: 'POLYGON ((19.35733408 54.39849968, 19.23433904 54.39861978, 19.02503061 54.43545825, 19.21399981 54.68183331, 19.3854994 54.56516679, 19.38233379 54.4651664, 19.35733408 54.39849968))',
  },
  {
    area_id: 'PL-Defence-Slupsk-Bank',
    country: 'Poland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 54.631922,
    center_longitude: 16.525385,
    polygon: 'POLYGON ((16.77266667 54.78383333, 16.84519072 54.58727085, 16.53554276 54.54221936, 16.42737037 54.48318987, 16.14616667 54.59, 16.2825 54.67916667, 16.77266667 54.78383333))',
  },
  {
    area_id: 'PL-Defence-Swinoujscie',
    country: 'Poland',
    area_type: 'National Defence Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 54.074925,
    center_longitude: 14.811528,
    polygon: 'POLYGON ((14.88270499 54.0523473, 14.77733219 54.03293851, 14.73966667 54.09516667, 14.84466667 54.1185, 14.88270499 54.0523473))',
  },
  {
    area_id: 'SE-Firing-Gotland-East',
    country: 'Sweden',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 57.576255,
    center_longitude: 18.021295,
    polygon: 'POLYGON ((18.12972122 57.48305447, 17.83805508 57.45805456, 17.84638824 57.59110961, 17.95472139 57.68277661, 18.1816665 57.70138754, 18.18166637 57.58999849, 18.20888836 57.57499848, 18.20194333 57.54944247, 18.12972122 57.48305447))',
  },
  {
    area_id: 'SE-Firing-Southern-Baltic',
    country: 'Sweden',
    area_type: 'Firing Area',
    tier: 'A',
    source: 'EMODnet emodnet:militaryareaspoly (Tier A curated)',
    center_latitude: 56.015113,
    center_longitude: 15.557762,
    polygon: 'POLYGON ((15.72210924 56.05519209, 15.69637539 55.92843437, 15.41258279 55.92820641, 15.4116277 56.08809496, 15.45409633 56.08958014, 15.48827017 56.11925844, 15.51739508 56.11811134, 15.55028366 56.09743276, 15.60981683 56.11764533, 15.63629572 56.10752491, 15.65110423 56.09487156, 15.64514435 56.08715104, 15.72210924 56.05519209))',
  },
];

export const ostseeInfrastruktur = [
  {
    "id": "EMODNET_NS1",
    "name": "NORD STREAM 1",
    "typ": "Gaspipeline",
    "geometrie_wkt": "LINESTRING (23.99979769 59.65753773, 22.52826538 59.45687113, 21.09415781 59.20373063, 21.05036577 59.14071078, 20.79176628 59.00212766, 20.52227967 58.8899604, 20.35897759 58.75928755, 20.33513795 58.70332492, 20.28324883 58.66143174, 20.22280991 58.57277674, 20.16576533 58.45778949, 20.09268027 58.41262821, 19.77499176 57.92896757, 19.65590183 57.7961544, 19.48553558 57.52118732, 19.42811943 57.4765614, 18.82467552 56.55030537, 18.57729168 56.34578188, 18.10737188 56.24843544, 16.4697971 55.66691842, 15.70063987 55.53579754, 15.42628948 55.38311764, 15.36550871 55.21530088, 15.21641767 54.93645624, 14.79831532 54.82518868, 14.49431612 54.60762594, 14.21395931 54.54142768, 14.08479726 54.5332317, 13.71688492 54.24308183, 13.70184897 54.21301225, 13.63614605 54.19030182, 13.63106352 54.1715519, 13.65426161 54.15419366)",
    "status": "In use",
    "medium": "Gas"
  },
  {
    "id": "EMODNET_BALTICCONNECTOR",
    "name": "Balticconnector",
    "typ": "Gaspipeline",
    "geometrie_wkt": "LINESTRING (24.08999047 59.95196291, 24.10611111 59.90552432, 24.03913381 59.86896759, 24.03472785 59.77162482, 24.12802207 59.50136118, 24.13429474 59.40054982)",
    "status": "In use",
    "medium": "Gas"
  },
  {
    "id": "EMODNET_NS2",
    "name": "NORD STREAM 2",
    "typ": "Gaspipeline",
    "geometrie_wkt": "LINESTRING (28.14227953 60.52449418, 28.08942727 60.38930494, 27.88258696 60.2229997, 27.51118065 60.1059404, 26.75406533 60.06940664, 25.89021281 59.84955768, 24.95805292 59.83262487, 23.91754305 59.59039612, 21.2288291 59.17345674, 20.42120071 58.73807712, 20.22016814 58.42254669, 18.9172008 56.58500187, 18.6313395 56.31518911, 18.09941614 56.18138567, 16.39890865 55.60140081, 15.77565632 55.50692414, 15.51506379 55.38733518, 15.28232415 54.90765986, 14.85696937 54.79483342, 14.55184069 54.5663022, 14.11857926 54.49406361, 13.72166074 54.2354678, 13.71325424 54.20953787, 13.63155895 54.18466936, 13.66381217 54.15715816)",
    "status": "In use",
    "medium": "Gas"
  },
  {
    "id": "EMODNET_PCABLE_BSH_1",
    "name": "Offshore power cable (BSH/CONTIS) - segment 1",
    "typ": "Seekabel",
    "geometrie_wkt": "LINESTRING (10.90364984 53.95650018, 10.9191502 53.97384028, 11.27548958 54.12651006, 11.71548958 54.29168972, 12.08401006 54.29203018, 12.22384028 54.3916996, 12.36051024 54.52486974, 12.6071802 54.67171038, 12.6408584 54.75531389)",
    "status": "In use",
    "medium": "Power"
  },
  {
    "id": "EMODNET_PCABLE_BSH_2",
    "name": "Offshore power cable (BSH/CONTIS) - segment 2",
    "typ": "Seekabel",
    "geometrie_wkt": "LINESTRING (13.65334547 54.15833425, 13.6471388 54.16392291, 13.65039886 54.18535332, 13.72114047 54.209195, 13.72543203 54.2374458, 13.8521765 54.32908812, 13.83924288 54.34864351, 13.86474657 54.37118565, 13.8573013 54.38355352, 13.84767269 54.50691848, 13.82201351 54.61820671, 13.87355762 54.72281662, 13.93517153 54.75760984, 13.69526362 54.82014481, 13.68019275 54.81793704)",
    "status": "In use",
    "medium": "Power"
  }
];

export const ostseePipelines = [
  {
    "pipeline_name": "NORD STREAM 1",
    "medium": "Gas",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (23.99979769 59.65753773, 22.52826538 59.45687113, 21.09415781 59.20373063, 21.05036577 59.14071078, 20.79176628 59.00212766, 20.52227967 58.8899604, 20.35897759 58.75928755, 20.33513795 58.70332492, 20.28324883 58.66143174, 20.22280991 58.57277674, 20.16576533 58.45778949, 20.09268027 58.41262821, 19.77499176 57.92896757, 19.65590183 57.7961544, 19.48553558 57.52118732, 19.42811943 57.4765614, 18.82467552 56.55030537, 18.57729168 56.34578188, 18.10737188 56.24843544, 16.4697971 55.66691842, 15.70063987 55.53579754, 15.42628948 55.38311764, 15.36550871 55.21530088, 15.21641767 54.93645624, 14.79831532 54.82518868, 14.49431612 54.60762594, 14.21395931 54.54142768, 14.08479726 54.5332317, 13.71688492 54.24308183, 13.70184897 54.21301225, 13.63614605 54.19030182, 13.63106352 54.1715519, 13.65426161 54.15419366)",
    "quelle": "EMODnet"
  },
  {
    "pipeline_name": "Balticconnector",
    "medium": "Gas",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (24.08999047 59.95196291, 24.10611111 59.90552432, 24.03913381 59.86896759, 24.03472785 59.77162482, 24.12802207 59.50136118, 24.13429474 59.40054982)",
    "quelle": "EMODnet"
  },
  {
    "pipeline_name": "NORD STREAM 2",
    "medium": "Gas",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (28.14227953 60.52449418, 28.08942727 60.38930494, 27.88258696 60.2229997, 27.51118065 60.1059404, 26.75406533 60.06940664, 25.89021281 59.84955768, 24.95805292 59.83262487, 23.91754305 59.59039612, 21.2288291 59.17345674, 20.42120071 58.73807712, 20.22016814 58.42254669, 18.9172008 56.58500187, 18.6313395 56.31518911, 18.09941614 56.18138567, 16.39890865 55.60140081, 15.77565632 55.50692414, 15.51506379 55.38733518, 15.28232415 54.90765986, 14.85696937 54.79483342, 14.55184069 54.5663022, 14.11857926 54.49406361, 13.72166074 54.2354678, 13.71325424 54.20953787, 13.63155895 54.18466936, 13.66381217 54.15715816)",
    "quelle": "EMODnet"
  },
  {
    "pipeline_name": "Offshore power cable (BSH/CONTIS) - segment 1",
    "medium": "Power",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (10.90364984 53.95650018, 10.9191502 53.97384028, 11.27548958 54.12651006, 11.71548958 54.29168972, 12.08401006 54.29203018, 12.22384028 54.3916996, 12.36051024 54.52486974, 12.6071802 54.67171038, 12.6408584 54.75531389)",
    "quelle": "EMODnet"
  },
  {
    "pipeline_name": "Offshore power cable (BSH/CONTIS) - segment 2",
    "medium": "Power",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (13.65334547 54.15833425, 13.6471388 54.16392291, 13.65039886 54.18535332, 13.72114047 54.209195, 13.72543203 54.2374458, 13.8521765 54.32908812, 13.83924288 54.34864351, 13.86474657 54.37118565, 13.8573013 54.38355352, 13.84767269 54.50691848, 13.82201351 54.61820671, 13.87355762 54.72281662, 13.93517153 54.75760984, 13.69526362 54.82014481, 13.68019275 54.81793704)",
    "quelle": "EMODnet"
  },
  {
    "pipeline_name": "Offshore power cable (BSH/CONTIS) - segment 3",
    "medium": "Power",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (13.65904106 54.15418313, 13.65157662 54.16214353, 13.65447918 54.18389494, 13.72556072 54.20741299, 13.72869218 54.2361995, 13.85757551 54.32839299, 13.8447444 54.34886923, 13.87210358 54.37126522, 13.8593145 54.40321336, 13.85233544 54.50737797, 13.83133606 54.61758676, 13.88167135 54.71999024, 13.94404829 54.75402269, 13.94487569 54.76889469, 14.03101089 54.7802854, 14.03452444 54.79647781, 14.09079304 54.8063018, 14.08239963 54.82000882, 14.08573124 54.83089995, 14.0737507 54.83464922)",
    "quelle": "EMODnet"
  },
  {
    "pipeline_name": "Offshore power cable (BSH/CONTIS) - segment 4",
    "medium": "Power",
    "status": "In use",
    "geo_typ": "LineString",
    "geometrie_wkt": "LINESTRING (13.65892037 54.15411266, 13.65006995 54.16298981, 13.65302761 54.18440004, 13.72451092 54.20822215, 13.72811924 54.23653393, 13.85575266 54.32879564, 13.84238285 54.34807076, 13.87038855 54.37134365, 13.86050454 54.3877265, 13.85078977 54.50746368, 13.82822298 54.61741701, 13.87880928 54.72075164, 13.94050529 54.75448205, 13.94109647 54.77022477, 14.0299514 54.78159185, 14.01889748 54.82548224, 14.07389896 54.83471551)",
    "quelle": "EMODnet"
  }
];

export const kpis = {
  fleetUnits: 42,
  personnel: 1280,
  surveillanceReports: 847,
  alerts: 2,
  nbaIncidents: 3,
};

/** Offline fallback for /api/analyses/buoy-analytics */
export const buoyAnalytics = {
  panels: buoyAnalyticsPanels.panels,
};

/** Offline fallback for /api/analyses/rus-fleet-analytics */
export const rusFleetAnalytics = {
  panels: rusFleetAnalyticsPanels.panels,
};

/** Offline fallback for /api/analyses/fleet-analytics */
export const fleetAnalytics = {
  panels: {
    messagesFrom: {
      columns: ["name", "value"],
      rows: [
        { name: "FGS Sachsen", value: 24 },
        { name: "FGS Braunschweig", value: 22 },
        { name: "U-32", value: 18 },
      ],
    },
    messagesTo: {
      columns: ["name", "value"],
      rows: [{ name: "NAVAL COMMAND", value: 120 }],
    },
    operationalStatus: {
      columns: ["name", "value"],
      rows: [
        { name: "Fully Operational", value: 2 },
        { name: "Limited Operational", value: 2 },
        { name: "Non-Operational", value: 1 },
      ],
    },
    systemStatus: {
      columns: ["name", "value"],
      rows: [
        { name: "All Systems Green", value: 2 },
        { name: "Minor Sensor Issues", value: 2 },
      ],
    },
    destinations: {
      columns: ["name", "value"],
      rows: [
        { name: "Kiel", value: 3 },
        { name: "Warnemünde", value: 1 },
      ],
    },
    unitStatus: {
      columns: [
        "unit_name",
        "unit_class",
        "nav_status",
        "operational_status",
        "system_status",
        "destination",
        "messages_out",
        "message_to",
      ],
      rows: [
        {
          unit_name: "FGS Sachsen",
          unit_class: "Fregatten",
          nav_status: "Underway using engine",
          operational_status: "Fully Operational",
          system_status: "All Systems Green",
          destination: "Kiel",
          messages_out: 24,
          message_to: "NAVAL COMMAND",
        },
      ],
    },
    recentMessages: {
      columns: ["unit_name", "unit_class", "message_id", "message_from", "message_to", "message_dtg", "message_subject"],
      rows: [
        {
          unit_name: "FGS Sachsen",
          unit_class: "Fregatten",
          message_id: "SR-DEMO-1",
          message_from: "FGS Sachsen",
          message_to: "NAVAL COMMAND",
          message_dtg: "151200ZNOV25",
          message_subject: "SITREP",
        },
      ],
    },
  },
};

export const datasets = [
  { name: "ShipsAIS", tables: ["ais_events_ice", "ships"], badge: "multipletables" },
  { name: "FlottenStatus", tables: ["german_navy_fleet", "marine_vessel_status"], badge: "multipletables" },
  { name: "SanctionedVessels", tables: ["sanctioned_vessels"], badge: "multipletables" },
  { name: "AreaViolations", tables: ["area_violation"], badge: "multipletables" },
  { name: "SurveillanceReports", tables: ["maritime_surveillance_reports"], badge: "multipletables" },
  { name: "AIS Positions", tables: ["ais_events_ice"], badge: "multipletables" },
  { name: "Buoy_near_harbours", tables: ["buoy_near_harbours"], badge: "singletable" },
  { name: "ObservationAreas", tables: ["observation_areas"], badge: "singletable" },
  {
    name: "OstseeInfrastructure",
    tables: ["ostsee_infrastruktur", "ostsee_pipelines"],
    badge: "multipletables",
  },
  { name: "BalticSeaHarbours", tables: ["baltic_sea_harbours"], badge: "singletable" },
  { name: "PersonalReport", tables: ["german_navy_fleet"], badge: "multipletables" },
  { name: "Social Media", tables: ["social_media_messages"], badge: "singletable" },
  { name: "AIS Vessel Tracker", tables: ["ais_events_ice", "ships"], badge: "multipletables" },
  { name: "LiDARSimulationMetadata", tables: ["lidar_simulation_metadata"], badge: "singletable" },
  { name: "DasCableSpectrogramMetadata", tables: ["das_cable_spectrogram_metadata"], badge: "singletable" },
  { name: "GpsJammerEvents", tables: ["gps_jammer_events", "lagebild"], badge: "multipletables" },
];
