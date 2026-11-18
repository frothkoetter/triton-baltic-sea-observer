/** Sample payloads when Impala is unreachable (Baltic Sea demo). */

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
  { mmsi: 123456007, ship_name: "Blue Horizon", vessel_type: "Container", flag: "Singapore", latitude: 54.52, longitude: 18.54, speed: 12.4, course: 285, event_timestamp: "2025-10-15T08:12:00Z", status: "Underway" },
  { mmsi: 123456014, ship_name: "Maritime Queen", vessel_type: "Naval", flag: "United States", latitude: 54.09, longitude: 12.14, speed: 8.1, course: 45, event_timestamp: "2025-10-15T08:10:00Z", status: "Underway" },
  { mmsi: 123456040, ship_name: "Sea Eagle", vessel_type: "Tug", flag: "Germany", latitude: 54.32, longitude: 10.12, speed: 5.2, course: 120, event_timestamp: "2025-10-15T08:08:00Z", status: "Underway" },
  { mmsi: 636022401, ship_name: "HS ATLANTICA", vessel_type: "Crude Oil Tanker", flag: "Liberia", latitude: 55.97, longitude: 17.78, speed: 9.8, course: 190, event_timestamp: "2025-10-15T08:05:00Z", status: "Underway", sanctioned: true },
  { mmsi: 123456028, ship_name: "Polaris", vessel_type: "Passenger", flag: "Sweden", latitude: 59.33, longitude: 18.07, speed: 14.0, course: 210, event_timestamp: "2025-10-15T08:03:00Z", status: "Underway" },
  { mmsi: 123456031, ship_name: "Island Hopper", vessel_type: "Ferry", flag: "Greece", latitude: 55.70, longitude: 21.14, speed: 11.3, course: 340, event_timestamp: "2025-10-15T08:01:00Z", status: "Underway" },
  { mmsi: 123456012, ship_name: "Storm Breaker", vessel_type: "Salvage", flag: "Germany", latitude: 54.71, longitude: 20.45, speed: 6.7, course: 75, event_timestamp: "2025-10-15T07:58:00Z", status: "Underway" },
  { mmsi: 123456041, ship_name: "Ocean Dream", vessel_type: "Container", flag: "United States", latitude: 56.95, longitude: 24.11, speed: 13.1, course: 160, event_timestamp: "2025-10-15T07:55:00Z", status: "Underway" },
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
  { Name: "Andromeda Star", IMO: 9402471, Type: "Crude Oil Tanker", Flag: "Russia", Sanction_Reason: "EU Annex XIII / irregular shipping practices", Linked_To: "EU" },
  { Name: "Lady R", IMO: 9161003, Type: "Crude Oil Tanker", Flag: "Russia", Sanction_Reason: "Defense sector goods transport", Linked_To: "EU" },
  { Name: "ARISTO", MMSI: 123456011, Type: "Chemical/Products Tanker", Flag: "Liberia", Sanction_Reason: "Blocked under E.O. 14024", Linked_To: "Hennesea Holdings Limited" },
];

export const areaViolations = [
  { mmsi: 636022401, area_id: "Bornholm Basin", flag: "Liberia", violation_time: "2025-10-14T22:15:00Z", ship_name: "HS ATLANTICA" },
  { mmsi: 123456029, area_id: "Gdańsk Deep", flag: "Russia", violation_time: "2025-10-14T19:40:00Z", ship_name: "Crystal River" },
  { mmsi: 123456033, area_id: "Gotlandic Expanse", flag: "Russia", violation_time: "2025-10-14T16:05:00Z", ship_name: "Mighty Ocean" },
];

export const surveillance = [
  { message_id: "SR-2025-8841", class: "SURFACE", name: "Contact Alpha", message_timestamp: "2025-10-15T07:45:00Z", message_latitude: 55.02, message_longitude: 17.44 },
  { message_id: "SR-2025-8840", class: "SUBSURFACE", name: "Contact Bravo", message_timestamp: "2025-10-15T06:30:00Z", message_latitude: 56.65, message_longitude: 19.71 },
  { message_id: "SR-2025-8838", class: "AIR", name: "Track Charlie", message_timestamp: "2025-10-15T05:12:00Z", message_latitude: 54.35, message_longitude: 13.08 },
];

export const observationAreas = [
  {
    area_id: "Bornholm Basin",
    center_latitude: 55.9679,
    center_longitude: 17.7773,
    polygon:
      "POLYGON((17.237478356677286 56.03107993705109, 17.237478356677286 55.9047066587936, 18.31712997618766 55.9047066587936, 18.317129976129976 56.03107993705109, 17.237478356677286 56.03107993705109))",
  },
  {
    area_id: "Gdańsk Deep",
    center_latitude: 55.3815,
    center_longitude: 18.7914,
    polygon:
      "POLYGON((18.29518008728172 55.53540464392404, 18.29518008728172 55.22756184778888, 19.28762566297803 55.22756184778888, 19.28762566297803 55.53540464392404, 18.29518008728172 55.53540464392404))",
  },
  {
    area_id: "Gotlandic Expanse",
    center_latitude: 56.6518,
    center_longitude: 19.7168,
    polygon:
      "POLYGON((19.101293326297395 56.70066121963845, 19.101293326297395 56.60288332018746, 20.332383399655697 56.60288332018746, 20.332383399655697 56.70066121963845, 19.101293326297395 56.70066121963845))",
  },
];

export const ostseeInfrastruktur = [
  {
    id: "NS1_MOCK",
    name: "Ostsee Pipeline Route A (Beispiel)",
    typ: "Gaspipeline",
    geometrie_wkt:
      "LINESTRING(13.6500 54.1500, 14.8000 54.8500, 16.2000 55.4000, 19.5000 57.2000, 28.0500 59.5000)",
    status: "In use",
    medium: "Gas",
  },
  {
    id: "CABLE_MOCK",
    name: "Baltic Sea Kabel B (Beispiel)",
    typ: "Seekabel",
    geometrie_wkt: "LINESTRING(14.7000 55.1000, 15.8000 55.6000, 17.9000 56.8000)",
    status: "In use",
    medium: "Telecom/Power",
  },
];

export const ostseePipelines = [
  {
    pipeline_name: "Ostsee Pipeline Route A (Beispiel)",
    medium: "Gas",
    status: "In use",
    geo_typ: "LineString",
    geometrie_wkt:
      "LINESTRING(13.6500 54.1500, 14.8000 54.8500, 16.2000 55.4000, 19.5000 57.2000, 28.0500 59.5000)",
    quelle: "WKT",
  },
  {
    pipeline_name: "Nord Stream 1 Mock GeoJSON",
    medium: "Gas",
    status: "In use",
    geo_typ: "MultiLineString",
    geometrie_wkt: "LINESTRING(13.6512 54.1523, 14.8045 54.8511, 16.2019 55.4088, 19.5 57.2)",
    quelle: "GeoJSON",
  },
];

export const kpis = {
  fleetUnits: 42,
  personnel: 1280,
  surveillanceReports: 847,
  alerts: 2,
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
];
