/** PREDEFINED key per snapshot dataset file (without .json). */

export const SNAPSHOT_PREDEFINED_MAP = {
  aisShips: "aisLatest",
  fleet: "fleet",
  sanctions: "sanctions",
  areaViolations: "areaViolations",
  surveillance: "surveillance",
  observationAreas: "observationAreas",
  ostseeInfrastruktur: "ostseeInfrastruktur",
  ostseePipelines: "ostseePipelines",
  gpsJammerEvents: "gpsJammerEvents",
  vesselOutliers: "vesselOutliersMap",
  germanFleetPositions: "germanFleetPositions",
  russianFleetPositions: "russianFleetPositions",
  lidarScans: "lidarScans",
  dasCableSegments: "dasCableSegments",
  socmintMessages: "socmintMessagesMap",
  threatHotTiles: "threatHotTiles",
  socmintDensityTiles: "socmintDensityTiles",
};

export const SNAPSHOT_POSITION_TRACKS = [
  { key: "ais", build: "buildAisTrackSql" },
  { key: "deu_fleet", build: "buildGermanFleetTrackSql" },
  { key: "rus_fleet", build: "buildRussianFleetTrackSql" },
];