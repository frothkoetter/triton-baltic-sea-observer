/** Read-only SQL templates aligned with CDV dataset names (defense schema). */

export const PREDEFINED = {
  aisLatest: `
    SELECT a.mmsi, s.ship_name, s.vessel_type, s.flag,
           a.latitude, a.longitude, a.speed, a.course, a.event_timestamp, a.status
    FROM defense.ais_events_ice a
    LEFT JOIN defense.ships s ON a.mmsi = s.mmsi
    ORDER BY a.event_timestamp DESC
    LIMIT 300
  `,
  fleet: `
    SELECT class, name, hull_number, year_commissioned, crew_size
    FROM defense.german_navy_fleet
    ORDER BY class, name
    LIMIT 200
  `,
  sanctions: `
    SELECT Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To
    FROM defense.sanctioned_vessels
    LIMIT 200
  `,
  areaViolations: `
    SELECT * FROM defense.area_violation LIMIT 200
  `,
  surveillance: `
    SELECT message_id, class, name, message_timestamp, message_latitude, message_longitude
    FROM defense.marine_messages
    ORDER BY message_timestamp DESC
    LIMIT 200
  `,
  observationAreas: `
    SELECT area_id, center_latitude, center_longitude FROM defense.observation_areas
  `,
  fleetKpi: `SELECT COUNT(*) AS cnt FROM defense.german_navy_fleet`,
  personnelKpi: `SELECT SUM(crew_size) AS cnt FROM defense.german_navy_fleet`,
  surveillanceKpi: `SELECT COUNT(*) AS cnt FROM defense.maritime_surveillance_reports`,
};

export const DATASET_STARTER_SQL = {
  ShipsAIS: PREDEFINED.aisLatest,
  FlottenStatus: PREDEFINED.fleet,
  SanctionedVessels: PREDEFINED.sanctions,
  AreaViolations: PREDEFINED.areaViolations,
  SurveillanceReports: PREDEFINED.surveillance,
  "AIS Positions": `SELECT mmsi, event_timestamp, latitude, longitude, speed, course FROM defense.ais_events_ice ORDER BY event_timestamp DESC LIMIT 500`,
  Buoy_near_harbours: `SELECT * FROM defense.buoy_near_harbours LIMIT 200`,
  ObservationAreas: PREDEFINED.observationAreas,
  BalticSeaHarbours: `SELECT name, country, latitude, longitude FROM defense.baltic_sea_harbours`,
  PersonalReport: PREDEFINED.fleet,
  "Social Media": `SELECT * FROM defense.social_media_messages LIMIT 200`,
  "AIS Vessel Tracker": PREDEFINED.aisLatest,
};
