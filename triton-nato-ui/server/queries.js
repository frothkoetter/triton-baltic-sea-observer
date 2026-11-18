/** Read-only SQL templates aligned with CDV dataset names (defense schema). */

function sqlLiteral(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

const tsParse = (alias) => `coalesce(
  try(from_iso8601_timestamp(${alias})),
  try(from_iso8601_timestamp(replace(${alias}, 'Z', '+00:00'))),
  try_cast(replace(${alias}, 'Z', '') AS timestamp)
)`;

/** AIS position history for route map (optional Filter Rail predicates on ships). */
export function buildAisTrackSql({ hours = 24, flag = "", vesselType = "", shipName = "" } = {}) {
  const predicates = [
    "a.latitude IS NOT NULL",
    "a.longitude IS NOT NULL",
    `${tsParse("a.event_timestamp")} >= current_timestamp - interval '${hours}' hour`,
  ];
  if (flag) predicates.push(`s.flag = ${sqlLiteral(flag)}`);
  if (vesselType) predicates.push(`s.vessel_type = ${sqlLiteral(vesselType)}`);
  if (shipName) predicates.push(`s.ship_name = ${sqlLiteral(shipName)}`);
  return `
    SELECT
      CAST(a.mmsi AS varchar) AS mmsi,
      s.ship_name,
      s.vessel_type,
      s.flag,
      a.latitude,
      a.longitude,
      a.speed,
      a.course,
      a.event_timestamp,
      'ais' AS track_kind
    FROM defense.ais_events_ice a
    LEFT JOIN defense.ships s ON a.mmsi = s.mmsi
    WHERE ${predicates.join("\n      AND ")}
    ORDER BY a.mmsi, ${tsParse("a.event_timestamp")} ASC
    LIMIT 25000
  `;
}

export function buildGermanFleetTrackSql({ hours = 24, shipName = "" } = {}) {
  const predicates = [
    "m.latitude IS NOT NULL",
    "m.longitude IS NOT NULL",
    `${tsParse("m.event_timestamp")} >= current_timestamp - interval '${hours}' hour`,
  ];
  if (shipName) predicates.push(`g.name = ${sqlLiteral(shipName)}`);
  return `
    SELECT
      CAST(m.mmsi AS varchar) AS mmsi,
      g.name AS ship_name,
      g.class AS vessel_type,
      'Germany' AS flag,
      m.latitude,
      m.longitude,
      m.speed,
      m.course,
      m.event_timestamp,
      'deu_fleet' AS track_kind
    FROM defense.marine_vessel_status m
    INNER JOIN defense.german_navy_fleet g ON CAST(m.mmsi AS varchar) = g.mmsi
    WHERE ${predicates.join("\n      AND ")}
    ORDER BY m.mmsi, ${tsParse("m.event_timestamp")} ASC
    LIMIT 15000
  `;
}

export function buildRussianFleetTrackSql({ hours = 24, shipName = "" } = {}) {
  const predicates = [
    "p.latitude IS NOT NULL",
    "p.longitude IS NOT NULL",
    `${tsParse("p.event_timestamp")} >= current_timestamp - interval '${hours}' hour`,
  ];
  if (shipName) predicates.push(`rf.name = ${sqlLiteral(shipName)}`);
  return `
    SELECT
      p.mmsi AS mmsi,
      rf.name AS ship_name,
      rf.class AS vessel_type,
      'Russia' AS flag,
      p.latitude,
      p.longitude,
      p.speed,
      p.course,
      p.event_timestamp,
      'rus_fleet' AS track_kind
    FROM defense.rus_naval_fleet_positions p
    LEFT JOIN defense.rus_fleet rf ON p.mmsi = rf.mmsi
    WHERE ${predicates.join("\n      AND ")}
    ORDER BY p.mmsi, ${tsParse("p.event_timestamp")} ASC
    LIMIT 15000
  `;
}

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
    SELECT class, name, mmsi, jahr_in_dienst, besatzung
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
    SELECT area_id, polygon, center_latitude, center_longitude,
           country, area_type, tier, source
    FROM defense.observation_areas
    WHERE coalesce(tier, 'A') = 'A'
    ORDER BY country, area_id
  `,
  ostseeInfrastruktur: `
    SELECT id, name, typ, geometrie_wkt, status, medium
    FROM defense.ostsee_infrastruktur
    WHERE geometrie_wkt IS NOT NULL
  `,
  ostseePipelines: `
    SELECT pipeline_name, medium, status, geo_typ, geometrie_wkt, quelle
    FROM defense.ostsee_pipelines
    WHERE geometrie_wkt IS NOT NULL
  `,
  /** Latest maritime anomalies for Lagebild map (Trino pipeline + legacy NiFi rows). */
  vesselOutliersMap: `
    SELECT
      mmsi,
      event_timestamp,
      latitude,
      longitude,
      speed,
      course,
      status,
      outlier_type,
      reason,
      severity_level
    FROM defense.vessel_outliers
    WHERE mmsi > 0
      AND latitude BETWEEN 53.0 AND 60.5
      AND longitude BETWEEN 10.0 AND 21.0
      AND coalesce(
        try(from_iso8601_timestamp(event_timestamp)),
        try(from_iso8601_timestamp(replace(event_timestamp, 'Z', '+00:00'))),
        try_cast(replace(event_timestamp, 'Z', '') AS timestamp)
      ) >= current_timestamp - interval '24' hour
    ORDER BY coalesce(
      try(from_iso8601_timestamp(event_timestamp)),
      try(from_iso8601_timestamp(replace(event_timestamp, 'Z', '+00:00'))),
      try_cast(replace(event_timestamp, 'Z', '') AS timestamp)
    ) DESC
    LIMIT 400
  `,
  /** German Navy — latest `marine_vessel_status` per unit (joined to `german_navy_fleet`). */
  germanFleetPositions: `
    WITH ranked AS (
      SELECT
        m.mmsi,
        m.event_timestamp,
        m.latitude,
        m.longitude,
        m.speed,
        m.course,
        m.status,
        m.operational_status,
        g.name AS unit_name,
        g.class AS unit_class,
        row_number() OVER (
          PARTITION BY m.mmsi
          ORDER BY coalesce(
            try(from_iso8601_timestamp(m.event_timestamp)),
            try(from_iso8601_timestamp(replace(m.event_timestamp, 'Z', '+00:00'))),
            try_cast(replace(m.event_timestamp, 'Z', '') AS timestamp)
          ) DESC
        ) AS rn
      FROM defense.marine_vessel_status m
      INNER JOIN defense.german_navy_fleet g ON CAST(m.mmsi AS varchar) = g.mmsi
      WHERE m.latitude IS NOT NULL AND m.longitude IS NOT NULL
    )
    SELECT
      mmsi, event_timestamp, latitude, longitude, speed, course, status,
      operational_status, unit_name, unit_class
    FROM ranked
    WHERE rn = 1
    ORDER BY unit_name
    LIMIT 80
  `,
  /** Russian Baltic Fleet — latest positions + registry metadata. */
  russianFleetPositions: `
    WITH ranked AS (
      SELECT
        p.mmsi,
        p.event_timestamp,
        p.latitude,
        p.longitude,
        p.speed,
        p.course,
        p.status,
        p.Destination AS destination,
        rf.name AS unit_name,
        rf.class AS unit_class,
        rf.home_base,
        rf.vessel_category,
        row_number() OVER (
          PARTITION BY p.mmsi
          ORDER BY coalesce(
            try(from_iso8601_timestamp(p.event_timestamp)),
            try(from_iso8601_timestamp(replace(p.event_timestamp, 'Z', '+00:00'))),
            try_cast(replace(p.event_timestamp, 'Z', '') AS timestamp)
          ) DESC
        ) AS rn
      FROM defense.rus_naval_fleet_positions p
      LEFT JOIN defense.rus_fleet rf ON p.mmsi = rf.mmsi
      WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
        AND coalesce(
          try(from_iso8601_timestamp(p.event_timestamp)),
          try(from_iso8601_timestamp(replace(p.event_timestamp, 'Z', '+00:00'))),
          try_cast(replace(p.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '24' hour
    )
    SELECT
      mmsi, event_timestamp, latitude, longitude, speed, course, status,
      destination, unit_name, unit_class, home_base, vessel_category
    FROM ranked
    WHERE rn = 1
    ORDER BY unit_name
    LIMIT 80
  `,
  /** Same events as UC1 SITREP (harbour-scoped dedupe in defense.lagebild). */
  lagebildOutliersMap: `
    SELECT
      id AS mmsi,
      "timestamp" AS event_timestamp,
      latitude,
      longitude,
      harbour_name,
      details AS reason,
      'AIS_Outlier' AS outlier_type
    FROM defense.lagebild
    WHERE Data_Source = 'AIS_Outlier'
    ORDER BY "timestamp" DESC
    LIMIT 200
  `,
  lidarScans: `
    SELECT filename, vessel_name, vessel_type, classification,
           center_lat, center_lon, heading_deg, point_count,
           hadoop_file_url, predicted_vessel_type, model_confidence, ingested_at
    FROM defense.lidar_simulation_metadata
    ORDER BY ingested_at DESC
    LIMIT 200
  `,
  fleetKpi: `SELECT COUNT(*) AS cnt FROM defense.german_navy_fleet`,
  personnelKpi: `SELECT COALESCE(SUM(besatzung), 0) AS cnt FROM defense.german_navy_fleet`,
  surveillanceKpi: `SELECT COUNT(*) AS cnt FROM defense.maritime_surveillance_reports`,
  /** Fleet analytics: german_navy_fleet ⋈ marine_messages ⋈ marine_vessel_status (via MMSI / unit name). */
  fleetAnalyticsMessagesFrom: `
    SELECT ms.message_from AS name, COUNT(*) AS value
    FROM defense.german_navy_fleet fl
    INNER JOIN defense.marine_messages ms ON ms.message_from = fl.name
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 12
  `,
  fleetAnalyticsMessagesTo: `
    SELECT ms.message_to AS name, COUNT(*) AS value
    FROM defense.marine_messages ms
    INNER JOIN defense.german_navy_fleet fl ON ms.message_from = fl.name
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 8
  `,
  fleetAnalyticsOperational: `
    WITH latest AS (
      SELECT
        m.operational_status AS name,
        row_number() OVER (
          PARTITION BY m.mmsi
          ORDER BY coalesce(
            try(from_iso8601_timestamp(m.event_timestamp)),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s'))
          ) DESC
        ) AS rn
      FROM defense.marine_vessel_status m
      INNER JOIN defense.german_navy_fleet g ON m.mmsi = g.mmsi
    )
    SELECT COALESCE(name, 'Unknown') AS name, COUNT(*) AS value
    FROM latest
    WHERE rn = 1
    GROUP BY 1
    ORDER BY 2 DESC
  `,
  fleetAnalyticsSystem: `
    WITH latest AS (
      SELECT
        m.system_status AS name,
        row_number() OVER (
          PARTITION BY m.mmsi
          ORDER BY coalesce(
            try(from_iso8601_timestamp(m.event_timestamp)),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s'))
          ) DESC
        ) AS rn
      FROM defense.marine_vessel_status m
      INNER JOIN defense.german_navy_fleet g ON m.mmsi = g.mmsi
    )
    SELECT COALESCE(name, 'Unknown') AS name, COUNT(*) AS value
    FROM latest
    WHERE rn = 1
    GROUP BY 1
    ORDER BY 2 DESC
  `,
  fleetAnalyticsDestinations: `
    WITH latest AS (
      SELECT
        COALESCE(NULLIF(trim(m.destination), ''), 'Unknown') AS name,
        row_number() OVER (
          PARTITION BY m.mmsi
          ORDER BY coalesce(
            try(from_iso8601_timestamp(m.event_timestamp)),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s'))
          ) DESC
        ) AS rn
      FROM defense.marine_vessel_status m
      INNER JOIN defense.german_navy_fleet g ON m.mmsi = g.mmsi
    )
    SELECT name, COUNT(*) AS value
    FROM latest
    WHERE rn = 1
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
  `,
  fleetAnalyticsUnitStatus: `
    WITH latest AS (
      SELECT
        m.mmsi,
        m.status AS nav_status,
        m.operational_status,
        m.system_status,
        m.destination,
        g.name AS unit_name,
        g.class AS unit_class,
        row_number() OVER (
          PARTITION BY m.mmsi
          ORDER BY coalesce(
            try(from_iso8601_timestamp(m.event_timestamp)),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
            try(date_parse(m.event_timestamp, '%Y-%m-%d %H:%i:%s'))
          ) DESC
        ) AS rn
      FROM defense.marine_vessel_status m
      INNER JOIN defense.german_navy_fleet g ON m.mmsi = g.mmsi
    ),
    msg_cnt AS (
      SELECT fl.name, COUNT(*) AS messages_out
      FROM defense.german_navy_fleet fl
      INNER JOIN defense.marine_messages ms ON ms.message_from = fl.name
      GROUP BY 1
    )
    SELECT
      l.unit_name,
      l.unit_class,
      l.nav_status,
      l.operational_status,
      l.system_status,
      l.destination,
      COALESCE(m.messages_out, 0) AS messages_out,
      CAST('NAVAL COMMAND' AS varchar) AS message_to
    FROM latest l
    LEFT JOIN msg_cnt m ON m.name = l.unit_name
    WHERE l.rn = 1
    ORDER BY messages_out DESC, l.unit_name
    LIMIT 80
  `,
  fleetAnalyticsRecentMessages: `
    SELECT
      fl.name AS unit_name,
      fl.class AS unit_class,
      ms.message_id,
      ms.message_from,
      ms.message_to,
      ms.message_dtg,
      ms.message_subject
    FROM defense.marine_messages ms
    INNER JOIN defense.german_navy_fleet fl ON ms.message_from = fl.name
    ORDER BY ms.message_timestamp DESC
    LIMIT 40
  `,
  /** Russian Baltic Fleet — latest position ⋈ rus_fleet; movement & port vs transit (24h). */
  rusAnalyticsUnitDisposition: `
    WITH harbours AS (
      SELECT name, latitude, longitude FROM defense.baltic_sea_harbours
    ),
    latest AS (
      SELECT
        p.mmsi,
        p.latitude,
        p.longitude,
        p.speed,
        p.status,
        rf.name AS unit_name,
        rf.class AS unit_class,
        rf.home_base,
        rf.vessel_category,
        row_number() OVER (
          PARTITION BY p.mmsi
          ORDER BY ${tsParse("p.event_timestamp")} DESC
        ) AS rn
      FROM defense.rus_naval_fleet_positions p
      LEFT JOIN defense.rus_fleet rf ON p.mmsi = rf.mmsi
      WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
    ),
    movement AS (
      SELECT
        mmsi,
        COUNT(*) AS pts_24h,
        MAX(speed) AS max_sog_24h,
        AVG(speed) AS avg_sog_24h
      FROM defense.rus_naval_fleet_positions
      WHERE ${tsParse("event_timestamp")} >= current_timestamp - interval '24' hour
      GROUP BY 1
    ),
    joined AS (
      SELECT
        l.mmsi,
        l.unit_name,
        l.unit_class,
        l.home_base,
        l.vessel_category,
        l.status,
        l.latitude,
        l.longitude,
        ROUND(
          6371 * 2 * ASIN(
            SQRT(
              POWER(SIN(RADIANS(l.latitude - h.latitude) / 2), 2) +
              COS(RADIANS(l.latitude)) * COS(RADIANS(h.latitude)) *
              POWER(SIN(RADIANS(l.longitude - h.longitude) / 2), 2)
            )
          ),
          1
        ) AS dist_home_km,
        m.pts_24h,
        ROUND(m.max_sog_24h, 2) AS max_sog_24h,
        ROUND(m.avg_sog_24h, 2) AS avg_sog_24h
      FROM latest l
      LEFT JOIN harbours h ON l.home_base = h.name
      LEFT JOIN movement m ON l.mmsi = m.mmsi
      WHERE l.rn = 1
    )
    SELECT
      mmsi,
      unit_name,
      unit_class,
      home_base,
      vessel_category,
      status,
      dist_home_km,
      max_sog_24h,
      avg_sog_24h,
      pts_24h,
      CASE
        WHEN dist_home_km IS NOT NULL AND dist_home_km < 12 THEN 'At home port'
        WHEN max_sog_24h IS NOT NULL AND max_sog_24h < 0.5 THEN 'In port / anchor (away)'
        WHEN max_sog_24h IS NOT NULL AND max_sog_24h < 4 THEN 'Patrol / slow'
        ELSE 'Transit'
      END AS activity,
      CASE
        WHEN dist_home_km IS NOT NULL AND dist_home_km < 12 THEN true
        ELSE false
      END AS at_home_port
    FROM joined
    ORDER BY unit_name
    LIMIT 80
  `,
  rusAnalyticsActivityMix: `
    WITH harbours AS (
      SELECT name, latitude, longitude FROM defense.baltic_sea_harbours
    ),
    latest AS (
      SELECT
        p.mmsi,
        p.latitude,
        p.longitude,
        rf.home_base,
        row_number() OVER (
          PARTITION BY p.mmsi
          ORDER BY ${tsParse("p.event_timestamp")} DESC
        ) AS rn
      FROM defense.rus_naval_fleet_positions p
      LEFT JOIN defense.rus_fleet rf ON p.mmsi = rf.mmsi
    ),
    movement AS (
      SELECT mmsi, MAX(speed) AS max_sog_24h
      FROM defense.rus_naval_fleet_positions
      WHERE ${tsParse("event_timestamp")} >= current_timestamp - interval '24' hour
      GROUP BY 1
    ),
    joined AS (
      SELECT
        l.mmsi,
        (6371 * 2 * ASIN(
          SQRT(
            POWER(SIN(RADIANS(l.latitude - h.latitude) / 2), 2) +
            COS(RADIANS(l.latitude)) * COS(RADIANS(h.latitude)) *
            POWER(SIN(RADIANS(l.longitude - h.longitude) / 2), 2)
          )
        )) AS dist_home_km,
        m.max_sog_24h
      FROM latest l
      LEFT JOIN harbours h ON l.home_base = h.name
      LEFT JOIN movement m ON l.mmsi = m.mmsi
      WHERE l.rn = 1
    )
    SELECT
      CASE
        WHEN dist_home_km IS NOT NULL AND dist_home_km < 12 THEN 'At home port'
        WHEN max_sog_24h IS NOT NULL AND max_sog_24h < 0.5 THEN 'In port / anchor (away)'
        WHEN max_sog_24h IS NOT NULL AND max_sog_24h < 4 THEN 'Patrol / slow'
        ELSE 'Transit'
      END AS name,
      COUNT(*) AS value
    FROM joined
    GROUP BY 1
    ORDER BY 2 DESC
  `,
  rusAnalyticsCategoryMix: `
    SELECT COALESCE(vessel_category, 'unknown') AS name, COUNT(*) AS value
    FROM defense.rus_fleet
    GROUP BY 1
    ORDER BY 2 DESC
  `,
  rusAnalyticsHomeBaseMix: `
    SELECT COALESCE(home_base, 'unknown') AS name, COUNT(*) AS value
    FROM defense.rus_fleet
    GROUP BY 1
    ORDER BY 2 DESC
  `,
  rusAnalyticsRestrictedUnits: `
    WITH observation_polygons AS (
      SELECT area_id, ST_GeometryFromText(polygon) AS geom
      FROM defense.observation_areas
    ),
    latest AS (
      SELECT
        p.mmsi,
        p.latitude,
        p.longitude,
        p.speed,
        p.status,
        rf.name AS unit_name,
        rf.vessel_category,
        row_number() OVER (
          PARTITION BY p.mmsi
          ORDER BY ${tsParse("p.event_timestamp")} DESC
        ) AS rn
      FROM defense.rus_naval_fleet_positions p
      LEFT JOIN defense.rus_fleet rf ON p.mmsi = rf.mmsi
      WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
    )
    SELECT
      l.mmsi,
      l.unit_name,
      l.vessel_category,
      l.status,
      o.area_id,
      l.latitude,
      l.longitude
    FROM latest l
    INNER JOIN observation_polygons o
      ON ST_Within(ST_Point(l.longitude, l.latitude), o.geom)
    WHERE l.rn = 1
    ORDER BY o.area_id, l.unit_name
    LIMIT 100
  `,
  rusAnalyticsRestrictedByArea: `
    WITH observation_polygons AS (
      SELECT area_id, ST_GeometryFromText(polygon) AS geom
      FROM defense.observation_areas
    ),
    latest AS (
      SELECT
        p.mmsi,
        p.latitude,
        p.longitude,
        row_number() OVER (
          PARTITION BY p.mmsi
          ORDER BY ${tsParse("p.event_timestamp")} DESC
        ) AS rn
      FROM defense.rus_naval_fleet_positions p
      WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
    )
    SELECT o.area_id AS name, COUNT(*) AS value
    FROM latest l
    INNER JOIN observation_polygons o
      ON ST_Within(ST_Point(l.longitude, l.latitude), o.geom)
    WHERE l.rn = 1
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
  `,
  /** Buoy detections near Baltic harbours (Analysen → Buoys). */
  buoyAnalyticsRecent: `
    SELECT
      buoyid,
      ts,
      payload_object_type,
      payload_object_classification,
      harbor_name,
      harbor_country,
      distance_km,
      geo_position_lat,
      geo_position_lon
    FROM defense.buoy_near_harbours
    ORDER BY ts DESC
    LIMIT 100
  `,
  buoyAnalyticsTypeMix: `
    SELECT payload_object_type AS name, COUNT(*) AS value
    FROM defense.buoy_near_harbours
    GROUP BY 1
    ORDER BY 2 DESC
  `,
  buoyAnalyticsClassificationGrid: `
    SELECT
      payload_object_type,
      payload_object_classification,
      avg(payload_object_confidence) AS avg_confidence,
      count(*) AS record_count
    FROM defense.buoy_near_harbours
    GROUP BY 1, 2
    ORDER BY 4 DESC
    LIMIT 5000
  `,
  buoyAnalyticsGeoClusters: `
    SELECT
      geo_position_lat,
      geo_position_lon,
      sum(altitude) AS sum_altitude,
      count(*) AS record_count
    FROM defense.buoy_near_harbours
    GROUP BY 1, 2
    ORDER BY 4 DESC
    LIMIT 5000
  `,
  alertsKpi: `
    SELECT COUNT(*) AS cnt
    FROM defense.vessel_outliers
    WHERE mmsi > 0
      AND latitude BETWEEN 53.0 AND 60.5
      AND longitude BETWEEN 10.0 AND 21.0
      AND severity_level >= 70
      AND coalesce(
        try(from_iso8601_timestamp(event_timestamp)),
        try(from_iso8601_timestamp(replace(event_timestamp, 'Z', '+00:00'))),
        try_cast(replace(event_timestamp, 'Z', '') AS timestamp)
      ) >= current_timestamp - interval '24' hour
  `,
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
  LiDARSimulationMetadata: PREDEFINED.lidarScans,
};
