-- =========================================================================================
-- TRINO SQL PIPELINE: MARITIME ANOMALY DETECTION (AIS & MAD SENSORS)
-- =========================================================================================
-- Catalog/schema: iceberg.defense
-- Targets: ais_baseline_sketches, vessel_outliers (see create_db_tables_trino.sql)
--
-- Pipeline steps:
--   1. Baseline training (T-Digest speed + SetDigest MMSI per tile/type/sector)
--   2. Case 1: Kinematics, misreported status, loitering
--   3. Case 2a: MMSI teleportation / implausible hop speed
--   4. Case 2b: Dark vessel (MAD buoy echo, no AIS within radius)
--   5. Case 3: Ship-to-ship rendezvous (STS)
--
-- Run: cd cdw-analyse && ./run_outlier_detection_trino.sh
-- Doc:  cdw-analyse/README_outlier_detection_trino.md
-- =========================================================================================

USE iceberg.defense;

-- =========================================================================================
-- STEP 1: SETUP & BASELINE SKETCHES
-- =========================================================================================

CREATE TABLE IF NOT EXISTS ais_baseline_sketches (
    quadkey varchar COMMENT 'Bing Tile Quadkey (Zoom 11, ~11 km grid)',
    vessel_type varchar COMMENT 'Vessel type from defense.ships',
    course_sector varchar COMMENT 'Course sector N, NE, E, SE, S, SW, W, NW',
    speed_tdigest varbinary COMMENT 'Serialized T-Digest of SOG (kn)',
    mmsi_set_digest varbinary COMMENT 'Serialized SetDigest (HLL) of MMSIs',
    updated_at timestamp COMMENT 'Last baseline merge time (UTC)'
);

MERGE INTO ais_baseline_sketches AS target
USING (
    WITH valid_ais AS (
        SELECT
            bing_tile_quadkey(bing_tile_at(a.latitude, a.longitude, 11)) AS quadkey,
            COALESCE(s.vessel_type, 'Unknown') AS vessel_type,
            CASE
                WHEN a.course >= 337.5 OR a.course < 22.5  THEN 'N'
                WHEN a.course >= 22.5  AND a.course < 67.5  THEN 'NE'
                WHEN a.course >= 67.5  AND a.course < 112.5 THEN 'E'
                WHEN a.course >= 112.5 AND a.course < 157.5 THEN 'SE'
                WHEN a.course >= 157.5 AND a.course < 202.5 THEN 'S'
                WHEN a.course >= 202.5 AND a.course < 247.5 THEN 'SW'
                WHEN a.course >= 247.5 AND a.course < 292.5 THEN 'W'
                ELSE 'NW'
            END AS course_sector,
            a.mmsi,
            a.speed
        FROM ais_events_ice a
        LEFT JOIN ships s ON a.mmsi = s.mmsi
        WHERE a.latitude BETWEEN -85.0 AND 85.0
          AND a.longitude BETWEEN -180.0 AND 180.0
          AND a.speed IS NOT NULL
          AND a.course IS NOT NULL
          AND coalesce(
              try(from_iso8601_timestamp(a.event_timestamp)),
              try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
          ) >= current_timestamp - interval '24' hour
    ),
    delta_sketches AS (
        SELECT
            quadkey, vessel_type, course_sector,
            tdigest_agg(speed) AS speed_td,
            make_set_digest(mmsi) AS mmsi_sd
        FROM valid_ais
        GROUP BY quadkey, vessel_type, course_sector
    ),
    combined_sketches AS (
        SELECT quadkey, vessel_type, course_sector, speed_td, mmsi_sd FROM delta_sketches
        UNION ALL
        SELECT t.quadkey, t.vessel_type, t.course_sector,
               CAST(t.speed_tdigest AS tdigest) AS speed_td,
               CAST(t.mmsi_set_digest AS setdigest) AS mmsi_sd
        FROM ais_baseline_sketches t
        INNER JOIN delta_sketches d
            ON t.quadkey = d.quadkey AND t.vessel_type = d.vessel_type AND t.course_sector = d.course_sector
    )
    SELECT
        quadkey, vessel_type, course_sector,
        CAST(merge(speed_td) AS varbinary) AS merged_speed_tdigest,
        CAST(merge_set_digest(mmsi_sd) AS varbinary) AS merged_mmsi_set_digest
    FROM combined_sketches
    GROUP BY quadkey, vessel_type, course_sector
) AS src
ON target.quadkey = src.quadkey AND target.vessel_type = src.vessel_type AND target.course_sector = src.course_sector
WHEN MATCHED THEN UPDATE SET
    speed_tdigest = src.merged_speed_tdigest,
    mmsi_set_digest = src.merged_mmsi_set_digest,
    updated_at = current_timestamp
WHEN NOT MATCHED THEN INSERT (
    quadkey, vessel_type, course_sector, speed_tdigest, mmsi_set_digest, updated_at
) VALUES (
    src.quadkey, src.vessel_type, src.course_sector,
    src.merged_speed_tdigest, src.merged_mmsi_set_digest, current_timestamp
);

-- =========================================================================================
-- STEP 2: CASE 1 — KINEMATICS, MISREPORTED STATUS, LOITERING
-- =========================================================================================

INSERT INTO vessel_outliers (
    MMSI, Event_Timestamp, Latitude, Longitude, Speed, Course, Status, Destination,
    Outlier_Type, Reason, Recommendation, Severity_Level
)
WITH recent_events AS (
    SELECT
        a.mmsi, a.event_timestamp,
        coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) AS ts,
        a.latitude, a.longitude, a.speed, a.course, a.status, a.Destination,
        COALESCE(s.vessel_type, 'Unknown') AS vessel_type,
        bing_tile_quadkey(bing_tile_at(a.latitude, a.longitude, 11)) AS quadkey,
        CASE
            WHEN a.course >= 337.5 OR a.course < 22.5  THEN 'N'
            WHEN a.course >= 22.5  AND a.course < 67.5  THEN 'NE'
            WHEN a.course >= 67.5  AND a.course < 112.5 THEN 'E'
            WHEN a.course >= 112.5 AND a.course < 157.5 THEN 'SE'
            WHEN a.course >= 157.5 AND a.course < 202.5 THEN 'S'
            WHEN a.course >= 202.5 AND a.course < 247.5 THEN 'SW'
            WHEN a.course >= 247.5 AND a.course < 292.5 THEN 'W'
            ELSE 'NW'
        END AS course_sector
    FROM ais_events_ice a
    LEFT JOIN ships s ON a.mmsi = s.mmsi
    WHERE a.latitude BETWEEN -85.0 AND 85.0 AND a.longitude BETWEEN -180.0 AND 180.0
      AND a.speed IS NOT NULL AND a.course IS NOT NULL
      AND coalesce(
          try(from_iso8601_timestamp(a.event_timestamp)),
          try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
      ) >= current_timestamp - interval '4' hour
),
loitering_stats AS (
    SELECT r.mmsi, approx_distinct(r.quadkey) AS distinct_tiles,
           date_diff('minute', min(r.ts), max(r.ts)) AS duration_min, max(r.speed) AS max_speed
    FROM recent_events r
    WHERE COALESCE(r.status, '') NOT IN ('Moored', 'Anchored', 'At anchor')
    GROUP BY r.mmsi
),
with_baseline AS (
    SELECT r.*,
           value_at_quantile(CAST(b.speed_tdigest AS tdigest), 0.99) AS speed_p99
    FROM recent_events r
    LEFT JOIN ais_baseline_sketches b
      ON r.quadkey = b.quadkey AND r.vessel_type = b.vessel_type AND r.course_sector = b.course_sector
),
kinematic AS (
    SELECT
        mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination,
        'Inconsistent Maneuvering' AS outlier_type,
        format(
            'SOG %.1f kn exceeds baseline P99 %.1f kn in quadkey %s',
            speed, COALESCE(speed_p99, 0), quadkey
        ) AS reason,
        65 AS severity_level
    FROM with_baseline
    WHERE speed_p99 IS NOT NULL AND speed > speed_p99 * 1.35
),
misreported AS (
    SELECT
        mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination,
        'Misreported Status' AS outlier_type,
        format('Status "%s" with SOG %.1f kn', COALESCE(status, ''), speed) AS reason,
        55 AS severity_level
    FROM recent_events
    WHERE (
        (LOWER(COALESCE(status, '')) LIKE '%moor%' OR LOWER(COALESCE(status, '')) LIKE '%anchor%')
        AND speed > 3.0
    ) OR speed > 45.0
),
loitering AS (
    SELECT
        r.mmsi, r.event_timestamp, r.latitude, r.longitude, r.speed, r.course, r.status, r.Destination,
        'Loitering' AS outlier_type,
        format('%s min in %d tiles (max SOG %.1f kn)', ls.duration_min, ls.distinct_tiles, ls.max_speed) AS reason,
        50 AS severity_level
    FROM recent_events r
    INNER JOIN loitering_stats ls ON r.mmsi = ls.mmsi
    WHERE ls.duration_min >= 90 AND ls.distinct_tiles <= 2 AND ls.max_speed < 8.0
      AND r.ts = (SELECT max(r2.ts) FROM recent_events r2 WHERE r2.mmsi = r.mmsi)
),
all_case1 AS (
    SELECT * FROM kinematic
    UNION ALL SELECT * FROM misreported
    UNION ALL SELECT * FROM loitering
)
SELECT
    mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination,
    outlier_type,
    reason,
    'Review track on chart; cross-check destination and status' AS recommendation,
    severity_level
FROM all_case1 c
WHERE NOT EXISTS (
    SELECT 1 FROM vessel_outliers v
    WHERE v.MMSI = c.mmsi
      AND v.Event_Timestamp = c.event_timestamp
      AND v.Outlier_Type = c.outlier_type
);

-- =========================================================================================
-- STEP 3: CASE 2a — TELEPORTATION / IMPLAUSIBLE HOP SPEED
-- =========================================================================================

INSERT INTO vessel_outliers (
    MMSI, Event_Timestamp, Latitude, Longitude, Speed, Course, Status, Destination,
    Outlier_Type, Reason, Recommendation, Severity_Level
)
WITH ordered_ais AS (
    SELECT
        a.mmsi, a.event_timestamp,
        coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) AS ts,
        a.latitude, a.longitude, a.speed, a.course, a.status, a.Destination,
        LAG(a.latitude) OVER (PARTITION BY a.mmsi ORDER BY a.event_timestamp) AS prev_lat,
        LAG(a.longitude) OVER (PARTITION BY a.mmsi ORDER BY a.event_timestamp) AS prev_lon,
        LAG(coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        )) OVER (PARTITION BY a.mmsi ORDER BY a.event_timestamp) AS prev_ts
    FROM ais_events_ice a
    WHERE coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '6' hour
),
hops AS (
    SELECT
        *,
        ST_Distance(
            to_spherical_geography(ST_Point(prev_lon, prev_lat)),
            to_spherical_geography(ST_Point(longitude, latitude))
        ) / 1852.0 AS hop_nm,
        date_diff('second', prev_ts, ts) / 3600.0 AS hop_h
    FROM ordered_ais
    WHERE prev_lat IS NOT NULL AND prev_ts IS NOT NULL AND prev_ts < ts
),
teleport AS (
    SELECT
        mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination,
        'MMSI Teleportation' AS outlier_type,
        format('Implied hop speed %.0f kn over %.1f nm in %.2f h', hop_nm / NULLIF(hop_h, 0), hop_nm, hop_h) AS reason,
        80 AS severity_level
    FROM hops
    WHERE hop_h > 0 AND hop_nm / hop_h > 55.0
)
SELECT
    mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination,
    outlier_type, reason,
    'Investigate AIS spoofing or duplicate MMSI identity' AS recommendation,
    severity_level
FROM teleport t
WHERE NOT EXISTS (
    SELECT 1 FROM vessel_outliers v
    WHERE v.MMSI = t.mmsi AND v.Event_Timestamp = t.event_timestamp AND v.Outlier_Type = t.outlier_type
);

-- =========================================================================================
-- STEP 4: CASE 2b — DARK VESSEL (MAD BUOYS WITHOUT AIS)
-- =========================================================================================

INSERT INTO vessel_outliers (
    MMSI, Event_Timestamp, Latitude, Longitude, Speed, Course, Status, Destination,
    Outlier_Type, Reason, Recommendation, Severity_Level
)
SELECT
    CAST(-1 AS bigint) AS mmsi,
    b.ts AS event_timestamp,
    b.geo_position_lat AS latitude,
    b.geo_position_lon AS longitude,
    CAST(0 AS double) AS speed,
    CAST(0 AS double) AS course,
    'Unknown' AS status,
    CAST(NULL AS varchar) AS destination,
    'Dark Vessel' AS outlier_type,
    format('MAD buoy %s detected echo; no AIS within 5 km', b.buoyid) AS reason,
    'Dispatch patrol or LiDAR asset to contact position' AS recommendation,
    70 AS severity_level
FROM buoy_data b
WHERE coalesce(try(from_iso8601_timestamp(b.ts)), try_cast(b.ts AS timestamp))
      >= current_timestamp - interval '4' hour
  AND COALESCE(b.payload_object_type, '') <> ''
  AND NOT EXISTS (
      SELECT 1 FROM ais_events_ice a
      WHERE coalesce(
              try(from_iso8601_timestamp(a.event_timestamp)),
              try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
          ) >= current_timestamp - interval '15' minute
        AND ST_Distance(
            to_spherical_geography(ST_Point(a.longitude, a.latitude)),
            to_spherical_geography(ST_Point(b.geo_position_lon, b.geo_position_lat))
        ) <= 5000
  )
  AND NOT EXISTS (
      SELECT 1 FROM vessel_outliers v
      WHERE v.Outlier_Type = 'Dark Vessel'
        AND v.Event_Timestamp = b.ts
        AND v.Latitude = b.geo_position_lat
        AND v.Longitude = b.geo_position_lon
  );

-- =========================================================================================
-- STEP 5: CASE 3 — SHIP-TO-SHIP (STS) RENDEZVOUS
-- =========================================================================================

INSERT INTO vessel_outliers (
    MMSI, Event_Timestamp, Latitude, Longitude, Speed, Course, Status, Destination,
    Outlier_Type, Reason, Recommendation, Severity_Level
)
WITH recent AS (
    SELECT
        a.mmsi, a.event_timestamp, a.latitude, a.longitude, a.speed, a.course,
        a.status, a.Destination,
        date_trunc('minute', coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        )) AS ts_min
    FROM ais_events_ice a
    WHERE coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '4' hour
),
pairs AS (
    SELECT
        a.mmsi AS mmsi_a, b.mmsi AS mmsi_b,
        a.event_timestamp, a.latitude, a.longitude, a.speed, a.course, a.status, a.Destination,
        ST_Distance(
            to_spherical_geography(ST_Point(a.longitude, a.latitude)),
            to_spherical_geography(ST_Point(b.longitude, b.latitude))
        ) / 1000.0 AS dist_km
    FROM recent a
    INNER JOIN recent b
        ON a.ts_min = b.ts_min AND a.mmsi < b.mmsi
    WHERE ST_Distance(
            to_spherical_geography(ST_Point(a.longitude, a.latitude)),
            to_spherical_geography(ST_Point(b.longitude, b.latitude))
        ) <= 5000
)
SELECT
    mmsi_a AS mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination,
    'Rendezvous' AS outlier_type,
    format('Within %.2f km of MMSI %s at same minute', dist_km, CAST(mmsi_b AS varchar)) AS reason,
    'Assess STS transfer risk; check sanctions lists' AS recommendation,
    60 AS severity_level
FROM pairs p
WHERE NOT EXISTS (
    SELECT 1 FROM vessel_outliers v
    WHERE v.MMSI = p.mmsi_a AND v.Event_Timestamp = p.event_timestamp AND v.Outlier_Type = 'Rendezvous'
);