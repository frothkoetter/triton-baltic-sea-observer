-- =========================================================================================
-- TRINO SQL PIPELINE: MARITIME ANOMALY DETECTION (AIS & MAD SENSORS)
-- =========================================================================================
-- Catalog/schema: iceberg.defense
-- Targets: ais_baseline_sketches, vessel_outliers, infrastructure_ais_suspicions
--
-- Pipeline steps:
--   1. Baseline training (T-Digest speed + SetDigest MMSI per tile/type/sector)
--   2. Case 1: Kinematics, misreported status, loitering
--   3. Case 2a: MMSI teleportation / implausible hop speed
--   4. Case 2b: Dark vessel (MAD buoy echo, no AIS within radius)
--   5. Case 3: Ship-to-ship rendezvous (STS)
--   6. Case 4: Pipeline/cable AIS blackout & corridor (spatial-lab 08 + 07)
--   7. Case 5: DAS cable acoustic threats (das_cable_spectrogram_metadata)
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
-- STEP 1b: ROLLING PURGE (recompute detections each job; avoids million-row accumulation)
-- =========================================================================================
DELETE FROM vessel_outliers
WHERE Outlier_Type IN (
    'Inconsistent Maneuvering',
    'Misreported Status',
    'Loitering',
    'MMSI Teleportation',
    'Dark Vessel',
    'Rendezvous',
    'Pipeline AIS Blackout',
    'Pipeline Korridor Loitering',
    'Pipeline Korridor Crossing'
);

CREATE TABLE IF NOT EXISTS infrastructure_ais_suspicions (
    MMSI bigint,
    Pipeline_Name varchar,
    Medium varchar,
    Suspicion_Kind varchar,
    Pattern varchar,
    Event_Start timestamp,
    Event_End timestamp,
    Latitude double,
    Longitude double,
    Speed double,
    Course double,
    Status varchar,
    Destination varchar,
    Detail varchar,
    Severity_Level integer,
    Updated_At timestamp
);

DELETE FROM infrastructure_ais_suspicions;

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
        LAG(a.latitude) OVER (
            PARTITION BY a.mmsi ORDER BY coalesce(
                try(from_iso8601_timestamp(a.event_timestamp)),
                try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
            )
        ) AS prev_lat,
        LAG(a.longitude) OVER (
            PARTITION BY a.mmsi ORDER BY coalesce(
                try(from_iso8601_timestamp(a.event_timestamp)),
                try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
            )
        ) AS prev_lon,
        LAG(coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        )) OVER (
            PARTITION BY a.mmsi ORDER BY coalesce(
                try(from_iso8601_timestamp(a.event_timestamp)),
                try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
            )
        ) AS prev_ts
    FROM ais_events_ice a
    WHERE coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '4' hour
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
        format('Implied hop speed %.0f kn over %.2f nm in %.1f min', hop_nm / NULLIF(hop_h, 0), hop_nm, hop_h * 60.0) AS reason,
        80 AS severity_level
    FROM hops
    WHERE hop_h >= (2.0 / 60.0)
      AND hop_nm >= 0.25
      AND hop_nm / hop_h > 75.0
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
WITH recent_raw AS (
    SELECT
        a.mmsi,
        a.event_timestamp,
        coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) AS ts,
        a.latitude,
        a.longitude,
        a.speed,
        a.course,
        a.status,
        a.Destination,
        date_trunc('minute', coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        )) AS ts_min
    FROM ais_events_ice a
    WHERE coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '2' hour
      AND a.latitude BETWEEN 53.0 AND 60.5
      AND a.longitude BETWEEN 10.0 AND 21.0
      AND a.mmsi > 0
),
recent AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            row_number() OVER (PARTITION BY r.mmsi, r.ts_min ORDER BY r.ts DESC) AS rn
        FROM recent_raw r
    ) x
    WHERE rn = 1
),
pairs AS (
    SELECT
        a.mmsi AS mmsi_a,
        b.mmsi AS mmsi_b,
        a.event_timestamp,
        a.latitude,
        a.longitude,
        a.speed,
        a.course,
        a.status,
        a.Destination,
        a.ts_min,
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
        ) <= 500
      AND COALESCE(a.speed, 99) < 4.0
      AND COALESCE(b.speed, 99) < 4.0
),
pair_once AS (
    SELECT
        p.*,
        row_number() OVER (
            PARTITION BY p.mmsi_a, p.mmsi_b, p.ts_min
            ORDER BY p.dist_km
        ) AS rn
    FROM pairs p
)
SELECT
    mmsi_a AS mmsi,
    event_timestamp,
    latitude,
    longitude,
    speed,
    course,
    status,
    Destination,
    'Rendezvous' AS outlier_type,
    format(
        'STS candidate: %.0f m from MMSI %s, both SOG < 4 kn (same minute)',
        dist_km * 1000.0,
        CAST(mmsi_b AS varchar)
    ) AS reason,
    'Assess STS transfer risk; check sanctions lists' AS recommendation,
    60 AS severity_level
FROM pair_once p
WHERE rn = 1;

-- =========================================================================================
-- STEP 6: CASE 4a — PIPELINE/CABLE AIS BLACKOUT (spatial-lab query 08)
-- Requires: ais_daten, ostsee_pipelines_segmented (see create_db_tables_trino.sql)
-- Window: 7d AIS positions; gaps 30–1440 min vs pipeline segments
-- =========================================================================================

INSERT INTO infrastructure_ais_suspicions (
    MMSI, Pipeline_Name, Medium, Suspicion_Kind, Pattern,
    Event_Start, Event_End, Latitude, Longitude, Speed, Course, Status, Destination,
    Detail, Severity_Level, Updated_At
)
WITH ais_recent AS (
    SELECT *
    FROM ais_daten
    WHERE zeitstempel >= current_timestamp - interval '7' day
),
ais_gaps AS (
    SELECT
        mmsi,
        zeitstempel AS blackout_start,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS blackout_ende,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat,
        sog AS start_sog,
        LEAD(sog) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_sog,
        cog AS start_cog,
        LEAD(cog) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_cog
    FROM ais_recent
),
verdaechtige_luecken AS (
    SELECT
        mmsi,
        blackout_start,
        blackout_ende,
        date_diff('minute', blackout_start, blackout_ende) AS luecke_minuten,
        start_lon,
        start_lat,
        end_lon,
        end_lat,
        start_sog,
        end_sog,
        start_cog,
        ST_Distance(
            to_spherical_geography(ST_Point(start_lon, start_lat)),
            to_spherical_geography(ST_Point(end_lon, end_lat))
        ) / 1852.0 AS distanz_sm,
        (COALESCE(start_sog, 0) * (date_diff('minute', blackout_start, blackout_ende) / 60.0)) AS erwartete_distanz_sm,
        ST_Point(start_lon, start_lat) AS start_geom,
        ST_Point(end_lon, end_lat) AS end_geom,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS varchar) || ' ' || CAST(start_lat AS varchar) || ', ' ||
            CAST(end_lon AS varchar) || ' ' || CAST(end_lat AS varchar) || ')'
        ) AS luecken_linie,
        LEAST(start_lon, end_lon) - 0.02 AS box_min_lon,
        GREATEST(start_lon, end_lon) + 0.02 AS box_max_lon,
        LEAST(start_lat, end_lat) - 0.02 AS box_min_lat,
        GREATEST(start_lat, end_lat) + 0.02 AS box_max_lat
    FROM ais_gaps
    WHERE end_lon IS NOT NULL
      AND date_diff('minute', blackout_start, blackout_ende) BETWEEN 30 AND 1440
      AND NOT (COALESCE(start_sog, 0) < 0.5 AND COALESCE(end_sog, 0) < 0.5)
      AND blackout_ende >= current_timestamp - interval '24' hour
),
pipeline_treffer AS (
    SELECT
        v.mmsi,
        p.pipeline_name,
        p.medium,
        v.blackout_start,
        v.blackout_ende,
        v.luecke_minuten,
        v.start_lon,
        v.start_lat,
        v.end_lon,
        v.end_lat,
        v.start_sog,
        v.end_sog,
        v.start_cog,
        v.distanz_sm,
        v.erwartete_distanz_sm,
        MAX(CASE
            WHEN LEAST(
                ST_Distance(
                    to_spherical_geography(v.start_geom),
                    to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
                ),
                ST_Distance(
                    to_spherical_geography(v.start_geom),
                    to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
                )
            ) <= 2000 THEN 1 ELSE 0
        END) AS start_nah_an_pipeline,
        MAX(CASE
            WHEN LEAST(
                ST_Distance(
                    to_spherical_geography(v.end_geom),
                    to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
                ),
                ST_Distance(
                    to_spherical_geography(v.end_geom),
                    to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
                )
            ) <= 2000 THEN 1 ELSE 0
        END) AS ende_nah_an_pipeline,
        MAX(CASE
            WHEN ST_Intersects(v.luecken_linie, ST_GeometryFromText(p.seg_wkt)) THEN 1 ELSE 0
        END) AS kreuzt_waehrend_blackout
    FROM verdaechtige_luecken v
    JOIN ostsee_pipelines_segmented p
      ON v.box_max_lon >= p.min_lon
     AND v.box_min_lon <= p.max_lon
     AND v.box_max_lat >= p.min_lat
     AND v.box_min_lat <= p.max_lat
     AND (
         ST_Intersects(v.luecken_linie, ST_GeometryFromText(p.seg_wkt))
         OR LEAST(
             ST_Distance(
                 to_spherical_geography(v.start_geom),
                 to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
             ),
             ST_Distance(
                 to_spherical_geography(v.start_geom),
                 to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
             )
         ) <= 2000
         OR LEAST(
             ST_Distance(
                 to_spherical_geography(v.end_geom),
                 to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
             ),
             ST_Distance(
                 to_spherical_geography(v.end_geom),
                 to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
             )
         ) <= 2000
     )
    WHERE p.status = 'In use'
    GROUP BY
        v.mmsi, p.pipeline_name, p.medium, v.blackout_start, v.blackout_ende,
        v.luecke_minuten, v.start_lon, v.start_lat, v.end_lon, v.end_lat,
        v.start_sog, v.end_sog, v.start_cog, v.distanz_sm, v.erwartete_distanz_sm
),
blackout_labeled AS (
    SELECT
        mmsi,
        pipeline_name,
        medium,
        blackout_start,
        blackout_ende,
        luecke_minuten,
        start_lon,
        start_lat,
        end_lon,
        end_lat,
        start_sog,
        end_sog,
        start_cog,
        distanz_sm,
        erwartete_distanz_sm,
        CASE
            WHEN start_nah_an_pipeline = 1 AND ende_nah_an_pipeline = 1 THEN 'Stationärer Blackout direkt über Pipeline'
            WHEN start_nah_an_pipeline = 1 THEN 'AIS-Abschaltung direkt an Pipeline'
            WHEN ende_nah_an_pipeline = 1 THEN 'AIS-Reaktivierung direkt an Pipeline'
            WHEN kreuzt_waehrend_blackout = 1 THEN 'Pipeline-Überquerung im Dark-Modus'
            ELSE 'Sonstiger Korridor-Kontakt'
        END AS verdachtsmuster,
        CASE
            WHEN start_nah_an_pipeline = 1 AND ende_nah_an_pipeline = 1 THEN 90
            WHEN kreuzt_waehrend_blackout = 1 THEN 85
            WHEN start_nah_an_pipeline = 1 OR ende_nah_an_pipeline = 1 THEN 75
            ELSE 55
        END AS severity_level
    FROM pipeline_treffer
)
SELECT
    mmsi,
    pipeline_name,
    medium,
    'blackout',
    verdachtsmuster,
    blackout_start,
    blackout_ende,
    start_lat,
    start_lon,
    start_sog,
    start_cog,
    CAST(NULL AS varchar),
    CAST(NULL AS varchar),
    format(
        'gap=%s min; sog before/after=%.1f/%.1f kn; nm actual/expected=%.2f/%.2f; ratio=%.2f',
        luecke_minuten,
        COALESCE(start_sog, 0),
        COALESCE(end_sog, 0),
        distanz_sm,
        erwartete_distanz_sm,
        distanz_sm / NULLIF(erwartete_distanz_sm, 0)
    ),
    severity_level,
    current_timestamp
FROM blackout_labeled;

-- =========================================================================================
-- STEP 7: CASE 4b — PIPELINE CORRIDOR (spatial-lab query 07: crossings + slow transit)
-- =========================================================================================

INSERT INTO infrastructure_ais_suspicions (
    MMSI, Pipeline_Name, Medium, Suspicion_Kind, Pattern,
    Event_Start, Event_End, Latitude, Longitude, Speed, Course, Status, Destination,
    Detail, Severity_Level, Updated_At
)
WITH ais_recent AS (
    SELECT *
    FROM ais_daten
    WHERE zeitstempel >= current_timestamp - interval '7' day
),
schiffs_segmente AS (
    SELECT
        mmsi,
        zeitstempel AS start_zeit,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_zeit,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat,
        sog AS start_sog,
        LEAD(sog) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_sog,
        cog AS start_cog
    FROM ais_recent
),
fahrt_linien AS (
    SELECT
        mmsi,
        start_zeit,
        end_zeit,
        start_lon,
        start_lat,
        start_sog,
        start_cog,
        date_diff('second', start_zeit, end_zeit) / 60.0 AS dauer_minuten,
        (start_sog + COALESCE(end_sog, start_sog)) / 2.0 AS avg_sog,
        LEAST(start_lon, end_lon) AS ship_min_lon,
        GREATEST(start_lon, end_lon) AS ship_max_lon,
        LEAST(start_lat, end_lat) AS ship_min_lat,
        GREATEST(start_lat, end_lat) AS ship_max_lat,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS varchar) || ' ' || CAST(start_lat AS varchar) || ', ' ||
            CAST(end_lon AS varchar) || ' ' || CAST(end_lat AS varchar) || ')'
        ) AS segment_geom
    FROM schiffs_segmente
    WHERE end_lon IS NOT NULL
      AND date_diff('minute', start_zeit, end_zeit) <= 120
      AND end_zeit >= current_timestamp - interval '24' hour
),
korridor_ereignisse AS (
    SELECT
        f.mmsi,
        p.pipeline_name,
        p.medium,
        f.start_zeit,
        f.end_zeit,
        f.start_lon,
        f.start_lat,
        f.start_sog,
        f.start_cog,
        f.dauer_minuten,
        f.avg_sog,
        MAX(CASE WHEN ST_Intersects(f.segment_geom, ST_GeometryFromText(p.seg_wkt)) THEN 1 ELSE 0 END) AS ist_ueberquerung,
        MAX(CASE WHEN f.avg_sog < 3.0 THEN 1 ELSE 0 END) AS ist_langsamfahrt
    FROM fahrt_linien f
    JOIN ostsee_pipelines_segmented p
      ON f.ship_max_lon >= p.min_lon
     AND f.ship_min_lon <= p.max_lon
     AND f.ship_max_lat >= p.min_lat
     AND f.ship_min_lat <= p.max_lat
     AND ST_Intersects(f.segment_geom, ST_Buffer(ST_GeometryFromText(p.seg_wkt), 0.005))
    WHERE p.status = 'In use'
    GROUP BY
        f.mmsi, p.pipeline_name, p.medium, f.start_zeit, f.end_zeit,
        f.start_lon, f.start_lat, f.start_sog, f.start_cog, f.dauer_minuten, f.avg_sog
),
korridor_agg AS (
    SELECT
        mmsi,
        pipeline_name,
        medium,
        SUM(ist_ueberquerung) AS anzahl_exakte_ueberquerungen,
        SUM(CASE WHEN ist_langsamfahrt = 1 THEN dauer_minuten ELSE 0 END) AS minuten_langsam_im_korridor,
        MIN(avg_sog) AS min_geschwindigkeit_kn,
        MIN(start_zeit) AS erster_kontakt,
        MAX(end_zeit) AS letzter_kontakt,
        MIN_BY(start_lon, start_zeit) AS ref_lon,
        MIN_BY(start_lat, start_zeit) AS ref_lat,
        MIN_BY(start_sog, start_zeit) AS ref_sog,
        MIN_BY(start_cog, start_zeit) AS ref_cog
    FROM korridor_ereignisse
    GROUP BY mmsi, pipeline_name, medium
    HAVING SUM(ist_ueberquerung) > 1
        OR SUM(CASE WHEN ist_langsamfahrt = 1 THEN dauer_minuten ELSE 0 END) > 15
),
korridor_rows AS (
    SELECT
        mmsi,
        pipeline_name,
        medium,
        CASE
            WHEN anzahl_exakte_ueberquerungen > 1 THEN 'Pipeline Korridor Crossing'
            ELSE 'Pipeline Korridor Loitering'
        END AS outlier_type,
        CASE
            WHEN anzahl_exakte_ueberquerungen > 1 THEN 'Mehrfach-Überquerung Pipeline-Korridor'
            ELSE 'Langsamfahrt im Pipeline-Korridor'
        END AS pattern,
        erster_kontakt,
        letzter_kontakt,
        ref_lat,
        ref_lon,
        ref_sog,
        ref_cog,
        CASE
            WHEN anzahl_exakte_ueberquerungen > 1 AND minuten_langsam_im_korridor > 15 THEN 70
            WHEN anzahl_exakte_ueberquerungen > 1 THEN 65
            ELSE 60
        END AS severity_level,
        format(
            'crossings=%s; slow_min=%.1f; min_sog=%.2f kn',
            anzahl_exakte_ueberquerungen,
            minuten_langsam_im_korridor,
            min_geschwindigkeit_kn
        ) AS detail
    FROM korridor_agg
)
SELECT
    mmsi,
    pipeline_name,
    medium,
    'korridor',
    pattern,
    erster_kontakt,
    letzter_kontakt,
    ref_lat,
    ref_lon,
    ref_sog,
    ref_cog,
    CAST(NULL AS varchar),
    CAST(NULL AS varchar),
    detail,
    severity_level,
    current_timestamp
FROM korridor_rows;

-- =========================================================================================
-- STEP 8: CASE 5 — DAS CABLE ACOUSTIC THREATS (DasCableAcousticSimulator metadata)
-- =========================================================================================

INSERT INTO vessel_outliers (
    MMSI, Event_Timestamp, Latitude, Longitude, Speed, Course, Status, Destination,
    Outlier_Type, Reason, Recommendation, Severity_Level
)
SELECT
    0 AS mmsi,
    CAST(d.ingested_at AS varchar),
    d.segment_lat,
    d.segment_lon,
    CAST(NULL AS double),
    CAST(NULL AS double),
    'DAS',
    d.cable_asset,
    'DAS Cable Threat',
    format(
        'threat=%s; ml=%s (conf=%.2f); cable=%s; chainage=%.0f m; batch=%s#%s',
        d.threat_class,
        COALESCE(d.predicted_threat_type, '—'),
        COALESCE(d.model_confidence, 0),
        COALESCE(d.cable_name, d.cable_asset),
        d.chainage_m,
        COALESCE(d.filename, d.hadoop_file_url),
        CAST(d.segment_index AS varchar)
    ),
    'Correlate AIS blackout / patrol assets; dispatch ROV or guard vessel',
    CASE COALESCE(d.predicted_threat_type, d.threat_class)
        WHEN 'cui_sabotage_tap' THEN 95
        WHEN 'anchor_drag' THEN 88
        WHEN 'cable_strain_anomaly' THEN 80
        WHEN 'trawler_scrape' THEN 65
        ELSE 55
    END
FROM defense.das_cable_spectrogram_metadata d
WHERE d.ingested_at >= current_timestamp - interval '24' hour
  AND d.segment_lat IS NOT NULL
  AND d.segment_lon IS NOT NULL
  AND (
      d.threat_class IN ('cui_sabotage_tap', 'anchor_drag', 'cable_strain_anomaly', 'trawler_scrape')
      OR d.predicted_threat_type IN ('cui_sabotage_tap', 'anchor_drag', 'cable_strain_anomaly', 'trawler_scrape')
  );

-- Promote infrastructure hits into vessel_outliers for lagebild (AIS_Outlier)
INSERT INTO vessel_outliers (
    MMSI, Event_Timestamp, Latitude, Longitude, Speed, Course, Status, Destination,
    Outlier_Type, Reason, Recommendation, Severity_Level
)
SELECT
    MMSI,
    CAST(Event_Start AS varchar),
    Latitude,
    Longitude,
    Speed,
    Course,
    Status,
    Destination,
    CASE
        WHEN Suspicion_Kind = 'blackout' THEN 'Pipeline AIS Blackout'
        WHEN Pattern LIKE '%Überquerung%' OR Pattern LIKE '%Crossing%' THEN 'Pipeline Korridor Crossing'
        ELSE 'Pipeline Korridor Loitering'
    END,
    format('%s | asset=%s (%s) | %s', Pattern, Pipeline_Name, Medium, Detail),
    'Verify against cable/pipeline protection zone; correlate EO/MAD if available',
    Severity_Level
FROM infrastructure_ais_suspicions;

-- =========================================================================================
-- STEP 9: BALTIC THREAT TILE SKETCHES (fast tile-level risk for SOCMINT / NBA)
-- =========================================================================================
-- Rebuild snapshots per job (~11 km Bing tiles, 4h threats). O(tiles) lookup vs O(posts×ais).

DELETE FROM baltic_threat_tile_sketches;

INSERT INTO baltic_threat_tile_sketches (
    quadkey,
    severity_tdigest,
    outlier_type_set,
    mmsi_set,
    sensor_lane_set,
    event_count,
    sev_p50,
    sev_p90,
    updated_at
)
WITH threat_points AS (
    SELECT
        bing_tile_quadkey(bing_tile_at(o.Latitude, o.Longitude, 11)) AS quadkey,
        CAST(COALESCE(o.Severity_Level, 55) AS double) AS sev,
        COALESCE(o.Outlier_Type, 'Unknown') AS otype,
        CAST(COALESCE(o.MMSI, 0) AS varchar) AS mmsi_id,
        CASE
            WHEN o.Outlier_Type = 'DAS Cable Threat' THEN 'DAS'
            WHEN o.Outlier_Type LIKE 'Pipeline%' THEN 'INFRA'
            ELSE 'AIS'
        END AS lane
    FROM vessel_outliers o
    WHERE o.Latitude BETWEEN 53.0 AND 66.5
      AND o.Longitude BETWEEN 9.0 AND 31.0
      AND coalesce(
          try(from_iso8601_timestamp(o.Event_Timestamp)),
          try_cast(replace(o.Event_Timestamp, 'Z', '') AS timestamp)
      ) >= current_timestamp - interval '4' hour
    UNION ALL
    SELECT
        bing_tile_quadkey(bing_tile_at(g.latitude, g.longitude, 11)) AS quadkey,
        75.0 AS sev,
        COALESCE(g.event_type, 'gps_jammer_event') AS otype,
        'GPS' AS mmsi_id,
        'GPS_EW' AS lane
    FROM defense.gps_jammer_events g
    WHERE g.jamming_indicator = TRUE
      AND g.latitude BETWEEN 53.0 AND 66.5
      AND g.longitude BETWEEN 9.0 AND 31.0
      AND coalesce(
          try(from_iso8601_timestamp(g.ts)),
          try_cast(replace(g.ts, 'Z', '') AS timestamp)
      ) >= current_timestamp - interval '4' hour
),
sk AS (
    SELECT
        quadkey,
        tdigest_agg(sev) AS sev_td,
        make_set_digest(otype) AS type_sd,
        make_set_digest(mmsi_id) AS mmsi_sd,
        make_set_digest(lane) AS lane_sd,
        COUNT(*) AS event_count
    FROM threat_points
    WHERE quadkey IS NOT NULL
    GROUP BY quadkey
)
SELECT
    quadkey,
    CAST(sev_td AS varbinary),
    CAST(type_sd AS varbinary),
    CAST(mmsi_sd AS varbinary),
    CAST(lane_sd AS varbinary),
    event_count,
    value_at_quantile(sev_td, 0.5),
    value_at_quantile(sev_td, 0.9),
    current_timestamp
FROM sk;

CREATE OR REPLACE VIEW threat_hot_tiles AS
SELECT
    quadkey,
    event_count,
    sev_p50,
    sev_p90,
    updated_at
FROM baltic_threat_tile_sketches
WHERE sev_p90 >= 60
   OR event_count >= 3;