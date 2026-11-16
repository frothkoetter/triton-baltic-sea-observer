-- AIS routes per MMSI from ais_events_ice (iceberg.defense).
-- Metadata columns + geo_json (LineString geometry only).
-- Run: ./run_vessel_route_geojson.sh
-- Doc: README_vessel_route_geojson.md

USE iceberg.defense;

CREATE OR REPLACE VIEW vessel_ais_route_geojson AS
WITH events AS (
    SELECT
        a.mmsi,
        a.latitude,
        a.longitude,
        a.Destination,
        coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) AS ts
    FROM ais_events_ice a
    WHERE a.latitude BETWEEN -85.0 AND 85.0
      AND a.longitude BETWEEN -180.0 AND 180.0
      AND coalesce(
            try(from_iso8601_timestamp(a.event_timestamp)),
            try_cast(replace(a.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '24' hour
),
ranked AS (
    SELECT
        e.*,
        row_number() OVER (PARTITION BY e.mmsi ORDER BY e.ts) AS rn,
        count(*) OVER (PARTITION BY e.mmsi) AS n
    FROM events e
),
sampled AS (
    SELECT *
    FROM ranked
    WHERE n >= 2
      AND (
        rn = 1
        OR rn = n
        OR mod(rn, greatest(1, CAST(n / 120 AS integer))) = 0
      )
),
agg AS (
    SELECT
        mmsi,
        min(ts) AS start_timestamp,
        max(ts) AS end_timestamp,
        min_by(Destination, ts) AS start_destination,
        max_by(Destination, ts) AS end_destination,
        min_by(longitude, ts) AS start_lon,
        min_by(latitude, ts) AS start_lat,
        max_by(longitude, ts) AS end_lon,
        max_by(latitude, ts) AS end_lat,
        array_agg(ARRAY[longitude, latitude] ORDER BY ts) AS coordinates,
        count(*) AS point_count
    FROM sampled
    GROUP BY mmsi
),
start_ports AS (
    SELECT mmsi, name AS start_port
    FROM (
        SELECT
            a.mmsi,
            h.name,
            row_number() OVER (
                PARTITION BY a.mmsi
                ORDER BY ST_Distance(
                    to_spherical_geography(ST_Point(h.longitude, h.latitude)),
                    to_spherical_geography(ST_Point(a.start_lon, a.start_lat))
                )
            ) AS rn
        FROM agg a
        CROSS JOIN baltic_sea_harbours h
    ) t
    WHERE rn = 1
),
end_ports AS (
    SELECT mmsi, name AS end_port
    FROM (
        SELECT
            a.mmsi,
            h.name,
            row_number() OVER (
                PARTITION BY a.mmsi
                ORDER BY ST_Distance(
                    to_spherical_geography(ST_Point(h.longitude, h.latitude)),
                    to_spherical_geography(ST_Point(a.end_lon, a.end_lat))
                )
            ) AS rn
        FROM agg a
        CROSS JOIN baltic_sea_harbours h
    ) t
    WHERE rn = 1
),
geo AS (
    SELECT
        a.mmsi,
        a.start_timestamp,
        a.end_timestamp,
        coalesce(sp.start_port, a.start_destination, 'Unknown') AS start_port,
        coalesce(ep.end_port, a.end_destination, 'Unknown') AS end_port,
        a.point_count,
        concat(
            '[',
            array_join(
                transform(
                    a.coordinates,
                    p -> format('[%s,%s]', cast(p[1] AS varchar), cast(p[2] AS varchar))
                ),
                ','
            ),
            ']'
        ) AS coords_json
    FROM agg a
    LEFT JOIN start_ports sp ON a.mmsi = sp.mmsi
    LEFT JOIN end_ports ep ON a.mmsi = ep.mmsi
)
SELECT
    'Route' AS route_type,
    g.mmsi,
    g.start_timestamp,
    g.end_timestamp,
    g.start_port,
    g.end_port,
    g.point_count,
    (g.end_timestamp - g.start_timestamp) AS journey_duration,
    date_diff('second', g.start_timestamp, g.end_timestamp) AS journey_duration_seconds,
    format(
        '{"type":"LineString","coordinates":%s}',
        g.coords_json
    ) AS geo_json
FROM geo g;

-- Example: single vessel
-- SELECT route_type, mmsi, start_port, end_port, journey_duration, geo_json
-- FROM vessel_ais_route_geojson WHERE mmsi = 311040004;

-- Example: full GeoJSON Feature (map tools)
-- SELECT format(
--   '{"type":"Feature","properties":{"type":"%s","mmsi":%s,"start_timestamp":"%s","end_timestamp":"%s","start_port":"%s","end_port":"%s","point_count":%s,"journey_duration_seconds":%s},"geometry":%s}',
--   route_type, cast(mmsi as varchar), cast(start_timestamp as varchar), cast(end_timestamp as varchar),
--   replace(start_port, '"', '\"'), replace(end_port, '"', '\"'), cast(point_count as varchar),
--   cast(journey_duration_seconds as varchar), geo_json
-- ) AS route_feature
-- FROM vessel_ais_route_geojson WHERE mmsi = 311040004;

-- Example: FeatureCollection (cap at 50 routes)
-- SELECT format('{"type":"FeatureCollection","features":[%s]}',
--   array_join(array_agg(route_feature), ','))
-- FROM (
--   SELECT format(
--     '{"type":"Feature","properties":{"type":"%s","mmsi":%s,"start_timestamp":"%s","end_timestamp":"%s","start_port":"%s","end_port":"%s","point_count":%s,"journey_duration_seconds":%s},"geometry":%s}',
--     route_type, cast(mmsi as varchar), cast(start_timestamp as varchar), cast(end_timestamp as varchar),
--     replace(start_port, '"', '\"'), replace(end_port, '"', '\"'), cast(point_count as varchar),
--     cast(journey_duration_seconds as varchar), geo_json
--   ) AS route_feature
--   FROM vessel_ais_route_geojson
--   ORDER BY mmsi
--   LIMIT 50
-- ) t;