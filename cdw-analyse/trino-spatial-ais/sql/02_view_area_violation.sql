-- AIS points inside WKT AOIs → enter/exit transitions (Trino)
CREATE OR REPLACE VIEW area_violation AS
WITH observation_polygons AS (
    SELECT
        area_id,
        ST_GeometryFromText(polygon) AS geom
    FROM defense.observation_areas
),
ship_positions AS (
    SELECT
        e.mmsi,
        e.event_timestamp,
        e.latitude,
        e.longitude,
        o.area_id
    FROM defense.ais_events_ice e
    JOIN observation_polygons o
      ON ST_Within(ST_Point(e.longitude, e.latitude), o.geom)
),
ship_transitions AS (
    SELECT
        mmsi,
        area_id,
        event_timestamp,
        CASE
            WHEN LAG(area_id) OVER (PARTITION BY mmsi ORDER BY event_timestamp) IS NULL
                 OR LAG(area_id) OVER (PARTITION BY mmsi ORDER BY event_timestamp) != area_id
            THEN 'enter'
            ELSE 'exit'
        END AS transition
    FROM ship_positions
)
SELECT mmsi, area_id, event_timestamp, transition
FROM ship_transitions;
