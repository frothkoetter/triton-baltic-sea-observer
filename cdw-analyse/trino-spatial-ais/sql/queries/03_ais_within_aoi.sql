-- E2E check: latest AIS positions inside a named SurveillanceArea (WKT)
WITH observation_polygons AS (
  SELECT area_id, ST_GeometryFromText(polygon) AS geom
  FROM defense.observation_areas
  WHERE area_id = 'Bornholm Basin'
),
latest_ais AS (
  SELECT a.*,
         ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY event_timestamp DESC) AS rn
  FROM defense.ais_events_ice a
  WHERE a.latitude IS NOT NULL AND a.longitude IS NOT NULL
)
SELECT
  l.mmsi,
  l.event_timestamp,
  l.latitude,
  l.longitude,
  o.area_id
FROM latest_ais l
CROSS JOIN observation_polygons o
WHERE l.rn = 1
  AND ST_Within(ST_Point(l.longitude, l.latitude), o.geom)
ORDER BY l.event_timestamp DESC;
