-- AIS tracks within 30 km of PortFacility Kiel (great-circle style ST_Distance on points)
SELECT
  a.mmsi,
  a.event_timestamp,
  a.latitude,
  a.longitude,
  h.name AS harbour_name,
  ST_Distance(
    ST_Point(a.longitude, a.latitude),
    ST_Point(h.longitude, h.latitude)
  ) AS distance_deg_approx
FROM defense.ais_events_ice a
CROSS JOIN defense.baltic_sea_harbours h
WHERE h.name = 'Kiel'
  AND a.latitude IS NOT NULL
ORDER BY a.event_timestamp DESC
LIMIT 50;
