-- Unterseekabel & Pipelines (OGC WKT: LINESTRING / MULTILINESTRING) — defense schema
USE iceberg.defense;

DROP TABLE IF EXISTS ostsee_infrastruktur;
CREATE TABLE ostsee_infrastruktur (
    id varchar,
    name varchar,
    typ varchar,
    geometrie_wkt varchar,
    status varchar,
    medium varchar
);

INSERT INTO ostsee_infrastruktur (id, name, typ, geometrie_wkt, status, medium) VALUES
('NS1_MOCK', 'Ostsee Pipeline Route A (Beispiel)', 'Gaspipeline',
 'LINESTRING(13.6500 54.1500, 14.8000 54.8500, 16.2000 55.4000, 19.5000 57.2000, 28.0500 59.5000)',
 'In use', 'Gas'),
('CABLE_MOCK', 'Baltic Sea Kabel B (Beispiel)', 'Seekabel',
 'LINESTRING(14.7000 55.1000, 15.8000 55.6000, 17.9000 56.8000)',
 'In use', 'Telecom/Power');

-- AIS alias view (Draft: ais_daten) → Triton ais_events_ice
DROP VIEW IF EXISTS ais_daten;
CREATE VIEW ais_daten AS
SELECT
    mmsi,
    COALESCE(
        TRY(from_iso8601_timestamp(event_timestamp)),
        TRY(date_parse(event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
        TRY(date_parse(event_timestamp, '%Y-%m-%d %H:%i:%s')),
        TRY(from_iso8601_timestamp(replace(event_timestamp, ' ', 'T')))
    ) AS zeitstempel,
    longitude AS lon,
    latitude AS lat,
    speed AS sog,
    course AS cog
FROM ais_events_ice
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND COALESCE(
        TRY(from_iso8601_timestamp(event_timestamp)),
        TRY(date_parse(event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
        TRY(date_parse(event_timestamp, '%Y-%m-%d %H:%i:%s')),
        TRY(from_iso8601_timestamp(replace(event_timestamp, ' ', 'T')))
      ) IS NOT NULL;
