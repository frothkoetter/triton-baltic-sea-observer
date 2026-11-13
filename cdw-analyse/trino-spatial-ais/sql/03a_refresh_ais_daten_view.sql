-- Hotfix: ais_events_ice uses ISO (…T…Z) or CDW-style "yyyy-MM-dd HH:mm:ss.SSS"
USE iceberg.defense;

CREATE OR REPLACE VIEW ais_daten AS
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
