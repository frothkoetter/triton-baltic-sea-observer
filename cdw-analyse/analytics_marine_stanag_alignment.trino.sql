-- Align STANAG marine_messages with marine_vessel_status (German Navy).
-- marine_vessel_status.event_timestamp is VARCHAR (NiFi ISO-like: '2026-11-18 08:03:21.882').
-- marine_messages.message_timestamp is TIMESTAMP WITH TIME ZONE (parsed from STANAG DTG).
-- Do not equate timestamps directly; join on name/MMSI plus a time window (processors run on different schedules).

WITH vessel_status AS (
    SELECT
        mmsi,
        latitude,
        longitude,
        speed,
        course,
        status,
        coalesce(
            try(from_iso8601_timestamp(event_timestamp)),
            try(from_iso8601_timestamp(replace(event_timestamp, 'Z', '+00:00'))),
            try(date_parse(event_timestamp, '%Y-%m-%d %H:%i:%s.%f')),
            try(date_parse(event_timestamp, '%Y-%m-%d %H:%i:%s'))
        ) AS event_ts
    FROM iceberg.defense.marine_vessel_status
    WHERE event_timestamp IS NOT NULL
),
marine_msg AS (
    SELECT
        message_id,
        message_from,
        message_dtg,
        message_timestamp,
        message_position,
        coalesce(
            message_latitude,
            CAST(regexp_extract(message_position, 'LAT\s+([0-9]+\.[0-9]+)', 1) AS double)
        ) AS msg_lat,
        coalesce(
            message_longitude,
            CAST(regexp_extract(message_position, 'LON\s+([0-9]+\.[0-9]+)', 1) AS double)
        ) AS msg_lon
    FROM iceberg.defense.marine_messages
    WHERE message_timestamp IS NOT NULL
),
paired AS (
    SELECT
        fl.mmsi,
        fl.name AS unit_name,
        ms.message_id,
        ms.message_dtg,
        ms.message_timestamp,
        ms.msg_lat,
        ms.msg_lon,
        st.event_ts,
        st.latitude AS status_lat,
        st.longitude AS status_lon,
        st.speed,
        st.status,
        abs(date_diff('minute', st.event_ts, ms.message_timestamp)) AS time_diff_min,
        abs(ms.msg_lat - st.latitude) AS dlat,
        abs(ms.msg_lon - st.longitude) AS dlon
    FROM iceberg.defense.german_navy_fleet fl
    INNER JOIN marine_msg ms ON ms.message_from = fl.name
    INNER JOIN vessel_status st ON st.mmsi = fl.mmsi
    WHERE st.event_ts IS NOT NULL
      AND abs(date_diff('minute', st.event_ts, ms.message_timestamp)) <= 30
)
SELECT *
FROM paired
WHERE mmsi = 'MAR123400'
ORDER BY time_diff_min ASC, message_timestamp DESC
LIMIT 50;