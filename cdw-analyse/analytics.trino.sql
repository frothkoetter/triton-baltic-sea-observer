WITH parsed_messages AS (
  SELECT
    message_subject,
    message_text,
    message_from,
    message_dtg,
    message_id,
    message_to,
    message_position,
    -- Extract and cast the latitude value using a more flexible pattern
    CAST(regexp_extract(message_position, 'LAT[^0-9.]+([0-9.]+)', 1) AS DOUBLE) AS latitude,
    -- Extract and cast the longitude value using a more flexible pattern
    CAST(regexp_extract(message_position, 'LON[^0-9.]+([0-9.]+)', 1) AS DOUBLE) AS longitude
  FROM
    iceberg.defense.maritime_surveillance_reports
  WHERE
    message_position IS NOT NULL AND message_position <> ''
),

latest_buoy_position AS (
  -- Find the most recent geographical position for the buoy 'MAD-098'.
  SELECT
    geo_position_lat AS buoy_lat,
    geo_position_lon AS buoy_lon
  FROM buoy_data
--  WHERE buoyid = 'MAD-098'
  ORDER BY ts DESC
  LIMIT 1
)

SELECT
  pm.message_id,
  pm.message_subject,
  pm.message_text,
  pm.message_from,
  pm.message_dtg,
  pm.latitude,
  pm.longitude,
  -- Calculate the spherical distance in meters using Trino's ST_Distance function.
  ST_Distance(
    to_spherical_geography(ST_Point(latest.buoy_lon, latest.buoy_lat)),
    to_spherical_geography(ST_Point(pm.longitude, pm.latitude))
  ) AS distance_meters,
  -- Convert the distance from meters to nautical miles (1 NM = 1852 meters).
  ST_Distance(
    to_spherical_geography(ST_Point(latest.buoy_lon, latest.buoy_lat)),
    to_spherical_geography(ST_Point(pm.longitude, pm.latitude))
  ) / 1852.0 AS distance_nautical_miles
FROM
  parsed_messages AS pm
CROSS JOIN
  latest_buoy_position AS latest
WHERE
  -- The final filter condition ensures we only return events within the 10 nautical mile radius.
  ST_Distance(
    to_spherical_geography(ST_Point(latest.buoy_lon, latest.buoy_lat)),
    to_spherical_geography(ST_Point(pm.longitude, pm.latitude))
  ) <= 50 * 1852.0;
