-- Remove duplicate rus_fleet rows (same MMSI from additive DDL re-runs).
--   SELECT mmsi, COUNT(*) FROM iceberg.defense.rus_fleet GROUP BY 1 HAVING COUNT(*) > 1;

CREATE TABLE IF NOT EXISTS rus_fleet_dedup (
    class varchar,
    name varchar,
    mmsi varchar,
    jahr_in_dienst integer,
    besatzung integer,
    home_base varchar,
    base_role varchar,
    data_source varchar,
    hull_number varchar,
    vessel_category varchar,
    default_mission_state varchar
);

DELETE FROM rus_fleet_dedup;

INSERT INTO rus_fleet_dedup (
    class,
    name,
    mmsi,
    jahr_in_dienst,
    besatzung,
    home_base,
    base_role,
    data_source,
    hull_number,
    vessel_category,
    default_mission_state
)
SELECT
    class,
    name,
    mmsi,
    jahr_in_dienst,
    besatzung,
    home_base,
    base_role,
    data_source,
    hull_number,
    vessel_category,
    default_mission_state
FROM (
  SELECT
    *,
    row_number() OVER (PARTITION BY mmsi ORDER BY name) AS rn
  FROM rus_fleet
) t
WHERE rn = 1;