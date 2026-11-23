-- Remove duplicate german_navy_fleet rows (same MMSI from additive DDL re-runs).
-- Run in Trino after verifying counts:
--   SELECT mmsi, COUNT(*) FROM iceberg.defense.german_navy_fleet GROUP BY 1 HAVING COUNT(*) > 1;

CREATE TABLE IF NOT EXISTS german_navy_fleet_dedup (
    class varchar,
    name varchar,
    mmsi varchar,
    jahr_in_dienst integer,
    besatzung integer
);

DELETE FROM german_navy_fleet_dedup;

INSERT INTO german_navy_fleet_dedup (class, name, mmsi, jahr_in_dienst, besatzung)
SELECT class, name, mmsi, jahr_in_dienst, besatzung
FROM (
  SELECT
    class,
    name,
    mmsi,
    jahr_in_dienst,
    besatzung,
    row_number() OVER (PARTITION BY mmsi ORDER BY name) AS rn
  FROM german_navy_fleet
) t
WHERE rn = 1;

-- Optional: swap tables when TRINO_SKIP_DROP=0 and you may recreate german_navy_fleet:
-- DROP TABLE german_navy_fleet;
-- CREATE TABLE german_navy_fleet AS SELECT * FROM german_navy_fleet_dedup;