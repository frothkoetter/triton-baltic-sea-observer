-- Minimal spatial lab: WKT AOIs + AIS + harbours (Trino / Iceberg)
-- Run after: CREATE SCHEMA iceberg.defense (see ../create_db_tables_trino.sql)

USE iceberg.defense;

DROP TABLE IF EXISTS observation_areas;
CREATE TABLE observation_areas (
  area_id varchar,
  polygon varchar COMMENT 'OGC WKT POLYGON',
  center_latitude double,
  center_longitude double
);

INSERT INTO observation_areas (area_id, polygon, center_latitude, center_longitude) VALUES
('Bornholm Basin', 'POLYGON((17.237478356677286 56.03107993705109, 17.237478356677286 55.9047066587936, 18.31712997618766 55.9047066587936, 18.317129976129976 56.03107993705109, 17.237478356677286 56.03107993705109))', 55.9679, 17.7773),
('Gdańsk Deep', 'POLYGON((18.29518008728172 55.53540464392404, 18.29518008728172 55.22756184778888, 19.28762566297803 55.22756184778888, 19.28762566297803 55.53540464392404, 18.29518008728172 55.53540464392404))', 55.3815, 18.7914);

DROP TABLE IF EXISTS ais_events_ice;
CREATE TABLE ais_events_ice (
  mmsi bigint,
  event_timestamp varchar,
  latitude double,
  longitude double,
  speed double,
  course double,
  status varchar,
  Destination varchar
);

DROP TABLE IF EXISTS baltic_sea_harbours;
CREATE TABLE baltic_sea_harbours (
  name varchar,
  country varchar,
  latitude double,
  longitude double
);

INSERT INTO baltic_sea_harbours (name, country, latitude, longitude) VALUES
('Kiel', 'Germany', 54.3233, 10.1228),
('Gdynia', 'Poland', 54.5189, 18.5305),
('Rostock', 'Germany', 54.0887, 12.1405);
