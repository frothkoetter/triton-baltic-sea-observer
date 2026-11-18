-- Minimal spatial lab: WKT AOIs + AIS + harbours (Trino / Iceberg)
-- observation_areas: apply sql/06_tier_a_observation_areas.sql first (EMODnet Tier A).

USE iceberg.defense;

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