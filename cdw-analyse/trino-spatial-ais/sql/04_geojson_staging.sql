-- GeoJSON staging table only; ostsee_pipelines is filled from ostsee_infrastruktur (EMODnet WKT).
USE iceberg.defense;

DROP TABLE IF EXISTS raw_ostsee_geojson;
CREATE TABLE raw_ostsee_geojson (
    json_payload varchar,
    quelle varchar
);

INSERT INTO raw_ostsee_geojson (json_payload, quelle) VALUES
(
 '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"NORD STREAM 1","medium":"Gas","status":"In use","operator":"EMODnet","country":"DE-RU"},"geometry":{"type":"MultiLineString","coordinates":[[[13.65426161,54.15419366],[15.70063987,55.53579754],[19.77499176,57.92896757],[23.99979769,59.65753773]]]}}]}',
 'emodnet_seed'
);

DROP TABLE IF EXISTS ostsee_pipelines;
CREATE TABLE ostsee_pipelines (
    pipeline_name varchar,
    medium varchar,
    status varchar,
    geo_typ varchar,
    geometrie_wkt varchar,
    quelle varchar
);

INSERT INTO ostsee_pipelines (pipeline_name, medium, status, geo_typ, geometrie_wkt, quelle)
SELECT
    name,
    medium,
    status,
    CASE
        WHEN geometrie_wkt LIKE 'MULTILINESTRING%' THEN 'MultiLineString'
        ELSE 'LineString'
    END,
    geometrie_wkt,
    'WKT'
FROM ostsee_infrastruktur;
-- Do not duplicate pipelines from raw_ostsee_geojson: staging JSON is legacy/simplified;
-- ostsee_infrastruktur (EMODnet WKT) is the sole source for ostsee_pipelines.
