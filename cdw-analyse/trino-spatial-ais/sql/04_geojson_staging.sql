-- GeoJSON Staging: Roh-JSON + ostsee_pipelines (nur WKT varchar — Iceberg ohne Geometry-Typ)
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

INSERT INTO ostsee_pipelines (pipeline_name, medium, status, geo_typ, geometrie_wkt, quelle)
SELECT
    json_extract_scalar(json_payload, '$.features[0].properties.name'),
    json_extract_scalar(json_payload, '$.features[0].properties.medium'),
    COALESCE(json_extract_scalar(json_payload, '$.features[0].properties.status'), 'Unknown'),
    json_extract_scalar(json_payload, '$.features[0].geometry.type'),
    'LINESTRING(13.65426161 54.15419366, 15.70063987 55.53579754, 19.77499176 57.92896757, 23.99979769 59.65753773)',
    'GeoJSON'
FROM raw_ostsee_geojson
WHERE quelle = 'emodnet_seed';
