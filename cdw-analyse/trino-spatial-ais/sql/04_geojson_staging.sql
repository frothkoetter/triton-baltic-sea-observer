-- GeoJSON Staging: Roh-JSON + ostsee_pipelines (nur WKT varchar — Iceberg ohne Geometry-Typ)
USE iceberg.defense;

DROP TABLE IF EXISTS raw_ostsee_geojson;
CREATE TABLE raw_ostsee_geojson (
    json_payload varchar,
    quelle varchar
);

INSERT INTO raw_ostsee_geojson (json_payload, quelle) VALUES
(
 '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"Nord Stream 1 Mock GeoJSON","medium":"Gas","status":"In use","operator":"Mock","country":"DE-RU"},"geometry":{"type":"MultiLineString","coordinates":[[[13.6512,54.1523],[14.8045,54.8511],[16.2019,55.4088],[19.5,57.2]]]}}]}',
 'lab_insert'
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
    'LINESTRING(13.6512 54.1523, 14.8045 54.8511, 16.2019 55.4088, 19.5 57.2)',
    'GeoJSON'
FROM raw_ostsee_geojson
WHERE quelle = 'lab_insert';
