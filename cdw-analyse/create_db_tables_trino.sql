-- CDW Trino / Iceberg: connect to catalog ``iceberg``, schema ``defense``
-- (JDBC host cdw-aw-se-trino…cloudera.site:443). Impala-style ``STORED BY ICEBERG`` removed.
-- Full local apply: ./setup_cdw_trino_local.sh (+ sql/trino_supplemental_ddl.order for NBA/views)
-- Or: TRINO_USER=… TRINO_PASSWORD=… python3 run_trino_ddl.py
-- Re-run: DROP TABLE IF EXISTS before each CREATE; safe to re-apply full DDL+INSERT.

CREATE SCHEMA IF NOT EXISTS iceberg.defense;
USE iceberg.defense;
DROP TABLE IF EXISTS ais_events_ice;
CREATE TABLE ais_events_ice(
  mmsi bigint COMMENT 'Maritime Mobile Service Identity, a unique 9-digit vessel identifier.',
  event_timestamp varchar COMMENT 'The time and date the AIS event was recorded (typically stored as ISO 8601 or similar format).',
  latitude double COMMENT 'The geographical latitude of the vessel (decimal degrees, -90 to +90).',
  longitude double COMMENT 'The geographical longitude of the vessel (decimal degrees, -180 to +180).',
  speed double COMMENT 'Speed Over Ground (SOG) of the vessel in knots or a standardized unit.',
  course double COMMENT 'Course Over Ground (COG) of the vessel in degrees (0 to 359.9).',
  status varchar COMMENT 'The navigation status of the vessel (e.g., Underway, Moored, Anchored).',
  Destination varchar COMMENT 'The reported destination of the vessel.'
);

DROP TABLE IF EXISTS rus_naval_fleet_positions;
CREATE TABLE rus_naval_fleet_positions(
  mmsi varchar COMMENT 'Synthetic Russian Baltic Fleet identifier (e.g. RUS273xxx; not a real 9-digit MMSI).',
  event_timestamp varchar COMMENT 'The time and date the AIS event was recorded (typically stored as ISO 8601 or similar format).',
  latitude double COMMENT 'The geographical latitude of the vessel (decimal degrees, -90 to +90).',
  longitude double COMMENT 'The geographical longitude of the vessel (decimal degrees, -180 to +180).',
  speed double COMMENT 'Speed Over Ground (SOG) of the vessel in knots or a standardized unit.',
  course double COMMENT 'Course Over Ground (COG) of the vessel in degrees (0 to 359.9).',
  status varchar COMMENT 'The navigation status of the vessel (e.g., Underway, Moored, Anchored).',
  Destination varchar COMMENT 'The reported destination of the vessel.'
);

-- Tier A military / restricted areas (EMODnet). Regenerate via scripts/build_emodnet_tier_a_restricted.py

DROP TABLE IF EXISTS observation_areas;
CREATE TABLE observation_areas (
  area_id varchar COMMENT 'Stable id for map + area_violation joins.',
  polygon varchar COMMENT 'OGC WKT POLYGON (lon lat).',
  center_latitude double,
  center_longitude double,
  country varchar COMMENT 'Reporting state from EMODnet.',
  area_type varchar COMMENT 'Firing Area, National Defence Area, …',
  tier varchar COMMENT 'A = tactically curated subset.',
  source varchar COMMENT 'Provenance.'
);

INSERT INTO observation_areas (
  area_id, polygon, center_latitude, center_longitude,
  country, area_type, tier, source
) VALUES
('DK-Firing-Kattegat', 'POLYGON ((10.49869315 58.04946684, 10.2153316 58.3661459, 10.4986758 58.49948646, 10.88205235 58.04947026, 10.49869315 58.04946684))', 58.244337, 10.532022, 'Denmark', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('DK-Firing-Skagerrak', 'POLYGON ((9.61534508 57.18276287, 9.53201035 57.11609303, 9.34866823 57.11609119, 9.24032727 57.1827591, 9.24032532 57.24109556, 9.39866234 57.31609659, 9.49866744 57.31609758, 9.61534508 57.18276287))', 57.209163, 9.423227, 'Denmark', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('EE-Defence-West-Estonia-24.3', 'POLYGON ((24.1999995 59.62844144, 24.28833293 59.64999998, 24.44166705 59.64999996, 24.44253504 59.53333396, 24.20000014 59.53333299, 24.1999995 59.62844144))', 59.589846, 24.324246, 'Estonia', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('FI-Defence-Helsinki-Approaches-24.2', 'POLYGON ((24.32780578 59.93760706, 24.35052746 59.92761237, 24.39514207 59.93798387, 24.440017 59.81177521, 24.08314521 59.77020252, 24.10748484 59.90306585, 24.00088921 59.92852552, 24.35922662 60.04672389, 24.38739852 60.01675102, 24.32780578 59.93760706))', 59.894373, 24.244816, 'Finland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('FI-Defence-Helsinki-Approaches-24.8', 'POLYGON ((24.77655359 60.13488342, 24.79405615 60.13238383, 24.77156072 60.13072304, 24.77655359 60.13488342))', 60.132663, 24.780723, 'Finland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('FI-Defence-Helsinki-Approaches-24.9', 'POLYGON ((24.90433281 60.09739061, 24.91656962 60.10405741, 24.9235089 60.1026671, 24.91211412 60.09461359, 24.90433281 60.09739061))', 60.099488, 24.913915, 'Finland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('FI-Defence-Helsinki-Approaches-25.0', 'POLYGON ((25.08824168 60.15017469, 25.06078341 60.12412515, 24.9871583 60.1246151, 24.99405947 60.14367294, 25.04918214 60.15808036, 25.08824168 60.15017469))', 60.139179, 25.036743, 'Finland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('FI-Defence-Helsinki-Approaches-25.1', 'POLYGON ((24.99623325 59.97705018, 25.02841691 60.0780288, 25.00377424 60.1042976, 25.0105273 60.11099832, 25.07892312 60.11143437, 25.12811078 60.09125666, 25.13444965 59.97907647, 24.99623325 59.97705018))', 60.040248, 25.07008, 'Finland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('DE-Firing-Bay-Kiel', 'POLYGON ((13.6638317 55.08349735, 14.01154432 55.07507442, 14.10434318 54.91666771, 13.66319123 54.91660231, 13.6638317 55.08349735))', 54.994677, 13.859742, 'Germany', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('DE-Firing-Bornholm-Bight', 'POLYGON ((15.24968165 55.50025778, 15.8335062 55.50032123, 15.83319975 54.91690263, 15.24994627 54.91690263, 15.24968165 55.50025778))', 55.208644, 15.541589, 'Germany', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('DE-Firing-Ostsee-Central', 'POLYGON ((13.85830781 54.63054244, 13.95713398 54.66653683, 14.33556346 54.66679063, 14.28407192 54.33299984, 14.13368999 54.33347574, 14.1327382 54.44959343, 13.99948839 54.45054522, 13.85830781 54.63054244))', 54.534106, 14.137062, 'Germany', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('DE-Firing-Putlos-Todendorf', 'POLYGON ((10.69445155 54.30651862, 10.63040295 54.35148339, 10.54585243 54.37770264, 10.46612971 54.45316955, 10.45185295 54.49143128, 10.45927687 54.50799233, 10.59976024 54.54339871, 10.72425364 54.53540372, 10.82933064 54.485416, 10.87158987 54.44810605, 10.866293 54.35444186, 10.76969178 54.30722723, 10.69445155 54.30651862))', 54.432617, 10.678147, 'Germany', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('PL-Defence-Gdansk-Bight', 'POLYGON ((19.35733408 54.39849968, 19.23433904 54.39861978, 19.02503061 54.43545825, 19.21399981 54.68183331, 19.3854994 54.56516679, 19.38233379 54.4651664, 19.35733408 54.39849968))', 54.512213, 19.232221, 'Poland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('PL-Defence-Slupsk-Bank', 'POLYGON ((16.77266667 54.78383333, 16.84519072 54.58727085, 16.53554276 54.54221936, 16.42737037 54.48318987, 16.14616667 54.59, 16.2825 54.67916667, 16.77266667 54.78383333))', 54.631922, 16.525385, 'Poland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('PL-Defence-Swinoujscie', 'POLYGON ((14.88270499 54.0523473, 14.77733219 54.03293851, 14.73966667 54.09516667, 14.84466667 54.1185, 14.88270499 54.0523473))', 54.074925, 14.811528, 'Poland', 'National Defence Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('SE-Firing-Gotland-East', 'POLYGON ((18.12972122 57.48305447, 17.83805508 57.45805456, 17.84638824 57.59110961, 17.95472139 57.68277661, 18.1816665 57.70138754, 18.18166637 57.58999849, 18.20888836 57.57499848, 18.20194333 57.54944247, 18.12972122 57.48305447))', 57.576255, 18.021295, 'Sweden', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)'),
('SE-Firing-Southern-Baltic', 'POLYGON ((15.72210924 56.05519209, 15.69637539 55.92843437, 15.41258279 55.92820641, 15.4116277 56.08809496, 15.45409633 56.08958014, 15.48827017 56.11925844, 15.51739508 56.11811134, 15.55028366 56.09743276, 15.60981683 56.11764533, 15.63629572 56.10752491, 15.65110423 56.09487156, 15.64514435 56.08715104, 15.72210924 56.05519209))', 56.015113, 15.557762, 'Sweden', 'Firing Area', 'A', 'EMODnet emodnet:militaryareaspoly (Tier A curated)');

-- Subsea pipelines & cables (OGC WKT). See cdw-analyse/trino-spatial-ais/ for EMODnet sync.
DROP TABLE IF EXISTS ostsee_pipelines_segmented;
DROP TABLE IF EXISTS ostsee_pipelines;
DROP TABLE IF EXISTS raw_ostsee_geojson;
DROP TABLE IF EXISTS ostsee_infrastruktur;

CREATE TABLE ostsee_infrastruktur (
    id varchar COMMENT 'External asset id (e.g. EMODnet or mock).',
    name varchar COMMENT 'Pipeline or cable display name.',
    typ varchar COMMENT 'Gaspipeline or Seekabel.',
    geometrie_wkt varchar COMMENT 'Route as OGC WKT LINESTRING/MULTILINESTRING (lon lat, WGS84).',
    status varchar COMMENT 'Operational status.',
    medium varchar COMMENT 'Gas, Telecom/Power, etc.'
);

INSERT INTO ostsee_infrastruktur (id, name, typ, geometrie_wkt, status, medium) VALUES
('EMODNET_NS1', 'NORD STREAM 1', 'Gaspipeline', 'LINESTRING (23.99979769 59.65753773, 22.52826538 59.45687113, 21.09415781 59.20373063, 21.05036577 59.14071078, 20.79176628 59.00212766, 20.52227967 58.8899604, 20.35897759 58.75928755, 20.33513795 58.70332492, 20.28324883 58.66143174, 20.22280991 58.57277674, 20.16576533 58.45778949, 20.09268027 58.41262821, 19.77499176 57.92896757, 19.65590183 57.7961544, 19.48553558 57.52118732, 19.42811943 57.4765614, 18.82467552 56.55030537, 18.57729168 56.34578188, 18.10737188 56.24843544, 16.4697971 55.66691842, 15.70063987 55.53579754, 15.42628948 55.38311764, 15.36550871 55.21530088, 15.21641767 54.93645624, 14.79831532 54.82518868, 14.49431612 54.60762594, 14.21395931 54.54142768, 14.08479726 54.5332317, 13.71688492 54.24308183, 13.70184897 54.21301225, 13.63614605 54.19030182, 13.63106352 54.1715519, 13.65426161 54.15419366)', 'In use', 'Gas'),
('EMODNET_BALTICCONNECTOR', 'Balticconnector', 'Gaspipeline', 'LINESTRING (24.08999047 59.95196291, 24.10611111 59.90552432, 24.03913381 59.86896759, 24.03472785 59.77162482, 24.12802207 59.50136118, 24.13429474 59.40054982)', 'In use', 'Gas'),
('EMODNET_NS2', 'NORD STREAM 2', 'Gaspipeline', 'LINESTRING (28.14227953 60.52449418, 28.08942727 60.38930494, 27.88258696 60.2229997, 27.51118065 60.1059404, 26.75406533 60.06940664, 25.89021281 59.84955768, 24.95805292 59.83262487, 23.91754305 59.59039612, 21.2288291 59.17345674, 20.42120071 58.73807712, 20.22016814 58.42254669, 18.9172008 56.58500187, 18.6313395 56.31518911, 18.09941614 56.18138567, 16.39890865 55.60140081, 15.77565632 55.50692414, 15.51506379 55.38733518, 15.28232415 54.90765986, 14.85696937 54.79483342, 14.55184069 54.5663022, 14.11857926 54.49406361, 13.72166074 54.2354678, 13.71325424 54.20953787, 13.63155895 54.18466936, 13.66381217 54.15715816)', 'In use', 'Gas'),
('EMODNET_PCABLE_BSH_1', 'Offshore power cable (BSH/CONTIS) - segment 1', 'Seekabel', 'LINESTRING (10.90364984 53.95650018, 10.9191502 53.97384028, 11.27548958 54.12651006, 11.71548958 54.29168972, 12.08401006 54.29203018, 12.22384028 54.3916996, 12.36051024 54.52486974, 12.6071802 54.67171038, 12.6408584 54.75531389)', 'In use', 'Power'),
('EMODNET_PCABLE_BSH_2', 'Offshore power cable (BSH/CONTIS) - segment 2', 'Seekabel', 'LINESTRING (13.65334547 54.15833425, 13.6471388 54.16392291, 13.65039886 54.18535332, 13.72114047 54.209195, 13.72543203 54.2374458, 13.8521765 54.32908812, 13.83924288 54.34864351, 13.86474657 54.37118565, 13.8573013 54.38355352, 13.84767269 54.50691848, 13.82201351 54.61820671, 13.87355762 54.72281662, 13.93517153 54.75760984, 13.69526362 54.82014481, 13.68019275 54.81793704)', 'In use', 'Power'),
('EMODNET_PCABLE_BSH_3', 'Offshore power cable (BSH/CONTIS) - segment 3', 'Seekabel', 'LINESTRING (13.65904106 54.15418313, 13.65157662 54.16214353, 13.65447918 54.18389494, 13.72556072 54.20741299, 13.72869218 54.2361995, 13.85757551 54.32839299, 13.8447444 54.34886923, 13.87210358 54.37126522, 13.8593145 54.40321336, 13.85233544 54.50737797, 13.83133606 54.61758676, 13.88167135 54.71999024, 13.94404829 54.75402269, 13.94487569 54.76889469, 14.03101089 54.7802854, 14.03452444 54.79647781, 14.09079304 54.8063018, 14.08239963 54.82000882, 14.08573124 54.83089995, 14.0737507 54.83464922)', 'In use', 'Power'),
('EMODNET_PCABLE_BSH_4', 'Offshore power cable (BSH/CONTIS) - segment 4', 'Seekabel', 'LINESTRING (13.65892037 54.15411266, 13.65006995 54.16298981, 13.65302761 54.18440004, 13.72451092 54.20822215, 13.72811924 54.23653393, 13.85575266 54.32879564, 13.84238285 54.34807076, 13.87038855 54.37134365, 13.86050454 54.3877265, 13.85078977 54.50746368, 13.82822298 54.61741701, 13.87880928 54.72075164, 13.94050529 54.75448205, 13.94109647 54.77022477, 14.0299514 54.78159185, 14.01889748 54.82548224, 14.07389896 54.83471551)', 'In use', 'Power');

CREATE TABLE raw_ostsee_geojson (
    json_payload varchar COMMENT 'Single-line GeoJSON FeatureCollection (staging).',
    quelle varchar COMMENT 'Source label: lab_insert, emodnet, s3, …'
);

INSERT INTO raw_ostsee_geojson (json_payload, quelle) VALUES
(
 '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"NORD STREAM 1","medium":"Gas","status":"In use","operator":"EMODnet","country":"DE-RU"},"geometry":{"type":"MultiLineString","coordinates":[[[13.65426161,54.15419366],[13.71688492,54.24308183],[14.79831532,54.82518868],[15.70063987,55.53579754],[18.82467552,56.55030537],[19.77499176,57.92896757],[20.28324883,58.66143174],[20.79176628,59.00212766],[23.99979769,59.65753773]]]}}]}',
 'emodnet_seed'
);

CREATE TABLE ostsee_pipelines (
    pipeline_name varchar COMMENT 'Asset name.',
    medium varchar COMMENT 'Gas, Telecom/Power, …',
    status varchar COMMENT 'Operational status.',
    geo_typ varchar COMMENT 'LineString or MultiLineString.',
    geometrie_wkt varchar COMMENT 'Route WKT (lon lat).',
    quelle varchar COMMENT 'WKT, GeoJSON, EMODnet, …'
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

CREATE TABLE ostsee_pipelines_segmented AS
WITH pipeline_geom AS (
    SELECT
        pipeline_name,
        medium,
        status,
        ST_GeometryFromText(geometrie_wkt) AS geom
    FROM ostsee_pipelines
),
single_lines AS (
    SELECT
        pipeline_name,
        medium,
        status,
        ST_GeometryN(geom, CAST(idx AS integer)) AS line_geom
    FROM pipeline_geom
    CROSS JOIN UNNEST(sequence(1, GREATEST(ST_NumGeometries(geom), 1))) AS t(idx)
    WHERE geom IS NOT NULL
),
line_segments AS (
    SELECT
        pipeline_name,
        medium,
        status,
        ST_LineFromText(
            'LINESTRING(' ||
            CAST(ST_X(ST_PointN(line_geom, CAST(pt_idx AS integer))) AS varchar) || ' ' ||
            CAST(ST_Y(ST_PointN(line_geom, CAST(pt_idx AS integer))) AS varchar) || ', ' ||
            CAST(ST_X(ST_PointN(line_geom, CAST(pt_idx + 1 AS integer))) AS varchar) || ' ' ||
            CAST(ST_Y(ST_PointN(line_geom, CAST(pt_idx + 1 AS integer))) AS varchar) || ')'
        ) AS seg_geom
    FROM single_lines
    CROSS JOIN UNNEST(sequence(1, GREATEST(ST_NumPoints(line_geom) - 1, 0))) AS t(pt_idx)
    WHERE ST_NumPoints(line_geom) >= 2
)
SELECT
    pipeline_name,
    medium,
    status,
    ST_AsText(seg_geom) AS seg_wkt,
    ST_XMin(ST_Buffer(seg_geom, 0.005)) AS min_lon,
    ST_XMax(ST_Buffer(seg_geom, 0.005)) AS max_lon,
    ST_YMin(ST_Buffer(seg_geom, 0.005)) AS min_lat,
    ST_YMax(ST_Buffer(seg_geom, 0.005)) AS max_lat
FROM line_segments
WHERE seg_geom IS NOT NULL;

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

DROP TABLE IF EXISTS sanctioned_vessels;
CREATE TABLE sanctioned_vessels (
  Name varchar COMMENT 'The official registered name of the vessel.',
  MMSI bigint COMMENT 'The unique 9-digit Maritime Mobile Service Identity number.',
  IMO bigint COMMENT 'The unique 7-digit International Maritime Organization identification number.',
  Type varchar COMMENT 'The primary classification or type of vessel (e.g., Crude Oil Tanker, Cargo).',
  Flag varchar COMMENT 'The country whose flag the vessel is sailing under (flag state).',
  Sanction_Reason varchar COMMENT 'The specific regulatory or legal reason for the vessel being sanctioned.',
  Linked_To varchar COMMENT 'Entities, organizations, or individuals associated with or controlling the sanctioned vessel.'
);

INSERT INTO sanctioned_vessels (Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To)
VALUES
('ARISTO',123456011 , 9327413, 'Chemical/Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HAI II', 123456021, 9259599, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS ARGE', 123456033, 9299745, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS ATLANTICA', 636022401, 9322839, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited; HS Atlantica Limited'),
('HS BURAQ', 636022364, 9381732, 'Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS ESBERG', 636022386, 9410894, 'Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS EVERETT', 636022403, 9410870, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS GLORY', 636018127, 9249087, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS LEGEND', 636022362, 9381744, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS STAR', 636018885, 9274446, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('LA PRIDE', 636022251, 9274616, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('MONA', 636022424, 9314818, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('NELLIS', 636022550, 9322267, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('OSPEROUS', 636022098, 9412995, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('PERIA', 636022479, 9322827, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('SARA II', 636022546, 9301615, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('SENSUS', 636022146, 9296585, 'Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('UZE', 636022072, 9323338, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited');


-- EU listed vessels (IMO-only in source feed); demo MMSIs 273800001–273800027 for AIS joins.
INSERT INTO sanctioned_vessels (Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To) VALUES
('M/V Angara', 273800001, 9179842, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: Beförderung von im Verteidigungs- und Sicherheitssektor verwendeten Gütern und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine', 'EU'),
('M/V Maria', 273800002, 8517839, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: Beförderung von im Verteidigungs- und Sicherheitssektor verwendeten Gütern und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine', 'EU'),
('Saam FSU', 273800003, 9915090, 'Floating Storage Unit', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('Koryak FSU', 273800004, 9915105, 'Floating Storage Unit', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('Hana', 273800005, 9353113, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation.', 'EU'),
('Canis Power', 273800006, 9289520, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation', 'EU'),
('Andromeda Star', 273800007, 9402471, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation', 'EU'),
('NS Lotus', 273800008, 9339337, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('NS Spirit', 273800009, 9318553, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('NS Stream', 273800010, 9318541, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('SCF Amur', 273800011, 9333436, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen oder von Russland strukturell für Zwecke des Energietransports genutzt werden, was dem Ziel der Verringerung der russischen Einnahmen in diesem Sektor zuwiderläuft', 'EU'),
('Lady R', 273800012, 9161003, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: im Verteidigungs- und Sicherheitssektor verwendete Güter und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine befördern', 'EU'),
('Maia-1', 273800013, 9358010, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: im Verteidigungs- und Sicherheitssektor verwendete Güter und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine befördern', 'EU'),
('Audax', 273800014, 9763837, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt werden', 'EU'),
('Pugnax', 273800015, 9763849, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt werden', 'EU'),
('Hunter Star', 273800016, 9830769, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt', 'EU'),
('Hebe', 273800017, 9259185, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Enisey', 273800018, 9079169, 'Bulk Carrier', 'Russia', 'Artikel 4x Absatz 2 Buchstabe d Betrieb derart, dass zu Maßnahmen oder Strategien beigetragen wird oder diese unterstützt werden, mit denen die wirtschaftliche Lebensfähigkeit oder die Ernährungssicherheit der Ukraine (etwa durch Beförderung gestohlenen ukrainischen Getreides) oder die Erhaltung des kulturellen Erbes der Ukraine (etwa durch Beförderung gestohlener ukrainischer Kulturgüter) untergraben oder bedroht werden', 'EU'),
('Vela Rain', 273800019, 9331141, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Ocean AMZ', 273800020, 9394935, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Galian 2', 273800021, 9331153, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Robon', 273800022, 9144782, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Beks Aqua', 273800023, 9277735, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Kemerovo', 273800024, 9312884, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Krymsk', 273800025, 9270529, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU'),
('Krasnoyarsk', 273800026, 9312896, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU'),
('Kaliningrad', 273800027, 9341067, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU');

INSERT INTO sanctioned_vessels (Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To) VALUES ('BALAZS', 123456001, 9327413, 'Chemical/Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited');


DROP TABLE IF EXISTS baltic_sea_harbours;
CREATE TABLE baltic_sea_harbours (
  name varchar COMMENT 'The official name of the harbour or port (Baltic, North Sea trade, naval).',
  country varchar COMMENT 'The country in which the harbour is located.',
  latitude double COMMENT 'The geographical latitude of the harbour’s central point (decimal degrees).',
  longitude double COMMENT 'The geographical longitude of the harbour’s central point (decimal degrees).'
);

INSERT INTO baltic_sea_harbours VALUES
('Stockholm', 'Sweden', 59.3293, 18.0686),
('Helsinki', 'Finland', 60.1695, 24.9354),
('Tallinn', 'Estonia', 59.4370, 24.7536),
('Riga', 'Latvia', 56.9496, 24.1052),
('Gdynia', 'Poland', 54.5189, 18.5305),
('Klaipėda', 'Lithuania', 55.7033, 21.1443),
('Turku', 'Finland', 60.4518, 22.2666),
('Mariehamn', 'Finland', 60.0973, 19.9348),
('Liepāja', 'Latvia', 56.5110, 21.0136),
('Ventspils', 'Latvia', 57.3890, 21.5610),
('Kaliningrad', 'Russia', 54.7104, 20.4522),
('Świnoujście', 'Poland', 53.9106, 14.2478),
('Rostock', 'Germany', 54.0887, 12.1405),
('Travemünde', 'Germany', 53.9624, 10.8672),
('St. Petersburg', 'Russia', 59.9343, 30.3351),
('Karlskrona', 'Sweden', 56.1612, 15.5869),
('Kiel', 'Germany', 54.3233, 10.1228),
('Wismar', 'Germany', 53.8934, 11.4536),
('Stralsund', 'Germany', 54.3091, 13.0810),
('Sassnitz', 'Germany', 54.5183, 13.6414),
('Greifswald', 'Germany', 54.0934, 13.3781),
('Nynäshamn', 'Sweden', 58.9036, 17.9470),
('Ustka', 'Poland', 54.5801, 16.8596),
('Pori', 'Finland', 61.4847, 21.7976),
('Kemi', 'Finland', 65.7369, 24.5636),
('Gdańsk', 'Poland', 54.3520, 18.6466),
('Paldiski', 'Estonia', 59.3567, 24.0539),
('Rønne', 'Denmark', 55.1037, 14.7065),
('Visby', 'Sweden', 57.6409, 18.2960),
('Bolderāja', 'Latvia', 56.9950, 24.0500),
('Primorsk', 'Russia', 60.3565, 28.6094),
('Hamburg', 'Germany', 53.551, 9.993),
('Bremerhaven', 'Germany', 53.539, 8.580),
('Wilhelmshaven', 'Germany', 53.513, 8.145),
('Emden', 'Germany', 53.367, 7.206),
('Cuxhaven', 'Germany', 53.867, 8.697),
('Rotterdam', 'Netherlands', 51.949, 4.145),
('Antwerp', 'Belgium', 51.263, 4.397),
('IJmuiden', 'Netherlands', 52.463, 4.556),
('Eckernförde', 'Germany', 54.473, 9.838),
('Warnemünde', 'Germany', 54.176, 12.089),
('Baltiysk', 'Russia', 54.64, 19.82),
('Kronstadt', 'Russia', 59.99, 29.77);



DROP TABLE IF EXISTS marine_vessel_status;

CREATE TABLE marine_vessel_status (
  MMSI varchar COMMENT 'Maritime Mobile Service Identity, a unique 9-digit vessel identifier.',
  Event_Timestamp varchar COMMENT 'The time and date the vessel status was reported.',
  Latitude double COMMENT 'The geographical latitude of the vessel.',
  Longitude double COMMENT 'The geographical longitude of the vessel.',
  Speed double COMMENT 'Speed Over Ground (SOG) of the vessel.',
  Course double COMMENT 'Course Over Ground (COG) of the vessel.',
  Status varchar COMMENT 'The navigational status of the vessel (e.g., Underway, Moored).',
  Destination varchar COMMENT 'The reported port or area destination of the vessel.',
  Depth double COMMENT 'The reported water depth at the vessel’s location.',
  Operational_Status varchar COMMENT 'The current operational state of the vessel (e.g., Fishing, Dredging, Tanker).',
  System_Status varchar COMMENT 'The reporting status of the AIS or monitoring system.'
) WITH (partitioning = ARRAY['truncate(event_timestamp, 10)']);

DROP TABLE IF EXISTS buoy_data;

CREATE TABLE buoy_data (
  buoyid varchar COMMENT 'A unique identifier for the specific buoy that recorded the data.',
  ts varchar COMMENT 'Timestamp of the data record, indicating when the measurement was taken.',
  geo_position_lat double COMMENT 'Geographical latitude where the buoy is currently located.',
  geo_position_lon double COMMENT 'Geographical longitude where the buoy is currently located.',
  altitude integer COMMENT 'The altitude or depth of the buoy at the time of the measurement (in meters or feet).',
  payload_magneticField_totalField double COMMENT 'The total intensity of the measured magnetic field.',
  payload_magneticField_anomaly double COMMENT 'The deviation of the measured magnetic field from the expected regional field.',
  payload_magneticField_gradient varchar COMMENT 'Detailed measurement of the rate of change of the magnetic field over distance.',
  payload_detectionConfidence varchar COMMENT 'A rating or score indicating the certainty of any object detection.',
  payload_object_type varchar COMMENT 'The classification category of the detected object (e.g., vessel, submarine, anomaly).',
  payload_object_classification varchar COMMENT 'A more specific sub-classification of the detected object (e.g., fishing trawler, cargo ship).',
  payload_object_confidence integer COMMENT 'A percentage or score indicating the confidence level in the object classification.',
  payload_object_estimatedDepth integer COMMENT 'The estimated depth of the detected object below the water surface.',
  payload_object_motion varchar COMMENT 'The observed motion characteristics of the detected object (e.g., static, slow-moving, erratic).',
  payload_object_extent varchar COMMENT 'A description of the size or dimensions of the detected object.',
  payload_object_notes varchar COMMENT 'Free-form text notes or observations regarding the detected object.',
  payload_object_orientation integer COMMENT 'The observed heading or orientation of the detected object (in degrees).',
  payload_object_correlationId varchar COMMENT 'An identifier used to link related measurements or detections across multiple buoys or systems.'
) WITH (partitioning = ARRAY['truncate(ts, 10)']);

DROP TABLE IF EXISTS maritime_surveillance_reports;
CREATE TABLE maritime_surveillance_reports (
  message_subject   varchar    COMMENT 'The subject line of the message',
  message_text      varchar    COMMENT 'The full text of the message report',
  message_from      varchar    COMMENT 'The source of the message (e.g., Frigate Alpha HQ)',
  message_dtg       varchar    COMMENT 'The date-time group of the message',
  message_id        varchar    COMMENT 'A unique identifier for the message',
  message_to        varchar    COMMENT 'The intended recipient of the message',
  message_position  varchar    COMMENT 'The position of the asset, if provided'
);


DROP TABLE IF EXISTS social_media_messages;

CREATE TABLE social_media_messages(
  user_name         varchar    COMMENT 'The name of the user who posted the tweet',
  user_username     varchar    COMMENT 'The username of the user',
  tweet             varchar    COMMENT 'The full text of the tweet',
  ts                varchar COMMENT 'The timestamp of the tweet',
  priority          varchar    COMMENT 'The priority level (e.g., niedrig)',
  latitude          double    COMMENT 'The latitude of the tweet location',
  longitude         double    COMMENT 'The longitude of the tweet location',
  metrics_retweets  integer       COMMENT 'The number of retweets',
  metrics_likes     integer       COMMENT 'The number of likes',
  metrics_replies   integer       COMMENT 'The number of replies'
) WITH (partitioning = ARRAY['truncate(ts, 10)']);

DROP TABLE IF EXISTS german_navy_fleet;
CREATE TABLE german_navy_fleet (
    class varchar COMMENT 'The type of naval vessel, e.g., Fregatten, Korvetten.',
    name varchar COMMENT 'The official name of the individual ship, e.g., FGS Brandenburg.',
    mmsi varchar COMMENT 'A unique identifier for the vessel, following a custom pattern.',
    jahr_in_dienst integer COMMENT 'The year the ship was commissioned and entered service.',
    besatzung integer COMMENT 'The total crew size or number of personnel for the vessel.'
);

INSERT INTO german_navy_fleet (class, name, mmsi, jahr_in_dienst, besatzung) VALUES
-- Fregatten (11)
('Fregatten', 'FGS Brandenburg', 'MAR123400', 1994, 219),
('Fregatten', 'FGS Schleswig-Holstein', 'MAR123401', 1995, 219),
('Fregatten', 'FGS Bayern', 'MAR123402', 1996, 219),
('Fregatten', 'FGS Mecklenburg-Vorpommern', 'MAR123403', 1996, 219),
('Fregatten', 'FGS Sachsen', 'MAR123404', 2003, 238),
('Fregatten', 'FGS Hamburg', 'MAR123405', 2004, 238),
('Fregatten', 'FGS Hessen', 'MAR123406', 2006, 238),
('Fregatten', 'FGS Baden-Württemberg', 'MAR123407', 2019, 120),
('Fregatten', 'FGS Nordrhein-Westfalen', 'MAR123408', 2020, 120),
('Fregatten', 'FGS Sachsen-Anhalt', 'MAR123409', 2021, 120),
('Fregatten', 'FGS Rheinland-Pfalz', 'MAR123410', 2022, 120),

-- Korvetten (5)
('Korvetten', 'FGS Braunschweig', 'MAR123411', 2008, 65),
('Korvetten', 'FGS Magdeburg', 'MAR123412', 2008, 65),
('Korvetten', 'FGS Erfurt', 'MAR123413', 2013, 65),
('Korvetten', 'FGS Oldenburg', 'MAR123414', 2013, 65),
('Korvetten', 'FGS Ludwigshafen am Rhein', 'MAR123415', 2013, 65),

-- U-Boote (6)
('U-Boote', 'U-31', 'MAR123416', 2005, 28),
('U-Boote', 'U-32', 'MAR123417', 2006, 28),
('U-Boote', 'U-33', 'MAR123418', 2006, 28),
('U-Boote', 'U-34', 'MAR123419', 2007, 28),
('U-Boote', 'U-35', 'MAR123420', 2015, 28),
('U-Boote', 'U-36', 'MAR123421', 2016, 28),

-- Minenabwehreinheiten (12)
('Minenabwehreinheiten', 'FGS Fulda', 'MAR123422', 1998, 38),
('Minenabwehreinheiten', 'FGS Weilheim', 'MAR123423', 1998, 38),
('Minenabwehreinheiten', 'FGS Sulzbach-Rosenberg', 'MAR123424', 1996, 38),
('Minenabwehreinheiten', 'FGS Dillingen', 'MAR123425', 1997, 38),
('Minenabwehreinheiten', 'FGS Homburg', 'MAR123426', 1998, 38),
('Minenabwehreinheiten', 'FGS Siegburg', 'MAR123427', 1990, 38),
('Minenabwehreinheiten', 'FGS Auerbach/Oberpfalz', 'MAR123428', 1998, 38),
('Minenabwehreinheiten', 'FGS Pegnitz', 'MAR123429', 1998, 38),
('Minenabwehreinheiten', 'FGS Passau', 'MAR123430', 1990, 38),
('Minenabwehreinheiten', 'FGS Weiden', 'MAR123431', 1991, 38),
('Minenabwehreinheiten', 'FGS Ensdorf', 'MAR123432', 1990, 38),
('Minenabwehreinheiten', 'FGS Kühlungsborn', 'MAR123433', 1990, 38),

-- Flottendienstboote (3)
('Flottendienstboote', 'A52 Oste', 'MAR123434', 1988, 36),
('Flottendienstboote', 'A50 Oker', 'MAR123435', 1988, 36),
('Flottendienstboote', 'A53 Alster', 'MAR123436', 1989, 36),

-- Versorgungsschiffe/Tender (11)
('Versorgungsschiffe/Tender', 'A1411 Berlin', 'MAR123437', 2001, 169),
('Versorgungsschiffe/Tender', 'A1412 Frankfurt am Main', 'MAR123438', 2002, 169),
('Versorgungsschiffe/Tender', 'A1413 Bonn', 'MAR123439', 2013, 169),
('Versorgungsschiffe/Tender', 'A511 Elbe', 'MAR123440', 1993, 35),
('Versorgungsschiffe/Tender', 'A512 Mosel', 'MAR123441', 1993, 35),
('Versorgungsschiffe/Tender', 'A513 Rhein', 'MAR123442', 1993, 35),
('Versorgungsschiffe/Tender', 'A514 Werra', 'MAR123443', 1993, 35),
('Versorgungsschiffe/Tender', 'A515 Main', 'MAR123444', 1994, 35),
('Versorgungsschiffe/Tender', 'A516 Donau', 'MAR123445', 1994, 35),
('Versorgungsschiffe/Tender', 'A1442 Spessart', 'MAR123446', 1977, 42),
('Versorgungsschiffe/Tender', 'A1443 Rhön', 'MAR123447', 1977, 42),

-- Sonstige (8)
('Sonstige', 'FGS Gorch Fock', 'MAR123448', 1958, 70),
('Sonstige', 'FGS Planet', 'MAR123449', 2005, 23),
('Sonstige', 'FGS Wangerooge', 'MAR123450', 1984, 25),
('Sonstige', 'FGS Baltrum', 'MAR123451', 1984, 25),
('Sonstige', 'FGS Borkum', 'MAR123452', 1985, 25),
('Sonstige', 'FGS Norderney', 'MAR123453', 1985, 25),
('Sonstige', 'FGS Juist', 'MAR123454', 1985, 25),
('Sonstige', 'FGS Langeoog', 'MAR123455', 1987, 25);

DROP TABLE IF EXISTS rus_fleet;
CREATE TABLE rus_fleet (
    class VARCHAR COMMENT 'Ship class designation.',
    name VARCHAR COMMENT 'Unit name.',
    mmsi VARCHAR COMMENT 'Synthetic fleet MMSI (not real AIS).',
    jahr_in_dienst INTEGER COMMENT 'Year commissioned.',
    besatzung INTEGER COMMENT 'Typical crew size.',
    home_base VARCHAR COMMENT 'Baltiysk, Kronstadt, Primorsk, or Vistula Lagoon Baltiysk.',
    base_role VARCHAR COMMENT 'HQ primary, repair/logistics hub, reserve/dispersal, small craft/patrol.',
    data_source VARCHAR COMMENT 'real or simulated.',
    hull_number VARCHAR COMMENT 'Optional pennant/hull number.',
    vessel_category VARCHAR COMMENT 'frigate, corvette, submarine, landing ship, patrol, support.',
    default_mission_state VARCHAR COMMENT 'eligible_mission or home_standby.'
);

INSERT INTO rus_fleet (class, name, mmsi, jahr_in_dienst, besatzung, home_base, base_role, data_source, hull_number, vessel_category, default_mission_state) VALUES
('Project 11540 Neustrashimy', 'Yaroslav Mudry', 'RUS273001', 2009, 210, 'Baltiysk', 'HQ primary', 'real', '727', 'frigate', 'eligible_mission'),
('Project 20380 Steregushchiy', 'Steregushchy', 'RUS273002', 2007, 100, 'Baltiysk', 'HQ primary', 'real', '530', 'corvette', 'eligible_mission'),
('Project 20380 Steregushchiy', 'Soobrazitelny', 'RUS273003', 2011, 100, 'Baltiysk', 'HQ primary', 'real', '531', 'corvette', 'eligible_mission'),
('Project 20380 Steregushchiy', 'Boikiy', 'RUS273004', 2013, 100, 'Baltiysk', 'HQ primary', 'real', '532', 'corvette', 'home_standby'),
('Project 20380 Steregushchiy', 'Stoikiy', 'RUS273005', 2014, 100, 'Baltiysk', 'HQ primary', 'real', '545', 'corvette', 'eligible_mission'),
('Project 20385 Gremyashchiy', 'Gremyashchiy', 'RUS273006', 2020, 100, 'Baltiysk', 'HQ primary', 'real', '337', 'corvette', 'home_standby'),
('Project 21631 Buyan-M', 'Grad Sviyazhsk', 'RUS273007', 2014, 52, 'Baltiysk', 'HQ primary', 'real', '652', 'corvette', 'eligible_mission'),
('Project 21631 Buyan-M', 'Velikiy Ustyug', 'RUS273008', 2014, 52, 'Baltiysk', 'HQ primary', 'real', '651', 'corvette', 'home_standby'),
('Project 21631 Buyan-M', 'Serpukhov', 'RUS273009', 2016, 52, 'Baltiysk', 'HQ primary', 'real', '603', 'corvette', 'eligible_mission'),
('Project 22800 Karakurt', 'Mytishchi', 'RUS273010', 2019, 39, 'Baltiysk', 'HQ primary', 'real', '899', 'corvette', 'home_standby'),
('Project 22800 Karakurt', 'Sovetsk', 'RUS273011', 2020, 39, 'Baltiysk', 'HQ primary', 'real', '577', 'corvette', 'eligible_mission'),
('Project 22800 Karakurt', 'Odintsovo', 'RUS273012', 2021, 39, 'Baltiysk', 'HQ primary', 'real', '626', 'corvette', 'home_standby'),
('Project 877 Kilo', 'Dmitrov', 'RUS273013', 1986, 52, 'Kronstadt', 'reserve/dispersal', 'real', 'B-806', 'submarine', 'eligible_mission'),
('Project 877 Kilo', 'Vladikavkaz', 'RUS273014', 1990, 52, 'Kronstadt', 'reserve/dispersal', 'real', 'B-177', 'submarine', 'home_standby'),
('Project 636.3 Improved Kilo', 'Kolpino', 'RUS273015', 2016, 52, 'Kronstadt', 'reserve/dispersal', 'real', 'B-271', 'submarine', 'eligible_mission'),
('Project 775 Ropucha', 'Kaliningrad', 'RUS273016', 1984, 87, 'Baltiysk', 'HQ primary', 'real', '102', 'landing ship', 'home_standby'),
('Project 775 Ropucha', 'Korolev', 'RUS273017', 1989, 87, 'Baltiysk', 'HQ primary', 'real', '130', 'landing ship', 'eligible_mission'),
('Project 1171 Alligator', 'Minsk', 'RUS273018', 1983, 107, 'Baltiysk', 'HQ primary', 'real', '127', 'landing ship', 'home_standby'),
('Project 1171 Alligator', 'Kalinin', 'RUS273019', 1985, 107, 'Baltiysk', 'repair/logistics hub', 'real', '152', 'landing ship', 'home_standby'),
('Project 860 Vishnya', 'Kildin', 'RUS273020', 1987, 80, 'Kronstadt', 'repair/logistics hub', 'real', '535', 'support', 'home_standby'),
('Project 861M Moma', 'Khariton Laptev', 'RUS273021', 1986, 74, 'Kronstadt', 'repair/logistics hub', 'real', '908', 'support', 'home_standby'),
('Project 20180 Altay', 'Elbrus', 'RUS273022', 2017, 32, 'Baltiysk', 'repair/logistics hub', 'real', 'A-168', 'support', 'home_standby'),
('Project 141 Kashtan', 'Pavel Khokhryakov', 'RUS273023', 1985, 36, 'Primorsk', 'repair/logistics hub', 'real', '323', 'support', 'home_standby'),
('Project 1265 Sonya', 'BT-215', 'RUS273024', 1976, 32, 'Kronstadt', 'small craft/patrol', 'real', 'BT-215', 'patrol', 'eligible_mission'),
('Project 1331M Mukha', 'PK-123', 'RUS273025', 1988, 28, 'Vistula Lagoon Baltiysk', 'small craft/patrol', 'real', 'PK-123', 'patrol', 'home_standby'),
('Project 20380 Steregushchiy', 'RFS Sim Corvette Alpha', 'RUS273026', 2018, 95, 'Baltiysk', 'HQ primary', 'simulated', 'SIM-01', 'corvette', 'eligible_mission'),
('Project 20380 Steregushchiy', 'RFS Sim Corvette Beta', 'RUS273027', 2019, 95, 'Baltiysk', 'HQ primary', 'simulated', 'SIM-02', 'corvette', 'home_standby'),
('Project 22800 Karakurt', 'RFS Sim Karakurt Gamma', 'RUS273028', 2022, 39, 'Baltiysk', 'HQ primary', 'simulated', 'SIM-03', 'corvette', 'home_standby'),
('Project 877 Kilo', 'RFS Sim Kilo Delta', 'RUS273029', 1992, 52, 'Kronstadt', 'reserve/dispersal', 'simulated', 'SIM-S1', 'submarine', 'home_standby'),
('Project 775 Ropucha', 'RFS Sim Ropucha Epsilon', 'RUS273030', 1991, 87, 'Baltiysk', 'HQ primary', 'simulated', 'SIM-L1', 'landing ship', 'eligible_mission'),
('Project 1171 Alligator', 'RFS Sim Alligator Zeta', 'RUS273031', 1988, 107, 'Primorsk', 'reserve/dispersal', 'simulated', 'SIM-L2', 'landing ship', 'home_standby'),
('Project 1265 Sonya', 'RFS Sim Minesweeper Eta', 'RUS273032', 1980, 32, 'Kronstadt', 'small craft/patrol', 'simulated', 'SIM-P1', 'patrol', 'eligible_mission'),
('Project 1331M Mukha', 'RFS Sim Patrol Theta', 'RUS273033', 1990, 28, 'Vistula Lagoon Baltiysk', 'small craft/patrol', 'simulated', 'SIM-P2', 'patrol', 'home_standby'),
('Project 860 Vishnya', 'RFS Sim AGI Iota', 'RUS273034', 1993, 80, 'Primorsk', 'repair/logistics hub', 'simulated', 'SIM-SUP1', 'support', 'home_standby'),
('Project 20180 Altay', 'RFS Sim Tug Kappa', 'RUS273035', 2015, 32, 'Baltiysk', 'repair/logistics hub', 'simulated', 'SIM-SUP2', 'support', 'home_standby');


DROP TABLE IF EXISTS ships;
CREATE TABLE ships (
  mmsi bigint COMMENT 'The unique 9-digit Maritime Mobile Service Identity number.',
  ship_name varchar COMMENT 'The current registered name of the vessel.',
  vessel_type varchar COMMENT 'The classification or type of vessel (e.g., Cargo, Tanker, Passenger).',
  imo_number varchar COMMENT 'The unique 7-digit International Maritime Organization identification number.',
  call_sign varchar COMMENT 'The unique radio communication identifier for the vessel.',
  flag varchar COMMENT 'The country whose flag the vessel is sailing under (flag state).',
  length_m double COMMENT 'The overall length of the vessel in meters.',
  beam_m double COMMENT 'The maximum width (beam) of the vessel in meters.',
  gross_tonnage double COMMENT 'The gross tonnage (GT) measurement of the vessel’s volume.',
  year_built integer COMMENT 'The year the vessel was constructed.',
  home_port varchar COMMENT 'Registered home harbour (German Baltic fleet fishing boats).'
);

INSERT INTO ships VALUES
(123456000, 'Poseidon Pride', 'Cargo', 'IMO9876541', 'PPDX', 'Liberia', 150.0, 25.0, 12000.0, 2010, NULL),
(123456001, 'Balazs', 'Tanker', 'IMO9876542', 'SSER', 'Panama', 200.0, 32.0, 35000.0, 2012, NULL),
(123456002, 'Ocean Wanderer', 'Passenger', 'IMO9876543', 'OWAN', 'Bahamas', 180.0, 28.0, 25000.0, 2015, NULL),
(123456003, 'North Star', 'Fishing', 'IMO9876544', 'NSTR', 'Norway', 50.0, 10.0, 800.0, 2005, NULL),
(123456004, 'Iron Duke', 'Bulk Carrier', 'IMO9876545', 'IDUK', 'Greece', 220.0, 40.0, 60000.0, 2018, NULL),
(123456005, 'Swift Explorer', 'Research', 'IMO9876546', 'SEPX', 'United Kingdom', 90.0, 15.0, 2500.0, 2020, NULL),
(123456006, 'Coastal Runner', 'Tug', 'IMO9876547', 'CRUN', 'Netherlands', 30.0, 8.0, 500.0, 2008, NULL),
(123456007, 'Blue Horizon', 'Container', 'IMO9876548', 'BHOR', 'Singapore', 300.0, 45.0, 80000.0, 2019, NULL),
(123456008, 'Windward', 'Sailboat', 'IMO9876549', 'WIND', 'France', 20.0, 5.0, 100.0, 2003, NULL),
(123456009, 'Great Voyager', 'Cruise Ship', 'IMO9876550', 'GVOY', 'Malta', 250.0, 35.0, 45000.0, 2016, NULL),
(123456010, 'Arctic Driller', 'Drilling Ship', 'IMO9876551', 'ADRL', 'Marshall Islands', 160.0, 30.0, 20000.0, 2014, NULL),
(123456011, 'Golden Ray', 'Livestock', 'IMO9876552', 'GRAY', 'Australia', 130.0, 22.0, 9000.0, 2011, NULL),
(123456012, 'Storm Breaker', 'Salvage', 'IMO9876553', 'STBR', 'Germany', 70.0, 12.0, 1500.0, 2009, NULL),
(123456013, 'Silver Arrow', 'Ro-Ro', 'IMO976554', 'SARO', 'Italy', 140.0, 20.0, 10000.0, 2017, NULL),
(123456014, 'Maritime Queen', 'Naval', 'IMO9876555', 'MQUE', 'United States', 280.0, 38.0, 50000.0, 2021, NULL),
(123456015, 'Sun Catcher', 'Pleasure Craft', 'IMO9876556', 'SCAT', 'Canada', 15.0, 4.0, 50.0, 2022, NULL),
(123456016, 'Ironclad', 'Dredger', 'IMO9876557', 'ICLD', 'Belgium', 60.0, 11.0, 1200.0, 2007, NULL),
(123456017, 'White Swan', 'Chemical Tanker', 'IMO9876558', 'WSWN', 'Japan', 160.0, 24.0, 15000.0, 2013, NULL),
(123456018, 'Titan', 'LNG Carrier', 'IMO9876559', 'TITN', 'Qatar', 320.0, 50.0, 100000.0, 2023, NULL),
(123456019, 'Horizons Edge', 'Hospital Ship', 'IMO9876560', 'HGED', 'Denmark', 120.0, 18.0, 7000.0, 2018, NULL),
(123456020, 'Voyages Glory', 'Bulk Carrier', 'IMO9876561', 'VGLR', 'Cyprus', 210.0, 38.0, 55000.0, 2017, NULL),
(123456021, 'Neptune Echo', 'Research', 'IMO9876562', 'NECH', 'South Korea', 95.0, 16.0, 3000.0, 2021, NULL),
(123456022, 'Lighthouse', 'Container', 'IMO9876563', 'LGHT', 'China', 350.0, 55.0, 120000.0, 2020, NULL),
(123456023, 'Phoenix Rising', 'Ro-Ro', 'IMO9876564', 'PRIS', 'Spain', 150.0, 24.0, 12000.0, 2019, NULL),
(123456024, 'Golden Compass', 'Offshore Supply', 'IMO9876565', 'GCOM', 'Brazil', 80.0, 14.0, 1800.0, 2015, NULL),
(123456025, 'Tritons Trident', 'Fishing', 'IMO9876566', 'TTRI', 'Iceland', 45.0, 9.0, 750.0, 2006, NULL),
(123456026, 'Morning Mist', 'Chemical Tanker', 'IMO9876567', 'MMIS', 'India', 170.0, 26.0, 18000.0, 2014, NULL),
(123456027, 'Azure Dragon', 'Tug', 'IMO9876568', 'ADRG', 'Ireland', 35.0, 9.0, 600.0, 2011, NULL),
(123456028, 'Polaris', 'Passenger', 'IMO9876569', 'POLA', 'Sweden', 200.0, 30.0, 30000.0, 2016, NULL),
(123456029, 'Crystal River', 'LNG Carrier', 'IMO9876570', 'CRIV', 'Russia', 310.0, 48.0, 95000.0, 2022, NULL),
(123456030, 'Seafarer', 'Bulk Carrier', 'IMO9876571', 'SEFR', 'Panama', 205.0, 36.0, 50000.0, 2013, NULL),
(123456031, 'Island Hopper', 'Ferry', 'IMO9876572', 'IHOP', 'Greece', 90.0, 18.0, 4000.0, 2010, NULL),
(123456032, 'Star Chaser', 'Research', 'IMO9876573', 'SCHA', 'Japan', 100.0, 17.0, 3200.0, 2019, NULL),
(123456033, 'Mighty Ocean', 'Container', 'IMO9876574', 'MOCH', 'Singapore', 380.0, 58.0, 150000.0, 2023, NULL),
(123456034, 'Delta Queen', 'Ro-Ro', 'IMO9876575', 'DQUE', 'Portugal', 160.0, 25.0, 14000.0, 2020, NULL),
(123456035, 'Pacific Dawn', 'Cargo', 'IMO9876576', 'PDWN', 'Cyprus', 145.0, 24.0, 11000.0, 2012, NULL),
(123456036, 'Atlantic Gale', 'Bulk Carrier', 'IMO9876577', 'AGAL', 'Malta', 230.0, 42.0, 65000.0, 2018, NULL),
(123456037, 'Horizon Express', 'Passenger', 'IMO9876578', 'HEXX', 'Bahamas', 190.0, 29.0, 28000.0, 2017, NULL),
(123456038, 'Arctic Fox', 'Fishing', 'IMO9876579', 'AFOX', 'Canada', 55.0, 11.0, 900.0, 2008, NULL),
(123456039, 'Neptunes Grace', 'Chemical Tanker', 'IMO9876580', 'NGRA', 'Liberia', 175.0, 28.0, 20000.0, 2015, NULL),
(123456040, 'Sea Eagle', 'Tug', 'IMO9876581', 'SEAG', 'Germany', 40.0, 10.0, 700.0, 2013, NULL),
(123456041, 'Ocean Dream', 'Container', 'IMO9876582', 'ODRE', 'United States', 320.0, 50.0, 100000.0, 2021, NULL),
(123456042, 'Swift Wind', 'Sailboat', 'IMO9876583', 'SWIN', 'France', 25.0, 6.0, 120.0, 2010, NULL),
(123456043, 'Crystal Voyager', 'Cruise Ship', 'IMO9876584', 'CVOY', 'Italy', 260.0, 36.0, 48000.0, 2019, NULL),
(123456044, 'Polar Explorer', 'Research', 'IMO9876585', 'PEXP', 'United Kingdom', 110.0, 19.0, 4000.0, 2022, NULL),
(123456045, 'Marine Express', 'Ferry', 'IMO9876586', 'MEXP', 'Greece', 95.0, 20.0, 4500.0, 2011, NULL),
(123456046, 'Sunfish', 'Fishing', 'IMO9876587', 'SFSH', 'Norway', 60.0, 12.0, 1000.0, 2009, NULL),
(123456047, 'Starry Night', 'Chemical Tanker', 'IMO9876588', 'SNGT', 'India', 180.0, 29.0, 22000.0, 2016, NULL),
(123456048, 'Valiant', 'Tug', 'IMO9876589', 'VALI', 'Netherlands', 45.0, 11.0, 800.0, 2014, NULL),
(123456049, 'Oceans Gate', 'Bulk Carrier', 'IMO9876590', 'OGAT', 'Australia', 240.0, 44.0, 70000.0, 2020, NULL),
(211871001, 'FV Ostseekrabbe', 'Fishing', 'IMO9821001', 'DAFV1', 'Germany', 26.0, 7.5, 350.0, 2004, 'Rostock'),
(211871002, 'FV Warnemünder Dorsch', 'Fishing', 'IMO9821002', 'DAFV2', 'Germany', 24.0, 7.0, 320.0, 2007, 'Rostock'),
(211871003, 'FV Mecklenburger Scholle', 'Fishing', 'IMO9821003', 'DAFV3', 'Germany', 30.0, 8.0, 420.0, 2011, 'Rostock'),
(211871004, 'FV Travemünder Hummer', 'Fishing', 'IMO9821004', 'DAFV4', 'Germany', 22.0, 6.5, 280.0, 2003, 'Travemünde'),
(211871005, 'FV Lübecker Büsum', 'Fishing', 'IMO9821005', 'DAFV5', 'Germany', 28.0, 7.8, 390.0, 2009, 'Travemünde'),
(211871006, 'FV Kieler Sprotte', 'Fishing', 'IMO9821006', 'DAFV6', 'Germany', 25.0, 7.2, 340.0, 2006, 'Kiel'),
(211871007, 'FV Holsteiner Hecht', 'Fishing', 'IMO9821007', 'DAFV7', 'Germany', 32.0, 8.5, 480.0, 2014, 'Kiel'),
(211871008, 'FV Kieler Bucht', 'Fishing', 'IMO9821008', 'DAFV8', 'Germany', 27.0, 7.6, 370.0, 2010, 'Kiel'),
(211871009, 'FV Wismarer Hering', 'Fishing', 'IMO9821009', 'DAFV9', 'Germany', 23.0, 6.8, 300.0, 2005, 'Wismar'),
(211871010, 'FV Poeler Garn', 'Fishing', 'IMO9821010', 'DAFVA', 'Germany', 29.0, 8.2, 410.0, 2012, 'Wismar'),
(211871011, 'FV Rügenfischer', 'Fishing', 'IMO9821011', 'DAFVB', 'Germany', 26.0, 7.4, 360.0, 2008, 'Stralsund'),
(211871012, 'FV Strelasunder Hake', 'Fishing', 'IMO9821012', 'DAFVC', 'Germany', 24.0, 7.0, 330.0, 2006, 'Stralsund'),
(211871013, 'FV Sassnitzer Lachs', 'Fishing', 'IMO9821013', 'DAFVD', 'Germany', 31.0, 8.3, 450.0, 2015, 'Sassnitz'),
(211871014, 'FV Jasmunder Flunder', 'Fishing', 'IMO9821014', 'DAFVE', 'Germany', 25.0, 7.1, 350.0, 2007, 'Sassnitz'),
(211871015, 'FV Greifswalder Makrele', 'Fishing', 'IMO9821015', 'DAFVF', 'Germany', 27.0, 7.5, 380.0, 2009, 'Greifswald'),
(211871016, 'FV Peenestrom', 'Fishing', 'IMO9821016', 'DAFVG', 'Germany', 22.0, 6.6, 290.0, 2004, 'Greifswald'),
(211871017, 'FV Vorpommern', 'Fishing', 'IMO9821017', 'DAFVH', 'Germany', 28.0, 7.9, 400.0, 2011, 'Greifswald'),
(211871018, 'FV Küstenfischer Nord', 'Fishing', 'IMO9821018', 'DAFVI', 'Germany', 24.0, 6.9, 325.0, 2005, 'Rostock'),
(211871019, 'FV Fehmarnbelt', 'Fishing', 'IMO9821019', 'DAFVJ', 'Germany', 26.0, 7.3, 355.0, 2008, 'Kiel'),
(211871020, 'FV Arkonasee', 'Fishing', 'IMO9821020', 'DAFVK', 'Germany', 30.0, 8.1, 430.0, 2013, 'Sassnitz');

-- lidar_simulation_metadata: one row per NiFi LidarSimulator LAS object (FlowFile attributes)
DROP TABLE IF EXISTS lidar_simulation_metadata;
CREATE TABLE lidar_simulation_metadata (
  hadoop_file_url varchar COMMENT 'Object store URI of the LAS file (hadoop.file.url), e.g. s3a://bucket/path/object.',
  center_lat double COMMENT 'Vessel scene center latitude in WGS84 (lidar.center.lat).',
  center_lon double COMMENT 'Vessel scene center longitude in WGS84 (lidar.center.lon).',
  classification integer COMMENT 'LAS point class code for vessel type (lidar.classification).',
  filename varchar COMMENT 'LAS basename written to object store (lidar.filename).',
  heading_deg double COMMENT 'Vessel heading in degrees clockwise from north (lidar.heading_deg).',
  point_count bigint COMMENT 'Number of points in the LAS file (lidar.point_count).',
  vessel_name varchar COMMENT 'Display name of the simulated vessel (lidar.vessel_name).',
  vessel_type varchar COMMENT 'Simulator vessel type key, e.g. corvette (lidar.vessel_type).',
  predicted_vessel_type varchar COMMENT 'ML-predicted vessel type label (inference pipeline).',
  model_confidence double COMMENT 'Model confidence for predicted_vessel_type (0.0–1.0).',
  ingested_at timestamp COMMENT 'When the metadata row was recorded (set at ingest).'
) WITH (partitioning = ARRAY['day(ingested_at)']);

-- das_cable_spectrogram_metadata: one row per DAS spectrogram segment (NiFi DasCableAcousticSimulator HDF5 batch)
DROP TABLE IF EXISTS das_cable_spectrogram_metadata;
CREATE TABLE das_cable_spectrogram_metadata (
  hadoop_file_url varchar COMMENT 'Object store URI of HDF5 batch (hadoop.file.url).',
  segment_index integer COMMENT 'Segment index inside batch (das.segment.index).',
  cable_asset varchar COMMENT 'Cable registry key (das.cable.asset).',
  cable_id varchar COMMENT 'Cable id (das.cable.id).',
  cable_name varchar COMMENT 'Cable display name (das.cable.name).',
  chainage_m double COMMENT 'Along-cable chainage in meters.',
  segment_lat double COMMENT 'Segment center latitude WGS84.',
  segment_lon double COMMENT 'Segment center longitude WGS84.',
  threat_class varchar COMMENT 'Ground-truth threat label (das.threat_class).',
  predicted_threat_type varchar COMMENT 'ML-predicted threat type.',
  model_confidence double COMMENT 'Model confidence for predicted_threat_type.',
  channel_count integer COMMENT 'Number of DAS channels in segment.',
  window_seconds double COMMENT 'Spectrogram window length (seconds).',
  fs_hz double COMMENT 'Sample rate Hz.',
  spectral_peak_hz double COMMENT 'Dominant spectral peak Hz (optional).',
  energy_rms double COMMENT 'RMS energy feature (optional).',
  filename varchar COMMENT 'HDF5 basename.',
  ingested_at timestamp COMMENT 'Row ingest time (partition key).'
) WITH (partitioning = ARRAY['day(ingested_at)']);

-- gps_jammer_events: airspace integrity / GPS jamming simulation (NiFi GpsJammer)
DROP TABLE IF EXISTS gps_jammer_events;
CREATE TABLE gps_jammer_events (
    geohash varchar COMMENT '6-character geohash identifying the tile',
    ts varchar COMMENT 'ISO 8601 Timestamp of the observation',
    latitude double COMMENT 'Latitude of the tile center',
    longitude double COMMENT 'Longitude of the tile center',
    adsb_nic integer COMMENT 'Navigation Integrity Category (0-8)',
    signal_integrity double COMMENT 'Calculated signal integrity (0.05 - 0.90)',
    jamming_indicator boolean COMMENT 'True if NIC < 5, indicating probable electronic warfare',
    event_type varchar COMMENT 'Classification of the record (e.g., gps_jammer_event)'
) WITH (partitioning = ARRAY['truncate(ts, 10)']);

CREATE OR REPLACE VIEW area_violation as
WITH observation_polygons AS (
    SELECT 
        area_id,
        ST_GeometryFromText(polygon) AS geom
    FROM defense.observation_areas
),
ship_positions AS (
    SELECT 
        e.mmsi,
        e.event_timestamp,
        e.latitude,
        e.longitude,
        o.area_id
    FROM 
        defense.ais_events_ice e
    JOIN 
        observation_polygons o
    ON 
        ST_Within(ST_Point(e.longitude, e.latitude), o.geom)
),
ship_transitions AS (
    SELECT
        mmsi,
        area_id,
        event_timestamp,
        CASE 
            WHEN LAG(area_id) OVER (PARTITION BY mmsi ORDER BY event_timestamp) IS NULL 
                 OR LAG(area_id) OVER (PARTITION BY mmsi ORDER BY event_timestamp) != area_id 
            THEN 'enter'
            ELSE 'exit'
        END AS transition
    FROM 
        ship_positions
) select 
        mmsi,
        area_id,
        event_timestamp,
        transition
from ship_transitions;

CREATE OR REPLACE VIEW buoy_near_harbours AS
SELECT
  b.buoyid,
  b.ts,
  b.geo_position_lat,
  b.geo_position_lon,
  b.altitude,
  b.payload_magneticfield_totalfield,
  b.payload_magneticfield_anomaly,
  b.payload_magneticfield_gradient,
  b.payload_detectionconfidence,
  b.payload_object_type,
  b.payload_object_classification,
  b.payload_object_confidence,
  b.payload_object_estimateddepth,
  b.payload_object_motion,
  b.payload_object_extent,
  b.payload_object_notes,
  b.payload_object_orientation,
  b.payload_object_correlationid,
  h.name AS harbor_name,
  h.country AS harbor_country,
  h.latitude AS harbor_latitude,
  h.longitude AS harbor_longitude,
  (
    6371 * 2 * ASIN(
      SQRT(
        POWER(SIN(RADIANS(b.geo_position_lat - h.latitude) / 2), 2) +
        COS(RADIANS(b.geo_position_lat)) * COS(RADIANS(h.latitude)) *
        POWER(SIN(RADIANS(b.geo_position_lon - h.longitude) / 2), 2)
      )
    )
  ) AS distance_km
FROM
  defense.buoy_data b
CROSS JOIN
  defense.baltic_sea_harbours h
WHERE
  (
    6371 * 2 * ASIN(
      SQRT(
        POWER(SIN(RADIANS(b.geo_position_lat - h.latitude) / 2), 2) +
        COS(RADIANS(b.geo_position_lat)) * COS(RADIANS(h.latitude)) *
        POWER(SIN(RADIANS(b.geo_position_lon - h.longitude) / 2), 2)
      )
    )
  ) <= 10;

DROP TABLE IF EXISTS ais_baseline_sketches;
CREATE TABLE ais_baseline_sketches (
  quadkey varchar COMMENT 'Bing quadkey zoom 11 (~11 km grid).',
  vessel_type varchar COMMENT 'Vessel type from ships or Unknown.',
  course_sector varchar COMMENT 'Course sector N, NE, E, SE, S, SW, W, NW.',
  speed_tdigest varbinary COMMENT 'Serialized T-Digest of SOG.',
  mmsi_set_digest varbinary COMMENT 'Serialized SetDigest of MMSIs.',
  updated_at timestamp COMMENT 'Last baseline update (UTC).'
);

DROP TABLE IF EXISTS vessel_outliers;
CREATE TABLE vessel_outliers (
  MMSI bigint COMMENT 'The unique 9-digit Maritime Mobile Service Identity number of the vessel.',
  Event_Timestamp varchar COMMENT 'The time and date the outlier event was detected.',
  Latitude double COMMENT 'The geographical latitude of the vessel at the time of the event.',
  Longitude double COMMENT 'The geographical longitude of the vessel at the time of the event.',
  Speed double COMMENT 'The observed Speed Over Ground (SOG) of the vessel.',
  Course double COMMENT 'The observed Course Over Ground (COG) of the vessel.',
  Status varchar COMMENT 'The reported navigation status of the vessel (e.g., Moored, Underway).',
  Destination varchar COMMENT 'The reported destination of the vessel.',
  Outlier_Type varchar COMMENT 'The category of the detected anomaly (e.g., Rendezvous, Inconsistent Maneuvering).',
  Reason varchar COMMENT 'A detailed explanation or justification for why the record was flagged as an outlier.',
  Recommendation varchar COMMENT 'Suggested action to investigate or resolve the detected anomaly.',
  Severity_Level integer COMMENT 'A numeric rating indicating the critical nature or impact of the detected outlier.'
);

DROP TABLE IF EXISTS infrastructure_ais_suspicions;
CREATE TABLE infrastructure_ais_suspicions (
  MMSI bigint COMMENT 'Vessel MMSI.',
  Pipeline_Name varchar COMMENT 'Pipeline or cable asset name (ostsee_pipelines_segmented).',
  Medium varchar COMMENT 'Gas, Telecom/Power, etc.',
  Suspicion_Kind varchar COMMENT 'blackout or korridor (spatial-lab query 08 / 07).',
  Pattern varchar COMMENT 'verdachtsmuster or korridor summary label.',
  Event_Start timestamp COMMENT 'Gap or corridor contact start (UTC).',
  Event_End timestamp COMMENT 'Gap end or last corridor contact.',
  Latitude double COMMENT 'Reference latitude (gap start or first corridor contact).',
  Longitude double COMMENT 'Reference longitude.',
  Speed double COMMENT 'SOG at reference point (kn).',
  Course double COMMENT 'COG if known.',
  Status varchar COMMENT 'Navigation status if joined from AIS.',
  Destination varchar COMMENT 'Reported destination if joined.',
  Detail varchar COMMENT 'Metrics: gap minutes, crossing counts, slow minutes, etc.',
  Severity_Level integer COMMENT 'Priority for lagebild / NBA.',
  Updated_At timestamp COMMENT 'Last outlier job refresh time.'
);

CREATE OR REPLACE VIEW latest_sanctioned_vessel_events AS
WITH LatestAISData AS (
    -- 1. Identify the latest record for each MMSI in ais_events_ice
    SELECT
        mmsi,
        event_timestamp,
        latitude,
        longitude,
        speed,
        course,
        status,
        destination,
        -- Assign a row number based on the event_timestamp, descending
        -- Partitioning ensures the numbering restarts for each unique MMSI
        ROW_NUMBER() OVER (
            PARTITION BY mmsi
            ORDER BY event_timestamp DESC
        ) as rn
    FROM
        ais_events_ice
)
-- 2. Join the sanctioned vessels with their latest AIS data
SELECT
    sv.name,
    sv.mmsi,
    sv.imo,
    sv.type AS vessel_type,
    sv.flag,
    sv.sanction_reason,
    sv.linked_to,
    lad.event_timestamp AS latest_event_timestamp,
    lad.latitude AS latest_latitude,
    lad.longitude AS latest_longitude,
    lad.speed AS latest_speed,
    lad.course AS latest_course,
    lad.status AS latest_status,
    lad.destination AS latest_destination
FROM
    sanctioned_vessels sv
LEFT JOIN
    LatestAISData lad
ON
    sv.mmsi = lad.mmsi
    AND lad.rn = 1;


CREATE OR REPLACE VIEW marine_messages as
select
    message_id,
    message_subject ,
    message_text ,
    message_from ,
    message_to,
    message_dtg ,
    coalesce(
        try(date_parse(upper(trim(message_dtg)), '%d%H%iZ%b%y')),
        try(from_iso8601_timestamp(message_dtg)),
        try(from_iso8601_timestamp(replace(message_dtg, 'Z', '+00:00'))),
        try_cast(replace(message_dtg, 'Z', '') AS timestamp),
        try_cast(message_dtg AS timestamp)
    ) AS message_timestamp,
    message_position,
    CAST(regexp_extract(message_position, 'LAT\s+([0-9]+\.[0-9]+)', 1) AS double) AS message_latitude,
    CAST(regexp_extract(message_position, 'LON\s+([0-9]+\.[0-9]+)', 1) AS double) AS message_longitude
from maritime_surveillance_reports;

CREATE OR REPLACE VIEW lagebild AS
WITH Harbour_Ref AS (
    -- Define the source for all harbors to check proximity against
    SELECT
        name AS harbour_name,
        country AS harbour_country,
        latitude AS Harbour_Lat,
        longitude AS Harbour_Lon,
        10000.0 AS Proximity_Meters -- 10 km in meters
    FROM
        defense.baltic_sea_harbours
),

-- CTE 1: Latest Buoy Data
LatestBuoyDetections AS (
    SELECT
        b.buoyid,
        b.ts AS buoy_time,
        b.geo_position_lat AS buoy_lat,
        b.geo_position_lon AS buoy_lon,
        b.payload_object_classification,
        b.payload_magneticfield_anomaly,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        -- HYBRID DISTANCE CALCULATION
        great_circle_distance(b.geo_position_lat, b.geo_position_lon, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY b.buoyid, h.harbour_name ORDER BY b.ts DESC) AS rn -- Partition by buoyid AND harbor
    FROM
        defense.buoy_data b
    CROSS JOIN
        Harbour_Ref h
    WHERE
        b.payload_detectionconfidence IS NOT NULL
        AND try_cast(b.ts AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredBuoyDetections AS (
    SELECT *
    FROM LatestBuoyDetections
    WHERE rn = 1
      AND distance_m <= 2000 -- Filter only events within 5 km of the specific harbor
),

-- CTE 2: Latest AIS Events
LatestAisEvents AS (
    SELECT
        a.mmsi,
        a.event_timestamp AS ship_time,
        a.latitude AS ship_lat,
        a.longitude AS ship_lon,
        s.Name AS sanctioned_name,
        s.Sanction_Reason,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(a.latitude, a.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY a.mmsi, h.harbour_name ORDER BY a.event_timestamp DESC) AS rn -- Partition by MMSI AND harbor
    FROM
        defense.ais_events_ice a
    CROSS JOIN
        Harbour_Ref h
    LEFT JOIN
        defense.sanctioned_vessels s ON a.mmsi = s.mmsi
    WHERE try_cast(a.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredAisEvents AS (
    SELECT *
    FROM LatestAisEvents
    WHERE rn = 1
      AND distance_m <= 30000 -- CORRECTED FILTER: Use 30km limit
),

-- CTE 3: Latest Marine Vessel Status
LatestMarineStatus AS (
    SELECT
        v.mmsi AS marine_mmsi,
        v.event_timestamp AS marine_time,
        v.longitude AS marine_lon,
        v.latitude AS marine_lat,
        v.operational_status,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(v.latitude, v.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY v.mmsi, h.harbour_name ORDER BY v.event_timestamp DESC) AS rn -- Partition by MMSI AND harbor
    FROM
        defense.marine_vessel_status v
    CROSS JOIN
        Harbour_Ref h
    WHERE try_cast(v.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredMarineStatus AS (
    SELECT *
    FROM LatestMarineStatus
    WHERE rn = 1
      AND distance_m <= 30000 -- CORRECTED FILTER: Use 10km limit
),

-- CTE 4: Latest Social Media Messages
LatestSocialMedia AS (
    SELECT
        s.tweet,
        s.user_username,
        s.priority,
        s.ts AS social_time,
        s.longitude AS social_lon,
        s.latitude AS social_lat,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(s.latitude, s.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY s.tweet, s.user_username, h.harbour_name ORDER BY s.ts DESC) AS rn -- Partition by user/tweet AND harbor
    FROM
        defense.social_media_messages s
    CROSS JOIN
        Harbour_Ref h
    WHERE try_cast(s.ts AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredSocialMedia AS (
    SELECT *
    FROM LatestSocialMedia
    WHERE rn = 1 AND distance_m <= 50000
),
-- NEW CTE 5: Latest Marine Message Reports
LatestMarineMessages AS (
    SELECT
        m.message_id,
        CAST(m.message_timestamp AS TIMESTAMP) AS message_time,
        m.message_latitude AS message_lat,
        m.message_longitude AS message_lon,
        m.message_subject,
        m.message_from,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(m.message_latitude, m.message_longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY m.message_id, h.harbour_name ORDER BY m.message_timestamp DESC) AS rn
    FROM
        defense.marine_messages m
    CROSS JOIN
        Harbour_Ref h
    -- Use message_timestamp for the time filter
    WHERE CAST(m.message_timestamp AS TIMESTAMP) >= current_timestamp - interval '24' hour
),
FilteredMarineMessages AS (
    SELECT *
    FROM LatestMarineMessages
    WHERE rn = 1 AND distance_m <= 50000 -- Filter only events within 50 km (50000 m) of the specific harbor
),

LatestLidarSimulations AS (
    SELECT
        l.hadoop_file_url,
        l.filename,
        l.ingested_at AS lidar_time,
        l.center_lat AS lidar_lat,
        l.center_lon AS lidar_lon,
        l.vessel_name,
        l.vessel_type,
        l.classification,
        l.point_count,
        l.heading_deg,
        l.predicted_vessel_type,
        l.model_confidence,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(l.center_lat, l.center_lon, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(l.hadoop_file_url, l.filename), h.harbour_name
            ORDER BY l.ingested_at DESC
        ) AS rn
    FROM
        defense.lidar_simulation_metadata l
    CROSS JOIN
        Harbour_Ref h
    WHERE
        l.ingested_at >= current_timestamp - interval '24' hour
        AND l.center_lat IS NOT NULL
        AND l.center_lon IS NOT NULL
),
FilteredLidarSimulations AS (
    SELECT *
    FROM LatestLidarSimulations
    WHERE rn = 1
      AND distance_m <= 50000
),

LatestDasCableSegments AS (
    SELECT
        d.hadoop_file_url,
        d.segment_index,
        d.filename,
        d.cable_asset,
        d.cable_name,
        d.chainage_m,
        d.ingested_at AS das_time,
        d.segment_lat AS das_lat,
        d.segment_lon AS das_lon,
        d.threat_class,
        d.predicted_threat_type,
        d.model_confidence,
        d.channel_count,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(d.segment_lat, d.segment_lon, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY d.hadoop_file_url, d.segment_index, h.harbour_name
            ORDER BY d.ingested_at DESC
        ) AS rn
    FROM
        defense.das_cable_spectrogram_metadata d
    CROSS JOIN
        Harbour_Ref h
    WHERE
        d.ingested_at >= current_timestamp - interval '24' hour
        AND d.segment_lat IS NOT NULL
        AND d.segment_lon IS NOT NULL
),
FilteredDasCableSegments AS (
    SELECT *
    FROM LatestDasCableSegments
    WHERE rn = 1
      AND distance_m <= 50000
),

-- CTE: Russian Baltic Fleet (positions + registry)
LatestRusNavalPositions AS (
    SELECT
        p.mmsi,
        p.event_timestamp AS rus_time,
        p.latitude AS rus_lat,
        p.longitude AS rus_lon,
        p.speed AS rus_speed,
        p.course AS rus_course,
        p.status AS rus_status,
        p.Destination AS rus_destination,
        rf.name AS rus_name,
        rf."class" AS rus_class,
        rf.home_base AS rus_home_base,
        rf.vessel_category AS rus_vessel_category,
        rf.hull_number AS rus_hull_number,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(p.latitude, p.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY p.mmsi, h.harbour_name ORDER BY p.event_timestamp DESC) AS rn
    FROM
        defense.rus_naval_fleet_positions p
    CROSS JOIN
        Harbour_Ref h
    LEFT JOIN
        defense.rus_fleet rf ON p.mmsi = rf.mmsi
    WHERE try_cast(p.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredRusNavalPositions AS (
    SELECT *
    FROM LatestRusNavalPositions
    WHERE rn = 1
      AND distance_m <= 30000
),

GpsJammerNearestHarbour AS (
    SELECT
        g.geohash,
        coalesce(
            try(from_iso8601_timestamp(g.ts)),
            try(from_iso8601_timestamp(replace(g.ts, 'Z', '+00:00'))),
            try_cast(replace(g.ts, 'Z', '') AS timestamp)
        ) AS jammer_time,
        g.latitude AS jammer_lat,
        g.longitude AS jammer_lon,
        g.adsb_nic,
        g.signal_integrity,
        g.jamming_indicator,
        g.event_type,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(g.latitude, g.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY g.geohash, g.ts
            ORDER BY great_circle_distance(g.latitude, g.longitude, h.Harbour_Lat, h.Harbour_Lon)
        ) AS rn_nearest
    FROM
        defense.gps_jammer_events g
    CROSS JOIN
        Harbour_Ref h
    WHERE coalesce(
            try(from_iso8601_timestamp(g.ts)),
            try(from_iso8601_timestamp(replace(g.ts, 'Z', '+00:00'))),
            try_cast(replace(g.ts, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '24' hour
        AND g.jamming_indicator = true
),
GpsJammerLatestPerTile AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY geohash ORDER BY jammer_time DESC) AS rn_latest
    FROM GpsJammerNearestHarbour
    WHERE rn_nearest = 1
),
FilteredGpsJammerEvents AS (
    SELECT *
    FROM GpsJammerLatestPerTile
    WHERE rn_latest = 1
),

-- CTE: AIS anomaly outliers (vessel_outliers job output)
LatestVesselOutliers AS (
    SELECT
        o.mmsi,
        coalesce(
            try(from_iso8601_timestamp(o.event_timestamp)),
            try(from_iso8601_timestamp(replace(o.event_timestamp, 'Z', '+00:00'))),
            try_cast(replace(o.event_timestamp, 'Z', '') AS timestamp)
        ) AS outlier_time,
        o.latitude AS outlier_lat,
        o.longitude AS outlier_lon,
        o.outlier_type,
        o.reason,
        o.recommendation,
        o.severity_level,
        o.speed,
        o.course,
        o.status,
        o.destination,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(o.latitude, o.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY o.mmsi, o.outlier_type, h.harbour_name
            ORDER BY coalesce(
                try(from_iso8601_timestamp(o.event_timestamp)),
                try(from_iso8601_timestamp(replace(o.event_timestamp, 'Z', '+00:00'))),
                try_cast(replace(o.event_timestamp, 'Z', '') AS timestamp)
            ) DESC
        ) AS rn
    FROM
        defense.vessel_outliers o
    CROSS JOIN
        Harbour_Ref h
    WHERE coalesce(
            try(from_iso8601_timestamp(o.event_timestamp)),
            try(from_iso8601_timestamp(replace(o.event_timestamp, 'Z', '+00:00'))),
            try_cast(replace(o.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '24' hour
),
FilteredVesselOutliers AS (
    SELECT *
    FROM LatestVesselOutliers
    WHERE rn = 1
      AND distance_m <= 30000
)

-- FINAL SELECT: Consolidate all latest, relevant data points, grouped by HARBOUR NAME
SELECT
    'Buoy' AS Data_Source,
    b.buoyid AS ID,
    try_cast(b.buoy_time AS timestamp) AS "timestamp",
    b.buoy_lat AS latitude,
    b.buoy_lon AS longitude,
    b.harbour_name AS harbour_name, -- Added to the final SELECT
    b.distance_m AS dist_m_raw,
    CAST(ROUND(b.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(b.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Object: ', b.payload_object_classification,
        ' | Mag Anomaly: ', CAST(b.payload_magneticfield_anomaly AS VARCHAR)
    ) AS details
FROM
    FilteredBuoyDetections b

UNION ALL

SELECT
    'AIS' AS Data_Source,
    CAST(a.mmsi AS VARCHAR) AS ID,
    try_cast(a.ship_time AS timestamp) AS "timestamp",
    a.ship_lat AS latitude,
    a.ship_lon AS longitude,
    a.harbour_name AS harbour_name, -- Added to the final SELECT
    a.distance_m AS dist_m_raw,
    CAST(ROUND(a.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(a.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Sanctioned: ', COALESCE(a.sanctioned_name, 'No'),
        COALESCE(CONCAT(' (Sanction Reason: ', a.Sanction_Reason, ')'), '') -- Included sanctioned details
    ) AS details
FROM
    FilteredAisEvents a

UNION ALL

SELECT
    'Marine' AS Data_Source,
    CAST(m.marine_mmsi AS VARCHAR) AS ID,
    try_cast(m.marine_time AS timestamp) AS "timestamp",
    m.marine_lat AS latitude,
    m.marine_lon AS longitude,
    m.harbour_name AS harbour_name, -- Added to the final SELECT
    m.distance_m AS dist_m_raw,
    CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Status: ', m.operational_status
    ) AS details
FROM
    FilteredMarineStatus m

UNION ALL

SELECT
    'SocialMedia' AS Data_Source,
    s.user_username AS ID,
    try_cast(s.social_time AS timestamp) AS "timestamp",
    s.social_lat AS latitude,
    s.social_lon AS longitude,
    s.harbour_name AS harbour_name, -- Added to the final SELECT
    s.distance_m AS dist_m_raw,
    CAST(ROUND(s.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(s.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Prio: ', CAST(s.priority AS VARCHAR),
        ' | Tweet: ', s.tweet
    ) AS details
FROM
    FilteredSocialMedia s
UNION ALL

SELECT
    'Marine_Message' AS Data_Source,
    m.message_id AS ID,
    try_cast(m.message_time AS timestamp) AS "timestamp",
    m.message_lat AS latitude,
    m.message_lon AS longitude,
    m.harbour_name AS harbour_name,
    m.distance_m AS dist_m_raw,
    CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Subject: ', m.message_subject,
        ' | From: ', m.message_from
    ) AS details
FROM
    FilteredMarineMessages m

UNION ALL

SELECT
    'LiDAR' AS Data_Source,
    COALESCE(l.filename, l.hadoop_file_url) AS ID,
    l.lidar_time AS "timestamp",
    l.lidar_lat AS latitude,
    l.lidar_lon AS longitude,
    l.harbour_name AS harbour_name,
    l.distance_m AS dist_m_raw,
    CAST(ROUND(l.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(l.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Vessel: ', COALESCE(l.vessel_name, 'unknown'),
        ' (', COALESCE(l.vessel_type, 'undefined'), ')',
        ' | Class: ', CAST(l.classification AS VARCHAR),
        ' | Points: ', CAST(l.point_count AS VARCHAR),
        ' | Hdg: ', CAST(ROUND(l.heading_deg, 1) AS VARCHAR), '°',
        COALESCE(
            CONCAT(' | ML: ', l.predicted_vessel_type, ' (conf ', CAST(l.model_confidence AS VARCHAR), ')'),
            ''
        ),
        COALESCE(CONCAT(' | LAS: ', l.hadoop_file_url), '')
    ) AS details
FROM
    FilteredLidarSimulations l

UNION ALL

SELECT
    'DAS_Cable' AS Data_Source,
    CONCAT(COALESCE(d.filename, d.hadoop_file_url), '#', CAST(d.segment_index AS VARCHAR)) AS ID,
    d.das_time AS "timestamp",
    d.das_lat AS latitude,
    d.das_lon AS longitude,
    d.harbour_name AS harbour_name,
    d.distance_m AS dist_m_raw,
    CAST(ROUND(d.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(d.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Cable: ', COALESCE(d.cable_name, d.cable_asset, 'unknown'),
        ' | Chainage: ', CAST(ROUND(d.chainage_m, 0) AS VARCHAR), ' m',
        ' | Threat: ', COALESCE(d.threat_class, 'ambient'),
        COALESCE(
            CONCAT(' | ML: ', d.predicted_threat_type, ' (conf ', CAST(d.model_confidence AS VARCHAR), ')'),
            ''
        ),
        ' | Ch: ', CAST(COALESCE(d.channel_count, 0) AS VARCHAR),
        COALESCE(CONCAT(' | HDF5: ', d.hadoop_file_url), '')
    ) AS details
FROM
    FilteredDasCableSegments d

UNION ALL

SELECT
    'RUS_Fleet' AS Data_Source,
    r.mmsi AS ID,
    try_cast(r.rus_time AS timestamp) AS "timestamp",
    r.rus_lat AS latitude,
    r.rus_lon AS longitude,
    r.harbour_name AS harbour_name,
    r.distance_m AS dist_m_raw,
    CAST(ROUND(r.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(r.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Unit: ', COALESCE(r.rus_name, r.mmsi),
        COALESCE(CONCAT(' (', r.rus_class, ')'), ''),
        ' | Home: ', COALESCE(r.rus_home_base, 'unknown'),
        ' | Cat: ', COALESCE(r.rus_vessel_category, ''),
        COALESCE(CONCAT(' | Hull: ', r.rus_hull_number), ''),
        ' | Status: ', COALESCE(r.rus_status, ''),
        ' | Dest: ', COALESCE(r.rus_destination, ''),
        ' | SOG: ', CAST(ROUND(r.rus_speed, 1) AS VARCHAR), ' kn',
        ' | COG: ', CAST(ROUND(r.rus_course, 0) AS VARCHAR), '°'
    ) AS details
FROM
    FilteredRusNavalPositions r

UNION ALL

SELECT
    'GPS_Jammer' AS Data_Source,
    j.geohash AS ID,
    j.jammer_time AS "timestamp",
    j.jammer_lat AS latitude,
    j.jammer_lon AS longitude,
    j.harbour_name AS harbour_name,
    j.distance_m AS dist_m_raw,
    CAST(ROUND(j.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(j.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Geohash: ', j.geohash,
        ' | NIC: ', CAST(j.adsb_nic AS VARCHAR),
        ' | Signal integrity: ', CAST(ROUND(j.signal_integrity, 2) AS VARCHAR),
        ' | Jamming: ', CAST(j.jamming_indicator AS VARCHAR),
        COALESCE(CONCAT(' | Type: ', j.event_type), '')
    ) AS details
FROM
    FilteredGpsJammerEvents j

UNION ALL

SELECT
    'AIS_Outlier' AS Data_Source,
    CAST(v.mmsi AS VARCHAR) AS ID,
    v.outlier_time AS "timestamp",
    v.outlier_lat AS latitude,
    v.outlier_lon AS longitude,
    v.harbour_name AS harbour_name,
    v.distance_m AS dist_m_raw,
    CAST(ROUND(v.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(v.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Type: ', COALESCE(v.outlier_type, 'unknown'),
        ' | Sev: ', CAST(COALESCE(v.severity_level, 0) AS VARCHAR),
        ' | Reason: ', COALESCE(v.reason, ''),
        COALESCE(CONCAT(' | Rec: ', v.recommendation), ''),
        COALESCE(CONCAT(' | SOG: ', CAST(ROUND(v.speed, 1) AS VARCHAR), ' kn'), ''),
        COALESCE(CONCAT(' | COG: ', CAST(ROUND(v.course, 0) AS VARCHAR), '°'), ''),
        COALESCE(CONCAT(' | Status: ', v.status), ''),
        COALESCE(CONCAT(' | Dest: ', v.destination), '')
    ) AS details
FROM
    FilteredVesselOutliers v
;

-- VIEW: defense.lagebild_all_events — every qualifying row in the 24h window.
-- (Trino: no ORDER BY in view definitions — order in SELECT at query time.)
-- Same feeds/thresholds as defense.lagebild but without ROW_NUMBER rn=1 latest-only dedupe.
-- CROSS JOIN harbours may multiply rows when an event is within range of several ports.
-- GPS jammer: all jamming rows in 24h; nearest harbour only (rn_nearest=1), not latest per geohash.
CREATE OR REPLACE VIEW lagebild_all_events AS
WITH Harbour_Ref AS (
    -- Define the source for all harbors to check proximity against
    SELECT
        name AS harbour_name,
        country AS harbour_country,
        latitude AS Harbour_Lat,
        longitude AS Harbour_Lon,
        10000.0 AS Proximity_Meters -- 10 km in meters
    FROM
        defense.baltic_sea_harbours
),

-- CTE 1: Buoy detections (all events in 24h)
LatestBuoyDetections AS (
    SELECT
        b.buoyid,
        b.ts AS buoy_time,
        b.geo_position_lat AS buoy_lat,
        b.geo_position_lon AS buoy_lon,
        b.payload_object_classification,
        b.payload_magneticfield_anomaly,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        -- HYBRID DISTANCE CALCULATION
        great_circle_distance(b.geo_position_lat, b.geo_position_lon, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.buoy_data b
    CROSS JOIN
        Harbour_Ref h
    WHERE
        b.payload_detectionconfidence IS NOT NULL
        AND try_cast(b.ts AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredBuoyDetections AS (
    SELECT *
    FROM LatestBuoyDetections
    WHERE distance_m <= 2000 -- Filter only events within 5 km of the specific harbor
),

-- CTE 2: AIS events (all in 24h)
LatestAisEvents AS (
    SELECT
        a.mmsi,
        a.event_timestamp AS ship_time,
        a.latitude AS ship_lat,
        a.longitude AS ship_lon,
        s.Name AS sanctioned_name,
        s.Sanction_Reason,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(a.latitude, a.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.ais_events_ice a
    CROSS JOIN
        Harbour_Ref h
    LEFT JOIN
        defense.sanctioned_vessels s ON a.mmsi = s.mmsi
    WHERE try_cast(a.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredAisEvents AS (
    SELECT *
    FROM LatestAisEvents
    WHERE distance_m <= 30000 -- CORRECTED FILTER: Use 30km limit
),

-- CTE 3: Marine status (all in 24h)
LatestMarineStatus AS (
    SELECT
        v.mmsi AS marine_mmsi,
        v.event_timestamp AS marine_time,
        v.longitude AS marine_lon,
        v.latitude AS marine_lat,
        v.operational_status,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(v.latitude, v.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.marine_vessel_status v
    CROSS JOIN
        Harbour_Ref h
    WHERE try_cast(v.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredMarineStatus AS (
    SELECT *
    FROM LatestMarineStatus
    WHERE distance_m <= 30000 -- CORRECTED FILTER: Use 10km limit
),

-- CTE 4: Social media (all in 24h)
LatestSocialMedia AS (
    SELECT
        s.tweet,
        s.user_username,
        s.priority,
        s.ts AS social_time,
        s.longitude AS social_lon,
        s.latitude AS social_lat,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(s.latitude, s.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.social_media_messages s
    CROSS JOIN
        Harbour_Ref h
    WHERE try_cast(s.ts AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredSocialMedia AS (
    SELECT *
    FROM LatestSocialMedia
    WHERE distance_m <= 50000
),
-- CTE 5: Marine messages (all in 24h)
LatestMarineMessages AS (
    SELECT
        m.message_id,
        CAST(m.message_timestamp AS TIMESTAMP) AS message_time,
        m.message_latitude AS message_lat,
        m.message_longitude AS message_lon,
        m.message_subject,
        m.message_from,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(m.message_latitude, m.message_longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.marine_messages m
    CROSS JOIN
        Harbour_Ref h
    -- Use message_timestamp for the time filter
    WHERE CAST(m.message_timestamp AS TIMESTAMP) >= current_timestamp - interval '24' hour
),
FilteredMarineMessages AS (
    SELECT *
    FROM LatestMarineMessages
    WHERE distance_m <= 50000 -- Filter only events within 50 km (50000 m) of the specific harbor
),

LatestLidarSimulations AS (
    SELECT
        l.hadoop_file_url,
        l.filename,
        l.ingested_at AS lidar_time,
        l.center_lat AS lidar_lat,
        l.center_lon AS lidar_lon,
        l.vessel_name,
        l.vessel_type,
        l.classification,
        l.point_count,
        l.heading_deg,
        l.predicted_vessel_type,
        l.model_confidence,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(l.center_lat, l.center_lon, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(l.hadoop_file_url, l.filename), h.harbour_name
            ORDER BY l.ingested_at DESC
        ) AS rn
    FROM
        defense.lidar_simulation_metadata l
    CROSS JOIN
        Harbour_Ref h
    WHERE
        l.ingested_at >= current_timestamp - interval '24' hour
        AND l.center_lat IS NOT NULL
        AND l.center_lon IS NOT NULL
),
FilteredLidarSimulations AS (
    SELECT *
    FROM LatestLidarSimulations
    WHERE distance_m <= 50000
),

LatestDasCableSegments AS (
    SELECT
        d.hadoop_file_url,
        d.segment_index,
        d.filename,
        d.cable_asset,
        d.cable_name,
        d.chainage_m,
        d.ingested_at AS das_time,
        d.segment_lat AS das_lat,
        d.segment_lon AS das_lon,
        d.threat_class,
        d.predicted_threat_type,
        d.model_confidence,
        d.channel_count,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(d.segment_lat, d.segment_lon, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY d.hadoop_file_url, d.segment_index, h.harbour_name
            ORDER BY d.ingested_at DESC
        ) AS rn
    FROM
        defense.das_cable_spectrogram_metadata d
    CROSS JOIN
        Harbour_Ref h
    WHERE
        d.ingested_at >= current_timestamp - interval '24' hour
        AND d.segment_lat IS NOT NULL
        AND d.segment_lon IS NOT NULL
),
FilteredDasCableSegments AS (
    SELECT *
    FROM LatestDasCableSegments
    WHERE distance_m <= 50000
),

-- CTE: Russian Baltic Fleet (positions + registry)
LatestRusNavalPositions AS (
    SELECT
        p.mmsi,
        p.event_timestamp AS rus_time,
        p.latitude AS rus_lat,
        p.longitude AS rus_lon,
        p.speed AS rus_speed,
        p.course AS rus_course,
        p.status AS rus_status,
        p.Destination AS rus_destination,
        rf.name AS rus_name,
        rf."class" AS rus_class,
        rf.home_base AS rus_home_base,
        rf.vessel_category AS rus_vessel_category,
        rf.hull_number AS rus_hull_number,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(p.latitude, p.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.rus_naval_fleet_positions p
    CROSS JOIN
        Harbour_Ref h
    LEFT JOIN
        defense.rus_fleet rf ON p.mmsi = rf.mmsi
    WHERE try_cast(p.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
FilteredRusNavalPositions AS (
    SELECT *
    FROM LatestRusNavalPositions
    WHERE distance_m <= 30000
),

GpsJammerNearestHarbour AS (
    SELECT
        g.geohash,
        coalesce(
            try(from_iso8601_timestamp(g.ts)),
            try(from_iso8601_timestamp(replace(g.ts, 'Z', '+00:00'))),
            try_cast(replace(g.ts, 'Z', '') AS timestamp)
        ) AS jammer_time,
        g.latitude AS jammer_lat,
        g.longitude AS jammer_lon,
        g.adsb_nic,
        g.signal_integrity,
        g.jamming_indicator,
        g.event_type,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(g.latitude, g.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY g.geohash, g.ts
            ORDER BY great_circle_distance(g.latitude, g.longitude, h.Harbour_Lat, h.Harbour_Lon)
        ) AS rn_nearest
    FROM
        defense.gps_jammer_events g
    CROSS JOIN
        Harbour_Ref h
    WHERE coalesce(
            try(from_iso8601_timestamp(g.ts)),
            try(from_iso8601_timestamp(replace(g.ts, 'Z', '+00:00'))),
            try_cast(replace(g.ts, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '24' hour
        AND g.jamming_indicator = true
),
FilteredGpsJammerEvents AS (
    SELECT *
    FROM GpsJammerNearestHarbour
    WHERE rn_nearest = 1
),

LatestVesselOutliers AS (
    SELECT
        o.mmsi,
        coalesce(
            try(from_iso8601_timestamp(o.event_timestamp)),
            try(from_iso8601_timestamp(replace(o.event_timestamp, 'Z', '+00:00'))),
            try_cast(replace(o.event_timestamp, 'Z', '') AS timestamp)
        ) AS outlier_time,
        o.latitude AS outlier_lat,
        o.longitude AS outlier_lon,
        o.outlier_type,
        o.reason,
        o.recommendation,
        o.severity_level,
        o.speed,
        o.course,
        o.status,
        o.destination,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        great_circle_distance(o.latitude, o.longitude, h.Harbour_Lat, h.Harbour_Lon) * 1000.0 AS distance_m
    FROM
        defense.vessel_outliers o
    CROSS JOIN
        Harbour_Ref h
    WHERE coalesce(
            try(from_iso8601_timestamp(o.event_timestamp)),
            try(from_iso8601_timestamp(replace(o.event_timestamp, 'Z', '+00:00'))),
            try_cast(replace(o.event_timestamp, 'Z', '') AS timestamp)
        ) >= current_timestamp - interval '24' hour
),
FilteredVesselOutliers AS (
    SELECT *
    FROM LatestVesselOutliers
    WHERE distance_m <= 30000
)

-- FINAL SELECT: all qualifying events in the reporting window (volume, not snapshot)
SELECT
    'Buoy' AS Data_Source,
    b.buoyid AS ID,
    try_cast(b.buoy_time AS timestamp) AS "timestamp",
    b.buoy_lat AS latitude,
    b.buoy_lon AS longitude,
    b.harbour_name AS harbour_name, -- Added to the final SELECT
    b.distance_m AS dist_m_raw,
    CAST(ROUND(b.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(b.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Object: ', b.payload_object_classification,
        ' | Mag Anomaly: ', CAST(b.payload_magneticfield_anomaly AS VARCHAR)
    ) AS details
FROM
    FilteredBuoyDetections b

UNION ALL

SELECT
    'AIS' AS Data_Source,
    CAST(a.mmsi AS VARCHAR) AS ID,
    try_cast(a.ship_time AS timestamp) AS "timestamp",
    a.ship_lat AS latitude,
    a.ship_lon AS longitude,
    a.harbour_name AS harbour_name, -- Added to the final SELECT
    a.distance_m AS dist_m_raw,
    CAST(ROUND(a.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(a.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Sanctioned: ', COALESCE(a.sanctioned_name, 'No'),
        COALESCE(CONCAT(' (Sanction Reason: ', a.Sanction_Reason, ')'), '') -- Included sanctioned details
    ) AS details
FROM
    FilteredAisEvents a

UNION ALL

SELECT
    'Marine' AS Data_Source,
    CAST(m.marine_mmsi AS VARCHAR) AS ID,
    try_cast(m.marine_time AS timestamp) AS "timestamp",
    m.marine_lat AS latitude,
    m.marine_lon AS longitude,
    m.harbour_name AS harbour_name, -- Added to the final SELECT
    m.distance_m AS dist_m_raw,
    CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Status: ', m.operational_status
    ) AS details
FROM
    FilteredMarineStatus m

UNION ALL

SELECT
    'SocialMedia' AS Data_Source,
    s.user_username AS ID,
    try_cast(s.social_time AS timestamp) AS "timestamp",
    s.social_lat AS latitude,
    s.social_lon AS longitude,
    s.harbour_name AS harbour_name, -- Added to the final SELECT
    s.distance_m AS dist_m_raw,
    CAST(ROUND(s.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(s.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Prio: ', CAST(s.priority AS VARCHAR),
        ' | Tweet: ', s.tweet
    ) AS details
FROM
    FilteredSocialMedia s
UNION ALL

SELECT
    'Marine_Message' AS Data_Source,
    m.message_id AS ID,
    try_cast(m.message_time AS timestamp) AS "timestamp",
    m.message_lat AS latitude,
    m.message_lon AS longitude,
    m.harbour_name AS harbour_name,
    m.distance_m AS dist_m_raw,
    CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(m.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Subject: ', m.message_subject,
        ' | From: ', m.message_from
    ) AS details
FROM
    FilteredMarineMessages m

UNION ALL

SELECT
    'LiDAR' AS Data_Source,
    COALESCE(l.filename, l.hadoop_file_url) AS ID,
    l.lidar_time AS "timestamp",
    l.lidar_lat AS latitude,
    l.lidar_lon AS longitude,
    l.harbour_name AS harbour_name,
    l.distance_m AS dist_m_raw,
    CAST(ROUND(l.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(l.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Vessel: ', COALESCE(l.vessel_name, 'unknown'),
        ' (', COALESCE(l.vessel_type, 'undefined'), ')',
        ' | Class: ', CAST(l.classification AS VARCHAR),
        ' | Points: ', CAST(l.point_count AS VARCHAR),
        ' | Hdg: ', CAST(ROUND(l.heading_deg, 1) AS VARCHAR), '°',
        COALESCE(
            CONCAT(' | ML: ', l.predicted_vessel_type, ' (conf ', CAST(l.model_confidence AS VARCHAR), ')'),
            ''
        ),
        COALESCE(CONCAT(' | LAS: ', l.hadoop_file_url), '')
    ) AS details
FROM
    FilteredLidarSimulations l

UNION ALL

SELECT
    'DAS_Cable' AS Data_Source,
    CONCAT(COALESCE(d.filename, d.hadoop_file_url), '#', CAST(d.segment_index AS VARCHAR)) AS ID,
    d.das_time AS "timestamp",
    d.das_lat AS latitude,
    d.das_lon AS longitude,
    d.harbour_name AS harbour_name,
    d.distance_m AS dist_m_raw,
    CAST(ROUND(d.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(d.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Cable: ', COALESCE(d.cable_name, d.cable_asset, 'unknown'),
        ' | Chainage: ', CAST(ROUND(d.chainage_m, 0) AS VARCHAR), ' m',
        ' | Threat: ', COALESCE(d.threat_class, 'ambient'),
        COALESCE(
            CONCAT(' | ML: ', d.predicted_threat_type, ' (conf ', CAST(d.model_confidence AS VARCHAR), ')'),
            ''
        ),
        ' | Ch: ', CAST(COALESCE(d.channel_count, 0) AS VARCHAR),
        COALESCE(CONCAT(' | HDF5: ', d.hadoop_file_url), '')
    ) AS details
FROM
    FilteredDasCableSegments d

UNION ALL

SELECT
    'RUS_Fleet' AS Data_Source,
    r.mmsi AS ID,
    try_cast(r.rus_time AS timestamp) AS "timestamp",
    r.rus_lat AS latitude,
    r.rus_lon AS longitude,
    r.harbour_name AS harbour_name,
    r.distance_m AS dist_m_raw,
    CAST(ROUND(r.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(r.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Unit: ', COALESCE(r.rus_name, r.mmsi),
        COALESCE(CONCAT(' (', r.rus_class, ')'), ''),
        ' | Home: ', COALESCE(r.rus_home_base, 'unknown'),
        ' | Cat: ', COALESCE(r.rus_vessel_category, ''),
        COALESCE(CONCAT(' | Hull: ', r.rus_hull_number), ''),
        ' | Status: ', COALESCE(r.rus_status, ''),
        ' | Dest: ', COALESCE(r.rus_destination, ''),
        ' | SOG: ', CAST(ROUND(r.rus_speed, 1) AS VARCHAR), ' kn',
        ' | COG: ', CAST(ROUND(r.rus_course, 0) AS VARCHAR), '°'
    ) AS details
FROM
    FilteredRusNavalPositions r

UNION ALL

SELECT
    'GPS_Jammer' AS Data_Source,
    j.geohash AS ID,
    j.jammer_time AS "timestamp",
    j.jammer_lat AS latitude,
    j.jammer_lon AS longitude,
    j.harbour_name AS harbour_name,
    j.distance_m AS dist_m_raw,
    CAST(ROUND(j.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(j.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Geohash: ', j.geohash,
        ' | NIC: ', CAST(j.adsb_nic AS VARCHAR),
        ' | Signal integrity: ', CAST(ROUND(j.signal_integrity, 2) AS VARCHAR),
        ' | Jamming: ', CAST(j.jamming_indicator AS VARCHAR),
        COALESCE(CONCAT(' | Type: ', j.event_type), '')
    ) AS details
FROM
    FilteredGpsJammerEvents j

UNION ALL

SELECT
    'AIS_Outlier' AS Data_Source,
    CAST(v.mmsi AS VARCHAR) AS ID,
    v.outlier_time AS "timestamp",
    v.outlier_lat AS latitude,
    v.outlier_lon AS longitude,
    v.harbour_name AS harbour_name,
    v.distance_m AS dist_m_raw,
    CAST(ROUND(v.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(v.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Type: ', COALESCE(v.outlier_type, 'unknown'),
        ' | Sev: ', CAST(COALESCE(v.severity_level, 0) AS VARCHAR),
        ' | Reason: ', COALESCE(v.reason, ''),
        COALESCE(CONCAT(' | Rec: ', v.recommendation), ''),
        COALESCE(CONCAT(' | SOG: ', CAST(ROUND(v.speed, 1) AS VARCHAR), ' kn'), ''),
        COALESCE(CONCAT(' | COG: ', CAST(ROUND(v.course, 0) AS VARCHAR), '°'), ''),
        COALESCE(CONCAT(' | Status: ', v.status), ''),
        COALESCE(CONCAT(' | Dest: ', v.destination), '')
    ) AS details
FROM
    FilteredVesselOutliers v
;


DROP TABLE IF EXISTS Situation_Awareness_Summary;
CREATE TABLE Situation_Awareness_Summary (
  summary_timestamp timestamp,
  summare_line integer,
  summary_text varchar
);

CREATE OR REPLACE VIEW vessel_proximity AS
WITH
latest_vessel_ref AS (
    SELECT
        CAST(mmsi AS varchar) AS vessel_mmsi,
        a.event_timestamp AS vessel_time,
        latitude AS vessel_lat,
        longitude AS vessel_lon,
        ROW_NUMBER() OVER (PARTITION BY a.mmsi ORDER BY a.event_timestamp DESC) AS rn
    FROM
        defense.ais_events_ice a
    WHERE
        try_cast(a.event_timestamp AS timestamp) >= current_timestamp - interval '24' hour
),
vessel_ref AS (
    SELECT *
    FROM latest_vessel_ref
    WHERE rn = 1
),
latest_ais_events AS (
    SELECT
        CAST(a.mmsi AS varchar) AS vessel_mmsi,
        a.event_timestamp AS vessel_time,
        a.latitude AS vessel_lat,
        a.longitude AS vessel_lon,
        great_circle_distance(a.latitude, a.longitude, h.vessel_lat, h.vessel_lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY a.mmsi, h.vessel_mmsi ORDER BY a.event_timestamp DESC) AS rn,
        h.vessel_mmsi AS ref_vessel_mmsi
    FROM
        defense.ais_events_ice a
        CROSS JOIN vessel_ref h
    WHERE
        try_cast(a.event_timestamp AS timestamp) >= try_cast(h.vessel_time AS timestamp) - interval '4' hour
),
filtered_ais_events AS (
    SELECT *
    FROM latest_ais_events
    WHERE rn = 1
),
latest_marine_status AS (
    SELECT
        v.mmsi AS marine_mmsi,
        v.event_timestamp AS marine_time,
        v.latitude AS marine_lat,
        v.longitude AS marine_lon,
        v.operational_status,
        great_circle_distance(v.latitude, v.longitude, h.vessel_lat, h.vessel_lon) * 1000.0 AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY v.mmsi, h.vessel_mmsi ORDER BY v.event_timestamp DESC) AS rn,
        h.vessel_mmsi AS ref_vessel_mmsi
    FROM
        defense.marine_vessel_status v
        CROSS JOIN vessel_ref h
    WHERE
        try_cast(v.event_timestamp AS timestamp) >= current_timestamp - interval '4' hour
),
filtered_marine_status AS (
    SELECT *
    FROM latest_marine_status
    WHERE rn = 1
)
SELECT
    'REF' AS Data_Source,
    vessel_mmsi,
    vessel_time,
    vessel_lon,
    vessel_lat,
    0 AS distance_m,
    0 AS distance_km,
    'Reference Vessel' AS details,
    vessel_mmsi AS ref_vessel_mmsi
FROM vessel_ref
WHERE rn = 1
UNION ALL
SELECT
    'AIS' AS Data_Source,
    a.vessel_mmsi,
    a.vessel_time,
    a.vessel_lon,
    a.vessel_lat,
    a.distance_m,
    ROUND(a.distance_m / 1000.0, 2) AS distance_km,
    CONCAT(
        'Distance: ',
        CAST(ROUND(a.distance_m / 1000.0, 2) AS varchar),
        ' km '
    ) AS details,
    a.ref_vessel_mmsi
FROM filtered_ais_events a
UNION ALL
SELECT
    'Fleet' AS Data_Source,
    CAST(m.marine_mmsi AS varchar) AS vessel_mmsi,
    m.marine_time AS vessel_time,
    m.marine_lon AS vessel_lon,
    m.marine_lat AS vessel_lat,
    m.distance_m,
    ROUND(m.distance_m / 1000.0, 2) AS distance_km,
    CONCAT(
        'Distance: ',
        CAST(ROUND(m.distance_m / 1000.0, 2) AS varchar),
        ' km | ',
        'Status: ',
        m.operational_status
    ) AS details,
    m.ref_vessel_mmsi
FROM filtered_marine_status m
ORDER BY Data_Source DESC;