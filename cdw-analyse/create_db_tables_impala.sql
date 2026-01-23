- DDL: Database and Use
CREATE DATABASE IF NOT EXISTS defense;

USE defense;

-- DDL: ais_events_ice (External Table)
DROP TABLE IF EXISTS ais_events_ice;
CREATE  TABLE ais_events_ice(
  `mmsi` BIGINT COMMENT 'Maritime Mobile Service Identity, a unique 9-digit vessel identifier.',
  `event_timestamp` STRING COMMENT 'The time and date the AIS event was recorded (typically stored as ISO 8601 or similar format).',
  `latitude` DOUBLE COMMENT 'The geographical latitude of the vessel (decimal degrees, -90 to +90).',
  `longitude` DOUBLE COMMENT 'The geographical longitude of the vessel (decimal degrees, -180 to +180).',
  `speed` DOUBLE COMMENT 'Speed Over Ground (SOG) of the vessel in knots or a standardized unit.',
  `course` DOUBLE COMMENT 'Course Over Ground (COG) of the vessel in degrees (0 to 359.9).',
  `status` STRING COMMENT 'The navigation status of the vessel (e.g., Underway, Moored, Anchored).',
  `Destination` STRING COMMENT 'The reported destination of the vessel.'
)
STORED BY Iceberg;

-- DDL: observation_areas
DROP TABLE IF EXISTS observation_areas;
CREATE TABLE observation_areas (
  area_id STRING COMMENT 'A unique identifier for the defined observation or surveillance area.',
  polygon STRING COMMENT 'The geographic boundary of the area, typically represented as a well-known text (WKT) string or a JSON array of coordinates.',
  center_latitude DOUBLE COMMENT 'The central latitude coordinate of the observation area.',
  center_longitude DOUBLE COMMENT 'The central longitude coordinate of the observation area.'
)
STORED BY ICEBERG;

-- DML: Insert into observation_areas
INSERT INTO observation_areas (area_id, polygon, center_latitude, center_longitude) VALUES
('Bornholm Basin', 'POLYGON((17.237478356677286 56.03107993705109, 17.237478356677286 55.9047066587936, 18.31712997618766 55.9047066587936, 18.317129976129976 56.03107993705109, 17.237478356677286 56.03107993705109))', 55.9679, 17.7773),
('Gdańsk Deep', 'POLYGON((18.29518008728172 55.53540464392404, 18.29518008728172 55.22756184778888, 19.28762566297803 55.22756184778888, 19.28762566297803 55.53540464392404, 18.29518008728172 55.53540464392404))', 55.3815, 18.7914),
('Gotlandic Expanse', 'POLYGON((19.101293326297395 56.70066121963845, 19.101293326297395 56.60288332018746, 20.332383399655697 56.60288332018746, 20.332383399655697 56.70066121963845, 19.101293326297395 56.70066121963845))', 56.6518, 19.7168),
('Słupsk Trough', 'POLYGON((18.631009957831964 56.33596162852314, 18.631009957831964 55.68428205974104, 18.81138185878598 55.68428205974104, 18.81138185878598 56.33596162852314, 18.631009957831964 56.33596162852314))', 56.0101, 18.7212),
('Baltic Shelf', 'POLYGON((17.20764611432847 56.797018539001215, 17.20764611432847 56.27911433081525, 17.4428839762831 56.27911433081525, 17.4428839762831 56.797018539001215, 17.20764611432847 56.797018539001215))', 56.5381, 17.3253);

-- DDL: sanctioned_vessels
DROP TABLE IF EXISTS sanctioned_vessels;
CREATE TABLE sanctioned_vessels (
  Name STRING COMMENT 'The official registered name of the vessel.',
  MMSI BIGINT COMMENT 'The unique 9-digit Maritime Mobile Service Identity number.',
  IMO BIGINT COMMENT 'The unique 7-digit International Maritime Organization identification number.',
  Type STRING COMMENT 'The primary classification or type of vessel (e.g., Crude Oil Tanker, Cargo).',
  Flag STRING COMMENT 'The country whose flag the vessel is sailing under (flag state).',
  Sanction_Reason STRING COMMENT 'The specific regulatory or legal reason for the vessel being sanctioned.',
  Linked_To STRING COMMENT 'Entities, organizations, or individuals associated with or controlling the sanctioned vessel.'
)
STORED BY ICEBERG;

-- DML: Insert into sanctioned_vessels (Records 1-18)
INSERT INTO sanctioned_vessels (Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To) VALUES
('ARISTO', 123456011, 9327413, 'Chemical/Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
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

-- DML: Insert into sanctioned_vessels (Records 19-45, without MMSI)
INSERT INTO sanctioned_vessels (Name, IMO, Type, Flag, Sanction_Reason, Linked_To) VALUES
('M/V Angara', 9179842, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: Beförderung von im Verteidigungs- und Sicherheitssektor verwendeten Gütern und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine', 'EU'),
('M/V Maria', 8517839, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: Beförderung von im Verteidigungs- und Sicherheitssektor verwendeten Gütern und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine', 'EU'),
('Saam FSU', 9915090, 'Floating Storage Unit', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('Koryak FSU', 9915105, 'Floating Storage Unit', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('Hana', 9353113, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation.', 'EU'),
('Canis Power', 9289520, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation', 'EU'),
('Andromeda Star', 9402471, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation', 'EU'),
('NS Lotus', 9339337, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('NS Spirit', 9318553, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('NS Stream', 9318541, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('SCF Amur', 9333436, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen oder von Russland strukturell für Zwecke des Energietransports genutzt werden, was dem Ziel der Verringerung der russischen Einnahmen in diesem Sektor zuwiderläuft', 'EU'),
('Lady R', 9161003, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: im Verteidigungs- und Sicherheitssektor verwendete Güter und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine befördern', 'EU'),
('Maia-1', 9358010, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: im Verteidigungs- und Sicherheitssektor verwendete Güter und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine befördern', 'EU'),
('Audax', 9763837, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt werden', 'EU'),
('Pugnax', 9763849, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt werden', 'EU'),
('Hunter Star', 9830769, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt', 'EU'),
('Hebe', 9259185, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Enisey', 9079169, 'Bulk Carrier', 'Russia', 'Artikel 4x Absatz 2 Buchstabe d Betrieb derart, dass zu Maßnahmen oder Strategien beigetragen wird oder diese unterstützt werden, mit denen die wirtschaftliche Lebensfähigkeit oder die Ernährungssicherheit der Ukraine (etwa durch Beförderung gestohlenen ukrainischen Getreides) oder die Erhaltung des kulturellen Erbes der Ukraine (etwa durch Beförderung gestohlener ukrainischer Kulturgüter) untergraben oder bedroht werden', 'EU'),
('Vela Rain', 9331141, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Ocean AMZ', 9394935, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Galian 2', 9331153, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Robon', 9144782, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Beks Aqua', 9277735, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Kemerovo', 9312884, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Krymsk', 9270529, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU'),
('Krasnoyarsk', 9312896, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU'),
('Kaliningrad', 9341067, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU');

-- DML: Insert into sanctioned_vessels (Record 46)
INSERT INTO sanctioned_vessels (Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To) VALUES ('BALAZS', 123456001, 9327413, 'Chemical/Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited');

-- DDL: baltic_sea_harbours
CREATE TABLE baltic_sea_harbours (
  name STRING COMMENT 'The official name of the harbour or port.',
  country STRING COMMENT 'The country in which the harbour is located.',
  latitude DOUBLE COMMENT 'The geographical latitude of the harbour’s central point (decimal degrees).',
  longitude DOUBLE COMMENT 'The geographical longitude of the harbour’s central point (decimal degrees).'
)
STORED BY ICEBERG;

-- DML: Insert into baltic_sea_harbours
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
('Primorsk', 'Russia', 60.3565, 28.6094);

-- DDL: marine_vessel_status
DROP TABLE IF EXISTS marine_vessel_status;
CREATE TABLE marine_vessel_status (
  MMSI STRING COMMENT 'Maritime Mobile Service Identity, a unique 9-digit vessel identifier.',
  event_timestamp STRING COMMENT 'The time and date the vessel status was reported.',
  Latitude DOUBLE COMMENT 'The geographical latitude of the vessel.',
  Longitude DOUBLE COMMENT 'The geographical longitude of the vessel.',
  Speed DOUBLE COMMENT 'Speed Over Ground (SOG) of the vessel.',
  Course DOUBLE COMMENT 'Course Over Ground (COG) of the vessel.',
  Status STRING COMMENT 'The navigational status of the vessel (e.g., Underway, Moored).',
  Destination STRING COMMENT 'The reported port or area destination of the vessel.',
  Depth DOUBLE COMMENT 'The reported water depth at the vessel’s location.',
  Operational_Status STRING COMMENT 'The current operational state of the vessel (e.g., Fishing, Dredging, Tanker).',
  System_Status STRING COMMENT 'The reporting status of the AIS or monitoring system.'
)
PARTITIONED BY SPEC (TRUNCATE(10,event_timestamp))  -- CORRECTED LINE
STORED BY ICEBERG;

-- DDL: buoy_data
DROP TABLE if EXISTS buoy_data;
CREATE TABLE buoy_data (
  buoyid STRING COMMENT 'A unique identifier for the specific buoy that recorded the data.',
  ts STRING COMMENT 'Timestamp of the data record, indicating when the measurement was taken.',
  geo_position_lat DOUBLE COMMENT 'Geographical latitude where the buoy is currently located.',
  geo_position_lon DOUBLE COMMENT 'Geographical longitude where the buoy is currently located.',
  altitude INT COMMENT 'The altitude or depth of the buoy at the time of the measurement (in meters or feet).',
  payload_magneticField_totalField DOUBLE COMMENT 'The total intensity of the measured magnetic field.',
  payload_magneticField_anomaly DOUBLE COMMENT 'The deviation of the measured magnetic field from the expected regional field.',
  payload_magneticField_gradient STRING COMMENT 'Detailed measurement of the rate of change of the magnetic field over distance.',
  payload_detectionConfidence STRING COMMENT 'A rating or score indicating the certainty of any object detection.',
  payload_object_type STRING COMMENT 'The classification category of the detected object (e.g., vessel, submarine, anomaly).',
  payload_object_classification STRING COMMENT 'A more specific sub-classification of the detected object (e.g., fishing trawler, cargo ship).',
  payload_object_confidence INT COMMENT 'A percentage or score indicating the confidence level in the object classification.',
  payload_object_estimatedDepth INT COMMENT 'The estimated depth of the detected object below the water surface.',
  payload_object_motion STRING COMMENT 'The observed motion characteristics of the detected object (e.g., static, slow-moving, erratic).',
  payload_object_extent STRING COMMENT 'A description of the size or dimensions of the detected object.',
  payload_object_notes STRING COMMENT 'Free-form text notes or observations regarding the detected object.',
  payload_object_orientation INT COMMENT 'The observed heading or orientation of the detected object (in degrees).',
  payload_object_correlationId STRING COMMENT 'An identifier used to link related measurements or detections across multiple buoys or systems.'
)
PARTITIONED BY SPEC (TRUNCATE(10, ts))
STORED BY ICEBERG;

-- DDL: maritime_surveillance_reports
CREATE TABLE IF NOT EXISTS maritime_surveillance_reports (
  message_subject   STRING    COMMENT 'The subject line of the message',
  message_text      STRING    COMMENT 'The full text of the message report',
  message_from      STRING    COMMENT 'The source of the message (e.g., Frigate Alpha HQ)',
  message_dtg       STRING    COMMENT 'The date-time group of the message',
  message_id        STRING    COMMENT 'A unique identifier for the message',
  message_to        STRING    COMMENT 'The intended recipient of the message',
  message_position  STRING    COMMENT 'The position of the asset, if provided'
)
COMMENT 'Table for maritime surveillance reports'
STORED BY ICEBERG;

-- DDL: social_media_messages
DROP TABLE IF EXISTS social_media_messages;
CREATE TABLE IF NOT EXISTS social_media_messages(
  user_name         STRING    COMMENT 'The name of the user who posted the tweet',
  user_username     STRING    COMMENT 'The username of the user',
  tweet             STRING    COMMENT 'The full text of the tweet',
  ts                STRING COMMENT 'The timestamp of the tweet',
  priority          STRING    COMMENT 'The priority level (e.g., niedrig)',
  latitude          DOUBLE    COMMENT 'The latitude of the tweet location',
  longitude         DOUBLE    COMMENT 'The longitude of the tweet location',
  metrics_retweets  INT       COMMENT 'The number of retweets',
  metrics_likes     INT       COMMENT 'The number of likes',
  metrics_replies   INT       COMMENT 'The number of replies'
)
PARTITIONED BY SPEC (TRUNCATE(10, ts))
STORED BY ICEBERG;

-- DDL: german_navy_fleet
CREATE TABLE german_navy_fleet (
    `class` STRING COMMENT 'The type of naval vessel, e.g., Fregatten, Korvetten.',
    name STRING COMMENT 'The official name of the individual ship, e.g., FGS Brandenburg.',
    mmsi STRING COMMENT 'A unique identifier for the vessel, following a custom pattern.',
    jahr_in_dienst INT COMMENT 'The year the ship was commissioned and entered service.',
    besatzung INT COMMENT 'The total crew size or number of personnel for the vessel.'
)
STORED BY ICEBERG;

-- DML: Insert into german_navy_fleet
INSERT INTO german_navy_fleet (`class`, name, mmsi, jahr_in_dienst, besatzung) VALUES
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
('Korvetten', 'FGS Braunschweig', 'MAR123411', 2008, 65),
('Korvetten', 'FGS Magdeburg', 'MAR123412', 2008, 65),
('Korvetten', 'FGS Erfurt', 'MAR123413', 2013, 65),
('Korvetten', 'FGS Oldenburg', 'MAR123414', 2013, 65),
('Korvetten', 'FGS Ludwigshafen am Rhein', 'MAR123415', 2013, 65),
('U-Boote', 'U-31', 'MAR123416', 2005, 28),
('U-Boote', 'U-32', 'MAR123417', 2006, 28),
('U-Boote', 'U-33', 'MAR123418', 2006, 28),
('U-Boote', 'U-34', 'MAR123419', 2007, 28),
('U-Boote', 'U-35', 'MAR123420', 2015, 28),
('U-Boote', 'U-36', 'MAR123421', 2016, 28),
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
('Flottendienstboote', 'A52 Oste', 'MAR123434', 1988, 36),
('Flottendienstboote', 'A50 Oker', 'MAR123435', 1988, 36),
('Flottendienstboote', 'A53 Alster', 'MAR123436', 1989, 36),
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
('Sonstige', 'FGS Gorch Fock', 'MAR123448', 1958, 70),
('Sonstige', 'FGS Planet', 'MAR123449', 2005, 23),
('Sonstige', 'FGS Wangerooge', 'MAR123450', 1984, 25),
('Sonstige', 'FGS Baltrum', 'MAR123451', 1984, 25),
('Sonstige', 'FGS Borkum', 'MAR123452', 1985, 25),
('Sonstige', 'FGS Norderney', 'MAR123453', 1985, 25),
('Sonstige', 'FGS Juist', 'MAR123454', 1985, 25),
('Sonstige', 'FGS Langeoog', 'MAR123455', 1987, 25);

-- DDL: ships
CREATE TABLE ships (
  mmsi BIGINT COMMENT 'The unique 9-digit Maritime Mobile Service Identity number.',
  ship_name STRING COMMENT 'The current registered name of the vessel.',
  vessel_type STRING COMMENT 'The classification or type of vessel (e.g., Cargo, Tanker, Passenger).',
  imo_number STRING COMMENT 'The unique 7-digit International Maritime Organization identification number.',
  call_sign STRING COMMENT 'The unique radio communication identifier for the vessel.',
  flag STRING COMMENT 'The country whose flag the vessel is sailing under (flag state).',
  length_m DOUBLE COMMENT 'The overall length of the vessel in meters.',
  beam_m DOUBLE COMMENT 'The maximum width (beam) of the vessel in meters.',
  gross_tonnage DOUBLE COMMENT 'The gross tonnage (GT) measurement of the vessel’s volume.',
  year_built INT COMMENT 'The year the vessel was constructed.'
)
STORED BY ICEBERG;

-- DML: Insert into ships
INSERT INTO TABLE ships VALUES
(123456000, 'Poseidon Pride', 'Cargo', 'IMO9876541', 'PPDX', 'Liberia', 150.0, 25.0, 12000.0, 2010),
(123456001, 'Balazs', 'Tanker', 'IMO9876542', 'SSER', 'Panama', 200.0, 32.0, 35000.0, 2012),
(123456002, 'Ocean Wanderer', 'Passenger', 'IMO9876543', 'OWAN', 'Bahamas', 180.0, 28.0, 25000.0, 2015),
(123456003, 'North Star', 'Fishing', 'IMO9876544', 'NSTR', 'Norway', 50.0, 10.0, 800.0, 2005),
(123456004, 'Iron Duke', 'Bulk Carrier', 'IMO9876545', 'IDUK', 'Greece', 220.0, 40.0, 60000.0, 2018),
(123456005, 'Swift Explorer', 'Research', 'IMO9876546', 'SEPX', 'United Kingdom', 90.0, 15.0, 2500.0, 2020),
(123456006, 'Coastal Runner', 'Tug', 'IMO9876547', 'CRUN', 'Netherlands', 30.0, 8.0, 500.0, 2008),
(123456007, 'Blue Horizon', 'Container', 'IMO9876548', 'BHOR', 'Singapore', 300.0, 45.0, 80000.0, 2019),
(123456008, 'Windward', 'Sailboat', 'IMO9876549', 'WIND', 'France', 20.0, 5.0, 100.0, 2003),
(123456009, 'Great Voyager', 'Cruise Ship', 'IMO9876550', 'GVOY', 'Malta', 250.0, 35.0, 45000.0, 2016),
(123456010, 'Arctic Driller', 'Drilling Ship', 'IMO9876551', 'ADRL', 'Marshall Islands', 160.0, 30.0, 20000.0, 2014),
(123456011, 'Golden Ray', 'Livestock', 'IMO9876552', 'GRAY', 'Australia', 130.0, 22.0, 9000.0, 2011),
(123456012, 'Storm Breaker', 'Salvage', 'IMO9876553', 'STBR', 'Germany', 70.0, 12.0, 1500.0, 2009),
(123456013, 'Silver Arrow', 'Ro-Ro', 'IMO976554', 'SARO', 'Italy', 140.0, 20.0, 10000.0, 2017),
(123456014, 'Maritime Queen', 'Naval', 'IMO9876555', 'MQUE', 'United States', 280.0, 38.0, 50000.0, 2021),
(123456015, 'Sun Catcher', 'Pleasure Craft', 'IMO9876556', 'SCAT', 'Canada', 15.0, 4.0, 50.0, 2022),
(123456016, 'Ironclad', 'Dredger', 'IMO9876557', 'ICLD', 'Belgium', 60.0, 11.0, 1200.0, 2007),
(123456017, 'White Swan', 'Chemical Tanker', 'IMO9876558', 'WSWN', 'Japan', 160.0, 24.0, 15000.0, 2013),
(123456018, 'Titan', 'LNG Carrier', 'IMO9876559', 'TITN', 'Qatar', 320.0, 50.0, 100000.0, 2023),
(123456019, 'Horizons Edge', 'Hospital Ship', 'IMO9876560', 'HGED', 'Denmark', 120.0, 18.0, 7000.0, 2018),
(123456020, 'Voyages Glory', 'Bulk Carrier', 'IMO9876561', 'VGLR', 'Cyprus', 210.0, 38.0, 55000.0, 2017),
(123456021, 'Neptune Echo', 'Research', 'IMO9876562', 'NECH', 'South Korea', 95.0, 16.0, 3000.0, 2021),
(123456022, 'Lighthouse', 'Container', 'IMO9876563', 'LGHT', 'China', 350.0, 55.0, 120000.0, 2020),
(123456023, 'Phoenix Rising', 'Ro-Ro', 'IMO9876564', 'PRIS', 'Spain', 150.0, 24.0, 12000.0, 2019),
(123456024, 'Golden Compass', 'Offshore Supply', 'IMO9876565', 'GCOM', 'Brazil', 80.0, 14.0, 1800.0, 2015),
(123456025, 'Tritons Trident', 'Fishing', 'IMO9876566', 'TTRI', 'Iceland', 45.0, 9.0, 750.0, 2006),
(123456026, 'Morning Mist', 'Chemical Tanker', 'IMO9876567', 'MMIS', 'India', 170.0, 26.0, 18000.0, 2014),
(123456027, 'Azure Dragon', 'Tug', 'IMO9876568', 'ADRG', 'Ireland', 35.0, 9.0, 600.0, 2011),
(123456028, 'Polaris', 'Passenger', 'IMO9876569', 'POLA', 'Sweden', 200.0, 30.0, 30000.0, 2016),
(123456029, 'Crystal River', 'LNG Carrier', 'IMO9876570', 'CRIV', 'Russia', 310.0, 48.0, 95000.0, 2022),
(123456030, 'Seafarer', 'Bulk Carrier', 'IMO9876571', 'SEFR', 'Panama', 205.0, 36.0, 50000.0, 2013),
(123456031, 'Island Hopper', 'Ferry', 'IMO9876572', 'IHOP', 'Greece', 90.0, 18.0, 4000.0, 2010),
(123456032, 'Star Chaser', 'Research', 'IMO9876573', 'SCHA', 'Japan', 100.0, 17.0, 3200.0, 2019),
(123456033, 'Mighty Ocean', 'Container', 'IMO9876574', 'MOCH', 'Singapore', 380.0, 58.0, 150000.0, 2023),
(123456034, 'Delta Queen', 'Ro-Ro', 'IMO9876575', 'DQUE', 'Portugal', 160.0, 25.0, 14000.0, 2020),
(123456035, 'Pacific Dawn', 'Cargo', 'IMO9876576', 'PDWN', 'Cyprus', 145.0, 24.0, 11000.0, 2012),
(123456036, 'Atlantic Gale', 'Bulk Carrier', 'IMO9876577', 'AGAL', 'Malta', 230.0, 42.0, 65000.0, 2018),
(123456037, 'Horizon Express', 'Passenger', 'IMO9876578', 'HEXX', 'Bahamas', 190.0, 29.0, 28000.0, 2017),
(123456038, 'Arctic Fox', 'Fishing', 'IMO9876579', 'AFOX', 'Canada', 55.0, 11.0, 900.0, 2008),
(123456039, 'Neptunes Grace', 'Chemical Tanker', 'IMO9876580', 'NGRA', 'Liberia', 175.0, 28.0, 20000.0, 2015),
(123456040, 'Sea Eagle', 'Tug', 'IMO9876581', 'SEAG', 'Germany', 40.0, 10.0, 700.0, 2013),
(123456041, 'Ocean Dream', 'Container', 'IMO9876582', 'ODRE', 'United States', 320.0, 50.0, 100000.0, 2021),
(123456042, 'Swift Wind', 'Sailboat', 'IMO9876583', 'SWIN', 'France', 25.0, 6.0, 120.0, 2010),
(123456043, 'Crystal Voyager', 'Cruise Ship', 'IMO9876584', 'CVOY', 'Italy', 260.0, 36.0, 48000.0, 2019),
(123456044, 'Polar Explorer', 'Research', 'IMO9876585', 'PEXP', 'United Kingdom', 110.0, 19.0, 4000.0, 2022),
(123456045, 'Marine Express', 'Ferry', 'IMO9876586', 'MEXP', 'Greece', 95.0, 20.0, 4500.0, 2011),
(123456046, 'Sunfish', 'Fishing', 'IMO9876587', 'SFSH', 'Norway', 60.0, 12.0, 1000.0, 2009),
(123456047, 'Starry Night', 'Chemical Tanker', 'IMO9876588', 'SNGT', 'India', 180.0, 29.0, 22000.0, 2016),
(123456048, 'Valiant', 'Tug', 'IMO9876589', 'VALI', 'Netherlands', 45.0, 11.0, 800.0, 2014),
(123456049, 'Oceans Gate', 'Bulk Carrier', 'IMO9876590', 'OGAT', 'Australia', 240.0, 44.0, 70000.0, 2020);


-- VIEW: area_violation
CREATE VIEW area_violation AS
WITH observation_polygons AS (
    SELECT
        area_id,
        ST_GeomFromText(polygon) AS geom
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
) SELECT
        mmsi,
        area_id,
        event_timestamp,
        transition
FROM ship_transitions;

-- VIEW: buoy_near_harbours (Comment removed from CREATE VIEW)
CREATE VIEW buoy_near_harbours
AS
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
JOIN
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

-- DDL: vessel_outliers
CREATE TABLE vessel_outliers (
  MMSI BIGINT COMMENT 'The unique 9-digit Maritime Mobile Service Identity number of the vessel.',
  Event_Timestamp STRING COMMENT 'The time and date the outlier event was detected.',
  Latitude DOUBLE COMMENT 'The geographical latitude of the vessel at the time of the event.',
  Longitude DOUBLE COMMENT 'The geographical longitude of the vessel at the time of the event.',
  Speed DOUBLE COMMENT 'The observed Speed Over Ground (SOG) of the vessel.',
  Course DOUBLE COMMENT 'The observed Course Over Ground (COG) of the vessel.',
  Status STRING COMMENT 'The reported navigation status of the vessel (e.g., Moored, Underway).',
  Destination STRING COMMENT 'The reported destination of the vessel.',
  Outlier_Type STRING COMMENT 'The category of the detected anomaly (e.g., Rendezvous, Inconsistent Maneuvering).',
  Reason STRING COMMENT 'A detailed explanation or justification for why the record was flagged as an outlier.',
  Recommendation STRING COMMENT 'Suggested action to investigate or resolve the detected anomaly.',
  Severity_Level INT COMMENT 'A numeric rating indicating the critical nature or impact of the detected outlier.'
)
STORED BY ICEBERG;

-- VIEW: latest_sanctioned_vessel_events
CREATE VIEW latest_sanctioned_vessel_events AS
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
        ) AS rn
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
INNER JOIN
    LatestAISData lad
ON
    sv.mmsi = lad.mmsi
WHERE
    -- Filter to only include the row with the latest timestamp (rn = 1)
    lad.rn = 1;



-- VIEW: marine_messages
CREATE VIEW marine_messages AS
SELECT
    message_id,
    message_subject ,
    message_text ,
    message_from ,
    message_to,
    message_dtg ,
    -- Impala's TO_TIMESTAMP is correct for the conversion
    TO_TIMESTAMP(message_dtg, 'ddHHmmZMMMyy') AS message_timestamp,
    message_position,
    -- Extract Latitude
    CAST(REGEXP_EXTRACT(message_position, 'LAT\\s+([0-9]+\\.[0-9]+)°', 1) AS DOUBLE) AS message_latitude,
    -- Extract Longitude
    CAST(REGEXP_EXTRACT(message_position, 'LON\\s+([0-9]+\\.[0-9]+)°', 1) AS DOUBLE) AS message_longitude
FROM maritime_surveillance_reports 
-- REMOVED: ORDER BY message_dtg DESC;
;

-- VIEW: defense.lagebild (Time interval and CAST changes for Impala)
CREATE VIEW defense.lagebild AS
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
        b.payload_magneticField_anomaly,
        h.harbour_name AS harbour_name,
        h.Proximity_Meters AS Proximity_Meters,
        -- HYBRID DISTANCE CALCULATION
        ST_GeodesicLengthWGS84(
            ST_SetSRID(
                ST_LineString(b.geo_position_lon, b.geo_position_lat, h.Harbour_Lon, h.Harbour_Lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY b.buoyid, h.harbour_name ORDER BY b.ts DESC) AS rn -- Partition by buoyid AND harbor
    FROM
        defense.buoy_data b
    CROSS JOIN
        Harbour_Ref h
    WHERE
        b.payload_detectionConfidence IS NOT NULL
        -- Impala syntax for "24 hours ago"
        AND CAST(b.ts AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL 24 HOURS
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
        ST_GeodesicLengthWGS84(
            ST_SetSRID(
                ST_LineString(a.longitude, a.latitude, h.Harbour_Lon, h.Harbour_Lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY a.mmsi, h.harbour_name ORDER BY a.event_timestamp DESC) AS rn -- Partition by MMSI AND harbor
    FROM
        defense.ais_events_ice a
    CROSS JOIN
        Harbour_Ref h
    LEFT JOIN
        defense.sanctioned_vessels s ON a.mmsi = s.mmsi
    -- Impala syntax for "24 hours ago"
    WHERE CAST(a.event_timestamp AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL 24 HOURS
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
        ST_GeodesicLengthWGS84(
            ST_SetSRID(
                ST_LineString(v.longitude, v.latitude, h.Harbour_Lon, h.Harbour_Lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY v.mmsi, h.harbour_name ORDER BY v.event_timestamp DESC) AS rn -- Partition by MMSI AND harbor
    FROM
        defense.marine_vessel_status v
    CROSS JOIN
        Harbour_Ref h
    -- Impala syntax for "24 hours ago"
    WHERE CAST(v.event_timestamp AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL 24 HOURS
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
        ST_GeodesicLengthWGS84(
            ST_SetSRID(
                ST_LineString(s.longitude, s.latitude, h.Harbour_Lon, h.Harbour_Lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY s.tweet, s.user_username, h.harbour_name ORDER BY s.ts DESC) AS rn -- Partition by user/tweet AND harbor
    FROM
        defense.social_media_messages s
    CROSS JOIN
        Harbour_Ref h
    -- Impala syntax for "24 hours ago"
    WHERE CAST(s.ts AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL 24 HOURS
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
        ST_GeodesicLengthWGS84(
            ST_SetSRID(
                ST_LineString(m.message_longitude, m.message_latitude, h.Harbour_Lon, h.Harbour_Lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY m.message_id, h.harbour_name ORDER BY m.message_timestamp DESC) AS rn
    FROM
        defense.marine_messages m
    CROSS JOIN
        Harbour_Ref h
    -- Use message_timestamp for the time filter (already a TIMESTAMP from marine_messages view)
    WHERE m.message_timestamp >= CAST(NOW() AS TIMESTAMP) - INTERVAL 24 HOURS
),
FilteredMarineMessages AS (
    SELECT *
    FROM LatestMarineMessages
    WHERE rn = 1 AND distance_m <= 50000 -- Filter only events within 50 km (50000 m) of the specific harbor
)

----------------------------------------------------------------------------------------------------

-- FINAL SELECT: Consolidate all latest, relevant data points, grouped by HARBOUR NAME
SELECT
    'Buoy' AS Data_Source,
    b.buoyid AS ID,
    b.buoy_time AS "timestamp",
    b.buoy_lat AS latitude,
    b.buoy_lon AS longitude,
    b.harbour_name AS harbour_name, -- Added to the final SELECT
    b.distance_m AS dist_m_raw,
    CAST(ROUND(b.distance_m / 1000.0, 2) AS VARCHAR) AS dist_km,
    CONCAT(
        'Distance: ', CAST(ROUND(b.distance_m / 1000.0, 2) AS VARCHAR), ' km | ',
        'Object: ', b.payload_object_classification,
        ' | Mag Anomaly: ', CAST(b.payload_magneticField_anomaly AS VARCHAR)
    ) AS details
FROM
    FilteredBuoyDetections b

UNION ALL

SELECT
    'AIS' AS Data_Source,
    CAST(a.mmsi AS VARCHAR) AS ID,
    a.ship_time AS "timestamp",
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
    m.marine_time AS "timestamp",
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
    s.social_time AS "timestamp",
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
    m.message_time AS "timestamp",
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

ORDER BY harbour_name, "timestamp" DESC;

-- DDL: Situation_Awareness_Summary
CREATE TABLE Situation_Awareness_Summary
(summary_timestamp TIMESTAMP,
  summare_line INT,
  summary_text STRING)
STORED BY Iceberg;

CREATE VIEW defense.vessel_proximity AS
WITH
-- CTE 1: Find the latest (most recent) AIS event for every vessel
latest_vessel_ref AS (
    SELECT
        CAST(mmsi AS STRING) AS vessel_mmsi,
        a.event_timestamp AS vessel_time,
        latitude AS vessel_lat,
        longitude AS vessel_lon,
        ROW_NUMBER() OVER (PARTITION BY a.mmsi ORDER BY a.event_timestamp DESC) AS rn
    FROM
        defense.ais_events_ice a
    WHERE
        -- Only consider events from the last 24 hours
        CAST(a.event_timestamp AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL 24 HOUR
),

-- CTE 2: Select only the very latest event for each vessel (rn = 1)
vessel_ref AS (
    SELECT
        *
    FROM
        latest_vessel_ref
    WHERE
        rn = 1
),

-- CTE 3: Find the latest AIS events for ALL vessels relative to each vessel in vessel_ref,
-- and calculate the geodesic distance between them.
latest_ais_events AS (
    SELECT
        CAST(a.mmsi AS STRING) AS vessel_mmsi,
        a.event_timestamp AS vessel_time,
        a.latitude AS vessel_lat,
        a.longitude AS vessel_lon,
        -- Calculate distance in meters using the WGS84 great-circle method
        ST_GEODESICLENGTHWGS84(
            ST_SETSRID(
                ST_LINESTRING(a.longitude, a.latitude, h.vessel_lon, h.vessel_lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY a.mmsi, h.vessel_mmsi ORDER BY a.event_timestamp DESC) AS rn,
        h.vessel_mmsi AS ref_vessel_mmsi
    FROM
        defense.ais_events_ice a
        -- Cross Join to compare every AIS event with every "reference" vessel
        CROSS JOIN vessel_ref h
    WHERE
        -- Only consider events in the 4 hours leading up to the reference vessel's latest time
        CAST(a.event_timestamp AS TIMESTAMP) >= CAST(h.vessel_time AS TIMESTAMP) - INTERVAL 4 HOUR
),

-- CTE 4: Select only the latest AIS event for each pair of (vessel, reference_vessel)
filtered_ais_events AS (
    SELECT
        *
    FROM
        latest_ais_events
    WHERE
        rn = 1
),

-- CTE 5: Find the latest 'marine_vessel_status' events for all vessels,
-- and calculate the geodesic distance to each reference vessel.
latest_marine_status AS (
    SELECT
        v.mmsi AS marine_mmsi,
        v.event_timestamp AS marine_time,
        v.latitude AS marine_lat,
        v.longitude AS marine_lon,
        v.operational_status,
        -- Calculate distance in meters using the WGS84 great-circle method
        ST_GEODESICLENGTHWGS84(
            ST_SETSRID(
                ST_LINESTRING(v.longitude, v.latitude, h.vessel_lon, h.vessel_lat),
                4326
            )
        ) AS distance_m,
        ROW_NUMBER() OVER (PARTITION BY v.mmsi, h.vessel_mmsi ORDER BY v.event_timestamp DESC) AS rn,
        h.vessel_mmsi AS ref_vessel_mmsi
    FROM
        defense.marine_vessel_status v
        CROSS JOIN vessel_ref h
    WHERE
        -- Only consider events from the last 4 hours
        CAST(v.event_timestamp AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL 4 HOUR
),

-- CTE 6: Select only the latest marine status event for each pair of (marine_vessel, reference_vessel)
filtered_marine_status AS (
    SELECT
        *
    FROM
        latest_marine_status
    WHERE
        rn = 1
)

-- Final SELECT: Combine all proximity data sets
SELECT
    'REF' AS Data_Source,
    vessel_mmsi,
    vessel_time,
    vessel_lon,
    vessel_lat,
    0 AS distance_m,
    0 AS distance_km,
    'Reference Vessel' AS "detail",
    vessel_mmsi AS ref_vessel_mmsi
FROM
    vessel_ref
WHERE
    rn = 1 -- Redundant, but kept for consistency
UNION ALL

-- Proximity results from filtered AIS events
SELECT
    'AIS' AS Data_Source,
    a.vessel_mmsi,
    a.vessel_time,
    a.vessel_lon,
    a.vessel_lat,
    a.distance_m,
    ROUND(a.distance_m / 1000.0, 2) AS dist_km,
    CONCAT(
        'Distance: ',
        CAST(ROUND(a.distance_m / 1000.0, 2) AS STRING),
        ' km '
    ) AS details,
    a.ref_vessel_mmsi
FROM
    filtered_ais_events a
UNION ALL

-- Proximity results from filtered Marine Status events
SELECT
    'Fleet' AS Data_Source,
    m.marine_mmsi,
    m.marine_time,
    m.marine_lon,
    m.marine_lat,
    m.distance_m,
    ROUND(m.distance_m / 1000.0, 2) AS dist_km,
    CONCAT(
        'Distance: ',
        CAST(ROUND(m.distance_m / 1000.0, 2) AS STRING),
        ' km | ',
        'Status: ',
        m.operational_status
    ) AS details,
    m.ref_vessel_mmsi
FROM
    filtered_marine_status m
ORDER BY
    Data_Source DESC;


-- DDL: gps_jammer_events
-- Diese Tabelle speichert die Integritätsdaten der Luftraumüberwachung
DROP TABLE IF EXISTS gps_jammer_events;

CREATE TABLE gps_jammer_events (
    geohash STRING COMMENT '6-character geohash identifying the tile',
    ts STRING COMMENT 'ISO 8601 Timestamp of the observation',
    latitude DOUBLE COMMENT 'Latitude of the tile center',
    longitude DOUBLE COMMENT 'Longitude of the tile center',
    adsb_nic INT COMMENT 'Navigation Integrity Category (0-8)',
    signal_integrity DOUBLE COMMENT 'Calculated signal integrity (0.05 - 0.90)',
    jamming_indicator BOOLEAN COMMENT 'True if NIC < 5, indicating probable electronic warfare',
    event_type STRING COMMENT 'Classification of the record (e.g., gps_jammer_event)'
)
PARTITIONED BY SPEC (TRUNCATE(10, ts)) -- Partitionierung nach Tag (YYYY-MM-DD)
STORED BY ICEBERG;
