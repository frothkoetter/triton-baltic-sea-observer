-- just for reference 
-- use the local Kafka and search for topic ais-event-records
--

CREATE TABLE `ssb`.`ssb_default`.`ais_events_record` (
  `MMSI` BIGINT,
  `Event_Timestamp` VARCHAR(2147483647),
  `Latitude` DOUBLE,
  `Longitude` DOUBLE,
  `Speed` DOUBLE,
  `Course` DOUBLE,
  `Status` VARCHAR(2147483647),
  `Destination` VARCHAR(2147483647),
  `eventTimestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp',
  WATERMARK FOR `eventTimestamp` AS `eventTimestamp` - INTERVAL '3' SECOND
) WITH (
  'properties.ssl.truststore.password' = '******',
  'properties.auto.offset.reset' = 'earliest',
  'properties.sasl.mechanism' = 'PLAIN',
  'format' = 'json',
  'properties.security.protocol' = 'SASL_SSL',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'triton-csa-master1.se-sandb.a465-9q4k.cloudera.site:9093, triton-csa-master0.se-sandb.a465-9q4k.cloudera.site:9093',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="frothkoetter" password="******";',
  'connector' = 'kafka',
  'properties.request.timeout.ms' = '120000',
  'properties.ssl.truststore.location' = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks',
  'properties.transaction.timeout.ms' = '900000',
  'topic' = 'ais_events_record'
)

