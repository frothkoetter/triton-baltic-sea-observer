-- Fleet analytics joins (Triton NATO UI → /api/analyses/fleet-analytics)
-- Sources: german_navy_fleet, marine_messages (view → maritime_surveillance_reports), marine_vessel_status

-- 1) Outbound STANAG volume per unit (message_from = unit name)
-- SELECT ms.message_from AS name, COUNT(*) AS value ...

-- 2) Recipients (typically NAVAL COMMAND in simulator data)
-- SELECT ms.message_to AS name, COUNT(*) AS value ...

-- 3–5) Latest status per MMSI → operational_status / system_status / destination

-- 6) Unit picture: fleet + message counts + latest status (see triton-ui/server/queries.js fleetAnalyticsUnitStatus)

-- 7) Recent messages from fleet units