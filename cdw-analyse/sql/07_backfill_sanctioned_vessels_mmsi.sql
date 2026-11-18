-- EU sanctions list rows (records 19–45) were seeded with IMO only → MMSI NULL broke
-- joins (`ais_events_ice.mmsi = sanctioned_vessels.mmsi`, lagebild, latest_sanctioned_vessel_events).
-- Synthetic demo MMSIs 273800001–273800027 (Russia EU listed shadow fleet). Safe to re-run.

UPDATE defense.sanctioned_vessels SET mmsi = 273800001 WHERE name = 'M/V Angara' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800002 WHERE name = 'M/V Maria' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800003 WHERE name = 'Saam FSU' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800004 WHERE name = 'Koryak FSU' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800005 WHERE name = 'Hana' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800006 WHERE name = 'Canis Power' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800007 WHERE name = 'Andromeda Star' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800008 WHERE name = 'NS Lotus' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800009 WHERE name = 'NS Spirit' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800010 WHERE name = 'NS Stream' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800011 WHERE name = 'SCF Amur' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800012 WHERE name = 'Lady R' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800013 WHERE name = 'Maia-1' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800014 WHERE name = 'Audax' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800015 WHERE name = 'Pugnax' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800016 WHERE name = 'Hunter Star' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800017 WHERE name = 'Hebe' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800018 WHERE name = 'Enisey' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800019 WHERE name = 'Vela Rain' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800020 WHERE name = 'Ocean AMZ' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800021 WHERE name = 'Galian 2' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800022 WHERE name = 'Robon' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800023 WHERE name = 'Beks Aqua' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800024 WHERE name = 'Kemerovo' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800025 WHERE name = 'Krymsk' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800026 WHERE name = 'Krasnoyarsk' AND mmsi IS NULL;
UPDATE defense.sanctioned_vessels SET mmsi = 273800027 WHERE name = 'Kaliningrad' AND mmsi IS NULL;