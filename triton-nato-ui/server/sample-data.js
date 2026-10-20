/** Sample payloads when Impala is unreachable (Baltic Sea demo). */

export const meta = {
  source: "sample",
  schema: "defense",
  message: "Impala unavailable — showing embedded demo data",
};

export const tables = [
  "ais_events_ice",
  "ships",
  "sanctioned_vessels",
  "german_navy_fleet",
  "marine_vessel_status",
  "baltic_sea_harbours",
  "buoy_data",
  "observation_areas",
  "area_violation",
  "maritime_surveillance_reports",
  "social_media_messages",
  "lagebild",
];

export const aisShips = [
  { mmsi: 123456007, ship_name: "Blue Horizon", vessel_type: "Container", flag: "Singapore", latitude: 54.52, longitude: 18.54, speed: 12.4, course: 285, event_timestamp: "2025-10-15T08:12:00Z", status: "Underway" },
  { mmsi: 123456014, ship_name: "Maritime Queen", vessel_type: "Naval", flag: "United States", latitude: 54.09, longitude: 12.14, speed: 8.1, course: 45, event_timestamp: "2025-10-15T08:10:00Z", status: "Underway" },
  { mmsi: 123456040, ship_name: "Sea Eagle", vessel_type: "Tug", flag: "Germany", latitude: 54.32, longitude: 10.12, speed: 5.2, course: 120, event_timestamp: "2025-10-15T08:08:00Z", status: "Underway" },
  { mmsi: 636022401, ship_name: "HS ATLANTICA", vessel_type: "Crude Oil Tanker", flag: "Liberia", latitude: 55.97, longitude: 17.78, speed: 9.8, course: 190, event_timestamp: "2025-10-15T08:05:00Z", status: "Underway", sanctioned: true },
  { mmsi: 123456028, ship_name: "Polaris", vessel_type: "Passenger", flag: "Sweden", latitude: 59.33, longitude: 18.07, speed: 14.0, course: 210, event_timestamp: "2025-10-15T08:03:00Z", status: "Underway" },
  { mmsi: 123456031, ship_name: "Island Hopper", vessel_type: "Ferry", flag: "Greece", latitude: 55.70, longitude: 21.14, speed: 11.3, course: 340, event_timestamp: "2025-10-15T08:01:00Z", status: "Underway" },
  { mmsi: 123456012, ship_name: "Storm Breaker", vessel_type: "Salvage", flag: "Germany", latitude: 54.71, longitude: 20.45, speed: 6.7, course: 75, event_timestamp: "2025-10-15T07:58:00Z", status: "Underway" },
  { mmsi: 123456041, ship_name: "Ocean Dream", vessel_type: "Container", flag: "United States", latitude: 56.95, longitude: 24.11, speed: 13.1, course: 160, event_timestamp: "2025-10-15T07:55:00Z", status: "Underway" },
];

export const fleet = [
  { class: "Fregatten", name: "FGS Brandenburg", hull_number: "F215", year_commissioned: 1994, crew_size: 118 },
  { class: "Fregatten", name: "FGS Sachsen", hull_number: "F219", year_commissioned: 2004, crew_size: 230 },
  { class: "Korvetten", name: "FGS Braunschweig", hull_number: "F260", year_commissioned: 2008, crew_size: 65 },
  { class: "U-Boote", name: "FGS U36", hull_number: "S186", year_commissioned: 2016, crew_size: 22 },
  { class: "Minenjäger", name: "FGS Dillingen", hull_number: "M1065", year_commissioned: 1990, crew_size: 38 },
];

export const sanctions = [
  { Name: "HS ATLANTICA", MMSI: 636022401, Type: "Crude Oil Tanker", Flag: "Liberia", Sanction_Reason: "Blocked under E.O. 14024", Linked_To: "Hennesea Holdings Limited" },
  { Name: "Andromeda Star", IMO: 9402471, Type: "Crude Oil Tanker", Flag: "Russia", Sanction_Reason: "EU Annex XIII / irregular shipping practices", Linked_To: "EU" },
  { Name: "Lady R", IMO: 9161003, Type: "Crude Oil Tanker", Flag: "Russia", Sanction_Reason: "Defense sector goods transport", Linked_To: "EU" },
  { Name: "ARISTO", MMSI: 123456011, Type: "Chemical/Products Tanker", Flag: "Liberia", Sanction_Reason: "Blocked under E.O. 14024", Linked_To: "Hennesea Holdings Limited" },
];

export const areaViolations = [
  { mmsi: 636022401, area_id: "Bornholm Basin", flag: "Liberia", violation_time: "2025-10-14T22:15:00Z", ship_name: "HS ATLANTICA" },
  { mmsi: 123456029, area_id: "Gdańsk Deep", flag: "Russia", violation_time: "2025-10-14T19:40:00Z", ship_name: "Crystal River" },
  { mmsi: 123456033, area_id: "Gotlandic Expanse", flag: "Russia", violation_time: "2025-10-14T16:05:00Z", ship_name: "Mighty Ocean" },
];

export const surveillance = [
  { message_id: "SR-2025-8841", class: "SURFACE", name: "Contact Alpha", message_timestamp: "2025-10-15T07:45:00Z", message_latitude: 55.02, message_longitude: 17.44 },
  { message_id: "SR-2025-8840", class: "SUBSURFACE", name: "Contact Bravo", message_timestamp: "2025-10-15T06:30:00Z", message_latitude: 56.65, message_longitude: 19.71 },
  { message_id: "SR-2025-8838", class: "AIR", name: "Track Charlie", message_timestamp: "2025-10-15T05:12:00Z", message_latitude: 54.35, message_longitude: 13.08 },
];

export const observationAreas = [
  { area_id: "Bornholm Basin", center_latitude: 55.9679, center_longitude: 17.7773 },
  { area_id: "Gdańsk Deep", center_latitude: 55.3815, center_longitude: 18.7914 },
  { area_id: "Gotlandic Expanse", center_latitude: 56.6518, center_longitude: 19.7168 },
];

export const kpis = {
  fleetUnits: 42,
  personnel: 1280,
  surveillanceReports: 847,
  alerts: 2,
};

export const datasets = [
  { name: "ShipsAIS", tables: ["ais_events_ice", "ships"], badge: "multipletables" },
  { name: "FlottenStatus", tables: ["german_navy_fleet", "marine_vessel_status"], badge: "multipletables" },
  { name: "SanctionedVessels", tables: ["sanctioned_vessels"], badge: "multipletables" },
  { name: "AreaViolations", tables: ["area_violation"], badge: "multipletables" },
  { name: "SurveillanceReports", tables: ["maritime_surveillance_reports"], badge: "multipletables" },
  { name: "AIS Positions", tables: ["ais_events_ice"], badge: "multipletables" },
  { name: "Buoy_near_harbours", tables: ["buoy_near_harbours"], badge: "singletable" },
  { name: "ObservationAreas", tables: ["observation_areas"], badge: "singletable" },
  { name: "BalticSeaHarbours", tables: ["baltic_sea_harbours"], badge: "singletable" },
  { name: "PersonalReport", tables: ["german_navy_fleet"], badge: "multipletables" },
  { name: "Social Media", tables: ["social_media_messages"], badge: "singletable" },
  { name: "AIS Vessel Tracker", tables: ["ais_events_ice", "ships"], badge: "multipletables" },
];
