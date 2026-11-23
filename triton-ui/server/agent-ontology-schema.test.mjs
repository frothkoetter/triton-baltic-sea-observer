import test from "node:test";
import assert from "node:assert/strict";
import { buildColumnRegistry, validateSqlMapping, semanticMappingForClasses } from "./agent-ontology-schema.js";

test("registry includes gps_jammer_events", () => {
  const reg = buildColumnRegistry();
  assert.ok(reg.tables.gps_jammer_events?.columns?.includes("adsb_nic"));
});

test("validate sql for sanctioned query", () => {
  const plan = { tables: ["sanctioned_vessels", "ais_events_ice"], views: [] };
  const sql = "SELECT sl.mmsi FROM defense.sanctioned_vessels sl JOIN defense.ais_events_ice a ON sl.mmsi = a.mmsi";
  const v = validateSqlMapping(sql, plan);
  assert.equal(v.warnings.length, 0);
});

test("semantic bindings for classes", () => {
  const b = semanticMappingForClasses(["GPSElectronicWarfareIndicator"]);
  assert.ok(b.some((x) => x.table === "gps_jammer_events"));
});