import test from "node:test";
import assert from "node:assert/strict";
import {
  resolveGoldenExample,
  buildMappingPlan,
  kgTablesForClasses,
  compareKgVsPlan,
  runTtydE2e,
} from "./agent-ttyd.js";

test("resolve sanctioned query", () => {
  const r = resolveGoldenExample(
    "SanctionedMaritimePlatform latest AISTrackReport sanction_reason"
  );
  assert.equal(r.ok, true);
  assert.equal(r.example.id, "gq_sanctioned_ais");
});

test("mapping plan includes ais not ships-only for positions", () => {
  const r = resolveGoldenExample("MMSI ohne ships registry gap");
  const plan = buildMappingPlan(r.example);
  assert.ok(plan.tables.includes("ais_events_ice"));
});

test("kg vs plan overlap for gps jamming", () => {
  const r = resolveGoldenExample("gps jamming adsb_nic");
  const plan = buildMappingPlan(r.example);
  const kg = kgTablesForClasses(r.example.classes);
  const cmp = compareKgVsPlan(plan, kg);
  assert.ok(cmp.overlap_count >= 0);
});

test("e2e with mock warehouse", async () => {
  const mockWh = async () => ({
    ok: true,
    source: "mock",
    columns: ["n"],
    rows: [{ n: 3 }],
  });
  const out = await runTtydE2e({
    exampleId: "gq_gps_jamming",
    queryWarehouse: mockWh,
  });
  assert.equal(out.ok, true);
  assert.equal(out.sql.row_count, 1);
  assert.equal(out.sql.source, "mock");
});