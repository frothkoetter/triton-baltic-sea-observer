import test from "node:test";
import assert from "node:assert/strict";
import {
  computeKpisFromBundle,
  resolveSampleData,
  resetResolvedSampleCache,
  snapshotStatus,
} from "./sample-snapshot-loader.js";

test("snapshot status without manifest", () => {
  const s = snapshotStatus();
  assert.equal(typeof s.available, "boolean");
});

test("resolve returns embedded when no overlay", () => {
  resetResolvedSampleCache();
  const embedded = { meta: { source: "sample" }, aisShips: [{ mmsi: 1 }] };
  const out = resolveSampleData(embedded);
  assert.deepEqual(out.aisShips, embedded.aisShips);
});

test("computeKpisFromBundle derives counts from snapshot arrays", () => {
  const kpis = computeKpisFromBundle({
    fleet: [{ besatzung: 100 }, { besatzung: 50 }],
    surveillance: [{ id: 1 }, { id: 2 }, { id: 3 }],
    vesselOutliers: [{ severity_level: 80 }, { severity_level: 50 }],
  });
  assert.equal(kpis.fleetUnits, 2);
  assert.equal(kpis.personnel, 150);
  assert.equal(kpis.surveillanceReports, 3);
  assert.equal(kpis.alerts, 1);
});