import test from "node:test";
import assert from "node:assert/strict";
import { resolveSampleData, snapshotStatus } from "./sample-snapshot-loader.js";

test("snapshot status without manifest", () => {
  const s = snapshotStatus();
  assert.equal(typeof s.available, "boolean");
});

test("resolve returns embedded when no overlay", () => {
  const embedded = { meta: { source: "sample" }, aisShips: [{ mmsi: 1 }] };
  const out = resolveSampleData(embedded);
  assert.deepEqual(out.aisShips, embedded.aisShips);
});