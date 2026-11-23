/** Regression: Trino replace() must not trip read-only validator (Node-side copy of rule). */
import test from "node:test";
import assert from "node:assert/strict";
import { validateReadOnlySql } from "../../../triton-ui/server/sql-validator.js";

test("allows replace() in SELECT", () => {
  const sql = "SELECT replace(event_timestamp, ' ', 'T') FROM defense.ais_events_ice";
  const r = validateReadOnlySql(sql);
  assert.equal(r.ok, true);
});