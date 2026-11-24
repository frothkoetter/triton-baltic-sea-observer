import test from "node:test";
import assert from "node:assert/strict";
import { buildAisLatestSql, buildAisAnalyticsQueries } from "./queries.js";

test("ais snapshot CTE uses AND for flag filters (no second WHERE)", () => {
  const sql = buildAisLatestSql({ flags: ["DE", "NO"] });
  assert.ok(sql.includes("WHERE rn = 1"), "expected rn filter");
  assert.ok(sql.includes("AND flag IN ('DE', 'NO')"), "expected flag AND clause");
  assert.equal((sql.match(/\bWHERE\b/g) || []).length, 2, "only two WHERE (latest + ais_snapshot)");
});

test("ais analytics KPI attaches SELECT after closed CTE", () => {
  const sql = buildAisAnalyticsQueries({ flags: ["DE"] }).kpi;
  assert.match(sql, /WHERE rn = 1\s+AND flag IN \('DE'\)\s+\)\s+SELECT/s);
});