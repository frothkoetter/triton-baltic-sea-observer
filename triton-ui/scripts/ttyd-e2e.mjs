#!/usr/bin/env node
/**
 * E2E smoke: ontology resolve + KG compare + SQL (live Trino if env set, else sample path).
 * Usage: node scripts/ttyd-e2e.mjs [baseUrl]
 */
import { runTtydE2e } from "../server/agent-ttyd.js";
import { queryWarehouse } from "../server/impala.js";

const base = process.argv[2] || `http://127.0.0.1:${process.env.PORT || 3847}`;
const useHttp = process.argv[2] !== "--local";

const EXAMPLES = [
  "gq_sanctioned_ais",
  "gq_gps_jamming",
  "gq_lagebild_sources",
  "gq_registry_gap",
  "gq_area_violation",
  "gq_outliers",
];

async function runLocal() {
  const results = [];
  for (const id of EXAMPLES) {
    const out = await runTtydE2e({ exampleId: id, queryWarehouse });
    results.push({
      id,
      ok: out.ok,
      sql_ok: out.sql?.ok,
      source: out.sql?.source,
      rows: out.sql?.row_count,
      compare: out.kg_compare?.overlap_count,
      summary: out.synthesis?.summary_de,
    });
  }
  console.log(JSON.stringify({ mode: "local", results }, null, 2));
  const failed = results.filter((r) => !r.ok || !r.sql_ok);
  process.exit(failed.length ? 1 : 0);
}

async function runHttp() {
  const results = [];
  for (const id of EXAMPLES) {
    const res = await fetch(`${base}/api/agents/ttyd/e2e`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ example_id: id }),
    });
    const out = await res.json();
    results.push({
      id,
      http: res.status,
      ok: out.ok,
      sql_ok: out.sql?.ok,
      source: out.sql?.source,
      rows: out.sql?.row_count,
    });
  }
  console.log(JSON.stringify({ mode: "http", base, results }, null, 2));
}

if (process.argv.includes("--local") || process.argv[2] === "--local") {
  await runLocal();
} else {
  try {
    await runHttp();
  } catch (e) {
    console.error("HTTP failed, falling back to --local", e.message);
    await runLocal();
  }
}