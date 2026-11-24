#!/usr/bin/env node
/**
 * Capture live Trino rows into server/data/live-snapshot/ for UI demo mode.
 * Requires TRINO_USER / TRINO_PASSWORD (same as Application).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { SNAPSHOT_PREDEFINED_MAP, SNAPSHOT_POSITION_TRACKS } from "../server/snapshot-datasets.js";
import {
  PREDEFINED,
  buildAisTrackSql,
  buildGermanFleetTrackSql,
  buildRussianFleetTrackSql,
} from "../server/queries.js";
import { runTrino, TRINO_CATALOG, TRINO_SCHEMA } from "../server/trino-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "..", "server", "data", "live-snapshot");

const BUILDERS = {
  buildAisTrackSql,
  buildGermanFleetTrackSql,
  buildRussianFleetTrackSql,
};

function trinoConfigured() {
  return Boolean(String(process.env.TRINO_USER ?? "").trim() && String(process.env.TRINO_PASSWORD ?? "").trim());
}

async function execSql(label, sql) {
  const res = await runTrino(sql);
  if (!res.ok) {
    throw new Error(`${label}: ${res.error ?? "Trino failed"}`);
  }
  return res.rows ?? [];
}

async function main() {
  if (!trinoConfigured()) {
    console.error("TRINO_USER/TRINO_PASSWORD required — source .env.trino.local on CML");
    process.exit(1);
  }
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const datasets = {};
  const stats = {};
  const errors = [];

  for (const [outKey, predefinedKey] of Object.entries(SNAPSHOT_PREDEFINED_MAP)) {
    const sql = PREDEFINED[predefinedKey];
    if (!sql) {
      errors.push(`missing PREDEFINED.${predefinedKey}`);
      continue;
    }
    try {
      const rows = await execSql(outKey, sql);
      const filename = `${outKey}.json`;
      fs.writeFileSync(path.join(OUT_DIR, filename), JSON.stringify(rows, null, 2), "utf8");
      datasets[outKey] = filename;
      stats[outKey] = rows.length;
      console.log(`OK ${outKey}: ${rows.length} rows`);
    } catch (e) {
      console.warn(`SKIP ${outKey}: ${e.message}`);
      errors.push(`${outKey}: ${e.message}`);
    }
  }

  const trackRows = [];
  const emptyFilter = {
    flags: [],
    vesselTypes: [],
    shipNames: [],
    germanUnits: [],
    russianUnits: [],
  };
  for (const spec of SNAPSHOT_POSITION_TRACKS) {
    const build = BUILDERS[spec.build];
    if (!build) continue;
    try {
      const sql = build(emptyFilter);
      const rows = await execSql(`tracks.${spec.key}`, sql);
      for (const row of rows) {
        trackRows.push({ ...row, track_kind: spec.key });
      }
      console.log(`OK positionTracks.${spec.key}: ${rows.length} points`);
    } catch (e) {
      console.warn(`SKIP tracks.${spec.key}: ${e.message}`);
    }
  }
  if (trackRows.length) {
    const filename = "positionTracks.json";
    fs.writeFileSync(path.join(OUT_DIR, filename), JSON.stringify(trackRows, null, 2), "utf8");
    datasets.positionTracks = filename;
    stats.positionTracks = trackRows.length;
  }

  const manifest = {
    version: 1,
    captured_at: new Date().toISOString(),
    source: "trino",
    catalog: TRINO_CATALOG,
    schema: TRINO_SCHEMA,
    datasets,
    row_counts: stats,
    capture_errors: errors,
  };
  fs.writeFileSync(path.join(OUT_DIR, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");
  console.log(`\nWrote ${Object.keys(datasets).length} datasets → ${OUT_DIR}`);
  console.log(`manifest captured_at=${manifest.captured_at}`);
  if (errors.length) {
    console.log(`Warnings: ${errors.length} (partial snapshot still usable)`);
    process.exitCode = errors.length === Object.keys(SNAPSHOT_PREDEFINED_MAP).length ? 1 : 0;
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});