/**
 * Demo mode: prefer JSON captured from live Trino (see scripts/capture-live-snapshot.mjs).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SNAPSHOT_DIR = path.join(__dirname, "data", "live-snapshot");
const MANIFEST_PATH = path.join(SNAPSHOT_DIR, "manifest.json");

let resolvedCache = null;
let resolvedCacheKey = null;

export function snapshotDir() {
  return SNAPSHOT_DIR;
}

function manifestCacheKey() {
  if (!fs.existsSync(MANIFEST_PATH)) return "none";
  try {
    return String(fs.statSync(MANIFEST_PATH).mtimeMs);
  } catch {
    return "none";
  }
}

export function readSnapshotManifest() {
  if (!fs.existsSync(MANIFEST_PATH)) return null;
  try {
    return JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"));
  } catch {
    return null;
  }
}

export function snapshotStatus() {
  const manifest = readSnapshotManifest();
  if (!manifest) {
    return { available: false, capturedAt: null, datasetCount: 0 };
  }
  let loaded = 0;
  for (const file of Object.values(manifest.datasets || {})) {
    if (fs.existsSync(path.join(SNAPSHOT_DIR, file))) loaded += 1;
  }
  return {
    available: loaded > 0,
    capturedAt: manifest.captured_at ?? null,
    datasetCount: loaded,
    rowCounts: manifest.row_counts ?? null,
    source: manifest.source ?? "trino",
    catalog: manifest.catalog ?? null,
    schema: manifest.schema ?? null,
  };
}

function loadDataset(filename) {
  const fp = path.join(SNAPSHOT_DIR, filename);
  if (!fs.existsSync(fp)) return null;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

/** Overlay live snapshot onto embedded sample exports. */
export function resolveSampleData(embedded) {
  const manifest = readSnapshotManifest();
  if (!manifest?.datasets) return embedded;

  const out = { ...embedded };
  let overlayCount = 0;
  for (const [key, filename] of Object.entries(manifest.datasets)) {
    const data = loadDataset(filename);
    if (data == null) continue;
    if (key === "positionTracks" && Array.isArray(data)) {
      out.samplePositionTrackRows = () => data;
      overlayCount += 1;
      continue;
    }
    if (key in out) {
      out[key] = data;
      overlayCount += 1;
    }
  }

  if (overlayCount === 0) return embedded;

  out.kpis = { ...embedded.kpis, ...computeKpisFromBundle(out) };

  out.meta = {
    ...embedded.meta,
    source: "sample",
    snapshot: true,
    snapshotCapturedAt: manifest.captured_at,
    snapshotDatasets: overlayCount,
    snapshotRowCounts: manifest.row_counts ?? null,
    message: `Demo aus Live-Snapshot (${manifest.captured_at ?? "unbekannt"})`,
  };
  return out;
}

/** Cached merge; invalidates when manifest.json mtime changes (re-capture without restart). */
export function getResolvedSampleData(embedded) {
  const key = manifestCacheKey();
  if (resolvedCache && resolvedCacheKey === key) return resolvedCache;
  resolvedCache = resolveSampleData(embedded);
  resolvedCacheKey = key;
  return resolvedCache;
}

export function resetResolvedSampleCache() {
  resolvedCache = null;
  resolvedCacheKey = null;
}

export function sampleResponseSource(bundle) {
  return bundle?.meta?.snapshot ? "sample-snapshot" : "sample";
}

export function computeKpisFromBundle(bundle) {
  const fleet = Array.isArray(bundle.fleet) ? bundle.fleet : [];
  const surveillance = Array.isArray(bundle.surveillance) ? bundle.surveillance : [];
  const outliers = Array.isArray(bundle.vesselOutliers) ? bundle.vesselOutliers : [];

  const fleetUnits = fleet.length;
  const personnel = fleet.reduce(
    (sum, r) => sum + Number(r.besatzung ?? r.crew_size ?? 0),
    0
  );
  const surveillanceReports = surveillance.length;
  const alerts = outliers.filter((r) => Number(r.severity_level ?? 0) >= 70).length;

  return {
    fleetUnits,
    personnel,
    surveillanceReports,
    alerts,
  };
}