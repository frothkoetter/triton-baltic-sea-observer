/**
 * Demo mode: prefer JSON captured from live Trino (see scripts/capture-live-snapshot.mjs).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SNAPSHOT_DIR = path.join(__dirname, "data", "live-snapshot");
const MANIFEST_PATH = path.join(SNAPSHOT_DIR, "manifest.json");

let cachedManifest = null;

export function readSnapshotManifest() {
  if (cachedManifest !== null) return cachedManifest;
  if (!fs.existsSync(MANIFEST_PATH)) {
    cachedManifest = false;
    return null;
  }
  try {
    cachedManifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"));
    return cachedManifest;
  } catch {
    cachedManifest = false;
    return null;
  }
}

export function snapshotStatus() {
  const manifest = readSnapshotManifest();
  if (!manifest) {
    return { available: false, capturedAt: null, datasetCount: 0 };
  }
  const files = manifest.datasets ? Object.keys(manifest.datasets) : [];
  let loaded = 0;
  for (const file of Object.values(manifest.datasets || {})) {
    if (fs.existsSync(path.join(SNAPSHOT_DIR, file))) loaded += 1;
  }
  return {
    available: loaded > 0,
    capturedAt: manifest.captured_at ?? null,
    datasetCount: loaded,
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

  out.meta = {
    ...embedded.meta,
    source: "sample",
    snapshot: true,
    snapshotCapturedAt: manifest.captured_at,
    snapshotDatasets: overlayCount,
    message: `Demo aus Live-Snapshot (${manifest.captured_at ?? "unbekannt"})`,
  };
  return out;
}