import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { shouldUseSampleData } from "./data-mode.js";
import { loadSnapshotDataset } from "./sample-snapshot-loader.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "sitrep-bridge.py");

export function sitrepConfigured() {
  return Boolean(
    process.env.SITREP_S3_PREFIX ||
      process.env.LAGEBILD_S3_PREFIX ||
      process.env.LAGEBILD_ARTIFACTS_DIR ||
      process.env.SITREP_ARTIFACTS_DIR
  );
}

function runBridge(args) {
  return new Promise((resolve) => {
    const child = spawn("python3", [BRIDGE, ...args], {
      env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => {
      out += d.toString();
    });
    child.stderr.on("data", (d) => {
      err += d.toString();
    });
    child.on("close", () => {
      try {
        resolve(JSON.parse(out || "{}"));
      } catch {
        resolve({ ok: false, error: err || out || "Invalid sitrep bridge response" });
      }
    });
  });
}

export async function listSitrepReports() {
  if (shouldUseSampleData()) {
    const rows = loadSnapshotDataset("sitrepReports");
    if (Array.isArray(rows) && rows.length > 0) {
      return { ok: true, source: "sample-snapshot", rows };
    }
  }
  return runBridge(["list"]);
}

export async function fetchSitrepPdfBase64(pdfName) {
  return runBridge(["pdf", pdfName]);
}