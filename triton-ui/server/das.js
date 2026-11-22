import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { s3Configured, validateLidarUri } from "./lidar.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "das-h5-bridge.py");

export { s3Configured as dasS3Configured };

export function validateDasUri(uri) {
  return validateLidarUri(uri);
}

export function fetchDasSegmentPreview(hadoopFileUrl, segmentIndex = 0) {
  const check = validateDasUri(hadoopFileUrl);
  if (!check.ok) {
    return Promise.resolve(check);
  }
  const seg = Number.isFinite(Number(segmentIndex)) ? String(Math.max(0, Math.floor(Number(segmentIndex)))) : "0";
  return new Promise((resolve) => {
    const child = spawn("python3", [BRIDGE, check.uri, seg], {
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
        const parsed = JSON.parse(out || "{}");
        if (!parsed.ok && !parsed.error) {
          parsed.error = err || "DAS HDF5 bridge failed";
        }
        resolve(parsed);
      } catch {
        resolve({ ok: false, error: err || out || "Invalid DAS bridge response" });
      }
    });
  });
}