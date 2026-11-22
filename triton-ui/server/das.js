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

const previewCache = new Map();
const CACHE_TTL_MS = 90_000;
const CACHE_MAX = 24;

function cacheGet(key) {
  const hit = previewCache.get(key);
  if (!hit) return null;
  if (Date.now() - hit.at > CACHE_TTL_MS) {
    previewCache.delete(key);
    return null;
  }
  return hit.value;
}

function cacheSet(key, value) {
  if (previewCache.size >= CACHE_MAX) {
    const oldest = previewCache.keys().next().value;
    if (oldest) previewCache.delete(oldest);
  }
  previewCache.set(key, { at: Date.now(), value });
}

export function fetchDasSegmentPreview(hadoopFileUrl, segmentIndex = 0) {
  const check = validateDasUri(hadoopFileUrl);
  if (!check.ok) {
    return Promise.resolve(check);
  }
  const segIdx = Math.max(0, Math.floor(Number(segmentIndex)) || 0);
  const seg = String(segIdx);
  const cacheKey = `${check.uri}|${seg}`;
  const cached = cacheGet(cacheKey);
  if (cached) {
    return Promise.resolve(cached);
  }
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
        if (parsed.ok) {
          cacheSet(cacheKey, parsed);
        }
        resolve(parsed);
      } catch {
        resolve({ ok: false, error: err || out || "Invalid DAS bridge response" });
      }
    });
  });
}

export async function fetchDasSegmentWav(hadoopFileUrl, segmentIndex = 0) {
  const preview = await fetchDasSegmentPreview(hadoopFileUrl, segmentIndex);
  if (!preview.ok) {
    return preview;
  }
  const b64 = preview.audio?.wav_base64;
  if (!b64) {
    return { ok: false, error: "HDF5 segment has no audio waveform" };
  }
  return {
    ok: true,
    wav: Buffer.from(b64, "base64"),
    duration_sec: preview.audio?.duration_sec,
    sample_rate_hz: preview.audio?.sample_rate_hz,
  };
}