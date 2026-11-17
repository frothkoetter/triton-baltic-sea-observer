import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "lidar-las-bridge.py");

const S3_URI = /^s3a?:\/\//i;

export function s3Configured() {
  return Boolean(
    (process.env.S3_WORKLOAD_USER && process.env.S3_WORKLOAD_PASSWORD) ||
      (process.env.S3_ACCESS_KEY_ID && process.env.S3_SECRET_ACCESS_KEY) ||
      (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) ||
      process.env.LIDAR_USE_CML_S3 === "true"
  );
}

export function validateLidarUri(uri) {
  const s = String(uri ?? "").trim();
  if (!S3_URI.test(s)) {
    return { ok: false, error: "hadoop_file_url must be s3:// or s3a://" };
  }
  if (s.length > 2048) {
    return { ok: false, error: "URI too long" };
  }
  return { ok: true, uri: s };
}

export function fetchLidarPointsFromS3(hadoopFileUrl) {
  const check = validateLidarUri(hadoopFileUrl);
  if (!check.ok) {
    return Promise.resolve(check);
  }
  return new Promise((resolve) => {
    const child = spawn("python3", [BRIDGE, check.uri], {
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
          parsed.error = err || "LAS bridge failed";
        }
        resolve(parsed);
      } catch {
        resolve({ ok: false, error: err || out || "Invalid LAS bridge response" });
      }
    });
  });
}