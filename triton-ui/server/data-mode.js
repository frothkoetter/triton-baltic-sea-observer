/**
 * Per-request data mode: live Trino vs embedded sample (cookie / header / env).
 */

import { AsyncLocalStorage } from "node:async_hooks";
import { trinoConfigured } from "./trino-client.js";
import { snapshotStatus } from "./sample-snapshot-loader.js";

const store = new AsyncLocalStorage();

const COOKIE_NAME = "triton_data_mode";
const HEADER_NAME = "x-triton-data-mode";

function parseCookieHeader(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(";")) {
    const [k, ...rest] = part.trim().split("=");
    if (k) out[k] = decodeURIComponent(rest.join("=").trim());
  }
  return out;
}

function normalizeMode(raw) {
  const m = String(raw ?? "").trim().toLowerCase();
  if (m === "live" || m === "sample" || m === "auto") return m;
  return null;
}

export function envForceSample() {
  return process.env.TRITON_FORCE_SAMPLE_DATA === "true";
}

export function envForceLive() {
  return process.env.TRITON_FORCE_LIVE_DATA === "true";
}

/** Resolve mode from request (before ALS). */
export function resolveDataModeFromRequest(req) {
  if (envForceSample()) return "sample";
  if (envForceLive()) return "live";
  const header = normalizeMode(req?.headers?.[HEADER_NAME]);
  if (header) return header;
  const cookies = parseCookieHeader(req?.headers?.cookie);
  const cookie = normalizeMode(cookies[COOKIE_NAME]);
  if (cookie) return cookie;
  const envDefault = normalizeMode(process.env.TRITON_DEFAULT_DATA_MODE);
  if (envDefault) return envDefault;
  return "auto";
}

export function runWithDataMode(req, fn) {
  const mode = resolveDataModeFromRequest(req);
  return store.run({ mode }, fn);
}

export function getEffectiveDataMode() {
  const mode = store.getStore()?.mode ?? normalizeMode(process.env.TRITON_DEFAULT_DATA_MODE) ?? "auto";
  if (envForceSample()) return "sample";
  if (envForceLive()) return "live";
  return mode;
}

export function shouldUseSampleData() {
  const mode = getEffectiveDataMode();
  if (mode === "sample") return true;
  if (mode === "live") return false;
  return !trinoConfigured();
}

export function getSampleReason() {
  if (envForceSample()) {
    return "TRITON_FORCE_SAMPLE_DATA=true (server env, cannot override)";
  }
  const mode = getEffectiveDataMode();
  if (mode === "sample") {
    return "Demo-Modus gewählt (Cookie/Header)";
  }
  if (!trinoConfigured()) {
    const user = String(process.env.TRINO_USER ?? "").trim();
    const pass = String(process.env.TRINO_PASSWORD ?? "").trim();
    const parts = [];
    if (!user) parts.push("TRINO_USER fehlt");
    if (!pass) parts.push("TRINO_PASSWORD fehlt");
    return parts.join("; ") || "Trino nicht konfiguriert";
  }
  return null;
}

export function dataModeMeta() {
  const mode = getEffectiveDataMode();
  const locked = envForceSample() || envForceLive();
  return {
    dataMode: mode,
    dataModeLocked: locked,
    forceSampleEnv: envForceSample(),
    forceLiveEnv: envForceLive(),
    trinoConfigured: trinoConfigured(),
    shouldUseSample: shouldUseSampleData(),
    sampleReason: getSampleReason(),
  };
}

export { COOKIE_NAME, HEADER_NAME };