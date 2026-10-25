import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const DEFAULT_UPSTREAM =
  "https://triton-csa-manager0.cdwh-cdp.djki-j7ns.cloudera.site/triton-csa/cdp-proxy-api/ssb-mve-api/api/v1/query/5196/real_time_proximity_alert";

const FIELD_KEYS = [
  "ship_a",
  "ship_b",
  "event_time",
  "ship_b_time",
  "ship_a_lat",
  "ship_a_lon",
  "ship_b_lat",
  "ship_b_lon",
  "distance_km",
];

function forceSample() {
  return process.env.TRITON_FORCE_SAMPLE_DATA === "true";
}

function pickField(row, key) {
  if (row == null || typeof row !== "object") return undefined;
  if (key in row) return row[key];
  const lower = key.toLowerCase();
  for (const [k, v] of Object.entries(row)) {
    if (k.toLowerCase() === lower) return v;
  }
  const snake = key.replace(/([A-Z])/g, "_$1").toLowerCase();
  if (snake in row) return row[snake];
  return undefined;
}

function normalizeRow(row) {
  const out = {};
  for (const key of FIELD_KEYS) {
    const val = pickField(row, key);
    if (val !== undefined && val !== null) {
      out[key] = String(val);
    }
  }
  if (
    out.ship_a &&
    out.ship_b &&
    out.event_time &&
    out.ship_a_lat &&
    out.ship_a_lon &&
    out.ship_b_lat &&
    out.ship_b_lon &&
    out.distance_km
  ) {
    if (!out.ship_b_time) out.ship_b_time = out.event_time;
    return out;
  }
  return null;
}

function extractRawRows(payload) {
  if (payload == null) return [];
  if (Array.isArray(payload)) return payload;
  if (typeof payload !== "object") return [];

  const candidates = [
    payload.data,
    payload.rows,
    payload.records,
    payload.result,
    payload.results,
    payload.items,
  ];
  for (const c of candidates) {
    if (Array.isArray(c)) return c;
    if (c && typeof c === "object") {
      if (Array.isArray(c.data)) return c.data;
      if (Array.isArray(c.rows)) return c.rows;
      if (Array.isArray(c.records)) return c.records;
    }
  }

  if (Array.isArray(payload.columns) && Array.isArray(payload.rows)) {
    const cols = payload.columns.map((c) =>
      typeof c === "string" ? c : String(c.name ?? c.field ?? c)
    );
    return payload.rows.map((r) => {
      if (Array.isArray(r)) {
        const obj = {};
        cols.forEach((col, i) => {
          obj[col] = r[i];
        });
        return obj;
      }
      return r;
    });
  }

  return [];
}

export function mapProximityPayload(payload) {
  const rawRows = extractRawRows(payload);
  const events = [];
  for (const row of rawRows) {
    const normalized = normalizeRow(row);
    if (normalized) events.push(normalized);
  }
  return events;
}

async function loadStaticSample() {
  const samplePath = path.join(__dirname, "..", "src", "data", "ship-proximity-events.json");
  const text = await fs.readFile(samplePath, "utf8");
  const parsed = JSON.parse(text);
  return mapProximityPayload(parsed);
}

function buildUpstreamUrl() {
  const base = (process.env.PROXIMITY_ALERT_API_URL || DEFAULT_UPSTREAM).replace(/\/$/, "");
  const key = process.env.PROXIMITY_ALERT_API_KEY;
  if (!key) {
    return { ok: false, error: "PROXIMITY_ALERT_API_KEY is not set" };
  }
  const limit = Number(process.env.PROXIMITY_ALERT_LIMIT || 100);
  const url = new URL(base);
  url.searchParams.set("key", key);
  url.searchParams.set("limit", String(limit));
  return { ok: true, url: url.toString() };
}

export async function fetchProximityAlertsLive() {
  if (forceSample()) {
    const events = await loadStaticSample();
    return {
      ok: true,
      source: "sample",
      events,
      fallbackReason: "TRITON_FORCE_SAMPLE_DATA=true",
    };
  }

  const built = buildUpstreamUrl();
  if (!built.ok) {
    const events = await loadStaticSample();
    return {
      ok: true,
      source: "sample",
      events,
      fallbackReason: built.error,
    };
  }

  const controller = new AbortController();
  const timeoutMs = Number(process.env.PROXIMITY_ALERT_TIMEOUT_MS || 45000);
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(built.url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Upstream HTTP ${res.status}${body ? `: ${body.slice(0, 200)}` : ""}`);
    }
    const payload = await res.json();
    const events = mapProximityPayload(payload);
    if (!events.length) {
      throw new Error("Upstream returned no mappable proximity rows");
    }
    return { ok: true, source: "live", events };
  } catch (err) {
    const events = await loadStaticSample();
    const message = err instanceof Error ? err.message : String(err);
    return {
      ok: true,
      source: "sample",
      events,
      fallbackReason: message,
    };
  } finally {
    clearTimeout(timer);
  }
}
