import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { lagebildEnginePreference, queryTrinoDefense } from "./data-engine.js";
import * as sample from "./sample-data.js";
import {
  PREDEFINED,
  buildAisAnalyticsQueries,
  buildAisLatestSql,
  buildAisTrackSql,
  buildGermanFleetTrackSql,
  buildRussianFleetTrackSql,
  buildSanctionedAnalyticsQueries,
  buildRestrictedAnalyticsQueries,
} from "./queries.js";
import { shortenSanctionReason } from "./sanction-text.js";
import { getEffectiveDataMode, shouldUseSampleData } from "./data-mode.js";
import { normalizeDasSegment } from "./das-segment.js";
import { normalizeAisFilterObject } from "./ais-filters.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "impala-bridge.py");

let connectionState = {
  checked: false,
  live: false,
  engine: null,
  lastError: null,
};

function forceSample() {
  return shouldUseSampleData();
}

export function getConnectionState() {
  return {
    ...connectionState,
    forceSample: forceSample(),
    dataMode: getEffectiveDataMode(),
  };
}

/** Clear stale lastError after a successful warehouse ping (e.g. /api/health SELECT 1). */
export function noteWarehouseProbeOk(engine = "trino") {
  connectionState.checked = true;
  connectionState.live = true;
  connectionState.engine = engine;
  connectionState.lastError = null;
}

function runPython(sql) {
  return new Promise((resolve, reject) => {
    const child = spawn("python3", [BRIDGE, sql], {
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
    child.on("close", (code) => {
      try {
        const parsed = JSON.parse(out || "{}");
        if (code !== 0 && !parsed.error) {
          parsed.error = err || `Bridge exited ${code}`;
        }
        resolve(parsed);
      } catch {
        reject(new Error(err || out || "Invalid bridge response"));
      }
    });
  });
}

export async function queryImpala(sql) {
  if (forceSample()) {
    return { ok: false, error: "Sample mode forced", source: "sample" };
  }
  const result = await runPython(sql);
  connectionState.checked = true;
  if (result.ok) {
    connectionState.live = true;
    connectionState.engine = "impala";
    connectionState.lastError = null;
  } else {
    connectionState.live = false;
    connectionState.lastError = result.error;
  }
  return result;
}

/** Trino (Iceberg) or Impala — prefers Trino when TRINO_* is set. */
export async function queryWarehouse(sql) {
  if (forceSample()) {
    return { ok: false, error: "Sample mode forced", source: "sample" };
  }
  const pref = lagebildEnginePreference();
  if (pref === "trino") {
    const result = await queryTrinoDefense(sql);
    connectionState.checked = true;
    if (result.ok) {
      connectionState.live = true;
      connectionState.engine = "trino";
      connectionState.lastError = null;
      return result;
    }
    connectionState.live = false;
    connectionState.lastError = result.error;
    if (
      String(process.env.LAGEBILD_DATA_ENGINE ?? "auto").toLowerCase() === "auto" &&
      String(process.env.IMPALA_HOST ?? "").trim()
    ) {
      const impala = await queryImpala(sql);
      if (impala.ok) return impala;
    }
    return result;
  }
  if (pref === "impala") {
    return queryImpala(sql);
  }
  return {
    ok: false,
    error: "Set TRINO_USER/TRINO_PASSWORD or IMPALA_HOST",
    source: "none",
  };
}

export async function runQueryOrSample(sql, sampleFn) {
  if (shouldUseSampleData()) {
    const rows = sampleFn();
    return {
      ok: true,
      source: "sample",
      columns: rows.length ? Object.keys(rows[0]) : [],
      rows,
      fallbackReason: getEffectiveDataMode() === "sample" ? "demo_mode" : "trino_not_configured",
    };
  }
  const live = await queryWarehouse(sql);
  if (live.ok) {
    return live;
  }
  const mode = getEffectiveDataMode();
  if (mode === "live") {
    return {
      ok: false,
      source: "none",
      error: live.error || "Warehouse query failed",
      rows: [],
      columns: [],
    };
  }
  const rows = sampleFn();
  return {
    ok: true,
    source: "sample",
    columns: rows.length ? Object.keys(rows[0]) : [],
    rows,
    fallbackReason: live.error,
  };
}

export async function listTables() {
  const sql = "SHOW TABLES IN defense";
  const res = await runQueryOrSample(sql, () =>
    sample.tables.map((name) => ({ name, tab_name: name }))
  );
  if (res.source === "sample") {
    return res;
  }
  const rows = (res.rows || []).map((r) => ({
    name: r.name ?? r.tab_name ?? Object.values(r)[0],
  }));
  return { ...res, rows };
}

function matchesSampleAisFilters(r, filters = {}) {
  const { flags, vesselTypes, shipNames } = normalizeAisFilterObject(filters);
  if (flags.length && !flags.includes(String(r.flag ?? ""))) return false;
  if (vesselTypes.length && !vesselTypes.includes(String(r.vessel_type ?? ""))) return false;
  if (shipNames.length && !shipNames.includes(String(r.ship_name ?? ""))) return false;
  return true;
}

function filterSampleAisRows(rows, filters = {}) {
  return rows.filter((r) => matchesSampleAisFilters(r, filters));
}

function aggregateNameValue(rows, key) {
  const map = new Map();
  for (const r of rows) {
    const k = String(r[key] ?? "Unknown").trim() || "Unknown";
    map.set(k, (map.get(k) ?? 0) + 1);
  }
  return [...map.entries()]
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
}

function speedBand(speed) {
  const s = Number(speed);
  if (!Number.isFinite(s) || s < 0.5) return "At anchor / moored";
  if (s < 4) return "Slow (<4 kn)";
  if (s < 12) return "Coastal (4–12 kn)";
  return "Open sea (>12 kn)";
}

function sampleAisAnalytics(filters = {}) {
  const rows = filterSampleAisRows(sample.aisShips, filters);
  const byFlag = aggregateNameValue(rows, "flag");
  const byType = aggregateNameValue(rows, "vessel_type");
  const topDestinations = aggregateNameValue(
    rows.map((r) => ({ ...r, destination: r.destination ?? "Unknown" })),
    "destination"
  );
  const byStatus = aggregateNameValue(rows, "status");
  const bySpeedBandMap = new Map();
  for (const r of rows) {
    const b = speedBand(r.speed);
    bySpeedBandMap.set(b, (bySpeedBandMap.get(b) ?? 0) + 1);
  }
  const bySpeedBand = [...bySpeedBandMap.entries()].map(([name, value]) => ({ name, value }));
  const avgByFlagMap = new Map();
  const flagCounts = new Map();
  for (const r of rows) {
    const f = r.flag ?? "Unknown";
    avgByFlagMap.set(f, (avgByFlagMap.get(f) ?? 0) + Number(r.speed ?? 0));
    flagCounts.set(f, (flagCounts.get(f) ?? 0) + 1);
  }
  const avgSogByFlag = [...avgByFlagMap.entries()].map(([name, sum]) => ({
    name,
    value: Math.round((sum / (flagCounts.get(name) ?? 1)) * 100) / 100,
  }));
  const underway = rows.filter((r) => Number(r.speed ?? 0) >= 0.5).length;
  const avgSog =
    rows.length > 0 ? rows.reduce((a, r) => a + Number(r.speed ?? 0), 0) / rows.length : 0;
  return {
    panels: {
      kpi: {
        columns: ["total_ships", "avg_sog", "underway_count"],
        rows: [{ total_ships: rows.length, avg_sog: Math.round(avgSog * 10) / 10, underway_count: underway }],
      },
      byFlag: { columns: ["name", "value"], rows: byFlag },
      byType: { columns: ["name", "value"], rows: byType },
      topDestinations: { columns: ["name", "value"], rows: topDestinations },
      byStatus: { columns: ["name", "value"], rows: byStatus },
      bySpeedBand: { columns: ["name", "value"], rows: bySpeedBand },
      avgSogByFlag: { columns: ["name", "value"], rows: avgSogByFlag },
    },
  };
}

const AIS_ANALYTICS_PANELS = [
  "kpi",
  "byFlag",
  "byType",
  "topDestinations",
  "byStatus",
  "bySpeedBand",
  "avgSogByFlag",
];

export async function getAisAnalytics(filters = {}) {
  const queries = buildAisAnalyticsQueries(filters);
  if (forceSample()) {
    return { ok: true, source: "sample", filters, ...sampleAisAnalytics(filters) };
  }
  const results = await Promise.all(
    AIS_ANALYTICS_PANELS.map((key) => queryWarehouse(queries[key]))
  );
  const panels = {};
  for (let i = 0; i < AIS_ANALYTICS_PANELS.length; i++) {
    const key = AIS_ANALYTICS_PANELS[i];
    const res = results[i];
    panels[key] = { columns: res.columns ?? [], rows: res.rows ?? [], ok: res.ok };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      filters,
      ...sampleAisAnalytics(filters),
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, filters, panels };
}

export async function getAis(filters = {}) {
  const sql = buildAisLatestSql(filters);
  return runQueryOrSample(sql, () => filterSampleAisRows(sample.aisShips, filters));
}

function filterSampleFleetTrackRows(rows, filters = {}) {
  const { germanUnits, russianUnits } = normalizeAisFilterObject(filters);
  return rows.filter((r) => {
    const kind = String(r.track_kind ?? "");
    const name = String(r.ship_name ?? r.unit_name ?? "");
    if (kind === "deu_fleet" && germanUnits.length && !germanUnits.includes(name)) return false;
    if (kind === "rus_fleet" && russianUnits.length && !russianUnits.includes(name)) return false;
  if (kind === "ais" && !matchesSampleAisFilters(r, filters)) return false;
    return true;
  });
}

/** Time-ordered positions for route map (AIS + DEU + RUS). */
export async function getPositionTracks(opts = {}) {
  const hours = Math.min(72, Math.max(1, Number(opts.hours) || 24));
  const includeAis = opts.ais !== false;
  const includeGerman = opts.german !== false;
  const includeRussian = opts.russian !== false;
  const filterArgs = { hours, ...normalizeAisFilterObject(opts) };

  const jobs = [];
  if (includeAis) {
    jobs.push(
      runQueryOrSample(buildAisTrackSql(filterArgs), () =>
        filterSampleFleetTrackRows(
          sample.samplePositionTrackRows().filter((r) => r.track_kind === "ais"),
          filterArgs
        )
      )
    );
  }
  if (includeGerman) {
    jobs.push(
      runQueryOrSample(buildGermanFleetTrackSql(filterArgs), () =>
        filterSampleFleetTrackRows(
          sample.samplePositionTrackRows().filter((r) => r.track_kind === "deu_fleet"),
          filterArgs
        )
      )
    );
  }
  if (includeRussian) {
    jobs.push(
      runQueryOrSample(buildRussianFleetTrackSql(filterArgs), () =>
        filterSampleFleetTrackRows(
          sample.samplePositionTrackRows().filter((r) => r.track_kind === "rus_fleet"),
          filterArgs
        )
      )
    );
  }

  const parts = await Promise.all(jobs);
  const rows = parts.flatMap((p) => p.rows ?? []);
  const source = parts.every((p) => p.source === "sample")
    ? "sample"
    : parts.find((p) => p.source && p.source !== "sample")?.source ?? "live";
  const truncated = parts.some((p) => p.truncated);
  const rowCount = rows.length;
  const fetchLimits = parts
    .map((p) => p.fetchLimit)
    .filter((n) => typeof n === "number" && n > 0);
  return {
    ok: parts.some((p) => p.ok),
    source,
    rows,
    rowCount,
    truncated,
    fetchLimit: fetchLimits.length ? Math.max(...fetchLimits) : undefined,
    hours,
    filters: filterArgs,
  };
}

export async function getFleet() {
  return runQueryOrSample(PREDEFINED.fleet, () => sample.fleet);
}

const RUS_FLEET_ANALYTICS_KEYS = [
  ["unitDisposition", "rusAnalyticsUnitDisposition"],
  ["activityMix", "rusAnalyticsActivityMix"],
  ["categoryMix", "rusAnalyticsCategoryMix"],
  ["homeBaseMix", "rusAnalyticsHomeBaseMix"],
  ["restrictedUnits", "rusAnalyticsRestrictedUnits"],
  ["restrictedByArea", "rusAnalyticsRestrictedByArea"],
];

export async function getRusFleetAnalytics() {
  if (forceSample()) {
    return { ok: true, source: "sample", ...sample.rusFleetAnalytics };
  }
  const results = await Promise.all(
    RUS_FLEET_ANALYTICS_KEYS.map(([, sqlKey]) => queryWarehouse(PREDEFINED[sqlKey]))
  );
  const panels = {};
  for (let i = 0; i < RUS_FLEET_ANALYTICS_KEYS.length; i++) {
    const [panelKey] = RUS_FLEET_ANALYTICS_KEYS[i];
    const res = results[i];
    panels[panelKey] = {
      columns: res.columns ?? [],
      rows: res.rows ?? [],
      ok: res.ok,
    };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      ...sample.rusFleetAnalytics,
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, panels };
}

const FLEET_ANALYTICS_KEYS = [
  ["messagesFrom", "fleetAnalyticsMessagesFrom"],
  ["messagesTo", "fleetAnalyticsMessagesTo"],
  ["operationalStatus", "fleetAnalyticsOperational"],
  ["systemStatus", "fleetAnalyticsSystem"],
  ["destinations", "fleetAnalyticsDestinations"],
  ["unitStatus", "fleetAnalyticsUnitStatus"],
  ["recentMessages", "fleetAnalyticsRecentMessages"],
];

const VESSEL_OUTLIER_ANALYTICS_KEYS = [
  ["kpi", "vesselOutlierAnalyticsKpi"],
  ["byType", "vesselOutlierAnalyticsByType"],
  ["bySeverity", "vesselOutlierAnalyticsBySeverity"],
  ["topMmsi", "vesselOutlierAnalyticsTopMmsi"],
  ["recent", "vesselOutlierAnalyticsRecent"],
  ["hourly", "vesselOutlierAnalyticsHourly"],
];

function sampleVesselOutlierAnalytics() {
  const rows = sample.vesselOutliers;
  const byType = new Map();
  const bySev = new Map();
  const mmsiStats = new Map();
  for (const r of rows) {
    const t = String(r.outlier_type ?? "Unknown");
    byType.set(t, (byType.get(t) ?? 0) + 1);
    const sev = Number(r.severity_level ?? 0);
    const band =
      sev >= 90
        ? "90+ (critical)"
        : sev >= 70
          ? "70–89 (high)"
          : sev >= 50
            ? "50–69 (medium)"
            : "<50 (low)";
    bySev.set(band, (bySev.get(band) ?? 0) + 1);
    const key = String(r.mmsi);
    const cur = mmsiStats.get(key) ?? {
      mmsi: key,
      ship_name: key,
      event_count: 0,
      max_severity: 0,
      latest_type: t,
    };
    cur.event_count += 1;
    cur.max_severity = Math.max(cur.max_severity, sev);
    cur.latest_type = t;
    mmsiStats.set(key, cur);
  }
  const high = rows.filter((r) => Number(r.severity_level ?? 0) >= 70).length;
  return {
    panels: {
      kpi: {
        columns: ["total_events", "unique_mmsi", "high_severity", "distinct_types"],
        rows: [
          {
            total_events: rows.length,
            unique_mmsi: mmsiStats.size,
            high_severity: high,
            distinct_types: byType.size,
          },
        ],
      },
      byType: {
        columns: ["name", "value"],
        rows: [...byType.entries()].map(([name, value]) => ({ name, value })),
      },
      bySeverity: {
        columns: ["name", "value"],
        rows: [...bySev.entries()].map(([name, value]) => ({ name, value })),
      },
      topMmsi: {
        columns: ["mmsi", "ship_name", "event_count", "max_severity", "latest_type"],
        rows: [...mmsiStats.values()].sort(
          (a, b) => b.event_count - a.event_count || b.max_severity - a.max_severity
        ),
      },
      recent: {
        columns: [
          "mmsi",
          "ship_name",
          "event_timestamp",
          "latitude",
          "longitude",
          "speed",
          "course",
          "status",
          "outlier_type",
          "reason",
          "severity_level",
        ],
        rows: rows.map((r) => ({
          ...r,
          ship_name: null,
        })),
      },
      hourly: {
        columns: ["hour_bucket", "event_count"],
        rows: [{ hour_bucket: rows[0]?.event_timestamp ?? null, event_count: rows.length }],
      },
    },
  };
}

export async function getVesselOutlierAnalytics() {
  if (forceSample()) {
    return { ok: true, source: "sample", ...sampleVesselOutlierAnalytics() };
  }
  const results = await Promise.all(
    VESSEL_OUTLIER_ANALYTICS_KEYS.map(([, sqlKey]) => queryWarehouse(PREDEFINED[sqlKey]))
  );
  const panels = {};
  for (let i = 0; i < VESSEL_OUTLIER_ANALYTICS_KEYS.length; i++) {
    const [panelKey] = VESSEL_OUTLIER_ANALYTICS_KEYS[i];
    const res = results[i];
    panels[panelKey] = {
      columns: res.columns ?? [],
      rows: res.rows ?? [],
      ok: res.ok,
    };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      ...sampleVesselOutlierAnalytics(),
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, panels };
}

const BUOY_ANALYTICS_KEYS = [
  ["recent", "buoyAnalyticsRecent"],
  ["typeMix", "buoyAnalyticsTypeMix"],
  ["classificationGrid", "buoyAnalyticsClassificationGrid"],
  ["geoClusters", "buoyAnalyticsGeoClusters"],
];

export async function getBuoyAnalytics() {
  if (forceSample()) {
    return { ok: true, source: "sample", ...sample.buoyAnalytics };
  }
  const results = await Promise.all(
    BUOY_ANALYTICS_KEYS.map(([, sqlKey]) => queryWarehouse(PREDEFINED[sqlKey]))
  );
  const panels = {};
  for (let i = 0; i < BUOY_ANALYTICS_KEYS.length; i++) {
    const [panelKey] = BUOY_ANALYTICS_KEYS[i];
    const res = results[i];
    panels[panelKey] = {
      columns: res.columns ?? [],
      rows: res.rows ?? [],
      ok: res.ok,
    };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      ...sample.buoyAnalytics,
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, panels };
}

export async function getFleetAnalytics() {
  if (forceSample()) {
    return { ok: true, source: "sample", ...sample.fleetAnalytics };
  }
  const results = await Promise.all(
    FLEET_ANALYTICS_KEYS.map(([, sqlKey]) => queryWarehouse(PREDEFINED[sqlKey]))
  );
  const panels = {};
  for (let i = 0; i < FLEET_ANALYTICS_KEYS.length; i++) {
    const [panelKey] = FLEET_ANALYTICS_KEYS[i];
    const res = results[i];
    panels[panelKey] = {
      columns: res.columns ?? [],
      rows: res.rows ?? [],
      ok: res.ok,
    };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      ...sample.fleetAnalytics,
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, panels };
}

export async function getSanctions() {
  return runQueryOrSample(PREDEFINED.sanctions, () => sample.sanctions);
}

function sampleSanctionedRouteLegs(historyRows) {
  const byMmsi = new Map();
  for (const r of historyRows) {
    const list = byMmsi.get(r.mmsi) ?? [];
    list.push(r.destination);
    byMmsi.set(r.mmsi, list);
  }
  const routeMap = new Map();
  for (const dests of byMmsi.values()) {
    for (let i = 1; i < dests.length; i++) {
      const prev = dests[i - 1];
      const cur = dests[i];
      if (prev && cur && prev !== cur) {
        const route = `${prev} → ${cur}`;
        routeMap.set(route, (routeMap.get(route) ?? 0) + 1);
      }
    }
  }
  return [...routeMap.entries()]
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
}

function sampleSanctionedAnalytics() {
  const aisByMmsi = new Map(sample.aisShips.map((r) => [r.mmsi, r]));
  const registry = sample.sanctions.map((sv) => {
    const ais = sv.MMSI != null ? aisByMmsi.get(sv.MMSI) : undefined;
    const reason = sv.Sanction_Reason;
    return {
      name: sv.Name,
      mmsi: sv.MMSI ?? null,
      imo: sv.IMO ?? null,
      vessel_type: sv.Type,
      flag: sv.Flag,
      sanction_reason_short: shortenSanctionReason(reason),
      sanction_reason: reason,
      linked_to: sv.Linked_To,
      destination: ais?.destination ?? "—",
      latest_ais_time: ais?.event_timestamp ?? null,
      latest_latitude: ais?.latitude ?? null,
      latest_longitude: ais?.longitude ?? null,
    };
  });
  const withMmsi = registry.filter((r) => r.mmsi != null).length;
  const withAis = registry.filter((r) => r.latest_ais_time).length;
  const withDest = registry.filter((r) => r.destination && r.destination !== "—").length;
  const destHistory = aggregateNameValue(
    sample.sanctionedAisDestHistory.map((r) => ({ dest: r.destination })),
    "dest"
  );
  const topRoutes = sampleSanctionedRouteLegs(sample.sanctionedAisDestHistory);
  const byReason = aggregateNameValue(
    registry.map((r) => ({ reason_short: r.sanction_reason_short })),
    "reason_short"
  );
  const byFlag = aggregateNameValue(registry, "flag");
  return {
    panels: {
      kpi: {
        columns: ["total_listed", "with_mmsi", "with_ais", "with_destination"],
        rows: [{ total_listed: registry.length, with_mmsi: withMmsi, with_ais: withAis, with_destination: withDest }],
      },
      registry: {
        columns: [
          "name",
          "mmsi",
          "flag",
          "vessel_type",
          "destination",
          "sanction_reason_short",
          "sanction_reason",
          "linked_to",
          "latest_ais_time",
        ],
        rows: registry,
      },
      destinationHistory: { columns: ["name", "value"], rows: destHistory },
      topRoutes: { columns: ["name", "value"], rows: topRoutes },
      byReason: { columns: ["name", "value"], rows: byReason },
      byFlag: { columns: ["name", "value"], rows: byFlag },
    },
  };
}

const SANCTIONED_ANALYTICS_PANELS = [
  "kpi",
  "registry",
  "destinationHistory",
  "topRoutes",
  "byReason",
  "byFlag",
];

export async function getSanctionedAnalytics() {
  const queries = buildSanctionedAnalyticsQueries();
  if (forceSample()) {
    return { ok: true, source: "sample", ...sampleSanctionedAnalytics() };
  }
  const results = await Promise.all(
    SANCTIONED_ANALYTICS_PANELS.map((key) => queryWarehouse(queries[key]))
  );
  const panels = {};
  for (let i = 0; i < SANCTIONED_ANALYTICS_PANELS.length; i++) {
    const key = SANCTIONED_ANALYTICS_PANELS[i];
    const res = results[i];
    panels[key] = { columns: res.columns ?? [], rows: res.rows ?? [], ok: res.ok };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      ...sampleSanctionedAnalytics(),
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, panels };
}

function sampleRestrictedAnalytics() {
  const rows = sample.restrictedRouteTracks;
  const vesselKey = (r) => `${r.mmsi}`;
  const zoneSet = new Set(rows.map((r) => r.area_id));
  const mmsiSet = new Set(rows.map((r) => r.mmsi));
  const pairSet = new Set(rows.map((r) => `${r.mmsi}:${r.area_id}`));
  const byZoneMap = new Map();
  const byFlagMap = new Map();
  const byTypeMap = new Map();
  const vesselStats = new Map();
  for (const r of rows) {
    byZoneMap.set(r.area_id, new Set([...(byZoneMap.get(r.area_id) ?? []), r.mmsi]));
    byFlagMap.set(r.flag, new Set([...(byFlagMap.get(r.flag) ?? []), r.mmsi]));
    byTypeMap.set(r.vessel_type, new Set([...(byTypeMap.get(r.vessel_type) ?? []), r.mmsi]));
    const vs = vesselStats.get(vesselKey(r)) ?? {
      ship_name: r.ship_name,
      mmsi: r.mmsi,
      vessel_type: r.vessel_type,
      flag: r.flag,
      zones: new Set(),
      reports: 0,
    };
    vs.zones.add(r.area_id);
    vs.reports += 1;
    vesselStats.set(vesselKey(r), vs);
  }
  const byZone = [...byZoneMap.entries()]
    .map(([name, set]) => ({ name, value: set.size }))
    .sort((a, b) => b.value - a.value);
  const byFlag = [...byFlagMap.entries()]
    .map(([name, set]) => ({ name, value: set.size }))
    .sort((a, b) => b.value - a.value);
  const topByType = [...byTypeMap.entries()]
    .map(([name, set]) => ({ name, value: set.size }))
    .sort((a, b) => b.value - a.value);
  const topVessels = [...vesselStats.values()]
    .map((v) => ({
      ship_name: v.ship_name,
      mmsi: v.mmsi,
      vessel_type: v.vessel_type,
      flag: v.flag,
      zones_hit: v.zones.size,
      reports_inside: v.reports,
    }))
    .sort((a, b) => b.zones_hit - a.zones_hit || b.reports_inside - a.reports_inside);
  const provocateurs = topVessels.slice(0, 12);
  return {
    panels: {
      kpi: {
        columns: ["unique_vessels", "zones_with_traffic", "position_reports", "vessel_zone_visits"],
        rows: [
          {
            unique_vessels: mmsiSet.size,
            zones_with_traffic: zoneSet.size,
            position_reports: rows.length,
            vessel_zone_visits: pairSet.size,
          },
        ],
      },
      byZone: { columns: ["name", "value"], rows: byZone },
      byFlag: { columns: ["name", "value"], rows: byFlag },
      topByType: { columns: ["name", "value"], rows: topByType },
      topVessels: {
        columns: ["ship_name", "mmsi", "vessel_type", "flag", "zones_hit", "reports_inside"],
        rows: topVessels,
      },
      provocateurs: {
        columns: ["ship_name", "mmsi", "vessel_type", "flag", "zones_hit", "reports_inside"],
        rows: provocateurs,
      },
      routeTracks: {
        columns: [
          "mmsi",
          "ship_name",
          "vessel_type",
          "flag",
          "area_id",
          "latitude",
          "longitude",
          "event_timestamp",
        ],
        rows,
      },
    },
  };
}

const RESTRICTED_ANALYTICS_PANELS = [
  "kpi",
  "byZone",
  "byFlag",
  "topByType",
  "topVessels",
  "provocateurs",
  "routeTracks",
];

export async function getRestrictedAnalytics() {
  const queries = buildRestrictedAnalyticsQueries();
  if (forceSample()) {
    return { ok: true, source: "sample", ...sampleRestrictedAnalytics() };
  }
  const results = await Promise.all(
    RESTRICTED_ANALYTICS_PANELS.map((key) => queryWarehouse(queries[key]))
  );
  const panels = {};
  for (let i = 0; i < RESTRICTED_ANALYTICS_PANELS.length; i++) {
    const key = RESTRICTED_ANALYTICS_PANELS[i];
    const res = results[i];
    panels[key] = { columns: res.columns ?? [], rows: res.rows ?? [], ok: res.ok };
  }
  const source = results.find((r) => r.ok && r.source)?.source ?? "sample";
  const allOk = results.every((r) => r.ok);
  if (!allOk) {
    return {
      ok: true,
      source: "sample",
      ...sampleRestrictedAnalytics(),
      fallbackReason: results.find((r) => !r.ok)?.error,
    };
  }
  return { ok: true, source, panels };
}

export async function getAreaViolations() {
  return runQueryOrSample(PREDEFINED.areaViolations, () => sample.areaViolations);
}

export async function getSurveillance() {
  return runQueryOrSample(PREDEFINED.surveillance, () => sample.surveillance);
}

export async function getObservationAreas() {
  return runQueryOrSample(PREDEFINED.observationAreas, () => sample.observationAreas);
}

export async function getOstseeInfrastructure() {
  const [infra, pipelines] = await Promise.all([
    runQueryOrSample(PREDEFINED.ostseeInfrastruktur, () => sample.ostseeInfrastruktur),
    runQueryOrSample(PREDEFINED.ostseePipelines, () => sample.ostseePipelines),
  ]);
  const source =
    infra.source === "sample" && pipelines.source === "sample"
      ? "sample"
      : infra.source ?? pipelines.source ?? "unknown";
  return {
    ok: infra.ok || pipelines.ok,
    source,
    infrastruktur: infra.rows ?? [],
    pipelines: pipelines.rows ?? [],
    fallbackReason: infra.fallbackReason ?? pipelines.fallbackReason,
  };
}

/** vessel_outliers for map; falls back to sample when warehouse unavailable. */
export async function getFleetPositions() {
  const [german, russian] = await Promise.all([
    runQueryOrSample(PREDEFINED.germanFleetPositions, () => sample.germanFleetPositions),
    runQueryOrSample(PREDEFINED.russianFleetPositions, () => sample.russianFleetPositions),
  ]);
  const source =
    german.source === "sample" && russian.source === "sample"
      ? "sample"
      : german.source ?? russian.source ?? "unknown";
  return {
    ok: german.ok || russian.ok,
    source,
    german: german.rows ?? [],
    russian: russian.rows ?? [],
    fallbackReason: german.fallbackReason ?? russian.fallbackReason,
  };
}

export async function getVesselOutliers() {
  const live = await queryWarehouse(PREDEFINED.vesselOutliersMap);
  if (live.ok && (live.rows?.length ?? 0) > 0) {
    return live;
  }
  const lagebild = await queryWarehouse(PREDEFINED.lagebildOutliersMap);
  if (lagebild.ok && (lagebild.rows?.length ?? 0) > 0) {
    return lagebild;
  }
  return runQueryOrSample(PREDEFINED.vesselOutliersMap, () => sample.vesselOutliers);
}

/** GPS jamming tiles (gps_jammer_events or lagebild GPS_Jammer). */
export async function getGpsJammerEvents() {
  const live = await queryWarehouse(PREDEFINED.gpsJammerEvents);
  if (live.ok && (live.rows?.length ?? 0) > 0) {
    return live;
  }
  const lagebild = await queryWarehouse(PREDEFINED.lagebildGpsJammer);
  if (lagebild.ok && (lagebild.rows?.length ?? 0) > 0) {
    return lagebild;
  }
  return runQueryOrSample(PREDEFINED.gpsJammerEvents, () => sample.gpsJammerEvents);
}

export async function getLidarScans() {
  return runQueryOrSample(PREDEFINED.lidarScans, () => sample.lidarScans);
}

export async function getDasCableSegments() {
  const raw = await runQueryOrSample(PREDEFINED.dasCableSegments, () => sample.dasCableSegments);
  const rows = raw.rows ?? raw.samples ?? [];
  const normalized = Array.isArray(rows) ? rows.map((r) => normalizeDasSegment(r)) : rows;
  return { ...raw, rows: normalized };
}

export async function getKpis() {
  if (forceSample()) {
    return {
      ok: true,
      source: "sample",
      ...sample.kpis,
      demoMode: true,
    };
  }
  const [fleet, personnel, surv, alerts] = await Promise.all([
    queryWarehouse(PREDEFINED.fleetKpi),
    queryWarehouse(PREDEFINED.personnelKpi),
    queryWarehouse(PREDEFINED.surveillanceKpi),
    queryWarehouse(PREDEFINED.alertsKpi),
  ]);
  const engine = fleet.source ?? connectionState.engine ?? "live";
  if (fleet.ok && personnel.ok && surv.ok) {
    return {
      ok: true,
      source: engine,
      fleetUnits: Number(fleet.rows?.[0]?.cnt ?? 0),
      personnel: Number(personnel.rows?.[0]?.cnt ?? 0),
      surveillanceReports: Number(surv.rows?.[0]?.cnt ?? 0),
      alerts: alerts.ok ? Number(alerts.rows?.[0]?.cnt ?? 0) : 2,
    };
  }
  return {
    ok: true,
    source: "sample",
    ...sample.kpis,
    fallbackReason: fleet.error,
    demoMode: true,
  };
}

export function getDatasets() {
  return sample.datasets;
}
