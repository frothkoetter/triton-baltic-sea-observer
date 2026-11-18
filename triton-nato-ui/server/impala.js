import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { lagebildEnginePreference, queryTrinoDefense } from "./data-engine.js";
import * as sample from "./sample-data.js";
import {
  PREDEFINED,
  buildAisTrackSql,
  buildGermanFleetTrackSql,
  buildRussianFleetTrackSql,
} from "./queries.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "impala-bridge.py");

let connectionState = {
  checked: false,
  live: false,
  engine: null,
  lastError: null,
};

function forceSample() {
  return process.env.TRITON_FORCE_SAMPLE_DATA === "true";
}

export function getConnectionState() {
  return { ...connectionState, forceSample: forceSample() };
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
  const live = await queryWarehouse(sql);
  if (live.ok) {
    return live;
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

export async function getAis() {
  return runQueryOrSample(PREDEFINED.aisLatest, () => sample.aisShips);
}

function filterSampleTrackRows(rows, { flag, vesselType, shipName } = {}) {
  return rows.filter((r) => {
    if (flag && r.flag !== flag) return false;
    if (vesselType && r.vessel_type !== vesselType) return false;
    if (shipName && r.ship_name !== shipName) return false;
    return true;
  });
}

/** Time-ordered positions for route map (AIS + DEU + RUS). */
export async function getPositionTracks(opts = {}) {
  const hours = Math.min(72, Math.max(1, Number(opts.hours) || 24));
  const flag = String(opts.flag ?? "").trim();
  const vesselType = String(opts.vesselType ?? "").trim();
  const shipName = String(opts.shipName ?? "").trim();
  const includeAis = opts.ais !== false;
  const includeGerman = opts.german !== false;
  const includeRussian = opts.russian !== false;

  const filterArgs = { hours, flag, vesselType, shipName };
  const jobs = [];
  if (includeAis) {
    jobs.push(
      runQueryOrSample(buildAisTrackSql(filterArgs), () =>
        filterSampleTrackRows(
          sample.samplePositionTrackRows().filter((r) => r.track_kind === "ais"),
          { flag, vesselType, shipName }
        )
      )
    );
  }
  if (includeGerman) {
    jobs.push(
      runQueryOrSample(buildGermanFleetTrackSql({ hours, shipName }), () =>
        filterSampleTrackRows(
          sample.samplePositionTrackRows().filter((r) => r.track_kind === "deu_fleet"),
          { shipName }
        )
      )
    );
  }
  if (includeRussian) {
    jobs.push(
      runQueryOrSample(buildRussianFleetTrackSql({ hours, shipName }), () =>
        filterSampleTrackRows(
          sample.samplePositionTrackRows().filter((r) => r.track_kind === "rus_fleet"),
          { shipName }
        )
      )
    );
  }

  const parts = await Promise.all(jobs);
  const rows = parts.flatMap((p) => p.rows ?? []);
  const source = parts.every((p) => p.source === "sample")
    ? "sample"
    : parts.find((p) => p.source && p.source !== "sample")?.source ?? "live";
  return {
    ok: parts.some((p) => p.ok),
    source,
    rows,
    hours,
    filters: { flag, vesselType, shipName },
  };
}

export async function getFleet() {
  return runQueryOrSample(PREDEFINED.fleet, () => sample.fleet);
}

export async function getSanctions() {
  return runQueryOrSample(PREDEFINED.sanctions, () => sample.sanctions);
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

export async function getLidarScans() {
  return runQueryOrSample(PREDEFINED.lidarScans, () => sample.lidarScans);
}

export async function getKpis() {
  if (forceSample()) {
    return { ok: true, source: "sample", ...sample.kpis };
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
  return { ok: true, source: "sample", ...sample.kpis, fallbackReason: fleet.error };
}

export function getDatasets() {
  return sample.datasets;
}
