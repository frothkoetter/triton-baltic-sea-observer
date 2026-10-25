import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as sample from "./sample-data.js";
import { PREDEFINED } from "./queries.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "impala-bridge.py");

let connectionState = {
  checked: false,
  live: false,
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
    connectionState.lastError = null;
  } else {
    connectionState.live = false;
    connectionState.lastError = result.error;
  }
  return result;
}

export async function runQueryOrSample(sql, sampleFn) {
  const impala = await queryImpala(sql);
  if (impala.ok) {
    return impala;
  }
  const rows = sampleFn();
  return {
    ok: true,
    source: "sample",
    columns: rows.length ? Object.keys(rows[0]) : [],
    rows,
    fallbackReason: impala.error,
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

export async function getKpis() {
  if (forceSample()) {
    return { ok: true, source: "sample", ...sample.kpis };
  }
  const [fleet, personnel, surv] = await Promise.all([
    queryImpala(PREDEFINED.fleetKpi),
    queryImpala(PREDEFINED.personnelKpi),
    queryImpala(PREDEFINED.surveillanceKpi),
  ]);
  if (fleet.ok && personnel.ok && surv.ok) {
    return {
      ok: true,
      source: "impala",
      fleetUnits: Number(fleet.rows?.[0]?.cnt ?? 0),
      personnel: Number(personnel.rows?.[0]?.cnt ?? 0),
      surveillanceReports: Number(surv.rows?.[0]?.cnt ?? 0),
      alerts: 2,
    };
  }
  return { ok: true, source: "sample", ...sample.kpis, fallbackReason: fleet.error };
}

export function getDatasets() {
  return sample.datasets;
}
