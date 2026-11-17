import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { nbaIncidentsSample, nbaCoaRunsSample } from "./nba-sample.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(__dirname, "trino-bridge.py");

const CATALOG = process.env.TRINO_CATALOG || "iceberg";
const SCHEMA = process.env.TRINO_SCHEMA || "defense";
const FQ = `${CATALOG}.${SCHEMA}`;

let trinoState = {
  checked: false,
  live: false,
  lastError: null,
};

function esc(value) {
  return String(value).replace(/'/g, "''");
}

function escUuid(value) {
  const v = String(value);
  if (!/^[0-9a-f-]{36}$/i.test(v)) {
    throw new Error("Invalid flow_uuid");
  }
  return v;
}

function escCoa(value) {
  const v = String(value).toUpperCase();
  if (!/^[A-D]$/.test(v)) {
    throw new Error("COA id must be A, B, C, or D");
  }
  return v;
}

function runTrino(sql, mode = "query") {
  return new Promise((resolve, reject) => {
    const child = spawn("python3", [BRIDGE, sql, mode], {
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
        trinoState.checked = true;
        if (parsed.ok) {
          trinoState.live = true;
          trinoState.lastError = null;
        } else {
          trinoState.live = false;
          trinoState.lastError = parsed.error || err;
        }
        if (code !== 0 && !parsed.error) {
          parsed.error = err || `Bridge exited ${code}`;
        }
        resolve(parsed);
      } catch {
        reject(new Error(err || out || "Invalid Trino bridge response"));
      }
    });
  });
}

export function getNbaTrinoState() {
  return { ...trinoState };
}

const INCIDENTS_SQL = `
SELECT flow_uuid, status, alert_type, priority, theatre, latitude, longitude,
       best_coa_id, approved_coa_id, approved_at, approved_by, sitrep_link,
       created_at, updated_at
FROM ${FQ}.nba_incidents
ORDER BY created_at DESC
LIMIT 50
`;

export async function listNbaIncidents() {
  if (process.env.TRITON_FORCE_SAMPLE_DATA === "true") {
    return {
      ok: true,
      source: "sample",
      rows: nbaIncidentsSample,
    };
  }
  const result = await runTrino(INCIDENTS_SQL);
  if (result.ok) {
    return result;
  }
  return {
    ok: true,
    source: "sample",
    rows: nbaIncidentsSample,
    fallbackReason: result.error,
  };
}

export async function getNbaCoaRuns(flowUuid) {
  const id = escUuid(flowUuid);
  const sql = `
SELECT coa_id, rank, score_total, score_latenz, score_entdeckung,
       score_eskalation, score_kosten, gates_passed, detail_json, created_at
FROM ${FQ}.nba_coa_runs
WHERE flow_uuid = '${id}'
ORDER BY rank ASC, score_total DESC
`;
  if (process.env.TRITON_FORCE_SAMPLE_DATA === "true") {
    return { ok: true, source: "sample", rows: nbaCoaRunsSample(id) };
  }
  const result = await runTrino(sql);
  if (result.ok) {
    return result;
  }
  return {
    ok: true,
    source: "sample",
    rows: nbaCoaRunsSample(id),
    fallbackReason: result.error,
  };
}

export async function approveNbaIncident(flowUuid, coaId, approvedBy) {
  const id = escUuid(flowUuid);
  const coa = escCoa(coaId);
  const by = esc(approvedBy || "operator");
  const sql = `
UPDATE ${FQ}.nba_incidents
SET approved_coa_id = '${coa}',
    approved_at = current_timestamp,
    approved_by = '${by}',
    status = 'APPROVED',
    updated_at = current_timestamp
WHERE flow_uuid = '${id}'
`;
  if (process.env.TRITON_FORCE_SAMPLE_DATA === "true") {
    const row = nbaIncidentsSample.find((r) => r.flow_uuid === id);
    if (!row) {
      return { ok: false, error: "Incident not found (sample mode)" };
    }
    row.approved_coa_id = coa;
    row.status = "APPROVED";
    row.approved_by = by;
    return { ok: true, source: "sample", flow_uuid: id, approved_coa_id: coa };
  }
  return runTrino(sql, "execute");
}