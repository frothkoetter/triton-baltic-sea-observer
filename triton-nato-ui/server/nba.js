import { nbaIncidentsSample, nbaCoaRunsSample } from "./nba-sample.js";
import { resolveNbaCoaRuns } from "./nba-coa.js";
import {
  getTrinoState,
  runTrino,
  TRINO_FQ as FQ,
} from "./trino-client.js";

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

export function getNbaTrinoState() {
  return getTrinoState();
}

const INCIDENTS_SQL = `
SELECT flow_uuid, status, alert_type, priority, theatre, latitude, longitude,
       best_coa_id, approved_coa_id, approved_at, approved_by, sitrep_link,
       decision_record_json, grounding_json, created_at, updated_at
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
  const trinoRows = result.ok ? result.rows ?? [] : [];
  const resolved = await resolveNbaCoaRuns(id, trinoRows);
  if (resolved.rows.length) {
    return {
      ok: true,
      source: resolved.source,
      rows: resolved.rows,
      fallbackReason: result.ok ? undefined : result.error,
    };
  }
  if (process.env.TRITON_FORCE_SAMPLE_DATA === "true") {
    return { ok: true, source: "sample", rows: nbaCoaRunsSample(id) };
  }
  if (!result.ok) {
    return {
      ok: true,
      source: "empty",
      rows: [],
      fallbackReason: result.error,
    };
  }
  return { ok: true, source: "empty", rows: [] };
}

export async function getNbaDecisionRecordFromTrino(flowUuid) {
  const id = escUuid(flowUuid);
  const sql = `
SELECT decision_record_json, grounding_json
FROM ${FQ}.nba_incidents
WHERE flow_uuid = '${id}'
LIMIT 1
`;
  if (process.env.TRITON_FORCE_SAMPLE_DATA === "true") {
    return { ok: false, reason: "sample" };
  }
  const result = await runTrino(sql);
  if (!result.ok || !result.rows?.length) {
    return { ok: false, reason: result.error ?? "empty" };
  }
  const row = result.rows[0];
  const raw = row.decision_record_json;
  if (!raw) {
    return { ok: false, reason: "no_json" };
  }
  try {
    return {
      ok: true,
      source: "trino",
      record: typeof raw === "string" ? JSON.parse(raw) : raw,
    };
  } catch {
    return { ok: false, reason: "invalid_json" };
  }
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