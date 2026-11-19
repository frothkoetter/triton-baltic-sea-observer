import { loadCoaMatrixArtifact, loadDecisionRecord } from "./nba-artifacts.js";
import { runTrino, TRINO_FQ as FQ } from "./trino-client.js";

function escUuid(value) {
  const v = String(value);
  if (!/^[0-9a-f-]{36}$/i.test(v)) {
    throw new Error("Invalid flow_uuid");
  }
  return v;
}

async function loadDecisionFromTrino(flowUuid) {
  const id = escUuid(flowUuid);
  const sql = `
SELECT decision_record_json
FROM ${FQ}.nba_incidents
WHERE flow_uuid = '${id}'
LIMIT 1
`;
  const result = await runTrino(sql);
  if (!result.ok || !result.rows?.length) {
    return null;
  }
  const raw = result.rows[0].decision_record_json;
  if (!raw) return null;
  try {
    return typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch {
    return null;
  }
}

/** Map decision_record §3 or coa_matrix.json → nba_coa_runs row shape */
export function coaRowsFromScores(scores, source) {
  if (!Array.isArray(scores) || !scores.length) {
    return [];
  }
  let passRank = 0;
  return scores.map((s) => {
    const coaId = String(s.coa_id ?? s.coa?.coa_id ?? "?").toUpperCase();
    const gatesPassed = Boolean(s.gates_passed);
    if (gatesPassed) passRank += 1;
    const detail = typeof s === "object" ? JSON.stringify(s) : String(s);
    return {
      coa_id: coaId,
      rank: gatesPassed ? passRank : 0,
      score_total: Number(s.score_total ?? 0),
      score_latenz: Number(s.score_latenz ?? 0),
      score_entdeckung: Number(s.score_entdeckung ?? 0),
      score_eskalation: Number(s.score_eskalation ?? 0),
      score_kosten: Number(s.score_kosten ?? 0),
      gates_passed: gatesPassed,
      detail_json: detail,
      created_at: null,
      _source: source,
    };
  });
}

export function coaRowsFromDecisionRecord(record) {
  const scores =
    record?.sections?.["3_ranked_coas"]?.coa_scores ?? record?.coa_scores ?? [];
  return coaRowsFromScores(scores, "decision_record");
}

export async function resolveNbaCoaRuns(flowUuid, trinoRows) {
  if (trinoRows?.length) {
    return { ok: true, source: "trino", rows: trinoRows };
  }

  const recordTrino = await loadDecisionFromTrino(flowUuid);
  if (recordTrino) {
    const rows = coaRowsFromDecisionRecord(recordTrino);
    if (rows.length) {
      return { ok: true, source: "decision_record_trino", rows };
    }
  }

  const fromArt = loadDecisionRecord(flowUuid, null);
  if (fromArt?.ok && fromArt.record) {
    const rows = coaRowsFromDecisionRecord(fromArt.record);
    if (rows.length) {
      return { ok: true, source: "decision_record_artifacts", rows };
    }
  }

  const matrix = loadCoaMatrixArtifact(flowUuid);
  if (matrix?.ok && matrix.rows?.length) {
    return {
      ok: true,
      source: "coa_matrix_artifacts",
      rows: coaRowsFromScores(matrix.rows, "coa_matrix"),
    };
  }

  return { ok: true, source: "empty", rows: [] };
}