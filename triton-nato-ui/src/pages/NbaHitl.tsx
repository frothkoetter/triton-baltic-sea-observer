import { useCallback, useEffect, useState } from "react";
import {
  approveNbaCoa,
  fetchNbaCoaRuns,
  fetchNbaDecisionRecord,
  fetchNbaIncidents,
  type NbaIncident,
  type NbaCoaRun,
} from "../api";
import { NbaDecisionRecorder } from "../components/NbaDecisionRecorder";
import { NbaRuleCitationButton } from "../components/NbaRuleCitation";
import type { NbaDecisionRecord } from "../lib/nba-decision";
import { parseCoaDetail } from "../lib/nba-decision";

const COA_LABELS: Record<string, string> = {
  A: "Multidomänen-Verbund (SAR + P-8A + K130)",
  B: "Nordsee-Fregatten + optische EO",
  C: "K130 + ALB-LiDAR",
  D: "Passive UAV / Partner",
};

export function NbaHitlPage() {
  const [incidents, setIncidents] = useState<NbaIncident[]>([]);
  const [source, setSource] = useState("…");
  const [selected, setSelected] = useState<string | null>(null);
  const [coaRuns, setCoaRuns] = useState<NbaCoaRun[]>([]);
  const [decision, setDecision] = useState<NbaDecisionRecord | null>(null);
  const [decisionSource, setDecisionSource] = useState<string>("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");

  const loadIncidents = useCallback(async () => {
    const res = await fetchNbaIncidents();
    setIncidents(res.rows ?? []);
    setSource(res.source ?? "unknown");
  }, []);

  const loadCoa = useCallback(async (flowUuid: string) => {
    const res = await fetchNbaCoaRuns(flowUuid);
    setCoaRuns(res.rows ?? []);
  }, []);

  const loadDecision = useCallback(async (flowUuid: string) => {
    const res = await fetchNbaDecisionRecord(flowUuid);
    setDecision(res.record ?? null);
    setDecisionSource(res.source ?? "");
  }, []);

  useEffect(() => {
    void loadIncidents();
  }, [loadIncidents]);

  useEffect(() => {
    if (selected) {
      void loadCoa(selected);
      void loadDecision(selected);
    } else {
      setCoaRuns([]);
      setDecision(null);
    }
  }, [selected, loadCoa, loadDecision]);

  async function onApprove(coaId: string) {
    if (!selected) return;
    setBusy(true);
    setMsg("");
    const res = await approveNbaCoa(selected, coaId);
    setBusy(false);
    if (!res.ok) {
      setMsg(res.error ?? "Approval failed");
      return;
    }
    setMsg(`Approved COA ${coaId.toUpperCase()} (§7 HITL)`);
    await loadIncidents();
    await loadCoa(selected);
  }

  const detail = incidents.find((i) => i.flow_uuid === selected);
  const ruleCitations = decision?.sections?.["4_rules_and_ontology"]?.grounding?.rule_citations ?? [];

  return (
    <div className="nba-hitl">
      <header className="panel-head">
        <h1>NBA — Course of Action (HITL)</h1>
        <p className="text-muted">
          Data: <strong>{source}</strong> · Trino <code>nba_incidents</code> / <code>nba_coa_runs</code> ·
          artifacts <code>decision_record.json</code>
        </p>
      </header>

      {msg && <p className="nba-msg">{msg}</p>}

      <div className="nba-grid">
        <section className="nba-list panel">
          <h2>Incidents</h2>
          <table className="data-table compact">
            <thead>
              <tr>
                <th>Theatre</th>
                <th>Type</th>
                <th>Best</th>
                <th>Approved</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {incidents.map((row) => (
                <tr
                  key={row.flow_uuid}
                  className={selected === row.flow_uuid ? "row-selected" : ""}
                  onClick={() => setSelected(row.flow_uuid)}
                >
                  <td>{row.theatre}</td>
                  <td className="mono">{row.alert_type}</td>
                  <td>{row.best_coa_id ?? "—"}</td>
                  <td>{row.approved_coa_id ?? "—"}</td>
                  <td>{row.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="nba-detail panel">
          {!detail ? (
            <p className="text-muted">Select an incident.</p>
          ) : (
            <>
              <h2>{detail.theatre}</h2>
              <dl className="nba-meta">
                <dt>flow_uuid</dt>
                <dd className="mono">{detail.flow_uuid}</dd>
                <dt>Priority</dt>
                <dd>{detail.priority}</dd>
                <dt>SITREP link</dt>
                <dd>{detail.sitrep_link ?? "—"}</dd>
                <dt>Position</dt>
                <dd>
                  {detail.latitude?.toFixed(4)}, {detail.longitude?.toFixed(4)}
                </dd>
              </dl>

              {ruleCitations.length > 0 && (
                <div className="nba-grounding-bar">
                  <span className="text-muted">Rules:&nbsp;</span>
                  {ruleCitations.map((c) => (
                    <NbaRuleCitationButton key={c.rule_id} ruleId={c.rule_id} fallback={c} />
                  ))}
                </div>
              )}

              <h3>COA matrix (scores · gates · rank)</h3>
              <table className="data-table compact nba-coa-matrix">
                <thead>
                  <tr>
                    <th>COA</th>
                    <th>Total</th>
                    <th>Latenz</th>
                    <th>Entdeck.</th>
                    <th>Esk.</th>
                    <th>Kosten</th>
                    <th>Gates</th>
                    <th>Rank</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {coaRuns.map((run) => {
                    const parsed = parseCoaDetail(run.detail_json);
                    const failReasons = parsed?.gate_fail_reasons ?? [];
                    return (
                      <tr key={run.coa_id} className={!run.gates_passed ? "nba-coa-fail" : ""}>
                        <td>
                          <strong>{run.coa_id}</strong>
                          <div className="text-muted" style={{ fontSize: 11 }}>
                            {COA_LABELS[run.coa_id] ?? parsed?.title ?? run.coa_id}
                          </div>
                        </td>
                        <td>{run.score_total?.toFixed(3) ?? parsed?.score_total?.toFixed(3) ?? "—"}</td>
                        <td>{fmt(run.score_latenz ?? parsed?.score_latenz)}</td>
                        <td>{fmt(run.score_entdeckung ?? parsed?.score_entdeckung)}</td>
                        <td>{fmt(run.score_eskalation ?? parsed?.score_eskalation)}</td>
                        <td>{fmt(run.score_kosten ?? parsed?.score_kosten)}</td>
                        <td title={failReasons.join("; ")}>
                          {run.gates_passed ? "PASS" : "FAIL"}
                          {!run.gates_passed && failReasons[0] && (
                            <div className="text-muted" style={{ fontSize: 10 }}>
                              {failReasons[0]}
                            </div>
                          )}
                        </td>
                        <td>{run.rank ?? "—"}</td>
                        <td>
                          <button
                            type="button"
                            className="btn-approve"
                            disabled={busy || !run.gates_passed}
                            onClick={() => void onApprove(run.coa_id)}
                          >
                            Approve §7
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>

              <NbaDecisionRecorder
                record={decision}
                recordSource={decisionSource}
                hitl={{
                  status: detail.status,
                  approved_coa_id: detail.approved_coa_id,
                  approved_at: detail.approved_at,
                  approved_by: detail.approved_by,
                }}
              />
            </>
          )}
        </section>
      </div>
    </div>
  );
}

function fmt(v: number | null | undefined): string {
  return v == null ? "—" : v.toFixed(3);
}