import type { NbaDecisionRecord, NbaRuleCitation } from "../lib/nba-decision";
import { NbaRuleCitationButton } from "./NbaRuleCitation";

type HitlState = {
  status?: string;
  approved_coa_id?: string | null;
  approved_at?: string | null;
  approved_by?: string | null;
};

type Props = {
  record: NbaDecisionRecord | null;
  recordSource?: string;
  hitl?: HitlState | null;
};

export function NbaDecisionRecorder({ record, recordSource, hitl }: Props) {
  if (!record?.sections) {
    return <p className="text-muted">Decision record not loaded.</p>;
  }

  const s = record.sections;
  const grounding = s["4_rules_and_ontology"]?.grounding;
  const citations = grounding?.rule_citations ?? [];

  return (
    <div className="nba-decision-recorder">
      <header className="nba-decision-head">
        <h3>Decision recorder (9 sections)</h3>
        <span className="text-muted" style={{ fontSize: 11 }}>
          Source: {recordSource ?? "—"} · {record.generated_at ?? ""}
        </span>
      </header>

      <section className="nba-decision-section">
        <h4>1 · Lage</h4>
        <p>
          <strong>{String(s["1_what_happened"]?.theatre ?? "—")}</strong> ·{" "}
          <span className="mono">{String(s["1_what_happened"]?.alert_type ?? "")}</span> (
          {String(s["1_what_happened"]?.priority ?? "")})
        </p>
        {grounding?.incident_concept_label && (
          <p className="text-muted" style={{ fontSize: 12 }}>
            Ontology: {grounding.incident_concept} — {grounding.incident_concept_label}
          </p>
        )}
      </section>

      <section className="nba-decision-section">
        <h4>2 · Data pin</h4>
        <ul className="nba-pin-list mono" style={{ fontSize: 11 }}>
          {(s["2_data_pin"]?.snapshots as { table_name?: string; snapshot_id?: number }[] | undefined)?.map(
            (snap) => (
              <li key={snap.table_name}>
                {snap.table_name} @ snapshot {snap.snapshot_id}
              </li>
            )
          )}
        </ul>
        {(s["2_data_pin"]?.materialized_tables as { mat_table?: string; row_limit?: number }[] | undefined)
          ?.length ? (
          <>
            <p className="text-muted" style={{ fontSize: 11, margin: "8px 0 4px" }}>
              Materialized (Phase 6):
            </p>
            <ul className="nba-pin-list mono" style={{ fontSize: 11 }}>
              {(
                s["2_data_pin"]?.materialized_tables as {
                  mat_table?: string;
                  source_table?: string;
                  row_limit?: number;
                }[]
              ).map((m) => (
                <li key={m.mat_table}>
                  <code>{m.mat_table}</code>
                  {m.row_limit != null ? ` (limit ${m.row_limit})` : ""}
                </li>
              ))}
            </ul>
          </>
        ) : null}
      </section>

      <section className="nba-decision-section">
        <h4>3 · Ranked COAs</h4>
        <p className="text-muted" style={{ fontSize: 12 }}>
          Best {s["3_ranked_coas"]?.best_coa_id ?? "—"} · Playbook{" "}
          {s["3_ranked_coas"]?.playbook_best_coa_id ?? "—"}
        </p>
      </section>

      <section className="nba-decision-section">
        <h4>4 · Rules &amp; ontology</h4>
        <div className="nba-rule-chips">
          {citations.map((c: NbaRuleCitation) => (
            <NbaRuleCitationButton key={c.rule_id} ruleId={c.rule_id} fallback={c} />
          ))}
          {!citations.length && <span className="text-muted">No playbook rules fired.</span>}
        </div>
      </section>

      <section className="nba-decision-section">
        <h4>5 · Simulation</h4>
        <table className="data-table compact">
          <thead>
            <tr>
              <th>COA</th>
              <th>Discovery @ T+6</th>
            </tr>
          </thead>
          <tbody>
            {(s["5_sim_outcomes"]?.simulations ?? []).map((sim) => (
              <tr key={sim.coa_id}>
                <td>{sim.coa_id}</td>
                <td>{sim.final_discovery?.toFixed(3) ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="nba-decision-section">
        <h4>6 · Narrative</h4>
        <p className="nba-narrative" style={{ whiteSpace: "pre-wrap", fontSize: 13 }}>
          {s["6_narrative"]?.text ?? "—"}
        </p>
        {s["6_narrative"]?.llm_used && (
          <span className="text-muted" style={{ fontSize: 11 }}>
            LLM narrative
          </span>
        )}
      </section>

      <section className="nba-decision-section">
        <h4>7 · HITL approval</h4>
        {hitl?.approved_coa_id ? (
          <dl className="nba-meta" style={{ marginBottom: 0 }}>
            <dt>COA</dt>
            <dd>
              <strong>{hitl.approved_coa_id}</strong>
            </dd>
            <dt>By</dt>
            <dd>{hitl.approved_by ?? "—"}</dd>
            <dt>At</dt>
            <dd className="mono">{hitl.approved_at ?? "—"}</dd>
            <dt>Status</dt>
            <dd>{hitl.status ?? "APPROVED"}</dd>
          </dl>
        ) : (
          <p className="text-muted" style={{ fontSize: 12 }}>
            {String(s["7_hitl_approval"]?.status_hint ?? "Awaiting operator approval in nba_incidents.")}
          </p>
        )}
      </section>

      <section className="nba-decision-section">
        <h4>8 · Scenario matrix</h4>
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Scenario</th>
              <th>Discovery</th>
            </tr>
          </thead>
          <tbody>
            {(s["8_scenario_matrix"]?.runs ?? []).map((run) => (
              <tr key={run.scenario_id}>
                <td>{run.label}</td>
                <td>{run.final_discovery.toFixed(3)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="nba-decision-section">
        <h4>9 · Agentic Red vs Blue</h4>
        {s["9_agentic_gameplay"]?.replay ? (
          <>
            <p className="text-muted" style={{ fontSize: 12 }}>
              COA {s["9_agentic_gameplay"].replay.coa_id} · Red doctrine{" "}
              {s["9_agentic_gameplay"].replay.red_doctrine} · Blue reward{" "}
              {s["9_agentic_gameplay"].replay.blue_reward?.toFixed(3)} · Red reward{" "}
              {s["9_agentic_gameplay"].replay.red_reward?.toFixed(3)}
            </p>
            <table className="data-table compact">
              <thead>
                <tr>
                  <th>T</th>
                  <th>Blue</th>
                  <th>Red</th>
                  <th>P(det)</th>
                </tr>
              </thead>
              <tbody>
                {(s["9_agentic_gameplay"].replay.ticks ?? []).map((t) => (
                  <tr key={t.tick}>
                    <td>{t.tick}</td>
                    <td className="mono">{t.blue_action}</td>
                    <td className="mono">{t.red_action}</td>
                    <td>{t.discovery_probability.toFixed(3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        ) : (
          <p className="text-muted">No gameplay replay.</p>
        )}
        {(s["9_agentic_gameplay"]?.what_if_red_doctrines as
          | { red_doctrine?: string; blue_reward?: number; red_reward?: number }[]
          | undefined)?.length ? (
          <table className="data-table compact" style={{ marginTop: 8 }}>
            <thead>
              <tr>
                <th>Red doctrine</th>
                <th>Blue reward</th>
                <th>Red reward</th>
              </tr>
            </thead>
            <tbody>
              {(s["9_agentic_gameplay"]?.what_if_red_doctrines as {
                red_doctrine?: string;
                blue_reward?: number;
                red_reward?: number;
              }[]).map((w) => (
                <tr key={w.red_doctrine}>
                  <td>{w.red_doctrine}</td>
                  <td>{w.blue_reward?.toFixed(3) ?? "—"}</td>
                  <td>{w.red_reward?.toFixed(3) ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}
      </section>
    </div>
  );
}