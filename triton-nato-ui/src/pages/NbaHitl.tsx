import { useCallback, useEffect, useState } from "react";
import { approveNbaCoa, fetchNbaCoaRuns, fetchNbaIncidents, type NbaIncident, type NbaCoaRun } from "../api";

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

  useEffect(() => {
    void loadIncidents();
  }, [loadIncidents]);

  useEffect(() => {
    if (selected) {
      void loadCoa(selected);
    } else {
      setCoaRuns([]);
    }
  }, [selected, loadCoa]);

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
    setMsg(`Approved COA ${coaId.toUpperCase()}`);
    await loadIncidents();
    await loadCoa(selected);
  }

  const detail = incidents.find((i) => i.flow_uuid === selected);

  return (
    <div className="nba-hitl">
      <header className="panel-head">
        <h1>NBA — Course of Action (HITL)</h1>
        <p className="text-muted">
          Data: <strong>{source}</strong> · Trino <code>nba_incidents</code> / <code>nba_coa_runs</code>
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

              <h3>COA matrix</h3>
              <table className="data-table compact">
                <thead>
                  <tr>
                    <th>COA</th>
                    <th>Score</th>
                    <th>Gates</th>
                    <th>Rank</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {coaRuns.map((run) => (
                    <tr key={run.coa_id}>
                      <td>
                        <strong>{run.coa_id}</strong>
                        <div className="text-muted" style={{ fontSize: 11 }}>
                          {COA_LABELS[run.coa_id] ?? run.coa_id}
                        </div>
                      </td>
                      <td>{run.score_total?.toFixed(3) ?? "—"}</td>
                      <td>{run.gates_passed ? "PASS" : "FAIL"}</td>
                      <td>{run.rank ?? "—"}</td>
                      <td>
                        <button
                          type="button"
                          className="btn-approve"
                          disabled={busy || !run.gates_passed}
                          onClick={() => void onApprove(run.coa_id)}
                        >
                          Approve
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </section>
      </div>
    </div>
  );
}