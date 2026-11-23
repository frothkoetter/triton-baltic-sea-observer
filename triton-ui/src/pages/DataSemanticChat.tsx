import { useCallback, useEffect, useRef, useState } from "react";
import { fetchJson } from "../api";
import { AgentConsolePage } from "./AgentConsole";

type TtydExample = { id: string; description_de?: string };
type ChatTurn = { role: "user" | "assistant"; content: string; payload?: TtydAskResponse };

type TtydAskResponse = {
  ok?: boolean;
  summary_de?: string;
  ontology_bindings?: { ontology_class: string; table: string; columns: string[] }[];
  sql_validation?: { ok?: boolean; warnings?: string[] };
  kg_compare?: { overlap_count?: number; tables_in_both?: string[] };
  agents?: {
    ontology_mapper?: { kdm_classes?: string[]; kg_query_id?: string; reasoning_de?: string };
    sql_executor?: {
      ok?: boolean;
      sql?: string;
      source?: string;
      row_count?: number;
      columns?: string[];
      rows?: Record<string, unknown>[];
      error?: string;
    };
  };
};

function AssistantCard({ payload }: { payload: TtydAskResponse }) {
  const mapper = payload.agents?.ontology_mapper;
  const sql = payload.agents?.sql_executor;
  return (
    <div className="dsc-steps">
      <p className="dsc-summary">{payload.summary_de}</p>
      <details open className="dsc-step">
        <summary>1 · Ontologie-Mapper</summary>
        <p className="text-muted" style={{ fontSize: 12 }}>
          {mapper?.reasoning_de}
        </p>
        <ul className="dsc-bindings">
          {(payload.ontology_bindings ?? []).map((b) => (
            <li key={`${b.ontology_class}-${b.table}`}>
              <span className="mono">{b.ontology_class}</span> →{" "}
              <span className="mono">defense.{b.table}</span>
              <div className="text-muted" style={{ fontSize: 10 }}>
                {(b.columns ?? []).slice(0, 8).join(", ")}
              </div>
            </li>
          ))}
        </ul>
        <p className="text-muted" style={{ fontSize: 10 }}>
          KG overlap: {payload.kg_compare?.overlap_count ?? 0}
        </p>
      </details>
      <details open className="dsc-step">
        <summary>
          2 · SQL (validiert{" "}
          {payload.sql_validation?.ok ? "✓" : payload.sql_validation?.warnings?.length ? "!" : "—"})
        </summary>
        {!payload.sql_validation?.ok && payload.sql_validation?.warnings?.length ? (
          <ul className="dsc-warn">
            {payload.sql_validation.warnings.map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        ) : null}
        <pre className="mono dsc-sql">{sql?.sql ?? "—"}</pre>
      </details>
      <details open className="dsc-step">
        <summary>
          3 · Live Trino ({sql?.source ?? "—"}, {sql?.row_count ?? 0} Zeilen)
        </summary>
        {sql?.error ? <div className="error-box">{sql.error}</div> : null}
        {sql?.rows?.length ? (
          <div className="dsc-table-wrap">
            <table className="dsc-table">
              <thead>
                <tr>
                  {(sql.columns ?? Object.keys(sql.rows[0])).map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sql.rows.slice(0, 12).map((row, i) => (
                  <tr key={i}>
                    {(sql.columns ?? Object.keys(row)).map((c) => (
                      <td key={c}>{String(row[c] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-muted">Keine Zeilen</p>
        )}
      </details>
    </div>
  );
}

export function DataSemanticChatPage() {
  const [examples, setExamples] = useState<TtydExample[]>([]);
  const [turns, setTurns] = useState<ChatTurn[]>([]);
  const [draft, setDraft] = useState(
    "SanctionedMaritimePlatform mit letzter AISTrackReport-Position und sanction_reason?"
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [showCrew, setShowCrew] = useState(false);
  const [kgCount, setKgCount] = useState({ nodes: 0, edges: 0 });
  const tailRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    void (async () => {
      const ttyd = await fetchJson<{ golden_examples?: TtydExample[] }>("/api/agents/ttyd");
      setExamples(ttyd.golden_examples ?? []);
      const kg = await fetchJson<{ graph?: { nodes?: unknown[]; edges?: unknown[] } }>(
        "/api/agents/kg-graph"
      );
      setKgCount({
        nodes: kg.graph?.nodes?.length ?? 0,
        edges: kg.graph?.edges?.length ?? 0,
      });
    })();
  }, []);

  useEffect(() => {
    tailRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [turns]);

  const ask = useCallback(async (text: string) => {
    const content = text.trim();
    if (!content || busy) return;
    setTurns((t) => [...t, { role: "user", content }]);
    setDraft("");
    setBusy(true);
    setError("");
    try {
      const payload = await fetchJson<TtydAskResponse>("/api/agents/ttyd/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: content }),
      });
      setTurns((t) => [
        ...t,
        {
          role: "assistant",
          content: payload.summary_de ?? "—",
          payload,
        },
      ]);
      if (!payload.ok) setError("Keine vollständige Antwort — Intent anpassen oder Beispiel wählen.");
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }, [busy]);

  if (showCrew) {
    return (
      <div>
        <button type="button" className="linkish dsc-back" onClick={() => setShowCrew(false)}>
          ← Zurück zur Daten-Semantik (Talk to Data)
        </button>
        <AgentConsolePage />
      </div>
    );
  }

  return (
    <div className="data-semantic-chat">
      <header className="dsc-header panel">
        <div>
          <h1 className="panel-title" style={{ marginBottom: 4 }}>
            Talk to your Data
          </h1>
          <p className="text-muted" style={{ fontSize: 12, margin: 0 }}>
            Semantica-Workflow: Ontologie → KG → validiertes SQL → Trino live · Prototyp wie MCP{" "}
            <span className="mono">ontology_mapper → sql_executor → answer_synthesizer</span>
          </p>
        </div>
        <div className="dsc-meta text-muted" style={{ fontSize: 11 }}>
          KG: {kgCount.nodes} Knoten / {kgCount.edges} Kanten
          <button type="button" className="linkish" style={{ marginLeft: 12 }} onClick={() => setShowCrew(true)}>
            Crew-Apps (Legacy)
          </button>
        </div>
      </header>

      <div className="dsc-examples">
        {examples.map((ex) => (
          <button
            key={ex.id}
            type="button"
            className="dsc-chip"
            disabled={busy}
            onClick={() => void ask(ex.description_de ?? ex.id)}
          >
            {ex.description_de ?? ex.id}
          </button>
        ))}
      </div>

      <main className="panel dsc-thread">
        {turns.length === 0 ? (
          <p className="text-muted" style={{ padding: 12 }}>
            Stellen Sie eine Frage in NATO/KDM-Begriffen. Das System mappt auf Tabellen und Spalten
            (`/api/agents/ontology-map`), führt nur freigegebenes SQL aus und zeigt die Trino-Antwort.
          </p>
        ) : (
          turns.map((turn, i) => (
            <div key={i} className={`agent-bubble agent-bubble-${turn.role}`}>
              <span className="agent-bubble-label">{turn.role === "user" ? "Sie" : "Semantica-Pipeline"}</span>
              {turn.role === "user" ? (
                <div className="agent-bubble-body">{turn.content}</div>
              ) : turn.payload ? (
                <AssistantCard payload={turn.payload} />
              ) : (
                <div className="agent-bubble-body">{turn.content}</div>
              )}
            </div>
          ))
        )}
        <div ref={tailRef} />
      </main>

      {error ? <div className="error-box">{error}</div> : null}

      <footer className="dsc-compose panel">
        <textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          rows={2}
          placeholder="z. B. fishing vessels near Kiel, GPS jamming NIC, Gdańsk lagebild…"
          disabled={busy}
          onKeyDown={(e) => {
            if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) void ask(draft);
          }}
        />
        <button type="button" className="btn-primary" disabled={busy} onClick={() => void ask(draft)}>
          {busy ? "…" : "An Trino senden"}
        </button>
      </footer>
    </div>
  );
}