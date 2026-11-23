import { useCallback, useEffect, useRef, useState } from "react";
import { fetchJson } from "../api";

type AgentApp = {
  app_id: string;
  title: string;
  subtitle: string;
  starter_prompt: string;
};

type ChatMessage = { role: "user" | "assistant"; content: string };

type OntologyConcept = { id: string; label?: string; iri?: string };

type BusinessRuleRow = {
  rule_id: string;
  title: string;
  source_ids?: string[];
  tables?: string[];
  evidence_template?: string;
};

type TrinoEntity = {
  entity_id: string;
  iri?: string;
  tables?: { table: string; role?: string }[];
};

type KgNode = { id: string; kind: string; label: string; iri?: string; table?: string };
type KgEdge = { from: string; to: string; relation: string };
type KgSqlMeta = {
  id: string;
  label: string;
  entity_id?: string;
  alert_types?: string[];
  description?: string;
};

type SqlRunResult = {
  ok?: boolean;
  query_id?: string;
  label?: string;
  source?: string;
  columns?: string[];
  rows?: Record<string, unknown>[];
  error?: string;
};

type TtydExample = {
  id: string;
  description_de?: string;
  kg_query_id?: string;
  classes?: string[];
};

type TtydE2eResult = {
  ok?: boolean;
  example_id?: string;
  mapping_plan?: { tables?: string[]; views?: string[] };
  kg_compare?: { overlap_count?: number; tables_in_both?: string[] };
  synthesis?: { summary_de?: string };
  sql?: SqlRunResult & { row_count?: number };
};

export function AgentConsolePage() {
  const [apps, setApps] = useState<AgentApp[]>([]);
  const [appId, setAppId] = useState("lage-analyst");
  const [concepts, setConcepts] = useState<OntologyConcept[]>([]);
  const [namespace, setNamespace] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [engine, setEngine] = useState<string>("");
  const [error, setError] = useState("");
  const [businessRules, setBusinessRules] = useState<BusinessRuleRow[]>([]);
  const [trinoEntities, setTrinoEntities] = useState<TrinoEntity[]>([]);
  const [rulesNote, setRulesNote] = useState("");
  const [kgNodes, setKgNodes] = useState<KgNode[]>([]);
  const [kgEdges, setKgEdges] = useState<KgEdge[]>([]);
  const [kgSqlQueries, setKgSqlQueries] = useState<KgSqlMeta[]>([]);
  const [sqlBusyId, setSqlBusyId] = useState<string | null>(null);
  const [sqlResult, setSqlResult] = useState<SqlRunResult | null>(null);
  const [ttydExamples, setTtydExamples] = useState<TtydExample[]>([]);
  const [ttydBusyId, setTtydBusyId] = useState<string | null>(null);
  const [ttydResult, setTtydResult] = useState<TtydE2eResult | null>(null);
  const tailRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    void (async () => {
      const appsRes = await fetchJson<{ apps: AgentApp[] }>("/api/agents/apps");
      setApps(appsRes.apps ?? []);
      const onto = await fetchJson<{
        ontology?: { concepts?: OntologyConcept[]; namespace?: string };
      }>("/api/agents/ontology");
      setConcepts(onto.ontology?.concepts ?? []);
      setNamespace(onto.ontology?.namespace ?? "");
      const br = await fetchJson<{
        ok?: boolean;
        business_rules?: BusinessRuleRow[];
        classification_note?: string;
        trino_mapping?: { semantic_entities?: TrinoEntity[] };
      }>("/api/agents/business-rules");
      setBusinessRules(br.business_rules ?? []);
      setRulesNote(br.classification_note ?? "");
      setTrinoEntities(br.trino_mapping?.semantic_entities ?? []);
      const kg = await fetchJson<{ graph?: { nodes?: KgNode[]; edges?: KgEdge[] } }>(
        "/api/agents/kg-graph"
      );
      setKgNodes(kg.graph?.nodes ?? []);
      setKgEdges(kg.graph?.edges ?? []);
      const kq = await fetchJson<{ queries?: KgSqlMeta[] }>("/api/agents/kg-sql");
      setKgSqlQueries(kq.queries ?? []);
      const ttyd = await fetchJson<{ golden_examples?: TtydExample[] }>("/api/agents/ttyd");
      setTtydExamples(ttyd.golden_examples ?? []);
    })();
  }, []);

  async function runTtydE2e(exampleId: string) {
    setTtydBusyId(exampleId);
    setTtydResult(null);
    try {
      const res = await fetchJson<TtydE2eResult>("/api/agents/ttyd/e2e", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ example_id: exampleId }),
      });
      setTtydResult(res);
      if (res.sql) setSqlResult(res.sql);
    } catch (e) {
      setTtydResult({ ok: false, synthesis: { summary_de: String(e) } });
    } finally {
      setTtydBusyId(null);
    }
  }

  async function runKgSql(queryId: string) {
    setSqlBusyId(queryId);
    setSqlResult(null);
    try {
      const res = await fetchJson<SqlRunResult>(
        `/api/agents/kg-sql/${encodeURIComponent(queryId)}/run`,
        { method: "POST" }
      );
      setSqlResult(res);
    } catch (e) {
      setSqlResult({ ok: false, error: String(e) });
    } finally {
      setSqlBusyId(null);
    }
  }

  const conceptNodes = kgNodes.filter((n) => n.kind === "IncidentConcept");

  useEffect(() => {
    tailRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const switchApp = useCallback(
    (id: string) => {
      setAppId(id);
      setMessages([]);
      setEngine("");
      setError("");
      const app = apps.find((a) => a.app_id === id);
      if (app?.starter_prompt) setDraft(app.starter_prompt);
    },
    [apps]
  );

  useEffect(() => {
    if (apps.length && !draft) {
      const app = apps.find((a) => a.app_id === appId);
      if (app) setDraft(app.starter_prompt);
    }
  }, [apps, appId, draft]);

  async function send(text?: string) {
    const content = (text ?? draft).trim();
    if (!content || busy) return;
    const next: ChatMessage[] = [...messages, { role: "user", content }];
    setMessages(next);
    setDraft("");
    setBusy(true);
    setError("");
  try {
      if (appId === "talk-to-data") {
        const res = await fetchJson<{
          ok?: boolean;
          summary_de?: string;
          reply?: string;
          error?: string;
          agents?: { ontology_mapper?: { example_id?: string } };
        }>("/api/agents/ttyd/ask", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ text: content }),
        });
        const body = res.reply || res.summary_de || res.error || "(keine Antwort)";
        setMessages([...next, { role: "assistant", content: body }]);
        setEngine(res.agents?.ontology_mapper?.example_id ? `ttyd:${res.agents.ontology_mapper.example_id}` : "ttyd");
        if (!res.ok) setError(res.error ?? "TTYD ohne Treffer");
      } else {
        const res = await fetchJson<{
          ok: boolean;
          message?: ChatMessage;
          engine?: string;
          error?: string;
        }>("/api/agents/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ app_id: appId, messages: next }),
        });
        if (res.ok && res.message) {
          setMessages([...next, res.message]);
          setEngine(res.engine ?? "");
        } else {
          setError(res.error ?? "Chat failed");
        }
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  const active = apps.find((a) => a.app_id === appId);

  return (
    <div className="agent-console-layout">
      <aside className="panel agent-apps-rail">
        <h2 className="panel-title">Agenten-Apps</h2>
        <p className="text-muted" style={{ fontSize: 12, marginTop: 0 }}>
          CrewAI + Semantica-KG · Trino-Evidence (allowlist).
        </p>
        {apps.map((a) => (
          <button
            key={a.app_id}
            type="button"
            className={`catalog-item ${appId === a.app_id ? "active" : ""}`}
            onClick={() => switchApp(a.app_id)}
          >
            <strong>{a.title}</strong>
            <span className="text-muted" style={{ display: "block", fontSize: 11 }}>
              {a.subtitle}
            </span>
          </button>
        ))}
      </aside>

      <section className="panel agent-chat-main">
        <header className="agent-chat-head">
          <div>
            <h2 className="panel-title" style={{ marginBottom: 4 }}>
              {active?.title ?? "Agent"}
            </h2>
            <span className="text-muted" style={{ fontSize: 11 }}>
              Engine: {engine || "—"} · Ontologie: <span className="mono">{namespace || "…"}</span>
            </span>
          </div>
        </header>
        <div className="agent-chat-thread">
          {messages.length === 0 ? (
            <p className="text-muted">Starter: {active?.starter_prompt}</p>
          ) : (
            messages.map((m, i) => (
              <div key={i} className={`agent-bubble agent-bubble-${m.role}`}>
                <span className="agent-bubble-label">{m.role === "user" ? "Sie" : "Agent"}</span>
                <button
                  type="button"
                  className="plaintext agent-bubble-body"
                  onClick={(e) => {
                    void navigator.clipboard?.writeText((e.currentTarget as HTMLButtonElement).innerText ?? "");
                  }}
                  title="Klicken zum Kopieren"
                >
                  {m.content}
                </button>
              </div>
            ))
          )}
          <div ref={tailRef} />
        </div>
        {error ? <div className="error-box">{error}</div> : null}
        <div className="agent-chat-compose">
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            rows={3}
            placeholder="Frage an den Agenten…"
            disabled={busy}
          />
          <button type="button" className="btn-primary" disabled={busy} onClick={() => void send()}>
            Senden
          </button>
        </div>
      </section>

      <aside className="panel agent-ontology-rail">
        <h2 className="panel-title">NATO Business Rules</h2>
        <p className="text-muted" style={{ fontSize: 10 }}>
          {rulesNote}
        </p>
        <ul className="agent-ontology-list">
          {businessRules.map((r) => (
            <li key={r.rule_id}>
              <button
                type="button"
                className="linkish"
                onClick={() => {
                  setDraft(
                    `Erkläre Regel ${r.rule_id} (${r.title}) und welche Trino-Tabellen ${(r.tables ?? []).join(", ")} als Evidence dienen.`
                  );
                }}
              >
                <span className="mono">{r.rule_id}</span>
              </button>
              <div className="text-muted" style={{ fontSize: 11 }}>
                {r.title}
              </div>
              {r.tables?.length ? (
                <div className="mono" style={{ fontSize: 10 }}>
                  {r.tables.join(" · ")}
                </div>
              ) : null}
            </li>
          ))}
        </ul>
        <h2 className="panel-title" style={{ marginTop: 12 }}>
          Semantica KG
        </h2>
        <p className="text-muted" style={{ fontSize: 10 }}>
          {conceptNodes.length} Konzepte · {kgEdges.length} Kanten (concept/entity → Trino)
        </p>
        <ul className="agent-ontology-list">
          {conceptNodes.slice(0, 8).map((c) => {
            const id = c.id.replace(/^concept:/, "");
            const linked = kgEdges
              .filter((e) => e.from === c.id)
              .map((e) => kgNodes.find((n) => n.id === e.to)?.table)
              .filter(Boolean)
              .slice(0, 3);
            return (
              <li key={c.id}>
                <button
                  type="button"
                  className="linkish"
                  onClick={() => {
                    setDraft(
                      `Semantica-KG: Erkläre Konzept ${id} (${c.label}) und Evidence-Tabellen ${linked.join(", ")}.`
                    );
                    setAppId("semantica-curator");
                  }}
                >
                  <span className="mono">{id}</span>
                </button>
                <div className="text-muted" style={{ fontSize: 10 }}>
                  {linked.length ? linked.join(" · ") : "—"}
                </div>
              </li>
            );
          })}
        </ul>
        <h2 className="panel-title" style={{ marginTop: 12 }}>
          Talk to your Data (E2E)
        </h2>
        <p className="text-muted" style={{ fontSize: 10 }}>
          Ontology → Mapping-Plan ↔ KG-Tabellen → Live-SQL
        </p>
        <ul className="agent-ontology-list">
          {ttydExamples.map((ex) => (
            <li key={ex.id}>
              <button
                type="button"
                className="linkish"
                style={{ fontSize: 11 }}
                disabled={ttydBusyId === ex.id}
                onClick={() => void runTtydE2e(ex.id)}
              >
                {ttydBusyId === ex.id ? "…" : "◎"} {ex.description_de ?? ex.id}
              </button>
            </li>
          ))}
        </ul>
        {ttydResult?.synthesis?.summary_de ? (
          <div className="text-muted" style={{ fontSize: 10, marginTop: 6 }}>
            {ttydResult.synthesis.summary_de}
            {ttydResult.kg_compare?.overlap_count != null
              ? ` · KG overlap: ${ttydResult.kg_compare.overlap_count}`
              : null}
          </div>
        ) : null}
        <h2 className="panel-title" style={{ marginTop: 12 }}>
          Trino Evidence
        </h2>
        <ul className="agent-ontology-list">
          {kgSqlQueries.map((q) => (
            <li key={q.id}>
              <button
                type="button"
                className="linkish"
                style={{ fontSize: 11, padding: "4px 8px" }}
                disabled={sqlBusyId === q.id}
                onClick={() => void runKgSql(q.id)}
              >
                {sqlBusyId === q.id ? "…" : "▶"} {q.label}
              </button>
              <div className="text-muted" style={{ fontSize: 10 }}>
                {q.description}
              </div>
            </li>
          ))}
        </ul>
        {sqlResult ? (
          <div className="panel" style={{ marginTop: 8, padding: 8, fontSize: 11 }}>
            <div>
              <strong>{sqlResult.label ?? sqlResult.query_id}</strong>
              <span className="text-muted"> · {sqlResult.source ?? "…"}</span>
            </div>
            {sqlResult.error ? (
              <div className="error-box">{sqlResult.error}</div>
            ) : (
              <pre className="mono" style={{ maxHeight: 160, overflow: "auto", margin: 0 }}>
                {(sqlResult.rows ?? [])
                  .slice(0, 12)
                  .map((row) => JSON.stringify(row))
                  .join("\n") || "(keine Zeilen)"}
              </pre>
            )}
          </div>
        ) : null}
        <h2 className="panel-title" style={{ marginTop: 12 }}>
          Semantica · Konzepte
        </h2>
        <ul className="agent-ontology-list">
          {concepts.map((c) => (
            <li key={c.id}>
              <button
                type="button"
                className="linkish"
                onClick={() => {
                  setDraft(`Erkläre Konzept ${c.id} (${c.label ?? ""})`);
                }}
              >
                <span className="mono">{c.id}</span>
              </button>
              <div className="text-muted" style={{ fontSize: 11 }}>
                {c.label}
              </div>
            </li>
          ))}
        </ul>
        <h2 className="panel-title" style={{ marginTop: 12 }}>
          Trino-Entitäten
        </h2>
        <ul className="agent-ontology-list">
          {trinoEntities.map((e) => (
            <li key={e.entity_id}>
              <span className="mono">{e.entity_id}</span>
              <div className="text-muted" style={{ fontSize: 10 }}>
                {(e.tables ?? []).map((t) => t.table).join(", ")}
              </div>
            </li>
          ))}
        </ul>
      </aside>
    </div>
  );
}