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
    })();
  }, []);

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
          CrewAI + Semantica-Ontologie — kein SQL-Workspace.
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
        <h2 className="panel-title">Semantica · KG-Seed</h2>
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
      </aside>
    </div>
  );
}