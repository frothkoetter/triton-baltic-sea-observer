# Agent Console — CrewAI + Semantica ontology (UI Phase 9)

Conversational agent apps on `next-best-action/data/baltic_ontology.json` (Semantica KG seed) plus **KG graph** and **allowlisted Trino evidence** in the NATO UI `#agents` tab. See [`docs/AGENT_SEMANTICA_KG.md`](../../docs/AGENT_SEMANTICA_KG.md).

## Apps

| `app_id` | Role |
|----------|------|
| `lage-analyst` | Lage / alert concepts → ontology IRIs |
| `coa-advisor` | COA, playbook, OPSEC rulebook refs |
| `semantica-curator` | Semantica MCP, SHACL, `record_decision` |

## Runtime

| `TRITON_AGENT_MODE` | Behaviour |
|---------------------|-----------|
| `auto` (default) | CrewAI if installed, else LLM (`LAGEBILD_*` / `NBA_*`), else offline templates |
| `crewai` | Force CrewAI path |
| `llm` | Skip CrewAI, use OpenAI-compatible API |
| `offline` | Deterministic ontology-aware replies |

## Bridge (used by `triton-ui` API)

```bash
echo '{"action":"chat","app_id":"lage-analyst","messages":[{"role":"user","content":"ais_pipeline_blackout"}]}' \
  | python3 chat_bridge.py
```

Env: `PYTHONPATH` = `agent-console` + `next-best-action` (set by `server/agent-chat.js`).

## Tests

```bash
cd cai-workbench/agent-console
TRITON_AGENT_MODE=offline python3 -m unittest tests.test_agent_offline -v
```

Optional CrewAI: only in an **isolated venv** on CML — see [`docs/CML_CREWAI_INSTALL.md`](../../docs/CML_CREWAI_INSTALL.md).  
On CML prefer `TRITON_AGENT_MODE=llm` with `LAGEBILD_*` / `NBA_*` keys (no CrewAI install).