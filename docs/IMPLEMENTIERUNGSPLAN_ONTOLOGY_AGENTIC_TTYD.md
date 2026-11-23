# Implementierungsplan: Ontology-Driven Agentic AI (Talk to your Data)

Stand: November 2026 · **Umsetzungsstatus:** MVP vollständig in `main` ab Build **0.2.18-ttyd-full**.

## Lieferumfang (implementiert)

| Komponente | Ort |
|------------|-----|
| 16 KDM-Klassen + 10 Golden Examples | `kdm_defense_classes.json` |
| 3-Agent-Pipeline (Mapper, SQL, Synthesizer) | `agent-ttyd-pipeline.js`, `POST /api/agents/ttyd/ask` |
| KG vs Plan Vergleich + E2E | `agent-ttyd.js`, `POST /api/agents/ttyd/e2e` |
| Allowlist SQL (inkl. Gdańsk, Kiel fishing, marine_messages, NBA/MDA) | `agent-kg.js` |
| UI: Agenten-App **Talk to your Data** + E2E-Buttons | `AgentConsole.tsx`, `apps.py` |
| DDL-Mapping-Validator | `cdw-analyse/validate_kdm_mapping.py` |
| E2E Smoke (10 Queries) | `scripts/ttyd-e2e.mjs` / `ttyd-e2e.sh` |

## Nächste Ausbaustufe (Backlog)

- CrewAI drei getrennte Agents mit LLM-Delegation (CML)
- `workflow_data` R2RML/YAML ins Repo spiegeln + CI DDL-Diff
- JC3IEDM/ADatP-3 Serialisierung aus SQL-Ergebnissen
- Materialized „latest AIS per MMSI“ für Sub-Second
- Semantica `record_decision` Hook nach TTYD-Antwort (HITL)

Siehe auch [`AGENT_SEMANTICA_KG.md`](./AGENT_SEMANTICA_KG.md).