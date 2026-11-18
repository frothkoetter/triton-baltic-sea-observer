# NBA Phase 8.4 — CDP (CrewAI + MCP)

Optional enterprise hooks. **CML default:** JSON state + manifest only (no extra pip).

## CrewAI `@persist` bridge

| Env / flag | Effect |
|------------|--------|
| `NBA_CREWAI_PERSIST=1` or `--crewai-persist` | If `crewai` is installed → `persist_backend=crewai` + bridge step |
| (off) | `agent_orchestrator` + `incident_state.json` (`persist_backend=nba_json`) |

```bash
pip install -r requirements-crewai.txt   # optional workstation only
export NBA_CREWAI_PERSIST=1
python3 run_nba.py --phase 7 --demo-greifswald --skip-llm --crewai-persist
```

Artifacts: `crewai_persist_manifest.json`, `state_history/state_*.json`.

## Ranger / Data Marketplace MCP (Cursor)

Copy commented blocks from [`.cursor/mcp.json.example`](../../../.cursor/mcp.json.example) into `.cursor/mcp.json` when CDP endpoints exist.

Per incident: `access_context.json` records `NBA_DATA_CLASS` (`defense`, `osint`, `national`) and MCP hook metadata for agents.

## Operators

```bash
export NBA_DATA_CLASS=defense   # or osint | national (policy label)
python3 run_nba.py --phase 7 ...
python3 -m json.tool artifacts/nba/<uuid>/access_context.json
```