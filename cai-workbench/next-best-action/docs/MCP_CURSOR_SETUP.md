# MCP setup for NBA Phase 8 (Cursor)

Copy [`.cursor/mcp.json.example`](../../../.cursor/mcp.json.example) to **`.cursor/mcp.json`** in the repo root (or merge into your user `~/.cursor/mcp.json`).

**Do not commit real passwords.** Use Cursor **Secrets** or a local untracked `mcp.json`.

## Triton vs example schema

| Variable | NBA / CDW default | Notes |
|----------|-------------------|--------|
| `TRINO_CATALOG` | `iceberg` | Same as `NBA_TRINO_CATALOG` |
| `TRINO_SCHEMA` | **`defense`** | NBA tables: `nba_incidents`, `ships`, `ais_events_ice` — not a demo `airlinedata` schema |

Align host/user with `cdw-analyse/.env.trino.local` (`TRINO_JDBC_URL` / `TRINO_USER`).

## iceberg-mcp-server-trino

- Upstream: [dipankarmazumdar/iceberg-mcp-server-trino](https://github.com/dipankarmazumdar/iceberg-mcp-server-trino)
- Launcher: `uvx --from git+https://github.com/dipankarmazumdar/iceberg-mcp-server-trino@main run-server`

**NBA use:** replace ad-hoc `$snapshots` SQL in `snapshot_bundle.py` with MCP tools (list snapshots, describe table, run read-only query for bundle pin). Materialized tables (`nba_mat_*`) stay CTAS in the pipeline or become MCP-assisted validation queries.

## Semantica MCP

- Docs: [MCP Server](https://docs.getsemantica.ai/reference/mcp_server/), [SHACL validation](https://docs.getsemantica.ai/guides/shacl-validation/)
- Command: `semantica-mcp` (after `pip install semantica`)

**NBA use:**

| Semantica capability | Phase 8 hook |
|---------------------|--------------|
| `record_decision` / `query_decisions` | Mirror HITL approve + `decision_record.json` into KG |
| `extract_entities` / `add_entity` | Enrich `baltic_ontology.json` from incident payloads |
| SHACL (`run_shacl_validation` in Python job) | Gate `governance_bundle.json` before Trino persist |
| `export_graph` | Publish Baltic maritime ontology alongside PROV bundle |

`SEMANTICA_KG_PATH` can point at the repo ontology JSON until a full KG is exported to RDF.

## Verify in Cursor

1. Settings → MCP → confirm both servers show **connected**.
2. In NBA chat, ask the agent to list latest Iceberg snapshot ids for `ships` and `ais_events_ice` in `iceberg.defense`.
3. After an incident run, ask Semantica to `record_decision` with the `flow_uuid` and `playbook_best_coa_id` from `incident_state.json`.