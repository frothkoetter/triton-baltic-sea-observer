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

**NBA use:** with `NBA_USE_ICEBERG_MCP=1`, the pipeline calls the same Python tools as the MCP server (`list_snapshots`, optional `get_table_health`) via `src/mcp_snapshot_bundle.py`; on import failure it falls back to JDBC `$snapshots` SQL. Materialized tables (`nba_mat_*`) stay CTAS in the pipeline.

| Runtime | Python | Install | Snapshots |
|---------|--------|---------|-----------|
| Cursor MCP (`uvx` server) | 3.13+ (tool env) | `requirements-mcp.txt` or uvx | MCP tools in IDE |
| CML / CDSW session | usually 3.10 | `requirements.txt` only | JDBC fallback + `mcp_audit.json` |

Always run the job from `cai-workbench/next-best-action` (or `cml_job` / `entry.py` from CML), not `$HOME`.

## Semantica MCP

- Docs: [MCP Server](https://docs.getsemantica.ai/reference/mcp_server/), [SHACL validation](https://docs.getsemantica.ai/guides/shacl-validation/)
- Command: `semantica-mcp` (after `pip install semantica`)

**NBA use:**

| Semantica capability | Phase 8 hook |
|---------------------|--------------|
| `record_decision` / `query_decisions` | Mirror HITL approve + `decision_record.json` into KG |
| `extract_entities` / `add_entity` | Enrich `baltic_ontology.json` from incident payloads |
| SHACL (`run_shacl_validation` in Python job) | Same shapes as `data/decision_record.ttl`; CML uses `pyshacl` via `NBA_SHACL_GATE=1` before Trino persist |
| `export_graph` | Publish Baltic maritime ontology alongside PROV bundle |

`SEMANTICA_KG_PATH` can point at the repo ontology JSON until a full KG is exported to RDF.

## Verify in Cursor

1. Settings → MCP → confirm both servers show **connected**.
2. In NBA chat, ask the agent to list latest Iceberg snapshot ids for `ships` and `ais_events_ice` in `iceberg.defense`.
3. After an incident run, open `artifacts/nba/<flow_uuid>/semantica_record.json` (or HITL **Phase 8 — Semantica MCP bridge**) and call `record_decision` with the summarized fields.

CML operators: [`CML_PHASE8_RUNBOOK.md`](CML_PHASE8_RUNBOOK.md).