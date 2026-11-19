# NBA Phase 8 — CML runbook

Operators on **CDSW/CML Python 3.10**. Cursor MCP (Iceberg + Semantica) is optional for interactive work.

## Recommended scheduled job

| Setting | Value |
|---------|--------|
| Working directory | `cai-workbench/next-best-action` |
| Command | `python3 run_nba.py --no-demo --exit-if-quiet --phase 7 --skip-llm --persist-trino --shacl-gate --iceberg-mcp` |

Secrets via project env (not git): `TRINO_USER`, `TRINO_PASSWORD`, `TRINO_JDBC_URL` (see `cdw-analyse/.env.trino.local`).

## Phase 8 flags (CLI → env)

| Flag | Env | CML typical |
|------|-----|-------------|
| `--iceberg-mcp` | `NBA_USE_ICEBERG_MCP=1` | On → JDBC fallback + `mcp_audit.json` |
| `--shacl-gate` | `NBA_SHACL_GATE=1` | On for prod persist |
| `--persist-trino` | `NBA_PERSIST_TRINO=1` | On for HITL UI via Trino |
| `--skip-llm` | `NBA_SKIP_LLM=1` | On for deterministic CML |

## Trino DDL (UC2 / persist / materialize)

Once per cluster (or after reset):

```bash
cd cdw-analyse
set -a && source .env.trino.local && set +a
bash sql/nba_apply_all_ddl.sh
```

Required when `NBA_MATERIALIZE=1` (registry `nba_materialized_tables`) or Phase 5+ (`decision_record_json` columns).

## Dependencies

```bash
cd cai-workbench/next-best-action
pip install 'pyparsing>=3.1.0' -r requirements.txt
```

Do **not** install `requirements-mcp.txt` on Python 3.10.

## Artifacts per incident (`artifacts/nba/<flow_uuid>/`)

| File | Phase |
|------|-------|
| `mcp_audit.json` | 8a snapshot audit |
| `decision_record.ttl`, `shacl_report.json` | 8b |
| `semantica_record.json` | 8.3 MCP bridge |
| `governance_bundle.json` | 7 |

## Verify

```bash
UUID=<flow_uuid>
jq '.compliance,.snapshots' artifacts/nba/$UUID/semantica_record.json
jq '.conforms' artifacts/nba/$UUID/shacl_report.json
```

## Cursor (interactive)

Copy `.cursor/mcp.json.example` → `.cursor/mcp.json`, set Trino secrets, open incident folder. Use Semantica `record_decision` with fields from `semantica_record.json`.