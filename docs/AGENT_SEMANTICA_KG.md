# Agent console — Semantica knowledge graph & Trino evidence

Roadmap (multi-agent „Talk to your Data“): [`IMPLEMENTIERUNGSPLAN_ONTOLOGY_AGENTIC_TTYD.md`](./IMPLEMENTIERUNGSPLAN_ONTOLOGY_AGENTIC_TTYD.md).

NATO UI tab **#agents** (Agenten) binds conversational agents to a **lightweight KG view** derived from:

| Source | Role |
|--------|------|
| `cai-workbench/next-best-action/data/baltic_ontology.json` | Incident concepts + rulebook IRIs (Semantica **KG seed**) |
| `baltic_trino_mapping.json` | Semantic entities → `iceberg.defense.*` tables |
| `alert_type_bindings` | Concept → primary evidence tables |
| `nba-semantica-bridge/1` (`semantica_record.json`) | MCP `record_decision` + Iceberg snapshot hints (NBA Phase 8) |

## API (triton-ui)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/agents/kg-graph` | Nodes/edges: `IncidentConcept`, `SemanticEntity`, `TrinoTable`, `RulebookRef` |
| GET | `/api/agents/kg-sql` | Allowlisted evidence queries (metadata only) |
| POST | `/api/agents/kg-sql/:queryId/run` | Execute one curated **SELECT** via Trino bridge |

Same read-only rules as `/api/query` (`sql-validator.js`): no DDL/DML.

### Query IDs

| `queryId` | Use case |
|-----------|----------|
| `kg_ships_registry_gap` | AIS MMSI without `ships` row (MaritimeUnit) |
| `kg_das_threat_gt` / `kg_das_threat_ml` | DAS ground truth vs ML column |
| `kg_das_threat_top1` | NBA DAS cable threat (24h window) |
| `kg_outliers_by_type` | `vessel_outliers` distribution |
| `kg_lagebild_sources` | `lagebild.Data_Source` |
| `kg_nba_open_incidents` | Recent `nba_incidents` |
| `kg_socmint_trust` | SOCMINT `trust_tag` counts |

SQL templates are duplicated for operators in [`cdw-analyse/agent_kg_evidence_trino.sql`](../cdw-analyse/agent_kg_evidence_trino.sql) (CML / Trino CLI).

## CML — run evidence SQL manually

```bash
cd ~/cdw-analyse
git pull origin main
[[ -f .env.trino.local ]] && set -a && source .env.trino.local && set +a
export TRINO_SCHEMA=defense TRINO_CONNECT_SCHEMA=defense

python3 run_trino_ddl.py --env-check 2>/dev/null || python3 - <<'PY'
from run_trino_ddl import connect
conn = connect(schema="defense")
cur = conn.cursor()
cur.execute("""
  SELECT threat_class AS label, COUNT(*) AS n
  FROM iceberg.defense.das_cable_spectrogram_metadata
  GROUP BY 1 ORDER BY n DESC LIMIT 10
""")
for row in cur.fetchall():
    print(row)
conn.close()
PY
```

## Semantica MCP (off UI)

After NBA run: `artifacts/nba/<flow_uuid>/semantica_record.json` — use fields in Semantica `record_decision`. See [`cai-workbench/next-best-action/docs/CML_PHASE8_RUNBOOK.md`](../cai-workbench/next-best-action/docs/CML_PHASE8_RUNBOOK.md).

## UI behaviour

- **Semantica-Kurator** agent: SHACL + MCP bridge narrative.
- Side panel **KG-Graph**: concept ↔ table edges.
- **Trino Evidence**: one-click runs allowlisted queries; results table below chat.

Agents do **not** emit arbitrary SQL; operators use KG-SQL buttons or CML CLI.
)