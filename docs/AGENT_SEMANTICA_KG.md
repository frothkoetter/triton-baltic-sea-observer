# Agent console — Semantica knowledge graph & Trino evidence

Vollständiger TTYD-Plan: [`IMPLEMENTIERUNGSPLAN_ONTOLOGY_AGENTIC_TTYD.md`](./IMPLEMENTIERUNGSPLAN_ONTOLOGY_AGENTIC_TTYD.md) · Build **0.2.18-ttyd-full**.

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
| GET | `/api/agents/ttyd` | KDM golden examples + TTYD SQL ids |
| POST | `/api/agents/ttyd/resolve` | Natural language → mapping plan (MVP) |
| POST | `/api/agents/ttyd/e2e` | KG compare + live Trino SQL for one golden example |
| POST | `/api/agents/ttyd/ask` | **3-Agent-Pipeline** (Mapper → SQL → Synthesizer) v2 + column validation |
| GET | `/api/agents/ontology-map` | Semantic entities + column registry (ontology → table.columns) |

**UI:** Tab **Agenten** = **Talk to your Data** (single-column Semantica-style chat). Crew-Apps unter „Legacy“.

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
| `ttyd_sanctioned_latest_ais` | Sanctioned + latest AIS |
| `ttyd_gps_jamming_nic` | GPS jamming NIC &lt; 5 |
| `ttyd_area_entry_sample` | `area_violation` enter events |
| `ttyd_lagebild_gdansk` | Lagebild harbour Gdańsk |
| `ttyd_marine_messages_baltic` | tactical messages Baltic |
| `ttyd_fishing_near_kiel` | fishing AIS near Kiel |
| `ttyd_nba_mda_history` | prior NBA incidents (MDA) |

SQL templates are duplicated for operators in [`cdw-analyse/agent_kg_evidence_trino.sql`](../cdw-analyse/agent_kg_evidence_trino.sql) (CML / Trino CLI).

**E2E smoke (CML Session with Trino env):**

```bash
cd ~/triton-ui
bash cdsw-build.sh          # once: installs Node into project (.nvm / .node-runtime)
bash scripts/ttyd-e2e.sh --local
```

CML sessions often have no global `node` — use **`ttyd-e2e.sh`**, not bare `node`.

Data: [`kdm_defense_classes.json`](../cai-workbench/next-best-action/data/kdm_defense_classes.json) (16 KDM classes + golden examples).

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