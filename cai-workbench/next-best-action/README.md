# Next Best Action (NBA) — CML module (Phase 0–2)

Simulationsgestützte **Next Best Action** auf dem bestehenden Baltic Sea Observer-Stack (Trino/Iceberg, NiFi-Simulatoren, optional CrewAI).

**Plan:** [`IMPLEMENTIERUNGSPLAN.md`](./IMPLEMENTIERUNGSPLAN.md) · Mission Planning + SAP: [`../../docs/IMPLEMENTIERUNGSPLAN_MISSION_SAP.md`](../../docs/IMPLEMENTIERUNGSPLAN_MISSION_SAP.md) · Mission Einsatz (NiFi/UI): [`../../docs/IMPLEMENTIERUNGSPLAN_MISSION_EINSATZ_SIMULATION.md`](../../docs/IMPLEMENTIERUNGSPLAN_MISSION_EINSATZ_SIMULATION.md) · UC1: [`../situation-awareness/`](../situation-awareness/) · **CML jobs (UC1/UC2):** [`../CML_UC_JOBS_SETUP.md`](../CML_UC_JOBS_SETUP.md)

## Mission Planning + SAP (MP-0 / MP-1)

```bash
export NBA_SAP_READINESS_MODE=sample   # off | sample | odata | trino
python3 run_nba.py --demo-greifswald --phase 1
# audit.json → sap_readiness_audit; decision_record.json → §10_mission_readiness
```

Fixtures: `data/sap_readiness_sample.json`, `data/sap_capability_map.json`. Trino DDL: `cdw-analyse/sql/nba_schema_mp0_readiness.sql`, `nba_schema_mp1_mission.sql`.

## CML Scheduled Job (UC2)

| Field | Value |
|-------|--------|
| Type | **Scheduled Job** — **not** a notebook cell |
| **Python script** | **`cml_job.py`** (nur `.py` im Wizard — kein `.sh`) |
| **Kernel** | Python 3 |
| **Build** | `bash cdsw-build.sh` |
| **CLI** | nicht nötig — setze `NBA_EXIT_IF_QUIET=1`, `NBA_PERSIST_TRINO=1`, `NBA_PHASE=2`, `NBA_DEMO_GREIFSWALD=0` im Job-Env |

**Wichtig:** Logs mit `Cell In[…]`, `ipykernel_launcher.py`, oder `-f …kernel….json` ⇒ Job startet den **Jupyter-Kernel**, nicht ein Batch-Skript. Preferred: **`bash run-from-session.sh`**. Wenn CML nur „Python file“ erlaubt: `python3 entry.py` — nicht Notebook-Zellen. (Ab `main`: Jupyter-`-f`-Args werden ignoriert.)

## Quickstart

CML: `CDSW_WORKING_DIRECTORY` may be empty — use script path or `/home/cdsw`:

```bash
cd /home/cdsw/cai-workbench/next-best-action
bash run-from-session.sh --demo-greifswald --phase 1
```

```bash
cd cai-workbench/next-best-action
./cdsw-build.sh
# Trino (CDW): from repo root
set -a && source ../../cdw-analyse/.env.trino.local && set +a
export TRINO_JDBC_URL TRINO_USER TRINO_PASSWORD

python3 run_nba.py --demo-greifswald --phase 1   # default phase 1: simulation + scoring
python3 run_nba.py --demo-greifswald --phase 0   # snapshot + COA only
python3 run_nba.py --demo-greifswald --persist-trino

# Phase 2 — event-driven CML (no Greifswald unless an alert fires)
python3 run_nba.py --no-demo --exit-if-quiet --persist-trino
```

**Phase 2 env (optional):** `NBA_DEDUPE_HOURS` (default 6), `NBA_PIPELINE_BLACKOUT=1`, `NBA_LAGEBILD_ARTIFACTS_DIR` (UC1 `artifacts/reports`), `NBA_EXIT_IF_QUIET=1` for scheduled idle exit 0.

Alert SQL: `sql/alerts/pipeline_blackout_top1.sql`, `sql/alerts/lagebild_nba_candidates.sql` (requires `ais_daten`, `ostsee_pipelines_segmented` for pipeline path).

## Phase 3 — Playbook + LLM narrative

```bash
python3 run_nba.py --demo-greifswald --phase 3 --skip-llm
# Live model (reuse Lagebild Azure/Mistral env):
export LAGEBILD_LLM_BASE_URL LAGEBILD_API_KEY LAGEBILD_MODEL_ID
python3 run_nba.py --no-demo --phase 3 --exit-if-quiet
```

Outputs: `incident_state.json`, `agent_narrative.md`, `state_history/`. Dedupe für `gps_jammer` nutzt `lagebild_id` (Geohash) in Trino.

## Phase 4 — HITL UI

DDL (einmalig auf bestehenden Tabellen): `cdw-analyse/sql/nba_schema_phase4_hitl.sql`

Operator-Freigabe: [triton-ui](../../triton-ui/) → Tab **NBA HITL** → Approve schreibt `approved_coa_id`. Neue Incidents setzen `sitrep_link` für FLASH / SITREP-ESCALATED.

## Phase 5 — Decision record → Trino

DDL: `cdw-analyse/sql/nba_schema_phase5_decision.sql`

Mit `python3 run_nba.py --phase 3 --persist-trino` landen `decision_record_json` und `grounding_json` auf `nba_incidents`. Die NATO UI lädt die Decision Card zuerst aus Trino, sonst aus `artifacts/nba/<uuid>/`.

## Phase 6 — Materialized facts

DDL: `cdw-analyse/sql/nba_schema_phase6_materialize.sql`

```bash
export NBA_MATERIALIZE=1
export NBA_MAT_ROW_LIMIT=25000
python3 run_nba.py --phase 3 --persist-trino
```

Erzeugt `nba_mat_<flow>_<table>` Iceberg-Kopien und Einträge in `nba_materialized_tables`. Decision record §2 listet Mat-Tabellen; §9 enthält Red-doctrine What-if-Vergleiche.

## Phase 7 — Governance / PROV

```bash
python3 run_nba.py --demo-greifswald --phase 7 --skip-llm
```

Schreibt `governance_bundle.json` (Artifact-Manifest mit SHA-256, PROV-O JSON-LD) und erweitert `agent_steps` um SimAnalyst + GovSeal.

## Phase 8 — MCP (Cursor)

MCP-Beispiel: [`.cursor/mcp.json.example`](../../.cursor/mcp.json.example) · Anleitung [`docs/MCP_CURSOR_SETUP.md`](docs/MCP_CURSOR_SETUP.md) (Iceberg-Trino-MCP + Semantica; Schema **`defense`**, keine Passwörter im Git).

**Phase 8a (CML / Pipeline):** `NBA_USE_ICEBERG_MCP=1` nutzt `iceberg_mcp_server_trino` wenn installiert (nur **Python ≥ 3.13**, z. B. Cursor/`requirements-mcp.txt`); auf **CML Python 3.10** ohne MCP-Paket → JDBC-Fallback, trotzdem `mcp_audit.json` + `snapshot_capture_source` in `audit.json`.

```bash
cd cai-workbench/next-best-action   # run_nba.py liegt hier, nicht im Home-Verzeichnis
pip install -r requirements.txt
# Falls rdflib/pyshacl scheitern (pyparsing DelimitedList): pip install 'pyparsing>=3.1.0'
# CML: kein requirements-mcp.txt (Py 3.13+ nötig)
export NBA_USE_ICEBERG_MCP=1
export NBA_TRINO_SCHEMA=defense
python3 run_nba.py --demo-greifswald --phase 7 --skip-llm --offline
```

**Phase 8b (SHACL):** Pro Lauf `decision_record.ttl` + `shacl_report.json` (Shapes in `data/decision_record.ttl`). `NBA_SHACL_GATE=1` verhindert Trino-Persist bei `conforms=false`. Semantica MCP kann dieselben Shapes in Cursor nutzen.

**Phase 8.3 (CML):** [`docs/CML_PHASE8_RUNBOOK.md`](docs/CML_PHASE8_RUNBOOK.md) · Artefakt `semantica_record.json` · CLI `--shacl-gate` / `--iceberg-mcp`.

**Phase 8.4 (CDP):** [`docs/CDP_PHASE84.md`](docs/CDP_PHASE84.md) · `crewai_persist_manifest.json` · optional `requirements-crewai.txt` + `--crewai-persist`.

DDL einspielen:

```bash
cd cdw-analyse
python3 run_trino_ddl.py sql/nba_schema_trino.sql
```

Tests:

```bash
cd cai-workbench/next-best-action
python3 -m unittest discover -s tests -p 'test_*.py'
```

## Phase 0 outputs

- `artifacts/nba/<flow_uuid>/audit.json` — Alert, Snapshot-Bündel, COA-Matrix
- Optional Iceberg: `nba_incidents`, `nba_incident_snapshots`, `nba_coa_runs`