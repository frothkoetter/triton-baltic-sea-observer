# Next Best Action (NBA) — CML module (Phase 0–2)

Simulationsgestützte **Next Best Action** auf dem bestehenden Baltic Sea Observer-Stack (Trino/Iceberg, NiFi-Simulatoren, optional CrewAI).

**Plan:** [`IMPLEMENTIERUNGSPLAN.md`](./IMPLEMENTIERUNGSPLAN.md) · UC1: [`../situation-awareness/`](../situation-awareness/)

## CML Scheduled Job (UC2)

| Field | Value |
|-------|--------|
| Type | **Scheduled Job** — **not** a notebook cell |
| **Script** | `cai-workbench/next-best-action/run_nba.py` or `entry.py` |
| **Command** | `python3 run_nba.py --no-demo --exit-if-quiet --persist-trino --phase 2` |
| Build | `bash cdsw-build.sh` once per environment |

`Cell In[…]` + `NameError: __file__` ⇒ wrong job type; use script path above.

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

Operator-Freigabe: [triton-nato-ui](../../triton-nato-ui/) → Tab **NBA HITL** → Approve schreibt `approved_coa_id`. Neue Incidents setzen `sitrep_link` für FLASH / SITREP-ESCALATED.

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