# Next Best Action (NBA) — CML module (Phase 0)

Simulationsgestützte **Next Best Action** auf dem bestehenden Baltic Sea Observer-Stack (Trino/Iceberg, NiFi-Simulatoren, optional CrewAI).

**Plan:** [`IMPLEMENTIERUNGSPLAN.md`](./IMPLEMENTIERUNGSPLAN.md) · UC1: [`../situation-awareness/`](../situation-awareness/)

## Quickstart

```bash
cd cai-workbench/next-best-action
./cdsw-build.sh
# Trino (CDW): from repo root
set -a && source ../../cdw-analyse/.env.trino.local && set +a
export TRINO_JDBC_URL TRINO_USER TRINO_PASSWORD

python3 run_nba.py --demo-greifswald --phase 1   # default phase 1: simulation + scoring
python3 run_nba.py --demo-greifswald --phase 0   # snapshot + COA only
python3 run_nba.py --demo-greifswald --persist-trino
```

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