#!/usr/bin/env bash
# Apply NBA Trino DDL phases 0–6 (run from cdw-analyse with .env.trino.local)
set -euo pipefail
cd "$(dirname "$0")/.."
for sql in \
  sql/nba_schema_trino.sql \
  sql/nba_schema_phase4_hitl.sql \
  sql/nba_schema_phase5_decision.sql \
  sql/nba_schema_phase6_materialize.sql \
  sql/nba_schema_mp0_readiness.sql \
  sql/nba_schema_mp1_mission.sql
do
  echo "Applying $sql …"
  python3 run_trino_ddl.py "$sql"
done
echo "NBA DDL complete."