#!/usr/bin/env bash
# Local integration test bundle for DAS CDW / NBA / UI (no live Trino required).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== NiFi DAS engine =="
PYTHONPATH=nifi-processors/upload:nifi-processors/tests \
  python3 -m unittest nifi-processors/tests/test_das_cable_acoustic.py -q

echo "== CDW static DDL =="
python3 -m unittest cdw-analyse/tests/test_das_cdw_ddl.py -v

echo "== NBA (alert + mapping) =="
(cd cai-workbench/next-best-action && PYTHONPATH=. python3 -m unittest \
  tests.test_alert_detection tests.test_business_rules_mapping -q)

echo "== Lagebild view DDL =="
(cd cai-workbench/situation-awareness && PYTHONPATH=. python3 -m unittest \
  tests.test_lagebild_all_events_view.TestLagebildAllEventsDdl -q)

echo "== DAS classifier NiFi→ML =="
python3 cai-workbench/das-cable-classifier/scripts/validate_nifi_ml.py

echo "== Triton UI build =="
(cd triton-ui && npm run build)

echo "== API smoke (sample mode) =="
(cd triton-ui && node -e "
import('./server/impala.js').then(async (m) => {
  const r = await m.getDasCableSegments();
  if (!r.rows?.length) throw new Error('no sample DAS rows');
  console.log('das segments', r.rows.length, 'source', r.source);
});
")

echo "OK: DAS integration checks passed."