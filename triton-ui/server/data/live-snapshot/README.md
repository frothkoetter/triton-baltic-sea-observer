# Live snapshot for demo mode

When Trino is unavailable, the UI can use **embedded demo data** or a **snapshot captured from live Trino**.

## Capture on CML (with live warehouse)

```bash
cd ~/triton-ui
[[ -f ../cdw-analyse/.env.trino.local ]] && set -a && source ../cdw-analyse/.env.trino.local && set +a
bash scripts/capture-live-snapshot.sh
```

Writes `manifest.json` plus one JSON file per dataset (`aisShips.json`, `fleet.json`, …).

Set footer **Demo** (or **Auto** without Trino). Check `/api/health`:

- `liveSnapshot.available: true` and `rowCounts.aisShips: 300` (etc.) when files are present.
- After re-capture you can **refresh the page** — the server re-reads `manifest.json` when its timestamp changes (no full app restart required in most cases).

API responses use `source: "sample-snapshot"` (KPI line **sample-snapshot**) when overlay is active; otherwise embedded demo (`source: "sample"`, ~8 AIS ships).

Re-run capture after simulator/warehouse updates; commit snapshot files if you want them in git (can be large).

## Without snapshot

Falls back to built-in `sample-data.js` (synthetic Baltic demo).