# Live snapshot for demo mode

When Trino is unavailable, the UI can use **embedded demo data** or a **snapshot captured from live Trino**.

## Capture on CML (with live warehouse)

```bash
cd ~/triton-ui
[[ -f ../cdw-analyse/.env.trino.local ]] && set -a && source ../cdw-analyse/.env.trino.local && set +a
bash scripts/capture-live-snapshot.sh
```

Writes `manifest.json` plus one JSON file per dataset (`aisShips.json`, `fleet.json`, …).

Restart the Application, set footer **Demo** — `/api/health` reports `liveSnapshot.available: true`.

Re-run capture after simulator/warehouse updates; commit snapshot files if you want them in git (can be large).

## Without snapshot

Falls back to built-in `sample-data.js` (synthetic Baltic demo).