# Cloudera AI Application — Failure / Logs

## Required setup (once per project storage)

Applications use a **fresh engine**; session `~/.nvm` is often **not** visible.

In a **Session** (same project):

```bash
cd "${CDSW_WORKING_DIRECTORY:-/home/cdsw}/triton-nato-ui"
bash cdsw-build.sh
ls -la dist/index.html node_modules/express
```

This writes `.nvm`, `node_modules/`, and `dist/` **into the project tree** (persists for the Application).

Then create/restart the Application.

## Application wizard

| Field | Value |
|-------|--------|
| Script | `triton-nato-ui/entry.py` |
| Kernel | `python3` |

Do **not** use `bash cdsw-app-start.sh` as the Application script.

## If status stays **Starting** (logs show Node listening)

Cloudera health-checks the process bound to **CDSW_APP_PORT**. Do **not** set a fixed `API_PORT=8091` in Application env if it overrides the platform port.

1. Remove `API_PORT` from Application environment (only use platform `CDSW_APP_PORT`).
2. `git pull` and **Restart** (uses `execv` so Node is the main listener, not a Python child).
3. Confirm logs end with `Triton API listening on http://0.0.0.0:<port>`.
4. Open the **Application subdomain URL** (not Session port forward only).

Probe manually from a session on the same cluster (if allowed):

```bash
curl -sS "http://127.0.0.1:${CDSW_APP_PORT}/healthz"
```

If status = Failure

Open **Application logs** and check:

| Log line | Action |
|----------|--------|
| `CDSW_APP_PORT=''` | Platform misconfig; recreate app |
| `cdsw-build.sh failed` | Run build manually in session; fix network/curl |
| `node not found` | Run `bash cdsw-build.sh` in session |
| `dist/ missing` | Same |
| `Cannot find module` | Run `bash cdsw-build.sh` |
| Python `trino` errors on API only | OK at startup; set `TRINO_*` env |

Paste the **last 30 lines** of Application logs for debugging.

## Env vars (Application settings)

`TRINO_USER`, `TRINO_PASSWORD`, `TRINO_CATALOG=iceberg`, `TRINO_SCHEMA=defense`