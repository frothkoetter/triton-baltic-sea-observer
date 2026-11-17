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

## Cloudera samples vs triton-nato-ui

| Topic | [Flask example](https://docs.cloudera.com/machine-learning/cloud/projects/topics/ml-projects-embedded-web-applications-example-flask-application.html) / [Shiny](https://docs.cloudera.com/machine-learning/cloud/projects/topics/ml-example--a-shiny-application.html) | This project |
|-------|---------------|--------------|
| Port | `os.getenv('CDSW_APP_PORT')` or `CDSW_READONLY_PORT` — **never hard-code** | `entry.py` → Node reads same vars |
| Bind address | **`127.0.0.1`** in the engine (proxy in front) | Express binds `127.0.0.1` when `CDSW_*_PORT` set |
| Entry script | Python `app.run(...)` on that port | Python `entry.py` starts `server/index.js` |
| Session embed | Shiny uses `CDSW_READONLY_PORT` + grid icon / `public-<session-id>` URL | Use **Applications** for long-running NATO UI |
| Health / **Running** | Optional `CDSW_APP_POLLING_ENDPOINT` (default `/`) + `/healthcheck` | Set **`CDSW_APP_POLLING_ENDPOINT=/healthz`** (or `/healthcheck`) |

Session `curl` to `127.0.0.1:8091` **does not** reach an Application — different engine/pod. Use the **Application subdomain URL**.

## If status stays **Starting** (logs show Node listening)

Cloudera periodically **polls** `CDSW_APP_POLLING_ENDPOINT` (default `/`) on **CDSW_APP_PORT**. Do **not** set a fixed `API_PORT=8091` in Application env.

1. Application env: `CDSW_APP_POLLING_ENDPOINT=/healthz` (matches Flask-style custom probe).
2. Remove manual `API_PORT` — only platform `CDSW_APP_PORT`.
3. `git pull` and **Restart**; logs should end with `Triton API listening on http://127.0.0.1:<port>`.
4. Open the **Application subdomain** (`https://<subdomain>.<domain>/`), not a Session.

Inside the **Application engine** (if you get a shell there), not a random Session:

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