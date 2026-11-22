# Cloudera AI Application — Failure / Logs

## Required setup (once per project storage)

Applications use a **fresh engine**; session `~/.nvm` is often **not** visible.

In a **Session** (same project):

```bash
cd "${CDSW_WORKING_DIRECTORY:-/home/cdsw}/triton-ui"
bash cdsw-build.sh
ls -la dist/index.html node_modules/express
```

This writes `.nvm`, `node_modules/`, and `dist/` **into the project tree** (persists for the Application).

Then create/restart the Application.

## Application wizard

| Field | Value |
|-------|--------|
| Script | `triton-ui/entry.py` |
| Kernel | `python3` |

Do **not** use `bash cdsw-app-start.sh` as the Application script.

## Cloudera samples vs triton-ui

| Topic | [Flask example](https://docs.cloudera.com/machine-learning/cloud/projects/topics/ml-projects-embedded-web-applications-example-flask-application.html) / [Shiny](https://docs.cloudera.com/machine-learning/cloud/projects/topics/ml-example--a-shiny-application.html) | This project |
|-------|---------------|--------------|
| Port | `os.getenv('CDSW_APP_PORT')` or `CDSW_READONLY_PORT` — **never hard-code** | `entry.py` → Node reads same vars |
| Bind address | **`127.0.0.1`** in the engine (proxy in front) | Express binds `127.0.0.1` when `CDSW_*_PORT` set |
| Entry script | Python `app.run(...)` on that port | Python `entry.py` starts `server/index.js` |
| Session embed | Shiny uses `CDSW_READONLY_PORT` + grid icon / `public-<session-id>` URL | Use **Applications** for long-running NATO UI |
| Health / **Running** | Optional `CDSW_APP_POLLING_ENDPOINT` (default `/`) + `/healthcheck` | Set **`CDSW_APP_POLLING_ENDPOINT=/healthz`** (or `/healthcheck`) |

Session `curl` to `127.0.0.1:8091` **does not** reach an Application — different engine/pod. Use the **Application subdomain URL**.

## Port mismatch (Node 8091, subdomain on 8100)

In **container/engine** logs, find:

```text
port scope":{"subdomain":"tritonnato","port":8100,...
```

Node must listen on **that** port. If Application logs say `127.0.0.1:8091` but `tritonnato` is **8100**, the public URL never reaches Express → **Starting** / blank UI.

**Fix:**

1. **Application → Environment** and **Project → Environment**: remove `API_PORT`, `CDSW_APP_PORT`, `CDSW_READONLY_PORT` (no manual ports).
2. **Restart** the Application; in startup logs `CDSW_APP_PORT` should match the `port scope` port (e.g. `8100`).
3. If you see `Failed to add app port` / `Duplicate port 0`, recreate the Application with a valid subdomain; do not set port env vars by hand.

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
| `UI bundle is stale` | Normal after `git pull` — wait for `cdsw-build.sh`, or run it in a Session |
| Footer **`Alerts: 2`** (no **Trino live \| Demo**) | Stale `dist/` from before PR #130 — `bash cdsw-build.sh` or restart with `TRITON_FORCE_UI_REBUILD=1` |
| Footer missing **`Build 0.2.0-data-mode`** | Same — old Vite bundle; check `/api/version` |
| `Cannot find module` | Run `bash cdsw-build.sh` |
| Footer **Trino live** / bridge **`error 404`** | Wrong host: use **`triton-trino.…`** (Copy Trino URL), not `cdw-triton.…`. Run `python3 triton-ui/server/trino-bridge.py --env-check`, fix CML Project env, restart Application |
| Python `trino` errors on API only | OK at startup; set `TRINO_*` env |
| LiDAR: `No module named 'laspy'` | Run once in Session: `python3 -m pip install -r requirements-cml-app.txt`; **Restart Application** (`entry.py` installs laspy when trino is present but laspy is not) |
| Startup: `Installing Python bridge deps (laspy)` | Normal on first restart after `git pull` |

Paste the **last 30 lines** of Application logs for debugging.

## LiDAR LAS — Python `laspy`

`requirements-cml-app.txt` includes **laspy**, **numpy**, **boto3**, **trino**. The Application only pip-installs when any of those are missing (older builds only checked **trino**).

**Session (once, persists in project venv/site-packages):**

```bash
cd "${CDSW_WORKING_DIRECTORY:-/home/cdsw}/triton-ui"
python3 -m pip install -r requirements-cml-app.txt
python3 -c "import laspy; print('laspy', laspy.__version__)"
```

Then **Restart** the Application. LiDAR tab should load real points instead of “stylized preview”.

## S3 errors (LiDAR LAS / SITREP PDF)

| Error | Typical cause |
|-------|----------------|
| `InvalidAccessKeyId` | **Not** a CDP object-store access key (often Trino/LDAP password pasted into `S3_WORKLOAD_PASSWORD`) |
| `403` on `HeadObject` | Wrong key, no RAZ/IAM on bucket prefix, or explicit keys override a broken config |

**Recommended (CML):**

1. Project → **Data Connections** → add **S3 Object Store** (same as LiDAR classifier).
2. Application env: **remove** `S3_WORKLOAD_USER` / `S3_WORKLOAD_PASSWORD` unless you have real workload keys.
3. Optional: `S3_PREFER_CML_CONNECTION=1` (default on CML).
4. If you **must** use keys: `S3_USE_EXPLICIT_KEYS=1` + `S3_ACCESS_KEY_ID` + `S3_SECRET_ACCESS_KEY` from CDP **Access Keys** (not LDAP).

**Test** (Application URL):

```text
GET /api/s3/diagnose?prefix=s3a://your-bucket/your-prefix/
```

Expect `"ok": true`, `"mode": "cml"`.

Prefixes must match where NiFi/Lagebild write (`SITREP_S3_PREFIX`, object keys for `hadoop_file_url`).

## Env vars (Application settings)

`TRINO_USER`, `TRINO_PASSWORD`, `TRINO_CATALOG=iceberg`, `TRINO_SCHEMA=defense`