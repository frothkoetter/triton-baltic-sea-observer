# Triton NATO UI (MVP)

React + Vite front end with an Express API and Python Impala bridge for the **defense** schema (Baltic Sea Observer).

## Quick start

```bash
cd triton-nato-ui
cp .env.example .env.local
# Edit .env.local — set IMPALA_USER and IMPALA_PASSWORD (never commit this file)
npm install
npm run dev
```

Open http://localhost:5173 (API on http://127.0.0.1:3001).

### Cloudera AI Workbench Application (official pattern)

Per [Cloudera AI Applications](https://docs.cloudera.com/machine-learning/cloud/applications/topics/ml-applications-c.html):

1. Project → **Applications** → **New Application**
2. **Script:** `triton-nato-ui/entry.py` (Python host; starts Node on **`CDSW_APP_PORT`**)
3. **Kernel:** `python3` (or ML Runtime Python editor/kernel)
4. **Subdomain:** e.g. `triton-nato-ui` → URL `https://<subdomain>.<domain>/`
5. **Environment variables** (application overrides project):

   | Name | Example |
   |------|---------|
   | `TRINO_USER` | LDAP user |
   | `TRINO_PASSWORD` | secret |
   | `TRINO_CATALOG` | `iceberg` |
   | `TRINO_SCHEMA` | `defense` |
   | `LAGEBILD_DATA_ENGINE` | `auto` (Lagebild AIS uses **Trino** when `TRINO_*` set) |
   | `TRITON_FORCE_SAMPLE_DATA` | `false` |
   | `CDSW_APP_POLLING_ENDPOINT` | `/healthz` (so status leaves **Starting**; same idea as [Flask sample](https://docs.cloudera.com/machine-learning/cloud/projects/topics/ml-projects-embedded-web-applications-example-flask-application.html)) |
   | `MAPBOX_ACCESS_TOKEN` | Mapbox `pk.…` (maps; **do not commit**) |
   | `S3_WORKLOAD_USER` | CDP **workload access key** (S3 read for `hadoop_file_url`) |
   | `S3_WORKLOAD_PASSWORD` | Workload **secret** |
   | `S3_ENDPOINT_URL` | Optional S3 API endpoint |
   | `S3_LIDAR_ALLOWED_PREFIX` | Optional allowlist, e.g. `s3a://goes-se-sandbox/...` |
   | `SITREP_S3_PREFIX` | Same as `LAGEBILD_S3_PREFIX` — hourly SITREP PDF + `index.json` |
   | Optional `IMPALA_*` | Lagebild tabs |

6. **Create Application** → wait for **Running** → open URL → **NBA HITL** (`#nba`)

**`entry.py`** runs `cdsw-build.sh` once if `dist/` is missing (installs Node via nvm), then starts `server/index.js` on **`CDSW_APP_PORT`** at **`127.0.0.1`** (Cloudera proxy pattern; see [Analytical Applications](https://docs.cloudera.com/machine-learning/cloud/applications/topics/ml-applications-c.html)).

**Session test** (before deploy): in a session with port forwarding, run:

```bash
cd /home/cdsw/triton-nato-ui
export CDSW_APP_PORT=8091
python3 entry.py
```

Do not set a fixed port in production — Cloudera injects `CDSW_APP_PORT`.

**Manual dev (non-CML):** `npm run dev` as above.

## Environment

See `.env.example`. Credentials belong only in `.env.local`.

| Variable | Purpose |
|----------|---------|
| `IMPALA_HOST` | CDW Impala coordinator hostname |
| `IMPALA_PORT` | Usually `443` for HTTPS transport |
| `IMPALA_DATABASE` | `defense` |
| `IMPALA_USER` / `IMPALA_PASSWORD` | LDAP or workload credentials |
| `IMPALA_AUTH_MECH` | `LDAP` (typical for AuthMech=12 workloads) |
| `IMPALA_HTTP_PATH` | `cliservice` |
| `TRITON_FORCE_SAMPLE_DATA` | `true` to skip Impala and use embedded demo data |
| `PROXIMITY_ALERT_API_KEY` | SSB MVE key for Schiffsnähe (`GET /api/proximity-alerts`) |

## Impala notes

JDBC reference used in CDV:

`jdbc:impala://<host>:443/default;AuthMech=12;transportMode=http;httpPath=cliservice;ssl=1;auth=browser`

The MVP maps this to `impyla` with `use_http_transport=true`. Browser OAuth cannot run inside the API; use username/password in `.env.local` when your CDW tenant allows LDAP/PLAIN for programmatic access.

If Impala is unreachable (503, VPN, or auth failure), map and analysis panels automatically use **sample data**; the status bar shows `SAMPLE DATA`.

## NBA HITL (Phase 4)

Tab **NBA HITL** (`#nba`): lists `nba_incidents`, COA matrix from `nba_coa_runs`, operator **Approve** writes `approved_coa_id` to Trino.

Requires `TRINO_USER` / `TRINO_PASSWORD` in `.env.local` (see `.env.example`). Apply DDL:

```bash
cd cdw-analyse
python3 run_trino_ddl.py sql/nba_schema_phase4_hitl.sql
```

Without Trino, the panel uses embedded sample incidents.

## SQL workspace

Read-only: `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN` only. DDL/DML is rejected server-side.

## Production

```bash
npm run build
NODE_ENV=production npm start
```

Serves the built SPA from `dist/` on `API_PORT`.
