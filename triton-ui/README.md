# Triton NATO UI (MVP)

React + Vite front end with an Express API and Python Impala bridge for the **defense** schema (Baltic Sea Observer).

## UI overview (data & main views)

Full integration description (Lagebild, Buoys, LiDAR, NBA HITL, APIs, tables):

**[docs/UI_OVERVIEW.md](./docs/UI_OVERVIEW.md)** · API latency: **[docs/API_LATENCY.md](./docs/API_LATENCY.md)** (`node scripts/bench-api-latency.mjs`)

![Main views — Lagebild, Buoys, LiDAR, NBA](./docs/images/ui-main-views-overview.svg)

## Quick start

```bash
cd triton-ui
cp .env.example .env.local
# Edit .env.local — set IMPALA_USER and IMPALA_PASSWORD (never commit this file)
npm install
npm run dev
```

Open http://localhost:5173 (API on http://127.0.0.1:3001).

### Cloudera AI Workbench Application (official pattern)

Per [Cloudera AI Applications](https://docs.cloudera.com/machine-learning/cloud/applications/topics/ml-applications-c.html):

1. Project → **Applications** → **New Application**
2. **Script:** `triton-ui/entry.py` (Python host; starts Node on **`CDSW_APP_PORT`**)
3. **Kernel:** `python3` (or ML Runtime Python editor/kernel)
4. **Subdomain:** e.g. `triton-ui` → URL `https://<subdomain>.<domain>/`
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

**`entry.py`** runs `cdsw-build.sh` once if `dist/` is missing (installs Node via nvm), pip-installs **`requirements-cml-app.txt`** (trino, boto3, **laspy**, numpy for LiDAR/S3 bridges), then starts `server/index.js` on **`CDSW_APP_PORT`** at **`127.0.0.1`** (Cloudera proxy pattern; see [Analytical Applications](https://docs.cloudera.com/machine-learning/cloud/applications/topics/ml-applications-c.html)).

**Session test** (before deploy): in a session with port forwarding, run:

```bash
cd /home/cdsw/triton-ui
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
| `TRITON_FORCE_SAMPLE_DATA` | `true` to skip Impala/Trino and use embedded demo data |
| *(autonomous demo)* | Leave `TRINO_*` / `IMPALA_*` empty **or** set `TRITON_FORCE_SAMPLE_DATA=true` — Lagebild uses `sample-data.js` **or** a **live snapshot** if present under `server/data/live-snapshot/` |
| **Live snapshot demo** | On CML with Trino up: `bash scripts/capture-live-snapshot.sh` → then use footer **Demo** offline with real captured rows (`/api/health` → `liveSnapshot.available`) |
| `PROXIMITY_ALERT_API_KEY` | SSB MVE key for Schiffsnähe (`GET /api/proximity-alerts`) |

## Impala notes

JDBC reference used in CDV:

`jdbc:impala://<host>:443/default;AuthMech=12;transportMode=http;httpPath=cliservice;ssl=1;auth=browser`

The MVP maps this to `impyla` with `use_http_transport=true`. Browser OAuth cannot run inside the API; use username/password in `.env.local` when your CDW tenant allows LDAP/PLAIN for programmatic access.

If Impala is unreachable (503, VPN, or auth failure), map and analysis panels automatically use **sample data**; the status bar shows `SAMPLE DATA`.

**Data mode toggle:** Footer **Trino live** / **Demo** sets cookie `triton_data_mode` (+ `X-Triton-Data-Mode` header). `GET /api/health` runs `SELECT 1` when live mode is active and reports `warehouseProbeError` if credentials or JDBC fail.

**Why SAMPLE DATA?** (1) `TRITON_FORCE_SAMPLE_DATA=true`, (2) `TRINO_USER` / `TRINO_PASSWORD` missing on the **Application** (inherit from project), (3) Demo mode selected, (4) previously: health showed `sample` before any query — fixed via Trino probe.

**Autonomous demo (no Trino):** With no `TRINO_USER` / `TRINO_PASSWORD`, `/api/nba/incidents` serves three embedded incidents (incl. Greifswald `49d1d9d5-…`), COA matrix from `nba-decision-record-sample.json`, decision recorder §8/§9, governance + Semantica download stubs — approve works in-memory on sample rows. Check `/api/health` → `nba.demoMode: true`.

## NBA HITL (Phase 4)

Tab **NBA HITL** (`#nba`): lists `nba_incidents`, COA matrix from `nba_coa_runs` (falls back to `decision_record_json` / `coa_matrix.json` when runs are empty), operator **Approve** writes `approved_coa_id` to Trino.

**Lagebild:** toggle **NBA incidents** — markers from `nba_incidents`; click opens HITL for that `flow_uuid`.

Requires `TRINO_USER` / `TRINO_PASSWORD` in `.env.local` (see `.env.example`). Apply DDL:

```bash
cd cdw-analyse
python3 run_trino_ddl.py sql/nba_schema_phase4_hitl.sql
```

Without Trino, the panel uses embedded sample incidents.

## Agent console (replaces SQL tab)

Tab **Agenten** (`#agents`): three CrewAI-backed chat apps grounded on the **Semantica** KG seed (`baltic_ontology.json`). API: `GET /api/agents/apps`, `GET /api/agents/ontology`, `POST /api/agents/chat`. Bridge: [`cai-workbench/agent-console/`](../cai-workbench/agent-console/). Set `TRITON_AGENT_MODE=auto` and reuse `LAGEBILD_*` / `NBA_*` LLM env for live answers.

Legacy `#sql` opens Agenten. `POST /api/query` remains for ops tooling only (not exposed in UI).

## Production (CML Application)

After **`git pull`**, either **restart** the Application ( `entry.py` rebuilds when `src/` is newer than `dist/`) or run manually:

```bash
cd triton-ui
bash cdsw-build.sh
# Restart the Application in CML
NODE_ENV=production npm start
```

**Verify deploy:** Footer shows **Daten: Trino live | Demo** and **`Build 0.2.2-das-preview`** (`build-stamp.json`).  
If you still see **`0.2.0-data-mode`**, `dist/` is stale — run `bash cdsw-build.sh` or set `TRITON_FORCE_UI_REBUILD=1` and restart.

API check: `curl -sS http://127.0.0.1:$CDSW_APP_PORT/api/version` → `"build":"0.2.2-das-preview"`.

Serves the built SPA from `dist/` on `API_PORT`.
