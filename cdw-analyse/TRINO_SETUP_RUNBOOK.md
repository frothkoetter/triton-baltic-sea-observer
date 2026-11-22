# Trino setup runbook (CDW + CML UI)

End-to-end steps used to bring **Iceberg `defense`** on Cloudera Data Warehouse (CDW) Trino online for local DDL, analytics, and the **triton-ui** “Trino live” mode. Use this after a fresh clone or when you see **404**, **401**, or footer **error 404** on the NATO UI.

**Related files**

| Area | Path |
|------|------|
| Local credentials template | `cdw-analyse/.env.trino.local.example` → copy to `.env.trino.local` (gitignored) |
| Connectivity checklist | `cdw-analyse/README_CDW_TRINO_LOCAL.md` |
| Full DDL apply | `cdw-analyse/setup_cdw_trino_local.sh` |
| Views only | `cdw-analyse/apply_views_trino.sh` |
| Preflight | `cdw-analyse/trino_cdw_doctor.py` |
| CML project env example | `scripts/cml/triton-project.env.example` |
| CML env API | `scripts/cml/set_project_environment.py` |
| UI on CML | `triton-ui/APPLICATION_TROUBLESHOOTING.md` |
| UC1 / UC2 CML jobs | `cai-workbench/CML_UC_JOBS_SETUP.md` |

---

## 1. Virtual warehouse and hostname

1. In **CDW**, open the Trino virtual warehouse and set status **Running** (not Stopped/Suspended).
2. **⋮ → Copy Trino URL** — HTTPS, port **443**, host like:
   - **Correct coordinator:** `triton-trino.<env-hash>.cloudera.site`
   - **Wrong (DW overview only):** `cdw-triton.<env-hash>.cloudera.site` → Trino REST calls return **HTTP 404** (including `SELECT version()`).
3. Paste into local env (see step 2). JDBC must use the **same host** as Copy Trino URL:
   ```bash
   TRINO_SERVER=https://triton-trino.dw-triton-cdp-env.a465-9q4k.cloudera.site:443
   TRINO_JDBC_URL=jdbc:trino://triton-trino.dw-triton-cdp-env.a465-9q4k.cloudera.site:443/iceberg/defense
   ```

---

## 2. Local credentials (`cdw-analyse`)

From the repo root or `cdw-analyse`:

```bash
cd cdw-analyse
cp .env.trino.local.example .env.trino.local
chmod 600 .env.trino.local
```

Edit `.env.trino.local`:

```bash
TRINO_USER=<CDP / Data Warehouse user>
TRINO_PASSWORD=<same password as CDW console — not CML API key>
TRINO_CATALOG=iceberg
TRINO_SCHEMA=defense
TRINO_CONNECT_SCHEMA=defense
TRINO_SKIP_CREATE_SCHEMA=1
TRINO_QUALIFY_SCHEMA=1
TRINO_SKIP_DROP=1
```

Load env for every session:

```bash
set -a && source .env.trino.local && set +a
```

Optional Python venv (setup script creates one if missing):

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-trino.txt
```

---

## 3. Doctor (always before DDL)

```bash
cd cdw-analyse
set -a && source .env.trino.local && set +a
python3 trino_cdw_doctor.py
```

| Outcome | Meaning | Action |
|---------|---------|--------|
| **HTTP 404** on probe | Wrong host or VW not running | Use `triton-trino…`, start warehouse, fix `TRINO_SERVER` |
| **HTTP 401** / Invalid credentials | Host OK, login wrong | Fix `TRINO_USER` / `TRINO_PASSWORD` (CDP password); exit code **3** |
| `SHOW TABLES` fails | Schema missing | Create schema **`defense`** on catalog **`iceberg`** in CDW UI (step 4) |
| All green | Proceed to DDL | |

Optional CLI (same URL as Copy Trino URL):

```bash
trino --server 'https://triton-trino....:443' --user YOU --password \
  --catalog iceberg --schema defense
# SELECT version();
```

---

## 4. Create `iceberg.defense` in CDW UI

On many CDW tenants, Trino **`CREATE SCHEMA`** / **`USE iceberg.defense`** from scripts return **404**. Reliable path:

1. CDW UI → catalog **iceberg** → create schema **defense**.
2. Keep `TRINO_SKIP_CREATE_SCHEMA=1` in `.env.trino.local`.
3. DDL runner qualifies objects as `iceberg.defense.<table>` and skips `USE` when skip-create is set (current `run_trino_ddl.py` on `main`).

---

## 5. Apply DDL locally

```bash
cd cdw-analyse
./setup_cdw_trino_local.sh
```

What it does:

- Sources `.env.trino.local`
- Runs **doctor** (aborts on 401 / connectivity failure)
- Applies `create_db_tables_trino.sql` via `run_trino_ddl.py`
- Optionally reloads NSB ship rows from `ships_nsb_trade_1000.sql`
- Smoke `COUNT(*)` on key tables

**Views only** (after base tables exist):

```bash
./apply_views_trino.sh
```

Expect **8/8** view statements OK when qualification and host are correct.

### Issues fixed in the DDL runner (pull latest `main`)

If you see these, `git pull origin main` (DDL fixes landed through Nov 2026):

| Error | Cause | Fix on `main` |
|-------|--------|----------------|
| `USE iceberg.defense` → 404 | CDW rejects session `USE` | Skip `USE`; qualify `iceberg.defense.*` |
| `VIEWCREATE` syntax error | Duplicate `CREATE OR REPLACE VIEW` in split logic | Fixed view boundary split |
| `iceberg.defense.iceberg.defense.*` | Over-qualified CTEs / views | Qualify only known `CREATE TABLE` names |
| `SHOW SCHEMAS` → 404 in preflight | CDW API quirk | Preflight `auto` warns and continues |
| `from __future__` SyntaxError | Import order | Fixed in `run_trino_ddl.py` |

Continue after a single failure:

```bash
python3 run_trino_ddl.py create_db_tables_trino.sql --continue
```

---

## 6. Git pull without blocking on local env

If `git pull` refuses because of local edits to CML example env:

```bash
git stash push -m local -p scripts/cml/triton-project.env.example
# or: git checkout -- scripts/cml/triton-project.env.example
git pull origin main
```

Never commit real passwords; keep secrets in `.env.trino.local` and `scripts/cml/triton-project.env.local` only.

---

## 7. CML project / Application environment (UI + jobs)

The NATO UI and Lagebild jobs read **project or Application** env vars, not your laptop `.env.trino.local`.

1. Copy template:

   ```bash
   cp scripts/cml/triton-project.env.example scripts/cml/triton-project.env.local
   ```

2. Set (minimum for **Trino live**):

   ```bash
   CDSW_APP_POLLING_ENDPOINT=/healthz
   TRINO_JDBC_URL=jdbc:trino://triton-trino.<env>.cloudera.site:443/iceberg/defense
   TRINO_USER=...
   TRINO_PASSWORD=...
   TRINO_CATALOG=iceberg
   TRINO_SCHEMA=defense
   LAGEBILD_DATA_ENGINE=trino
   LAGEBILD_TRINO_HOST=triton-trino.<env>.cloudera.site
   LAGEBILD_TRINO_PORT=443
   LAGEBILD_TRINO_USER=...    # same as TRINO_USER
   LAGEBILD_TRINO_PASSWORD=...
   ```

   **Do not** use `cdw-triton` for JDBC or `LAGEBILD_TRINO_HOST` (footer shows **error 404**). On newer UI builds, `triton-bridge.py` may rewrite `cdw-triton` → `triton-trino` for probes only — still fix project env for NBA/Lagebild.

3. Apply to CML project (API key from CML **User Settings → API Keys**):

   ```bash
   export CML_HOST='https://<ml-workbench>.cloudera.site'
   export CML_API_KEY='...'
   python3 scripts/cml/set_project_environment.py \
     --project <your-project-name> \
     --env-file scripts/cml/triton-project.env.local \
     --dry-run
   # remove --dry-run to apply
   ```

4. Restart the **triton-ui** Application after env changes.

---

## 8. Build and run triton-ui on CML

In a **Session** (persists `node_modules` + `dist` for the Application):

```bash
cd "${CDSW_WORKING_DIRECTORY:-/home/cdsw}/triton-ui"
git pull origin main
bash cdsw-build.sh
ls -la dist/index.html node_modules/express
```

Application wizard:

| Field | Value |
|-------|--------|
| Script | `triton-ui/entry.py` |
| Kernel | `python3` |

Do **not** hard-code `API_PORT`. Use `CDSW_APP_POLLING_ENDPOINT=/healthz`.

Verify:

```bash
curl -sS "http://127.0.0.1:${CDSW_APP_PORT}/api/health"
curl -sS "http://127.0.0.1:${CDSW_APP_PORT}/api/version"
```

Footer: **Trino live** | **Demo**, build **`0.2.0-data-mode`**. **Trino live** should show live AIS when `dataSource` is `trino` and `warehouseProbeError` is null.

Session probe (same env as Application):

```bash
cd triton-ui
set -a && source /path/to/env-with-trino && set +a
python3 server/trino-bridge.py "SELECT 1"
```

Until Trino works, use footer **Demo** (embedded sample data).

See `triton-ui/APPLICATION_TROUBLESHOOTING.md` for **Starting** / port mismatch / stale `dist/`.

---

## 9. Quick verification matrix

| Check | Command / UI | Success |
|-------|----------------|---------|
| Coordinator | `trino_cdw_doctor.py` | HTTP probe OK, `SELECT version()` OK |
| DDL | `./setup_cdw_trino_local.sh` | All statements OK; smoke counts > 0 |
| Views | `./apply_views_trino.sh` | 8 view statements OK |
| UI bridge | `python3 server/trino-bridge.py "SELECT 1"` | `"ok": true` |
| UI | `/api/health` | `dataSource: "trino"`, no `warehouseProbeError` |

---

## 10. Troubleshooting summary

| Symptom | Fix |
|---------|-----|
| Everything **404** | `triton-trino` host + VW **Running** |
| **401** | CDP password in `TRINO_PASSWORD`; not API tokens |
| `USE` / `CREATE SCHEMA` **404** | Create **defense** in DW UI; `TRINO_SKIP_CREATE_SCHEMA=1` |
| UI **error 404** on Trino live | Update CML `TRINO_JDBC_URL` / `LAGEBILD_TRINO_HOST` to `triton-trino` |
| UI no Trino toggle | `bash cdsw-build.sh` (stale `dist/`) |
| Merge conflict on `create_db_tables_trino.sql` | Resolve or stash local SQL, pull, re-run DDL |

---

## 11. UC1 & UC2 CML scheduled jobs

After Trino and DDL are green, configure hourly **Situation awareness (UC1)** and **Next Best Action (UC2)** jobs:

**[cai-workbench/CML_UC_JOBS_SETUP.md](../cai-workbench/CML_UC_JOBS_SETUP.md)**

Includes cron order (outliers → UC1 → UC2), wizard fields, environment tables, and UI alignment.

---

*Hostnames in this doc use the `a465-9q4k` example tenant; replace with your **Copy Trino URL** host from CDW.*