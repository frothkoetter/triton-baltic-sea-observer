# Trino scheduled jobs (CML Workbench)

Runs **CDW Trino SQL** from `cdw-analyse/` as CML Scheduled Jobs (`.py` only in the job picker).

**See also:** [`CML_UC_JOBS_SETUP.md`](../CML_UC_JOBS_SETUP.md) (wizard fields + project env), [`cdw-analyse/TRINO_SETUP_RUNBOOK.md`](../../cdw-analyse/TRINO_SETUP_RUNBOOK.md).

---

## CML Scheduled Job configuration

Create **one or three** jobs (same script, different **Job → Environment**). Do **not** run notebook cells — only **`cml_job.py`**.

| Wizard field | Value |
|--------------|--------|
| **Type** | Scheduled Job |
| **Script** | `cai-workbench/trino-jobs/cml_job.py` |
| **Kernel** | Python 3 |
| **Arguments** | *(empty)* |
| **Working directory** | project default (`/home/cdsw`) — must be the **full git repo** with `cdw-analyse/` |

**Once per project** (Session): `bash cai-workbench/trino-jobs/cdsw-build.sh` (installs `trino` client from `requirements.txt`).

### Job dispatch (`TRINO_JOB`)

| `TRINO_JOB` | Aliases | SQL / runner | Outputs (Iceberg `defense`) |
|-------------|---------|--------------|---------------------------|
| `outliers` *(default)* | `outlier`, `ais_outliers` | `cdw-analyse/outlier_detection_trino.sql` | `ais_baseline_sketches`, `vessel_outliers`, `infrastructure_ais_suspicions`, **`baltic_threat_tile_sketches`**, view `threat_hot_tiles` |
| `socmint_sketch` | `socmint`, `socmint_verify`, `threat_sketch` | `cdw-analyse/socmint_sketch_verification_trino.sql` | `socmint_tile_sketches`, view `socmint_sketch_verified`, MERGE into `social_media_messages` |
| `lagebild_views` | `lagebild`, `views` | `cdw-analyse/run_lagebild_views.py` | `lagebild`, `lagebild_all_events` (includes `Data_Source='AIS_Outlier'`) |

If `TRINO_JOB` is unset on the job, **`outliers`** runs.

For a custom SQL file only: set `TRINO_JOB` to any other string and **`TRINO_SQL_FILE`** (path relative to repo root or `cdw-analyse/`). Optional **`TRINO_STOP_ON_ERROR=1`**.

### Environment variables

#### Credentials (required — job or project env)

Set **`TRINO_*`** on the job, **or** reuse UC1 **`LAGEBILD_TRINO_*`** on the project (`cml_job.py` copies into `TRINO_*` when `TRINO_*` is empty):

| Variable | Required | Description |
|----------|----------|-------------|
| `TRINO_USER` | yes* | CDP / Data Warehouse user |
| `TRINO_PASSWORD` | yes* | CDW password (not CML API key) |
| `TRINO_JDBC_URL` | recommended | `jdbc:trino://triton-trino.<env>.cloudera.site:443/iceberg/defense` — host must be **`triton-trino.*`**, not `cdw-triton.*` |

\* Or provide the same fields as `LAGEBILD_TRINO_USER` / `LAGEBILD_TRINO_PASSWORD` (+ host/port or JDBC).

| Variable | Default | Description |
|----------|---------|-------------|
| `TRINO_HOST` | from JDBC | Coordinator hostname if JDBC not set |
| `TRINO_PORT` | `443` | HTTPS port |
| `TRINO_CATALOG` | `iceberg` | Trino catalog |
| `TRINO_SCHEMA` | `defense` | Schema for DDL runner |

**`LAGEBILD_TRINO_*` → `TRINO_*` mapping** (in `cml_job.py`): `USER`, `PASSWORD`, `JDBC_URL`, `HOST`, `PORT`, `CATALOG`, `SCHEMA`.

#### Job selector

| Variable | Default | Description |
|----------|---------|-------------|
| `TRINO_JOB` | `outliers` | See table above |

#### DDL runner (`run_trino_ddl.py` — used by outlier / socmint jobs)

| Variable | Default for `outliers` / `socmint_sketch` | Description |
|----------|-------------------------------------------|-------------|
| `TRINO_CONTINUE_ON_ERROR` | `1` *(set automatically)* | Log statement errors and continue |
| `TRINO_SCHEMA_PREFLIGHT` | `auto` | Fail fast if `iceberg.defense` not visible; use `off` to skip |
| `TRINO_SKIP_CREATE_SCHEMA` | — | `1` when schema exists in DW UI |
| `TRINO_SKIP_DROP` | — | `1` for additive reruns |
| `TRINO_ADDITIVE_DDL` | — | `1` skip destructive drops in some paths |
| `TRINO_STOP_ON_ERROR` | `0` | `1` only when using custom `TRINO_SQL_FILE` |
| `TRINO_SQL_FILE` | — | Relative path when `TRINO_JOB` is not a known alias |

#### Debug

| Variable | Description |
|----------|-------------|
| `TRINO_JOB_DEBUG` | `1` — print resolved paths (e.g. `run_lagebild_views.py`) |

**Project template:** [`scripts/cml/triton-project.env.example`](../../scripts/cml/triton-project.env.example) — add per-job overrides in **CML → Job → Environment** (e.g. `TRINO_JOB=outliers` vs `TRINO_JOB=socmint_sketch`).

### Example: three scheduled jobs

| Job name (example) | Cron | Job env |
|--------------------|------|---------|
| `trino-outliers` | `45 * * * *` | `TRINO_JOB=outliers` |
| `trino-socmint-sketch` | `50 * * * *` | `TRINO_JOB=socmint_sketch` |
| `trino-lagebild-views` | weekly or after DDL | `TRINO_JOB=lagebild_views` |

Share **project** `TRINO_USER` / `TRINO_PASSWORD` / `TRINO_JDBC_URL` (or `LAGEBILD_TRINO_*`); only **`TRINO_JOB`** differs per job.

### Session smoke test

```bash
cd /home/cdsw
export TRINO_JOB=outliers   # or socmint_sketch | lagebild_views
# credentials: export TRINO_USER=… TRINO_PASSWORD=…
# or rely on project LAGEBILD_TRINO_*
python3 cai-workbench/trino-jobs/cml_job.py
```

Expected log tail: `Preflight: schema iceberg.defense is visible…` and **`OK statements: N`** with no `ERROR:` block (outliers ~18, socmint ~6–7, lagebild_views ~2).

---

## Scripts (reference)

| CML job file | `TRINO_JOB` | What runs |
|--------------|-------------|-----------|
| `cai-workbench/trino-jobs/cml_job.py` | `outliers` (default) | `cdw-analyse/outlier_detection_trino.sql` → `vessel_outliers`, `infrastructure_ais_suspicions`, baselines, **`baltic_threat_tile_sketches` (Step 9)** |
| same | `socmint_sketch` (aliases: `socmint`, `socmint_verify`) | `socmint_sketch_verification_trino.sql` → `socmint_tile_sketches`, view `socmint_sketch_verified`, MERGE `social_media_messages` — **run after outliers** for trust upgrades |
| same | `lagebild_views` | `defense.lagebild` + `defense.lagebild_all_events` (includes **AIS_Outlier** union) |

## Suggested schedule (simulation)

1. **:45** — `TRINO_JOB=outliers` (`cml_job.py`) — refresh baselines + `vessel_outliers` + threat tile sketches
2. **:50** — `TRINO_JOB=socmint_sketch` — SOCMINT tile density + trust MERGE (needs Step 9 data for corroboration)
3. **:55** — `TRINO_JOB=lagebild_views` — only needed after DDL/view changes (or weekly smoke)
4. **:00** — UC1 `situation-awareness/cml_job.py` (SITREP reads `iceberg.defense.lagebild`, now with `Data_Source='AIS_Outlier'`)

Verify:

```sql
SELECT Data_Source, COUNT(*) FROM iceberg.defense.lagebild GROUP BY 1;
SELECT Outlier_Type, COUNT(*) FROM iceberg.defense.vessel_outliers GROUP BY 1;
```

Build step (session or project): `bash cai-workbench/trino-jobs/cdsw-build.sh`

## `SystemExit: 0` / Engine exited with status 1 (job was OK)

If the log shows **`OK statements: N`** and **`SystemExit: 0`**, Trino finished successfully; the CML **Jupyter kernel** still reports a failed engine. Use a **Scheduled Job** pointing at `cml_job.py` (not a notebook cell), or `git pull` for the kernel-safe exit path in `cml_job.py` (success does not raise `SystemExit` in ipykernel).

## `OK statements: 0/2` / syntax error at line 501 (`;`)

The job reads `~/cdw-analyse/run_lagebild_views.py` from the **full git repo**, not only `cai-workbench`. A feature-branch checkout often leaves that file old while `git pull` says "Already up to date".

```bash
cd /home/cdsw
git fetch origin main
git checkout main
git pull origin main
grep 'drop statement-splitting' cdw-analyse/run_lagebild_views.py
TRINO_JOB=lagebild_views python3 cai-workbench/trino-jobs/cml_job.py
```

Expect `OK statements: 2/2`. Set `TRINO_JOB_DEBUG=1` to print which `run_lagebild_views.py` path is used.

## Manual test in a CML session

From `~/cai-workbench/trino-jobs` (CML does not need `./` if the file is not executable):

```bash
export TRINO_USER=… TRINO_PASSWORD=…   # or LAGEBILD_TRINO_*
TRINO_JOB=lagebild_views python3 cml_job.py
# or: bash -c 'TRINO_JOB=lagebild_views exec python3 cml_job.py'
```

If you prefer `./cml_job.py`, run `chmod +x cml_job.py` once after git pull (executable bit is set in repo).