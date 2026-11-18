# Trino scheduled jobs (CML Workbench)

Runs **CDW Trino SQL** from `cdw-analyse/` as CML Scheduled Jobs (`.py` only in the job picker).

## Scripts

| CML job file | `TRINO_JOB` | What runs |
|--------------|-------------|-----------|
| `cai-workbench/trino-jobs/cml_job.py` | `outliers` (default) | `cdw-analyse/outlier_detection_trino.sql` → `vessel_outliers`, `infrastructure_ais_suspicions`, baselines |
| same | `lagebild_views` | `defense.lagebild` + `defense.lagebild_all_events` (includes **AIS_Outlier** union) |

## Environment

Same credentials as UC1 Lagebild when possible:

- `TRINO_USER` / `TRINO_PASSWORD`, or `TRINO_JDBC_URL=jdbc:trino://host:443/iceberg/defense`
- Or `LAGEBILD_TRINO_*` (auto-copied into `TRINO_*` by `cml_job.py`)

Optional: `TRINO_JOB=lagebild_views` on a **one-off** job after deploying view SQL; re-run when `create_db_tables_trino.sql` changes.

## Suggested schedule (simulation)

1. **:45** — `TRINO_JOB=outliers` (`cml_job.py`) — refresh baselines + `vessel_outliers`
2. **:50** — `TRINO_JOB=lagebild_views` — only needed after DDL/view changes (or weekly smoke)
3. **:00** — UC1 `situation-awareness/cml_job.py` (SITREP reads `iceberg.defense.lagebild`, now with `Data_Source='AIS_Outlier'`)

Verify:

```sql
SELECT Data_Source, COUNT(*) FROM iceberg.defense.lagebild GROUP BY 1;
SELECT Outlier_Type, COUNT(*) FROM iceberg.defense.vessel_outliers GROUP BY 1;
```

Build step (session or project): `bash cai-workbench/trino-jobs/cdsw-build.sh`

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