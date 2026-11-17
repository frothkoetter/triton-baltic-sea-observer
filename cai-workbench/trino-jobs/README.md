# Trino scheduled jobs (CML Workbench)

Runs **CDW Trino SQL** from `cdw-analyse/` as CML Scheduled Jobs (`.py` only in the job picker).

## Scripts

| CML job file | `TRINO_JOB` | What runs |
|--------------|-------------|-----------|
| `cai-workbench/trino-jobs/cml_job.py` | `outliers` (default) | `cdw-analyse/outlier_detection_trino.sql` → `vessel_outliers` |
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