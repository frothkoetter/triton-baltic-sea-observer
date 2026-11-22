# Cloudera AI — UC1 & UC2 scheduled jobs

Configure **CML Scheduled Jobs** for **UC1** (Situation awareness / SITREP) and **UC2** (Next Best Action). Complete **Trino** and **DDL** setup first: [`cdw-analyse/TRINO_SETUP_RUNBOOK.md`](../cdw-analyse/TRINO_SETUP_RUNBOOK.md).

**Related**

| Topic | Path |
|-------|------|
| UC1 module | [`situation-awareness/README.md`](situation-awareness/README.md) |
| UC2 module | [`next-best-action/README.md`](next-best-action/README.md) |
| Trino batch (outliers / views) | [`trino-jobs/README.md`](trino-jobs/README.md) |
| Project env template | [`scripts/cml/triton-project.env.example`](../scripts/cml/triton-project.env.example) |
| Apply env via API | [`scripts/cml/set_project_environment.py`](../scripts/cml/set_project_environment.py) |
| UC2 Phase 8 ops | [`next-best-action/docs/CML_PHASE8_RUNBOOK.md`](next-best-action/docs/CML_PHASE8_RUNBOOK.md) |

---

## 1. Prerequisites

1. CML project with repo at e.g. `/home/cdsw` (`git pull origin main`).
2. Trino coordinator **`triton-trino.*`** (not `cdw-triton`), catalog **`iceberg`**, schema **`defense`**.
3. Base DDL applied: `cdw-analyse/setup_cdw_trino_local.sh` (or equivalent).
4. **NBA** tables (if UC2 persists to Iceberg):  
   `cd cdw-analyse && bash sql/nba_apply_all_ddl.sh`  
   Required when `NBA_PERSIST_TRINO=1` and/or `NBA_MATERIALIZE=1`.
5. **Once per module** in a Session (persists on project Python env):  
   `bash cai-workbench/situation-awareness/cdsw-build.sh`  
   (installs **fpdf2** — import name `fpdf` — required for NATO SITREP PDFs)
   `bash cai-workbench/next-best-action/cdsw-build.sh`  
   `bash cai-workbench/trino-jobs/cdsw-build.sh`

---

## 2. Project environment (shared)

Copy secrets to a local file (never commit):

```bash
cp scripts/cml/triton-project.env.example scripts/cml/triton-project.env.local
# edit TRINO_*, LAGEBILD_TRINO_*, LAGEBILD_LLM_*, S3_*, NBA_*
```

Apply to the CML project:

```bash
export CML_HOST='https://<ml-workbench>.cloudera.site'
export CML_API_KEY='…'
python3 scripts/cml/set_project_environment.py \
  --project <your-cml-project> \
  --env-file scripts/cml/triton-project.env.local \
  --dry-run
# remove --dry-run to apply
```

Use **`triton-trino`** in `TRINO_JDBC_URL` and `LAGEBILD_TRINO_HOST`. Mirror `LAGEBILD_S3_PREFIX` and `SITREP_S3_PREFIX` for the **triton-ui** SITREP tab.

**Note:** `triton-project.env.example` sets `NBA_EXIT_IF_QUIET=0` (good for manual runs). On the **UC2 scheduled job**, set **`NBA_EXIT_IF_QUIET=1`** in **Job → Environment** so idle hours exit successfully.

---

## 3. Recommended job schedule

| Cron (example) | Script | Purpose |
|----------------|--------|---------|
| `45 * * * *` | `cai-workbench/trino-jobs/cml_job.py` | `TRINO_JOB=outliers` → `vessel_outliers` |
| `50 * * * *` (weekly / after DDL) | same | `TRINO_JOB=lagebild_views` → refresh `lagebild` views |
| `0 * * * *` | `cai-workbench/situation-awareness/cml_job.py` | **UC1** hourly SITREP |
| `5 * * * *` | `cai-workbench/next-best-action/cml_job.py` | **UC2** alert detection + NBA pipeline |

Run **outliers** before **UC1** so `lagebild` includes `AIS_Outlier` rows.

---

## 4. Job A — Trino outliers (feeds UC1)

| Field | Value |
|-------|--------|
| Type | **Scheduled Job** |
| Python script | `cai-workbench/trino-jobs/cml_job.py` |
| Kernel | Python 3 |
| Arguments | none (use env only) |

**Job environment**

| Variable | Value |
|----------|--------|
| `TRINO_USER` / `TRINO_PASSWORD` | CDW credentials |
| `TRINO_JDBC_URL` | `jdbc:trino://triton-trino.…/iceberg/defense` |
| `TRINO_JOB` | `outliers` |

Optional: same as project via `LAGEBILD_TRINO_*` (job copies into `TRINO_*`).

**Session test**

```bash
export TRINO_JOB=outliers
python3 cai-workbench/trino-jobs/cml_job.py
```

**Verify**

```sql
SELECT Outlier_Type, COUNT(*) FROM iceberg.defense.vessel_outliers GROUP BY 1;
```

---

## 5. Job B — UC1 Situation awareness (Lagebild / SITREP)

| Field | Value |
|-------|--------|
| Type | **Scheduled Job** |
| Python script | **`cai-workbench/situation-awareness/cml_job.py`** |
| Kernel | Python 3 |
| Schedule | `0 * * * *` |
| Arguments | none |

Do **not** run the notebook cells as the job (logs with `Cell In[…]`). Use the **`.py` script** only, or `bash cai-workbench/situation-awareness/run-from-session.sh`.

### UC1 — minimum live environment

| Variable | Purpose |
|----------|---------|
| `LAGEBILD_DATA_ENGINE` | `trino` |
| `LAGEBILD_TRINO_HOST` | `triton-trino.…` |
| `LAGEBILD_TRINO_PORT` | `443` |
| `LAGEBILD_TRINO_USER` / `LAGEBILD_TRINO_PASSWORD` | CDW login |
| `LAGEBILD_TRINO_CATALOG` | `iceberg` |
| `LAGEBILD_TRINO_SCHEMA` | `defense` |
| `LAGEBILD_LLM_BASE_URL` | Azure / model serving chat URL |
| `LAGEBILD_MODEL_ID` | Deployment name |
| `LAGEBILD_API_KEY` | LLM key |
| `LAGEBILD_S3_UPLOAD` | `1` to publish PDFs |
| `LAGEBILD_S3_PREFIX` | e.g. `s3a://…/reports/` |
| `S3_WORKLOAD_USER` / `S3_WORKLOAD_PASSWORD` | CDP object store |

**Optional**

| Variable | Purpose |
|----------|---------|
| `LAGEBILD_SKIP_LLM` | `1` = PDF shell without LLM |
| `LAGEBILD_INSERT_SQL` | `1` → `situation_awareness_summary` |
| `LAGEBILD_ALL_EVENTS` | `1` → `lagebild_all_events` view |
| `LAGEBILD_USE_SAMPLE` | `1` offline only |

Full list: [`situation-awareness/.env.example`](situation-awareness/.env.example).

**Session test**

```bash
cd cai-workbench/situation-awareness
python3 cml_job.py
```

Outputs: `artifacts/reports/nato_sitrep_*.pdf`; with S3 upload, same prefix as **triton-ui** `#sitrep`.

---

## 6. Job C — UC2 Next Best Action (NBA)

| Field | Value |
|-------|--------|
| Type | **Scheduled Job** |
| Python script | **`cai-workbench/next-best-action/cml_job.py`** |
| Kernel | Python 3 |
| Schedule | e.g. `5 * * * *` (after UC1) |
| Arguments | none (`cml_job.py` calls `run_nba.main()` from env) |

### UC2 — production-style environment

| Variable | Typical value |
|----------|----------------|
| `TRINO_JDBC_URL` | `jdbc:trino://triton-trino.…/iceberg/defense` |
| `TRINO_USER` / `TRINO_PASSWORD` | CDW login |
| `NBA_PERSIST_TRINO` | `1` (NBA HITL tab reads Iceberg) |
| `NBA_PHASE` | `7` (governance + prior phases); use `3` for playbook-only |
| `NBA_DEMO_GREIFSWALD` | `0` live alerts; `1` for Greifswald demo only |
| `NBA_EXIT_IF_QUIET` | **`1` on scheduled job** |
| `NBA_MATERIALIZE` | `1` if Phase 6 materialized tables |
| `NBA_SHACL_GATE` | `1` block persist on SHACL failure |
| `NBA_SIM_TICKS` | `6` when simulation runs |
| `NBA_PIPELINE_BLACKOUT` | `1` |
| `NBA_DAS_CABLE_THREAT` | `1` |
| `NBA_DEDUPE_HOURS` | `6` |
| `NBA_SKIP_LLM` | `1` for deterministic CML without model call |

LLM (Phase 3+): reuse `LAGEBILD_LLM_BASE_URL`, `LAGEBILD_API_KEY`, `LAGEBILD_MODEL_ID`.

**Session — demo**

```bash
cd cai-workbench/next-best-action
python3 run_nba.py --demo-greifswald --phase 7 --persist-trino
```

**Session — live job equivalent**

```bash
export NBA_PERSIST_TRINO=1 NBA_PHASE=7 NBA_DEMO_GREIFSWALD=0 NBA_EXIT_IF_QUIET=1
python3 cml_job.py
```

---

## 7. Alignment with triton-ui

| UI feature | Required config |
|------------|-----------------|
| **Trino live** | `TRINO_*` on Application + Project |
| **SITREP** | UC1 `LAGEBILD_S3_UPLOAD=1`; `SITREP_S3_PREFIX` = `LAGEBILD_S3_PREFIX` |
| **NBA HITL** | `NBA_PERSIST_TRINO=1`; tables `nba_incidents`, `nba_coa_runs`, … |
| **DAS Kabel** | `das_cable_spectrogram_metadata` + NiFi ingest |

Deploy UI: `triton-ui/entry.py` Application — see [`triton-ui/APPLICATION_TROUBLESHOOTING.md`](../triton-ui/APPLICATION_TROUBLESHOOTING.md).

---

## 8. Troubleshooting

| Symptom | Action |
|---------|--------|
| Job log shows `Cell In[` | Job must use `cml_job.py`, not notebook |
| UC1 `missing … fpdf2` | Session: `cd cai-workbench/situation-awareness && pip install -r requirements.txt`; or `LAGEBILD_AUTO_PIP=1` (default) on job — `cml_job.py` auto-installs once |
| Log shows `Cell In[` | Use **Scheduled Job** script `cml_job.py`, not notebook cells |
| Trino `error 404` | Host `triton-trino.*`; VW **Running** |
| UC2 job “failed” but no alert | Set `NBA_EXIT_IF_QUIET=1` on the job |
| SITREP empty in UI | UC1 S3 upload + matching prefix; check object store credentials |
| `l agebild_views` 0/2 statements | `git pull main`; run from full repo with current `run_lagebild_views.py` |
| SHACL blocks persist | Fix `decision_record.ttl` or set `NBA_SHACL_GATE=0` in lab |
| UC2 `cannot import name 'apply_jdbc_url_env' from run_trino_ddl` | `git pull origin main` (full repo including `cdw-analyse/trino_endpoint.py`); NBA imports `apply_all_trino_env` from there |
| UC2 log shows `Cell In[` | Scheduled Job script `next-best-action/cml_job.py`, not notebook |

---

*Replace example hostnames with your CDW **Copy Trino URL** and bucket paths.*