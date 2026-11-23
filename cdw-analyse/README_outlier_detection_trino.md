# Outlier detection with Trino (CDW)

SQL pipeline for **maritime anomaly detection** on `iceberg.defense`, replacing or complementing the NiFi LLM outlier step and the CML rule model in [`../cai-workbench/ais-outlier-detector/`](../cai-workbench/ais-outlier-detector/).

## Files

| File | Purpose |
|------|---------|
| [`outlier_detection_trino.sql`](outlier_detection_trino.sql) | Full pipeline (baseline + 4 detection cases) |
| [`run_outlier_detection_trino.sh`](run_outlier_detection_trino.sh) | Run against CDW using `.env.trino.local` |
| [`run_trino_ddl.py`](run_trino_ddl.py) | Statement splitter / Trino executor |

## Prerequisites

1. Schema applied: `./setup_cdw_trino_local.sh` (or `create_db_tables_trino.sql`).
2. **AIS** flowing into `ais_events_ice` (NiFi simulators or live feed).
3. **`ships`** metadata for vessel types (incl. NSB fleet).
4. Optional: **`buoy_data`** for dark-vessel (Case 2b).

Tables written:

| Table | Role |
|-------|------|
| `ais_baseline_sketches` | T-Digest SOG + MMSI SetDigest per quadkey / type / course sector |
| `vessel_outliers` | Detected anomalies (Iceberg; NiFi PutIceberg compatible) |
| `infrastructure_ais_suspicions` | Pipeline/cable AIS blackout & korridor hits (spatial-lab 08/07) |
| `baltic_threat_tile_sketches` | **Step 9** — per-tile T-Digest severity + SetDigest outlier types/MMSIs (4h window) |
| `socmint_tile_sketches` | SOCMINT post density per tile (companion job) |

## Pipeline steps

| Step | Case | `Outlier_Type` | Logic (summary) |
|------|------|----------------|-----------------|
| 1 | Baseline | — | Merge 24h AIS into `ais_baseline_sketches` (zoom-11 Bing tiles) |
| 2 | Kinematics / loitering | `Inconsistent Maneuvering`, `Misreported Status`, `Loitering` | SOG vs baseline P99; moored + SOG; long dwell in few tiles |
| 3 | Spoofing | `MMSI Teleportation` | Implied hop &gt; 75 kn, hop ≥ 0.25 nm, gap ≥ 2 min (LAG ordered by parsed timestamp) |
| 4 | Dark vessel | `Dark Vessel` | MAD buoy echo with no AIS within 5 km (synthetic MMSI `-1`) |
| 5 | STS | `Rendezvous` | Baltic 2h window: ≤500 m, both SOG &lt; 4 kn, latest AIS per MMSI/minute, pair dedupe |
| 6 | Infrastructure | `Pipeline AIS Blackout` | 7d `ais_daten` gaps 30–1440 min vs `ostsee_pipelines_segmented` (lab query 08); ends in last 24h |
| 7 | Infrastructure | `Pipeline Korridor Crossing`, `Pipeline Korridor Loitering` | Buffered corridor intersects + multi-cross or &gt;15 min &lt;3 kn (lab query 07) |
| 8 | DAS | `DAS Cable Threat` | `das_cable_spectrogram_metadata` threats → `vessel_outliers` |
| 9 | **Threat sketches** | — | Rebuild `baltic_threat_tile_sketches` + view `threat_hot_tiles` for fast SOCMINT/NBA |

Each job run **deletes** rows for the Trino rule types above, then recomputes (prevents million-row `Rendezvous` buildup). Legacy NiFi labels (e.g. `Route Deviation`) are untouched.

**Infrastructure prerequisites:** `ais_daten` view and `ostsee_pipelines_segmented` (see `create_db_tables_trino.sql` Ostsee block). Detailed rows land in `infrastructure_ais_suspicions`; the same job copies them into `vessel_outliers` so `defense.lagebild` shows them as `AIS_Outlier`.

Time windows are in-SQL (`interval '4' hour` etc.); tune in `outlier_detection_trino.sql` for your ingest rate.

**Trino API:** baseline P99 uses `value_at_quantile(CAST(speed_tdigest AS tdigest), 0.99)` (not legacy `value_at`, which is not registered on CDW Trino).

## CML scheduled job (Workbench)

Prefer a **`.py` scheduled job** instead of shell on CML:

| Setting | Value |
|---------|--------|
| Script | `/home/cdsw/cai-workbench/trino-jobs/cml_job.py` |
| Env | `TRINO_JOB=outliers` (default), `TRINO_USER` / `TRINO_PASSWORD` (or `LAGEBILD_TRINO_*`) |
| Build | `bash cai-workbench/trino-jobs/cdsw-build.sh` |

After deploying view DDL, run once with `TRINO_JOB=lagebild_views` so UC1 sees `AIS_Outlier` in `defense.lagebild` ([`run_lagebild_views.py`](run_lagebild_views.py)). Trino views omit `ORDER BY` (Impala/Hive DDL used to include it); `run_lagebild_views.py` strips any legacy `ORDER BY` tail.

## Run locally

```bash
cd cdw-analyse
cp .env.trino.local.example .env.trino.local   # set TRINO_PASSWORD
chmod +x run_outlier_detection_trino.sh
./run_outlier_detection_trino.sh
```

Manual:

```bash
set -a && source .env.trino.local && set +a
TRINO_CONTINUE_ON_ERROR=1 python3 run_trino_ddl.py outlier_detection_trino.sql --continue
```

## Verify

```sql
USE iceberg.defense;
SELECT Outlier_Type, COUNT(*) AS n
FROM vessel_outliers
GROUP BY 1
ORDER BY n DESC;

SELECT * FROM vessel_outliers
ORDER BY Event_Timestamp DESC
LIMIT 20;
```

## SOCMINT verification (sketch lookup, Trino only)

After Step 9 threats land in `baltic_threat_tile_sketches`, run:

```bash
./run_socmint_sketch_verification_trino.sh
# or on CML: TRINO_JOB=socmint_sketch
```

See [`socmint_sketch_verification_trino.sql`](socmint_sketch_verification_trino.sql) and view `socmint_sketch_verified`. Trust upgrades use tile **sev_p50 / sev_p90** instead of heavy AIS↔post spatial joins.

| Sketch | Type | Use |
|--------|------|-----|
| `ais_baseline_sketches` | T-Digest SOG + MMSI set per tile/type/sector | Kinematic outlier **detection** (P99 SOG) |
| `baltic_threat_tile_sketches` | T-Digest severity + type/MMSI/lane sets | Gefahren **tile lookup** + SOCMINT **verification** |
| `socmint_tile_sketches` | Post count + account set per tile | IW burst / coordinated tile activity |

## Scheduling

- Run **baseline (Step 1)** hourly (needs ~24h AIS history).
- Run **detection (Steps 2–9)** every 5–15 minutes after NiFi batches land in Iceberg.
- Run **SOCMINT sketch verify** immediately after outliers (same cadence or every 15 min).
- Use CDP Query Studio, Oozie/cron, or NiFi **ExecuteSQL** / Trino reporting against this script.

## Related

- NiFi flow writes `vessel_outliers` in some deployments — dedupe uses `(MMSI, Event_Timestamp, Outlier_Type)` anti-joins.
- Rule parity: [`cai-workbench/ais-outlier-detector/README.md`](../cai-workbench/ais-outlier-detector/README.md).