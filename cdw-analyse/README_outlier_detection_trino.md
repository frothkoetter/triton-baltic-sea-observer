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

## Pipeline steps

| Step | Case | `Outlier_Type` | Logic (summary) |
|------|------|----------------|-----------------|
| 1 | Baseline | — | Merge 24h AIS into `ais_baseline_sketches` (zoom-11 Bing tiles) |
| 2 | Kinematics / loitering | `Inconsistent Maneuvering`, `Misreported Status`, `Loitering` | SOG vs baseline P99; moored + SOG; long dwell in few tiles |
| 3 | Spoofing | `MMSI Teleportation` | Implied hop speed &gt; 55 kn between consecutive AIS points |
| 4 | Dark vessel | `Dark Vessel` | MAD buoy echo with no AIS within 5 km (synthetic MMSI `-1`) |
| 5 | STS | `Rendezvous` | Two MMSIs within 5 km in the same minute |

Time windows are in-SQL (`interval '4' hour` etc.); tune in `outlier_detection_trino.sql` for your ingest rate.

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

## Scheduling

- Run **baseline (Step 1)** hourly (needs ~24h AIS history).
- Run **detection (Steps 2–5)** every 5–15 minutes after NiFi batches land in Iceberg.
- Use CDP Query Studio, Oozie/cron, or NiFi **ExecuteSQL** / Trino reporting against this script.

## Related

- NiFi flow writes `vessel_outliers` in some deployments — dedupe uses `(MMSI, Event_Timestamp, Outlier_Type)` anti-joins.
- Rule parity: [`cai-workbench/ais-outlier-detector/README.md`](../cai-workbench/ais-outlier-detector/README.md).