# Iceberg Snapshots on CDW Trino (`cdw-triton`)

Live probe (`probe_iceberg_snapshots.py`) against **Cloudera Trino** `cloudera-0.481.1.2026.0.24.0-37-003aafd` / **Iceberg 1.10.2** (verified November 2026).

## Credentials

```bash
cp .env.trino.local.example .env.trino.local   # TRINO_PASSWORD via environment secret
set -a && source .env.trino.local && set +a
python3 probe_iceberg_snapshots.py --table ships
```

## Result matrix (CDW Trino)

| Feature | Syntax | Result |
| :---- | :---- | :---- |
| Coordinator version | `SELECT version()` | OK |
| List snapshots | `SELECT * FROM iceberg.defense."<table>$snapshots"` | OK on Iceberg **tables** |
| List refs (branches/tags) | `SELECT * FROM iceberg.defense."<table>$refs"` | OK (`main` branch only) |
| Time travel by snapshot ID | `… FOR VERSION AS OF <bigint>` | OK |
| Time travel by timestamp | `… FOR TIMESTAMP AS OF TIMESTAMP '…'` | OK |
| Time travel via branch name | `… FOR VERSION AS OF 'main'` | OK |
| Hive/Impala clauses | `FOR SYSTEM_VERSION` / `FOR SYSTEM_TIME` | **Not supported** on Trino |
| Hive dotted metadata | `table.snapshots` / `table.HISTORY` | **Syntax error** (use `$snapshots`) |
| Create branch (Trino SQL) | `CREATE BRANCH … IN TABLE` | **NOT_SUPPORTED** (“does not support creating branches”) |
| Hive branch DDL on Trino | `ALTER TABLE … CREATE BRANCH` | **Syntax error** |
| Show branches | `SHOW BRANCHES IN TABLE …` | Parses OK; often empty result (refs via `$refs`) |
| Rollback | `ALTER TABLE … EXECUTE rollback_to_snapshot(id)` | OK (use only on test tables) |
| Expire snapshots | `ALTER TABLE … EXECUTE expire_snapshots(retention_threshold => '30d')` | OK |

## Views (`lagebild`, `lagebild_all_events`)

These are **views**, not Iceberg tables: no `"lagebild$snapshots"`, no `SHOW CREATE TABLE`. `FOR VERSION AS OF <snapshot_id>` on `lagebild` does **not** change row counts (clause ignored or not propagated through the view definition); do not use views for incident snapshot pinning.

NBA snapshot pinning should target **underlying Iceberg fact tables** (e.g. `ais_events_ice`, fleet/readiness tables) or materialize a **dedicated Iceberg table** per incident run.

## NBA architecture implication

1. **Do** fix incident context with `FOR VERSION AS OF <snapshot_id>` on writable Iceberg tables (fleet, AIS, readiness).
2. **Do not** plan Red-vs.-Blue COA forks via Trino `CREATE BRANCH`; use separate `nba_sim_*` tables or offline engines (NiFi replay / Parquet in CML).
3. **Do not** use `rollback_to_snapshot` in agent workflows on production tables.