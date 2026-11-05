# Cloudera Data Visualization — Trino import

## Files

| File | Purpose |
|------|---------|
| `triton_visuals_Oct_30_2026_trino.json` | Triton dashboard export with **Trino/Iceberg** table names |
| `convert_cdv_impala_to_trino.py` | Regenerate from an Impala-style export |

Impala exports use `defense.<table>`. Trino needs the **catalog** qualifier:

```text
iceberg.defense.ais_events_ice
iceberg.defense.lagebild
…
```

## Before import

1. In **Data Visualization**, create a **Trino** connection to your CDW Trino endpoint (same cluster as `cdw-analyse/create_db_tables_trino.sql`).
2. Ensure schema **`iceberg.defense`** exists and tables/views match the DDL in the repo.
3. Note the **connection id** assigned in your workspace (Impala export used `dataconnection: 2`).

## Import

1. **Settings → Import** (or workspace import) and select `triton_visuals_Oct_30_2026_trino.json`.
2. If datasets fail to bind, open each dataset and point it at your **Trino** connection (fully qualified names in the export should match without a default database).
3. SQL datasets (**FlottenStatus**, **ShipsAIS**) embed `FROM iceberg.defense.<table>` in the converted export.

## Re-convert another export

```bash
python3 cdv/convert_cdv_impala_to_trino.py path/to/impala_export.json path/to/trino_export.json
```
