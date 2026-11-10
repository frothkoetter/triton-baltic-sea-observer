# Cloudera Data Visualization — Trino import

## Files

| File | Purpose |
|------|---------|
| `triton_visuals_Oct_30_2026_trino.json` | Triton dashboard export with **Trino/Iceberg** table names |
| `visuals_triton-nato-c2-ops.json` | **NATO C2 ops** export (Trino `iceberg.defense.*`; dashboards + `lagebild`) |
| `convert_cdv_impala_to_trino.py` | Regenerate from an Impala-style export |

`visuals_triton-nato-c2-ops.json` is already **Trino-qualified** (`iceberg.defense.<table>`); import it against your Trino connection without running the converter.

Older Impala exports use `defense.<table>`. Trino needs the **catalog** qualifier:

```text
iceberg.defense.ais_events_ice
iceberg.defense.lagebild (snapshot) and iceberg.defense.lagebild_all_events (full 24h stream; see cdw-analyse/lagebild_volume_compare.sql)
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
