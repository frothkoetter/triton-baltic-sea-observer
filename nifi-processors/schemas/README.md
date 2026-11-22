# NiFi record schemas (JsonTreeReader)

Avro JSON schemas for **JSONTreeReader** → **PutIceberg** / record processors.  
Field names match **UpdateAttribute** column names and **AttributesToJSON** lists (not dotted NiFi attributes).

| File | Iceberg table | Ontology |
|------|----------------|----------|
| [`lidar_simulation_metadata.avsc`](./lidar_simulation_metadata.avsc) | `defense.lidar_simulation_metadata` | [`ontology/mappings/lidar_simulation_metadata.yaml`](../../ontology/mappings/lidar_simulation_metadata.yaml) |
| [`das_cable_spectrogram_metadata.avsc`](./das_cable_spectrogram_metadata.avsc) | `defense.das_cable_spectrogram_metadata` | [`ontology/mappings/das_cable_spectrogram_metadata.yaml`](../../ontology/mappings/das_cable_spectrogram_metadata.yaml) |

## JsonTreeReader (controller service)

| Property | Value |
|----------|--------|
| **Schema Access Strategy** | `schema-text-property` (or `Use 'Schema Text' Property`) |
| **Schema Text** | Paste contents of the `.avsc` file, or Parameter `${das.avro.schema}` / `${avro.schema}` |
| **Schema Application Strategy** | `SELECTED_PART` (one JSON object per FlowFile) |
| **Allow Comments** | `false` if you paste raw JSON; Avro schema is JSON without comments |

For **LiDar Simulation**, the showcase flow uses **Infer Schema** with `${avro.schema}` — put the **lidar** `.avsc` body in that parameter.

## DAS AttributesToJSON (suggested list)

```
hadoop_file_url,segment_index,cable_asset,cable_id,cable_name,chainage_m,segment_lat,segment_lon,threat_class,predicted_threat_type,model_confidence,channel_count,window_seconds,fs_hz,filename,ingested_at
```

Optional: `spectral_peak_hz`, `energy_rms` (already in Avro schema).

## MergeContent batches

If merged content is a **JSON array** `[{...},{...}]`, use **SplitJson** on `$.*` before the reader, or configure the reader for array (schema root `array` of the record type) per your NiFi version.

## `ingested_at` and PutIceberg

Iceberg column `ingested_at` is **TIMESTAMP**. PutIceberg fails if the record field is a plain **string** like `2026-11-22 17:08:54`:

```text
FieldConversionException: … ingested_at … to LocalDateTime … NumberFormatException
```

**Fix:**

1. **UpdateAttribute:** `ingested_at` = `${now():toNumber()}` (UTC epoch millis; not `format("yyyy-MM-dd HH:mm:ss")`).
2. **Avro / JsonTreeReader:** `ingested_at` as `long` with logical type `timestamp-millis` (see `.avsc` files).
3. Restart **JsonTreeReader** after pasting the updated schema.

AttributesToJSON may still emit millis as a JSON string (`"1732296534000"`); JsonTreeReader coerces that into `long` for this schema.

## Validate locally

```bash
python3 -c "import json; json.load(open('nifi-processors/schemas/das_cable_spectrogram_metadata.avsc'))"
```