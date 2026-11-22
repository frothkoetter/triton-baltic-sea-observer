# NiFi record schemas (JsonTreeReader)

Avro JSON schemas for **JSONTreeReader** → **PutIceberg** / record processors.  
Field names match **UpdateAttribute** column names and **AttributesToJSON** lists (not dotted NiFi attributes).

| File | Iceberg table | Ontology |
|------|----------------|----------|
| [`lidar_simulation_metadata.avsc`](./lidar_simulation_metadata.avsc) | `defense.lidar_simulation_metadata` | [`ontology/mappings/lidar_simulation_metadata.yaml`](../../ontology/mappings/lidar_simulation_metadata.yaml) |
| [`das_cable_spectrogram_metadata.avsc`](./das_cable_spectrogram_metadata.avsc) | `defense.das_cable_spectrogram_metadata` | [`ontology/mappings/das_cable_spectrogram_metadata.yaml`](../../ontology/mappings/das_cable_spectrogram_metadata.yaml) |

## JsonTreeReader (controller service)

Match **`LiDar Simulation`** controller **`JsonTreeReader-lidar`** in `proj_triton_showcase-version-2.json`:

| Property | LiDAR showcase | DAS (recommended) |
|----------|----------------|-------------------|
| **Timestamp Format** | `yyyy-MM-dd HH:mm:ss.SSS` | **same** |
| **Schema Access Strategy** | `infer-schema` | `infer-schema` or `schema-text-property` (paste DAS `.avsc`) |
| **Schema Text** | `${avro.schema}` | `${das.avro.schema}` or paste `.avsc` |
| **Schema Application Strategy** | `SELECTED_PART` | `SELECTED_PART` |
| **Allow Comments** | `false` | `false` |

## DAS AttributesToJSON (suggested list)

```
hadoop_file_url,segment_index,cable_asset,cable_id,cable_name,chainage_m,segment_lat,segment_lon,threat_class,predicted_threat_type,model_confidence,channel_count,window_seconds,fs_hz,filename,ingested_at
```

Optional: `spectral_peak_hz`, `energy_rms` (already in Avro schema).

## MergeContent batches

If merged content is a **JSON array** `[{...},{...}]`, use **SplitJson** on `$.*` before the reader, or configure the reader for array (schema root `array` of the record type) per your NiFi version.

## `ingested_at` and PutIceberg (LiDAR parity)

**LiDar Simulation** `UpdateAttribute`:

```text
ingested_at = ${now():toNumber():toDate():format("yyyy-MM-dd HH:mm:ss.SSS")}
```

Use the **same expression** on DAS. Do **not** use `yyyy-MM-dd HH:mm:ss` without `.SSS` — PutIceberg → `LocalDateTime` fails unless the string matches **JsonTreeReader → Timestamp Format**.

**JsonTreeReader** must set **Timestamp Format** = `yyyy-MM-dd HH:mm:ss.SSS` (showcase `JsonTreeReader-lidar`).

Avro field type is **`string`** for `ingested_at` (not `long` / `timestamp-millis`), same as inferred LiDAR metadata JSON.

## Validate locally

```bash
python3 -c "import json; json.load(open('nifi-processors/schemas/das_cable_spectrogram_metadata.avsc'))"
```