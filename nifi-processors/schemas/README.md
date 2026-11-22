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

## Validate locally

```bash
python3 -c "import json; json.load(open('nifi-processors/schemas/das_cable_spectrogram_metadata.avsc'))"
```