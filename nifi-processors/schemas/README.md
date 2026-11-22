# NiFi record schemas (JsonTreeReader)

Avro JSON schemas for **JSONTreeReader** → **PutIceberg** / record processors.  
Field names match **UpdateAttribute** column names and **AttributesToJSON** lists (not dotted NiFi attributes).

| File | Iceberg table | Ontology |
|------|----------------|----------|
| [`lidar_simulation_metadata.avsc`](./lidar_simulation_metadata.avsc) | `defense.lidar_simulation_metadata` | [`ontology/mappings/lidar_simulation_metadata.yaml`](../../ontology/mappings/lidar_simulation_metadata.yaml) |
| [`das_cable_spectrogram_metadata.avsc`](./das_cable_spectrogram_metadata.avsc) | `defense.das_cable_spectrogram_metadata` | [`ontology/mappings/das_cable_spectrogram_metadata.yaml`](../../ontology/mappings/das_cable_spectrogram_metadata.yaml) |

## JsonTreeReader (controller service)

### Path A — same as LiDAR (formatted timestamp string)

From **`JsonTreeReader-lidar`** in `proj_triton_showcase-version-2.json`:

| Property | Value |
|----------|--------|
| **Schema Access Strategy** | **`infer-schema`** (not “Use Schema Text Property”) |
| **Timestamp Format** | `yyyy-MM-dd HH:mm:ss.SSS` |
| **Schema Application Strategy** | `SELECTED_PART` |

**UpdateAttribute:** `ingested_at` = `${now():toNumber():toDate():format("yyyy-MM-dd HH:mm:ss.SSS")}`

Inference + Timestamp Format produce a **Record TIMESTAMP** field; PutIceberg can write Iceberg `TIMESTAMP`.

### Path B — explicit Schema Text (your DAS `.avsc`)

| Property | Value |
|----------|--------|
| **Schema Access Strategy** | **Use 'Schema Text' Property** |
| **Schema Text** | Paste `das_cable_spectrogram_metadata.avsc` |
| **Timestamp Format** | optional (not used for `long` millis) |

**UpdateAttribute:** `ingested_at` = `${now():toNumber()}` (UTC epoch **milliseconds** as attribute text).

The `.avsc` declares `ingested_at` as **`long`** + `timestamp-millis`. AttributesToJSON may emit `"1732296534967"` in quotes; JsonTreeReader coerces that to `long`.

### Why explicit `string` + format fails PutIceberg

With **Schema Text** and `"ingested_at": ["null", "string"]`, the Record field stays **STRING**. PutIceberg maps Iceberg `TIMESTAMP` using a converter that tries **numeric** parsing first → `NumberFormatException` on `"2026-11-22 17:16:53.967"` even when **Timestamp Format** is set on the reader. That format only applies when the **schema type** is Timestamp (infer-schema or `long` millis), not plain string.

## DAS AttributesToJSON (suggested list)

```
hadoop_file_url,segment_index,cable_asset,cable_id,cable_name,chainage_m,segment_lat,segment_lon,threat_class,predicted_threat_type,model_confidence,channel_count,window_seconds,fs_hz,filename,ingested_at
```

Optional: `spectral_peak_hz`, `energy_rms` (already in Avro schema).

## MergeContent batches

If merged content is a **JSON array** `[{...},{...}]`, use **SplitJson** on `$.*` before the reader, or configure the reader for array (schema root `array` of the record type) per your NiFi version.

## `ingested_at` quick pick

| You use | UpdateAttribute `ingested_at` | JsonTreeReader |
|---------|-------------------------------|----------------|
| **Infer schema** (LiDAR-like) | `${now():toNumber():toDate():format("yyyy-MM-dd HH:mm:ss.SSS")}` | **Infer Schema**, Timestamp Format `yyyy-MM-dd HH:mm:ss.SSS` |
| **Schema Text** (`.avsc` file) | `${now():toNumber()}` | **Use Schema Text**, `ingested_at` = `long` + `timestamp-millis` in schema |

## Validate locally

```bash
python3 -c "import json; json.load(open('nifi-processors/schemas/das_cable_spectrogram_metadata.avsc'))"
```