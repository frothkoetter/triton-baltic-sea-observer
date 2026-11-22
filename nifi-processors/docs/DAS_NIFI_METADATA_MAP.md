# DAS → `iceberg.defense.das_cable_spectrogram_metadata` (NiFi)

Map **FlowFile attributes** from `DasCableAcousticSimulator` (+ object store + optional classifier) to **Iceberg column names** in **UpdateAttribute** before `AttributesToJSON` / `PutDatabaseRecord` / JDBC.

Ontology reference: [`ontology/mappings/das_cable_spectrogram_metadata.yaml`](../../ontology/mappings/das_cable_spectrogram_metadata.yaml).

## Flow order (typical)

1. **DasCableAcousticSimulator** → HDF5 + `das.*` / `filename` / `latitude` / `longitude`
2. **PutCDPObjectStore** → sets **`hadoop.file.url`** (or `s3.fullpath`) on success
3. **UpdateAttribute** (this table) → rename / add columns
4. Optional: **InvokeHTTP** → CML `das-cable-classifier` → `predicted_threat_type`, `model_confidence`
5. **PutDatabaseRecord** / Trino sink → one metadata row

**Note:** The simulator emits **one FlowFile per HDF5 batch** with **one primary** `das.segment.*` (highest-priority threat segment). For **six rows per batch** (one per segment inside the HDF5), add a **Split** / custom processor that reads `json_meta` from HDF5 or explodes `batch_meta["segments"]` — not covered here.

---

## UpdateAttribute — Property = **column name**, Value = **Expression Language**

Configure **UpdateAttribute**: “Delete attributes” = `false`, “Store state” = optional.

| Property (Iceberg column) | Value (Expression Language) | Source |
|---------------------------|-----------------------------|--------|
| `hadoop_file_url` | `${hadoop.file.url}` | After **PutCDPObjectStore** success (or `${s3a.object.uri}` if your Put uses that name) |
| `segment_index` | `${das.segment.index}` | Simulator |
| `cable_asset` | `${das.cable.asset}` | Simulator |
| `cable_id` | `${das.cable.id}` | Simulator |
| `cable_name` | `${das.cable.name}` | Simulator |
| `chainage_m` | `${das.chainage_m}` | Simulator |
| `segment_lat` | `${das.segment.lat}` | Simulator |
| `segment_lon` | `${das.segment.lon}` | Simulator |
| `threat_class` | `${das.threat_class}` | Simulator (primary segment GT label) |
| `channel_count` | `${das.channel.count}` | Simulator |
| `window_seconds` | `${das.window.seconds}` | Simulator |
| `fs_hz` | `${das.fs_hz}` | Simulator |
| `filename` | `${filename}` | Simulator (use **v1.2.1+** for unique names on Put) |
| `ingested_at` | `${now():format("yyyy-MM-dd HH:mm:ss", "UTC")}` | NiFi (partition key) |
| `predicted_threat_type` | `${das.predicted.threat_type}` | Optional — classifier leg |
| `model_confidence` | `${das.model.confidence}` | Optional — classifier leg |
| `spectral_peak_hz` | *(leave empty or `${das.spectral.peak_hz}`)* | Optional ML features |
| `energy_rms` | *(leave empty or `${das.energy.rms}`)* | Optional ML features |

### Attributes you have today — direct mapping

Your FlowFile already matches most ontology fields (no rename needed if Put/JDBC reads `das.*` names). Use this block only when the sink expects **SQL column names** as attribute keys:

| Your attribute | Maps to column |
|----------------|----------------|
| `das.cable.asset` | `cable_asset` |
| `das.cable.id` | `cable_id` |
| `das.cable.name` | `cable_name` |
| `das.segment.index` | `segment_index` |
| `das.segment.lat` | `segment_lat` |
| `das.segment.lon` | `segment_lon` |
| `das.chainage_m` | `chainage_m` |
| `das.threat_class` | `threat_class` |
| `das.channel.count` | `channel_count` |
| `das.window.seconds` | `window_seconds` |
| `das.fs_hz` | `fs_hz` |
| `filename` | `filename` |
| `latitude` / `longitude` | batch centroid only — **not** table columns (use `segment_lat` / `segment_lon`) |
| `das.centroid.lat` / `das.centroid.lon` | not stored in table (optional custom column) |
| `das.schema` | metadata only |
| `das.threat.classes` | batch summary only (comma list) |
| `mime.type`, `path`, `uuid` | not persisted to this table |

### Copy-paste (column-named attributes)

In **UpdateAttribute**, add one property per row (Property = left, Value = right):

```
hadoop_file_url     ${hadoop.file.url}
segment_index       ${das.segment.index}
cable_asset         ${das.cable.asset}
cable_id            ${das.cable.id}
cable_name          ${das.cable.name}
chainage_m          ${das.chainage_m}
segment_lat         ${das.segment.lat}
segment_lon         ${das.segment.lon}
threat_class        ${das.threat_class}
channel_count       ${das.channel.count}
window_seconds      ${das.window.seconds}
fs_hz               ${das.fs_hz}
filename            ${filename}
ingested_at         ${now():format("yyyy-MM-dd HH:mm:ss", "UTC")}
```

Run this **after** PutCDPObjectStore so `hadoop.file.url` exists. If Put fails, route to failure — do not insert metadata without `hadoop_file_url`.

---

## Unique `filename` (PutCDPObjectStore)

If Put fails with *“file with same name already exists”*, deploy **`DasCableAcousticSimulator` 1.2.1+** (`das_*_{UTC}_{batch_id}.h5`) or prepend in UpdateAttribute:

```
filename    ${filename:replace('.h5','')}_${now():format('yyyyMMddHHmmss')}.h5
```

---

## Verify in Trino

```sql
SELECT hadoop_file_url, segment_index, cable_asset, threat_class, segment_lat, segment_lon, filename, ingested_at
FROM iceberg.defense.das_cable_spectrogram_metadata
ORDER BY ingested_at DESC
LIMIT 10;
```

NATO UI **DAS Kabel** tab: `GET /api/das-cable/segments` reads the same table.