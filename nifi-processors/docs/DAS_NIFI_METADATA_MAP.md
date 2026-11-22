# DAS → `iceberg.defense.das_cable_spectrogram_metadata` (NiFi)

Map **FlowFile attributes** from `DasCableAcousticSimulator` (+ object store + optional classifier) to **Iceberg column names** in **UpdateAttribute** before `AttributesToJSON` / `PutDatabaseRecord` / JDBC.

Ontology reference: [`ontology/mappings/das_cable_spectrogram_metadata.yaml`](../../ontology/mappings/das_cable_spectrogram_metadata.yaml).

## Flow order (typical)

1. **DasCableAcousticSimulator** → HDF5 + `das.*` / `filename` / `latitude` / `longitude`
2. **PutCDPObjectStore** → sets **`hadoop.file.url`** (or `s3.fullpath`) on success
3. **UpdateAttribute** (this table) → rename / add columns
4. Optional: **InvokeHTTP** → CML `das-cable-classifier` → **EvaluateJsonPath** → Iceberg columns as attributes → **UpdateAttribute**
5. **PutDatabaseRecord** / Trino sink → one metadata row (or **SplitJson** → six rows per HDF5 batch)

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

**After classifier (EvaluateJsonPath):** map ML into the same attribute names the Iceberg table uses:

```
predicted_threat_type   ${predicted_threat_type}
model_confidence        ${model_confidence}
```

(or keep `das.predicted.threat_type` / `das.model.confidence` from JsonPath and reference those in UpdateAttribute — see below.)

---

## EvaluateJsonPath (after InvokeHTTP → `response` relationship)

Wire **InvokeHTTP** `response` → **EvaluateJsonPath** → **UpdateAttribute** → metadata JSON.

**Processor settings** (defaults are fine):

| Setting | Value |
|---------|--------|
| **Destination** | `flowfile-attribute` |
| **Return Type** | `auto-detect` |
| **Path Not Found Behavior** | `ignore` |
| **Null Value Representation** | `empty string` |

**Remove LiDAR property names** (`predicted_vessel_type`, `$.response.vessel_type`, `$.response.confidence` on a vessel field).

### CML Model Service body shape (6 segments)

InvokeHTTP response content (attributes on the same FlowFile as `das.segment.index`, `hadoop.file.url`, …):

```json
{
  "success": true,
  "StatusCode": 200,
  "response": {
    "count": 6,
    "predictions": [
      {
        "segment": "seg_000",
        "threat_type": "cable_strain_anomaly",
        "confidence": 0.578125,
        "ground_truth": "vessel_passage",
        "probabilities": { "…": 0.0 }
      }
    ],
    "threat_type": "cable_strain_anomaly",
    "confidence": 0.578125,
    "primary_segment": "seg_000"
  }
}
```

`response.threat_type` / `response.confidence` are the **highest-confidence** segment (rollup for a single metadata row). Prefer **index-aligned** ML when `das.segment.index` is the simulator’s primary segment.

### Mode A — one Iceberg row per batch (primary segment)

**InvokeHTTP** request body must include the primary segment index (matches `das.segment.index` on the FlowFile):

```json
{"request":{"uri":"${hadoop.file.url}","segment_index":"${das.segment.index}"}}
```

The model copies that segment’s ML result onto `response.threat_type` and `response.confidence` so NiFi can use **literal** JsonPaths (EvaluateJsonPath does **not** accept Expression Language inside paths such as `$.response.predictions[${das.segment.index}].confidence` — you will get “expression was not valid”).

**EvaluateJsonPath** — Property = attribute name (Iceberg column), Value = JsonPath:

| Property (attribute) | JsonPath |
|----------------------|----------|
| `model_status_code` | `$.StatusCode` |
| `predicted_threat_type` | `$.response.threat_type` |
| `model_confidence` | `$.response.confidence` |

**UpdateAttribute** before `AttributesToJSON` — add or confirm:

```
predicted_threat_type    ${predicted_threat_type}
model_confidence         ${model_confidence}
```

(Plus the simulator / PutCDP columns from the table above.)

### Mode B — one Iceberg row **per segment** (full batch)

1. **InvokeHTTP** `response` → **EvaluateJsonPath** (optional): `model_status_code` → `$.StatusCode`
2. **SplitJson** — **JsonPath Expression** = `$.response.predictions`
3. On each split child, **EvaluateJsonPath** (content is one prediction object):

| Property | JsonPath |
|----------|----------|
| `predicted_threat_type` | `$.threat_type` |
| `model_confidence` | `$.confidence` |

4. **UpdateAttribute** on each child:

```
segment_index            ${segment:substring(4):toNumber()}
threat_class             ${ground_truth}
```

(`ground_truth` only if you add it via JsonPath: Property `threat_class` ← `$.ground_truth`, or keep simulator `threat_class` from parent via attribute copy.)

5. Copy parent attributes (`hadoop_file_url`, `cable_asset`, `filename`, …) — **SplitJson** retains parent attributes by default.

6. **AttributesToJSON** / **MergeContent** / **PutIceberg** as for LiDAR metadata.

### Iceberg column cheat sheet (classifier leg)

| Iceberg column | Source after JsonPath |
|----------------|------------------------|
| `predicted_threat_type` | attribute `predicted_threat_type` |
| `model_confidence` | attribute `model_confidence` (double) |
| `threat_class` | simulator `das.threat_class` or per-segment `ground_truth` |
| `segment_index` | `das.segment.index` or split + `segment_index` |

See also: [`cai-workbench/das-cable-classifier/NIFI_INVOKEHTTP.md`](../../cai-workbench/das-cable-classifier/NIFI_INVOKEHTTP.md).

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