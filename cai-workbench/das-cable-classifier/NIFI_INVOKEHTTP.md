# NiFi → DAS cable classifier (CML Model Service)

Same gateway pattern as [lidar-vessel-classifier/NIFI_INVOKEHTTP.md](../lidar-vessel-classifier/NIFI_INVOKEHTTP.md), but the response has **`threat_type`** / **`predictions[]`**, not `vessel_type`.

## Request body (after PutCDPObjectStore)

Pass the simulator primary segment so the model returns **flat** fields for EvaluateJsonPath (no array index in JsonPath):

```json
{"request":{"uri":"${hadoop.file.url}","segment_index":"${das.segment.index}"}}
```

(`s3_uri` / `s3a://…` are accepted; model normalizes `s3a` → `s3`.)

## EvaluateJsonPath (replace LiDAR paths)

**Do not use** `$.response.vessel_type` or LiDAR-style single-segment paths on a multi-segment DAS response.

**Do not put Expression Language inside JsonPath** — paths like `$.response.predictions[${das.segment.index}].confidence` are **rejected** by EvaluateJsonPath (invalid expression). Array indices must be literals if you use `predictions[n]` at all.

Use **static** paths on rollup fields inside `response` (model sets these from `segment_index` in the request body):

| Property (attribute = Iceberg column) | JsonPath |
|--------------------------------------|----------|
| `model_status_code` | `$.StatusCode` |
| `predicted_threat_type` | `$.response.threat_type` |
| `model_confidence` | `$.response.confidence` |

Redeploy `predict.py` after pull so multi-segment batches always include `threat_type` / `confidence` on `response` (not only `predictions[]`).

## UpdateAttribute → `das_cable_spectrogram_metadata`

JsonPath can write **Iceberg names directly** (`predicted_threat_type`, `model_confidence`). Then merge with simulator columns before `AttributesToJSON`:

```
hadoop_file_url        ${hadoop.file.url}
segment_index          ${das.segment.index}
cable_asset            ${das.cable.asset}
…
predicted_threat_type  ${predicted_threat_type}
model_confidence       ${model_confidence}
ingested_at            ${now():format("yyyy-MM-dd HH:mm:ss", "UTC")}
```

Full map: [`nifi-processors/docs/DAS_NIFI_METADATA_MAP.md`](../../nifi-processors/docs/DAS_NIFI_METADATA_MAP.md).

## Six rows per HDF5 file

`SplitJson` with `$.response.predictions`, then per-child JsonPath `$.threat_type` / `$.confidence` and `segment_index` from `${segment:substring(4):toNumber()}` — see metadata map **Mode B**.