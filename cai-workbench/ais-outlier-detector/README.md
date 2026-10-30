# AIS outlier detection (Cloudera AI Workbench)

Replace the LLM “experienced analyst” step with a **deployable CML model** that ingests **~15 minutes of AIS NDJSON** from `ShipSimulationRoutesGemini` and returns **only a JSON array** of outlier records.

## Outlier types (rules)

| Type | Detection |
|------|-----------|
| **Rendezvous** | Two MMSIs within **5 km** at the same `Event_Timestamp` |
| **Misreported Status** | e.g. Moored/Anchored with SOG > 3 kn, or implausible SOG |
| **Erratic Status Changes** | ≥ 2 distinct status changes per MMSI in the batch |
| **Inconsistent Maneuvering** | Large course or speed swings within the batch |

Each hit includes: `MMSI`, `Event_Timestamp`, `Longitude`, `Latitude`, `Speed`, `Course`, `Status`, `Destination`, `Outlier_Type`, `Reason`, `Recommendation`, `Severity_Level` (0–100).

**ML role:** `fit.py` trains an **IsolationForest** on synthetic normal traffic to **calibrate severity** (rules still decide *what* is an outlier).

## Workbench layout

| File | Role |
|------|------|
| `cdsw-build.sh` | Install deps on model build |
| `fit.py` | Experiment → writes `model.pkl` |
| `predict.py` | Deploy **`detect_outliers`** or **`predict`** (returns `{"body": "[...]"}`) |
| `predict_array.py` | Deploy **`predict_array`** → raw JSON **array** response |
| `run_local.py` | Local CLI test without CML |
| `src/rules.py` | Rule engine |
| `src/detect.py` | Rules + ML severity |

**Local git data:** NDJSON fixtures under [`../data/ais/`](../data/ais/), e.g.:

```bash
cat ../data/ais/sample_rendezvous.ndjson | python3 run_local.py
```

## Train (Workbench experiment)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
bash cdsw-build.sh
python3 fit.py
```

## Deploy model

1. Build with `cdsw-build.sh`.
2. Deploy **`predict_array.py`** with entry function **`predict_array`** (preferred for NiFi — response is a JSON array).
3. Or deploy **`predict.py`** with **`detect_outliers`** (array) or **`predict`** (wrapper with `body` string).

### Request body

```json
{
  "records": [
    {
      "MMSI": 123456000,
      "Event_Timestamp": "2026-10-28 12:00:00.000",
      "Latitude": 54.32,
      "Longitude": 10.12,
      "Speed": 8.5,
      "Course": 90.0,
      "Status": "Underway using engine",
      "Destination": "Rostock"
    }
  ]
}
```

Or `{"ndjson": "<one JSON object per line>"}`.

### Response

JSON array only (no prose), e.g.:

```json
[
  {
    "MMSI": 123456006,
    "Event_Timestamp": "2025-10-14 17:05:54.044",
    "Longitude": 12.04434,
    "Latitude": 53.98346,
    "Speed": 34.0,
    "Course": 180.0,
    "Status": "Moored",
    "Destination": "Primorsk",
    "Outlier_Type": "Misreported Status",
    "Reason": "Status 'Moored' but SOG 34.0 kn indicates movement",
    "Recommendation": "Cross-check AIS status against speed/position; request corrected AIS or visual confirm",
    "Severity_Level": 65
  }
]
```

## NiFi integration

### Option A — Python processor (this repo)

Upload `nifi-processors/upload/AisOutlierCmlInvoke.py` to NiFi Python extensions.

- **CML Model URL:** your deployed model HTTPS URL  
- **CDP API Key:** bearer token if required  
- Input: FlowFile with 15-minute AIS **NDJSON** (one ship event per line) or JSON array  
- Output: **only** the outlier JSON array in `flowfile-content`

Place after the AIS aggregate / `MergeContent` that builds the 15-minute batch; remove or bypass the LLM `InvokeHTTP` + prompt processor.

### Option B — InvokeHTTP (standard processor)

Use this when you **do not** want a custom Python extension. Point **InvokeHTTP** at the same **`predict_array`** model URL as Option A.

#### Model endpoint

Deploy **`predict_array.py`** with entry function **`predict_array`**. The HTTP response body should be a **JSON array** (first non-whitespace character `[`). No LLM-style header or footer.

#### Request body shape

The model expects a JSON object (CML passes it as the `args` dict):

```json
{"records": [ { "MMSI": 123, "Event_Timestamp": "...", ... }, ... ]}
```

Alternatively `{"ndjson": "<one JSON object per line>"}` (same as `AisOutlierCmlInvoke`).

#### NiFi flow (typical)

```text
[15 min AIS batch FlowFile] → [Wrap as {"records": ...}] → InvokeHTTP → [optional validate] → downstream (PutS3 / PutDatabase / Merge)
```

1. **Upstream** — `MergeContent` (or `MergeRecord`) on a 15-minute window so one FlowFile holds the batch (NDJSON lines or a JSON array from the sim).

2. **Wrap payload** — choose one:

   | Input content | Processor | Action |
   |---------------|-----------|--------|
   | JSON array `[{...},...]` | **ReplaceText** | Replacement: `{"records": $1}` with search `^(\[.*\])$` (single-line array) or read full content |
   | NDJSON (one object per line) | **ExecuteScript** (Groovy) or Option A processor | Parse lines, `new groovy.json.JsonBuilder([records: list]).toString()` |
   | Already wrapped | *(none)* | Body is already `{"records":...}` |

   Minimal Groovy (ExecuteScript, property **Script Body**), FlowFile content = NDJSON:

   ```groovy
   def flowFile = session.get()
   if (!flowFile) return
   def text = new String(session.read(flowFile).bytes, 'UTF-8').trim()
   def records = text.startsWith('[')
       ? new groovy.json.JsonSlurper().parseText(text)
       : text.readLines().findAll { it.trim() }.collect { new groovy.json.JsonSlurper().parseText(it) }
   def body = new groovy.json.JsonBuilder([records: records]).toString()
   flowFile = session.write(flowFile, { out -> out.write(body.getBytes('UTF-8')) } as OutputStreamCallback)
   session.transfer(flowFile, REL_SUCCESS)
   ```

3. **InvokeHTTP** — suggested properties:

   | Property | Value |
   |----------|--------|
   | HTTP Method | `POST` |
   | Remote URL | `https://<your-cai-model-host>/...` (deployed `predict_array` URL) |
   | Content-Type | `application/json` |
   | Send Message Body | `true` |
   | Put Response Body in Attribute | `false` |
   | Output Response Regardless | `true` (or route on `Response Code`) |
   | Connection Timeout / Read Timeout | e.g. `30 sec` / `120 sec` (large batches) |

   **Authorization (CDP):** add a dynamic property on InvokeHTTP:

   - Name: `Authorization`
   - Value: `Bearer ${cdp.api.key}` (store the token in a **Parameter Context**, not in the flow JSON)

   Use an **StandardRestrictedSSLContextService** controller service if the cluster requires explicit TLS trust.

4. **Response** — with **`predict_array`**, the response FlowFile **content** is the outlier array. Connect **Response** to success; set `mime.type=application/json` via **UpdateAttribute** if needed.

   If you deployed **`predict`** / **`detect_outliers`** with a wrapper instead, the body may look like `{"body":"[...]"}` or `{"prediction":[...]}`. Then add **EvaluateJsonPath** (content → attribute) or **JoltTransformJSON** to extract the array before storage.

5. **Replace LLM step** — remove the old **InvokeHTTP** to the chat/completions URL and any **UpdateAttribute** that sets the analyst prompt. Reuse the same success/failure relationships into your existing “Risk Classification” / CDW ingest chain; only the HTTP target and request body change.

6. **Errors** — route **Original** / non-2xx to **RetryFlowFile** or failure; log `invokehttp.status.code` and response body (truncated) for model errors.

#### curl equivalent (sanity check)

```bash
curl -sS -X POST "$MODEL_URL" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $CDP_API_KEY" \
  -d '{"records":[{"MMSI":123456006,"Event_Timestamp":"2025-10-14 17:05:54.044","Latitude":53.98346,"Longitude":12.04434,"Speed":34.0,"Course":180.0,"Status":"Moored","Destination":"Primorsk"}]}'
```

Stdout should start with `[` and contain only outlier objects (empty array `[]` if none).

## Local test

```bash
cd cai-workbench/ais-outlier-detector
pip install -r requirements.txt
python3 fit.py
python3 -m unittest discover -s tests -q
head -2 your_ais.ndjson | python3 run_local.py
```

## CDW table

Maps to `vessel_outliers` in `cdw-analyse/create_db_tables_*.sql`.

## Proposed model choice

| Layer | Method | Why |
|-------|--------|-----|
| Detection | **Deterministic rules** | Matches analyst categories exactly; auditable; no hallucinated reasons |
| Severity | **IsolationForest** (sklearn) | Trainable in Workbench; adjusts 0–100 from movement/context features |
| Serving | **CML `@cml_model`** | Same pattern as `lidar-vessel-classifier`; HTTPS from NiFi |

Future: replace or augment rules with a supervised classifier once labeled outliers exist in Impala (`vessel_outliers`).
