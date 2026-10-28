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

### Option B — InvokeHTTP

1. **POST** to the model URL  
2. Body: `{"records": ${json.array:...}}`  — often built with `MergeContent` + `AttributesToJSON` or a small script  
3. Use **EvaluateJsonPath** or **JoltTransformJSON** if the platform wraps the array; with `predict_array` the body should start with `[`

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
