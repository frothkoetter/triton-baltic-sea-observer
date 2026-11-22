# DAS cable threat classifier (Cloudera AI)

Klassifiziert **Seekabel-DAS-Spektrogramme** (HDF5 aus NiFi `DasCableAcousticSimulator`) nach Ereignistyp — Ankerschleifen, CUI-Sabotage-Tapping, Trawler, Schiffspassage, etc.

Spiegelbild von [`lidar-vessel-classifier`](../lidar-vessel-classifier/): `fit.py` trainiert, `predict.py` scored HDF5 oder Feature-Vektoren.

## Datenquelle

| Komponente | Pfad |
|------------|------|
| NiFi Prozessor | `nifi-processors/upload/DasCableAcousticSimulator.py` |
| Engine + HDF5 | `nifi-processors/upload/das_acoustic_engine.py` |
| Kabel (NiFi Property **Cable Asset**) | `BALTICCONNECTOR` (Default), `NORD_STREAM_1`, `NORD_STREAM_2`, `PCABLE_BSH_1`–`4` — WKT aus `ostsee_infrastruktur` |

Schema: `triton/das-cable/v1` — Gruppe `segments/seg_XXX` mit `channels`, `spectrogram`, `frequencies_hz`, `threat_class` (Ground Truth für Training).

## Schnellstart (Session / lokal)

```bash
cd cai-workbench/das-cable-classifier
pip install -r requirements.txt
python3 train.py --cable BALTICCONNECTOR --batches 80
python3 scripts/validate_nifi_ml.py
```

`validate_nifi_ml.py` erzeugt ein NiFi-kompatibles HDF5-Batch, extrahiert Features wie der Flow und vergleicht Predictions mit synthetischer Ground Truth.

## CML

| Artefakt | Rolle |
|----------|--------|
| `fit.py` | Experiment — Training |
| `predict.py` | Model Deployment — `predict({ "hdf5_bytes_base64": "..." })` |
| `model.pkl` | Gesynct von Training oder Build (`bundled/model.pkl` → Root) |
| `cdsw-build.sh` | Engine Build — `pip install` + Kopie `bundled/model.pkl` |
| `bundled/model.pkl` | Vorgefertigtes Modell im Repo (Deploy ohne Experiment) |

**Model deploy:** Projektordner `cai-workbench/das-cable-classifier`, Entry **`predict.py`**, Input function **`predict`**. Build engine = `cdsw-build.sh`.

Fehler `FileNotFoundError: ... /home/cdsw/model.pkl` — **alter** `predict.py` (ohne `bundled/`-Fallback). Im Projekt prüfen: `head -3 predict.py` enthält `_model_pkl_path`. Dann **Engine neu bauen** (`cdsw-build.sh`) und **Model neu deployen** (nicht nur Session). `model.pkl` ist gitignored — Build kopiert `bundled/model.pkl`. Logs zeigen `das-cable-classifier: loading …` bei Erfolg.

### predict API

```json
{
  "hdf5_bytes_base64": "<base64 von NiFi FlowFile (HDF5)>"
}
```

Antwort: `threat_type`, `confidence`, `probabilities`, optional `ground_truth` wenn im HDF5.

## Klassen

`ambient`, `vessel_passage`, `trawler_scrape`, `anchor_drag`, `cui_sabotage_tap`, `cable_strain_anomaly`