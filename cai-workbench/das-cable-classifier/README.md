# DAS cable threat classifier (Cloudera AI)

Klassifiziert **Seekabel-DAS-Spektrogramme** (HDF5 aus NiFi `DasCableAcousticSimulator`) nach Ereignistyp — Anker, CUI, Trawler, Schiffspassage, Walruf, U-Boot-Tonal usw.

Spiegelbild von [`lidar-vessel-classifier`](../lidar-vessel-classifier/): `train.py` / `fit.py` trainieren, `predict.py` scored HDF5 oder Feature-Vektoren in CML **Model Deployment**.

## Datenquelle

| Komponente | Pfad |
|------------|------|
| NiFi Prozessor | `nifi-processors/upload/DasCableAcousticSimulator.py` |
| Engine + HDF5 | `nifi-processors/upload/das_acoustic_engine.py` |
| Kabel (NiFi Property **Cable Asset**) | `BALTICCONNECTOR` (Default), `NORD_STREAM_1`, `NORD_STREAM_2`, `PCABLE_BSH_1`–`4`, `RANDOM` |

Schema: `triton/das-cable/v1` — HDF5-Gruppe `segments/seg_XXX` mit `spectrogram`, `frequencies_hz`, Attribut **`threat_class`** (Ground Truth).

End-to-end NiFi, Iceberg, NATO UI: [`docs/DAS_NIFI_AND_UI_FORMAT.md`](../../docs/DAS_NIFI_AND_UI_FORMAT.md).

---

## Model artifacts (Git vs Workspace)

| Datei | In Git? | Rolle |
|-------|---------|--------|
| **`bundled/model.pkl`** | **ja** | Standardmodell für Deploy (aktuell **11 Klassen**, sklearn `Pipeline`) |
| **`bundled/label_names.json`** | ja | Label-Liste (muss zu `clf.classes_` passen) |
| **`model.pkl`** (Projekt-Root) | nein (`.gitignore`) | Laufzeit-Kopie für CML Build / Session; `cdsw-build.sh` kopiert aus `bundled/` |
| **`artifacts/`** | nein | Training: `model.joblib`, `training_meta.json`, `training_rows.csv` |

**Trainiertes Modell ins Repo übernehmen** (nach Session oder Upload):

```bash
cd cai-workbench/das-cable-classifier
cp -f /pfad/zu/model.pkl bundled/model.pkl
cp -f bundled/model.pkl model.pkl
python3 -c "import joblib; c=list(joblib.load('bundled/model.pkl').named_steps['clf'].classes_); import json; json.dump(c, open('bundled/label_names.json','w'), indent=2)"
git add bundled/model.pkl bundled/label_names.json
```

Dann `git push` → auf CML `git pull` → Model **neu deployen** (siehe unten).

---

## Threat classes (11)

Abgestimmt mit `das_acoustic_engine.THREAT_CLASSES` und `src/labels.py`:

`ambient`, `vessel_passage`, `trawler_scrape`, `anchor_drag`, `anchor_slip`, `anchor_drop_tow`, `cui_sabotage_tap`, `cable_strain_anomaly`, `uboat_acoustic`, `whale_vocalization`, `hard_beep`

**Trino:** Spalte **`threat_class`** = Simulator-GT. **`predicted_threat_type`** = Output dieses Modells (NiFi InvokeHTTP). NATO UI Tab **#das**: Filter **Ground truth** vs **ML** — siehe DAS-Format-Doku.

Ältere Bundles mit nur **6** Klassen neigten zu ML-Bias (`cable_strain_anomaly`). Repo-`bundled/model.pkl` auf `main` enthält **11** Klassen (RandomForest, 200 Bäume).

---

## Schnellstart (CML Session)

Voraussetzung: **volles Git-Repo** (`nifi-processors/upload` für `das_acoustic_engine`).

```bash
cd ~
git pull origin main
cd cai-workbench/das-cable-classifier
python3 -m pip install -r requirements.txt

# Synthetisches Training (NiFi-kompatible Features)
python3 train.py --cable BALTICCONNECTOR --batches 120 --seed 42
python3 scripts/validate_nifi_ml.py
```

Ergebnis: `model.pkl`, `bundled/model.pkl`, `artifacts/training_meta.json`.

---

## CML Experiment

| Feld | Wert |
|------|------|
| Script | `cai-workbench/das-cable-classifier/fit.py` |
| Kernel | Python 3 |

`fit.py` wechselt automatisch ins Klassifier-Verzeichnis (nicht nur `/home/cdsw`).

---

## Model Deployment (predict)

| Feld | Wert |
|------|------|
| Projektordner | Repo-Root oder `cai-workbench/das-cable-classifier` |
| Entry | **`predict.py`** (Datei — **kein** Notebook) |
| Input function | **`predict`** |
| Build | `bash cdsw-build.sh` |

**Nach `git pull` mit neuem `bundled/model.pkl`:**

```bash
cd cai-workbench/das-cable-classifier
git pull origin main
cp -f bundled/model.pkl model.pkl
bash cdsw-build.sh
# CML UI → Models → Rebuild engine + Redeploy
```

**Model Files** (wenn Build-Context klein ist): `predict.py`, `cml_root.py`, `src/`, `requirements.txt`, `bundled/model.pkl`, `bundled/label_names.json`.

CML kopiert manchmal kein `bundled/` (Build-Log `transferring context: 3kB`). Dann fällt `cdsw-build.sh` auf `scripts/build_model_for_deploy.py` zurück (schnelles **synthetisches** 11-Klassen-Modell — für Demo, nicht für Produktion). **Lösung:** `bundled/` explizit in Model-Files oder frisch `train.py` + `model.pkl` im Root vor Deploy.

**Logs bei Erfolg:** `das-cable-classifier: loading …/model.pkl` (stderr aus `predict.py`).

NiFi-Anbindung: [`NIFI_INVOKEHTTP.md`](NIFI_INVOKEHTTP.md).

### predict API

HDF5 inline:

```json
{ "hdf5_bytes_base64": "<base64>" }
```

S3 (CML Data Connection **S3 Object Store**, oder Env — siehe `src/s3_client.py`):

```json
{
  "request": {
    "uri": "s3a://bucket/.../das_balticconnector_6seg.h5",
    "segment_index": "2"
  }
}
```

Antwort (rollup): `threat_type`, `confidence`, `predictions[]` optional.

---

## Troubleshooting

| Problem | Lösung |
|---------|--------|
| `ModuleNotFoundError: das_acoustic_engine` | `cd cai-workbench/das-cable-classifier`; Repo mit `nifi-processors/upload` |
| `import train` in altem `fit.py` | `git pull` (fix ab 2026-11) |
| `FileNotFoundError: model.pkl` | `cdsw-build.sh`; `bundled/model.pkl` committen oder trainieren |
| `__file__ is not defined` | Model-Entry auf **`predict.py`**, nicht Notebook |
| ML nur `cable_strain_anomaly` in UI | Altes Modell / falsche Klasse — **Redeploy** mit 11-Klassen-`bundled/model.pkl`; UI kurz **ML** vs **Ground truth** vergleichen |
| NiFi leere `predicted_threat_type` | JsonPath `$.response.threat_type`, Request mit `segment_index` — siehe NIFI-Doku |

---

## Referenzen

| Dokument | Inhalt |
|----------|--------|
| [`docs/DAS_NIFI_AND_UI_FORMAT.md`](../../docs/DAS_NIFI_AND_UI_FORMAT.md) | HDF5, Iceberg, UI, S3 preview |
| [`nifi-processors/docs/DAS_NIFI_METADATA_MAP.md`](../../nifi-processors/docs/DAS_NIFI_METADATA_MAP.md) | Attribute → Tabelle |
| [`NIFI_INVOKEHTTP.md`](NIFI_INVOKEHTTP.md) | InvokeHTTP + EvaluateJsonPath |