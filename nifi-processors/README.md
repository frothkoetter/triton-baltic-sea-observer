# NiFi Python processors

These scripts run inside Apache NiFi (Python processors) or locally for development.

## Local setup (LiDAR simulator and others)

From the repository root, with your virtualenv activated:

```bash
cd nifi-processors
python3 -m pip install -r requirements-dev.txt
python3 LidarSimulator.py
```

`LidarSimulator.py` needs **laspy** and **pyproj** (in `requirements-dev.txt`; NiFi installs them via `ProcessorDetails.dependencies`).

Each run generates **one random vessel type** as EPSG:4326 LAS. Scene centers are placed on **Baltic Sea water** (offshore seeds / sea zones; not uniform land boxes). About **10%** of runs are **unclassified** (LAS class `0`, `lidar.vessel_type` / `lidar.vessel_name` = `undefined`) with the center **5–20 km** offshore from a major port. Optional **Random Seed** and **Unclassified Fraction** control repeatability and the unclassified rate.

| Error | Fix |
|-------|-----|
| `No module named 'laspy'` | `python3 -m pip install -r requirements-dev.txt` |
| `No module named 'pyproj'` but pip installed **pyroj** | Wrong package — `requirements.txt` line 5 must be `pyproj`, not `pyroj`. Run: `python3 -m pip uninstall pyroj -y && python3 -m pip install pyproj` |

Output in the current directory:

| File | Use |
|------|-----|
| `baltic_fleet.las` | WGS84 lon/lat (EPSG:4326) for NiFi and GIS pipelines |
| `baltic_fleet_enu.las` | UTM meters (EPSG:32635) for **3D viewers** |

### Viewing in 3D

Open **`baltic_fleet_enu.las`** in [CloudCompare](https://www.cloudcompare.org/) (or similar). Color points by **Classification** (1–9 = vessel types).

Do **not** use a plain 3D plot of `baltic_fleet.las` with lon/lat on X/Y and height on Z without reprojection — degrees and meters share the same axis, so ships look like flat smears, not hulls.

The geometry is **parametric** (hull + superstructure silhouettes), not CAD mesh models of real vessels.

### One LAS per vessel type

```bash
python3 export_vessel_samples.py
ls samples/
```

Creates `samples/sample_oil_tanker.las`, … `sample_landing_ship.las`, and `samples/all_vessel_types_enu.las` (3×3 grid). Color by **Classification** 1–9 in CloudCompare.

## NiFi runtime

On NiFi, declare processor dependencies in each processor’s `ProcessorDetails.dependencies` (see `LidarSimulator`). NiFi also installs shared `requirements.txt` (keep it minimal — **numpy only**) plus those deps into each processor venv under `/var/opt/nifi/python/work/extensions/`.

**LiDAR validation / `Failed to install requirements for LidarSimulator` (pip return code 1):** In `nifi-app.log`, scroll above the `RuntimeError` for `OSError: [Errno 28] No space left on device`. The main consumer is usually **Python processor venvs** under `/var/opt/nifi/python/work/extensions/` (pip installs numpy/laspy/pyproj per processor), not LAS output. Fix: free disk there (`rm -rf` stale version dirs), deploy minimal `requirements.txt`, restart the processor.

**LiDAR temp files:** Each run writes one `.las` under a `lidar_sim_*` temp directory, loads it into the FlowFile, then deletes the directory (v4.2.1+). Older builds used `mkdtemp` without cleanup and could leak under `/tmp` if scheduled often. Downstream **PutHDFS / PutIceberg** storage is separate from the NiFi pod disk.

The `LidarSimulator` class must live at **module top level** (not inside `if` blocks) or NiFi will not list it in the processor palette after S3 sync.

`baltic_water_placement.py` uses coarse **MAINLAND_BOXES** and **ISLAND_BOXES** (Gotland, Åland, Saaremaa, Rügen, etc.) so open-water zones and random seeds do not treat islands as sea. Redeploy this file whenever placement logic changes.

Upload **`baltic_water_placement.py`** and **`baltic_ship_routing.py`** with processors that import them (`MarineShipSimulator.py`, `ShipSimulationRoutesGemini.py`, `STANAGSimulator.py`, `BojenSensorSimulator.py`, plus `lidar_vessel_geometry.py` for `LidarSimulator.py`). Each processor adds the extensions directory to `sys.path` on load (NiFi runs them in isolated venvs). All files share one Python extensions directory. Restart or reload extensions after upload.

**Ship AIS:** `MarineShipSimulator` (1.5.3) and `ShipSimulationProcessor` in `ShipSimulationRoutesGemini.py` (1.2.3) route via offshore waypoints. Any processor that imports `baltic_water_placement.py` must declare **`pyproj`** in `ProcessorDetails.dependencies` (LiDAR, STANAG, Marine, Bojen, Gemini ship sim). Deploy `baltic_water_placement.py` + `baltic_ship_routing.py` with them. After upgrade, delete stale state on NiFi nodes: `/tmp/ship_state.json`, `/tmp/marineship_state.json`.

**Route state reset:** `BalticShipRouteStateReset` (deploy with `baltic_ship_sim_state.py`) clears only those `/tmp` JSON files—or empties `RouteWaypoints` / `RouteIndex` in place—so AIS and marine simulators pick up new water routing without touching LiDAR temp dirs or `marineship_version_state.json`. Wire it on a manual **GenerateFlowFile** or schedule after deployments.
