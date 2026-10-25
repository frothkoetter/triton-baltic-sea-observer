# NiFi Python processors

These scripts run inside Apache NiFi (Python processors) or locally for development.

## Local setup (LiDAR simulator and others)

From the repository root, with your virtualenv activated:

```bash
cd nifi-processors
python3 -m pip install -r requirements.txt
python3 LidarSimulator.py
```

`LidarSimulator.py` needs **laspy** and **pyproj** (listed in `requirements.txt`).

Each run generates **one random vessel type** as EPSG:4326 LAS. Scene centers are placed on **Baltic Sea water** (offshore seeds / sea zones; not uniform land boxes). About **10%** of runs are **unclassified** (LAS class `0`, `lidar.vessel_type` / `lidar.vessel_name` = `undefined`) with the center **5–20 km** offshore from a major port. Optional **Random Seed** and **Unclassified Fraction** control repeatability and the unclassified rate.

| Error | Fix |
|-------|-----|
| `No module named 'laspy'` | `python3 -m pip install -r requirements.txt` |
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

On NiFi, declare processor dependencies in each processor’s `ProcessorDetails.dependencies` (see `LidarSimulator`). NiFi installs those packages for the Python processor bundle; a local venv is only for offline testing.

The `LidarSimulator` class must live at **module top level** (not inside `if` blocks) or NiFi will not list it in the processor palette after S3 sync.

Upload **`baltic_water_placement.py`** and **`baltic_ship_routing.py`** with processors that import them (`MarineShipSimulator.py`, `ShipSimulationRoutesGemini.py`, `STANAGSimulator.py`, `BojenSensorSimulator.py`, plus `lidar_vessel_geometry.py` for `LidarSimulator.py`). Each processor adds the extensions directory to `sys.path` on load (NiFi runs them in isolated venvs). All files share one Python extensions directory. Restart or reload extensions after upload.

**Ship AIS:** `MarineShipSimulator` (1.5.1) and `ShipSimulationProcessor` in `ShipSimulationRoutesGemini.py` (1.2.1) route via offshore waypoints — not straight lines to harbor coordinates on land.
