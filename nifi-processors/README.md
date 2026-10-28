# NiFi Python processors

Layout:

| Directory | Purpose |
|-----------|---------|
| **`upload/`** | **Sync this folder only** to NiFi `python/extensions/` (Python Auto-Loader / S3). |
| **`tests/`** | Regression tests — not uploaded to NiFi. |
| **`dev/`** | Local LAS export, legacy scripts, `requirements-dev.txt`. |

See **`upload/README.md`** for the file list and roles.

## Local setup

```bash
cd nifi-processors/dev
python3 -m pip install -r requirements-dev.txt
PYTHONPATH=../upload python3 ../upload/LidarSimulator.py
```

`LidarSimulator.py` needs **laspy** and **pyproj** (in `requirements-dev.txt`; NiFi installs them via `ProcessorDetails.dependencies`).

Each LiDAR run generates **one random vessel type** as EPSG:4326 LAS on Baltic water. About **10%** of runs are **unclassified** (class `0`). Optional **Random Seed** and **Unclassified Fraction** on the processor.

| Error | Fix |
|-------|-----|
| `No module named 'laspy'` | `pip install -r dev/requirements-dev.txt` |
| Wrong **pyroj** package | Uninstall pyroj; `pip install pyproj` |

### Tests

```bash
cd nifi-processors
PYTHONPATH=upload:tests python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=upload:tests python3 tests/test_baltic_ship_routing.py
```

Coast pins live in **`tests/baltic_coast_pins.py`** (tests only).

### Vessel LAS samples (dev)

```bash
cd nifi-processors/dev
PYTHONPATH=../upload python3 export_vessel_samples.py
```

Writes to **`dev/samples/`**. Open in CloudCompare; color by **Classification**.

## NiFi deploy

1. Upload **contents of `upload/`** (flat — no `upload/` prefix on the cluster).
2. Do **not** upload `tests/`, `dev/`, or `test_*.py`.
3. Reload Python extensions after changes.

On NiFi, shared **`upload/requirements.txt`** is minimal (**numpy**); each processor adds **`ProcessorDetails.dependencies`** (`pyproj`, `laspy`, `faker`, …).

**Ship AIS:** Only **`upload/ShipSimulationRoutesGemini.py`** for `ShipSimulationProcessor` (**1.3.1**). No legacy duplicate AIS modules. **`MarineShipSimulator` 1.6.3** shares the same routing fast path. Each GenerateFlowFile tick (**1 min**) = **1 hour** simulated time. After upgrading from 1.3.0 / 1.6.2, run route state reset or delete `/tmp/ship_state.json` and `/tmp/marineship_state.json` once so `RouteDestKey` is populated.

**Route state reset:** Include **`upload/BalticShipRouteStateReset.py`** (**1.1.0**) in the sync bundle (with `baltic_ship_sim_state.py`, `marine_fleet_registry.py`). With **Dump State To Payload** = true (default), each run embeds pre-reset `/tmp` JSON under `snapshots` in the outgoing FlowFile, then deletes or clears routes—useful when the NiFi pod `/tmp` is not reachable.

**LiDAR disk:** Failed pip installs often trace to **`No space left on device`** under `/var/opt/nifi/python/work/extensions/`. Keep **`requirements.txt`** minimal; prune stale venv dirs.

**Ghost processor:** If the flow references an old bundle (e.g. **1.2.9**), set **`ShipSimulationProcessor`** to **1.3.0** and remove obsolete extension files from the node.
