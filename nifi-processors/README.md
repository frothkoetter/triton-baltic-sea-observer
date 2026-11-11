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

On NiFi, shared **`upload/requirements.txt`** must stay **empty** (NiFi runs it for every processor). Each processor adds what it needs via **`ProcessorDetails.dependencies`** (`numpy`, `pyproj`, `laspy`, …).

**TwitterMessageGenerator not loading:** Check `nifi-app.log` for `Failed to install requirements for TwitterMessageGenerator`. Common cause: **`[Errno 28] No space left on device`** while pip installs shared `requirements.txt` into `/var/opt/nifi/python/work/extensions/...`. Fix: empty `requirements.txt` on the NiFi host, delete stale dirs under `.../python/work/extensions/`, redeploy `MessengerSimulator.py` (`dependencies = []`), restart processor.

**Ship AIS:** **`upload/ShipSimulationRoutesGemini.py`** — civilian `ShipSimulationProcessor`. **`MarineShipSimulator` 1.7.0** — German Navy mission pattern (`ship.simulation.route` = `german_navy_missions`): home bases Kiel / Wilhelmshaven / Eckernförde / Warnemünde, Baltic or coastal patrol 1–4 weeks, ~7 days moored after return. Delete `/tmp/marineship_state.json` after upgrading from harbor-hopping 1.6.x.

**Route state reset:** Include **`upload/BalticShipRouteStateReset.py`** (**1.2.0**) in the sync bundle (with `baltic_ship_sim_state.py`, `marine_fleet_registry.py`). With **Dump State To Payload** = true (default), each run embeds pre-reset `/tmp` JSON under `snapshots` in the outgoing FlowFile, then deletes or clears routes—useful when the NiFi pod `/tmp` is not reachable.

**LiDAR disk:** Failed pip installs often trace to **`No space left on device`** under `/var/opt/nifi/python/work/extensions/`. Keep **`requirements.txt`** minimal; prune stale venv dirs.

**Russian Baltic Fleet:** `upload/RusFleetPositionSimulator.py` with `rus_fleet_registry.py` / `rus_fleet_sim_engine.py`. FlowFile attribute `ship.simulation.route` = `rus_fleet_positions`; state `/tmp/rus_fleet_state.json`. Reset via **Reset RUS Fleet State** on `BalticShipRouteStateReset`. Details in `upload/README.md`.

**German fishing boats:** `upload/FishingBoatSimulator.py` (**1.0.0**) with `fishing_fleet_registry.py` / `fishing_boat_mission_engine.py`. Route attribute `ship.simulation.route` = `german_fishing_tours`. NiFi property **Number of Fishing Boats** (default **10**, max 20). State `/tmp/fishing_boat_state.json`. Reset via **Reset Fishing Boat State** on `BalticShipRouteStateReset` **1.2.0**. LiDAR type **`fishing_boat`** (LAS class **10**) in `LidarSimulator` **4.4.0**.

**Ghost processor:** If the flow references an old bundle (e.g. **1.2.9**), set **`ShipSimulationProcessor`** to **1.3.0** and remove obsolete extension files from the node.
