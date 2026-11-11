# NiFi Python extensions (upload bundle)

Sync **this entire directory** to Cloudera Data Flow / NiFi `python/extensions/` (Python Auto-Loader or S3). Do not upload parent `tests/` or `dev/`.

| File | Role |
|------|------|
| `ShipSimulationRoutesGemini.py` | AIS — `ShipSimulationProcessor` |
| `MarineShipSimulator.py` | German Navy AIS (`german_navy_missions` — base patrol return) |
| `german_navy_mission_engine.py` | Mission logic for MarineShipSimulator |
| `LidarSimulator.py` | LiDAR LAS generator |
| `BojenSensorSimulator.py` | Buoy / MAD sensor sim |
| `STANAGSimulator.py` | STANAG messages |
| `MessengerSimulator.py` | Messenger sim |
| `gpsjammer.py` | GPS jamming sim |
| `BalticShipRouteStateReset.py` | Reset `/tmp` ship route state (optional dump to FlowFile) |
| `RusFleetPositionSimulator.py` | Russian Baltic Fleet positions (`rus_fleet_positions`) |
| `FishingBoatSimulator.py` | German fishing fleet AIS (`german_fishing_tours`) |
| `fishing_fleet_registry.py` | 20 German trawlers, MMSI `2118710xx`, home ports |
| `fishing_boat_mission_engine.py` | Home-port tours (12–36 h), low speed |
| `rus_fleet_registry.py` | Fleet MMSI `RUS273xxx`, home bases, patrol seeds |
| `rus_fleet_sim_engine.py` | Mission / port / anchor state machine |
| `baltic_water_placement.py` | Water / land mask (shared) |
| `baltic_ship_routing.py` | Sea routes (AIS + marine) |
| `baltic_ship_sim_state.py` | Route reset helpers + AIS / RUS fleet state paths |
| `marine_fleet_registry.py` | Marine / STANAG fleet registry |
| `lidar_vessel_geometry.py` | LiDAR vessel meshes |
| `requirements.txt` | Empty on purpose; per-processor deps in `ProcessorDetails.dependencies` |

Per-processor extras (`pyproj`, `laspy`, `faker`, …) come from each file’s `ProcessorDetails.dependencies`.

## Russian Baltic Fleet route

FlowFile attribute **`ship.simulation.route`** = **`rus_fleet_positions`**.

State: **`/tmp/rus_fleet_state.json`** (separate from `/tmp/ship_state.json` and `/tmp/marineship_state.json`).

Use **BalticShipRouteStateReset** with **Reset RUS Fleet State** = true, or delete the file manually.

## German fishing fleet route

FlowFile attribute **`ship.simulation.route`** = **`german_fishing_tours`**.

Processor property **Number of Fishing Boats** (default **10**).

State: **`/tmp/fishing_boat_state.json`** — delete when changing boat count or after logic upgrades.

## After deploy

Reload Python extensions; run route state reset or clear `/tmp/ship_state.json`, `/tmp/marineship_state.json`, and `/tmp/rus_fleet_state.json` when routing or fleet logic changes.

Tests (from repo `nifi-processors/`): `python3 -m unittest tests.test_rus_fleet_sim test_baltic_ship_routing`
