# NiFi Python extensions (upload bundle)

Sync **this entire directory** to Cloudera Data Flow / NiFi `python/extensions/` (Python Auto-Loader or S3). Do not upload parent `tests/` or `dev/`.

| File | Role |
|------|------|
| `ShipSimulationRoutesGemini.py` | AIS — `ShipSimulationProcessor` |
| `MarineShipSimulator.py` | German Navy AIS (`german_navy_missions` — base patrol return) |
| `german_navy_mission_engine.py` | Mission logic for MarineShipSimulator |
| `LidarSimulator.py` | LiDAR LAS generator |
| `BojenSensorSimulator.py` | Buoy / MAD sensor sim |
| `DasCableAcousticSimulator.py` | DAS Seekabel-Akustik — HDF5 Spektrogramme (**1.0.0**) |
| `das_acoustic_engine.py` | Multi-channel series + STFT along cable corridor |
| `cable_corridor_registry.py` | EMODnet WKT (Balticconnector, …) |
| `STANAGSimulator.py` | STANAG / ops messaging (**2.0.0** — HQ ↔ unit ↔ unit) |
| `stanag_message_engine.py` | Message templates, traffic profiles, NDJSON render |
| `MessengerSimulator.py` | Messenger sim |
| `gpsjammer.py` | GPS jamming sim |
| `BalticShipRouteStateReset.py` | Reset `/tmp` ship route state (optional dump to FlowFile) |
| `RusFleetPositionSimulator.py` | Russian Baltic Fleet positions (`rus_fleet_positions`) |
| `FishingBoatSimulator.py` | German fishing fleet AIS (`german_fishing_tours`) |
| `NorthSeaBalticTradeSimulator.py` | North Sea ↔ Baltic trade AIS (`north_sea_baltic_trade`) |
| `north_sea_baltic_fleet_registry.py` | Deterministic trade fleet MMSI `311040001+` |
| `north_sea_baltic_transit_engine.py` | Hub/port cycle, moored fast-path, pair-route cache |
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

## DAS cable acoustic (Balticconnector)

**DasCableAcousticSimulator** — synthetic **48-channel** DAS windows along **`BALTICCONNECTOR`** (EMODnet LINESTRING from `ostsee_infrastruktur`). Output **HDF5** (`triton/das-cable/v1`) with `channels`, `spectrogram`, ground-truth `threat_class` for ML.

CML classifier: `cai-workbench/das-cable-classifier/` — `python3 scripts/validate_nifi_ml.py` after `train.py`.

| Property | Default |
|----------|---------|
| Cable Asset | `BALTICCONNECTOR` |
| Segment Count | 6 |
| Output Format | `hdf5` |

## STANAG / marine position alignment

**StanagMessageGenerator** (`STANAGSimulator.py` **2.0.0**) uses MarineShipSimulator coordinates and simulates **NATO-style operational traffic**:

- **Unit → HQ:** SITREP, POSREP, SPOT, MEDREP  
- **HQ → unit:** FRAGO, WMSG (MOC / TFHQ / LOGCELL …)  
- **Unit → unit:** INTSNO (PIM / escort handover), LOGREQ rendezvous  

| Property | Typical value |
|----------|----------------|
| Traffic Profile | `watch_section` (14–22 msgs), `major_exercise` (42–58), `custom` + Number of Messages |
| Exercise Designation | `BALTOPS` (or empty → random) |
| Output Format | `text_bundle` (CDV) or `ndjson` (Kafka / SplitRecord) |
| Include Inter-Unit / HQ Traffic | `true` |

1. Connect **MarineShipSimulator → success → StanagMessageGenerator** (NDJSON preferred).  
2. Fallback: **`/tmp/marineship_state.json`** on the NiFi node.  
3. **Require Marine Ship Positions** = `true` avoids random coordinates.

FlowFile attributes include `message_id`, `message_dtg`, `message_from`, `message_to`, `stanag.traffic.*` counts for **AttributesToJSON**.

## Russian Baltic Fleet route

FlowFile attribute **`ship.simulation.route`** = **`rus_fleet_positions`**.

State: **`/tmp/rus_fleet_state.json`** (separate from `/tmp/ship_state.json` and `/tmp/marineship_state.json`).

Ships at **home_port** periodically roll new patrol missions (`RusFleetPositionSimulator` **1.0.1**+). After upgrading, reset state so stuck `home_port` rows are not reused.

Use **BalticShipRouteStateReset** with **Reset RUS Fleet State** = true, or delete the file manually.

## North Sea ↔ Baltic trade route

FlowFile attribute **`ship.simulation.route`** = **`north_sea_baltic_trade`**.

Processor property **Number of Ships** (default **1000**, max **2000**). State **`/tmp/nsb_trade_state.json`**; count file **`/tmp/nsb_trade_version_state.json`**.

Benchmark: `NUM_SHIPS=1000 PYTHONPATH=upload:tests python3 tests/benchmark_nsb_trade_runtime.py`.

Seed **`ships`** rows: `cdw-analyse/ships_nsb_trade_1000.sql` (or `dev/generate_nsb_ships_sql.py`).

## German fishing fleet route

FlowFile attribute **`ship.simulation.route`** = **`german_fishing_tours`**.

Processor property **Number of Fishing Boats** (default **10**).

State: **`/tmp/fishing_boat_state.json`** (count changes also use `/tmp/fishing_boat_version_state.json` on the simulator).

Use **BalticShipRouteStateReset** **1.2.0** with **Reset Fishing Boat State** = true, or delete the file manually.

## After deploy

Reload Python extensions; run route state reset or clear `/tmp/ship_state.json`, `/tmp/marineship_state.json`, `/tmp/rus_fleet_state.json`, and `/tmp/fishing_boat_state.json` when routing or fleet logic changes.

Tests (from repo `nifi-processors/`): `python3 -m unittest tests.test_rus_fleet_sim test_baltic_ship_routing`
