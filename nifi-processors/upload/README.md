# NiFi Python extensions (upload bundle)

Sync **this entire directory** to Cloudera Data Flow / NiFi `python/extensions/` (Python Auto-Loader or S3). Do not upload parent `tests/` or `dev/`.

| File | Role |
|------|------|
| `ShipSimulationRoutesGemini.py` | AIS — `ShipSimulationProcessor` |
| `MarineShipSimulator.py` | Marine AIS |
| `LidarSimulator.py` | LiDAR LAS generator |
| `BojenSensorSimulator.py` | Buoy / MAD sensor sim |
| `STANAGSimulator.py` | STANAG messages |
| `MessengerSimulator.py` | Messenger sim |
| `gpsjammer.py` | GPS jamming sim |
| `BalticShipRouteStateReset.py` | Reset `/tmp` ship route state |
| `AisOutlierCmlInvoke.py` | POST AIS batch to CML **ais-outlier-detector** model (JSON array out) |
| `baltic_water_placement.py` | Water / land mask (shared) |
| `baltic_ship_routing.py` | Sea routes (AIS + marine) |
| `baltic_ship_sim_state.py` | Route reset helpers + AIS state path |
| `marine_fleet_registry.py` | Marine / STANAG fleet registry |
| `lidar_vessel_geometry.py` | LiDAR vessel meshes |
| `requirements.txt` | NiFi `pip install` base (`numpy`) |

Per-processor extras (`pyproj`, `laspy`, `faker`, …) come from each file’s `ProcessorDetails.dependencies`.

After deploy: reload Python extensions; clear `/tmp/ship_state.json` and `/tmp/marineship_state.json` when routing changes.
