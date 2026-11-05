# NiFi upload bundle — Russian Baltic Fleet

Deploy these files together with parent-directory shared modules (`baltic_ship_routing.py`, `baltic_water_placement.py`).

| File | Role |
|------|------|
| `rus_fleet_registry.py` | Fleet list (MMSI `RUS273xxx`), home bases, patrol zone seeds |
| `rus_fleet_sim_engine.py` | Mission / port / anchor state machine (unit-testable) |
| `RusFleetPositionSimulator.py` | NiFi `FlowFileTransform` processor |

## Simulation route

FlowFile attribute **`ship.simulation.route`** = **`rus_fleet_positions`**.

State file: **`/tmp/rus_fleet_state.json`** (separate from marine and civilian AIS simulators).

Version file when ship count changes: `/tmp/rus_fleet_version_state.json`.

## Behavior summary

- Registry matches CDW table **`rus_fleet`** (~35 units; real public names + simulated fillers).
- ~**33%** of fleet on Baltic patrol missions at init (from `default_mission_state` + fill to target fraction).
- Missions loop offshore patrol boxes (Fehmarn Belt, Mecklenburg Bight, Swedish EEZ, etc.) via water-only routing, then return to **`home_base`**.
- Occasional **anchoring** (Speed 0, Status Anchored) up to 24 simulated hours at waypoints.
- **7 days** in port (`Moored`) after each mission before standby or re-assignment.
- Non-mission units **anchored** near Baltiysk (~60% of home-port fleet), Kronstadt, Primorsk, or Vistula Lagoon.

## Processor properties

- **Number of Ships** — subset of full registry (default: all).
- **Random Seed** — optional reproducibility for tests / demos.

## Tests

From `nifi-processors/`:

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m unittest tests.test_rus_fleet_sim test_baltic_ship_routing
```

## Reset state

Use `BalticShipRouteStateReset` with **Reset RUS Fleet State** = true, or delete `/tmp/rus_fleet_state.json` manually.
