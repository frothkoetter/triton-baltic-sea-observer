# Parallel NBA missions — NiFi audit

**Requirement:** Multiple missions may run **at the same time** (different incidents / `flow_uuid`).  
Plan: [IMPLEMENTIERUNGSPLAN_MISSION_EINSATZ_SIMULATION.md](../../docs/IMPLEMENTIERUNGSPLAN_MISSION_EINSATZ_SIMULATION.md) §4.8.

## Runtime state

| Store | Scope | Key |
|-------|--------|-----|
| `iceberg.defense.nba_mission_orders` | cluster-wide | `mission_id` |
| `/tmp/triton_mission_runtime.json` | per NiFi node | `missions.{mission_id}` |
| `/tmp/marineship_state.json` | per NiFi node | ships; `activeMissionIds[]` |

Trino is authoritative for order status; runtime JSON holds **bindings** (MMSI/buoy IDs).

## Component checklist

| Component | Parallel-safe? | Rule |
|-----------|----------------|------|
| `MissionOrderExecutor.py` | **Yes** | Iterates all `ACTIVE` records; cap `Max Active Missions` |
| `mission_parallel.py` | **Yes** | MMSI/buoy allocation sets; no global single-mission flag |
| `BojenSensorSimulator.py` | **Yes** (v1.3.0) | `Deployment Mode=MISSION_PLAYBOOK`; mission-scoped IDs via runtime store |
| `MarineShipSimulator.py` | **Partial** | One state file; parallel via distinct MMSI per mission (`allocate_surface_units`) |
| `german_navy_mission_engine.py` | **Partial** | Default patrol unrelated; NBA sets `activeMissionIds` per ship |
| `STANAGSimulator.py` | **Yes** | One batch per FlowFile; pass `mission.id` + `mission.buoy.ids` attributes |
| `simulation_control` | **Caution** | One row per `fleet_scope` today — use **`issued_by=mission:{id}`** + `mmsi_allowlist` in JSON (multiple rows Phase 3) |

## Recommended flow pattern

```text
GenerateFlowFile / QueryDatabaseTable (QUEUED orders JSON array)
  → MissionOrderExecutor  (runtime store; bundle {missions:[]})
  → MissionBundleSplit  (NDJSON one line per mission)
  → SplitText (\\n)  → **N parallel paths**
      ├→ UpdateAttribute (copy attrs from JSON — optional)
      ├→ BojenSensorSimulator (Deployment Mode=MISSION_PLAYBOOK)
      ├→ MarineShipSimulator (NBA intercept from runtime store)
      └→ STANAGSimulator (tags mission.id from incoming attrs)
```

**Processors:** `MissionOrderExecutor.py`, `MissionBundleSplit.py`, `BojenSensorSimulator.py` 1.3.0, `MarineShipSimulator.py` 1.8.0, `STANAGSimulator.py` 2.1.0.

**Do not** merge all missions into one attribute set on a single FlowFile unless downstream splits by `mission.id`.

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `MISSION_MAX_PARALLEL` | 8 | Active mission cap |
| `MISSION_MAX_SURFACE_UNITS` | 4 | Shared German Navy pool |
| `MISSION_MAX_BUOY_SLOTS` | 32 | Total buoy IDs |
| `TRITON_MISSION_RUNTIME_STATE` | `/tmp/triton_mission_runtime.json` | Path override |

## Multi-node NiFi

Each node has its own `/tmp` runtime — **only run `MissionOrderExecutor` on one node** or move bindings to Trino (`nba_mission_asset_allocations`) with JDBC from all nodes.