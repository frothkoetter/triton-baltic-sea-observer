# Mission Execution — NiFi 2 process group

Parallel NBA mission simulation: **MissionOrderExecutor** → **MissionBundleSplit** → **SplitText** → **BuoySensorSimulator** / **StanagMessageGenerator**, plus timer-driven **MarineShipSimulator** (shared `/tmp/triton_mission_runtime.json`).

## Files

| File | Use |
|------|-----|
| `Mission_Execution-version-1.json` | Full flow root `Mission_Execution` (import as new DF flow) |
| `Mission_Execution-process-group.json` | Single PG only — copy into `proj_triton_showcase` canvas |
| `build_mission_execution_flow.py` | Regenerate after processor property changes |

## Prerequisites

1. Sync Python extensions: `nifi-processors/upload/` → NiFi `python/extensions` (see [`nifi-processors/README.md`](../nifi-processors/README.md)).
2. Processors required: `MissionOrderExecutor`, `MissionBundleSplit`, `BuoySensorSimulator` (**Deployment Mode** `MISSION_PLAYBOOK`), `MarineShipSimulator` 1.8+, `StanagMessageGenerator` 2.1+.
3. Optional: Trino JDBC on executor for production orders (demo uses **GenerateFlowFile** with 2× `QUEUED` sample missions every 30s).

## Import (Cloudera Data Flow)

1. **Flows** → **Import** → `Mission_Execution-version-1.json`
2. Enable PG **Mission Execution**
3. Replace **Load demo QUEUED orders** with **QueryDatabaseTable** / ListenHTTP when Trino orders are live
4. Wire **PutDatabaseRecord** / **PutIceberg** after buoy/STANAG success (copy from existing Buoy PG in showcase)

## Topology

```text
Load demo QUEUED orders (30s)
  → MissionOrderExecutor
  → MissionBundleSplit
  → SplitText
       ├→ BuoySensorSimulator (MISSION_PLAYBOOK) → Log
       └→ StanagMessageGenerator → Log

Marine tick (30s) → MarineShipSimulator → Log
```

## Parallel missions

Up to **8** active missions (`Max Active Missions`). SplitText emits **one FlowFile per mission line** — parallel execution within the PG.

See [`nifi-processors/docs/MISSION_PARALLEL_NIFI.md`](../nifi-processors/docs/MISSION_PARALLEL_NIFI.md).

## Regenerate

```bash
python3 nifi-flow/build_mission_execution_flow.py
```