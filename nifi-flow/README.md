# NiFi flow definitions (Cloudera Data Flow)

Import these JSON exports into a DF **Flow** or NiFi Registry version.

| File | Flow name | Notes |
|------|-----------|--------|
| **`proj_triton_showcase-version-2.json`** | `proj_triton_showcase` | Showcase deployment: simulators, Kafka, Iceberg, RUS fleet, LiDAR classify, route reset |
| `proj_triton-version-11.json` | `proj_triton` | Earlier full project export |
| `proj_triton-version-2.json` | Triton Data Simulator | Legacy simulator-focused export |
| `Triton_Data_Simulator_V2.json` | Triton Data Simulator | Simulator variant |

Supporting files:

- `triton-parameter-groups.json` — parameter context for deployment (`flow_create.sh`)
- `triton-custom-python-config.json` — Python extension bundle config for DF

Python processors live under `nifi-processors/upload/` (sync to NiFi extensions before starting the flow).
