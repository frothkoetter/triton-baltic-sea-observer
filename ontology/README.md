# Triton defense ontology

YAML definitions describe **object types** and how they map to:

| Layer | Purpose |
|--------|---------|
| `column` | `defense.*` Iceberg/Hive table (see `cdw-analyse/`) |
| `nifiAttribute` | NiFi FlowFile attributes from simulators |
| `cdvParameter` | Cloudera Data Visualization application parameters |

## Files

- `triton_defense_ontology.yaml` — object types and property mappings (currently **LidarSimulationMetadata** → `defense.lidar_simulation_metadata`).

## LiDAR table DDL

```bash
# Impala / CDW
cdw-analyse/lidar_simulation_metadata_impala.sql
```

## CDV

Use dataset **`defense.lidar_simulation_metadata`** (or ontology name `LidarSimulationMetadata`) and the parameter names in `cdv/lidar_metadata_iris_style_example.html`.
