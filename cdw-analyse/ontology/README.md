# Semantica MCP bundle (Triton defense)

Ontology, knowledge graph, and business rules for [frothkoetter/semantica](https://github.com/frothkoetter/semantica.git) MCP — same layout as the airline demo (`SEMANTICA_KG_PATH`, `SEMANTICA_MAPPING_CONFIG`, `SEMANTICA_BUSINESS_RULES`).

## Files

| File | Role |
|------|------|
| `data/triton_defense_graph.json` | ContextGraph JSON (KDM classes + Trino tables) |
| `defense_r2rml_db_mapping.yaml` | Ontology class ↔ `defense.*` table/column map |
| `defense_business_rules.yaml` | `sql_agent_hints`, JOINs for ontology_mapper / sql_executor |
| `build_triton_defense_graph.py` | Regenerate KG after KDM/mapping edits |
| `semantica_mcp_invoke.py` | CLI for Node bridge → `get_graph_summary` / `get_business_rules` |

## Regenerate graph

```bash
python3 cdw-analyse/ontology/build_triton_defense_graph.py
```

## Cursor / Claude MCP (local)

```json
{
  "mcpServers": {
    "semantica-triton": {
      "command": "python",
      "args": ["-m", "semantica.mcp_server"],
      "env": {
        "SEMANTICA_KG_PATH": "/path/to/triton-baltic-sea-observer/cdw-analyse/ontology/data/triton_defense_graph.json",
        "SEMANTICA_MAPPING_CONFIG": "/path/to/triton-baltic-sea-observer/cdw-analyse/ontology/defense_r2rml_db_mapping.yaml",
        "SEMANTICA_BUSINESS_RULES": "/path/to/triton-baltic-sea-observer/cdw-analyse/ontology/defense_business_rules.yaml",
        "SEMANTICA_MCP_TOOLSET": "ontology_mapper",
        "SEMANTICA_MCP_DISABLE_ML": "1"
      }
    }
  }
}
```

Install Semantica:

```bash
pip install "git+https://github.com/frothkoetter/semantica.git"
```

## Triton UI API

- `GET /api/agents/semantica/graph-summary` — `get_graph_summary` (Python MCP or Node KG snapshot)
- `GET /api/agents/semantica/business-rules` — JOIN hints + thresholds
- `POST /api/agents/ttyd/ask` — includes `semantica_mcp` on mapper success

Trino execution stays in the Node warehouse bridge; use iceberg-mcp or env `TRINO_*` for live SQL outside the UI.