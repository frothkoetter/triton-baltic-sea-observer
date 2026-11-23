#!/usr/bin/env python3
"""Build Semantica ContextGraph JSON for Triton defense (no ML deps)."""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
KDM = REPO / "cai-workbench/next-best-action/data/kdm_defense_classes.json"
MAPPING = REPO / "cai-workbench/next-best-action/data/baltic_trino_mapping.json"
OUT = Path(__file__).resolve().parent / "data" / "triton_defense_graph.json"
NS = "https://w3id.org/triton/baltic-sea-observer/defense#"


def main() -> int:
    kdm = json.loads(KDM.read_text(encoding="utf-8"))
    mapping = json.loads(MAPPING.read_text(encoding="utf-8"))
    nodes: list[dict] = []
    edges: list[dict] = []

    onto_id = NS
    nodes.append(
        {
            "id": onto_id,
            "type": "Ontology",
            "properties": {
                "uri": onto_id,
                "content": "Triton Baltic defense KDM ontology",
                "source": "triton_build",
            },
        }
    )

    tables_seen: set[str] = set()
    for cls in kdm.get("classes", []):
        cid = cls["class_id"]
        uri = f"{NS}{cid}"
        nodes.append(
            {
                "id": uri,
                "type": "OntologyClass",
                "properties": {
                    "uri": uri,
                    "label": cls.get("label") or cid,
                    "content": cls.get("label") or cid,
                    "ontology_uri": onto_id,
                    "source": "kdm_defense_classes",
                },
            }
        )
        edges.append(
            {
                "id": f"definedIn:{cid}",
                "source_id": uri,
                "target_id": onto_id,
                "type": "definedIn",
                "properties": {},
            }
        )
        for key in ("primary_table", "primary_view"):
            t = cls.get(key)
            if t:
                tables_seen.add(t)
                table_uri = f"urn:triton:defense:table:{t}"
                if not any(n["id"] == table_uri for n in nodes):
                    nodes.append(
                        {
                            "id": table_uri,
                            "type": "DatabaseTable",
                            "properties": {
                                "table_name": t,
                                "schema": "defense",
                                "content": t,
                                "source": "kdm_defense_classes",
                            },
                        }
                    )
                edges.append(
                    {
                        "id": f"mapsTo:{cid}:{t}",
                        "source_id": uri,
                        "target_id": table_uri,
                        "type": "mapsToPhysicalTable",
                        "properties": {"role": key},
                    }
                )

    for ent in mapping.get("semantic_entities", []):
        euri = ent.get("iri") or f"triton:{ent['entity_id']}"
        nodes.append(
            {
                "id": euri,
                "type": "SemanticEntity",
                "properties": {
                    "uri": euri,
                    "label": ent["entity_id"],
                    "content": ent["entity_id"],
                    "source": "baltic_trino_mapping",
                },
            }
        )
        for t in ent.get("tables", []):
            table = t["table"]
            tables_seen.add(table)
            table_uri = f"urn:triton:defense:table:{table}"
            if not any(n["id"] == table_uri for n in nodes):
                nodes.append(
                    {
                        "id": table_uri,
                        "type": "DatabaseTable",
                        "properties": {
                            "table_name": table,
                            "schema": "defense",
                            "content": table,
                            "source": "baltic_trino_mapping",
                        },
                    }
                )
            edges.append(
                {
                    "id": f"semantic:{ent['entity_id']}:{table}",
                    "source_id": euri,
                    "target_id": table_uri,
                    "type": "evidenceIn",
                    "properties": {"role": t.get("role")},
                }
            )

    payload = {
        "graph_id": str(uuid.uuid4()),
        "nodes": nodes,
        "edges": edges,
        "links": [],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(nodes)} nodes, {len(edges)} edges → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())