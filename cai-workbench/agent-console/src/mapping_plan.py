"""KDM mapping plan resolver (Ontology Mapper MVP)."""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

_DATA = Path(__file__).resolve().parents[2] / "next-best-action" / "data"
_KDM_PATH = _DATA / "kdm_defense_classes.json"


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def load_kdm() -> dict[str, Any]:
    if not _KDM_PATH.is_file():
        return {"classes": [], "golden_examples": [], "warehouse": {}}
    return json.loads(_KDM_PATH.read_text(encoding="utf-8"))


def resolve_golden_example(text: str) -> dict[str, Any]:
    kdm = load_kdm()
    hay = _norm(text)
    best: dict[str, Any] | None = None
    best_score = 0
    for ex in kdm.get("golden_examples") or []:
        score = 0
        for p in ex.get("prompts") or []:
            if _norm(p) in hay:
                score += 2
        if ex.get("id") and _norm(ex["id"]) in hay:
            score += 3
        for c in ex.get("classes") or []:
            if _norm(c) in hay:
                score += 1
        if score > best_score:
            best_score = score
            best = ex
    if not best or best_score < 2:
        return {"ok": False, "error": "no match", "score": best_score}
    return {"ok": True, "example": best, "score": best_score}


def build_mapping_plan(example: dict[str, Any]) -> dict[str, Any]:
    kdm = load_kdm()
    by_id = {c["class_id"]: c for c in kdm.get("classes") or [] if c.get("class_id")}
    classes = [by_id[cid] for cid in example.get("classes") or [] if cid in by_id]
    tables: set[str] = set()
    views: set[str] = set()
    for c in classes:
        if c.get("primary_table"):
            tables.add(c["primary_table"])
        if c.get("primary_view"):
            views.add(c["primary_view"])
        for t in c.get("primary_tables") or []:
            tables.add(t)
        pv = c.get("position_via") or {}
        if pv.get("table"):
            tables.add(pv["table"])
        if c.get("preferred_view"):
            views.add(c["preferred_view"])
    return {
        "schema": "triton-mapping-plan/1",
        "example_id": example.get("id"),
        "kdm_classes": example.get("classes"),
        "warehouse": kdm.get("warehouse") or {"catalog": "iceberg", "schema": "defense"},
        "tables": sorted(tables),
        "views": sorted(views),
        "kg_query_id": example.get("kg_query_id"),
    }