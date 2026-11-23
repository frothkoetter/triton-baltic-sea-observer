#!/usr/bin/env python3
"""Smoke: kdm_defense_classes tables exist as substrings in create_db_tables_trino.sql."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KDM = ROOT / "cai-workbench" / "next-best-action" / "data" / "kdm_defense_classes.json"
DDL = ROOT / "cdw-analyse" / "create_db_tables_trino.sql"


def main() -> int:
    if not KDM.is_file() or not DDL.is_file():
        print("missing kdm or ddl", file=sys.stderr)
        return 1
    kdm = json.loads(KDM.read_text(encoding="utf-8"))
    ddl = DDL.read_text(encoding="utf-8").lower()
    missing = []
    for c in kdm.get("classes") or []:
        for key in ("primary_table", "primary_view"):
            t = c.get(key)
            if t and t.lower() not in ddl:
                missing.append(f"{c['class_id']}.{key}={t}")
        for t in c.get("primary_tables") or []:
            if t.lower() not in ddl:
                missing.append(f"{c['class_id']}.table={t}")
        pv = (c.get("position_via") or {}).get("table")
        if pv and pv.lower() not in ddl:
            missing.append(f"{c['class_id']}.position_via={pv}")
    if missing:
        print("FAIL missing in DDL:", *missing, sep="\n  ")
        return 1
    print("OK", len(kdm.get("classes") or []), "classes,", len(kdm.get("golden_examples") or []), "golden")
    return 0


if __name__ == "__main__":
    sys.exit(main())