"""Consistency: ontology ↔ business rules ↔ Trino mapping."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

_DATA = Path(__file__).resolve().parents[1] / "data"


class TestBusinessRulesMapping(unittest.TestCase):
    def setUp(self) -> None:
        self.onto = json.loads((_DATA / "baltic_ontology.json").read_text())
        self.rules = json.loads((_DATA / "baltic_business_rules.json").read_text())
        self.trino = json.loads((_DATA / "baltic_trino_mapping.json").read_text())

    def test_alert_types_reference_known_rules(self) -> None:
        rule_ids = {r["rule_id"] for r in self.rules["business_rules"]}
        for _key, meta in self.onto["alert_types"].items():
            for rid in meta.get("business_rule_ids", []):
                self.assertIn(rid, rule_ids, msg=_key)

    def test_playbook_links_exist(self) -> None:
        pb_ids = {r["id"] for r in json.loads((_DATA / "rules_playbook.json").read_text())["alert_rules"]}
        for rule in self.rules["business_rules"]:
            pid = rule.get("playbook_rule_id")
            if pid:
                self.assertIn(pid, pb_ids)

    def test_trino_tables_are_entities(self) -> None:
        known_tables = set()
        for ent in self.trino["semantic_entities"]:
            for t in ent["tables"]:
                known_tables.add(t["table"])
        for rule in self.rules["business_rules"]:
            for tbl in (rule.get("trino_bindings") or {}).get("tables", []):
                self.assertIn(tbl, known_tables, msg=rule["rule_id"])

    def test_alert_bindings_tables_exist(self) -> None:
        entities = {e["entity_id"]: e for e in self.trino["semantic_entities"]}
        for alert, bind in self.trino["alert_type_bindings"].items():
            self.assertIn(alert, self.onto["alert_types"])
            for tbl in bind["primary_tables"]:
                found = any(
                    tbl == t["table"]
                    for e in entities.values()
                    for t in e["tables"]
                )
                self.assertTrue(found, msg=f"{alert}:{tbl}")


if __name__ == "__main__":
    unittest.main()