"""Phase 8b SHACL gate on decision_record RDF."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest import mock

from src.config import Settings
from src.decision_rdf import SHAPES_PATH, decision_record_to_ttl
from src.grounding import build_grounding
from src.pipeline import run_phase1
from src.rules_engine import evaluate_playbook
from src.alert_rules import demo_greifswald_alert
from src.scoring import EnvironmentContext, rank_coas
from src.shacl_gate import (
    ShaclGateBlocked,
    assert_shacl_conforms,
    validate_decision_artifacts,
    validate_decision_ttl,
)
from src.persistence import update_decision_artifacts


class TestDecisionShacl(unittest.TestCase):
    def test_greifswald_ttl_conforms(self):
        settings = Settings()
        alert = demo_greifswald_alert(settings)
        env = EnvironmentContext.from_alert_payload(alert.payload)
        ranked = rank_coas(env)
        legal = evaluate_playbook(alert, ranked)
        grounding = build_grounding(
            alert, legal, ranked, playbook_best="A", best_coa_id="A"
        )
        decision = {
            "flow_uuid": alert.flow_uuid,
            "generated_at": "2026-11-18T12:00:00+00:00",
            "sections": {
                "3_ranked_coas": {
                    "best_coa_id": "A",
                    "playbook_best_coa_id": "A",
                    "coa_scores": [s.to_dict() for s in ranked],
                },
                "4_rules_and_ontology": {"grounding": grounding.to_dict()},
            },
        }
        ttl, report = validate_decision_artifacts(decision, grounding.to_dict())
        self.assertIn("nba:IncidentDecision", ttl)
        if report.engine == "pyshacl":
            self.assertTrue(report.conforms, report.report_text[:500])

    def test_invalid_ttl_fails_shacl(self):
        bad = (
            "@prefix nba: <https://triton.baltic/nba#> .\n"
            "nba:decision_x a nba:IncidentDecision .\n"
        )
        report = validate_decision_ttl(bad, SHAPES_PATH)
        if report.engine == "pyshacl":
            self.assertFalse(report.conforms)

    def test_gate_blocks_persist(self):
        settings = Settings()
        settings.shacl_gate = True
        report = {
            "conforms": False,
            "violations": ["test"],
            "engine": "artifact",
            "report_excerpt": "fail",
        }
        with self.assertRaises(ShaclGateBlocked):
            update_decision_artifacts(
                "x",
                decision_record={"flow_uuid": "x", "sections": {}},
                grounding={},
                settings=settings,
                shacl_report=report,
            )

    def test_gate_off_allows_persist(self):
        settings = Settings()
        settings.shacl_gate = False
        with mock.patch("src.persistence.trino_client.execute_sql") as execute:
            update_decision_artifacts(
                "x",
                decision_record={"flow_uuid": "x", "sections": {}},
                grounding=None,
                settings=settings,
                shacl_report={"conforms": False},
            )
        execute.assert_called_once()

    def test_phase1_writes_ttl_and_shacl_report(self):
        settings = Settings()
        settings.demo_mode = True
        settings.offline_snapshots = True
        settings.phase = 1
        root = Path("/tmp/nba_shacl_test")
        settings.artifacts_dir = root
        result = run_phase1(settings)
        ttl_path = result.artifact_dir / "decision_record.ttl"
        shacl_path = result.artifact_dir / "shacl_report.json"
        self.assertTrue(ttl_path.is_file())
        self.assertTrue(shacl_path.is_file())
        data = json.loads(shacl_path.read_text(encoding="utf-8"))
        self.assertIn("conforms", data)

    def test_assert_shacl_conforms_noop_when_disabled(self):
        from src.shacl_gate import ShaclReport

        report = ShaclReport(
            conforms=False,
            report_text="",
            violations=["x"],
            engine="pyshacl",
        )
        assert_shacl_conforms(report, gate_enabled=False)


if __name__ == "__main__":
    unittest.main()