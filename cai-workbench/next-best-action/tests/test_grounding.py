"""Grounding, scenario matrix, decision record."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.config import Settings
from src.grounding import build_grounding
from src.pipeline import run_phase1
from src.rules_engine import evaluate_playbook
from src.scenario_matrix import run_scenario_matrix
from src.scoring import EnvironmentContext, rank_coas
from src.alert_rules import demo_greifswald_alert


class TestGrounding(unittest.TestCase):
    def test_greifswald_grounding_has_citations(self):
        settings = Settings()
        alert = demo_greifswald_alert(settings)
        env = EnvironmentContext.from_alert_payload(alert.payload)
        ranked = rank_coas(env)
        legal = evaluate_playbook(alert, ranked)
        g = build_grounding(
            alert,
            legal,
            ranked,
            playbook_best="A",
            best_coa_id="A",
        )
        d = g.to_dict()
        self.assertEqual(d["incident_concept"], "triton:TrainingScenarioGreifswald")
        self.assertTrue(d["coa_justifications"])


class TestDecisionArtifacts(unittest.TestCase):
    def test_phase1_writes_decision_record(self):
        settings = Settings()
        settings.demo_mode = True
        settings.offline_snapshots = True
        settings.phase = 1
        root = Path("/tmp/nba_decision_test")
        settings.artifacts_dir = root
        result = run_phase1(settings)
        path = result.artifact_dir / "decision_record.json"
        self.assertTrue(path.is_file())
        data = json.loads(path.read_text(encoding="utf-8"))
        self.assertIn("sections", data)
        self.assertIn("8_scenario_matrix", data["sections"])
        scenarios = run_scenario_matrix(
            result.alert,
            EnvironmentContext.from_alert_payload(result.alert.payload),
        )
        self.assertGreaterEqual(len(scenarios), 3)


if __name__ == "__main__":
    unittest.main()