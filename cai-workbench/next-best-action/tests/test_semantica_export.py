"""Phase 8.3 semantica_record bridge artifact."""

import json
import unittest
from pathlib import Path

from src.config import Settings
from src.pipeline import run_phase1
from src.semantica_export import BRIDGE_SCHEMA, build_semantica_record


class TestSemanticaExport(unittest.TestCase):
    def test_build_payload(self):
        decision = {
            "flow_uuid": "abc",
            "generated_at": "2026-01-01T00:00:00+00:00",
            "sections": {
                "1_what_happened": {
                    "theatre": "Baltic",
                    "alert_type": "demo",
                    "priority": "HIGH",
                },
                "3_ranked_coas": {
                    "best_coa_id": "A",
                    "playbook_best_coa_id": "A",
                },
            },
        }
        payload = build_semantica_record(
            flow_uuid="abc",
            decision=decision,
            grounding={
                "incident_concept": "triton:Test",
                "ontology_version": "2026-11",
            },
            shacl={"conforms": True, "engine": "pyshacl"},
            snapshot_capture_source="jdbc-fallback",
            governance_exported=True,
        )
        self.assertEqual(payload["schema"], BRIDGE_SCHEMA)
        self.assertEqual(payload["decision"]["best_coa_id"], "A")
        self.assertEqual(payload["compliance"]["shacl_conforms"], True)
        self.assertEqual(payload["snapshots"]["capture_source"], "jdbc-fallback")

    def test_phase1_writes_semantica_record(self):
        settings = Settings()
        settings.demo_mode = True
        settings.offline_snapshots = True
        settings.phase = 1
        settings.artifacts_dir = Path("/tmp/nba_semantica_test")
        result = run_phase1(settings)
        path = result.artifact_dir / "semantica_record.json"
        self.assertTrue(path.is_file())
        data = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(data["flow_uuid"], result.alert.flow_uuid)
        self.assertIn("mcp_hints", data)


if __name__ == "__main__":
    unittest.main()