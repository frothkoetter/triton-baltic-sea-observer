"""Phase 7 governance PROV bundle."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.config import Settings
from src.governance_export import export_governance_bundle
from src.pipeline import run_phase7


class TestGovernanceExport(unittest.TestCase):
    def test_prov_bundle_written(self):
        settings = Settings()
        settings.demo_mode = True
        settings.offline_snapshots = True
        settings.skip_llm = True
        settings.phase = 7
        root = Path("/tmp/nba_phase7_test")
        settings.artifacts_dir = root
        result = run_phase7(settings)
        gov_path = result.artifact_dir / "governance_bundle.json"
        self.assertTrue(gov_path.is_file())
        data = json.loads(gov_path.read_text(encoding="utf-8"))
        self.assertIn("prov", data)
        self.assertIn("artifact_manifest", data)


if __name__ == "__main__":
    unittest.main()