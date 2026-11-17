"""Golden-path: Greifswald demo + Warnemünde K130 ETA anchor."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_WORKSPACE = Path(__file__).resolve().parents[3]
_UPLOAD = _WORKSPACE / "nifi-processors" / "upload"
for p in (_UPLOAD.as_posix(), _WORKSPACE.as_posix()):
    if p not in sys.path:
        sys.path.insert(0, p)

from german_navy_mission_engine import GERMAN_NAVAL_BASES  # noqa: E402

from src.config import Settings
from src.pipeline import run_phase1


class TestGreifswaldScenario(unittest.TestCase):
    def test_warnemuende_base_exists(self):
        self.assertIn("Warnemünde", GERMAN_NAVAL_BASES)

    def test_phase0_offline_pipeline_best_a(self):
        settings = Settings()
        settings.offline_snapshots = True
        settings.demo_mode = True
        settings.artifacts_dir = Path("/tmp/nba_test_artifacts")
        settings.phase = 1
        result = run_phase1(settings)
        self.assertEqual(result.alert.theatre, "Greifswald / Adlergrund")
        self.assertAlmostEqual(result.alert.latitude, 54.55, places=1)
        self.assertEqual(result.best_coa_id, "A")
        audit = result.artifact_dir / "audit.json"
        sim = result.artifact_dir / "simulation_trace.json"
        self.assertTrue(audit.is_file())
        self.assertTrue(sim.is_file())
        self.assertIsNotNone(result.simulations)