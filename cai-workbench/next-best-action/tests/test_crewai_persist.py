"""Phase 8.4 CrewAI persist bridge."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from src.config import Settings
from src.crewai_persist import (
    BACKEND_JSON,
    run_orchestrator_with_persist,
    write_persist_manifest,
)
from src.maritime_incident_state import MaritimeIncidentState
from src.pipeline import run_phase7


class TestCrewaiPersist(unittest.TestCase):
    def test_manifest_written(self):
        path = write_persist_manifest(
            Path("/tmp/nba_crewai_manifest"),
            flow_uuid="f-1",
            backend=BACKEND_JSON,
            crewai_error="not installed",
        )
        self.assertTrue(path.is_file())

    def test_default_backend_json(self):
        state = MaritimeIncidentState(
            flow_uuid="f-2",
            phase=7,
            alert={"flow_uuid": "f-2"},
        )
        root = Path("/tmp/nba_crewai_orc")
        out = root / "f-2"
        out.mkdir(parents=True, exist_ok=True)
        state = run_orchestrator_with_persist(
            state,
            out,
            crewai_persist=False,
            simulations=None,
            decision_record={"sections": {"1": {}}},
        )
        self.assertEqual(state.persist_backend, BACKEND_JSON)
        self.assertTrue((out / "crewai_persist_manifest.json").is_file())

    @patch("src.crewai_persist._crewai_import_error", return_value=None)
    @patch("src.crewai_persist._crewai_persist_touch")
    def test_crewai_backend_when_flag(self, mock_touch, _err):
        touched = MaritimeIncidentState(
            flow_uuid="f-3",
            phase=7,
            alert={"flow_uuid": "f-3"},
            persist_backend="crewai",
        )
        mock_touch.return_value = touched
        state = MaritimeIncidentState(
            flow_uuid="f-3",
            phase=7,
            alert={"flow_uuid": "f-3"},
        )
        out = Path("/tmp/nba_crewai_crew") / "f-3"
        out.mkdir(parents=True, exist_ok=True)
        state = run_orchestrator_with_persist(
            state,
            out,
            crewai_persist=True,
            simulations=None,
            decision_record={"sections": {}},
        )
        self.assertEqual(state.persist_backend, "crewai")
        mock_touch.assert_called_once()

    def test_phase7_writes_access_context(self):
        settings = Settings()
        settings.demo_mode = True
        settings.offline_snapshots = True
        settings.phase = 7
        settings.skip_llm = True
        settings.artifacts_dir = Path("/tmp/nba_phase84_gov")
        result = run_phase7(settings)
        ac = result.artifact_dir / "access_context.json"
        manifest = result.artifact_dir / "crewai_persist_manifest.json"
        self.assertTrue(ac.is_file())
        self.assertTrue(manifest.is_file())
        self.assertEqual(result.agent_state.persist_backend, BACKEND_JSON)


if __name__ == "__main__":
    unittest.main()