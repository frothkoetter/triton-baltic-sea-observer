"""Phase 3: playbook, state persist, gps_jammer dedupe SQL shape."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest import mock

from src.alert_rules import demo_greifswald_alert, recent_incident_exists
from src.alert_types import NbaAlert
from src.config import Settings
from src.pipeline import run_phase3
from src.rules_engine import evaluate_playbook
from src.scoring import EnvironmentContext, rank_coas


class TestPlaybook(unittest.TestCase):
    def test_gps_jammer_blocks_heavy_surface_unless_flash(self):
        alert = demo_greifswald_alert(Settings())
        alert.alert_type = "gps_jammer"
        alert.priority = "PRIORITY"
        alert.payload = dict(alert.payload)
        alert.payload["cloud_cover_pct"] = 0
        alert.payload["night"] = False
        env = EnvironmentContext.from_alert_payload(alert.payload)
        ranked = rank_coas(env)
        legal = evaluate_playbook(alert, ranked)
        self.assertIn("JAMMER_PASSIVE_BIAS", legal.applicable_rule_ids)
        self.assertIn("B", legal.coa_flags_blocked)


class TestDedupeGeohash(unittest.TestCase):
    def test_sql_uses_lagebild_id(self):
        settings = Settings()
        alert = NbaAlert(
            flow_uuid="x",
            alert_type="gps_jammer",
            priority="FLASH",
            latitude=1.0,
            longitude=2.0,
            theatre="T",
            created_at=demo_greifswald_alert(settings).created_at,
            payload={"lagebild_id": "u3qr1d"},
        )
        with mock.patch("src.alert_rules.trino_client.fetch_all") as fetch:
            fetch.return_value = [{"flow_uuid": "old"}]
            self.assertTrue(recent_incident_exists(alert, settings))
        sql = fetch.call_args[0][0]
        self.assertIn("json_extract_scalar", sql)
        self.assertIn("u3qr1d", sql)


class TestAgentOffline(unittest.TestCase):
    def test_phase3_writes_incident_state(self):
        settings = Settings()
        settings.demo_mode = True
        settings.offline_snapshots = True
        settings.skip_llm = True
        settings.phase = 3
        root = Path("/tmp/nba_phase3_test")
        settings.artifacts_dir = root
        result = run_phase3(settings)
        state_path = result.artifact_dir / "incident_state.json"
        self.assertTrue(state_path.is_file())
        data = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(data["flow_uuid"], result.alert.flow_uuid)
        self.assertTrue(data["agent_narrative"])
        self.assertTrue((result.artifact_dir / "agent_narrative.md").is_file())


if __name__ == "__main__":
    unittest.main()