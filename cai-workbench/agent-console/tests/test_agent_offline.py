import unittest

from src.crew_runtime import run_agent_chat
from src.ontology import load_ontology, ontology_summary
from src.apps import apps_catalog


class TestAgentOffline(unittest.TestCase):
    def test_ontology_loads(self):
        onto = load_ontology()
        self.assertIn("alert_types", onto)
        summary = ontology_summary(onto)
        self.assertGreaterEqual(len(summary["concepts"]), 3)

    def test_catalog(self):
        self.assertGreaterEqual(len(apps_catalog()), 4)

    def test_offline_chat(self):
        import os

        os.environ["TRITON_AGENT_MODE"] = "offline"
        res = run_agent_chat(
            app_id="coa-advisor",
            messages=[{"role": "user", "content": "OPSEC GPS jamming"}],
        )
        self.assertTrue(res["ok"])
        self.assertEqual(res["engine"], "offline")
        self.assertIn("assistant", res["message"]["role"])


if __name__ == "__main__":
    unittest.main()