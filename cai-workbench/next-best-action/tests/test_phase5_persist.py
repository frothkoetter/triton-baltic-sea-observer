"""Phase 5 Trino decision_record update SQL shape."""

from __future__ import annotations

import unittest
from unittest import mock

from src.config import Settings
from src.persistence import update_decision_artifacts


class TestPhase5Persist(unittest.TestCase):
    def test_update_decision_sql(self):
        settings = Settings()
        with mock.patch("src.persistence.trino_client.execute_sql") as execute:
            update_decision_artifacts(
                "abc-def",
                decision_record={"flow_uuid": "abc-def", "sections": {}},
                grounding={"rule_citations": []},
                settings=settings,
            )
        sql = execute.call_args[0][0]
        self.assertIn("decision_record_json", sql)
        self.assertIn("grounding_json", sql)
        self.assertIn("abc-def", sql)


if __name__ == "__main__":
    unittest.main()