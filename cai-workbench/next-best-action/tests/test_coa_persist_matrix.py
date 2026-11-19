"""Persist full COA matrix including gate failures."""

from __future__ import annotations

import unittest
from unittest import mock

from src.config import Settings
from src.persistence import insert_coa_scores
from src.coa_catalog import COA_CATALOG
from src.scoring import CoaScore


class TestCoaPersistMatrix(unittest.TestCase):
    def test_inserts_failed_and_passing_coas(self):
        ranked: list[CoaScore] = []
        for i, c in enumerate(COA_CATALOG):
            ranked.append(
                CoaScore(
                    coa=c,
                    gates_passed=i == 0,
                    gate_fail_reasons=[] if i == 0 else ["test-fail"],
                    score_latenz=0.5,
                    score_entdeckung=0.5,
                    score_eskalation=0.5,
                    score_kosten=0.5,
                    score_total=0.5 if i == 0 else 0.0,
                )
            )
        settings = Settings()
        with mock.patch("src.persistence.trino_client.execute_sql") as execute:
            insert_coa_scores("uuid-1", ranked, settings)
        self.assertEqual(execute.call_count, 4)
        sql_joined = " ".join(c[0][0] for c in execute.call_args_list)
        self.assertIn("FALSE", sql_joined)
        self.assertIn("TRUE", sql_joined)


if __name__ == "__main__":
    unittest.main()