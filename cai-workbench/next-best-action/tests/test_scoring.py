import unittest

from src.coa_catalog import COA_CATALOG
from src.scoring import EnvironmentContext, rank_coas


class TestScoring(unittest.TestCase):
    def test_greifswald_night_picks_coa_a(self):
        env = EnvironmentContext(
            cloud_cover_pct=82,
            night=True,
            seastate=4,
            turbidity_high=True,
            water_depth_m=18,
        )
        ranked = rank_coas(env)
        best = ranked[0]
        self.assertTrue(best.gates_passed)
        self.assertEqual(best.coa.coa_id, "A")
        failed_ids = {s.coa.coa_id for s in ranked if not s.gates_passed}
        self.assertIn("B", failed_ids)
        self.assertIn("C", failed_ids)

    def test_catalog_has_four_coas(self):
        self.assertEqual(len(COA_CATALOG), 4)