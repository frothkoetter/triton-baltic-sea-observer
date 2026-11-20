"""MP-1 SAP readiness gates on COA matrix."""

from __future__ import annotations

import os
import unittest

from src.config import Settings
from src.sap_readiness import (
    apply_sap_readiness_gates,
    evaluate_sap_for_coa,
    fetch_mission_readiness,
)
from src.scoring import EnvironmentContext, rank_coas


class TestSapReadinessGates(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["NBA_SAP_READINESS_MODE"] = "sample"

    def tearDown(self) -> None:
        os.environ.pop("NBA_SAP_READINESS_MODE", None)

    def test_sample_loads_units(self) -> None:
        ctx = fetch_mission_readiness(Settings())
        self.assertIsNotNone(ctx)
        assert ctx is not None
        self.assertEqual(ctx.source, "sap_sample")
        self.assertGreaterEqual(len(ctx.units), 4)

    def test_coa_b_blocked_by_f124_r3(self) -> None:
        ctx = fetch_mission_readiness(Settings())
        assert ctx is not None
        ok, reasons = evaluate_sap_for_coa("B", ctx)
        self.assertFalse(ok)
        self.assertTrue(any("Sachsen" in r or "F124" in r for r in reasons))

    def test_coa_a_executable_in_sample(self) -> None:
        ctx = fetch_mission_readiness(Settings())
        assert ctx is not None
        ok, _ = evaluate_sap_for_coa("A", ctx)
        self.assertTrue(ok)

    def test_apply_gates_keeps_a_best_after_env_pass(self) -> None:
        env = EnvironmentContext(
            cloud_cover_pct=82,
            night=True,
            seastate=4,
            turbidity_high=True,
            water_depth_m=18,
        )
        ranked = rank_coas(env)
        ctx = fetch_mission_readiness(Settings())
        assert ctx is not None
        merged, section = apply_sap_readiness_gates(ranked, ctx)
        self.assertIn("coa_impact", section)
        self.assertFalse(section["coa_impact"]["B"]["executable"])
        best = next(s for s in merged if s.gates_passed)
        self.assertEqual(best.coa.coa_id, "A")
        failed_b = next(s for s in merged if s.coa.coa_id == "B")
        self.assertFalse(failed_b.gates_passed)
        self.assertTrue(
            any(r.startswith("sap_readiness:") for r in failed_b.gate_fail_reasons)
        )


if __name__ == "__main__":
    unittest.main()