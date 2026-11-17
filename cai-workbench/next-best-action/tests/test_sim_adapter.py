import unittest

from src.alert_rules import demo_greifswald_alert
from src.config import Settings
from src.scoring import EnvironmentContext, rank_coas
from src.sim_adapter import (
    apply_simulation_to_scores,
    compute_asset_etas,
    geodesic_nm,
    replay_k130_approach,
    simulate_coa_ticks,
)


class TestSimAdapter(unittest.TestCase):
    def test_warnemuende_k130_eta_order_of_magnitude(self):
        settings = Settings()
        alert = demo_greifswald_alert(settings)
        etas = compute_asset_etas(alert)
        k130_wm = [
            e
            for e in etas
            if e.asset_class == "Korvetten" and e.base == "Warnemünde"
        ][0]
        self.assertGreater(k130_wm.eta_hours, 2.5)
        self.assertLess(k130_wm.eta_hours, 6.0)

    def test_coa_a_beats_others_after_sim_blend(self):
        settings = Settings()
        alert = demo_greifswald_alert(settings)
        env = EnvironmentContext.from_alert_payload(alert.payload)
        ranked = rank_coas(env)
        sims = [simulate_coa_ticks("A", alert, env, ticks=6)]
        ranked = apply_simulation_to_scores(ranked, sims)
        best = [s for s in ranked if s.gates_passed][0]
        self.assertEqual(best.coa.coa_id, "A")

    def test_k130_replay_runs_nifi_engine_ticks(self):
        settings = Settings()
        alert = demo_greifswald_alert(settings)
        summary = replay_k130_approach(alert, ticks=3)
        self.assertEqual(summary["ticks"], 3)
        self.assertIn("ship", summary)
        self.assertGreater(summary["start_nm_to_incident"], 50.0)

    def test_geodesic_warnemuende_greifswald_nm(self):
        # Rough check ~70–90 nm great-circle Warnemünde → Greifswald corridor
        wm = (12.089, 54.176)
        gp = (14.10, 54.55)
        nm = geodesic_nm(wm[0], wm[1], gp[0], gp[1])
        self.assertGreater(nm, 60.0)
        self.assertLess(nm, 100.0)