from __future__ import annotations

import unittest

from src.mission_dispatch import (
    build_mission_order,
    expand_buoy_roles,
    resolve_target_from_incident,
    validate_order_against_active,
)


class TestMissionDispatch(unittest.TestCase):
    def test_coa_a_combined_multidomain_buoys(self):
        incident = {
            "flow_uuid": "demo-flow",
            "latitude": 54.55,
            "longitude": 13.45,
            "approved_coa_id": "A",
            "alert_payload": '{"target_mmsi":"211234567","sog_kn":8,"cog_deg":95}',
        }
        order = build_mission_order(incident)
        self.assertEqual(order.mission_type, "COMBINED")
        self.assertEqual(order.target.mode, "VESSEL_MMSI")
        self.assertEqual(order.target.mmsi, "211234567")
        self.assertEqual(len(expand_buoy_roles("A")), 6)
        self.assertTrue(order.parameters_json["dynamic_tracking"]["enabled"])
        self.assertEqual(order.parameters_json["buoys"]["formation"], "LEADING_SEMICIRCLE")

    def test_coa_d_passive_only_no_surface(self):
        incident = {
            "flow_uuid": "x",
            "latitude": 55.0,
            "longitude": 15.0,
            "approved_coa_id": "D",
            "alert_payload": "{}",
        }
        order = build_mission_order(incident)
        self.assertEqual(order.mission_type, "BUOY_TRACK_ONLY")
        self.assertEqual(order.parameters_json["surface"]["dispatch_units"], 0)
        self.assertEqual(len(expand_buoy_roles("D")), 8)

    def test_parallel_cap_same_flow_uuid(self):
        incident = {
            "flow_uuid": "same-flow",
            "latitude": 54.0,
            "longitude": 11.0,
            "approved_coa_id": "A",
        }
        order = build_mission_order(incident)
        active = [{"flow_uuid": "same-flow", "status": "ACTIVE", "parameters_json": order.parameters_json}]
        ok, reasons = validate_order_against_active(order, active)
        self.assertFalse(ok)
        self.assertTrue(any("incident" in r for r in reasons))

    def test_static_fallback_without_mmsi(self):
        from src.mission_dispatch import load_playbook

        incident = {"latitude": 54.1, "longitude": 12.0, "best_coa_id": "B"}
        target = resolve_target_from_incident(incident, load_playbook(), "B")
        self.assertEqual(target.mode, "STATIC_POINT")


if __name__ == "__main__":
    unittest.main()