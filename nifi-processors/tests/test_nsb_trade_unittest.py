"""NSB trade engine regression tests."""

from __future__ import annotations

import os
import tempfile
import unittest

import north_sea_baltic_fleet_registry as reg
import north_sea_baltic_transit_engine as nsb


class NsbTradeTests(unittest.TestCase):
    def test_fleet_deterministic(self) -> None:
        a = reg.fleet_unit_for_index(42)
        b = reg.fleet_unit_for_index(42)
        self.assertEqual(a["mmsi"], b["mmsi"])
        self.assertEqual(a["name"], b["name"])
        self.assertEqual(a["north_sea_hub"], b["north_sea_hub"])

    def test_mmsi_range(self) -> None:
        u = reg.fleet_unit_for_index(999)
        self.assertEqual(u["mmsi"], str(reg.MMSI_BASE + 999))

    def test_moored_tick_no_route_work(self) -> None:
        ships = nsb.initialize_nsb_trade_ships(20)
        moored = [s for s in ships if s.get("Phase") in nsb.MOORED_PHASES]
        self.assertGreater(len(moored), 0)
        before_routes = sum(len(s.get("RouteWaypoints") or []) for s in moored)
        for s in moored:
            s["RouteWaypoints"] = []
        nsb.update_nsb_trade_movements(moored)
        after_routes = sum(len(s.get("RouteWaypoints") or []) for s in moored)
        self.assertEqual(before_routes, 0)
        self.assertEqual(after_routes, 0)

    def test_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            reg.NSB_TRADE_STATE_FILE = os.path.join(tmp, "nsb.json")
            ships = nsb.initialize_nsb_trade_ships(10)
            nsb.save_nsb_trade_state(ships)
            loaded = nsb.load_nsb_trade_state(10)
            self.assertEqual(len(loaded), 10)
            self.assertEqual(loaded[0]["MMSI"], ships[0]["MMSI"])


if __name__ == "__main__":
    unittest.main()