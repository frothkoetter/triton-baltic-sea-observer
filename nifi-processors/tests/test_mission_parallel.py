from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1] / "upload"
if _ROOT.as_posix() not in sys.path:
    sys.path.insert(0, _ROOT.as_posix())

from mission_parallel import (  # noqa: E402
    activate_mission_from_order,
    allocated_mmsis,
    buoy_id_for_mission,
    load_runtime_store,
    mission_id_short,
    parallel_resource_totals,
    save_runtime_store,
    validate_parallel_activation,
)


def _order(mid: str, coa: str = "A", flow: str = "f1", surface: int = 1, buoys: int = 6) -> dict:
    return {
        "mission_id": mid,
        "flow_uuid": flow,
        "status": "QUEUED",
        "approved_coa_id": coa,
        "target_mode": "STATIC_POINT",
        "target_latitude": 54.5,
        "target_longitude": 13.0,
        "parameters_json": {
            "surface": {"dispatch_units": surface, "unit_class": "Korvetten"},
            "buoys": {"count": buoys, "sensor_profile": "MAD"},
            "dynamic_tracking": {"track_duration_min": 60},
        },
    }


class TestMissionParallel(unittest.TestCase):
    def test_buoy_ids_unique_per_mission(self):
        a = buoy_id_for_mission("aaaa-bbbb-cccc", 1)
        b = buoy_id_for_mission("dddd-eeee-ffff", 1)
        self.assertNotEqual(a, b)
        self.assertTrue(a.startswith("MAD-"))

    def test_two_missions_parallel_different_mmsi(self):
        store = load_runtime_store()
        o1 = _order(str(__import__("uuid").uuid4()), flow="flow-1")
        o2 = _order(str(__import__("uuid").uuid4()), flow="flow-2", surface=1, buoys=3)
        v1 = activate_mission_from_order(store, o1)
        v2 = activate_mission_from_order(store, o2)
        self.assertTrue(v1.ok and v2.ok)
        taken = allocated_mmsis(store)
        self.assertEqual(len(taken), 2)
        active, surface, buoys = parallel_resource_totals(store)
        self.assertEqual(active, 2)
        self.assertEqual(surface, 2)
        self.assertEqual(buoys, 9)

    def test_surface_cap_blocks_third_mission(self):
        store = load_runtime_store()
        for i in range(2):
            activate_mission_from_order(store, _order(f"m{i}", flow=f"f{i}"))
        verdict = validate_parallel_activation(store, needs_surface=1, needs_buoys=0, max_surface=2)
        self.assertFalse(verdict.ok)

    def test_persist_reload(self):
        with tempfile.TemporaryDirectory() as td:
            path = f"{td}/runtime.json"
            store = load_runtime_store()
            activate_mission_from_order(store, _order("persist-1"))
            save_runtime_store(store, path)
            loaded = load_runtime_store(path)
            self.assertIn("persist-1", loaded.missions)
            self.assertEqual(loaded.missions["persist-1"].status, "ACTIVE")

    def test_mission_id_short_stable(self):
        self.assertEqual(len(mission_id_short("12345678-abcd-efgh")), 8)


if __name__ == "__main__":
    unittest.main()