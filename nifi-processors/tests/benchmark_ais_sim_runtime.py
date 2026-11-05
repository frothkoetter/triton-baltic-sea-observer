#!/usr/bin/env python3
"""One-shot AIS sim timing (50 ships). Run: PYTHONPATH=upload:tests python3 tests/benchmark_ais_sim_runtime.py"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import types

for mod in ("nifiapi", "nifiapi.flowfiletransform", "nifiapi.properties"):
    sys.modules.setdefault(mod, types.ModuleType(mod))
sys.modules["nifiapi.flowfiletransform"].FlowFileTransform = object
sys.modules["nifiapi.flowfiletransform"].FlowFileTransformResult = dict
sys.modules["nifiapi.properties"].PropertyDescriptor = lambda **k: k
sys.modules["nifiapi.properties"].StandardValidators = types.SimpleNamespace(
    POSITIVE_INTEGER_VALIDATOR=None
)

import ShipSimulationRoutesGemini as sim  # noqa: E402
import baltic_ship_routing as bsr  # noqa: E402


def main() -> None:
    num = int(os.environ.get("NUM_SHIPS", "50"))
    tmpdir = tempfile.mkdtemp()
    sim.STATE_FILE = os.path.join(tmpdir, "ship_state.json")

    bsr._ROUTE_CACHE.clear()
    for name in (
        "_STATIC_GRAPH_NODES",
        "_STATIC_GRAPH_ADJ",
        "_ROUTING_SEEDS",
        "_MAIN_SEED_IDX",
        "_CHANNEL_LANE_NODES",
    ):
        setattr(bsr, name, None)
    bsr._path_with_static_graph_cached.cache_clear()

    print(f"AIS sim runtime benchmark ({num} ships)")
    t0 = time.perf_counter()
    ships = sim.initialize_ships(num)
    t_init = time.perf_counter() - t0
    routed = sum(1 for s in ships if s.get("RouteWaypoints"))
    print(f"  initialize_ships (cold): {t_init:.2f}s  routed={routed}/{num}")

    t0 = time.perf_counter()
    ships = sim.update_ship_movements(ships)
    print(f"  update_ship_movements (tick 1): {time.perf_counter() - t0:.3f}s")

    sim.save_ship_state(ships)
    t0 = time.perf_counter()
    ships = sim.load_ship_state(num)
    t_load = time.perf_counter() - t0
    t0 = time.perf_counter()
    ships = sim.update_ship_movements(ships)
    t_tick = time.perf_counter() - t0
    t0 = time.perf_counter()
    for s in ships:
        sim.generate_ais_message(s)
    t_ais = time.perf_counter() - t0
    print(
        f"  steady NiFi run (load+tick+AIS): {t_load + t_tick + t_ais:.3f}s"
        f"  (load={t_load:.3f}s tick={t_tick:.3f}s ais={t_ais:.3f}s)"
    )


if __name__ == "__main__":
    main()
