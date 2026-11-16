#!/usr/bin/env python3
"""NSB trade sim timing. Run: PYTHONPATH=upload:tests python3 tests/benchmark_nsb_trade_runtime.py"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import types

for mod in ("nifiapi", "nifiapi.flowfiletransform", "nifiapi.properties"):
    sys.modules.setdefault(mod, types.ModuleType(mod))

import baltic_ship_routing as bsr  # noqa: E402
import north_sea_baltic_fleet_registry as reg  # noqa: E402
import north_sea_baltic_transit_engine as nsb  # noqa: E402


def _clear_routing_cache() -> None:
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


def main() -> None:
    num = int(os.environ.get("NUM_SHIPS", "1000"))
    tmpdir = tempfile.mkdtemp()
    reg.NSB_TRADE_STATE_FILE = os.path.join(tmpdir, "nsb_trade_state.json")
    _clear_routing_cache()

    print(f"NSB trade runtime benchmark ({num} ships)")
    t0 = time.perf_counter()
    nsb.warm_nsb_pair_routes()
    t_warm = time.perf_counter() - t0
    print(f"  warm pair routes (once): {t_warm:.2f}s")
    t0 = time.perf_counter()
    ships = nsb.initialize_nsb_trade_ships(num)
    t_init = time.perf_counter() - t0
    moored = sum(1 for s in ships if str(s.get("Phase", "")).startswith("moored"))
    routed = sum(1 for s in ships if s.get("RouteWaypoints"))
    print(f"  initialize (cold): {t_init:.2f}s  moored={moored}/{num} pre_routed={routed}/{num}")

    t0 = time.perf_counter()
    ships = nsb.update_nsb_trade_movements(ships)
    t_tick1 = time.perf_counter() - t0
    print(f"  update tick 1 (routes build): {t_tick1:.3f}s")

    nsb.save_nsb_trade_state(ships)
    # Second movement tick = typical 1-min NiFi cadence after routes are assigned
    t0 = time.perf_counter()
    ships = nsb.update_nsb_trade_movements(ships)
    t_tick2 = time.perf_counter() - t0
    t0 = time.perf_counter()
    ships = nsb.load_nsb_trade_state(num)
    t_load = time.perf_counter() - t0
    t0 = time.perf_counter()
    ships = nsb.update_nsb_trade_movements(ships)
    t_tick = time.perf_counter() - t0
    t0 = time.perf_counter()
    for s in ships:
        nsb.generate_nsb_ais_message(s)
    t_ais = time.perf_counter() - t0
    steady = t_load + t_tick + t_ais
    print(f"  update tick 2 (steady movement): {t_tick2:.3f}s")
    print(
        f"  steady NiFi run (load+tick+AIS): {steady:.3f}s"
        f"  (load={t_load:.3f}s tick={t_tick:.3f}s ais={t_ais:.3f}s)"
    )
    ok = steady < 55.0
    print(f"  within_60s_schedule: {ok}")


if __name__ == "__main__":
    main()