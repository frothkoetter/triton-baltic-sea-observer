"""Phase 1: transit ETAs + lightweight Red-vs.-Blue discovery replay."""

from __future__ import annotations

import hashlib
import math
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_UPLOAD = Path(__file__).resolve().parents[3] / "nifi-processors" / "upload"
if _UPLOAD.is_dir() and str(_UPLOAD) not in sys.path:
    sys.path.insert(0, str(_UPLOAD.as_posix()))

from baltic_ship_routing import (  # noqa: E402
    SIMULATION_HOURS_PER_TICK,
    _GEOD,
    build_sea_route,
    move_towards_on_water,
)
from german_navy_mission_engine import (  # noqa: E402
    GERMAN_NAVAL_BASES,
    enrich_unit,
    home_base_for_unit,
    initialize_german_navy_ships,
    update_german_navy_movements,
)
from marine_fleet_registry import fleet_unit_for_index  # noqa: E402

from .alert_rules import NbaAlert
from .coa_catalog import COA_CATALOG
from .scoring import EnvironmentContext


# Knoten → Seemeilen/h (1 kn = 1 nm/h).
_PLATFORM_SPEED_KN: dict[str, float] = {
    "Korvetten": 24.0,
    "Fregatten": 22.0,
    "Minenabwehreinheiten": 15.0,
    "U-Boote": 12.0,
    "P-8A": 450.0,
    "NH90": 140.0,
}

_AIR_BASE = {
    "Nordholz": {"name": "Nordholz", "Latitude": 53.765, "Longitude": 8.658},
}

_COASTAL_FACTOR: dict[str, float] = {
    "Warnemünde": 1.15,
    "Kiel": 1.20,
    "Wilhelmshaven": 1.25,
    "Eckernförde": 1.18,
}


@dataclass(frozen=True)
class AssetEta:
    asset_class: str
    name: str
    base: str
    distance_nm: float
    speed_kn: float
    eta_hours: float
    operational: bool = True


@dataclass
class TickTrace:
    tick: int
    red_phase: str
    discovery_probability: float
    notes: str


@dataclass
class SimOutcome:
    coa_id: str
    ticks: int
    tick_traces: list[TickTrace] = field(default_factory=list)
    final_discovery: float = 0.0
    asset_etas: list[AssetEta] = field(default_factory=list)
    replay_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "coa_id": self.coa_id,
            "ticks": self.ticks,
            "final_discovery": round(self.final_discovery, 4),
            "asset_etas": [
                {
                    "asset_class": e.asset_class,
                    "name": e.name,
                    "base": e.base,
                    "distance_nm": round(e.distance_nm, 2),
                    "speed_kn": e.speed_kn,
                    "eta_hours": round(e.eta_hours, 2),
                }
                for e in self.asset_etas
            ],
            "tick_traces": [
                {
                    "tick": t.tick,
                    "red_phase": t.red_phase,
                    "discovery_probability": round(t.discovery_probability, 4),
                    "notes": t.notes,
                }
                for t in self.tick_traces
            ],
            "replay_summary": self.replay_summary,
        }


def _rng_for_flow(flow_uuid: str) -> random.Random:
    seed = int(hashlib.sha256(flow_uuid.encode()).hexdigest()[:8], 16)
    return random.Random(seed)


def geodesic_nm(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    _, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
    return abs(dist_m) / 1852.0


def eta_hours(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
    speed_kn: float,
    *,
    coastal_base: str | None = None,
) -> tuple[float, float]:
    nm = geodesic_nm(lon1, lat1, lon2, lat2)
    factor = _COASTAL_FACTOR.get(coastal_base or "", 1.0)
    sea_nm = nm * factor
    if speed_kn <= 0:
        return sea_nm, float("inf")
    return sea_nm, sea_nm / speed_kn


def compute_asset_etas(alert: NbaAlert) -> list[AssetEta]:
    lat, lon = alert.latitude, alert.longitude
    etas: list[AssetEta] = []

    for cls, speed in (
        ("Korvetten", _PLATFORM_SPEED_KN["Korvetten"]),
        ("Fregatten", _PLATFORM_SPEED_KN["Fregatten"]),
        ("Minenabwehreinheiten", _PLATFORM_SPEED_KN["Minenabwehreinheiten"]),
    ):
        for base_name, base in GERMAN_NAVAL_BASES.items():
            sea_nm, hours = eta_hours(
                float(base["Longitude"]),
                float(base["Latitude"]),
                lon,
                lat,
                speed,
                coastal_base=base_name,
            )
            etas.append(
                AssetEta(
                    asset_class=cls,
                    name=f"Nearest {cls} via {base_name}",
                    base=base_name,
                    distance_nm=sea_nm,
                    speed_kn=speed,
                    eta_hours=hours,
                )
            )

    air = _AIR_BASE["Nordholz"]
    sea_nm, hours = eta_hours(
        float(air["Longitude"]),
        float(air["Latitude"]),
        lon,
        lat,
        _PLATFORM_SPEED_KN["P-8A"],
    )
    etas.append(
        AssetEta(
            asset_class="P-8A",
            name="P-8A Poseidon",
            base="Nordholz",
            distance_nm=sea_nm,
            speed_kn=_PLATFORM_SPEED_KN["P-8A"],
            eta_hours=hours,
        )
    )

    etas.sort(key=lambda e: e.eta_hours)
    return etas


def _coa_blue_profile(coa_id: str) -> dict[str, bool]:
    return {
        "A": {
            "passive_sonar": True,
            "active_sonar": False,
            "sar": True,
            "surface_dispatch": True,
        },
        "B": {"passive_sonar": False, "active_sonar": False, "sar": False, "surface_dispatch": True},
        "C": {"passive_sonar": False, "active_sonar": False, "sar": False, "surface_dispatch": True},
        "D": {"passive_sonar": True, "active_sonar": False, "sar": False, "surface_dispatch": False},
    }.get(coa_id, {"passive_sonar": True, "active_sonar": False, "sar": False, "surface_dispatch": False})


def _discovery_step(
    *,
    tick: int,
    red_phase: str,
    profile: dict[str, bool],
    env: EnvironmentContext,
    discovery: float,
) -> tuple[float, str, str]:
    notes: list[str] = []
    new_phase = red_phase

    gain = 0.0
    if profile.get("sar") and tick <= 2:
        gain += 0.12
    if profile.get("passive_sonar"):
        gain += 0.10
    if profile.get("active_sonar") and red_phase == "surface":
        gain += 0.18
        new_phase = "submerged"
        notes.append("Red dives on active sonar")
    elif profile.get("active_sonar"):
        gain += 0.04
        notes.append("Active sonar; Red already deep")

    if red_phase == "submerged":
        gain *= 0.55
        if env.turbidity_high:
            gain *= 0.7
            notes.append("Halocline/turbidity attenuation")

    if env.seastate >= 4:
        gain *= 0.85
        notes.append(f"Seegang {env.seastate} limits bojen")

    if profile.get("surface_dispatch") and tick >= 4:
        gain += 0.05

    discovery = min(1.0, discovery + gain)
    return discovery, new_phase, "; ".join(notes) if notes else "hold"


def simulate_coa_ticks(
    coa_id: str,
    alert: NbaAlert,
    env: EnvironmentContext,
    *,
    ticks: int = 6,
) -> SimOutcome:
    profile = _coa_blue_profile(coa_id)
    etas = compute_asset_etas(alert)
    red_phase = "surface"
    discovery = 0.08
    traces: list[TickTrace] = []

    for tick in range(1, ticks + 1):
        discovery, red_phase, notes = _discovery_step(
            tick=tick,
            red_phase=red_phase,
            profile=profile,
            env=env,
            discovery=discovery,
        )
        traces.append(
            TickTrace(
                tick=tick,
                red_phase=red_phase,
                discovery_probability=discovery,
                notes=notes,
            )
        )

    return SimOutcome(
        coa_id=coa_id,
        ticks=ticks,
        tick_traces=traces,
        final_discovery=discovery,
        asset_etas=etas[:8],
    )


def replay_k130_approach(
    alert: NbaAlert,
    *,
    ticks: int = 3,
) -> dict[str, Any]:
    """One Korvette from Warnemünde: route + movement ticks (NiFi engine)."""
    rng = _rng_for_flow(alert.flow_uuid)
    ships = initialize_german_navy_ships(
        12,
        rng=rng,
        operational_status_options=["Fully Operational"],
        system_status_options=["All Systems Green"],
    )
    k130 = next((s for s in ships if s.get("Class") == "Korvetten"), ships[0])
    base = GERMAN_NAVAL_BASES["Warnemünde"]
    k130["Phase"] = "mission_patrol"
    k130["Latitude"] = base["Latitude"]
    k130["Longitude"] = base["Longitude"]
    k130["RouteWaypoints"] = build_sea_route(
        float(base["Longitude"]),
        float(base["Latitude"]),
        alert.longitude,
        alert.latitude,
    )
    k130["RouteIndex"] = 0
    k130["Speed"] = _PLATFORM_SPEED_KN["Korvetten"]

    start_nm = geodesic_nm(
        float(k130["Longitude"]),
        float(k130["Latitude"]),
        alert.longitude,
        alert.latitude,
    )

    for _ in range(ticks):
        update_german_navy_movements([k130], rng=rng)

    end_nm = geodesic_nm(
        float(k130["Longitude"]),
        float(k130["Latitude"]),
        alert.longitude,
        alert.latitude,
    )
    return {
        "ship": k130.get("Name"),
        "start_nm_to_incident": round(start_nm, 2),
        "end_nm_to_incident": round(end_nm, 2),
        "ticks": ticks,
        "sim_hours": ticks * SIMULATION_HOURS_PER_TICK,
    }


def simulate_passing_coas(
    alert: NbaAlert,
    env: EnvironmentContext,
    *,
    ticks: int = 6,
) -> list[SimOutcome]:
    outcomes: list[SimOutcome] = []
    for coa in COA_CATALOG:
        out = simulate_coa_ticks(coa.coa_id, alert, env, ticks=ticks)
        if coa.coa_id == "A":
            out.replay_summary = replay_k130_approach(alert, ticks=min(3, ticks))
        outcomes.append(out)
    return outcomes


def apply_simulation_to_scores(
    ranked_scores: list,
    simulations: list[SimOutcome],
) -> list:
    """Blend template discovery with simulated final_discovery for passing COAs."""
    by_id = {s.coa_id: s for s in simulations}
    for score in ranked_scores:
        sim = by_id.get(score.coa.coa_id)
        if not sim or not score.gates_passed:
            continue
        blended = 0.5 * score.score_entdeckung + 0.5 * sim.final_discovery
        latenz_bonus = 0.0
        k130_eta = next(
            (e.eta_hours for e in sim.asset_etas if e.base == "Warnemünde" and e.asset_class == "Korvetten"),
            None,
        )
        if k130_eta is not None and k130_eta <= 4.0:
            latenz_bonus = 0.05
        score.score_entdeckung = blended
        score.score_total = (
            0.35 * score.score_latenz
            + 0.35 * blended
            + 0.15 * score.score_eskalation
            + 0.15 * score.score_kosten
            + latenz_bonus
        )
    ranked_scores.sort(
        key=lambda s: (not s.gates_passed, -s.score_total, s.coa.coa_id)
    )
    return ranked_scores