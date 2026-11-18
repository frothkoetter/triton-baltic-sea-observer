"""Multi-scenario COA discovery fan (deterministic)."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .alert_types import NbaAlert
from .scoring import EnvironmentContext, rank_coas
from .sim_adapter import SimOutcome, simulate_coa_ticks

SCENARIO_PRESETS: tuple[dict[str, Any], ...] = (
    {
        "scenario_id": "baseline",
        "label": "Baseline (current env)",
        "red_doctrine": "evasive",
        "weather_draw": "as_observed",
        "blue_delay_h": 0.0,
        "partner_isr": True,
    },
    {
        "scenario_id": "aggressive_red",
        "label": "Aggressive Red (fast go-dark)",
        "red_doctrine": "aggressive",
        "weather_draw": "as_observed",
        "blue_delay_h": 0.0,
        "partner_isr": True,
    },
    {
        "scenario_id": "weather_stress",
        "label": "High seastate",
        "red_doctrine": "evasive",
        "weather_draw": "high_seastate",
        "blue_delay_h": 0.0,
        "partner_isr": False,
    },
    {
        "scenario_id": "c2_delay",
        "label": "Blue C2 delay +2h",
        "red_doctrine": "cooperative",
        "weather_draw": "as_observed",
        "blue_delay_h": 2.0,
        "partner_isr": True,
    },
)


def _env_for_draw(base: EnvironmentContext, draw: str) -> EnvironmentContext:
    env = deepcopy(base)
    if draw == "high_seastate":
        env.seastate = max(env.seastate, 5)
    return env


def _discovery_adjust(
    outcome: SimOutcome,
    *,
    red_doctrine: str,
    partner_isr: bool,
    blue_delay_h: float,
) -> float:
    d = outcome.final_discovery
    if red_doctrine == "aggressive":
        d *= 0.88
    elif red_doctrine == "cooperative":
        d *= 1.08
    if not partner_isr:
        d *= 0.92
    if blue_delay_h >= 2.0:
        d *= 0.9
    return min(1.0, max(0.0, d))


@dataclass
class ScenarioRun:
    scenario_id: str
    label: str
    coa_id: str
    final_discovery: float
    ticks: int
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "label": self.label,
            "coa_id": self.coa_id,
            "final_discovery": round(self.final_discovery, 4),
            "ticks": self.ticks,
            "notes": self.notes,
        }


def run_scenario_matrix(
    alert: NbaAlert,
    env: EnvironmentContext,
    *,
    coa_id: str | None = None,
    ticks: int = 6,
) -> list[ScenarioRun]:
    ranked = rank_coas(env)
    target = coa_id or next((s.coa.coa_id for s in ranked if s.gates_passed), "A")
    runs: list[ScenarioRun] = []

    for preset in SCENARIO_PRESETS:
        scen_env = _env_for_draw(env, preset["weather_draw"])
        out = simulate_coa_ticks(target, alert, scen_env, ticks=ticks)
        fd = _discovery_adjust(
            out,
            red_doctrine=preset["red_doctrine"],
            partner_isr=bool(preset["partner_isr"]),
            blue_delay_h=float(preset["blue_delay_h"]),
        )
        notes = (
            f"red={preset['red_doctrine']}; weather={preset['weather_draw']}; "
            f"delay={preset['blue_delay_h']}h; partner_isr={preset['partner_isr']}"
        )
        runs.append(
            ScenarioRun(
                scenario_id=preset["scenario_id"],
                label=preset["label"],
                coa_id=target,
                final_discovery=fd,
                ticks=ticks,
                notes=notes,
            )
        )
    return runs