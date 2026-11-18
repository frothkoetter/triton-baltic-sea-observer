"""Agentic Red vs Blue discrete-action replay (deterministic policies)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .alert_types import NbaAlert
from .scoring import EnvironmentContext
from .sim_adapter import _coa_blue_profile, _discovery_step, compute_asset_etas


RED_ACTIONS: dict[str, tuple[str, ...]] = {
    "cooperative": ("surface_transit", "maintain_ais", "hold"),
    "evasive": ("go_dark_ais", "halocline_dip", "surface_transit"),
    "aggressive": ("jam_gps", "pipeline_loiter", "submerge"),
}


@dataclass
class GameplayTick:
    tick: int
    blue_action: str
    red_action: str
    red_phase: str
    discovery_probability: float
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "tick": self.tick,
            "blue_action": self.blue_action,
            "red_action": self.red_action,
            "red_phase": self.red_phase,
            "discovery_probability": round(self.discovery_probability, 4),
            "notes": self.notes,
        }


@dataclass
class RedBlueReplay:
    coa_id: str
    red_doctrine: str
    ticks: list[GameplayTick] = field(default_factory=list)
    blue_reward: float = 0.0
    red_reward: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "coa_id": self.coa_id,
            "red_doctrine": self.red_doctrine,
            "ticks": [t.to_dict() for t in self.ticks],
            "blue_reward": round(self.blue_reward, 4),
            "red_reward": round(self.red_reward, 4),
        }


def _blue_action_for_tick(tick: int, profile: dict[str, bool]) -> str:
    if tick == 1 and profile.get("sar"):
        return "cue_sar_pass"
    if tick <= 2 and profile.get("passive_sonar"):
        return "deploy_passive_buoy_line"
    if profile.get("surface_dispatch") and tick >= 3:
        return "dispatch_k130_approach"
    if profile.get("active_sonar"):
        return "active_sonar_ping"
    return "hold_partner_isr"


def run_red_blue_episode(
    alert: NbaAlert,
    env: EnvironmentContext,
    *,
    coa_id: str = "A",
    red_doctrine: str = "evasive",
    ticks: int = 6,
) -> RedBlueReplay:
    profile = _coa_blue_profile(coa_id)
    red_moves = RED_ACTIONS.get(red_doctrine, RED_ACTIONS["evasive"])
    red_phase = "surface"
    discovery = 0.08
    trace: list[GameplayTick] = []

    for tick in range(1, ticks + 1):
        blue = _blue_action_for_tick(tick, profile)
        red = red_moves[(tick - 1) % len(red_moves)]
        if red == "submerge" or red == "halocline_dip":
            red_phase = "submerged"
        discovery, red_phase, sim_notes = _discovery_step(
            tick=tick,
            red_phase=red_phase,
            profile=profile,
            env=env,
            discovery=discovery,
        )
        notes = f"Blue:{blue}; Red:{red}; {sim_notes}"
        trace.append(
            GameplayTick(
                tick=tick,
                blue_action=blue,
                red_action=red,
                red_phase=red_phase,
                discovery_probability=discovery,
                notes=notes,
            )
        )

    compute_asset_etas(alert)
    blue_reward = discovery * 0.7 + (0.2 if profile.get("passive_sonar") else 0.0)
    red_reward = (1.0 - discovery) * 0.8 + (0.1 if red_phase == "submerged" else 0.0)

    return RedBlueReplay(
        coa_id=coa_id,
        red_doctrine=red_doctrine,
        ticks=trace,
        blue_reward=blue_reward,
        red_reward=red_reward,
    )