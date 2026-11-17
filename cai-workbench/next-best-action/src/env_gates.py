"""Physical environment gates for sensors (Phase 1 stub)."""

from __future__ import annotations

from dataclasses import dataclass

from .scoring import EnvironmentContext


@dataclass(frozen=True)
class SensorGateResult:
    passive_buoy_ok: bool
    alb_ok: bool
    optical_eo_ok: bool
    messages: list[str]

    def to_dict(self) -> dict:
        return {
            "passive_buoy_ok": self.passive_buoy_ok,
            "alb_ok": self.alb_ok,
            "optical_eo_ok": self.optical_eo_ok,
            "messages": self.messages,
        }


def evaluate_sensor_gates(env: EnvironmentContext) -> SensorGateResult:
    msgs: list[str] = []
    buoy_ok = env.seastate <= 5
    if not buoy_ok:
        msgs.append("AN/SSQ-955 limited above seastate 5")
    alb_ok = not env.turbidity_high and env.water_depth_m <= 15.0
    if not alb_ok:
        msgs.append("ALB blocked (turbidity or depth > 15m)")
    eo_ok = not env.night and env.cloud_cover_pct <= 15.0
    if not eo_ok:
        msgs.append("Optical EO blocked (night or clouds > 15%)")
    return SensorGateResult(
        passive_buoy_ok=buoy_ok,
        alb_ok=alb_ok,
        optical_eo_ok=eo_ok,
        messages=msgs,
    )