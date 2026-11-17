"""Deterministic COA gates and weighted scoring (no LLM)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .coa_catalog import COA_CATALOG, CoaTemplate


@dataclass
class EnvironmentContext:
    cloud_cover_pct: float = 0.0
    night: bool = False
    seastate: int = 0
    turbidity_high: bool = False
    water_depth_m: float = 20.0

    @classmethod
    def from_alert_payload(cls, payload: dict[str, Any]) -> EnvironmentContext:
        return cls(
            cloud_cover_pct=float(payload.get("cloud_cover_pct", 0)),
            night=bool(payload.get("night", False)),
            seastate=int(payload.get("seastate", 0)),
            turbidity_high=bool(payload.get("turbidity_high", False)),
            water_depth_m=float(payload.get("water_depth_m", 20)),
        )


@dataclass
class CoaScore:
    coa: CoaTemplate
    gates_passed: bool
    gate_fail_reasons: list[str]
    score_latenz: float
    score_entdeckung: float
    score_eskalation: float
    score_kosten: float
    score_total: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "coa_id": self.coa.coa_id,
            "title": self.coa.title,
            "gates_passed": self.gates_passed,
            "gate_fail_reasons": self.gate_fail_reasons,
            "score_latenz": round(self.score_latenz, 4),
            "score_entdeckung": round(self.score_entdeckung, 4),
            "score_eskalation": round(self.score_eskalation, 4),
            "score_kosten": round(self.score_kosten, 4),
            "score_total": round(self.score_total, 4),
        }


def _latenz_score(hours: float) -> float:
    if hours <= 1.0:
        return 1.0
    if hours <= 4.0:
        return 0.85
    if hours <= 8.0:
        return 0.5
    return 0.2


def evaluate_gates(coa: CoaTemplate, env: EnvironmentContext) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if coa.requires_optical_eo and (env.night or env.cloud_cover_pct > 15):
        reasons.append("EO blocked: night or cloud cover > 15%")
    if coa.requires_alb_lidar and (env.turbidity_high or env.water_depth_m > 15):
        reasons.append("ALB blocked: turbidity or depth > 15m")
    if coa.passive_only and env.seastate >= 4:
        reasons.append("Passive track hold weak at seastate >= 4")
    return (len(reasons) == 0, reasons)


def score_coa(coa: CoaTemplate, env: EnvironmentContext) -> CoaScore:
    passed, reasons = evaluate_gates(coa, env)
    latenz = _latenz_score(coa.first_contact_hours)
    entdeckung = coa.discovery_score
    eskalation = 1.0 - coa.escalation_risk
    kosten = 1.0 - coa.cost_index
    if not passed:
        total = 0.0
    else:
        total = 0.35 * latenz + 0.35 * entdeckung + 0.15 * eskalation + 0.15 * kosten
    return CoaScore(
        coa=coa,
        gates_passed=passed,
        gate_fail_reasons=reasons,
        score_latenz=latenz,
        score_entdeckung=entdeckung,
        score_eskalation=eskalation,
        score_kosten=kosten,
        score_total=total,
    )


def rank_coas(env: EnvironmentContext) -> list[CoaScore]:
    scored = [score_coa(c, env) for c in COA_CATALOG]
    eligible = [s for s in scored if s.gates_passed]
    eligible.sort(key=lambda s: s.score_total, reverse=True)
    failed = [s for s in scored if not s.gates_passed]
    failed.sort(key=lambda s: s.coa.coa_id)
    return eligible + failed