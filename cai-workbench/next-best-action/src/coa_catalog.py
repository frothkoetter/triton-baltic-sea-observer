"""Course-of-action templates (Options A–D) for Baltic NBA Phase 0."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CoaTemplate:
    coa_id: str
    title: str
    first_contact_hours: float
    discovery_score: float  # 0–1 simulated 12h track hold
    escalation_risk: float  # 0–1 lower is better
    cost_index: float  # 0–1 lower is better
    requires_optical_eo: bool = False
    requires_alb_lidar: bool = False
    requires_heavy_surface: bool = False
    passive_only: bool = False


COA_CATALOG: tuple[CoaTemplate, ...] = (
    CoaTemplate(
        coa_id="A",
        title="Gestaffelter Multidomänen-Verbund (ICEYE SAR + P-8A + K130)",
        first_contact_hours=0.43,
        discovery_score=0.92,
        escalation_risk=0.25,
        cost_index=0.55,
    ),
    CoaTemplate(
        coa_id="B",
        title="Nordsee-Fregatten + optische EO",
        first_contact_hours=8.0,
        discovery_score=0.35,
        escalation_risk=0.30,
        cost_index=0.70,
        requires_optical_eo=True,
        requires_heavy_surface=True,
    ),
    CoaTemplate(
        coa_id="C",
        title="K130-Einzelfahrt + ALB-LiDAR",
        first_contact_hours=3.53,
        discovery_score=0.40,
        escalation_risk=0.35,
        cost_index=0.35,
        requires_alb_lidar=True,
    ),
    CoaTemplate(
        coa_id="D",
        title="Passive UAV/Partner-Beobachtung",
        first_contact_hours=1.5,
        discovery_score=0.28,
        escalation_risk=0.20,
        cost_index=0.15,
        passive_only=True,
    ),
)