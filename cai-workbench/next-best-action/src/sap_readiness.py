"""SAP D&S mission readiness fusion (MP-0 sample / MP-1 gates)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .coa_catalog import COA_CATALOG, CoaTemplate
from .config import Settings
from .scoring import CoaScore

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_ACCEPTABLE_READINESS = frozenset({"R1", "R2"})


@dataclass
class SapUnit:
    force_element_id: str
    name: str
    readiness_code: str
    capabilities: list[str]
    rotation_conflict: bool = False
    fuel_days_remaining: int | None = None
    munition_gate: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "force_element_id": self.force_element_id,
            "name": self.name,
            "readiness_code": self.readiness_code,
            "capabilities": self.capabilities,
            "rotation_conflict": self.rotation_conflict,
            "fuel_days_remaining": self.fuel_days_remaining,
            "munition_gate": self.munition_gate,
        }


@dataclass
class MissionReadinessContext:
    source: str
    queried_at: str
    capability_filter: list[str]
    units: list[SapUnit]
    coa_requirements: dict[str, Any]

    def section_10_payload(self, coa_impact: dict[str, Any]) -> dict[str, Any]:
        return {
            "source": self.source,
            "queried_at": self.queried_at,
            "capability_filter": self.capability_filter,
            "units": [u.to_dict() for u in self.units],
            "coa_impact": coa_impact,
        }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _capability_map_path(settings: Settings) -> Path:
    raw = settings.sap_capability_map_path
    if raw:
        return Path(raw)
    return _DATA_DIR / "sap_capability_map.json"


def _sample_path(settings: Settings) -> Path:
    raw = settings.sap_readiness_sample_path
    if raw:
        return Path(raw)
    return _DATA_DIR / "sap_readiness_sample.json"


def load_capability_map(settings: Settings) -> dict[str, Any]:
    return _load_json(_capability_map_path(settings))


def _parse_units(raw_units: list[dict[str, Any]]) -> list[SapUnit]:
    out: list[SapUnit] = []
    for row in raw_units:
        caps = row.get("capabilities") or []
        if isinstance(caps, str):
            caps = [c.strip() for c in caps.split(",") if c.strip()]
        out.append(
            SapUnit(
                force_element_id=str(row["force_element_id"]),
                name=str(row.get("name", row["force_element_id"])),
                readiness_code=str(row.get("readiness_code", "R4")),
                capabilities=[str(c) for c in caps],
                rotation_conflict=bool(row.get("rotation_conflict")),
                fuel_days_remaining=row.get("fuel_days_remaining"),
                munition_gate=str(row.get("munition_gate", "ok")),
            )
        )
    return out


def _load_from_sample(settings: Settings) -> MissionReadinessContext:
    payload = _load_json(_sample_path(settings))
    cap_map = load_capability_map(settings)
    caps = sorted(
        {
            c
            for req in cap_map.get("coa_requirements", {}).values()
            for c in req.get("capabilities", [])
        }
    )
    return MissionReadinessContext(
        source="sap_sample",
        queried_at=str(payload.get("queried_at", datetime.now(timezone.utc).isoformat())),
        capability_filter=caps,
        units=_parse_units(payload.get("units", [])),
        coa_requirements=cap_map.get("coa_requirements", {}),
    )


def _load_from_trino(settings: Settings) -> MissionReadinessContext | None:
    try:
        from . import trino_client
    except ImportError:
        return None
    sql = (
        f"SELECT force_element_id, name, readiness_code, capabilities, "
        f"rotation_conflict, fuel_days_remaining, munition_gate "
        f"FROM {settings.trino_catalog}.{settings.trino_schema}.sap_readiness_staging "
        f"ORDER BY snapshot_at DESC LIMIT 100"
    )
    try:
        rows = trino_client.fetch_all(sql)
    except Exception:
        return None
    if not rows:
        return None
    cap_map = load_capability_map(settings)
    return MissionReadinessContext(
        source="trino_staging",
        queried_at=datetime.now(timezone.utc).isoformat(),
        capability_filter=[],
        units=_parse_units(rows),
        coa_requirements=cap_map.get("coa_requirements", {}),
    )


def _load_from_odata_stub(settings: Settings) -> MissionReadinessContext:
    """MP-1: OData client placeholder — same payload as sample until BTP wired."""
    ctx = _load_from_sample(settings)
    ctx.source = "sap_odata_stub"
    return ctx


def fetch_mission_readiness(settings: Settings) -> MissionReadinessContext | None:
    mode = (settings.sap_readiness_mode or "off").strip().lower()
    if mode == "off":
        return None
    if mode == "sample":
        return _load_from_sample(settings)
    if mode == "odata":
        return _load_from_odata_stub(settings)
    if mode == "trino":
        ctx = _load_from_trino(settings)
        return ctx or _load_from_sample(settings)
    return None


def _units_covering(capability: str, units: list[SapUnit]) -> list[SapUnit]:
    return [u for u in units if capability in u.capabilities]


def evaluate_sap_for_coa(coa_id: str, ctx: MissionReadinessContext) -> tuple[bool, list[str]]:
    req = ctx.coa_requirements.get(coa_id) or {}
    needed: list[str] = list(req.get("capabilities") or [])
    if not needed:
        return True, []
    allowed = set(req.get("readiness_codes") or _ACCEPTABLE_READINESS)
    blockers: list[str] = []
    for cap in needed:
        candidates = _units_covering(cap, ctx.units)
        if not candidates:
            blockers.append(f"sap_readiness:no unit for capability {cap}")
            continue
        ok_unit = None
        for u in candidates:
            if u.rotation_conflict:
                continue
            if u.munition_gate == "blocked":
                continue
            if u.readiness_code not in allowed:
                continue
            ok_unit = u
            break
        if ok_unit is None:
            best = candidates[0]
            blockers.append(
                f"sap_readiness:{best.name} ({best.force_element_id}) "
                f"not {','.join(sorted(allowed))} for {cap} "
                f"(actual {best.readiness_code})"
            )
    return (len(blockers) == 0, blockers)


def build_coa_impact(ctx: MissionReadinessContext) -> dict[str, Any]:
    impact: dict[str, Any] = {}
    for coa in COA_CATALOG:
        ok, blockers = evaluate_sap_for_coa(coa.coa_id, ctx)
        impact[coa.coa_id] = {"executable": ok, "blockers": blockers}
    return impact


def apply_sap_readiness_gates(
    ranked: list[CoaScore], ctx: MissionReadinessContext
) -> tuple[list[CoaScore], dict[str, Any]]:
    """Merge SAP blockers into COA scores and re-rank."""
    coa_impact = build_coa_impact(ctx)
    updated: list[CoaScore] = []
    for score in ranked:
        sap_ok, sap_reasons = evaluate_sap_for_coa(score.coa.coa_id, ctx)
        env_passed = score.gates_passed and len(score.gate_fail_reasons) == 0
        if env_passed and sap_ok:
            passed = True
            reasons = list(score.gate_fail_reasons)
            total = score.score_total
        else:
            passed = False
            reasons = list(score.gate_fail_reasons)
            if not sap_ok:
                reasons.extend(sap_reasons)
            elif not env_passed:
                reasons = list(score.gate_fail_reasons)
            total = 0.0
        updated.append(
            CoaScore(
                coa=score.coa,
                gates_passed=passed,
                gate_fail_reasons=reasons,
                score_latenz=score.score_latenz,
                score_entdeckung=score.score_entdeckung,
                score_eskalation=score.score_eskalation,
                score_kosten=score.score_kosten,
                score_total=total,
            )
        )
    eligible = [s for s in updated if s.gates_passed]
    eligible.sort(key=lambda s: s.score_total, reverse=True)
    failed = [s for s in updated if not s.gates_passed]
    failed.sort(key=lambda s: s.coa.coa_id)
    section = ctx.section_10_payload(coa_impact)
    return eligible + failed, section