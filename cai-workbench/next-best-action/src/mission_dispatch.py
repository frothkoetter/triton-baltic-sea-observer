"""Build Trino mission orders from NBA incident + approved COA + NATO playbook."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

_DATA = Path(__file__).resolve().parent.parent / "data" / "mission_nato_playbook.json"


@dataclass
class MissionTarget:
    mode: str
    latitude: float
    longitude: float
    mmsi: str | None = None
    correlation_id: str | None = None
    cog_deg: float | None = None
    sog_kn: float | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "target_mode": self.mode,
            "target_latitude": self.latitude,
            "target_longitude": self.longitude,
        }
        if self.mmsi:
            out["target_mmsi"] = self.mmsi
        if self.correlation_id:
            out["target_correlation_id"] = self.correlation_id
        if self.cog_deg is not None:
            out["target_cog_deg"] = self.cog_deg
        if self.sog_kn is not None:
            out["target_sog_kn"] = self.sog_kn
        return out


@dataclass
class MissionOrderDraft:
    mission_id: str
    flow_uuid: str
    approved_coa_id: str
    mission_type: str
    status: str
    target: MissionTarget
    parameters_json: dict[str, Any]
    track_duration_min: int
    playbook_version: str
    notes: str = ""

    def to_sql_values(self, issued_by: str = "nba-ui") -> dict[str, Any]:
        return {
            "mission_id": self.mission_id,
            "flow_uuid": self.flow_uuid,
            "approved_coa_id": self.approved_coa_id,
            "mission_type": self.mission_type,
            "status": self.status,
            "target_latitude": self.target.latitude,
            "target_longitude": self.target.longitude,
            "target_mode": self.target.mode,
            "target_mmsi": self.target.mmsi,
            "target_correlation_id": self.target.correlation_id,
            "track_duration_min": self.track_duration_min,
            "parameters_json": json.dumps(self.parameters_json, ensure_ascii=False),
            "playbook_version": self.playbook_version,
            "issued_by": issued_by,
            "notes": self.notes,
        }


def load_playbook(path: Path | None = None) -> dict[str, Any]:
    p = path or _DATA
    with p.open(encoding="utf-8") as f:
        return json.load(f)


def _parse_alert_payload(raw: str | dict | None) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def resolve_target_from_incident(incident: dict[str, Any], playbook: dict[str, Any], coa_id: str) -> MissionTarget:
    """Resolve static vs dynamic target object from incident + alert_payload."""
    payload = _parse_alert_payload(incident.get("alert_payload"))
    coa = (coa_id or incident.get("approved_coa_id") or incident.get("best_coa_id") or "D").upper()
    deployment = playbook.get("coa_deployments", {}).get(coa, {})
    default_mode = deployment.get("target_mode_default", "STATIC_POINT")

    lat = float(incident.get("latitude") or payload.get("latitude") or 54.5)
    lon = float(incident.get("longitude") or payload.get("longitude") or 11.0)
    mmsi = payload.get("target_mmsi") or payload.get("mmsi") or payload.get("reference_mmsi")
    correlation = payload.get("correlation_id") or payload.get("sonar_contact_id")
    cog = payload.get("cog_deg") or payload.get("cog")
    sog = payload.get("sog_kn") or payload.get("sog")

    mode = payload.get("target_mode") or default_mode
    if mmsi and mode == "STATIC_POINT" and default_mode == "VESSEL_MMSI":
        mode = "VESSEL_MMSI"
    if not mmsi and mode == "VESSEL_MMSI":
        mode = "CORRELATED_OBJECT" if correlation else "STATIC_POINT"

    return MissionTarget(
        mode=mode,
        latitude=lat,
        longitude=lon,
        mmsi=str(mmsi) if mmsi else None,
        correlation_id=str(correlation) if correlation else None,
        cog_deg=float(cog) if cog is not None else None,
        sog_kn=float(sog) if sog is not None else None,
    )


def build_mission_order(
    incident: dict[str, Any],
    *,
    coa_id: str | None = None,
    mission_id: str | None = None,
    playbook: dict[str, Any] | None = None,
    status: str = "QUEUED",
) -> MissionOrderDraft:
    """Merge approved COA, NATO buoy playbook, and dynamic tracking parameters."""
    pb = playbook or load_playbook()
    coa = (coa_id or incident.get("approved_coa_id") or "D").upper()
    dep = pb.get("coa_deployments", {}).get(coa)
    if not dep:
        raise ValueError(f"Unknown COA for mission playbook: {coa}")

    target = resolve_target_from_incident(incident, pb, coa)
    mid = mission_id or str(uuid.uuid4())
    flow = str(incident.get("flow_uuid") or "")

    params: dict[str, Any] = {
        "coa_id": coa,
        "playbook_rule_ids": dep.get("playbook_rule_ids", []),
        "dynamic_tracking": {
            "enabled": target.mode != "STATIC_POINT",
            "track_duration_min": dep.get("track_duration_min", 120),
            "refresh_source": "ais_events_ice",
            "snapshot_fallback": True,
        },
        "surface": dep.get("surface", {}),
        "buoys": dep.get("buoys", {}),
        "stanag": {
            "profile": dep.get("stanag_profile", "watch_section"),
            "footer_template": pb.get("stanag_footer_template"),
            "subject_prefix": pb.get("stanag_subject_prefix"),
            "tag_mission_id": True,
            "tag_buoy_on_deploy": bool(dep.get("buoys", {}).get("stanag_on_deploy")),
        },
        "formations": pb.get("formations", {}),
    }

    mission_type = dep.get("mission_type", "BUOY_SENSOR_RING")
    return MissionOrderDraft(
        mission_id=mid,
        flow_uuid=flow,
        approved_coa_id=coa,
        mission_type=mission_type,
        status=status,
        target=target,
        parameters_json=params,
        track_duration_min=int(dep.get("track_duration_min", 120)),
        playbook_version=str(pb.get("version", "")),
        notes=f"NATO playbook COA {coa}; target_mode={target.mode}",
    )


def expand_buoy_roles(coa_id: str, playbook: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Flat list of buoy slots with role, sensor type, and STANAG tagging intent."""
    pb = playbook or load_playbook()
    coa = coa_id.upper()
    roles = pb.get("coa_deployments", {}).get(coa, {}).get("buoys", {}).get("roles", [])
    out: list[dict[str, Any]] = []
    seq = 0
    for spec in roles:
        for _ in range(int(spec.get("count", 1))):
            seq += 1
            out.append(
                {
                    "seq": seq,
                    "deployment_role": spec.get("role", "MAD"),
                    "payload_object_type": spec.get("object_type", "VESSEL"),
                    "payload_object_classification": spec.get("classification"),
                    "stanag_on_deploy": pb.get("coa_deployments", {}).get(coa, {})
                    .get("buoys", {})
                    .get("stanag_on_deploy", False),
                }
            )
    return out


def parallelism_limits(playbook: dict[str, Any] | None = None) -> dict[str, int | bool]:
    pb = playbook or load_playbook()
    par = pb.get("parallelism") or {}
    return {
        "max_active_missions": int(par.get("max_active_missions", 8)),
        "max_surface_units_total": int(par.get("max_surface_units_total", 4)),
        "max_buoy_slots_total": int(par.get("max_buoy_slots_total", 32)),
        "allow_same_flow_uuid_parallel": bool(par.get("allow_same_flow_uuid_parallel", False)),
    }


def estimate_resource_needs(order: MissionOrderDraft) -> tuple[int, int]:
    surface = int((order.parameters_json.get("surface") or {}).get("dispatch_units") or 0)
    buoys = int((order.parameters_json.get("buoys") or {}).get("count") or 0)
    return surface, buoys


def validate_order_against_active(
    order: MissionOrderDraft,
    active_orders: Iterable[dict[str, Any]],
    playbook: dict[str, Any] | None = None,
) -> tuple[bool, list[str]]:
    """UI/CML check before INSERT — mirrors NiFi mission_parallel caps."""
    limits = parallelism_limits(playbook)
    reasons: list[str] = []
    active = [
        o
        for o in active_orders
        if str(o.get("status", "")).upper() not in ("COMPLETED", "ABORTED", "FAILED")
    ]
    if len(active) >= int(limits["max_active_missions"]):
        reasons.append("max_active_missions reached")

    surface = buoys = 0
    for o in active:
        p = o.get("parameters_json")
        if isinstance(p, str):
            p = json.loads(p)
        surface += int((p or {}).get("surface", {}).get("dispatch_units") or 0)
        buoys += int((p or {}).get("buoys", {}).get("count") or 0)

    need_s, need_b = estimate_resource_needs(order)
    if surface + need_s > int(limits["max_surface_units_total"]):
        reasons.append("max_surface_units_total exceeded")
    if buoys + need_b > int(limits["max_buoy_slots_total"]):
        reasons.append("max_buoy_slots_total exceeded")

    if not limits["allow_same_flow_uuid_parallel"]:
        for o in active:
            if o.get("flow_uuid") == order.flow_uuid:
                reasons.append("incident already has active mission")
                break

    return (not reasons, reasons)