"""
Multi-mission runtime: parallel NBA missions in one NiFi node.

Trino holds orders; this module holds ephemeral bindings (MMSI, buoys, tick state)
keyed by mission_id. Marine state remains one /tmp file — ships carry
activeMissionBindings[] or legacy activeMissionId.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from marine_fleet_registry import GERMAN_NAVAL_FLEET, UNIT_BY_MMSI

MISSION_RUNTIME_STATE_FILE = os.environ.get(
    "TRITON_MISSION_RUNTIME_STATE", "/tmp/triton_mission_runtime.json"
)
DEFAULT_MAX_ACTIVE_MISSIONS = int(os.environ.get("MISSION_MAX_PARALLEL", "8"))
DEFAULT_MAX_SURFACE_UNITS = int(os.environ.get("MISSION_MAX_SURFACE_UNITS", "4"))
DEFAULT_MAX_BUOY_SLOTS = int(os.environ.get("MISSION_MAX_BUOY_SLOTS", "32"))

ACTIVE_STATUSES = frozenset({"QUEUED", "ACTIVE", "ON_STATION", "COMPLETING"})
TERMINAL_STATUSES = frozenset({"COMPLETED", "ABORTED", "FAILED"})


def mission_id_short(mission_id: str, length: int = 8) -> str:
    clean = re.sub(r"[^a-zA-Z0-9]", "", mission_id)
    return (clean or "msn")[:length].upper()


def buoy_id_for_mission(mission_id: str, seq: int) -> str:
    return f"MAD-{mission_id_short(mission_id)}-{seq:02d}"


@dataclass
class MissionAssetBinding:
    asset_type: str  # surface_unit, buoy_slot
    asset_key: str  # mmsi or buoy_id
    role: str = ""
    bound_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class MissionRuntimeRecord:
    mission_id: str
    flow_uuid: str
    status: str
    elapsed_track_min: float = 0.0
    target_mode: str = "STATIC_POINT"
    target_latitude: float = 0.0
    target_longitude: float = 0.0
    target_mmsi: str | None = None
    approved_coa_id: str = "D"
    parameters_json: dict[str, Any] = field(default_factory=dict)
    assets: list[MissionAssetBinding] = field(default_factory=list)
    buoy_seq: int = 0
    stanag_burst_pending: bool = False

    def surface_mmsis(self) -> list[str]:
        return [a.asset_key for a in self.assets if a.asset_type == "surface_unit"]

    def buoy_ids(self) -> list[str]:
        return [a.asset_key for a in self.assets if a.asset_type == "buoy_slot"]


@dataclass
class MissionRuntimeStore:
    version: int = 1
    missions: dict[str, MissionRuntimeRecord] = field(default_factory=dict)

    def active_missions(self) -> list[MissionRuntimeRecord]:
        return [m for m in self.missions.values() if m.status in ACTIVE_STATUSES - {"QUEUED"}]

    def all_non_terminal(self) -> list[MissionRuntimeRecord]:
        return [m for m in self.missions.values() if m.status not in TERMINAL_STATUSES]


def load_runtime_store(path: str | None = None) -> MissionRuntimeStore:
    p = path or MISSION_RUNTIME_STATE_FILE
    if not os.path.exists(p):
        return MissionRuntimeStore()
    with open(p, encoding="utf-8") as f:
        raw = json.load(f)
    missions: dict[str, MissionRuntimeRecord] = {}
    for mid, blob in (raw.get("missions") or {}).items():
        assets = [MissionAssetBinding(**a) for a in blob.pop("assets", [])]
        missions[mid] = MissionRuntimeRecord(**blob, assets=assets)
    return MissionRuntimeStore(version=int(raw.get("version", 1)), missions=missions)


def save_runtime_store(store: MissionRuntimeStore, path: str | None = None) -> None:
    p = path or MISSION_RUNTIME_STATE_FILE
    payload = {
        "version": store.version,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "missions": {
            mid: {**asdict(rec), "assets": [asdict(a) for a in rec.assets]}
            for mid, rec in store.missions.items()
        },
    }
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def _units_by_class(unit_class: str) -> list[dict[str, str]]:
    cls = unit_class.strip()
    return [u for u in GERMAN_NAVAL_FLEET if u.get("class") == cls]


def allocated_mmsis(store: MissionRuntimeStore, exclude_mission: str | None = None) -> set[str]:
    out: set[str] = set()
    for mid, rec in store.missions.items():
        if exclude_mission and mid == exclude_mission:
            continue
        if rec.status in TERMINAL_STATUSES:
            continue
        out.update(rec.surface_mmsis())
    return out


def allocate_surface_units(
    store: MissionRuntimeStore,
    record: MissionRuntimeRecord,
    *,
    unit_class: str,
    count: int,
) -> list[str]:
    """Pick MMSIs not already bound to another active mission."""
    if count <= 0:
        return []
    taken = allocated_mmsis(store, exclude_mission=record.mission_id)
    chosen: list[str] = []
    for unit in _units_by_class(unit_class):
        mmsi = str(unit.get("mmsi", ""))
        if not mmsi or mmsi in taken or mmsi in chosen:
            continue
        chosen.append(mmsi)
        record.assets.append(
            MissionAssetBinding(asset_type="surface_unit", asset_key=mmsi, role=unit_class)
        )
        if len(chosen) >= count:
            break
    return chosen


def reserve_buoy_slots(record: MissionRuntimeRecord, count: int, role: str = "MAD") -> list[str]:
    ids: list[str] = []
    for _ in range(count):
        record.buoy_seq += 1
        bid = buoy_id_for_mission(record.mission_id, record.buoy_seq)
        record.assets.append(
            MissionAssetBinding(asset_type="buoy_slot", asset_key=bid, role=role)
        )
        ids.append(bid)
    return ids


def parallel_resource_totals(store: MissionRuntimeStore) -> tuple[int, int, int]:
    surface = 0
    buoys = 0
    active = 0
    for rec in store.missions.values():
        if rec.status in TERMINAL_STATUSES:
            continue
        if rec.status in ACTIVE_STATUSES:
            active += 1
        surface += len(rec.surface_mmsis())
        buoys += len(rec.buoy_ids())
    return active, surface, buoys


@dataclass
class ParallelActivationVerdict:
    ok: bool
    reasons: list[str] = field(default_factory=list)


def validate_parallel_activation(
    store: MissionRuntimeStore,
    *,
    needs_surface: int,
    needs_buoys: int,
    max_active: int = DEFAULT_MAX_ACTIVE_MISSIONS,
    max_surface: int = DEFAULT_MAX_SURFACE_UNITS,
    max_buoys: int = DEFAULT_MAX_BUOY_SLOTS,
    flow_uuid: str | None = None,
    allow_same_flow_parallel: bool = True,
) -> ParallelActivationVerdict:
    reasons: list[str] = []
    active, surface, buoys = parallel_resource_totals(store)

    if active >= max_active:
        reasons.append(f"max_active_missions ({max_active}) reached")
    if surface + needs_surface > max_surface:
        reasons.append(f"surface_units would exceed cap ({max_surface})")
    if buoys + needs_buoys > max_buoys:
        reasons.append(f"buoy_slots would exceed cap ({max_buoys})")

    if flow_uuid and not allow_same_flow_parallel:
        for rec in store.missions.values():
            if rec.flow_uuid == flow_uuid and rec.status in ACTIVE_STATUSES:
                reasons.append(f"flow_uuid {flow_uuid} already has active mission")
                break

    return ParallelActivationVerdict(ok=not reasons, reasons=reasons)


def register_order_in_store(store: MissionRuntimeStore, order: dict[str, Any]) -> MissionRuntimeRecord:
    mid = str(order["mission_id"])
    params = order.get("parameters_json") or {}
    if isinstance(params, str):
        params = json.loads(params)
    rec = MissionRuntimeRecord(
        mission_id=mid,
        flow_uuid=str(order.get("flow_uuid", "")),
        status=str(order.get("status", "QUEUED")),
        target_mode=str(order.get("target_mode", "STATIC_POINT")),
        target_latitude=float(order.get("target_latitude", 0)),
        target_longitude=float(order.get("target_longitude", 0)),
        target_mmsi=order.get("target_mmsi"),
        approved_coa_id=str(order.get("approved_coa_id", "D")),
        parameters_json=params,
    )
    store.missions[mid] = rec
    return rec


def activate_mission_from_order(store: MissionRuntimeStore, order: dict[str, Any]) -> ParallelActivationVerdict:
    rec = register_order_in_store(store, order)
    surface = (rec.parameters_json.get("surface") or {})
    buoys = (rec.parameters_json.get("buoys") or {})
    needs_surface = int(surface.get("dispatch_units") or 0)
    buoy_count = int(buoys.get("count") or 0)
    verdict = validate_parallel_activation(
        store,
        needs_surface=needs_surface,
        needs_buoys=buoy_count,
        flow_uuid=rec.flow_uuid,
    )
    if not verdict.ok:
        rec.status = "FAILED"
        return verdict

    if needs_surface > 0:
        allocate_surface_units(
            store,
            rec,
            unit_class=str(surface.get("unit_class", "Korvetten")),
            count=needs_surface,
        )
    if buoy_count > 0:
        reserve_buoy_slots(rec, buoy_count, role=str(buoys.get("sensor_profile", "MAD")))

    rec.status = "ACTIVE"
    return verdict


def apply_mission_bindings_to_ships(
    ships: list[dict[str, Any]],
    store: MissionRuntimeStore,
) -> list[dict[str, Any]]:
    """
    Annotate marine simulator JSON with per-mission bindings (supports parallel).
    Does not replace german_navy_mission_engine tick — NBA executor sets routes.
    """
    by_mmsi: dict[str, list[str]] = {}
    for rec in store.active_missions():
        for mmsi in rec.surface_mmsis():
            by_mmsi.setdefault(mmsi, []).append(rec.mission_id)

    for ship in ships:
        mmsi = str(ship.get("MMSI", ""))
        mids = by_mmsi.get(mmsi, [])
        if mids:
            ship["activeMissionIds"] = mids
            ship["activeMissionId"] = mids[0]
        else:
            ship.pop("activeMissionIds", None)
            if ship.get("activeMissionId") and not mids:
                ship.pop("activeMissionId", None)
    return ships


def tick_runtime_elapsed(store: MissionRuntimeStore, tick_minutes: float = 1.0) -> None:
    for rec in store.active_missions():
        rec.elapsed_track_min += tick_minutes
        track_cap = float(
            (rec.parameters_json.get("dynamic_tracking") or {}).get("track_duration_min")
            or rec.parameters_json.get("track_duration_min")
            or 120
        )
        if rec.elapsed_track_min >= track_cap and rec.status == "ACTIVE":
            rec.status = "COMPLETING"


def orders_to_flowfile_attributes(record: MissionRuntimeRecord) -> dict[str, str]:
    """NiFi downstream attributes — one FlowFile per mission per dispatch tick."""
    return {
        "mission.id": record.mission_id,
        "mission.flow_uuid": record.flow_uuid,
        "mission.status": record.status,
        "mission.coa_id": record.approved_coa_id,
        "mission.target.lat": str(record.target_latitude),
        "mission.target.lon": str(record.target_longitude),
        "mission.target.mode": record.target_mode,
        "mission.surface.mmsis": ",".join(record.surface_mmsis()),
        "mission.buoy.ids": ",".join(record.buoy_ids()),
    }