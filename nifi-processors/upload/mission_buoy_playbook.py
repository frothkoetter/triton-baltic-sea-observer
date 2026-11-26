"""Build MAD buoy records for NBA MISSION_PLAYBOOK deployments."""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from typing import Any

from baltic_water_placement import is_baltic_water, snap_to_baltic_water

from mission_parallel import buoy_id_for_mission
from mission_target_track import TrackSample, formation_slots


def _pick_object_example(examples: list[dict], *, obj_type: str | None = None) -> dict:
    if obj_type:
        filtered = [e for e in examples if e.get("type") == obj_type]
        if filtered:
            return random.choice(filtered)
    return random.choice(examples)


def build_mission_buoy_records(
    *,
    mission_id: str,
    target_lat: float,
    target_lon: float,
    buoy_ids: list[str],
    formations_spec: dict[str, Any],
    formation_key: str,
    buoy_roles: list[dict[str, Any]] | None,
    object_examples: list[dict],
    correlation_id: str | None = None,
    cog_deg: float | None = None,
    sensor_profile: str = "MAD",
) -> list[dict[str, Any]]:
    lat, lon = snap_to_baltic_water(target_lon, target_lat)
    center = TrackSample(latitude=lat, longitude=lon, cog_deg=cog_deg, sog_kn=None)
    count = len(buoy_ids) if buoy_ids else 1
    slots = formation_slots(center, formation_key, count, formations_spec)
    if len(slots) < count:
        slots = slots + [(lat, lon, f"slot_{i}") for i in range(len(slots), count)]

    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")[:-6] + "Z"
    records: list[dict[str, Any]] = []
    for idx, buoy_id in enumerate(buoy_ids or [buoy_id_for_mission(mission_id, 1)]):
        slot_lat, slot_lon, _ = slots[idx] if idx < len(slots) else (lat, lon)
        if not is_baltic_water(slot_lon, slot_lat):
            slot_lon, slot_lat = snap_to_baltic_water(slot_lon, slot_lat)

        role = (buoy_roles or [{}])[idx] if idx < len(buoy_roles or []) else {}
        obj_type = role.get("payload_object_type")
        example = _pick_object_example(object_examples, obj_type=obj_type)
        classification = role.get("payload_object_classification") or example.get("classification")
        conf = example.get("confidence")
        conf_val = conf() if callable(conf) else conf
        depth = example.get("estimatedDepth")
        depth_val = depth() if callable(depth) else (depth if depth is not None else 20)

        corr = correlation_id or f"MSN-{mission_id[:8]}-{idx + 1:02d}"

        records.append(
            {
                "buoyid": buoy_id,
                "mission_id": mission_id,
                "einsatz_id": mission_id,
                "deployment_role": role.get("deployment_role", "MAD"),
                "playbook_sensor_profile": sensor_profile,
                "ts": ts,
                "geo_position_lat": round(slot_lat, 4),
                "geo_position_lon": round(slot_lon, 4),
                "altitude": random.randint(50, 180),
                "payload_magneticField_totalField": round(random.uniform(49900.0, 50300.0), 1),
                "payload_magneticField_anomaly": round(random.uniform(-120.0, 120.0), 1),
                "payload_magneticField_gradient": example.get("gradient", "MODERATE"),
                "payload_detectionConfidence": example.get("detectionConfidence", "HIGH"),
                "payload_object_type": example.get("type"),
                "payload_object_classification": classification,
                "payload_object_confidence": conf_val,
                "payload_object_estimatedDepth": depth_val,
                "payload_object_motion": example.get("motion", "MOVING"),
                "payload_object_extent": example.get("extent"),
                "payload_object_notes": example.get("notes"),
                "payload_object_orientation": example.get("orientation"),
                "payload_object_correlationId": corr,
            }
        )
    return records


def records_to_ndjson(records: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in records)